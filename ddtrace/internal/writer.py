import os
import sys
import time

from .. import api
from .. import compat
from .. import _worker
from ..internal.logger import get_logger
from ..sampler import BasePrioritySampler
from ..settings import config
from ..encoding import JSONEncoderV2
from ddtrace.vendor.six.moves import queue

log = get_logger(__name__)


DEFAULT_TIMEOUT = 5
LOG_ERR_INTERVAL = 60


class LogWriter:
    def __init__(self, out=sys.stdout, filters=None, sampler=None, priority_sampler=None):
        self._filters = filters
        self._sampler = sampler
        self._priority_sampler = priority_sampler
        self.encoder = JSONEncoderV2()
        self.out = out

    def recreate(self):
        """ Create a new instance of :class:`LogWriter` using the same settings from this instance

        :rtype: :class:`LogWriter`
        :returns: A new :class:`LogWriter` instance
        """
        writer = self.__class__(
            out=self.out, filters=self._filters, sampler=self._sampler, priority_sampler=self._priority_sampler
        )
        return writer

    def _apply_filters(self, traces):
        if self._filters is not None:
            filtered_traces = []
            for trace in traces:
                for filtr in self._filters:
                    trace = filtr.process_trace(trace)
                    if trace is None:
                        break
                if trace is not None:
                    filtered_traces.append(trace)
            return filtered_traces
        return traces

    def write(self, spans=None, services=None):
        # We immediately flush all spans
        if not spans:
            return

        # Before logging the traces, make them go through the
        # filters
        try:
            traces = self._apply_filters([spans])
        except Exception:
            log.error("error while filtering traces", exc_info=True)
            return
        if len(traces) == 0:
            return
        encoded = self.encoder.encode_traces(traces)
        self.out.write(encoded + "\n")
        self.out.flush()


class AgentWriter(_worker.PeriodicWorkerThread):

    QUEUE_PROCESSING_INTERVAL = 1
    QUEUE_MAX_TRACES_DEFAULT = 1000

    def __init__(
        self,
        hostname="localhost",
        port=8126,
        uds_path=None,
        https=False,
        shutdown_timeout=DEFAULT_TIMEOUT,
        filters=None,
        sampler=None,
        priority_sampler=None,
        dogstatsd=None,
    ):
        super(AgentWriter, self).__init__(
            interval=self.QUEUE_PROCESSING_INTERVAL, exit_timeout=shutdown_timeout, name=self.__class__.__name__
        )
        # DEV: provide a _temporary_ solution to allow users to specify a custom max
        maxsize = int(os.getenv("DD_TRACE_MAX_TPS", self.QUEUE_MAX_TRACES_DEFAULT))
        self._trace_queue = TraceCollector(maxsize=maxsize)
        self._filters = filters or []
        self._sampler = sampler
        self._priority_sampler = priority_sampler
        self._last_error_ts = 0
        self.dogstatsd = dogstatsd
        self.api = api.API(
            hostname, port, uds_path=uds_path, https=https, priority_sampling=priority_sampler is not None
        )
        if hasattr(time, "thread_time"):
            self._last_thread_time = time.thread_time()
        self._started = False

    def recreate(self):
        """Create a new instance of :class:`AgentWriter` using the same settings from this instance

        :rtype: :class:`AgentWriter`
        :returns: A new :class:`AgentWriter` instance
        """
        writer = self.__class__(
            hostname=self.api.hostname,
            port=self.api.port,
            uds_path=self.api.uds_path,
            https=self.api.https,
            shutdown_timeout=self.exit_timeout,
            filters=self._filters,
            priority_sampler=self._priority_sampler,
            dogstatsd=self.dogstatsd,
        )
        return writer

    @property
    def _send_stats(self):
        """Determine if we're sending stats or not."""
        return bool(config.health_metrics_enabled and self.dogstatsd)

    def _apply_filters(self, trace):
        for f in self._filters:
            try:
                trace = f.process_trace(trace)
            except Exception:
                log.error("error while filtering traces in filter %r", f, exc_info=True)
        return trace

    def write(self, spans=None, services=None):
        # Start the AgentWriter on first write.
        # Starting it earlier might be an issue with gevent, see:
        # https://github.com/DataDog/dd-trace-py/issues/1192
        if self._started is False:
            self.start()
            self._started = True

        if spans:
            trace = self._apply_filters(spans)
            self._trace_queue.put(trace)

    def _send_payload(self, payload):
        try:
            response = self.api.send_trace_payload(payload)
        except Exception as e:
            self._log_api_error(e)
            if self._send_stats:
                self.dogstatsd.increment("datadog.tracer.api.errors")
        else:
            if response.status >= 400:
                self._log_api_error(
                    "HTTP error status %s, reason %s, message %s", response.status, response.reason, response.msg
                )
            elif self._priority_sampler or isinstance(self._sampler, BasePrioritySampler):
                result_traces_json = response.get_json()
                if result_traces_json and "rate_by_service" in result_traces_json:
                    if self._priority_sampler:
                        self._priority_sampler.update_rate_by_service_sample_rates(
                            result_traces_json["rate_by_service"],
                        )
                    if isinstance(self._sampler, BasePrioritySampler):
                        self._sampler.update_rate_by_service_sample_rates(result_traces_json["rate_by_service"],)

            if self._send_stats:
                self.dogstatsd.increment("datadog.tracer.api.responses", tags=["status:%d" % response.status])
        finally:
            if self._send_stats:
                self.dogstatsd.increment("datadog.tracer.api.requests")

    def flush_queue(self):
        try:
            traces = self._trace_queue.get(block=False)
        except queue.Empty:
            return

        payload = api.Payload(self.api.encoder)
        for trace in traces:
            try:
                payload.add_trace(trace)
            except api.PayloadFull as e:
                self._send_payload(payload)
                payload = e.next_payload

        self._send_payload(payload)

        # Dump statistics
        # NOTE: Do not use the buffering of dogstatsd as it's not thread-safe
        # https://github.com/DataDog/datadogpy/issues/439
        if self._send_stats:
            traces_queue_length = len(traces)
            traces_queue_spans = sum(map(len, traces))
            traces_filtered = len(traces) - traces_queue_length

            # Statistics about the queue length, size and number of spans
            self.dogstatsd.increment("datadog.tracer.flushes")
            self._histogram_with_total("datadog.tracer.flush.traces", traces_queue_length)
            self._histogram_with_total("datadog.tracer.flush.spans", traces_queue_spans)

            # Statistics about the filtering
            self._histogram_with_total("datadog.tracer.flush.traces_filtered", traces_filtered)

            # Statistics about the writer thread
            if hasattr(time, "thread_time"):
                new_thread_time = time.thread_time()
                diff = new_thread_time - self._last_thread_time
                self._last_thread_time = new_thread_time
                self.dogstatsd.histogram("datadog.tracer.writer.cpu_time", diff)

    def _histogram_with_total(self, name, value, tags=None):
        """Helper to add metric as a histogram and with a `.total` counter"""
        self.dogstatsd.histogram(name, value, tags=tags)
        self.dogstatsd.increment("%s.total" % (name,), value, tags=tags)

    def run_periodic(self):
        if self._send_stats:
            self.dogstatsd.gauge("datadog.tracer.heartbeat", 1)

        try:
            self.flush_queue()
        finally:
            if not self._send_stats:
                return

            # Statistics about the rate at which spans are inserted in the queue
            dropped, enqueued, enqueued_lengths = self._trace_queue.reset_stats()
            self.dogstatsd.gauge("datadog.tracer.queue.max_length", self._trace_queue.maxsize)
            self.dogstatsd.increment("datadog.tracer.queue.dropped.traces", dropped)
            self.dogstatsd.increment("datadog.tracer.queue.enqueued.traces", enqueued)
            self.dogstatsd.increment("datadog.tracer.queue.enqueued.spans", enqueued_lengths)

    def on_shutdown(self):
        try:
            self.run_periodic()
        finally:
            if not self._send_stats:
                return

            self.dogstatsd.increment("datadog.tracer.shutdown")

    def _log_api_error(self, msg, *args, **kwargs):
        log_level = log.debug
        now = compat.monotonic()
        if now > self._last_error_ts + LOG_ERR_INTERVAL:
            log_level = log.error
            self._last_error_ts = now
        prefix = "Failed to send traces to Datadog Agent at %s: "
        log_level(prefix + msg, self.api, *args, **kwargs)


class TraceCollector(queue.Queue):
    """
    """

    def __init__(self, report_stats=False, maxsize=0):
        queue.Queue.__init__(self, maxsize)
        self._report_stats = report_stats
        self._stats = dict(
            dropped=0,  # Number of item dropped (queue full)
            accepted=0,  # Number of items accepted
            accepted_lengths=0,  # Cumulative length of accepted items
        )

    def put(self, trace):
        try:
            queue.Queue.put(self, trace, block=False)
        except queue.Full:
            self._stats["dropped"] += 1
            log.warning("Trace collector is full has more than %d traces, dropping trace %r", self.maxsize, trace)
        else:
            with self.mutex:
                self._stats["accepted"] += 1
                self._stats["accepted_lengths"] += len(trace)

    def _reset_stats(self):
        """Reset the stats to 0.

        :return: The current value of dropped, accepted and accepted_lengths.
        """
        with self.mutex:
            self._stats["dropped"] = 0
            self._stats["accepted"] = 0
            self._stats["accepted_lengths"] = 0

    def _get(self):
        things = self.queue
        self._init(self.maxsize)
        return things
        # try:
        #     return self.queue
        # finally:
        #     self.clear()
