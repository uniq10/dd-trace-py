import ddtrace
from json import loads
import socket

from .encoding import Encoder, JSONEncoder
from .compat import httplib, PYTHON_VERSION, PYTHON_INTERPRETER, get_connection_response
from .internal.logger import get_logger
from .internal.runtime import container
from .utils.deprecation import deprecated


log = get_logger(__name__)


_VERSIONS = {'v0.4': {'traces': '/v0.4/traces',
                      'services': '/v0.4/services',
                      'compatibility_mode': False,
                      'fallback': 'v0.3'},
             'v0.3': {'traces': '/v0.3/traces',
                      'services': '/v0.3/services',
                      'compatibility_mode': False,
                      'fallback': 'v0.2'},
             'v0.2': {'traces': '/v0.2/traces',
                      'services': '/v0.2/services',
                      'compatibility_mode': True,
                      'fallback': None}}


class Response(object):
    """
    Custom API Response object to represent a response from calling the API.

    We do this to ensure we know expected properties will exist, and so we
    can call `resp.read()` and load the body once into an instance before we
    close the HTTPConnection used for the request.
    """
    __slots__ = ['status', 'body', 'reason', 'msg']

    def __init__(self, status=None, body=None, reason=None, msg=None):
        self.status = status
        self.body = body
        self.reason = reason
        self.msg = msg

    @classmethod
    def from_http_response(cls, resp):
        """
        Build a ``Response`` from the provided ``HTTPResponse`` object.

        This function will call `.read()` to consume the body of the ``HTTPResponse`` object.

        :param resp: ``HTTPResponse`` object to build the ``Response`` from
        :type resp: ``HTTPResponse``
        :rtype: ``Response``
        :returns: A new ``Response``
        """
        return cls(
            status=resp.status,
            body=resp.read(),
            reason=getattr(resp, 'reason', None),
            msg=getattr(resp, 'msg', None),
        )

    def get_json(self):
        """Helper to parse the body of this request as JSON"""
        try:
            body = self.body
            if not body:
                log.debug('Empty reply from Datadog Agent, %r', self)
                return

            if not isinstance(body, str) and hasattr(body, 'decode'):
                body = body.decode('utf-8')

            if hasattr(body, 'startswith') and body.startswith('OK'):
                # This typically happens when using a priority-sampling enabled
                # library with an outdated agent. It still works, but priority sampling
                # will probably send too many traces, so the next step is to upgrade agent.
                log.debug('Cannot parse Datadog Agent response, please make sure your Datadog Agent is up to date')
                return

            return loads(body)
        except (ValueError, TypeError):
            log.debug('Unable to parse Datadog Agent JSON response: %r', body, exc_info=True)

    def __repr__(self):
        return '{0}(status={1!r}, body={2!r}, reason={3!r}, msg={4!r})'.format(
            self.__class__.__name__,
            self.status,
            self.body,
            self.reason,
            self.msg,
        )


class UDSHTTPConnection(httplib.HTTPConnection):
    """An HTTP connection established over a Unix Domain Socket."""

    # It's "important" to keep the hostname and port arguments here; while there are not used by the connection
    # mechanism, they are actually used as HTTP headers such as `Host`.
    def __init__(self, path, https, *args, **kwargs):
        if https:
            httplib.HTTPSConnection.__init__(self, *args, **kwargs)
        else:
            httplib.HTTPConnection.__init__(self, *args, **kwargs)
        self.path = path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.path)
        self.sock = sock


class API(object):
    """
    Send data to the trace agent using the HTTP protocol and JSON format
    """

    TRACE_COUNT_HEADER = 'X-Datadog-Trace-Count'

    # Default timeout when establishing HTTP connection and sending/receiving from socket.
    # This ought to be enough as the agent is local
    TIMEOUT = 2

    def __init__(self, hostname, port, uds_path=None, https=False, headers=None, encoder=None, priority_sampling=False):
        """Create a new connection to the Tracer API.

        :param hostname: The hostname.
        :param port: The TCP port to use.
        :param uds_path: The path to use if the connection is to be established with a Unix Domain Socket.
        :param headers: The headers to pass along the request.
        :param encoder: The encoder to use to serialize data.
        :param priority_sampling: Whether to use priority sampling.
        """
        self.hostname = hostname
        self.port = int(port)
        self.uds_path = uds_path
        self.https = https

        self._headers = headers or {}
        self._version = None

        if priority_sampling:
            self._set_version('v0.4', encoder=encoder)
        else:
            self._set_version('v0.3', encoder=encoder)

        self._headers.update({
            'Datadog-Meta-Lang': 'python',
            'Datadog-Meta-Lang-Version': PYTHON_VERSION,
            'Datadog-Meta-Lang-Interpreter': PYTHON_INTERPRETER,
            'Datadog-Meta-Tracer-Version': ddtrace.__version__,
        })

        # Add container information if we have it
        self._container_info = container.get_container_info()
        if self._container_info and self._container_info.container_id:
            self._headers.update({
                'Datadog-Container-Id': self._container_info.container_id,
            })

    def __str__(self):
        if self.uds_path:
            return 'unix://' + self.uds_path
        if self.https:
            scheme = 'https://'
        else:
            scheme = 'http://'
        return '%s%s:%s' % (scheme, self.hostname, self.port)

    def _set_version(self, version, encoder=None):
        if version not in _VERSIONS:
            version = 'v0.2'
        if version == self._version:
            return
        self._version = version
        self._traces_endpoint = _VERSIONS[version]['traces']
        self._fallback = _VERSIONS[version]['fallback']
        self._compatibility_mode = _VERSIONS[version]['compatibility_mode']
        if self._compatibility_mode:
            self._encoder = JSONEncoder()
        else:
            self._encoder = encoder or Encoder()
        # overwrite the Content-type with the one chosen in the Encoder
        self._headers.update({'Content-Type': self._encoder.content_type})

    def _downgrade(self):
        """
        Downgrades the used encoder and API level. This method must fallback to a safe
        encoder and API, so that it will success despite users' configurations. This action
        ensures that the compatibility mode is activated so that the downgrade will be
        executed only once.
        """
        self._set_version(self._fallback)

    @property
    def encoder(self):
        return self._encoder

    def send_trace_payload(self, payload):
        headers = {self.TRACE_COUNT_HEADER: str(payload.length)}

        response = self._put(self._traces_endpoint, payload.get_payload(), headers)

        # the API endpoint is not available so we should downgrade the connection and re-try the call
        if response.status in [404, 415] and self._fallback:
            log.debug("calling endpoint '%s' but received %s; downgrading API", self._traces_endpoint, response.status)
            self._downgrade()
            return self.send_trace_payload(payload)

        return response

    @deprecated(message='Sending services to the API is no longer necessary', version='1.0.0')
    def send_services(self, *args, **kwargs):
        return

    def _put(self, endpoint, data, headers=None):
        _headers = self._headers.copy()
        if headers:
            _headers.update(headers)

        if self.uds_path is None:
            if self.https:
                conn = httplib.HTTPSConnection(self.hostname, self.port, timeout=self.TIMEOUT)
            else:
                conn = httplib.HTTPConnection(self.hostname, self.port, timeout=self.TIMEOUT)
        else:
            conn = UDSHTTPConnection(self.uds_path, self.https, self.hostname, self.port, timeout=self.TIMEOUT)

        try:
            conn.request('PUT', endpoint, data, _headers)

            # Parse the HTTPResponse into an API.Response
            # DEV: This will call `resp.read()` which must happen before the `conn.close()` below,
            #      if we call `.close()` then all future `.read()` calls will return `b''`
            resp = get_connection_response(conn)
            return Response.from_http_response(resp)
        finally:
            conn.close()


class PayloadFull(Exception):
    """The payload is full."""

    def __init__(self, next_payload):
        self.next_payload = next_payload


class TraceTooBig(Exception):
    """The payload is full."""
    pass


class Payload(object):
    """
    Trace agent API payload buffer class

    This class is used to encoded and store traces to build the payload we send to
    the trace agent.

    DEV: We encoded and buffer traces so that we can reliable determine the size of
         the payload easily so we can flush based on the payload size.
    """
    __slots__ = ('traces', 'size', 'encoder', 'max_payload_size')

    # Trace agent limit payload size of 10 MB
    # 5 MB should be a good average efficient size
    DEFAULT_MAX_PAYLOAD_SIZE = 5 * 1000000

    def __init__(self, encoder=None, max_payload_size=DEFAULT_MAX_PAYLOAD_SIZE):
        """
        Constructor for Payload

        :param encoder: The encoded to use, default is the default encoder
        :type encoder: ``ddtrace.encoding.Encoder``
        :param max_payload_size: The max number of bytes a payload should be before
            being considered full (default: 5mb)
        """
        self.max_payload_size = max_payload_size
        self.encoder = encoder or Encoder()
        self.traces = []
        self.size = 0

    def add_trace(self, trace):
        """
        Encode and append a trace to this payload

        :param trace: A trace to append
        :type trace: A list of :class:`ddtrace.span.Span`
        """
        # No trace or empty trace was given, ignore
        if not trace:
            return

        # Encode the trace, append, and add its length to the size
        encoded = self.encoder.encode_trace(trace)
        if len(encoded) > self.max_payload_size:
            log.warning('Trace %r is too big to fit in a payload, dropping it', trace)
            raise TraceTooBig()
        elif len(encoded) + self.size > self.max_payload_size:
            # Start the next payload
            next_payload = Payload(self.encoder)
            next_payload.traces.append(encoded)
            raise PayloadFull(encoded)

        self.traces.append(encoded)
        self.size += len(encoded)

    @property
    def length(self):
        """
        Get the number of traces in this payload

        :returns: The number of traces in the payload
        :rtype: int
        """
        return len(self.traces)

    @property
    def empty(self):
        """
        Whether this payload is empty or not

        :returns: Whether this payload is empty or not
        :rtype: bool
        """
        return self.length == 0

    def get_payload(self):
        """
        Get the fully encoded payload

        :returns: The fully encoded payload
        :rtype: str | bytes
        """
        # DEV: `self.traces` is an array of encoded traces, `join_encoded` joins them together
        return self.encoder.join_encoded(self.traces)

    def __repr__(self):
        """Get the string representation of this payload"""
        return '{0}(length={1}, size={2} B, max_payload_size={3} B)'.format(
            self.__class__.__name__, self.length, self.size, self.max_payload_size)
