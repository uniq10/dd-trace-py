import threading

from .context import Context


try:
    from contextvars import ContextVar

    _DD_CONTEXTVAR = ContextVar("datadog_contextvar", default=None)
    CONTEXTVARS_IS_AVAILABLE = True
except ImportError:
    CONTEXTVARS_IS_AVAILABLE = False


class BaseContextProvider(object):
    """
    A ``ContextProvider`` is an interface that provides the blueprint
    for a callable class, capable to retrieve the current active
    ``Context`` instance. Context providers must inherit this class
    and implement:
    * the ``active`` method, that returns the current active ``Context``
    * the ``activate`` method, that sets the current active ``Context``
    """

    def _has_active_context(self):
        raise NotImplementedError

    def activate(self, context):
        raise NotImplementedError

    def active(self):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        """Method available for backward-compatibility. It proxies the call to
        ``self.active()`` and must not do anything more.
        """
        return self.active()


class ThreadContextProvider(BaseContextProvider):
    """
    ThreadLocalContext can be used as a tracer global reference to create
    a different ``Context`` for each thread. In synchronous tracer, this
    is required to prevent multiple threads sharing the same ``Context``
    in different executions.
    """
    def __init__(self):
        self._locals = threading.local()

    def _has_active_context(self):
        """
        Determine whether we have a currently active context for this thread

        :returns: Whether an active context exists
        :rtype: bool
        """
        ctx = getattr(self._locals, "context", None)
        return ctx is not None

    def activate(self, context):
        setattr(self._locals, "context", context)

    def active(self):
        ctx = getattr(self._locals, "context", None)
        if not ctx:
            ctx = Context()
            self._locals.context = ctx

        return ctx


class ContextVarContextProvider(BaseContextProvider):
    """
    Default context provider that retrieves all contexts from the current
    thread-local storage. It is suitable for synchronous programming and
    Python WSGI frameworks.
    """
    def __init__(self):
        _DD_CONTEXTVAR.set(None)

    def _has_active_context(self):
        """
        Check whether we have a currently active context.

        :returns: Whether we have an active context
        :rtype: bool
        """
        ctx = _DD_CONTEXTVAR.get()
        return ctx is not None

    def activate(self, context):
        """Makes the given ``context`` active, so that the provider calls
        the thread-local storage implementation.
        """
        _DD_CONTEXTVAR.set(context)

    def active(self):
        """Returns the current active ``Context`` for this tracer. Returned
        ``Context`` must be thread-safe or thread-local for this specific
        implementation.
        """
        ctx = _DD_CONTEXTVAR.get()
        if not ctx:
            ctx = Context()
            self.activate(ctx)

        return ctx


if CONTEXTVARS_IS_AVAILABLE:
    DefaultContextProvider = ContextVarContextProvider
else:
    DefaultContextProvider = ThreadContextProvider
