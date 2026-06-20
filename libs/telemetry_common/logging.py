import contextlib
import contextvars
import logging
import sys
import uuid

from opentelemetry import trace

_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="")


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        span_context = trace.get_current_span().get_span_context()
        record.trace_id = format(span_context.trace_id, "032x") if span_context.is_valid else ""
        record.correlation_id = get_correlation_id()
        return True


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(ContextFilter())
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s trace_id=%(trace_id)s "
            "correlation_id=%(correlation_id)s %(message)s"
        )
    )
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)


def get_correlation_id() -> str:
    return _correlation_id.get()


@contextlib.contextmanager
def correlation_context(value: str | None = None):
    token = _correlation_id.set(value or str(uuid.uuid4()))
    try:
        yield get_correlation_id()
    finally:
        _correlation_id.reset(token)
