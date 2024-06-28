import logging
from contextlib import contextmanager


@contextmanager
def assert_logs(logger_name, level):
    log_capture = []

    class ListHandler(logging.Handler):
        def emit(self, record):
            log_capture.append(self.format(record))

    logger = logging.getLogger(logger_name)
    list_handler = ListHandler()
    list_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(list_handler)
    logger.setLevel(level)
    try:
        yield log_capture
    finally:
        logger.removeHandler(list_handler)
