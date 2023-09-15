import logging
import os
from typing import Optional

__all__ = [
    "logger",
    "getLogLevel",
]


def _configure_logging() -> logging.Logger:
    """Configure logging for the Spark Connect clients."""
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s %(process)d %(levelname)s %(funcName)s %(message)s")
    )
    logger.addHandler(handler)

    # Check the environment variables for log levels:
    if "SPARK_CONNECT_LOG_LEVEL" in os.environ:
        logger.setLevel(os.environ["SPARK_CONNECT_LOG_LEVEL"].upper())
    else:
        logger.disabled = True
    return logger


# Instantiate the logger based on the environment configuration.
logger = _configure_logging()


def getLogLevel() -> Optional[int]:
    """
    This returns this log level as integer, or none (if no logging is enabled).

    Spark Connect logging can be configured with environment variable 'SPARK_CONNECT_LOG_LEVEL'

    .. versionadded:: 3.5.0
    """

    if not logger.disabled:
        return logger.level
    return None
