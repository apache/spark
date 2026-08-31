#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import logging
import os
from typing import Optional

from pyspark.logger.logger import JSONFormatter

__all__ = ["configureLogging", "getLogger", "getLogLevel"]

# Root of the Spark Connect logger hierarchy. It owns the handler that all Connect loggers
# write through, and its level is the default for every logger below it.
_CONNECT_LOGGER_NAME = "pyspark.sql.connect"

# Above CRITICAL, so nothing is emitted. Used instead of `Logger.disabled` because `disabled`
# is only honored on the logger a message is logged on, which would leave child loggers
# inheriting the standard library root logger's WARNING instead of staying silent.
_LOG_LEVEL_OFF = logging.CRITICAL + 1

_handler: Optional[logging.Handler] = None


def getLogger(name: Optional[str] = None) -> logging.Logger:
    """
    Return a logger in the Spark Connect logger hierarchy, creating it if necessary.

    Spark Connect logs through one logger per module, all rooted at ``pyspark.sql.connect``,
    so a single area can be turned on without enabling the rest:

    .. code-block:: python

        import logging

        logging.getLogger("pyspark.sql.connect.client.retries").setLevel(logging.DEBUG)

    The available loggers are ``pyspark.sql.connect`` and, below it, ``client.core``,
    ``client.retries``, ``client.reattach``, ``client.artifact``, ``dataframe``, ``plan``,
    and ``session``.

    Parameters
    ----------
    name : str, optional
        Name of the logger, either relative to ``pyspark.sql.connect`` or fully qualified.
        When omitted, the root Spark Connect logger is returned.

    .. versionadded:: 4.4.0
    """
    if not name or name == _CONNECT_LOGGER_NAME:
        qualified_name = _CONNECT_LOGGER_NAME
    elif name.startswith(_CONNECT_LOGGER_NAME + "."):
        qualified_name = name
    else:
        qualified_name = f"{_CONNECT_LOGGER_NAME}.{name}"

    # Deliberately a plain logger rather than PySparkLogger, which attaches a handler to every
    # instance it creates. Records propagate to the handler on the root Connect logger instead.
    return logging.getLogger(qualified_name)


def configureLogging(level: Optional[str] = None) -> logging.Logger:
    """
    Configure log level for Spark Connect components.
    When not specified as a parameter, log level will be configured based on
    the SPARK_CONNECT_LOG_LEVEL environment variable.
    When both are absent, logging is disabled.

    The level applies to the root Spark Connect logger, and therefore to every Connect logger
    that does not have a level of its own. See :func:`getLogger` for enabling a single logger.

    .. versionadded:: 4.0.0

    .. versionchanged:: 4.4.0
        Repeated calls no longer attach an additional handler, and a level set on the root
        Spark Connect logger before PySpark is imported is no longer overwritten.
    """
    global _handler

    logger = logging.getLogger(_CONNECT_LOGGER_NAME)

    if _handler is None:
        _handler = logging.StreamHandler()
        _handler.setFormatter(JSONFormatter())
    if _handler not in logger.handlers:
        logger.addHandler(_handler)

    if level is None:
        level = os.environ.get("SPARK_CONNECT_LOG_LEVEL")

    if level is not None:
        logger.setLevel(level.upper())
        logger.disabled = False
    elif logger.level == logging.NOTSET:
        logger.setLevel(_LOG_LEVEL_OFF)
    return logger


# Instantiate the root Spark Connect logger based on the environment configuration. Kept as a
# module-level name for backwards compatibility, new code should use getLogger(__name__).
logger = configureLogging()


def getLogLevel() -> Optional[int]:
    """
    This returns this log level as integer, or none (if no logging is enabled).

    Spark Connect logging can be configured with environment variable 'SPARK_CONNECT_LOG_LEVEL'

    .. versionadded:: 3.5.0
    """

    if not logger.disabled and logger.level < _LOG_LEVEL_OFF:
        return logger.level
    return None
