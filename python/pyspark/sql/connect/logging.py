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
from pyspark.logger import PySparkLogger
import os
from typing import Optional

__all__ = ["configureLogging", "getLogLevel"]


def configureLogging(level: Optional[str] = None) -> logging.Logger:
    """
    Configure log level for Spark Connect components.
    When not specified as a parameter, log level will be configured based on
    the SPARK_CONNECT_LOG_LEVEL environment variable.
    When both are absent, logging is disabled.

    .. versionadded:: 4.0.0
    """
    logger = PySparkLogger.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s %(process)d %(levelname)s %(funcName)s %(message)s")
    )
    logger.addHandler(handler)

    if level is not None:
        logger.setLevel(level.upper())
    elif "SPARK_CONNECT_LOG_LEVEL" in os.environ:
        logger.setLevel(os.environ["SPARK_CONNECT_LOG_LEVEL"].upper())
    else:
        logger.disabled = True
    return logger


# Instantiate the logger based on the environment configuration.
logger = configureLogging()


def getLogLevel() -> Optional[int]:
    """
    This returns this log level as integer, or none (if no logging is enabled).

    Spark Connect logging can be configured with environment variable 'SPARK_CONNECT_LOG_LEVEL'

    .. versionadded:: 3.5.0
    """

    if not logger.disabled:
        return logger.level
    return None
