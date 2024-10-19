# -*- encoding: utf-8 -*-
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
import json
from typing import cast, Optional

SPARK_LOG_SCHEMA = (
    "ts TIMESTAMP, "
    "level STRING, "
    "msg STRING, "
    "context map<STRING, STRING>, "
    "exception STRUCT<class STRING, msg STRING, "
    "stacktrace ARRAY<STRUCT<class STRING, method STRING, file STRING,line STRING>>>,"
    "logger STRING"
)


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for logging records.

    This formatter converts the log record to a JSON object with the following fields:
    - timestamp: The time the log record was created.
    - level: The log level of the record.
    - name: The name of the logger.
    - message: The log message.
    - kwargs: Any additional keyword arguments passed to the logger.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the specified record as a JSON string.

        Parameters
        ----------
        record : logging.LogRecord
            The log record to be formatted.

        Returns
        -------
        str
            The formatted log record as a JSON string.
        """
        log_entry = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "context": record.__dict__.get("kwargs", {}),
        }
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            log_entry["exception"] = {
                "class": exc_type.__name__ if exc_type else "UnknownException",
                "msg": str(exc_value),
                "stacktrace": self.formatException(record.exc_info).splitlines(),
            }
        return json.dumps(log_entry, ensure_ascii=False)


class PySparkLogger(logging.Logger):
    """
    Custom logging.Logger wrapper for PySpark that logs messages in a structured JSON format.

    PySparkLogger extends the standard Python logging.Logger class, allowing seamless integration
    with existing logging setups. It customizes the log output to JSON format, including additional
    context information, making it more useful for PySpark applications.

    .. versionadded:: 4.0.0

    Example
    -------
    >>> import logging
    >>> import json
    >>> from io import StringIO
    >>> from pyspark.logger import PySparkLogger

    >>> logger = PySparkLogger.getLogger("ExampleLogger")
    >>> logger.setLevel(logging.INFO)
    >>> stream = StringIO()
    >>> handler = logging.StreamHandler(stream)
    >>> logger.addHandler(handler)

    >>> logger.info(
    ...     "This is an informational message",
    ...     extra={"user": "test_user", "action": "test_action"}
    ... )
    >>> log_output = stream.getvalue().strip().split('\\n')[0]
    >>> log = json.loads(log_output)
    >>> _ = log.pop("ts")  # Remove the timestamp field for static testing

    >>> print(json.dumps(log, ensure_ascii=False, indent=2))
    {
      "level": "INFO",
      "logger": "ExampleLogger",
      "msg": "This is an informational message",
      "context": {
        "extra": {
          "user": "test_user",
          "action": "test_action"
        }
      }
    }
    """

    def __init__(self, name: str = "PySparkLogger"):
        super().__init__(name, level=logging.WARN)
        _handler = logging.StreamHandler()
        self.addHandler(_handler)

    def addHandler(self, handler: logging.Handler) -> None:
        """
        Add the specified handler to this logger in structured JSON format.
        """
        handler.setFormatter(JSONFormatter())
        super().addHandler(handler)

    @staticmethod
    def getLogger(name: Optional[str] = None) -> "PySparkLogger":
        """
        Return a PySparkLogger with the specified name, creating it if necessary.

        If no name is specified, return the logging.RootLogger.

        Parameters
        ----------
        name : str, optional
            The name of the logger.

        Returns
        -------
        PySparkLogger
            A configured instance of PySparkLogger.
        """
        existing_logger = logging.getLoggerClass()
        if not isinstance(existing_logger, PySparkLogger):
            logging.setLoggerClass(PySparkLogger)

        pyspark_logger = logging.getLogger(name)
        # Reset to the existing logger
        logging.setLoggerClass(existing_logger)

        return cast(PySparkLogger, pyspark_logger)

    def info(self, msg: object, *args: object, **kwargs: object) -> None:
        """
        Log 'msg % args' with severity 'INFO' in structured JSON format.

        Parameters
        ----------
        msg : str
            The log message.
        """
        super().info(msg, *args, extra={"kwargs": kwargs})

    def warning(self, msg: object, *args: object, **kwargs: object) -> None:
        """
        Log 'msg % args' with severity 'WARNING' in structured JSON format.

        Parameters
        ----------
        msg : str
            The log message.
        """
        super().warning(msg, *args, extra={"kwargs": kwargs})

    def error(self, msg: object, *args: object, **kwargs: object) -> None:
        """
        Log 'msg % args' with severity 'ERROR' in structured JSON format.

        Parameters
        ----------
        msg : str
            The log message.
        """
        super().error(msg, *args, extra={"kwargs": kwargs})

    def exception(self, msg: object, *args: object, **kwargs: object) -> None:
        """
        Convenience method for logging an ERROR with exception information.

        Parameters
        ----------
        msg : str
            The log message.
        exc_info : bool = True
            If True, exception information is added to the logging message.
            This includes the exception type, value, and traceback. Default is True.
        """
        super().error(msg, *args, exc_info=True, extra={"kwargs": kwargs})
