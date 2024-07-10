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

import io
import logging
import json
from typing import Any, Optional, Union


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
        return json.dumps(log_entry, ensure_ascii=False)


class PySparkLogger(logging.Logger):
    """
    Custom logger for PySpark that logs messages in a structured JSON format.

    This logger provides methods for logging messages at different levels (info, warning, error)
    with additional keyword arguments that are included in the JSON log entry.
    """

    def __init__(
        self,
        name: str = "PySparkLogger",
        level: int = logging.INFO,
        stream: Optional[Union[None, io.TextIOWrapper]] = None,
        filename: Optional[str] = None,
    ):
        super().__init__(name, level)
        if not self.hasHandlers():
            self._handler = (
                logging.FileHandler(filename) if filename else logging.StreamHandler(stream)
            )
            self._handler.setFormatter(JSONFormatter())
            self.addHandler(self._handler)
            self.setLevel(level)

    @staticmethod
    def getLogger(
        name: str = "PySparkLogger",
        level: int = logging.INFO,
        stream: Optional[Union[None, io.TextIOWrapper]] = None,
        filename: Optional[str] = None,
    ) -> "PySparkLogger":
        """
        Get a configured instance of PySparkLogger.

        Parameters
        ----------
        name : str, optional
            The name of the logger (default is "PySparkLogger").
        level : int, optional
            The log level of the logger (default is logging.INFO).
        stream : Optional[Union[None, io.TextIOWrapper]], optional
            The stream to which log messages are written (default is None, which uses sys.stderr).
        filename : Optional[str], optional
            The name of the file to which log messages are written (default is None).

        Returns
        -------
        PySparkLogger
            A configured instance of PySparkLogger.
        """
        return PySparkLogger(name, level, stream, filename)

    def info(self, message: str, **kwargs: Any) -> None:
        """
        Log an info message with additional keyword arguments.

        Parameters
        ----------
        message : str
            The log message.
        **kwargs
            Additional keyword arguments to include in the log entry.
        """
        super().info(message, exc_info=False, extra={"kwargs": kwargs})

    def warn(self, message: str, **kwargs: Any) -> None:
        """
        Log a warning message with additional keyword arguments.

        Parameters
        ----------
        message : str
            The log message.
        **kwargs
            Additional keyword arguments to include in the log entry.
        """
        super().warning(message, exc_info=False, extra={"kwargs": kwargs})

    def error(self, message: str, **kwargs: Any) -> None:
        """
        Log an error message with additional keyword arguments.

        Parameters
        ----------
        message : str
            The log message.
        **kwargs
            Additional keyword arguments to include in the log entry.
        """
        super().error(message, exc_info=False, extra={"kwargs": kwargs})
