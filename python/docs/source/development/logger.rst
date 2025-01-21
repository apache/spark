..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

==================
Logging in PySpark
==================

.. currentmodule:: pyspark.logger

Introduction
============

The :ref:`pyspark.logger</reference/pyspark.logger.rst>` module facilitates structured client-side logging for PySpark users.

This module includes a :class:`PySparkLogger` class that provides several methods for logging messages at different levels in a structured JSON format:

- :meth:`PySparkLogger.info`
- :meth:`PySparkLogger.warning`
- :meth:`PySparkLogger.error`
- :meth:`PySparkLogger.exception`

The logger can be easily configured to write logs to either the console or a specified file.

Customizing Log Format
======================
The default log format is JSON, which includes the timestamp, log level, logger name, and the log message along with any additional context provided.

Example log entry:

.. code-block:: python

    {
      "ts": "2024-06-28 19:53:48,563",
      "level": "ERROR",
      "logger": "DataFrameQueryContextLogger",
      "msg": "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. SQLSTATE: 22012\n== DataFrame ==\n\"divide\" was called from\n/.../spark/python/test_error_context.py:17\n",
      "context": {
        "file": "/path/to/file.py",
        "line": "17",
        "fragment": "divide"
        "errorClass": "DIVIDE_BY_ZERO"
      },
      "exception": {
        "class": "Py4JJavaError",
        "msg": "An error occurred while calling o52.showString.\n: org.apache.spark.SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. SQLSTATE: 22012\n== DataFrame ==\n\"divide\" was called from\n/path/to/file.py:17 ...",
        "stacktrace": ["Traceback (most recent call last):", "  File \".../spark/python/pyspark/errors/exceptions/captured.py\", line 247, in deco", "    return f(*a, **kw)", "  File \".../lib/python3.9/site-packages/py4j/protocol.py\", line 326, in get_return_value" ...]
      },
    }

Setting Up
==========
To start using the PySpark logging module, you need to import the :class:`PySparkLogger` from the :ref:`pyspark.logger</reference/pyspark.logger.rst>`.

.. code-block:: python

    from pyspark.logger import PySparkLogger

Usage
=====
Creating a Logger
-----------------
You can create a logger instance by calling the :meth:`PySparkLogger.getLogger`. By default, it creates a logger named "PySparkLogger" with an INFO log level.

.. code-block:: python

    logger = PySparkLogger.getLogger()

Logging Messages
----------------
The logger provides three main methods for log messages: :meth:`PySparkLogger.info`, :meth:`PySparkLogger.warning` and :meth:`PySparkLogger.error`.

- **PySparkLogger.info**: Use this method to log informational messages.
  
  .. code-block:: python

      user = "test_user"
      action = "login"
      logger.info(f"User {user} performed {action}", user=user, action=action)

- **PySparkLogger.warning**: Use this method to log warning messages.
  
  .. code-block:: python

      user = "test_user"
      action = "access"
      logger.warning("User {user} attempted an unauthorized {action}", user=user, action=action)

- **PySparkLogger.error**: Use this method to log error messages.
  
  .. code-block:: python

      user = "test_user"
      action = "update_profile"
      logger.error("An error occurred for user {user} during {action}", user=user, action=action)

Logging to Console
------------------

.. code-block:: python

    from pyspark.logger import PySparkLogger

    # Create a logger that logs to console
    logger = PySparkLogger.getLogger("ConsoleLogger")

    user = "test_user"
    action = "test_action"

    logger.warning(f"User {user} takes an {action}", user=user, action=action)

This logs an information in the following JSON format:

.. code-block:: python

    {
      "ts": "2024-06-28 19:44:19,030",
      "level": "WARNING",
      "logger": "ConsoleLogger",
      "msg": "User test_user takes an test_action",
      "context": {
        "user": "test_user",
        "action": "test_action"
      },
    }

Logging to a File
-----------------

To log messages to a file, use the :meth:`PySparkLogger.addHandler` for adding `FileHandler` from the standard Python logging module to your logger.

This approach aligns with the standard Python logging practices.

.. code-block:: python

    from pyspark.logger import PySparkLogger
    import logging

    # Create a logger that logs to a file
    file_logger = PySparkLogger.getLogger("FileLogger")
    handler = logging.FileHandler("application.log")
    file_logger.addHandler(handler)

    user = "test_user"
    action = "test_action"

    file_logger.warning(f"User {user} takes an {action}", user=user, action=action)

The log messages will be saved in `application.log` in the same JSON format.
