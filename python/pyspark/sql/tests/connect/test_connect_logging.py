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

import contextlib
import io
import logging
import os
import unittest
from unittest import mock

from pyspark.testing.connectutils import (
    connect_requirement_message,
    should_test_connect,
)

if should_test_connect:
    from pyspark.sql.connect import logging as connect_logging

CONNECT_ROOT = "pyspark.sql.connect"

MODULE_LOGGERS = [
    "pyspark.sql.connect.client.core",
    "pyspark.sql.connect.client.retries",
    "pyspark.sql.connect.client.reattach",
    "pyspark.sql.connect.client.artifact",
    "pyspark.sql.connect.dataframe",
    "pyspark.sql.connect.plan",
    "pyspark.sql.connect.session",
]


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ConnectLoggingTests(unittest.TestCase):
    def setUp(self):
        self._saved = {}
        for logger in [logging.getLogger()] + [
            logging.getLogger(name) for name in [CONNECT_ROOT] + MODULE_LOGGERS
        ]:
            self._saved[logger.name] = (logger.level, logger.disabled, list(logger.handlers))

        # Detach the handler owned by the module so that enabling a level in a test does not
        # write to stderr. configureLogging reattaches it, which test_single_handler relies on.
        logging.getLogger(CONNECT_ROOT).removeHandler(connect_logging._handler)

    def tearDown(self):
        for name, (level, disabled, handlers) in self._saved.items():
            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.disabled = disabled
            logger.handlers[:] = handlers
        logging.Logger.manager._clear_cache()

    @contextlib.contextmanager
    def env_log_level(self, value=None):
        env = {} if value is None else {"SPARK_CONNECT_LOG_LEVEL": value}
        with mock.patch.dict(os.environ, env, clear=True):
            yield

    @contextlib.contextmanager
    def capture(self, *names):
        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setLevel(logging.NOTSET)
        loggers = [logging.getLogger(name) for name in names]
        for logger in loggers:
            logger.addHandler(handler)
        # Keep captured records off stderr, in case a preceding configureLogging reattached it.
        logging.getLogger(CONNECT_ROOT).removeHandler(connect_logging._handler)
        try:
            yield buffer
        finally:
            for logger in loggers:
                logger.removeHandler(handler)

    def configure_default(self):
        """Reconfigure as if PySpark were freshly imported with no logging configured."""
        for name in [CONNECT_ROOT] + MODULE_LOGGERS:
            logging.getLogger(name).setLevel(logging.NOTSET)
        logging.Logger.manager._clear_cache()
        with self.env_log_level():
            connect_logging.configureLogging()

    def test_get_logger_resolves_names(self):
        self.assertEqual(connect_logging.getLogger().name, CONNECT_ROOT)
        self.assertEqual(
            connect_logging.getLogger("client.retries").name,
            "pyspark.sql.connect.client.retries",
        )
        self.assertEqual(
            connect_logging.getLogger("pyspark.sql.connect.client.retries").name,
            "pyspark.sql.connect.client.retries",
        )

    def test_default_is_silent(self):
        self.configure_default()
        for name in MODULE_LOGGERS:
            logger = logging.getLogger(name)
            self.assertGreater(logger.getEffectiveLevel(), logging.CRITICAL, name)
            self.assertFalse(logger.isEnabledFor(logging.WARNING), name)

    def test_no_warning_leak_to_root(self):
        # A disabled parent does not silence its children, so the off switch has to be a level.
        # Without it, children would inherit the standard library root logger's WARNING.
        self.configure_default()
        with self.capture(CONNECT_ROOT, "") as buffer:
            logging.getLogger("pyspark.sql.connect.client.retries").warning("leaked")
        self.assertEqual(buffer.getvalue(), "")

    def test_single_handler(self):
        for _ in range(3):
            with self.env_log_level():
                connect_logging.configureLogging()
        handlers = logging.getLogger(CONNECT_ROOT).handlers
        self.assertEqual(handlers.count(connect_logging._handler), 1)

    def test_env_var_sets_root_level(self):
        with self.env_log_level("debug"):
            connect_logging.configureLogging()
        self.assertEqual(logging.getLogger(CONNECT_ROOT).level, logging.DEBUG)
        for name in MODULE_LOGGERS:
            self.assertTrue(logging.getLogger(name).isEnabledFor(logging.DEBUG), name)

    def test_configure_logging_argument_re_enables(self):
        self.configure_default()
        self.assertIsNone(connect_logging.getLogLevel())

        with self.env_log_level():
            connect_logging.configureLogging("warn")
        self.assertEqual(connect_logging.getLogLevel(), logging.WARNING)

    def test_child_enabled_independently(self):
        self.configure_default()
        logging.getLogger("pyspark.sql.connect.client.retries").setLevel(logging.DEBUG)

        with self.capture(CONNECT_ROOT) as buffer:
            logging.getLogger("pyspark.sql.connect.client.retries").debug("retry detail")
            logging.getLogger("pyspark.sql.connect.client.core").debug("proto dump")

        self.assertIn("retry detail", buffer.getvalue())
        self.assertNotIn("proto dump", buffer.getvalue())

    def test_get_log_level(self):
        self.configure_default()
        self.assertIsNone(connect_logging.getLogLevel())

        with self.env_log_level("debug"):
            connect_logging.configureLogging()
        self.assertEqual(connect_logging.getLogLevel(), logging.DEBUG)

    def test_user_set_root_level_survives_configure(self):
        # A level set before PySpark is imported must not be overwritten by the default.
        logging.getLogger(CONNECT_ROOT).setLevel(logging.INFO)
        with self.env_log_level():
            connect_logging.configureLogging()
        self.assertEqual(logging.getLogger(CONNECT_ROOT).level, logging.INFO)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
