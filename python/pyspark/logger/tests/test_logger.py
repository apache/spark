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
import unittest
import json
from io import StringIO
from pyspark.errors import ArithmeticException
from pyspark.logger.logger import PySparkLogger
from pyspark.testing.sqlutils import ReusedSQLTestCase


class LoggerTestsMixin:
    def setUp(self):
        self.handler = logging.StreamHandler(StringIO())

        self.logger = PySparkLogger.getLogger("TestLogger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.handler)

        dataframe_query_context_logger = PySparkLogger.getLogger("DataFrameQueryContextLogger")
        dataframe_query_context_logger.setLevel(logging.INFO)
        dataframe_query_context_logger.addHandler(self.handler)

    def test_log_structure(self):
        self.logger.info("Test logging structure")
        log_json = json.loads(self.handler.stream.getvalue().strip())
        keys = ["ts", "level", "logger", "msg", "context"]
        for key in keys:
            self.assertTrue(key in log_json)

    def test_log_info(self):
        self.logger.info("This is an info log", user="test_user_info", action="test_action_info")
        log_json = json.loads(self.handler.stream.getvalue().strip())

        self.assertEqual(log_json["msg"], "This is an info log")
        self.assertEqual(
            log_json["context"], {"action": "test_action_info", "user": "test_user_info"}
        )
        self.assertTrue("exception" not in log_json)

    def test_log_warn(self):
        self.logger.warn("This is an warn log", user="test_user_warn", action="test_action_warn")
        log_json = json.loads(self.handler.stream.getvalue().strip())

        self.assertEqual(log_json["msg"], "This is an warn log")
        self.assertEqual(
            log_json["context"], {"action": "test_action_warn", "user": "test_user_warn"}
        )
        self.assertTrue("exception" not in log_json)

    def test_log_error(self):
        self.logger.error(
            "This is an error log", user="test_user_error", action="test_action_error"
        )
        log_json = json.loads(self.handler.stream.getvalue().strip())

        self.assertEqual(log_json["msg"], "This is an error log")
        self.assertEqual(
            log_json["context"], {"action": "test_action_error", "user": "test_user_error"}
        )
        self.assertTrue("exception" not in log_json)

    def test_log_exception(self):
        self.logger.exception(
            "This is an exception log", user="test_user_exception", action="test_action_exception"
        )
        log_json = json.loads(self.handler.stream.getvalue().strip())

        self.assertEqual(log_json["msg"], "This is an exception log")
        self.assertEqual(
            log_json["context"], {"action": "test_action_exception", "user": "test_user_exception"}
        )
        self.assertTrue("exception" in log_json)
        self.assertTrue("class" in log_json["exception"])
        self.assertTrue("msg" in log_json["exception"])
        self.assertTrue("stacktrace" in log_json["exception"])

    def test_dataframe_query_context_logger(self):
        with self.sql_conf({"spark.sql.ansi.enabled": True}):
            df = self.spark.range(10)

            with self.assertRaises(ArithmeticException):
                df.withColumn("div_zero", df.id / 0).collect()
            log_json = json.loads(self.handler.stream.getvalue().strip())

            err_msg = "[DIVIDE_BY_ZERO] Division by zero."
            self.assertTrue(err_msg in log_json["msg"])
            self.assertTrue(err_msg in log_json["exception"]["msg"])
            self.assertEqual(log_json["context"]["fragment"], "__truediv__")
            self.assertEqual(log_json["context"]["errorClass"], "DIVIDE_BY_ZERO")
            # Only the class name is different between classic and connect.
            # Py4JJavaError for classic, _MultiThreadedRendezvous for connect
            self.assertTrue(
                log_json["exception"]["class"] in ("Py4JJavaError", "_MultiThreadedRendezvous")
            )
            self.assertTrue("Traceback" in log_json["exception"]["stacktrace"][0])


class LoggerTests(LoggerTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.logger.tests.test_logger import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
