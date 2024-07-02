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

import unittest
import json
from io import StringIO
from pyspark.logger.logger import PySparkLogger


class LoggerTest(unittest.TestCase):
    def setUp(self):
        self.logger = PySparkLogger.get_logger("TestLogger", stream=StringIO())

    def test_log_info(self):
        self.logger.log_info(
            "This is an info log", user="test_user_info", action="test_action_info"
        )
        log_json = json.loads(self.logger.handler.stream.getvalue().strip())

        self.assertEqual(log_json["message"], "This is an info log")
        self.assertEqual(log_json["user"], "test_user_info")
        self.assertEqual(log_json["action"], "test_action_info")

    def test_log_warn(self):
        self.logger.log_warn(
            "This is an warn log", user="test_user_warn", action="test_action_warn"
        )
        log_json = json.loads(self.logger.handler.stream.getvalue().strip())

        self.assertEqual(log_json["message"], "This is an warn log")
        self.assertEqual(log_json["user"], "test_user_warn")
        self.assertEqual(log_json["action"], "test_action_warn")

    def test_log_error(self):
        self.logger.log_error(
            "This is an error log", user="test_user_error", action="test_action_error"
        )
        log_json = json.loads(self.logger.handler.stream.getvalue().strip())

        self.assertEqual(log_json["message"], "This is an error log")
        self.assertEqual(log_json["user"], "test_user_error")
        self.assertEqual(log_json["action"], "test_action_error")


if __name__ == "__main__":
    import unittest
    from pyspark.logger.tests.test_logger import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
