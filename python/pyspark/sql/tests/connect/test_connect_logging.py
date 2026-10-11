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
import unittest
from unittest.mock import patch

from pyspark.testing.connectutils import connect_requirement_message, should_test_connect

if should_test_connect:
    from pyspark.logger import PySparkLogger
    from pyspark.sql.connect import logging as connect_logging


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectLoggingTests(unittest.TestCase):
    def test_log_level_configuration(self):
        for level, env_level, expected in [
            (None, None, None),
            (None, "info", logging.INFO),
            ("debug", "error", logging.DEBUG),
            ("warning", None, logging.WARNING),
        ]:
            with self.subTest(level=level, env_level=env_level), patch.dict(os.environ):
                os.environ.pop("SPARK_CONNECT_LOG_LEVEL", None)
                if env_level is not None:
                    os.environ["SPARK_CONNECT_LOG_LEVEL"] = env_level
                logger = PySparkLogger("connect_logging_test")
                try:
                    with patch.object(PySparkLogger, "getLogger", return_value=logger):
                        self.assertIs(connect_logging.configureLogging(level), logger)
                    self.assertEqual(logger.disabled, expected is None)
                    if expected is not None:
                        self.assertEqual(logger.level, expected)
                    with patch.object(connect_logging, "logger", logger):
                        self.assertEqual(connect_logging.getLogLevel(), expected)
                finally:
                    for handler in logger.handlers[:]:
                        logger.removeHandler(handler)
                        handler.close()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
