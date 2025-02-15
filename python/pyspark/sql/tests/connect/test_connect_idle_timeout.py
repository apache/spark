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

import time
import unittest

from pyspark.errors.exceptions.connect import SparkConnectException
from pyspark.testing.connectutils import (
    should_test_connect,
    connect_requirement_message,
    ReusedConnectTestCase,
)


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class IdleTimeoutTests(ReusedConnectTestCase):
    """
    This test class ensures that Spark Connect respects the configured idle timeout
    and shuts down after the specified period of inactivity.
    """

    def conf(self):
        conf = super().conf()
        conf["spark.connect.server.idleTimeout"] = "2000"
        return conf

    def test_idle_timeout_shutdown(self):
        result = self.spark.range(1).collect()
        self.assertEqual(len(result), 1)

        time.sleep(3)

        # Any call now should raise a SparkConnectException because there's no active session.
        with self.assertRaises(SparkConnectException) as e:
            self.spark.range(1).collect()
            self.check_error(
                exception=e.exception,
                errorClass="NO_ACTIVE_SESSION",
                messageParameters={},
            )


if __name__ == "__main__":
    import xmlrunner

    testRunner = None
    try:
        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        pass

    unittest.main(testRunner=testRunner, verbosity=2)
