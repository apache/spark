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

from pyspark.sql.tests.test_python_worker_env import NUL, PREFIX, PythonWorkerEnvMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class PythonWorkerEnvParityTests(PythonWorkerEnvMixin, ReusedConnectTestCase):
    # A Spark Connect client sets a configuration through the config RPC, which is the one write
    # path that can refuse an invalid environment before it is stored. Classic has no such point,
    # so these two are specific to Connect rather than part of the shared mixin.

    def test_a_secret_looking_name_cannot_be_read_back(self):
        # The config RPC withholds any value whose key matches spark.redaction.regex, and the
        # default pattern covers "token". So a variable named like a secret still reaches the
        # worker but reads back as absent -- unlike classic, which returns it. Pinned here so the
        # asymmetry is a recorded decision rather than a surprise.
        self.spark.conf.set(PREFIX + "API_TOKEN", "abc")
        try:
            self.assertIsNone(self.spark.conf.get(PREFIX + "API_TOKEN", None))
            self.assertEqual(self._read_in_worker("API_TOKEN"), "abc")
        finally:
            self.spark.conf.unset(PREFIX + "API_TOKEN")

    def test_invalid_name_fails_the_set(self):
        with self.assertRaises(Exception) as context:
            self.spark.conf.set(PREFIX + "1INVALID", "x")
        message = str(context.exception)
        # Assert on the message text rather than the condition name, which the client is not
        # required to surface in the string form of the exception.
        self.assertIn("is not valid", message)
        self.assertIn("1INVALID", message)
        # The write was refused, so nothing was stored to break later queries.
        self.assertIsNone(self.spark.conf.get(PREFIX + "1INVALID", None))

    def test_value_containing_nul_fails_the_set(self):
        with self.assertRaises(Exception) as context:
            self.spark.conf.set(PREFIX + "WITH_NUL", "abc" + NUL + "def")
        message = str(context.exception)
        self.assertIn("NUL character", message)
        self.assertIn("WITH_NUL", message)
        # A value can be a secret, so it must not appear in the failure.
        self.assertNotIn("abc", message)
        self.assertIsNone(self.spark.conf.get(PREFIX + "WITH_NUL", None))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
