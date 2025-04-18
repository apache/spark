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

from pyspark.sql.tests.test_functions import FunctionsTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class FunctionsParityTests(FunctionsTestsMixin, ReusedConnectTestCase):
    @unittest.skip("Spark Connect does not support Spark Context but the test depends on that.")
    def test_basic_functions(self):
        super().test_basic_functions()

    @unittest.skip("Spark Connect does not support Spark Context but the test depends on that.")
    def test_function_parity(self):
        super().test_function_parity()

    @unittest.skip("Spark Connect does not support Spark Context but the test depends on that.")
    def test_input_file_name_reset_for_rdd(self):
        super().test_input_file_name_reset_for_rdd()

    @unittest.skip("No need to test in Spark Connect.")
    def test_wildcard_import(self):
        super().test_wildcard_import()


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
