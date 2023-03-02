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

from pyspark.sql.connect.readwriter import DataFrameWriterV2
from pyspark.sql.tests.test_readwriter import ReadwriterTestsMixin, ReadwriterV2TestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class ReadwriterParityTests(ReadwriterTestsMixin, ReusedConnectTestCase):
    pass


class ReadwriterV2ParityTests(ReadwriterV2TestsMixin, ReusedConnectTestCase):
    def test_api(self):
        self.check_api(DataFrameWriterV2)

    def test_partitioning_functions(self):
        self.check_partitioning_functions(DataFrameWriterV2)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_readwriter import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
