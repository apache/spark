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

from pyspark.sql.tests.pandas.test_pandas_udf_grouped_agg import GroupedAggPandasUDFTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class PandasUDFGroupedAggParityTests(GroupedAggPandasUDFTestsMixin, ReusedConnectTestCase):
    # TODO(SPARK-43727): Parity returnType check in Spark Connect
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_unsupported_types(self):
        super().test_unsupported_types()

    @unittest.skip("Spark Connect does not support convert UNPARSED to catalyst types.")
    def test_manual(self):
        super().test_manual()


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_pandas_udf_grouped_agg import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
