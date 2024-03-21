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

from pyspark.sql.tests.test_collection import DataFrameCollectionTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class DataFrameCollectionParityTests(
    DataFrameCollectionTestsMixin,
    ReusedConnectTestCase,
):
    def test_to_local_iterator_not_fully_consumed(self):
        self.check_to_local_iterator_not_fully_consumed()

    def test_to_pandas_for_array_of_struct(self):
        # Spark Connect's implementation is based on Arrow.
        super().check_to_pandas_for_array_of_struct(True)

    def test_to_pandas_from_null_dataframe(self):
        self.check_to_pandas_from_null_dataframe()

    def test_to_pandas_on_cross_join(self):
        self.check_to_pandas_on_cross_join()

    def test_to_pandas_from_empty_dataframe(self):
        self.check_to_pandas_from_empty_dataframe()

    def test_to_pandas_with_duplicated_column_names(self):
        self.check_to_pandas_with_duplicated_column_names()

    def test_to_pandas_from_mixed_dataframe(self):
        self.check_to_pandas_from_mixed_dataframe()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_collection import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
