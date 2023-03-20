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

from pyspark.sql.tests.pandas.test_pandas_grouped_map import GroupedApplyInPandasTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class GroupedApplyInPandasTests(GroupedApplyInPandasTestsMixin, ReusedConnectTestCase):
    # TODO(SPARK-42822): Fix ambiguous reference for case-insensitive grouping column
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_case_insensitive_grouping_column(self):
        super().test_case_insensitive_grouping_column()

    # TODO(SPARK-42857): Support CreateDataFrame from Decimal128
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_supported_types(self):
        super().test_supported_types()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_wrong_return_type(self):
        super().test_wrong_return_type()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_wrong_args(self):
        super().test_wrong_args()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_unsupported_types(self):
        super().test_unsupported_types()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_register_grouped_map_udf(self):
        super().test_register_grouped_map_udf()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_column_order(self):
        super().test_column_order()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_returning_no_column_names_and_wrong_amount(self):
        super().test_apply_in_pandas_returning_no_column_names_and_wrong_amount()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_not_returning_pandas_dataframe(self):
        super().test_apply_in_pandas_not_returning_pandas_dataframe()

    @unittest.skip("Spark Connect doesn't support RDD but the test depends on it.")
    def test_grouped_with_empty_partition(self):
        super().test_grouped_with_empty_partition()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_returning_empty_dataframe_and_wrong_number_of_columns(self):
        super().test_apply_in_pandas_returning_empty_dataframe_and_wrong_number_of_columns()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_returning_wrong_number_of_columns(self):
        super().test_apply_in_pandas_returning_wrong_number_of_columns()


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_pandas_grouped_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
