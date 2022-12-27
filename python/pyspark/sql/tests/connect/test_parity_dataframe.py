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
import os

from pyspark.sql import SparkSession
from pyspark.sql.tests.test_dataframe import DataFrameTestsMixin
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.sqlutils import ReusedSQLTestCase


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class DataFrameParityTests(DataFrameTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(DataFrameParityTests, cls).setUpClass()
        cls._spark = cls.spark  # Assign existing Spark session to run the server
        # Sets the remote address. Now, we create a remote Spark Session.
        # Note that this is only allowed in testing.
        os.environ["SPARK_REMOTE"] = "sc://localhost"
        cls.spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # TODO(SPARK-41529): Implement stop in RemoteSparkSession.
        #  Stop the regular Spark session (server) too.
        cls.spark = cls._spark
        super(DataFrameParityTests, cls).tearDownClass()
        del os.environ["SPARK_REMOTE"]

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cache(self):
        super().test_cache()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_create_dataframe_from_array_of_long(self):
        super().test_create_dataframe_from_array_of_long()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_create_dataframe_from_pandas_with_day_time_interval(self):
        super().test_create_dataframe_from_pandas_with_day_time_interval()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_create_dataframe_from_pandas_with_dst(self):
        super().test_create_dataframe_from_pandas_with_dst()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_create_dataframe_from_pandas_with_timestamp(self):
        super().test_create_dataframe_from_pandas_with_timestamp()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_create_dataframe_required_pandas_not_found(self):
        super().test_create_dataframe_required_pandas_not_found()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_create_nan_decimal_dataframe(self):
        super().test_create_nan_decimal_dataframe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_drop_duplicates(self):
        super().test_drop_duplicates()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_duplicated_column_names(self):
        super().test_duplicated_column_names()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_extended_hint_types(self):
        super().test_extended_hint_types()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_fillna(self):
        super().test_fillna()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_generic_hints(self):
        super().test_generic_hints()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_help_command(self):
        super().test_help_command()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_input_files(self):
        super().test_input_files()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_invalid_join_method(self):
        super().test_invalid_join_method()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_join_without_on(self):
        super().test_join_without_on()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_observe(self):
        super().test_observe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_observe_str(self):
        super().test_observe_str()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_pandas_api(self):
        super().test_pandas_api()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_repartitionByRange_dataframe(self):
        super().test_repartitionByRange_dataframe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_replace(self):
        super().test_replace()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_repr_behaviors(self):
        super().test_repr_behaviors()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_require_cross(self):
        super().test_require_cross()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_same_semantics_error(self):
        super().test_same_semantics_error()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_sample(self):
        super().test_sample()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to(self):
        super().test_to()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_local_iterator(self):
        super().test_to_local_iterator()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_local_iterator_not_fully_consumed(self):
        super().test_to_local_iterator_not_fully_consumed()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_local_iterator_prefetch(self):
        super().test_to_local_iterator_prefetch()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas(self):
        super().test_to_pandas()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_avoid_astype(self):
        super().test_to_pandas_avoid_astype()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_for_array_of_struct(self):
        super().test_to_pandas_for_array_of_struct()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_from_empty_dataframe(self):
        super().test_to_pandas_from_empty_dataframe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_from_mixed_dataframe(self):
        super().test_to_pandas_from_mixed_dataframe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_from_null_dataframe(self):
        super().test_to_pandas_from_null_dataframe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_on_cross_join(self):
        super().test_to_pandas_on_cross_join()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_required_pandas_not_found(self):
        super().test_to_pandas_required_pandas_not_found()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_to_pandas_with_duplicated_column_names(self):
        super().test_to_pandas_with_duplicated_column_names()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_unpivot(self):
        super().test_unpivot()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_dataframe import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
