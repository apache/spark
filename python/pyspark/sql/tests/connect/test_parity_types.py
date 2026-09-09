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

import datetime
import unittest

from pyspark.sql.tests.test_types import TypesTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class TypesParityTests(TypesTestsMixin, ReusedConnectTestCase):
    # SPARK-57462 follow-up: the nanosecond timestamp value path now works over Spark Connect,
    # whose data path goes through Arrow (to_arrow_type / ArrowTableToRowsConversion). The
    # data-path tests -- test_timestamp_nanos_type, test_timestamp_nanos_type_preview_flag_off and
    # test_timestamp_nanos_type_arrow_conversion -- are therefore inherited and run here. The
    # nanosecond tests below exercise classic-only mechanisms that do not apply to Connect: the
    # Py4J (useArrow=False) UDF path and the classic collect map-key guard in classic/dataframe.py.
    @unittest.skip("SPARK-57462: uses the classic Py4J (useArrow=False) UDF path, not Connect.")
    def test_timestamp_nanos_type_python_udf(self):
        super().test_timestamp_nanos_type_python_udf()

    @unittest.skip("SPARK-57462: the collect map-key guard is classic-only (classic/dataframe.py).")
    def test_timestamp_nanos_type_map_key_collision(self):
        super().test_timestamp_nanos_type_map_key_collision()

    @unittest.skip("SPARK-57462: uses the classic Py4J (useArrow=False) UDF input path, not Connect.")
    def test_timestamp_nanos_type_python_udf_input(self):
        super().test_timestamp_nanos_type_python_udf_input()

    @unittest.skip("SPARK-57462: uses the classic Py4J (useArrow=False) UDF input path, not Connect.")
    def test_timestamp_nanos_type_map_key_python_udf_input(self):
        super().test_timestamp_nanos_type_map_key_python_udf_input()

    def test_timestamp_nanos_type_connect_data_path(self):
        # SPARK-57462 follow-up: the Spark Connect data path goes through Arrow, so collecting a
        # nanosecond value now succeeds. collect() yields microsecond-resolution datetime.datetime
        # (the Python boundary), while toPandas keeps full nanosecond precision (datetime64[ns]).
        import pandas as pd

        with self.sql_conf({"spark.sql.timestampNanosTypes.enabled": True}):
            df = self.spark.sql(
                "SELECT CAST('2020-01-02 03:04:05.123456789' AS TIMESTAMP_NTZ(9)) AS ts"
            )
            self.assertEqual(
                datetime.datetime(2020, 1, 2, 3, 4, 5, 123456), df.collect()[0].ts
            )
            self.assertEqual(
                pd.Timestamp("2020-01-02 03:04:05.123456789"), df.toPandas()["ts"][0]
            )

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_apply_schema(self):
        super().test_apply_schema()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_apply_schema_to_dict_and_rows(self):
        super().test_apply_schema_to_dict_and_rows()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_apply_schema_to_row(self):
        super().test_apply_schema_to_row()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_geospatial_create_dataframe_rdd(self):
        super().test_geospatial_create_dataframe_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_create_dataframe_schema_mismatch(self):
        super().test_create_dataframe_schema_mismatch()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_array_element_type_empty_rdd(self):
        super().test_infer_array_element_type_empty_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_array_merge_element_types_with_rdd(self):
        super().test_infer_array_merge_element_types_with_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_map_pair_type_empty_rdd(self):
        super().test_infer_map_pair_type_empty_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_map_merge_pair_types_with_rdd(self):
        super().test_infer_map_merge_pair_types_with_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_binary_type(self):
        super().test_infer_binary_type()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_long_type(self):
        super().test_infer_long_type()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_nested_dict_as_struct_with_rdd(self):
        super().test_infer_nested_dict_as_struct_with_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_nested_schema(self):
        super().test_infer_nested_schema()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema(self):
        super().test_infer_schema()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema_to_local(self):
        super().test_infer_schema_to_local()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema_upcast_int_to_string(self):
        super().test_infer_schema_upcast_int_to_string()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_rdd_with_udt(self):
        super().test_rdd_with_udt()

    @unittest.skip("Requires JVM access.")
    def test_udt(self):
        super().test_udt()

    @unittest.skip("Requires JVM access.")
    def test_schema_with_collations_json_ser_de(self):
        super().test_schema_with_collations_json_ser_de()

    @unittest.skip(
        "The inherited Classic contract also asserts that PYSPARK_YM_INTERVAL_LEGACY=1 returns "
        "the integer months (Row(interval=128)), which Spark Connect cannot satisfy: PyArrow has "
        "no INTERVAL_MONTHS array support, so the legacy flag is not honored and collect raises "
        "NOT_IMPLEMENTED regardless. The default-raise behavior Connect does match is covered by "
        "test_connect_error.SparkConnectErrorTests.test_ym_interval_in_collect."
    )
    def test_ym_interval_in_collect(self):
        super().test_ym_interval_in_collect()

    @unittest.skip("This test is dedicated for PySpark Classic.")
    def test_cal_interval_in_collect(self):
        super().test_cal_interval_in_collect()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
