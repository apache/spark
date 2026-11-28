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
    @property
    def sc(self):
        return self.spark.sparkContext

    # SPARK-57462 follow-up: the nanosecond timestamp value path now works over Spark Connect,
    # whose data path goes through Arrow (to_arrow_type / ArrowTableToRowsConversion). The
    # data-path tests -- and the collect map-key collision guard, now mirrored onto the Connect
    # DataFrame -- are therefore inherited and run here. The nanosecond tests still skipped below
    # exercise the classic-only Py4J (useArrow=False) UDF path, which does not apply to Connect.
    @unittest.skip("SPARK-57462: uses the classic Py4J (useArrow=False) UDF path, not Connect.")
    def test_timestamp_nanos_type_python_udf(self):
        super().test_timestamp_nanos_type_python_udf()

    @unittest.skip("SPARK-57462: classic-only Py4J UDF input path (useArrow=False), not Connect.")
    def test_timestamp_nanos_type_python_udf_input(self):
        super().test_timestamp_nanos_type_python_udf_input()

    @unittest.skip("SPARK-57462: classic-only Py4J map-key UDF input path, not Connect.")
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
            self.assertEqual(datetime.datetime(2020, 1, 2, 3, 4, 5, 123456), df.collect()[0].ts)
            self.assertEqual(pd.Timestamp("2020-01-02 03:04:05.123456789"), df.toPandas()["ts"][0])

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
