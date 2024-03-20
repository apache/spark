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

from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    FloatType,
    DayTimeIntervalType,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    have_pandas,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import QuietTest


class DataFrameCollectionTestsMixin:
    def _to_pandas(self):
        from datetime import datetime, date, timedelta

        schema = (
            StructType()
            .add("a", IntegerType())
            .add("b", StringType())
            .add("c", BooleanType())
            .add("d", FloatType())
            .add("dt", DateType())
            .add("ts", TimestampType())
            .add("ts_ntz", TimestampNTZType())
            .add("dt_interval", DayTimeIntervalType())
        )
        data = [
            (
                1,
                "foo",
                True,
                3.0,
                date(1969, 1, 1),
                datetime(1969, 1, 1, 1, 1, 1),
                datetime(1969, 1, 1, 1, 1, 1),
                timedelta(days=1),
            ),
            (2, "foo", True, 5.0, None, None, None, None),
            (
                3,
                "bar",
                False,
                -1.0,
                date(2012, 3, 3),
                datetime(2012, 3, 3, 3, 3, 3),
                datetime(2012, 3, 3, 3, 3, 3),
                timedelta(hours=-1, milliseconds=421),
            ),
            (
                4,
                "bar",
                False,
                6.0,
                date(2100, 4, 4),
                datetime(2100, 4, 4, 4, 4, 4),
                datetime(2100, 4, 4, 4, 4, 4),
                timedelta(microseconds=123),
            ),
        ]
        df = self.spark.createDataFrame(data, schema)
        return df.toPandas()

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas(self):
        import numpy as np

        pdf = self._to_pandas()
        types = pdf.dtypes
        self.assertEqual(types[0], np.int32)
        self.assertEqual(types[1], object)
        self.assertEqual(types[2], bool)
        self.assertEqual(types[3], np.float32)
        self.assertEqual(types[4], object)  # datetime.date
        self.assertEqual(types[5], "datetime64[ns]")
        self.assertEqual(types[6], "datetime64[ns]")
        self.assertEqual(types[7], "timedelta64[ns]")

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_with_duplicated_column_names(self):
        for arrow_enabled in [False, True]:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                self.check_to_pandas_with_duplicated_column_names()

    def check_to_pandas_with_duplicated_column_names(self):
        import numpy as np

        sql = "select 1 v, 1 v"
        df = self.spark.sql(sql)
        pdf = df.toPandas()
        types = pdf.dtypes
        self.assertEqual(types.iloc[0], np.int32)
        self.assertEqual(types.iloc[1], np.int32)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_on_cross_join(self):
        for arrow_enabled in [False, True]:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                self.check_to_pandas_on_cross_join()

    def check_to_pandas_on_cross_join(self):
        import numpy as np

        sql = """
        select t1.*, t2.* from (
          select explode(sequence(1, 3)) v
        ) t1 left join (
          select explode(sequence(1, 3)) v
        ) t2
        """
        with self.sql_conf({"spark.sql.crossJoin.enabled": True}):
            df = self.spark.sql(sql)
            pdf = df.toPandas()
            types = pdf.dtypes
            self.assertEqual(types.iloc[0], np.int32)
            self.assertEqual(types.iloc[1], np.int32)

    @unittest.skipIf(have_pandas, "Required Pandas was found.")
    def test_to_pandas_required_pandas_not_found(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegex(ImportError, "Pandas >= .* must be installed"):
                self._to_pandas()

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_avoid_astype(self):
        import numpy as np

        schema = StructType().add("a", IntegerType()).add("b", StringType()).add("c", IntegerType())
        data = [(1, "foo", 16777220), (None, "bar", None)]
        df = self.spark.createDataFrame(data, schema)
        types = df.toPandas().dtypes
        self.assertEqual(types[0], np.float64)  # doesn't convert to np.int32 due to NaN value.
        self.assertEqual(types[1], object)
        self.assertEqual(types[2], np.float64)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_from_empty_dataframe(self):
        is_arrow_enabled = [True, False]
        for value in is_arrow_enabled:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": value}):
                self.check_to_pandas_from_empty_dataframe()

    def check_to_pandas_from_empty_dataframe(self):
        # SPARK-29188 test that toPandas() on an empty dataframe has the correct dtypes
        # SPARK-30537 test that toPandas() on an empty dataframe has the correct dtypes
        # when arrow is enabled
        import numpy as np

        sql = """
            SELECT CAST(1 AS TINYINT) AS tinyint,
            CAST(1 AS SMALLINT) AS smallint,
            CAST(1 AS INT) AS int,
            CAST(1 AS BIGINT) AS bigint,
            CAST(0 AS FLOAT) AS float,
            CAST(0 AS DOUBLE) AS double,
            CAST(1 AS BOOLEAN) AS boolean,
            CAST('foo' AS STRING) AS string,
            CAST('2019-01-01' AS TIMESTAMP) AS timestamp,
            CAST('2019-01-01' AS TIMESTAMP_NTZ) AS timestamp_ntz,
            INTERVAL '1563:04' MINUTE TO SECOND AS day_time_interval
            """
        dtypes_when_nonempty_df = self.spark.sql(sql).toPandas().dtypes
        dtypes_when_empty_df = self.spark.sql(sql).filter("False").toPandas().dtypes
        self.assertTrue(np.all(dtypes_when_empty_df == dtypes_when_nonempty_df))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_from_null_dataframe(self):
        is_arrow_enabled = [True, False]
        for value in is_arrow_enabled:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": value}):
                self.check_to_pandas_from_null_dataframe()

    def check_to_pandas_from_null_dataframe(self):
        # SPARK-29188 test that toPandas() on a dataframe with only nulls has correct dtypes
        # SPARK-30537 test that toPandas() on a dataframe with only nulls has correct dtypes
        # using arrow
        import numpy as np

        sql = """
            SELECT CAST(NULL AS TINYINT) AS tinyint,
            CAST(NULL AS SMALLINT) AS smallint,
            CAST(NULL AS INT) AS int,
            CAST(NULL AS BIGINT) AS bigint,
            CAST(NULL AS FLOAT) AS float,
            CAST(NULL AS DOUBLE) AS double,
            CAST(NULL AS BOOLEAN) AS boolean,
            CAST(NULL AS STRING) AS string,
            CAST(NULL AS TIMESTAMP) AS timestamp,
            CAST(NULL AS TIMESTAMP_NTZ) AS timestamp_ntz,
            INTERVAL '1563:04' MINUTE TO SECOND AS day_time_interval
            """
        pdf = self.spark.sql(sql).toPandas()
        types = pdf.dtypes
        self.assertEqual(types[0], np.float64)
        self.assertEqual(types[1], np.float64)
        self.assertEqual(types[2], np.float64)
        self.assertEqual(types[3], np.float64)
        self.assertEqual(types[4], np.float32)
        self.assertEqual(types[5], np.float64)
        self.assertEqual(types[6], object)
        self.assertEqual(types[7], object)
        self.assertTrue(np.can_cast(np.datetime64, types[8]))
        self.assertTrue(np.can_cast(np.datetime64, types[9]))
        self.assertTrue(np.can_cast(np.timedelta64, types[10]))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_from_mixed_dataframe(self):
        is_arrow_enabled = [True, False]
        for value in is_arrow_enabled:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": value}):
                self.check_to_pandas_from_mixed_dataframe()

    def check_to_pandas_from_mixed_dataframe(self):
        # SPARK-29188 test that toPandas() on a dataframe with some nulls has correct dtypes
        # SPARK-30537 test that toPandas() on a dataframe with some nulls has correct dtypes
        # using arrow
        import numpy as np

        sql = """
        SELECT CAST(col1 AS TINYINT) AS tinyint,
        CAST(col2 AS SMALLINT) AS smallint,
        CAST(col3 AS INT) AS int,
        CAST(col4 AS BIGINT) AS bigint,
        CAST(col5 AS FLOAT) AS float,
        CAST(col6 AS DOUBLE) AS double,
        CAST(col7 AS BOOLEAN) AS boolean,
        CAST(col8 AS STRING) AS string,
        timestamp_seconds(col9) AS timestamp,
        timestamp_seconds(col10) AS timestamp_ntz,
        INTERVAL '1563:04' MINUTE TO SECOND AS day_time_interval
        FROM VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
                    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
        pdf_with_some_nulls = self.spark.sql(sql).toPandas()
        pdf_with_only_nulls = self.spark.sql(sql).filter("tinyint is null").toPandas()
        self.assertTrue(np.all(pdf_with_only_nulls.dtypes == pdf_with_some_nulls.dtypes))

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_to_pandas_for_array_of_struct(self):
        for is_arrow_enabled in [True, False]:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": is_arrow_enabled}):
                self.check_to_pandas_for_array_of_struct(is_arrow_enabled)

    def check_to_pandas_for_array_of_struct(self, is_arrow_enabled):
        # SPARK-38098: Support Array of Struct for Pandas UDFs and toPandas
        import numpy as np
        import pandas as pd

        df = self.spark.createDataFrame(
            [[[("a", 2, 3.0), ("a", 2, 3.0)]], [[("b", 5, 6.0), ("b", 5, 6.0)]]],
            "array_struct_col Array<struct<col1:string, col2:long, col3:double>>",
        )

        pdf = df.toPandas()
        self.assertEqual(type(pdf), pd.DataFrame)
        self.assertEqual(type(pdf["array_struct_col"]), pd.Series)
        if is_arrow_enabled:
            self.assertEqual(type(pdf["array_struct_col"][0]), np.ndarray)
        else:
            self.assertEqual(type(pdf["array_struct_col"][0]), list)

    def test_to_local_iterator(self):
        df = self.spark.range(8, numPartitions=4)
        expected = df.collect()
        it = df.toLocalIterator()
        self.assertEqual(expected, list(it))

        # Test DataFrame with empty partition
        df = self.spark.range(3, numPartitions=4)
        it = df.toLocalIterator()
        expected = df.collect()
        self.assertEqual(expected, list(it))

    def test_to_local_iterator_prefetch(self):
        df = self.spark.range(8, numPartitions=4)
        expected = df.collect()
        it = df.toLocalIterator(prefetchPartitions=True)
        self.assertEqual(expected, list(it))

    def test_to_local_iterator_not_fully_consumed(self):
        with QuietTest(self.sc):
            self.check_to_local_iterator_not_fully_consumed()

    def check_to_local_iterator_not_fully_consumed(self):
        # SPARK-23961: toLocalIterator throws exception when not fully consumed
        # Create a DataFrame large enough so that write to socket will eventually block
        df = self.spark.range(1 << 20, numPartitions=2)
        it = df.toLocalIterator()
        self.assertEqual(df.take(1)[0], next(it))
        it = None  # remove iterator from scope, socket is closed when cleaned up
        # Make sure normal df operations still work
        result = []
        for i, row in enumerate(df.toLocalIterator()):
            result.append(row)
            if i == 7:
                break
        self.assertEqual(df.take(8), result)


class DataFrameCollectionTests(
    DataFrameCollectionTestsMixin,
    ReusedSQLTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_collection import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
