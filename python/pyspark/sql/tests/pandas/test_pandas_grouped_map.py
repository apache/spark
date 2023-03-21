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

from collections import OrderedDict
from decimal import Decimal
from typing import cast

from pyspark.sql import Row
from pyspark.sql.functions import (
    array,
    explode,
    col,
    lit,
    udf,
    sum,
    pandas_udf,
    PandasUDFType,
    window,
)
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    ArrayType,
    BinaryType,
    ByteType,
    LongType,
    DecimalType,
    ShortType,
    FloatType,
    StringType,
    BooleanType,
    StructType,
    StructField,
    NullType,
    TimestampType,
)
from pyspark.errors import PythonException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd
    from pandas.testing import assert_frame_equal

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class GroupedApplyInPandasTestsMixin:
    @property
    def data(self):
        return (
            self.spark.range(10)
            .toDF("id")
            .withColumn("vs", array([lit(i) for i in range(20, 30)]))
            .withColumn("v", explode(col("vs")))
            .drop("vs")
        )

    def test_supported_types(self):

        values = [
            1,
            2,
            3,
            4,
            5,
            1.1,
            2.2,
            Decimal(1.123),
            [1, 2, 2],
            True,
            "hello",
            bytearray([0x01, 0x02]),
            None,
        ]
        output_fields = [
            ("id", IntegerType()),
            ("byte", ByteType()),
            ("short", ShortType()),
            ("int", IntegerType()),
            ("long", LongType()),
            ("float", FloatType()),
            ("double", DoubleType()),
            ("decim", DecimalType(10, 3)),
            ("array", ArrayType(IntegerType())),
            ("bool", BooleanType()),
            ("str", StringType()),
            ("bin", BinaryType()),
            ("null", NullType()),
        ]

        output_schema = StructType([StructField(*x) for x in output_fields])
        df = self.spark.createDataFrame([values], schema=output_schema)

        # Different forms of group map pandas UDF, results of these are the same
        udf1 = pandas_udf(
            lambda pdf: pdf.assign(
                byte=pdf.byte * 2,
                short=pdf.short * 2,
                int=pdf.int * 2,
                long=pdf.long * 2,
                float=pdf.float * 2,
                double=pdf.double * 2,
                decim=pdf.decim * 2,
                bool=False if pdf.bool else True,
                str=pdf.str + "there",
                array=pdf.array,
                bin=pdf.bin,
                null=pdf.null,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP,
        )

        udf2 = pandas_udf(
            lambda _, pdf: pdf.assign(
                byte=pdf.byte * 2,
                short=pdf.short * 2,
                int=pdf.int * 2,
                long=pdf.long * 2,
                float=pdf.float * 2,
                double=pdf.double * 2,
                decim=pdf.decim * 2,
                bool=False if pdf.bool else True,
                str=pdf.str + "there",
                array=pdf.array,
                bin=pdf.bin,
                null=pdf.null,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP,
        )

        udf3 = pandas_udf(
            lambda key, pdf: pdf.assign(
                id=key[0],
                byte=pdf.byte * 2,
                short=pdf.short * 2,
                int=pdf.int * 2,
                long=pdf.long * 2,
                float=pdf.float * 2,
                double=pdf.double * 2,
                decim=pdf.decim * 2,
                bool=False if pdf.bool else True,
                str=pdf.str + "there",
                array=pdf.array,
                bin=pdf.bin,
                null=pdf.null,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP,
        )

        result1 = df.groupby("id").apply(udf1).sort("id").toPandas()
        expected1 = df.toPandas().groupby("id").apply(udf1.func).reset_index(drop=True)

        result2 = df.groupby("id").apply(udf2).sort("id").toPandas()
        expected2 = expected1

        result3 = df.groupby("id").apply(udf3).sort("id").toPandas()
        expected3 = expected1

        assert_frame_equal(expected1, result1)
        assert_frame_equal(expected2, result2)
        assert_frame_equal(expected3, result3)

    def test_array_type_correct(self):
        df = self.data.withColumn("arr", array(col("id"))).repartition(1, "id")

        output_schema = StructType(
            [
                StructField("id", LongType()),
                StructField("v", IntegerType()),
                StructField("arr", ArrayType(LongType())),
            ]
        )

        udf = pandas_udf(lambda pdf: pdf, output_schema, PandasUDFType.GROUPED_MAP)

        result = df.groupby("id").apply(udf).sort("id").toPandas()
        expected = df.toPandas().groupby("id").apply(udf.func).reset_index(drop=True)
        assert_frame_equal(expected, result)

    def test_register_grouped_map_udf(self):
        foo_udf = pandas_udf(lambda x: x, "id long", PandasUDFType.GROUPED_MAP)
        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                ValueError,
                "f.*SQL_BATCHED_UDF.*SQL_SCALAR_PANDAS_UDF.*SQL_GROUPED_AGG_PANDAS_UDF.*",
            ):
                self.spark.catalog.registerFunction("foo_udf", foo_udf)

    def test_decorator(self):
        df = self.data

        @pandas_udf("id long, v int, v1 double, v2 long", PandasUDFType.GROUPED_MAP)
        def foo(pdf):
            return pdf.assign(v1=pdf.v * pdf.id * 1.0, v2=pdf.v + pdf.id)

        result = df.groupby("id").apply(foo).sort("id").toPandas()
        expected = df.toPandas().groupby("id").apply(foo.func).reset_index(drop=True)
        assert_frame_equal(expected, result)

    def test_coerce(self):
        df = self.data

        foo = pandas_udf(lambda pdf: pdf, "id long, v double", PandasUDFType.GROUPED_MAP)

        result = df.groupby("id").apply(foo).sort("id").toPandas()
        expected = df.toPandas().groupby("id").apply(foo.func).reset_index(drop=True)
        expected = expected.assign(v=expected.v.astype("float64"))
        assert_frame_equal(expected, result)

    def test_complex_groupby(self):
        df = self.data

        @pandas_udf("id long, v int, norm double", PandasUDFType.GROUPED_MAP)
        def normalize(pdf):
            v = pdf.v
            return pdf.assign(norm=(v - v.mean()) / v.std())

        result = df.groupby(col("id") % 2 == 0).apply(normalize).sort("id", "v").toPandas()
        pdf = df.toPandas()
        expected = pdf.groupby(pdf["id"] % 2 == 0, as_index=False).apply(normalize.func)
        expected = expected.sort_values(["id", "v"]).reset_index(drop=True)
        expected = expected.assign(norm=expected.norm.astype("float64"))
        assert_frame_equal(expected, result)

    def test_empty_groupby(self):
        df = self.data

        @pandas_udf("id long, v int, norm double", PandasUDFType.GROUPED_MAP)
        def normalize(pdf):
            v = pdf.v
            return pdf.assign(norm=(v - v.mean()) / v.std())

        result = df.groupby().apply(normalize).sort("id", "v").toPandas()
        pdf = df.toPandas()
        expected = normalize.func(pdf)
        expected = expected.sort_values(["id", "v"]).reset_index(drop=True)
        expected = expected.assign(norm=expected.norm.astype("float64"))
        assert_frame_equal(expected, result)

    def test_apply_in_pandas_not_returning_pandas_dataframe(self):
        df = self.data

        def stats(key, _):
            return key

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Return type of the user-defined function should be pandas.DataFrame, "
                "but is <class 'tuple'>",
            ):
                df.groupby("id").applyInPandas(stats, schema="id integer, m double").collect()

    def test_apply_in_pandas_returning_wrong_number_of_columns(self):
        df = self.data

        def stats(key, pdf):
            v = pdf.v
            # returning three columns
            res = pd.DataFrame([key + (v.mean(), v.std())])
            return res

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Number of columns of the returned pandas.DataFrame doesn't match "
                "specified schema. Expected: 2 Actual: 3",
            ):
                # stats returns three columns while here we set schema with two columns
                df.groupby("id").applyInPandas(stats, schema="id integer, m double").collect()

    def test_apply_in_pandas_returning_empty_dataframe(self):
        df = self.data

        def odd_means(key, pdf):
            if key[0] % 2 == 0:
                return pd.DataFrame([])
            else:
                return pd.DataFrame([key + (pdf.v.mean(),)])

        expected_ids = {row[0] for row in self.data.collect() if row[0] % 2 != 0}

        result = (
            df.groupby("id")
            .applyInPandas(odd_means, schema="id integer, m double")
            .sort("id", "m")
            .collect()
        )

        actual_ids = {row[0] for row in result}
        self.assertSetEqual(expected_ids, actual_ids)

        self.assertEqual(len(expected_ids), len(result))
        for row in result:
            self.assertEqual(24.5, row[1])

    def test_apply_in_pandas_returning_empty_dataframe_and_wrong_number_of_columns(self):
        df = self.data

        def odd_means(key, pdf):
            if key[0] % 2 == 0:
                return pd.DataFrame([], columns=["id"])
            else:
                return pd.DataFrame([key + (pdf.v.mean(),)])

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Number of columns of the returned pandas.DataFrame doesn't match "
                "specified schema. Expected: 2 Actual: 1",
            ):
                # stats returns one column for even keys while here we set schema with two columns
                df.groupby("id").applyInPandas(odd_means, schema="id integer, m double").collect()

    def test_datatype_string(self):
        df = self.data

        foo_udf = pandas_udf(
            lambda pdf: pdf.assign(v1=pdf.v * pdf.id * 1.0, v2=pdf.v + pdf.id),
            "id long, v int, v1 double, v2 long",
            PandasUDFType.GROUPED_MAP,
        )

        result = df.groupby("id").apply(foo_udf).sort("id").toPandas()
        expected = df.toPandas().groupby("id").apply(foo_udf.func).reset_index(drop=True)
        assert_frame_equal(expected, result)

    def test_wrong_return_type(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                NotImplementedError,
                "Invalid return type.*grouped map Pandas UDF.*ArrayType.*TimestampType",
            ):
                pandas_udf(
                    lambda pdf: pdf, "id long, v array<timestamp>", PandasUDFType.GROUPED_MAP
                )

    def test_wrong_args(self):
        df = self.data

        with QuietTest(self.sc):
            with self.assertRaisesRegex(ValueError, "Invalid udf"):
                df.groupby("id").apply(lambda x: x)
            with self.assertRaisesRegex(ValueError, "Invalid udf"):
                df.groupby("id").apply(udf(lambda x: x, DoubleType()))
            with self.assertRaisesRegex(ValueError, "Invalid udf"):
                df.groupby("id").apply(sum(df.v))
            with self.assertRaisesRegex(ValueError, "Invalid udf"):
                df.groupby("id").apply(df.v + 1)
            with self.assertRaisesRegex(ValueError, "Invalid function"):
                df.groupby("id").apply(
                    pandas_udf(lambda: 1, StructType([StructField("d", DoubleType())]))
                )
            with self.assertRaisesRegex(ValueError, "Invalid udf"):
                df.groupby("id").apply(pandas_udf(lambda x, y: x, DoubleType()))
            with self.assertRaisesRegex(ValueError, "Invalid udf.*GROUPED_MAP"):
                df.groupby("id").apply(
                    pandas_udf(lambda x, y: x, DoubleType(), PandasUDFType.SCALAR)
                )

    def test_unsupported_types(self):
        common_err_msg = "Invalid return type.*grouped map Pandas UDF.*"
        unsupported_types = [
            StructField("arr_ts", ArrayType(TimestampType())),
            StructField("struct", StructType([StructField("l", LongType())])),
        ]

        for unsupported_type in unsupported_types:
            schema = StructType([StructField("id", LongType(), True), unsupported_type])
            with QuietTest(self.sc):
                with self.assertRaisesRegex(NotImplementedError, common_err_msg):
                    pandas_udf(lambda x: x, schema, PandasUDFType.GROUPED_MAP)

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [
            datetime.datetime(2015, 11, 1, 0, 30),
            datetime.datetime(2015, 11, 1, 1, 30),
            datetime.datetime(2015, 11, 1, 2, 30),
        ]
        df = self.spark.createDataFrame(dt, "timestamp").toDF("time")
        foo_udf = pandas_udf(lambda pdf: pdf, "time timestamp", PandasUDFType.GROUPED_MAP)
        result = df.groupby("time").apply(foo_udf).sort("time")
        assert_frame_equal(df.toPandas(), result.toPandas())

    def test_udf_with_key(self):
        import numpy as np

        df = self.data
        pdf = df.toPandas()

        def foo1(key, pdf):
            assert type(key) == tuple
            assert type(key[0]) == np.int64

            return pdf.assign(
                v1=key[0], v2=pdf.v * key[0], v3=pdf.v * pdf.id, v4=pdf.v * pdf.id.mean()
            )

        def foo2(key, pdf):
            assert type(key) == tuple
            assert type(key[0]) == np.int64
            assert type(key[1]) == np.int32

            return pdf.assign(v1=key[0], v2=key[1], v3=pdf.v * key[0], v4=pdf.v + key[1])

        def foo3(key, pdf):
            assert type(key) == tuple
            assert len(key) == 0
            return pdf.assign(v1=pdf.v * pdf.id)

        # v2 is int because numpy.int64 * pd.Series<int32> results in pd.Series<int32>
        # v3 is long because pd.Series<int64> * pd.Series<int32> results in pd.Series<int64>
        udf1 = pandas_udf(
            foo1, "id long, v int, v1 long, v2 int, v3 long, v4 double", PandasUDFType.GROUPED_MAP
        )

        udf2 = pandas_udf(
            foo2, "id long, v int, v1 long, v2 int, v3 int, v4 int", PandasUDFType.GROUPED_MAP
        )

        udf3 = pandas_udf(foo3, "id long, v int, v1 long", PandasUDFType.GROUPED_MAP)

        # Test groupby column
        result1 = df.groupby("id").apply(udf1).sort("id", "v").toPandas()
        expected1 = (
            pdf.groupby("id", as_index=False)
            .apply(lambda x: udf1.func((x.id.iloc[0],), x))
            .sort_values(["id", "v"])
            .reset_index(drop=True)
        )
        assert_frame_equal(expected1, result1)

        # Test groupby expression
        result2 = df.groupby(df.id % 2).apply(udf1).sort("id", "v").toPandas()
        expected2 = (
            pdf.groupby(pdf.id % 2, as_index=False)
            .apply(lambda x: udf1.func((x.id.iloc[0] % 2,), x))
            .sort_values(["id", "v"])
            .reset_index(drop=True)
        )
        assert_frame_equal(expected2, result2)

        # Test complex groupby
        result3 = df.groupby(df.id, df.v % 2).apply(udf2).sort("id", "v").toPandas()
        expected3 = (
            pdf.groupby([pdf.id, pdf.v % 2], as_index=False)
            .apply(
                lambda x: udf2.func(
                    (
                        x.id.iloc[0],
                        (x.v % 2).iloc[0],
                    ),
                    x,
                )
            )
            .sort_values(["id", "v"])
            .reset_index(drop=True)
        )
        assert_frame_equal(expected3, result3)

        # Test empty groupby
        result4 = df.groupby().apply(udf3).sort("id", "v").toPandas()
        expected4 = udf3.func((), pdf)
        assert_frame_equal(expected4, result4)

    def test_column_order(self):

        # Helper function to set column names from a list
        def rename_pdf(pdf, names):
            pdf.rename(
                columns={old: new for old, new in zip(pd_result.columns, names)}, inplace=True
            )

        df = self.data
        grouped_df = df.groupby("id")
        grouped_pdf = df.toPandas().groupby("id", as_index=False)

        # Function returns a pdf with required column names, but order could be arbitrary using dict
        def change_col_order(pdf):
            # Constructing a DataFrame from a dict should result in the same order,
            # but use OrderedDict to ensure the pdf column order is different than schema
            return pd.DataFrame.from_dict(
                OrderedDict([("id", pdf.id), ("u", pdf.v * 2), ("v", pdf.v)])
            )

        ordered_udf = pandas_udf(
            change_col_order, "id long, v int, u int", PandasUDFType.GROUPED_MAP
        )

        # The UDF result should assign columns by name from the pdf
        result = grouped_df.apply(ordered_udf).sort("id", "v").select("id", "u", "v").toPandas()
        pd_result = grouped_pdf.apply(change_col_order)
        expected = pd_result.sort_values(["id", "v"]).reset_index(drop=True)
        assert_frame_equal(expected, result)

        # Function returns a pdf with positional columns, indexed by range
        def range_col_order(pdf):
            # Create a DataFrame with positional columns, fix types to long
            return pd.DataFrame(list(zip(pdf.id, pdf.v * 3, pdf.v)), dtype="int64")

        range_udf = pandas_udf(
            range_col_order, "id long, u long, v long", PandasUDFType.GROUPED_MAP
        )

        # The UDF result uses positional columns from the pdf
        result = grouped_df.apply(range_udf).sort("id", "v").select("id", "u", "v").toPandas()
        pd_result = grouped_pdf.apply(range_col_order)
        rename_pdf(pd_result, ["id", "u", "v"])
        expected = pd_result.sort_values(["id", "v"]).reset_index(drop=True)
        assert_frame_equal(expected, result)

        # Function returns a pdf with columns indexed with integers
        def int_index(pdf):
            return pd.DataFrame(OrderedDict([(0, pdf.id), (1, pdf.v * 4), (2, pdf.v)]))

        int_index_udf = pandas_udf(int_index, "id long, u int, v int", PandasUDFType.GROUPED_MAP)

        # The UDF result should assign columns by position of integer index
        result = grouped_df.apply(int_index_udf).sort("id", "v").select("id", "u", "v").toPandas()
        pd_result = grouped_pdf.apply(int_index)
        rename_pdf(pd_result, ["id", "u", "v"])
        expected = pd_result.sort_values(["id", "v"]).reset_index(drop=True)
        assert_frame_equal(expected, result)

        @pandas_udf("id long, v int", PandasUDFType.GROUPED_MAP)
        def column_name_typo(pdf):
            return pd.DataFrame({"iid": pdf.id, "v": pdf.v})

        @pandas_udf("id long, v decimal", PandasUDFType.GROUPED_MAP)
        def invalid_positional_types(pdf):
            return pd.DataFrame([(1, datetime.date(2020, 10, 5))])

        with self.sql_conf({"spark.sql.execution.pandas.convertToArrowArraySafely": False}):
            with QuietTest(self.sc):
                with self.assertRaisesRegex(Exception, "KeyError: 'id'"):
                    grouped_df.apply(column_name_typo).collect()
                with self.assertRaisesRegex(Exception, "[D|d]ecimal.*got.*date"):
                    grouped_df.apply(invalid_positional_types).collect()

    def test_positional_assignment_conf(self):
        with self.sql_conf(
            {"spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": False}
        ):

            @pandas_udf("a string, b float", PandasUDFType.GROUPED_MAP)
            def foo(_):
                return pd.DataFrame([("hi", 1)], columns=["x", "y"])

            df = self.data
            result = df.groupBy("id").apply(foo).select("a", "b").collect()
            for r in result:
                self.assertEqual(r.a, "hi")
                self.assertEqual(r.b, 1)

    def test_self_join_with_pandas(self):
        @pandas_udf("key long, col string", PandasUDFType.GROUPED_MAP)
        def dummy_pandas_udf(df):
            return df[["key", "col"]]

        df = self.spark.createDataFrame(
            [Row(key=1, col="A"), Row(key=1, col="B"), Row(key=2, col="C")]
        )
        df_with_pandas = df.groupBy("key").apply(dummy_pandas_udf)

        # this was throwing an AnalysisException before SPARK-24208
        res = df_with_pandas.alias("temp0").join(
            df_with_pandas.alias("temp1"), col("temp0.key") == col("temp1.key")
        )
        self.assertEqual(res.count(), 5)

    def test_mixed_scalar_udfs_followed_by_groupby_apply(self):
        df = self.spark.range(0, 10).toDF("v1")
        df = df.withColumn("v2", udf(lambda x: x + 1, "int")(df["v1"])).withColumn(
            "v3", pandas_udf(lambda x: x + 2, "int")(df["v1"])
        )

        result = df.groupby().apply(
            pandas_udf(
                lambda x: pd.DataFrame([x.sum().sum()]), "sum int", PandasUDFType.GROUPED_MAP
            )
        )

        self.assertEqual(result.collect()[0]["sum"], 165)

    def test_grouped_with_empty_partition(self):
        data = [Row(id=1, x=2), Row(id=1, x=3), Row(id=2, x=4)]
        expected = [Row(id=1, x=5), Row(id=1, x=5), Row(id=2, x=4)]
        num_parts = len(data) + 1
        df = self.spark.createDataFrame(self.sc.parallelize(data, numSlices=num_parts))

        f = pandas_udf(
            lambda pdf: pdf.assign(x=pdf["x"].sum()), "id long, x int", PandasUDFType.GROUPED_MAP
        )

        result = df.groupBy("id").apply(f).collect()
        self.assertEqual(result, expected)

    def test_grouped_over_window(self):

        data = [
            (0, 1, "2018-03-10T00:00:00+00:00", [0]),
            (1, 2, "2018-03-11T00:00:00+00:00", [0]),
            (2, 2, "2018-03-12T00:00:00+00:00", [0]),
            (3, 3, "2018-03-15T00:00:00+00:00", [0]),
            (4, 3, "2018-03-16T00:00:00+00:00", [0]),
            (5, 3, "2018-03-17T00:00:00+00:00", [0]),
            (6, 3, "2018-03-21T00:00:00+00:00", [0]),
        ]

        expected = {0: [0], 1: [1, 2], 2: [1, 2], 3: [3, 4, 5], 4: [3, 4, 5], 5: [3, 4, 5], 6: [6]}

        df = self.spark.createDataFrame(data, ["id", "group", "ts", "result"])
        df = df.select(col("id"), col("group"), col("ts").cast("timestamp"), col("result"))

        def f(pdf):
            # Assign each result element the ids of the windowed group
            pdf["result"] = [pdf["id"]] * len(pdf)
            return pdf

        result = (
            df.groupby("group", window("ts", "5 days"))
            .applyInPandas(f, df.schema)
            .select("id", "result")
            .collect()
        )
        for r in result:
            self.assertListEqual(expected[r[0]], r[1])

    def test_grouped_over_window_with_key(self):

        data = [
            (0, 1, "2018-03-10T00:00:00+00:00", [0]),
            (1, 2, "2018-03-11T00:00:00+00:00", [0]),
            (2, 2, "2018-03-12T00:00:00+00:00", [0]),
            (3, 3, "2018-03-15T00:00:00+00:00", [0]),
            (4, 3, "2018-03-16T00:00:00+00:00", [0]),
            (5, 3, "2018-03-17T00:00:00+00:00", [0]),
            (6, 3, "2018-03-21T00:00:00+00:00", [0]),
        ]

        expected_window = [
            {
                "start": datetime.datetime(2018, 3, 10, 0, 0),
                "end": datetime.datetime(2018, 3, 15, 0, 0),
            },
            {
                "start": datetime.datetime(2018, 3, 15, 0, 0),
                "end": datetime.datetime(2018, 3, 20, 0, 0),
            },
            {
                "start": datetime.datetime(2018, 3, 20, 0, 0),
                "end": datetime.datetime(2018, 3, 25, 0, 0),
            },
        ]

        expected_key = {
            0: (1, expected_window[0]),
            1: (2, expected_window[0]),
            2: (2, expected_window[0]),
            3: (3, expected_window[1]),
            4: (3, expected_window[1]),
            5: (3, expected_window[1]),
            6: (3, expected_window[2]),
        }

        # id -> array of group with len of num records in window
        expected = {0: [1], 1: [2, 2], 2: [2, 2], 3: [3, 3, 3], 4: [3, 3, 3], 5: [3, 3, 3], 6: [3]}

        df = self.spark.createDataFrame(data, ["id", "group", "ts", "result"])
        df = df.select(col("id"), col("group"), col("ts").cast("timestamp"), col("result"))

        def f(key, pdf):
            group = key[0]
            window_range = key[1]

            # Make sure the key with group and window values are correct
            for _, i in pdf.id.items():
                assert expected_key[i][0] == group, "{} != {}".format(expected_key[i][0], group)
                assert expected_key[i][1] == window_range, "{} != {}".format(
                    expected_key[i][1], window_range
                )

            return pdf.assign(result=[[group] * len(pdf)] * len(pdf))

        result = (
            df.groupby("group", window("ts", "5 days"))
            .applyInPandas(f, df.schema)
            .select("id", "result")
            .collect()
        )

        for r in result:
            self.assertListEqual(expected[r[0]], r[1])

    def test_case_insensitive_grouping_column(self):
        # SPARK-31915: case-insensitive grouping column should work.
        def my_pandas_udf(pdf):
            return pdf.assign(score=0.5)

        df = self.spark.createDataFrame([[1, 1]], ["column", "score"])
        row = (
            df.groupby("COLUMN")
            .applyInPandas(my_pandas_udf, schema="column integer, score float")
            .first()
        )
        self.assertEqual(row.asDict(), Row(column=1, score=0.5).asDict())


class GroupedApplyInPandasTests(GroupedApplyInPandasTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_grouped_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
