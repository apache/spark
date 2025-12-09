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
import logging
import os

from collections import OrderedDict
from decimal import Decimal
from typing import cast, Iterator, Tuple, Any

from pyspark.sql import Row, functions as sf
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
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
    MapType,
    YearMonthIntervalType,
)
from pyspark.errors import PythonException, PySparkTypeError, PySparkValueError
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.util import is_remote_only

if have_pyarrow and have_pandas:
    import pandas as pd
    from pandas.testing import assert_frame_equal


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class ApplyInPandasTestsMixin:
    @property
    def data(self):
        return (
            self.spark.range(10)
            .withColumn("vs", sf.array([sf.lit(i) for i in range(20, 30)]))
            .withColumn("v", sf.explode(sf.col("vs")))
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
        df = self.data.withColumn("arr", sf.array(sf.col("id"))).repartition(1, "id")

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
        with self.quiet(), self.temp_func("foo_udf"):
            foo_udf = pandas_udf(lambda x: x, "id long", PandasUDFType.GROUPED_MAP)

            with self.assertRaises(PySparkTypeError) as pe:
                self.spark.catalog.registerFunction("foo_udf", foo_udf)

            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_UDF_EVAL_TYPE",
                messageParameters={
                    "eval_type": "SQL_BATCHED_UDF, SQL_ARROW_BATCHED_UDF, "
                    "SQL_SCALAR_PANDAS_UDF, SQL_SCALAR_ARROW_UDF, "
                    "SQL_SCALAR_PANDAS_ITER_UDF, SQL_SCALAR_ARROW_ITER_UDF, "
                    "SQL_GROUPED_AGG_PANDAS_UDF, SQL_GROUPED_AGG_ARROW_UDF or "
                    "SQL_GROUPED_AGG_ARROW_ITER_UDF"
                },
            )

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

        result = df.groupby(sf.col("id") % 2 == 0).apply(normalize).sort("id", "v").toPandas()
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
        with self.quiet():
            self.check_apply_in_pandas_not_returning_pandas_dataframe()

    def check_apply_in_pandas_not_returning_pandas_dataframe(self):
        with self.assertRaisesRegex(
            PythonException,
            "Return type of the user-defined function should be pandas.DataFrame, but is tuple.",
        ):
            self._test_apply_in_pandas(lambda key, pdf: key)

    def test_apply_in_pandas_returning_column_names(self):
        self._test_apply_in_pandas(
            lambda key, pdf: pd.DataFrame([(pdf.v.mean(),) + key], columns=["mean", "id"])
        )

    def test_apply_in_pandas_returning_no_column_names(self):
        self._test_apply_in_pandas(lambda key, pdf: pd.DataFrame([key + (pdf.v.mean(),)]))

    def test_apply_in_pandas_returning_column_names_sometimes(self):
        def stats(key, pdf):
            if key[0] % 2:
                return pd.DataFrame([(pdf.v.mean(),) + key], columns=["mean", "id"])
            else:
                return pd.DataFrame([key + (pdf.v.mean(),)])

        self._test_apply_in_pandas(stats)

    def test_apply_in_pandas_returning_wrong_column_names(self):
        with self.quiet():
            self.check_apply_in_pandas_returning_wrong_column_names()

    def check_apply_in_pandas_returning_wrong_column_names(self):
        with self.assertRaisesRegex(
            PythonException,
            "Column names of the returned pandas.DataFrame do not match specified schema. "
            "Missing: mean. Unexpected: median, std.\n",
        ):
            self._test_apply_in_pandas(
                lambda key, pdf: pd.DataFrame(
                    [key + (pdf.v.median(), pdf.v.std())], columns=["id", "median", "std"]
                )
            )

    def test_apply_in_pandas_returning_no_column_names_and_wrong_amount(self):
        with self.quiet():
            self.check_apply_in_pandas_returning_no_column_names_and_wrong_amount()

    def check_apply_in_pandas_returning_no_column_names_and_wrong_amount(self):
        with self.assertRaisesRegex(
            PythonException,
            "Number of columns of the returned pandas.DataFrame doesn't match "
            "specified schema. Expected: 2 Actual: 3\n",
        ):
            self._test_apply_in_pandas(
                lambda key, pdf: pd.DataFrame([key + (pdf.v.mean(), pdf.v.std())])
            )

    @unittest.skipIf(
        os.environ.get("SPARK_SKIP_CONNECT_COMPAT_TESTS") == "1", "SPARK-54482: To be reenabled"
    )
    def test_apply_in_pandas_returning_empty_dataframe(self):
        self._test_apply_in_pandas_returning_empty_dataframe(pd.DataFrame())

    @unittest.skipIf(
        os.environ.get("SPARK_SKIP_CONNECT_COMPAT_TESTS") == "1", "SPARK-54482: To be reenabled"
    )
    def test_apply_in_pandas_returning_incompatible_type(self):
        with self.quiet():
            self.check_apply_in_pandas_returning_incompatible_type()

    def check_apply_in_pandas_returning_incompatible_type(self):
        for safely in [True, False]:
            with self.subTest(convertToArrowArraySafely=safely), self.sql_conf(
                {"spark.sql.execution.pandas.convertToArrowArraySafely": safely}
            ):
                # sometimes we see ValueErrors
                with self.subTest(convert="string to double"):
                    expected = (
                        r"ValueError: Exception thrown when converting pandas.Series \(object\) "
                        r"with name 'mean' to Arrow Array \(double\)."
                    )
                    if safely:
                        expected = expected + (
                            " It can be caused by overflows or other "
                            "unsafe conversions warned by Arrow. Arrow safe type check "
                            "can be disabled by using SQL config "
                            "`spark.sql.execution.pandas.convertToArrowArraySafely`."
                        )
                    with self.assertRaisesRegex(PythonException, expected + "\n"):
                        self._test_apply_in_pandas(
                            lambda key, pdf: pd.DataFrame([key + ("test_string",)]),
                            output_schema="id long, mean double",
                        )

                # sometimes we see TypeErrors
                with self.subTest(convert="double to string"):
                    with self.assertRaisesRegex(
                        PythonException,
                        r"TypeError: Exception thrown when converting pandas.Series \(float64\) "
                        r"with name 'mean' to Arrow Array \(string\).\n",
                    ):
                        self._test_apply_in_pandas(
                            lambda key, pdf: pd.DataFrame([key + (pdf.v.mean(),)]),
                            output_schema="id long, mean string",
                        )

    def test_apply_in_pandas_int_to_decimal_coercion(self):
        def int_to_decimal_func(key, pdf):
            return pd.DataFrame([{"id": key[0], "decimal_result": 12345}])

        with self.sql_conf(
            {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": True}
        ):
            result = (
                self.data.groupby("id")
                .applyInPandas(int_to_decimal_func, schema="id long, decimal_result decimal(10,2)")
                .collect()
            )

            self.assertTrue(len(result) > 0)
            for row in result:
                self.assertEqual(row.decimal_result, 12345.00)

        with self.sql_conf(
            {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": False}
        ):
            with self.assertRaisesRegex(
                PythonException, "Exception thrown when converting pandas.Series"
            ):
                (
                    self.data.groupby("id")
                    .applyInPandas(
                        int_to_decimal_func, schema="id long, decimal_result decimal(10,2)"
                    )
                    .collect()
                )

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
        with self.quiet():
            self.check_wrong_return_type()

    def check_wrong_return_type(self):
        with self.assertRaisesRegex(
            NotImplementedError,
            "Invalid return type.*grouped map Pandas UDF.*ArrayType.*YearMonthIntervalType",
        ):
            pandas_udf(
                lambda pdf: pdf,
                StructType().add("id", LongType()).add("v", ArrayType(YearMonthIntervalType())),
                PandasUDFType.GROUPED_MAP,
            )

    def test_wrong_args(self):
        with self.quiet():
            self.check_wrong_args()

    def check_wrong_args(self):
        df = self.data

        with self.assertRaisesRegex(PySparkTypeError, "INVALID_UDF_EVAL_TYPE"):
            df.groupby("id").apply(lambda x: x)
        with self.assertRaisesRegex(PySparkTypeError, "INVALID_UDF_EVAL_TYPE"):
            df.groupby("id").apply(udf(lambda x: x, DoubleType()))
        with self.assertRaisesRegex(PySparkTypeError, "INVALID_UDF_EVAL_TYPE"):
            df.groupby("id").apply(sf.sum(df.v))
        with self.assertRaisesRegex(PySparkTypeError, "INVALID_UDF_EVAL_TYPE"):
            df.groupby("id").apply(df.v + 1)
        with self.assertRaisesRegex(PySparkTypeError, "INVALID_UDF_EVAL_TYPE"):
            df.groupby("id").apply(pandas_udf(lambda x, y: x, DoubleType()))
        with self.assertRaisesRegex(PySparkTypeError, "INVALID_UDF_EVAL_TYPE"):
            df.groupby("id").apply(pandas_udf(lambda x, y: x, DoubleType(), PandasUDFType.SCALAR))

        with self.assertRaisesRegex(PySparkValueError, "INVALID_PANDAS_UDF"):
            df.groupby("id").apply(
                pandas_udf(lambda: 1, StructType([StructField("d", DoubleType())]))
            )

    def test_wrong_args_in_apply_func(self):
        df1 = self.spark.range(11)
        df2 = self.spark.range(22)

        with self.assertRaisesRegex(PySparkValueError, "INVALID_PANDAS_UDF"):
            df1.groupby("id").applyInPandas(lambda: 1, StructType([StructField("d", DoubleType())]))

        with self.assertRaisesRegex(PySparkValueError, "INVALID_PANDAS_UDF"):
            df1.groupby("id").applyInArrow(lambda: 1, StructType([StructField("d", DoubleType())]))

        with self.assertRaisesRegex(PySparkValueError, "INVALID_PANDAS_UDF"):
            df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
                lambda: 1, StructType([StructField("d", DoubleType())])
            )

        with self.assertRaisesRegex(PySparkValueError, "INVALID_PANDAS_UDF"):
            df1.groupby("id").cogroup(df2.groupby("id")).applyInArrow(
                lambda: 1, StructType([StructField("d", DoubleType())])
            )

    def test_unsupported_types(self):
        with self.quiet():
            self.check_unsupported_types()

    def check_unsupported_types(self):
        common_err_msg = "Invalid return type.*grouped map Pandas UDF.*"
        unsupported_types = [
            StructField("array_struct", ArrayType(YearMonthIntervalType())),
            StructField("map", MapType(StringType(), YearMonthIntervalType())),
        ]

        for unsupported_type in unsupported_types:
            with self.subTest(unsupported_type=unsupported_type.name):
                schema = StructType([StructField("id", LongType(), True), unsupported_type])

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
        with self.quiet():
            self.check_column_order()

    def check_column_order(self):
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
            with self.assertRaisesRegex(
                PythonException,
                "Column names of the returned pandas.DataFrame do not match "
                "specified schema. Missing: id. Unexpected: iid.\n",
            ):
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
            df_with_pandas.alias("temp1"), sf.col("temp0.key") == sf.col("temp1.key")
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
        df = self.spark.createDataFrame(data).repartition(num_parts)

        f = pandas_udf(
            lambda pdf: pdf.assign(x=pdf["x"].sum()), "id long, x int", PandasUDFType.GROUPED_MAP
        )

        result = df.groupBy("id").apply(f).sort("id").collect()
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
        df = df.select(
            sf.col("id"), sf.col("group"), sf.col("ts").cast("timestamp"), sf.col("result")
        )

        def f(pdf):
            # Assign each result element the ids of the windowed group
            pdf["result"] = [pdf["id"]] * len(pdf)
            return pdf

        result = (
            df.groupby("group", sf.window("ts", "5 days"))
            .applyInPandas(f, df.schema)
            .select("id", "result")
            .orderBy("id")
            .collect()
        )

        self.assertListEqual([Row(id=key, result=val) for key, val in expected.items()], result)

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

        timezone = self.spark.conf.get("spark.sql.session.timeZone")
        expected_window = [
            {
                key: (
                    pd.Timestamp(ts)
                    .tz_localize(datetime.timezone.utc)
                    .tz_convert(timezone)
                    .tz_localize(None)
                )
                for key, ts in w.items()
            }
            for w in [
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
        df = df.select(
            sf.col("id"), sf.col("group"), sf.col("ts").cast("timestamp"), sf.col("result")
        )

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
            df.groupby("group", sf.window("ts", "5 days"))
            .applyInPandas(f, df.schema)
            .select("id", "result")
            .orderBy("id")
            .collect()
        )

        self.assertListEqual([Row(id=key, result=val) for key, val in expected.items()], result)

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

    def _test_apply_in_pandas(self, f, output_schema="id long, mean double"):
        df = self.data

        result = (
            df.groupby("id").applyInPandas(f, schema=output_schema).sort("id", "mean").toPandas()
        )
        expected = df.select("id").distinct().withColumn("mean", sf.lit(24.5)).toPandas()

        assert_frame_equal(expected, result)

    def _test_apply_in_pandas_returning_empty_dataframe(self, empty_df):
        """Tests some returned DataFrames are empty."""
        df = self.data

        def stats(key, pdf):
            if key[0] % 2 == 0:
                return pd.DataFrame([key + (pdf.v.mean(),)])
            return empty_df

        result = (
            df.groupby("id")
            .applyInPandas(stats, schema="id long, mean double")
            .sort("id", "mean")
            .collect()
        )

        actual_ids = {row[0] for row in result}
        expected_ids = {row[0] for row in self.data.collect() if row[0] % 2 == 0}
        self.assertSetEqual(expected_ids, actual_ids)
        self.assertEqual(len(expected_ids), len(result))
        for row in result:
            self.assertEqual(24.5, row[1])

    def _test_apply_in_pandas_returning_empty_dataframe_error(self, empty_df, error):
        with self.quiet():
            with self.assertRaisesRegex(PythonException, error):
                self._test_apply_in_pandas_returning_empty_dataframe(empty_df)

    def test_arrow_cast_enabled_numeric_to_decimal(self):
        import numpy as np

        columns = [
            "int8",
            "int16",
            "int32",
            "uint8",
            "uint16",
            "uint32",
            "float64",
        ]

        pdf = pd.DataFrame({key: np.arange(1, 2).astype(key) for key in columns})
        df = self.spark.range(2).repartition(1)

        for column in columns:
            with self.subTest(column=column):
                v = pdf[column].iloc[:1]
                schema_str = "id long, value decimal(10,0)"

                @pandas_udf(schema_str, PandasUDFType.GROUPED_MAP)
                def test(pdf):
                    return pdf.assign(**{"value": v})

                row = df.groupby("id").apply(test).first()
                res = row[1]
                self.assertEqual(res, Decimal("1"))

    def test_arrow_cast_enabled_str_to_numeric(self):
        df = self.spark.range(2).repartition(1)

        types = ["int", "long", "float", "double"]

        for type_str in types:
            with self.subTest(type=type_str):
                schema_str = "id long, value " + type_str

                @pandas_udf(schema_str, PandasUDFType.GROUPED_MAP)
                def test(pdf):
                    return pdf.assign(value=pd.Series(["123"]))

                row = df.groupby("id").apply(test).first()
                self.assertEqual(row[1], 123)

    def test_arrow_batch_slicing(self):
        n = 100000

        df = self.spark.range(n).select((sf.col("id") % 2).alias("key"), sf.col("id").alias("v"))
        cols = {f"col_{i}": sf.col("v") + i for i in range(20)}
        df = df.withColumns(cols)

        def min_max_v(pdf):
            assert len(pdf) == n / 2, len(pdf)
            return pd.DataFrame(
                {
                    "key": [pdf.key.iloc[0]],
                    "min": [pdf.v.min()],
                    "max": [pdf.v.max()],
                }
            )

        expected = (
            df.groupby("key").agg(sf.min("v").alias("min"), sf.max("v").alias("max")).sort("key")
        ).collect()

        for maxRecords, maxBytes in [(1000, 2**31 - 1), (0, 4096), (1000, 4096)]:
            with self.subTest(maxRecords=maxRecords, maxBytes=maxBytes):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.maxRecordsPerBatch": maxRecords,
                        "spark.sql.execution.arrow.maxBytesPerBatch": maxBytes,
                    }
                ):
                    result = (
                        df.groupBy("key")
                        .applyInPandas(min_max_v, "key long, min long, max long")
                        .sort("key")
                    ).collect()

                    self.assertEqual(expected, result)

    def test_negative_and_zero_batch_size(self):
        for batch_size in [0, -1]:
            with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": batch_size}):
                ApplyInPandasTestsMixin.test_complex_groupby(self)

    @unittest.skipIf(is_remote_only(), "Requires JVM access")
    def test_apply_in_pandas_with_logging(self):
        import pandas as pd

        def func_with_logging(pdf):
            assert isinstance(pdf, pd.DataFrame)
            logger = logging.getLogger("test_pandas_grouped_map")
            logger.warning(
                f"pandas grouped map: {dict(id=list(pdf['id']), value=list(pdf['value']))}"
            )
            return pdf

        df = self.spark.range(9).withColumn("value", sf.col("id") * 10)
        grouped_df = df.groupBy((sf.col("id") % 2).cast("int"))

        with self.sql_conf({"spark.sql.pyspark.worker.logging.enabled": "true"}):
            assertDataFrameEqual(
                grouped_df.applyInPandas(func_with_logging, "id long, value long"),
                df,
            )

            logs = self.spark.tvf.python_worker_logs()

            assertDataFrameEqual(
                logs.select("level", "msg", "context", "logger"),
                [
                    Row(
                        level="WARNING",
                        msg=f"pandas grouped map: {dict(id=lst, value=[v*10 for v in lst])}",
                        context={"func_name": func_with_logging.__name__},
                        logger="test_pandas_grouped_map",
                    )
                    for lst in [[0, 2, 4, 6, 8], [1, 3, 5, 7]]
                ],
            )

    def test_apply_in_pandas_iterator_basic(self):
        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def sum_func(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            total = 0
            for batch in batches:
                total += batch["v"].sum()
            yield pd.DataFrame({"v": [total]})

        result = df.groupby("id").applyInPandas(sum_func, schema="v double").orderBy("v").collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 3.0)
        self.assertEqual(result[1][0], 18.0)

    def test_apply_in_pandas_iterator_with_keys(self):
        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def sum_func(
            key: Tuple[Any, ...], batches: Iterator[pd.DataFrame]
        ) -> Iterator[pd.DataFrame]:
            total = 0
            for batch in batches:
                total += batch["v"].sum()
            yield pd.DataFrame({"id": [key[0]], "v": [total]})

        result = (
            df.groupby("id")
            .applyInPandas(sum_func, schema="id long, v double")
            .orderBy("id")
            .collect()
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[0][1], 3.0)
        self.assertEqual(result[1][0], 2)
        self.assertEqual(result[1][1], 18.0)

    def test_apply_in_pandas_iterator_batch_slicing(self):
        df = self.spark.range(100000).select(
            (sf.col("id") % 2).alias("key"), sf.col("id").alias("v")
        )
        cols = {f"col_{i}": sf.col("v") + i for i in range(20)}
        df = df.withColumns(cols)

        def min_max_v(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            # Collect all batches to compute min/max across the entire group
            all_data = []
            key_val = None
            for batch in batches:
                all_data.append(batch)
                if key_val is None:
                    key_val = batch.key.iloc[0]

            combined = pd.concat(all_data, ignore_index=True)
            assert len(combined) == 100000 / 2, len(combined)

            yield pd.DataFrame(
                {
                    "key": [key_val],
                    "min": [combined.v.min()],
                    "max": [combined.v.max()],
                }
            )

        expected = (
            df.groupby("key")
            .agg(
                sf.min("v").alias("min"),
                sf.max("v").alias("max"),
            )
            .sort("key")
        ).collect()

        for maxRecords, maxBytes in [(1000, 2**31 - 1), (0, 4096), (1000, 4096)]:
            with self.subTest(maxRecords=maxRecords, maxBytes=maxBytes):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.maxRecordsPerBatch": maxRecords,
                        "spark.sql.execution.arrow.maxBytesPerBatch": maxBytes,
                    }
                ):
                    result = (
                        df.groupBy("key")
                        .applyInPandas(min_max_v, "key long, min long, max long")
                        .sort("key")
                    ).collect()

                    self.assertEqual(expected, result)

    def test_apply_in_pandas_iterator_with_keys_batch_slicing(self):
        df = self.spark.range(100000).select(
            (sf.col("id") % 2).alias("key"), sf.col("id").alias("v")
        )
        cols = {f"col_{i}": sf.col("v") + i for i in range(20)}
        df = df.withColumns(cols)

        def min_max_v(
            key: Tuple[Any, ...], batches: Iterator[pd.DataFrame]
        ) -> Iterator[pd.DataFrame]:
            # Collect all batches to compute min/max across the entire group
            all_data = []
            for batch in batches:
                all_data.append(batch)

            combined = pd.concat(all_data, ignore_index=True)
            assert len(combined) == 100000 / 2, len(combined)

            yield pd.DataFrame(
                {
                    "key": [key[0]],
                    "min": [combined.v.min()],
                    "max": [combined.v.max()],
                }
            )

        expected = (
            df.groupby("key").agg(sf.min("v").alias("min"), sf.max("v").alias("max")).sort("key")
        ).collect()

        for maxRecords, maxBytes in [(1000, 2**31 - 1), (0, 4096), (1000, 4096)]:
            with self.subTest(maxRecords=maxRecords, maxBytes=maxBytes):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.maxRecordsPerBatch": maxRecords,
                        "spark.sql.execution.arrow.maxBytesPerBatch": maxBytes,
                    }
                ):
                    result = (
                        df.groupBy("key")
                        .applyInPandas(min_max_v, "key long, min long, max long")
                        .sort("key")
                    ).collect()

                    self.assertEqual(expected, result)

    def test_apply_in_pandas_iterator_multiple_output_batches(self):
        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0)], ("id", "v")
        )

        def split_and_yield(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            # Yield multiple output batches for each input batch
            for batch in batches:
                for _, row in batch.iterrows():
                    # Yield each row as a separate batch to test multiple yields
                    yield pd.DataFrame(
                        {"id": [row["id"]], "v": [row["v"]], "v_doubled": [row["v"] * 2]}
                    )

        result = (
            df.groupby("id")
            .applyInPandas(split_and_yield, schema="id long, v double, v_doubled double")
            .orderBy("id", "v")
            .collect()
        )

        expected = [
            Row(id=1, v=1.0, v_doubled=2.0),
            Row(id=1, v=2.0, v_doubled=4.0),
            Row(id=1, v=3.0, v_doubled=6.0),
            Row(id=2, v=4.0, v_doubled=8.0),
            Row(id=2, v=5.0, v_doubled=10.0),
            Row(id=2, v=6.0, v_doubled=12.0),
        ]
        self.assertEqual(result, expected)

    def test_apply_in_pandas_iterator_filter_multiple_batches(self):
        df = self.spark.createDataFrame(
            [(1, i * 1.0) for i in range(20)] + [(2, i * 1.0) for i in range(20)], ("id", "v")
        )

        def filter_and_yield(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            # Yield filtered results from each batch
            for batch in batches:
                # Filter even values and yield
                even_batch = batch[batch["v"] % 2 == 0]
                if not even_batch.empty:
                    yield even_batch

                # Filter odd values and yield separately
                odd_batch = batch[batch["v"] % 2 == 1]
                if not odd_batch.empty:
                    yield odd_batch

        result = (
            df.groupby("id")
            .applyInPandas(filter_and_yield, schema="id long, v double")
            .orderBy("id", "v")
            .collect()
        )

        # Verify all 40 rows are present (20 per group)
        self.assertEqual(len(result), 40)

        # Verify group 1 has all values 0-19
        group1 = [row for row in result if row[0] == 1]
        self.assertEqual(len(group1), 20)
        self.assertEqual([row[1] for row in group1], [float(i) for i in range(20)])

        # Verify group 2 has all values 0-19
        group2 = [row for row in result if row[0] == 2]
        self.assertEqual(len(group2), 20)
        self.assertEqual([row[1] for row in group2], [float(i) for i in range(20)])

    def test_apply_in_pandas_iterator_with_keys_multiple_batches(self):
        df = self.spark.createDataFrame(
            [
                (1, "a", 1.0),
                (1, "b", 2.0),
                (1, "c", 3.0),
                (2, "d", 4.0),
                (2, "e", 5.0),
                (2, "f", 6.0),
            ],
            ("id", "name", "v"),
        )

        def process_with_key(
            key: Tuple[Any, ...], batches: Iterator[pd.DataFrame]
        ) -> Iterator[pd.DataFrame]:
            # Yield multiple processed batches, including the key in each output
            for batch in batches:
                # Split batch and yield multiple output batches
                for chunk_size in [1, 2]:
                    for i in range(0, len(batch), chunk_size):
                        chunk = batch.iloc[i : i + chunk_size]
                        if not chunk.empty:
                            result = chunk.assign(id=key[0], total=chunk["v"].sum())
                            yield result[["id", "name", "total"]]

        result = (
            df.groupby("id")
            .applyInPandas(process_with_key, schema="id long, name string, total double")
            .orderBy("id", "name")
            .collect()
        )

        # Verify we get results (may have duplicates due to splitting)
        self.assertTrue(len(result) > 6)

        # Verify all original names are present
        names = [row[1] for row in result]
        self.assertIn("a", names)
        self.assertIn("b", names)
        self.assertIn("c", names)
        self.assertIn("d", names)
        self.assertIn("e", names)
        self.assertIn("f", names)

        # Verify keys are correct
        for row in result:
            if row[1] in ["a", "b", "c"]:
                self.assertEqual(row[0], 1)
            else:
                self.assertEqual(row[0], 2)

    def test_apply_in_pandas_iterator_process_multiple_input_batches(self):
        # Create large dataset to trigger batch slicing
        df = self.spark.range(100000).select(
            (sf.col("id") % 2).alias("key"), sf.col("id").alias("v")
        )

        def process_batches_progressively(
            batches: Iterator[pd.DataFrame],
        ) -> Iterator[pd.DataFrame]:
            # Process each input batch and yield output immediately
            batch_count = 0
            for batch in batches:
                batch_count += 1
                # Yield a summary for each input batch processed
                yield pd.DataFrame(
                    {
                        "key": [batch.key.iloc[0]],
                        "batch_num": [batch_count],
                        "count": [len(batch)],
                        "sum": [batch.v.sum()],
                    }
                )

        # Use small batch size to force multiple input batches
        with self.sql_conf(
            {
                "spark.sql.execution.arrow.maxRecordsPerBatch": 10000,
            }
        ):
            result = (
                df.groupBy("key")
                .applyInPandas(
                    process_batches_progressively,
                    schema="key long, batch_num long, count long, sum long",
                )
                .orderBy("key", "batch_num")
                .collect()
            )

        # Verify we got multiple batches per group (100000/2 = 50000 rows per group)
        # With maxRecordsPerBatch=10000, should get 5 batches per group
        group_0_batches = [r for r in result if r[0] == 0]
        group_1_batches = [r for r in result if r[0] == 1]

        # Verify multiple batches were processed
        self.assertGreater(len(group_0_batches), 1)
        self.assertGreater(len(group_1_batches), 1)

        # Verify the sum across all batches equals expected total (using Python's built-in sum)
        group_0_sum = sum(r[3] for r in group_0_batches)
        group_1_sum = sum(r[3] for r in group_1_batches)

        # Expected: sum of even numbers 0,2,4,...,99998
        expected_even_sum = sum(range(0, 100000, 2))
        expected_odd_sum = sum(range(1, 100000, 2))

        self.assertEqual(group_0_sum, expected_even_sum)
        self.assertEqual(group_1_sum, expected_odd_sum)

    def test_apply_in_pandas_iterator_streaming_aggregation(self):
        # Create dataset with multiple batches per group
        df = self.spark.range(50000).select(
            (sf.col("id") % 3).alias("key"),
            (sf.col("id") % 100).alias("category"),
            sf.col("id").alias("value"),
        )

        def streaming_aggregate(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            # Maintain running aggregates and yield intermediate results
            running_sum = 0
            running_count = 0

            for batch in batches:
                # Update running aggregates
                running_sum += batch.value.sum()
                running_count += len(batch)

                # Yield current stats after processing each batch
                yield pd.DataFrame(
                    {
                        "key": [batch.key.iloc[0]],
                        "running_count": [running_count],
                        "running_avg": [running_sum / running_count],
                    }
                )

        # Force multiple batches with small batch size
        with self.sql_conf(
            {
                "spark.sql.execution.arrow.maxRecordsPerBatch": 5000,
            }
        ):
            result = (
                df.groupBy("key")
                .applyInPandas(
                    streaming_aggregate, schema="key long, running_count long, running_avg double"
                )
                .collect()
            )

        # Verify we got multiple rows per group (one per input batch)
        for key_val in [0, 1, 2]:
            key_results = [r for r in result if r[0] == key_val]
            # Should have multiple batches
            # (50000/3 ≈ 16667 rows per group, with 5000 per batch = ~4 batches)
            self.assertGreater(len(key_results), 1, f"Expected multiple batches for key {key_val}")

            # Verify running_count increases monotonically
            counts = [r[1] for r in key_results]
            for i in range(1, len(counts)):
                self.assertGreater(
                    counts[i], counts[i - 1], "Running count should increase with each batch"
                )

    def test_apply_in_pandas_iterator_partial_iteration(self):
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 2}):

            def func(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
                # Only consume the first batch from the iterator
                first = next(batches)
                yield pd.DataFrame({"value": first["id"] % 4})

            df = self.spark.range(20)
            grouped_df = df.groupBy((sf.col("id") % 4).cast("int"))

            # Should get two records for each group (first batch only)
            expected = [Row(value=x) for x in [0, 0, 1, 1, 2, 2, 3, 3]]

            actual = grouped_df.applyInPandas(func, "value long").collect()
            self.assertEqual(actual, expected)

    def test_grouped_map_pandas_udf_with_compression_codec(self):
        # Test grouped map Pandas UDF with different compression codec settings
        @pandas_udf("id long, v int, v1 double", PandasUDFType.GROUPED_MAP)
        def foo(pdf):
            return pdf.assign(v1=pdf.v * pdf.id * 1.0)

        df = self.data
        pdf = df.toPandas()
        expected = pdf.groupby("id", as_index=False).apply(foo.func).reset_index(drop=True)

        for codec in ["none", "zstd", "lz4"]:
            with self.subTest(compressionCodec=codec):
                with self.sql_conf({"spark.sql.execution.arrow.compression.codec": codec}):
                    result = df.groupby("id").apply(foo).sort("id").toPandas()
                    assert_frame_equal(expected, result)

    def test_apply_in_pandas_with_compression_codec(self):
        # Test applyInPandas with different compression codec settings
        def stats(key, pdf):
            return pd.DataFrame([(key[0], pdf.v.mean())], columns=["id", "mean"])

        df = self.data
        expected = df.select("id").distinct().withColumn("mean", sf.lit(24.5)).toPandas()

        for codec in ["none", "zstd", "lz4"]:
            with self.subTest(compressionCodec=codec):
                with self.sql_conf({"spark.sql.execution.arrow.compression.codec": codec}):
                    result = (
                        df.groupby("id")
                        .applyInPandas(stats, schema="id long, mean double")
                        .sort("id")
                        .toPandas()
                    )
                    assert_frame_equal(expected, result)

    def test_apply_in_pandas_iterator_with_compression_codec(self):
        # Test applyInPandas with iterator and compression
        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def sum_func(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            total = 0
            for batch in batches:
                total += batch["v"].sum()
            yield pd.DataFrame({"v": [total]})

        expected = [Row(v=3.0), Row(v=18.0)]

        for codec in ["none", "zstd", "lz4"]:
            with self.subTest(compressionCodec=codec):
                with self.sql_conf({"spark.sql.execution.arrow.compression.codec": codec}):
                    result = (
                        df.groupby("id")
                        .applyInPandas(sum_func, schema="v double")
                        .orderBy("v")
                        .collect()
                    )
                    self.assertEqual(result, expected)


class ApplyInPandasTests(ApplyInPandasTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_grouped_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
