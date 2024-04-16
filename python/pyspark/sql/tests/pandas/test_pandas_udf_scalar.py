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
import os
import random
import shutil
import tempfile
import time
import unittest
from datetime import date, datetime
from decimal import Decimal
from typing import cast

from pyspark import TaskContext
from pyspark.util import PythonEvalType
from pyspark.sql import Column
from pyspark.sql.functions import array, col, expr, lit, sum, struct, udf, pandas_udf, PandasUDFType
from pyspark.sql.types import (
    IntegerType,
    ByteType,
    StructType,
    ShortType,
    BooleanType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    ArrayType,
    StructField,
    Row,
    TimestampType,
    MapType,
    DateType,
    BinaryType,
    YearMonthIntervalType,
)
from pyspark.errors import AnalysisException, PythonException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    test_compiled,
    test_not_compiled_message,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import assertDataFrameEqual

if have_pandas:
    import pandas as pd

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class ScalarPandasUDFTestsMixin:
    @property
    def nondeterministic_vectorized_udf(self):
        import numpy as np

        @pandas_udf("double")
        def random_udf(v):
            return pd.Series(np.random.random(len(v)))

        random_udf = random_udf.asNondeterministic()
        return random_udf

    @property
    def nondeterministic_vectorized_iter_udf(self):
        import numpy as np

        @pandas_udf("double", PandasUDFType.SCALAR_ITER)
        def random_udf(it):
            for v in it:
                yield pd.Series(np.random.random(len(v)))

        random_udf = random_udf.asNondeterministic()
        return random_udf

    @property
    def df_with_nested_structs(self):
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField(
                    "info",
                    StructType(
                        [
                            StructField("name", StringType(), False),
                            StructField("age", IntegerType(), False),
                            StructField(
                                "details",
                                StructType(
                                    [
                                        StructField("field1", StringType(), False),
                                        StructField("field2", IntegerType(), False),
                                    ]
                                ),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
            ]
        )
        data = [(1, ("John", 30, ("Value1", 10)))]
        df = self.spark.createDataFrame(data, schema)
        struct_df = df.select(struct(df.columns).alias("struct"))
        # struct_df.dtype:
        # [(
        #   'struct',
        #   'struct<id:int,info:
        #     struct<name:string,age:int,details:
        #       struct<field1:string, field2:int>>>'
        # )]
        return struct_df

    @property
    def df_with_nested_maps(self):
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "attributes", MapType(StringType(), MapType(StringType(), StringType())), True
                ),
            ]
        )
        data = [("1", {"personal": {"name": "John", "city": "New York"}})]
        return self.spark.createDataFrame(data, schema)

    @property
    def df_with_nested_arrays(self):
        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("nested_array", ArrayType(ArrayType(IntegerType())), nullable=False),
            ]
        )
        data = [(1, [[1, 2, 3], [4, 5]])]
        return self.spark.createDataFrame(data, schema)

    def test_pandas_udf_tokenize(self):
        tokenize = pandas_udf(
            lambda s: s.apply(lambda str: str.split(" ")), ArrayType(StringType())
        )
        self.assertEqual(tokenize.returnType, ArrayType(StringType()))
        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual([Row(hi=["hi", "boo"]), Row(hi=["bye", "boo"])], result.collect())

    def test_pandas_udf_nested_arrays(self):
        tokenize = pandas_udf(
            lambda s: s.apply(lambda str: [str.split(" ")]), ArrayType(ArrayType(StringType()))
        )
        self.assertEqual(tokenize.returnType, ArrayType(ArrayType(StringType())))
        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual([Row(hi=[["hi", "boo"]]), Row(hi=[["bye", "boo"]])], result.collect())

    def test_input_nested_structs(self):
        df = self.df_with_nested_structs

        mirror = pandas_udf(lambda s: s, df.dtypes[0][1])

        self.assertEqual(
            df.select(mirror(df.struct).alias("res")).first(),
            Row(
                res=Row(
                    id=1, info=Row(name="John", age=30, details=Row(field1="Value1", field2=10))
                )
            ),
        )

    def test_input_nested_maps(self):
        df = self.df_with_nested_maps

        str_repr = pandas_udf(lambda s: s.astype(str), StringType())
        self.assertEqual(
            df.select(str_repr(df.attributes).alias("res")).first(),
            Row(res="{'personal': {'name': 'John', 'city': 'New York'}}"),
        )

        extract_name = pandas_udf(lambda s: s.apply(lambda x: x["personal"]["name"]), StringType())
        self.assertEqual(
            df.select(extract_name(df.attributes).alias("res")).first(),
            Row(res="John"),
        )

    def test_input_nested_arrays(self):
        df = self.df_with_nested_arrays

        str_repr = pandas_udf(lambda s: s.astype(str), StringType())
        self.assertEqual(
            df.select(str_repr(df.nested_array).alias("res")).first(),
            Row(res="[array([1, 2, 3], dtype=int32) array([4, 5], dtype=int32)]"),
        )

    def test_pandas_array_struct(self):
        # SPARK-38098: Support Array of Struct for Pandas UDFs and toPandas
        import numpy as np

        @pandas_udf("Array<struct<col1:string, col2:long, col3:double>>")
        def return_cols(cols):
            assert type(cols) == pd.Series
            assert type(cols[0]) == np.ndarray
            assert type(cols[0][0]) == dict
            return cols

        df = self.spark.createDataFrame(
            [[[("a", 2, 3.0), ("a", 2, 3.0)]], [[("b", 5, 6.0), ("b", 5, 6.0)]]],
            "array_struct_col Array<struct<col1:string, col2:long, col3:double>>",
        )
        result = df.select(return_cols("array_struct_col"))
        self.assertEqual(
            [
                Row(output=[Row(col1="a", col2=2, col3=3.0), Row(col1="a", col2=2, col3=3.0)]),
                Row(output=[Row(col1="b", col2=5, col3=6.0), Row(col1="b", col2=5, col3=6.0)]),
            ],
            result.collect(),
        )

    def test_vectorized_udf_basic(self):
        df = self.spark.range(10).select(
            col("id").cast("string").alias("str"),
            col("id").cast("int").alias("int"),
            col("id").alias("long"),
            col("id").cast("float").alias("float"),
            col("id").cast("double").alias("double"),
            col("id").cast("decimal").alias("decimal"),
            col("id").cast("boolean").alias("bool"),
            array(col("id")).alias("array_long"),
        )

        def f(x):
            return x

        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            str_f = pandas_udf(f, StringType(), udf_type)
            int_f = pandas_udf(f, IntegerType(), udf_type)
            long_f = pandas_udf(f, LongType(), udf_type)
            float_f = pandas_udf(f, FloatType(), udf_type)
            double_f = pandas_udf(f, DoubleType(), udf_type)
            decimal_f = pandas_udf(f, DecimalType(), udf_type)
            bool_f = pandas_udf(f, BooleanType(), udf_type)
            array_long_f = pandas_udf(f, ArrayType(LongType()), udf_type)
            res = df.select(
                str_f(col("str")),
                int_f(col("int")),
                long_f(col("long")),
                float_f(col("float")),
                double_f(col("double")),
                decimal_f("decimal"),
                bool_f(col("bool")),
                array_long_f("array_long"),
            )
            self.assertEqual(df.collect(), res.collect())

    def test_register_nondeterministic_vectorized_udf_basic(self):
        random_pandas_udf = pandas_udf(
            lambda x: random.randint(6, 6) + x, IntegerType()
        ).asNondeterministic()
        self.assertEqual(random_pandas_udf.deterministic, False)
        self.assertEqual(random_pandas_udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
        nondeterministic_pandas_udf = self.spark.catalog.registerFunction(
            "randomPandasUDF", random_pandas_udf
        )
        self.assertEqual(nondeterministic_pandas_udf.deterministic, False)
        self.assertEqual(nondeterministic_pandas_udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
        [row] = self.spark.sql("SELECT randomPandasUDF(1)").collect()
        self.assertEqual(row[0], 7)

        def random_iter_udf(it):
            for i in it:
                yield random.randint(6, 6) + i

        random_pandas_iter_udf = pandas_udf(
            random_iter_udf, IntegerType(), PandasUDFType.SCALAR_ITER
        ).asNondeterministic()
        self.assertEqual(random_pandas_iter_udf.deterministic, False)
        self.assertEqual(random_pandas_iter_udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)
        nondeterministic_pandas_iter_udf = self.spark.catalog.registerFunction(
            "randomPandasIterUDF", random_pandas_iter_udf
        )
        self.assertEqual(nondeterministic_pandas_iter_udf.deterministic, False)
        self.assertEqual(
            nondeterministic_pandas_iter_udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
        )
        [row] = self.spark.sql("SELECT randomPandasIterUDF(1)").collect()
        self.assertEqual(row[0], 7)

    def test_vectorized_udf_null_boolean(self):
        data = [(True,), (True,), (None,), (False,)]
        schema = StructType().add("bool", BooleanType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            bool_f = pandas_udf(lambda x: x, BooleanType(), udf_type)
            res = df.select(bool_f(col("bool")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_byte(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("byte", ByteType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            byte_f = pandas_udf(lambda x: x, ByteType(), udf_type)
            res = df.select(byte_f(col("byte")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_short(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("short", ShortType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            short_f = pandas_udf(lambda x: x, ShortType(), udf_type)
            res = df.select(short_f(col("short")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_int(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("int", IntegerType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            int_f = pandas_udf(lambda x: x, IntegerType(), udf_type)
            res = df.select(int_f(col("int")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_long(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("long", LongType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            long_f = pandas_udf(lambda x: x, LongType(), udf_type)
            res = df.select(long_f(col("long")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_float(self):
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("float", FloatType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            float_f = pandas_udf(lambda x: x, FloatType(), udf_type)
            res = df.select(float_f(col("float")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_double(self):
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("double", DoubleType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            double_f = pandas_udf(lambda x: x, DoubleType(), udf_type)
            res = df.select(double_f(col("double")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_decimal(self):
        data = [(Decimal(3.0),), (Decimal(5.0),), (Decimal(-1.0),), (None,)]
        schema = StructType().add("decimal", DecimalType(38, 18))
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            decimal_f = pandas_udf(lambda x: x, DecimalType(38, 18), udf_type)
            res = df.select(decimal_f(col("decimal")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_string(self):
        data = [("foo",), (None,), ("bar",), ("bar",)]
        schema = StructType().add("str", StringType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            str_f = pandas_udf(lambda x: x, StringType(), udf_type)
            res = df.select(str_f(col("str")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_string_in_udf(self):
        df = self.spark.range(10)

        def scalar_f(x):
            return pd.Series(map(str, x))

        def iter_f(it):
            for i in it:
                yield scalar_f(i)

        for f, udf_type in [(scalar_f, PandasUDFType.SCALAR), (iter_f, PandasUDFType.SCALAR_ITER)]:
            str_f = pandas_udf(f, StringType(), udf_type)
            actual = df.select(str_f(col("id")))
            expected = df.select(col("id").cast("string"))
            self.assertEqual(expected.collect(), actual.collect())

    def test_vectorized_udf_datatype_string(self):
        df = self.spark.range(10).select(
            col("id").cast("string").alias("str"),
            col("id").cast("int").alias("int"),
            col("id").alias("long"),
            col("id").cast("float").alias("float"),
            col("id").cast("double").alias("double"),
            col("id").cast("decimal").alias("decimal"),
            col("id").cast("boolean").alias("bool"),
        )

        def f(x):
            return x

        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            str_f = pandas_udf(f, "string", udf_type)
            int_f = pandas_udf(f, "integer", udf_type)
            long_f = pandas_udf(f, "long", udf_type)
            float_f = pandas_udf(f, "float", udf_type)
            double_f = pandas_udf(f, "double", udf_type)
            decimal_f = pandas_udf(f, "decimal(38, 18)", udf_type)
            bool_f = pandas_udf(f, "boolean", udf_type)
            res = df.select(
                str_f(col("str")),
                int_f(col("int")),
                long_f(col("long")),
                float_f(col("float")),
                double_f(col("double")),
                decimal_f("decimal"),
                bool_f(col("bool")),
            )
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_null_binary(self):
        data = [(bytearray(b"a"),), (None,), (bytearray(b"bb"),), (bytearray(b"ccc"),)]
        schema = StructType().add("binary", BinaryType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            str_f = pandas_udf(lambda x: x, BinaryType(), udf_type)
            res = df.select(str_f(col("binary")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_array_type(self):
        data = [([1, 2],), ([3, 4],)]
        array_schema = StructType([StructField("array", ArrayType(IntegerType()))])
        df = self.spark.createDataFrame(data, schema=array_schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            array_f = pandas_udf(lambda x: x, ArrayType(IntegerType()), udf_type)
            result = df.select(array_f(col("array")))
            self.assertEqual(df.collect(), result.collect())

    def test_vectorized_udf_null_array(self):
        data = [([1, 2],), (None,), (None,), ([3, 4],), (None,)]
        array_schema = StructType([StructField("array", ArrayType(IntegerType()))])
        df = self.spark.createDataFrame(data, schema=array_schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            array_f = pandas_udf(lambda x: x, ArrayType(IntegerType()), udf_type)
            result = df.select(array_f(col("array")))
            self.assertEqual(df.collect(), result.collect())

    def test_vectorized_udf_struct_type(self):
        df = self.spark.range(10)
        return_type = StructType([StructField("id", LongType()), StructField("str", StringType())])

        def scalar_func(id):
            return pd.DataFrame({"id": id, "str": id.apply(str)})

        def iter_func(it):
            for id in it:
                yield scalar_func(id)

        for func, udf_type in [
            (scalar_func, PandasUDFType.SCALAR),
            (iter_func, PandasUDFType.SCALAR_ITER),
        ]:
            f = pandas_udf(func, returnType=return_type, functionType=udf_type)

            expected = df.select(
                struct(col("id"), col("id").cast("string").alias("str")).alias("struct")
            ).collect()

            actual = df.select(f(col("id")).alias("struct")).collect()
            self.assertEqual(expected, actual)

            g = pandas_udf(func, "id: long, str: string", functionType=udf_type)
            actual = df.select(g(col("id")).alias("struct")).collect()
            self.assertEqual(expected, actual)

            struct_f = pandas_udf(lambda x: x, return_type, functionType=udf_type)
            actual = df.select(struct_f(struct(col("id"), col("id").cast("string").alias("str"))))
            self.assertEqual(expected, actual.collect())

    def test_vectorized_udf_struct_complex(self):
        df = self.spark.range(10)
        return_type = StructType(
            [StructField("ts", TimestampType()), StructField("arr", ArrayType(LongType()))]
        )

        def _scalar_f(id):
            return pd.DataFrame(
                {"ts": id.apply(lambda i: pd.Timestamp(i)), "arr": id.apply(lambda i: [i, i + 1])}
            )

        scalar_f = pandas_udf(_scalar_f, returnType=return_type)

        @pandas_udf(returnType=return_type, functionType=PandasUDFType.SCALAR_ITER)
        def iter_f(it):
            for id in it:
                yield _scalar_f(id)

        for f, udf_type in [(scalar_f, PandasUDFType.SCALAR), (iter_f, PandasUDFType.SCALAR_ITER)]:
            actual = df.withColumn("f", f(col("id"))).collect()
            for i, row in enumerate(actual):
                id, f = row
                self.assertEqual(i, id)
                self.assertEqual(pd.Timestamp(i).to_pydatetime(), f[0])
                self.assertListEqual([i, i + 1], f[1])

    def test_vectorized_udf_struct_empty(self):
        df = self.spark.range(3)
        return_type = StructType()

        def _scalar_f(id):
            return pd.DataFrame(index=id)

        scalar_f = pandas_udf(_scalar_f, returnType=return_type)

        @pandas_udf(returnType=return_type, functionType=PandasUDFType.SCALAR_ITER)
        def iter_f(it):
            for id in it:
                yield _scalar_f(id)

        for f, udf_type in [(scalar_f, "SCALAR"), (iter_f, "SCALAR_ITER")]:
            with self.subTest(udf_type=udf_type):
                assertDataFrameEqual(
                    df.withColumn("f", f(col("id"))),
                    [Row(id=0, f=Row()), Row(id=1, f=Row()), Row(id=2, f=Row())],
                )

    def test_vectorized_udf_nested_struct(self):
        with self.quiet():
            self.check_vectorized_udf_nested_struct()

    def check_vectorized_udf_nested_struct(self):
        df = self.spark.range(2)

        nested_type = StructType(
            [
                StructField("id", IntegerType()),
                StructField(
                    "nested",
                    StructType([StructField("foo", StringType()), StructField("bar", FloatType())]),
                ),
            ]
        )

        def func_dict(pser: pd.Series) -> pd.DataFrame:
            return pd.DataFrame(
                {"id": pser, "nested": pser.apply(lambda x: {"foo": str(x), "bar": float(x)})}
            )

        def func_row(pser: pd.Series) -> pd.DataFrame:
            return pd.DataFrame(
                {"id": pser, "nested": pser.apply(lambda x: Row(foo=str(x), bar=float(x)))}
            )

        expected = [
            Row(udf=Row(id=0, nested=Row(foo="0", bar=0.0))),
            Row(udf=Row(id=1, nested=Row(foo="1", bar=1.0))),
        ]

        for f in [func_dict, func_row]:
            for udf_type, func in [
                (PandasUDFType.SCALAR, f),
                (PandasUDFType.SCALAR_ITER, lambda iter: (f(pser) for pser in iter)),
            ]:
                with self.subTest(udf_type=udf_type, udf=f.__name__):
                    result = df.select(
                        pandas_udf(func, returnType=nested_type, functionType=udf_type)(
                            col("id")
                        ).alias("udf")
                    ).collect()
                    self.assertEqual(result, expected)

    def test_vectorized_udf_map_type(self):
        data = [({},), ({"a": 1},), ({"a": 1, "b": 2},), ({"a": 1, "b": 2, "c": 3},)]
        schema = StructType([StructField("map", MapType(StringType(), LongType()))])
        df = self.spark.createDataFrame(data, schema=schema)
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            map_f = pandas_udf(lambda x: x, MapType(StringType(), LongType()), udf_type)
            result = df.select(map_f(col("map")))
            self.assertEqual(df.collect(), result.collect())

    def test_vectorized_udf_complex(self):
        df = self.spark.range(10).select(
            col("id").cast("int").alias("a"),
            col("id").cast("int").alias("b"),
            col("id").cast("double").alias("c"),
        )
        scalar_add = pandas_udf(lambda x, y: x + y, IntegerType())
        scalar_power2 = pandas_udf(lambda x: 2**x, IntegerType())
        scalar_mul = pandas_udf(lambda x, y: x * y, DoubleType())

        @pandas_udf(IntegerType(), PandasUDFType.SCALAR_ITER)
        def iter_add(it):
            for x, y in it:
                yield x + y

        @pandas_udf(IntegerType(), PandasUDFType.SCALAR_ITER)
        def iter_power2(it):
            for x in it:
                yield 2**x

        @pandas_udf(DoubleType(), PandasUDFType.SCALAR_ITER)
        def iter_mul(it):
            for x, y in it:
                yield x * y

        for add, power2, mul in [
            (scalar_add, scalar_power2, scalar_mul),
            (iter_add, iter_power2, iter_mul),
        ]:
            res = df.select(add(col("a"), col("b")), power2(col("a")), mul(col("b"), col("c")))
            expected = df.select(expr("a + b"), expr("power(2, a)"), expr("b * c"))
            self.assertEqual(expected.collect(), res.collect())

    def test_vectorized_udf_exception(self):
        with self.quiet():
            self.check_vectorized_udf_exception()

    def check_vectorized_udf_exception(self):
        df = self.spark.range(10)
        scalar_raise_exception = pandas_udf(lambda x: x * (1 / 0), LongType())

        @pandas_udf(LongType(), PandasUDFType.SCALAR_ITER)
        def iter_raise_exception(it):
            for x in it:
                yield x * (1 / 0)

        for raise_exception in [scalar_raise_exception, iter_raise_exception]:
            with self.assertRaisesRegex(Exception, "division( or modulo)? by zero"):
                df.select(raise_exception(col("id"))).collect()

    def test_vectorized_udf_invalid_length(self):
        with self.quiet():
            self.check_vectorized_udf_invalid_length()

    def check_vectorized_udf_invalid_length(self):
        df = self.spark.range(10)
        raise_exception = pandas_udf(lambda _: pd.Series(1), LongType())
        with self.assertRaisesRegex(
            Exception, "Result vector from pandas_udf was not the required length"
        ):
            df.select(raise_exception(col("id"))).collect()

        @pandas_udf(LongType(), PandasUDFType.SCALAR_ITER)
        def iter_udf_wong_output_size(it):
            for _ in it:
                yield pd.Series(1)

        with self.assertRaisesRegex(
            Exception, "The length of output in Scalar iterator.*" "the length of output was 1"
        ):
            df.select(iter_udf_wong_output_size(col("id"))).collect()

        @pandas_udf(LongType(), PandasUDFType.SCALAR_ITER)
        def iter_udf_not_reading_all_input(it):
            for batch in it:
                batch_len = len(batch)
                yield pd.Series([1] * batch_len)
                break

        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 3}):
            df1 = self.spark.range(10).repartition(1)
            with self.assertRaisesRegex(Exception, "pandas iterator UDF should exhaust"):
                df1.select(iter_udf_not_reading_all_input(col("id"))).collect()

    def test_vectorized_udf_chained(self):
        df = self.spark.range(10)
        scalar_f = pandas_udf(lambda x: x + 1, LongType())
        scalar_g = pandas_udf(lambda x: x - 1, LongType())

        iter_f = pandas_udf(
            lambda it: map(lambda x: x + 1, it), LongType(), PandasUDFType.SCALAR_ITER
        )
        iter_g = pandas_udf(
            lambda it: map(lambda x: x - 1, it), LongType(), PandasUDFType.SCALAR_ITER
        )

        for f, g in [(scalar_f, scalar_g), (iter_f, iter_g)]:
            res = df.select(g(f(col("id"))))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_chained_struct_type(self):
        df = self.spark.range(10)
        return_type = StructType([StructField("id", LongType()), StructField("str", StringType())])

        @pandas_udf(return_type)
        def scalar_f(id):
            return pd.DataFrame({"id": id, "str": id.apply(str)})

        scalar_g = pandas_udf(lambda x: x, return_type)

        @pandas_udf(return_type, PandasUDFType.SCALAR_ITER)
        def iter_f(it):
            for id in it:
                yield pd.DataFrame({"id": id, "str": id.apply(str)})

        iter_g = pandas_udf(lambda x: x, return_type, PandasUDFType.SCALAR_ITER)

        expected = df.select(
            struct(col("id"), col("id").cast("string").alias("str")).alias("struct")
        ).collect()

        for f, g in [(scalar_f, scalar_g), (iter_f, iter_g)]:
            actual = df.select(g(f(col("id"))).alias("struct")).collect()
            self.assertEqual(expected, actual)

    def test_vectorized_udf_wrong_return_type(self):
        with self.quiet():
            self.check_vectorized_udf_wrong_return_type()

    def check_vectorized_udf_wrong_return_type(self):
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            with self.assertRaisesRegex(
                NotImplementedError,
                "Invalid return type.*scalar Pandas UDF.*ArrayType.*YearMonthIntervalType",
            ):
                pandas_udf(lambda x: x, ArrayType(YearMonthIntervalType()), udf_type)

    def test_vectorized_udf_return_scalar(self):
        with self.quiet():
            self.check_vectorized_udf_return_scalar()

    def check_vectorized_udf_return_scalar(self):
        df = self.spark.range(10)
        scalar_f = pandas_udf(lambda x: 1.0, DoubleType())
        iter_f = pandas_udf(
            lambda it: map(lambda x: 1.0, it), DoubleType(), PandasUDFType.SCALAR_ITER
        )
        for f in [scalar_f, iter_f]:
            with self.assertRaisesRegex(Exception, "Return.*type.*Series"):
                df.select(f(col("id"))).collect()

    def test_vectorized_udf_decorator(self):
        df = self.spark.range(10)

        @pandas_udf(returnType=LongType())
        def scalar_identity(x):
            return x

        @pandas_udf(returnType=LongType(), functionType=PandasUDFType.SCALAR_ITER)
        def iter_identity(x):
            return x

        for identity in [scalar_identity, iter_identity]:
            res = df.select(identity(col("id")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_empty_partition(self):
        df = self.spark.createDataFrame(self.sc.parallelize([Row(id=1)], 2))
        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            f = pandas_udf(lambda x: x, LongType(), udf_type)
            res = df.select(f(col("id")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_struct_with_empty_partition(self):
        df = self.spark.createDataFrame(self.sc.parallelize([Row(id=1)], 2)).withColumn(
            "name", lit("John Doe")
        )

        @pandas_udf("first string, last string")
        def scalar_split_expand(n):
            return n.str.split(expand=True)

        @pandas_udf("first string, last string", PandasUDFType.SCALAR_ITER)
        def iter_split_expand(it):
            for n in it:
                yield n.str.split(expand=True)

        for split_expand in [scalar_split_expand, iter_split_expand]:
            result = df.select(split_expand("name")).collect()
            self.assertEqual(1, len(result))
            row = result[0]
            self.assertEqual("John", row[0]["first"])
            self.assertEqual("Doe", row[0]["last"])

    def test_vectorized_udf_varargs(self):
        df = self.spark.range(start=1, end=2)
        scalar_f = pandas_udf(lambda *v: v[0], LongType())

        @pandas_udf(LongType(), PandasUDFType.SCALAR_ITER)
        def iter_f(it):
            for v in it:
                yield v[0]

        for f in [scalar_f, iter_f]:
            res = df.select(f(col("id"), col("id")))
            self.assertEqual(df.collect(), res.collect())

    def test_vectorized_udf_dates(self):
        schema = StructType().add("idx", LongType()).add("date", DateType())
        data = [
            (
                0,
                date(1969, 1, 1),
            ),
            (
                1,
                date(2012, 2, 2),
            ),
            (
                2,
                None,
            ),
            (
                3,
                date(2100, 4, 4),
            ),
            (
                4,
                date(2262, 4, 12),
            ),
        ]
        df = self.spark.createDataFrame(data, schema=schema)

        def scalar_check_data(idx, date, date_copy):
            msgs = []
            is_equal = date.isnull()
            for i in range(len(idx)):
                if (is_equal[i] and data[idx[i]][1] is None) or date[i] == data[idx[i]][1]:
                    msgs.append(None)
                else:
                    msgs.append(
                        "date values are not equal (date='%s': data[%d][1]='%s')"
                        % (date[i], idx[i], data[idx[i]][1])
                    )
            return pd.Series(msgs)

        def iter_check_data(it):
            for idx, test_date, date_copy in it:
                yield scalar_check_data(idx, test_date, date_copy)

        pandas_scalar_check_data = pandas_udf(scalar_check_data, StringType())
        pandas_iter_check_data = pandas_udf(
            iter_check_data, StringType(), PandasUDFType.SCALAR_ITER
        )

        for check_data, udf_type in [
            (pandas_scalar_check_data, PandasUDFType.SCALAR),
            (pandas_iter_check_data, PandasUDFType.SCALAR_ITER),
        ]:
            date_copy = pandas_udf(lambda t: t, returnType=DateType(), functionType=udf_type)
            df = df.withColumn("date_copy", date_copy(col("date")))
            result = df.withColumn(
                "check_data", check_data(col("idx"), col("date"), col("date_copy"))
            ).collect()

            self.assertEqual(len(data), len(result))
            for i in range(len(result)):
                self.assertEqual(data[i][1], result[i][1])  # "date" col
                self.assertEqual(data[i][1], result[i][2])  # "date_copy" col
                self.assertIsNone(result[i][3])  # "check_data" col

    def test_vectorized_udf_timestamps(self):
        schema = StructType(
            [StructField("idx", LongType(), True), StructField("timestamp", TimestampType(), True)]
        )
        data = [
            (0, datetime(1969, 1, 1, 1, 1, 1)),
            (1, datetime(2012, 2, 2, 2, 2, 2)),
            (2, None),
            (3, datetime(2100, 3, 3, 3, 3, 3)),
        ]

        df = self.spark.createDataFrame(data, schema=schema)

        def scalar_check_data(idx, timestamp, timestamp_copy):
            msgs = []
            is_equal = timestamp.isnull()  # use this array to check values are equal
            for i in range(len(idx)):
                # Check that timestamps are as expected in the UDF
                if (is_equal[i] and data[idx[i]][1] is None) or timestamp[
                    i
                ].to_pydatetime() == data[idx[i]][1]:
                    msgs.append(None)
                else:
                    msgs.append(
                        "timestamp values are not equal (timestamp='%s': data[%d][1]='%s')"
                        % (timestamp[i], idx[i], data[idx[i]][1])
                    )
            return pd.Series(msgs)

        def iter_check_data(it):
            for idx, timestamp, timestamp_copy in it:
                yield scalar_check_data(idx, timestamp, timestamp_copy)

        pandas_scalar_check_data = pandas_udf(scalar_check_data, StringType())
        pandas_iter_check_data = pandas_udf(
            iter_check_data, StringType(), PandasUDFType.SCALAR_ITER
        )

        for check_data, udf_type in [
            (pandas_scalar_check_data, PandasUDFType.SCALAR),
            (pandas_iter_check_data, PandasUDFType.SCALAR_ITER),
        ]:
            # Check that a timestamp passed through a pandas_udf will not be altered by timezone
            # calc
            f_timestamp_copy = pandas_udf(
                lambda t: t, returnType=TimestampType(), functionType=udf_type
            )
            df = df.withColumn("timestamp_copy", f_timestamp_copy(col("timestamp")))
            result = df.withColumn(
                "check_data", check_data(col("idx"), col("timestamp"), col("timestamp_copy"))
            ).collect()
            # Check that collection values are correct
            self.assertEqual(len(data), len(result))
            for i in range(len(result)):
                self.assertEqual(data[i][1], result[i][1])  # "timestamp" col
                self.assertEqual(data[i][1], result[i][2])  # "timestamp_copy" col
                self.assertIsNone(result[i][3])  # "check_data" col

    def test_vectorized_udf_return_timestamp_tz(self):
        df = self.spark.range(10)

        @pandas_udf(returnType=TimestampType())
        def scalar_gen_timestamps(id):
            ts = [pd.Timestamp(i, unit="D", tz="America/Los_Angeles") for i in id]
            return pd.Series(ts)

        @pandas_udf(returnType=TimestampType(), functionType=PandasUDFType.SCALAR_ITER)
        def iter_gen_timestamps(it):
            for id in it:
                ts = [pd.Timestamp(i, unit="D", tz="America/Los_Angeles") for i in id]
                yield pd.Series(ts)

        for gen_timestamps in [scalar_gen_timestamps, iter_gen_timestamps]:
            result = df.withColumn("ts", gen_timestamps(col("id"))).collect()
            spark_ts_t = TimestampType()
            for r in result:
                i, ts = r
                ts_tz = pd.Timestamp(i, unit="D", tz="America/Los_Angeles").to_pydatetime()
                expected = spark_ts_t.fromInternal(spark_ts_t.toInternal(ts_tz))
                self.assertEqual(expected, ts)

    def test_vectorized_udf_check_config(self):
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 3}):
            df = self.spark.range(10, numPartitions=1)

            @pandas_udf(returnType=LongType())
            def scalar_check_records_per_batch(x):
                return pd.Series(x.size).repeat(x.size)

            @pandas_udf(returnType=LongType(), functionType=PandasUDFType.SCALAR_ITER)
            def iter_check_records_per_batch(it):
                for x in it:
                    yield pd.Series(x.size).repeat(x.size)

            for check_records_per_batch in [
                scalar_check_records_per_batch,
                iter_check_records_per_batch,
            ]:
                result = df.select(check_records_per_batch(col("id"))).collect()
                for (r,) in result:
                    self.assertTrue(r <= 3)

    def test_vectorized_udf_timestamps_respect_session_timezone(self):
        schema = StructType(
            [StructField("idx", LongType(), True), StructField("timestamp", TimestampType(), True)]
        )
        data = [
            (1, datetime(1969, 1, 1, 1, 1, 1)),
            (2, datetime(2012, 2, 2, 2, 2, 2)),
            (3, None),
            (4, datetime(2100, 3, 3, 3, 3, 3)),
        ]
        df = self.spark.createDataFrame(data, schema=schema)

        scalar_internal_value = pandas_udf(
            lambda ts: ts.apply(lambda ts: ts.value if ts is not pd.NaT else None), LongType()
        )

        @pandas_udf(LongType(), PandasUDFType.SCALAR_ITER)
        def iter_internal_value(it):
            for ts in it:
                yield ts.apply(lambda ts: ts.value if ts is not pd.NaT else None)

        for internal_value, udf_type in [
            (scalar_internal_value, PandasUDFType.SCALAR),
            (iter_internal_value, PandasUDFType.SCALAR_ITER),
        ]:
            f_timestamp_copy = pandas_udf(lambda ts: ts, TimestampType(), udf_type)
            timezone = "America/Los_Angeles"
            with self.sql_conf({"spark.sql.session.timeZone": timezone}):
                df_la = df.withColumn("tscopy", f_timestamp_copy(col("timestamp"))).withColumn(
                    "internal_value", internal_value(col("timestamp"))
                )
                result_la = df_la.select(col("idx"), col("internal_value")).collect()
                # Correct result_la by adjusting 3 hours difference between Los Angeles and New York
                diff = 3 * 60 * 60 * 1000 * 1000 * 1000
                result_la_corrected = df_la.select(
                    col("idx"), col("tscopy"), col("internal_value") + diff
                ).collect()

            timezone = "America/New_York"
            with self.sql_conf({"spark.sql.session.timeZone": timezone}):
                df_ny = df.withColumn("tscopy", f_timestamp_copy(col("timestamp"))).withColumn(
                    "internal_value", internal_value(col("timestamp"))
                )
                result_ny = df_ny.select(col("idx"), col("tscopy"), col("internal_value")).collect()

                self.assertNotEqual(result_ny, result_la)
                self.assertEqual(result_ny, result_la_corrected)

    def test_nondeterministic_vectorized_udf(self):
        # Test that nondeterministic UDFs are evaluated only once in chained UDF evaluations
        @pandas_udf("double")
        def scalar_plus_ten(v):
            return v + 10

        @pandas_udf("double", PandasUDFType.SCALAR_ITER)
        def iter_plus_ten(it):
            for v in it:
                yield v + 10

        for plus_ten in [scalar_plus_ten, iter_plus_ten]:
            random_udf = self.nondeterministic_vectorized_udf

            df = self.spark.range(10).withColumn("rand", random_udf(col("id")))
            result1 = df.withColumn("plus_ten(rand)", plus_ten(df["rand"])).toPandas()

            self.assertEqual(random_udf.deterministic, False)
            self.assertTrue(result1["plus_ten(rand)"].equals(result1["rand"] + 10))

    def test_nondeterministic_vectorized_udf_in_aggregate(self):
        with self.quiet():
            self.check_nondeterministic_analysis_exception()

    def check_nondeterministic_analysis_exception(self):
        df = self.spark.range(10)
        for random_udf in [
            self.nondeterministic_vectorized_udf,
            self.nondeterministic_vectorized_iter_udf,
        ]:
            with self.assertRaisesRegex(AnalysisException, "Non-deterministic"):
                df.groupby(df.id).agg(sum(random_udf(df.id))).collect()
            with self.assertRaisesRegex(AnalysisException, "Non-deterministic"):
                df.agg(sum(random_udf(df.id))).collect()

    def test_register_vectorized_udf_basic(self):
        df = self.spark.range(10).select(
            col("id").cast("int").alias("a"), col("id").cast("int").alias("b")
        )
        scalar_original_add = pandas_udf(lambda x, y: x + y, IntegerType())
        self.assertEqual(scalar_original_add.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        @pandas_udf(IntegerType(), PandasUDFType.SCALAR_ITER)
        def iter_original_add(it):
            for x, y in it:
                yield x + y

        self.assertEqual(iter_original_add.evalType, PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)

        for original_add in [scalar_original_add, iter_original_add]:
            self.assertEqual(original_add.deterministic, True)
            new_add = self.spark.catalog.registerFunction("add1", original_add)
            res1 = df.select(new_add(col("a"), col("b")))
            res2 = self.spark.sql(
                "SELECT add1(t.a, t.b) FROM (SELECT id as a, id as b FROM range(10)) t"
            )
            expected = df.select(expr("a + b"))
            self.assertEqual(expected.collect(), res1.collect())
            self.assertEqual(expected.collect(), res2.collect())

    def test_scalar_iter_udf_init(self):
        import numpy as np

        @pandas_udf("int", PandasUDFType.SCALAR_ITER)
        def rng(batch_iter):
            context = TaskContext.get()
            part = context.partitionId()
            np.random.seed(part)
            for batch in batch_iter:
                yield pd.Series(np.random.randint(100, size=len(batch)))

        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 2}):
            df = self.spark.range(10, numPartitions=2).select(rng(col("id").alias("v")))
            result1 = df.collect()
            result2 = df.collect()
            self.assertEqual(
                result1,
                result2,
                "SCALAR ITER UDF can initialize state and produce deterministic RNG",
            )

    def test_scalar_iter_udf_close(self):
        with self.quiet():
            self.check_scalar_iter_udf_close()

    def check_scalar_iter_udf_close(self):
        @pandas_udf("int", PandasUDFType.SCALAR_ITER)
        def test_close(batch_iter):
            try:
                for batch in batch_iter:
                    yield batch
            finally:
                raise RuntimeError("reached finally block")

        with self.assertRaisesRegex(Exception, "reached finally block"):
            self.spark.range(1).select(test_close(col("id"))).collect()

    @unittest.skip("LimitPushDown should push limits through Python UDFs so this won't occur")
    def test_scalar_iter_udf_close_early(self):
        tmp_dir = tempfile.mkdtemp()
        try:
            tmp_file = tmp_dir + "/reach_finally_block"

            @pandas_udf("int", PandasUDFType.SCALAR_ITER)
            def test_close(batch_iter):
                generator_exit_caught = False
                try:
                    for batch in batch_iter:
                        yield batch
                        time.sleep(1.0)  # avoid the function finish too fast.
                except GeneratorExit as ge:
                    generator_exit_caught = True
                    raise ge
                finally:
                    assert generator_exit_caught, "Generator exit exception was not caught."
                    open(tmp_file, "a").close()

            with self.quiet():
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.maxRecordsPerBatch": 1,
                        "spark.sql.execution.pandas.udf.buffer.size": 4,
                    }
                ):
                    self.spark.range(10).repartition(1).select(test_close(col("id"))).limit(
                        2
                    ).collect()
                    # wait here because python udf worker will take some time to detect
                    # jvm side socket closed and then will trigger `GenerateExit` raised.
                    # wait timeout is 10s.
                    for i in range(100):
                        time.sleep(0.1)
                        if os.path.exists(tmp_file):
                            break

                    assert os.path.exists(tmp_file), "finally block not reached."

        finally:
            shutil.rmtree(tmp_dir)

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [
            datetime(2015, 11, 1, 0, 30),
            datetime(2015, 11, 1, 1, 30),
            datetime(2015, 11, 1, 2, 30),
        ]
        df = self.spark.createDataFrame(dt, "timestamp").toDF("time")

        for udf_type in [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER]:
            foo_udf = pandas_udf(lambda x: x, "timestamp", udf_type)
            result = df.withColumn("time", foo_udf(df.time))
            self.assertEqual(df.collect(), result.collect())

    def test_udf_category_type(self):
        @pandas_udf("string")
        def to_category_func(x):
            return x.astype("category")

        pdf = pd.DataFrame({"A": ["a", "b", "c", "a"]})
        df = self.spark.createDataFrame(pdf)
        df = df.withColumn("B", to_category_func(df["A"]))
        result_spark = df.toPandas()

        spark_type = df.dtypes[1][1]
        # spark data frame and arrow execution mode enabled data frame type must match pandas
        self.assertEqual(spark_type, "string")

        # Check result of column 'B' must be equal to column 'A' in type and values
        pd.testing.assert_series_equal(result_spark["A"], result_spark["B"], check_names=False)

    def test_type_annotation(self):
        # Regression test to check if type hints can be used. See SPARK-23569.
        def noop(col: pd.Series) -> pd.Series:
            return col

        df = self.spark.range(1).select(pandas_udf(f=noop, returnType="bigint")("id"))
        self.assertEqual(df.first()[0], 0)

    def test_mixed_udf(self):
        df = self.spark.range(0, 1).toDF("v")

        # Test mixture of multiple UDFs and Pandas UDFs.

        @udf("int")
        def f1(x):
            assert type(x) == int
            return x + 1

        @pandas_udf("int")
        def f2_scalar(x):
            assert type(x) == pd.Series
            return x + 10

        @pandas_udf("int", PandasUDFType.SCALAR_ITER)
        def f2_iter(it):
            for x in it:
                assert type(x) == pd.Series
                yield x + 10

        @udf("int")
        def f3(x):
            assert type(x) == int
            return x + 100

        @pandas_udf("int")
        def f4_scalar(x):
            assert type(x) == pd.Series
            return x + 1000

        @pandas_udf("int", PandasUDFType.SCALAR_ITER)
        def f4_iter(it):
            for x in it:
                assert type(x) == pd.Series
                yield x + 1000

        expected_chained_1 = df.withColumn("f2_f1", df["v"] + 11).collect()
        expected_chained_2 = df.withColumn("f3_f2_f1", df["v"] + 111).collect()
        expected_chained_3 = df.withColumn("f4_f3_f2_f1", df["v"] + 1111).collect()
        expected_chained_4 = df.withColumn("f4_f2_f1", df["v"] + 1011).collect()
        expected_chained_5 = df.withColumn("f4_f3_f1", df["v"] + 1101).collect()

        expected_multi = (
            df.withColumn("f1", df["v"] + 1)
            .withColumn("f2", df["v"] + 10)
            .withColumn("f3", df["v"] + 100)
            .withColumn("f4", df["v"] + 1000)
            .withColumn("f2_f1", df["v"] + 11)
            .withColumn("f3_f1", df["v"] + 101)
            .withColumn("f4_f1", df["v"] + 1001)
            .withColumn("f3_f2", df["v"] + 110)
            .withColumn("f4_f2", df["v"] + 1010)
            .withColumn("f4_f3", df["v"] + 1100)
            .withColumn("f3_f2_f1", df["v"] + 111)
            .withColumn("f4_f2_f1", df["v"] + 1011)
            .withColumn("f4_f3_f1", df["v"] + 1101)
            .withColumn("f4_f3_f2", df["v"] + 1110)
            .withColumn("f4_f3_f2_f1", df["v"] + 1111)
            .collect()
        )

        for f2, f4 in [
            (f2_scalar, f4_scalar),
            (f2_scalar, f4_iter),
            (f2_iter, f4_scalar),
            (f2_iter, f4_iter),
        ]:
            # Test single expression with chained UDFs
            df_chained_1 = df.withColumn("f2_f1", f2(f1(df["v"])))
            df_chained_2 = df.withColumn("f3_f2_f1", f3(f2(f1(df["v"]))))
            df_chained_3 = df.withColumn("f4_f3_f2_f1", f4(f3(f2(f1(df["v"])))))
            df_chained_4 = df.withColumn("f4_f2_f1", f4(f2(f1(df["v"]))))
            df_chained_5 = df.withColumn("f4_f3_f1", f4(f3(f1(df["v"]))))

            self.assertEqual(expected_chained_1, df_chained_1.collect())
            self.assertEqual(expected_chained_2, df_chained_2.collect())
            self.assertEqual(expected_chained_3, df_chained_3.collect())
            self.assertEqual(expected_chained_4, df_chained_4.collect())
            self.assertEqual(expected_chained_5, df_chained_5.collect())

            # Test multiple mixed UDF expressions in a single projection
            df_multi_1 = (
                df.withColumn("f1", f1(col("v")))
                .withColumn("f2", f2(col("v")))
                .withColumn("f3", f3(col("v")))
                .withColumn("f4", f4(col("v")))
                .withColumn("f2_f1", f2(col("f1")))
                .withColumn("f3_f1", f3(col("f1")))
                .withColumn("f4_f1", f4(col("f1")))
                .withColumn("f3_f2", f3(col("f2")))
                .withColumn("f4_f2", f4(col("f2")))
                .withColumn("f4_f3", f4(col("f3")))
                .withColumn("f3_f2_f1", f3(col("f2_f1")))
                .withColumn("f4_f2_f1", f4(col("f2_f1")))
                .withColumn("f4_f3_f1", f4(col("f3_f1")))
                .withColumn("f4_f3_f2", f4(col("f3_f2")))
                .withColumn("f4_f3_f2_f1", f4(col("f3_f2_f1")))
            )

            # Test mixed udfs in a single expression
            df_multi_2 = (
                df.withColumn("f1", f1(col("v")))
                .withColumn("f2", f2(col("v")))
                .withColumn("f3", f3(col("v")))
                .withColumn("f4", f4(col("v")))
                .withColumn("f2_f1", f2(f1(col("v"))))
                .withColumn("f3_f1", f3(f1(col("v"))))
                .withColumn("f4_f1", f4(f1(col("v"))))
                .withColumn("f3_f2", f3(f2(col("v"))))
                .withColumn("f4_f2", f4(f2(col("v"))))
                .withColumn("f4_f3", f4(f3(col("v"))))
                .withColumn("f3_f2_f1", f3(f2(f1(col("v")))))
                .withColumn("f4_f2_f1", f4(f2(f1(col("v")))))
                .withColumn("f4_f3_f1", f4(f3(f1(col("v")))))
                .withColumn("f4_f3_f2", f4(f3(f2(col("v")))))
                .withColumn("f4_f3_f2_f1", f4(f3(f2(f1(col("v"))))))
            )

            self.assertEqual(expected_multi, df_multi_1.collect())
            self.assertEqual(expected_multi, df_multi_2.collect())

    def test_mixed_udf_and_sql(self):
        self._test_mixed_udf_and_sql(Column)

    def _test_mixed_udf_and_sql(self, col_type):
        df = self.spark.range(0, 1).toDF("v")

        # Test mixture of UDFs, Pandas UDFs and SQL expression.

        @udf("int")
        def f1(x):
            assert type(x) == int
            return x + 1

        def f2(x):
            assert type(x) == col_type
            return x + 10

        @pandas_udf("int")
        def f3s(x):
            assert type(x) == pd.Series
            return x + 100

        @pandas_udf("int", PandasUDFType.SCALAR_ITER)
        def f3i(it):
            for x in it:
                assert type(x) == pd.Series
                yield x + 100

        expected = (
            df.withColumn("f1", df["v"] + 1)
            .withColumn("f2", df["v"] + 10)
            .withColumn("f3", df["v"] + 100)
            .withColumn("f1_f2", df["v"] + 11)
            .withColumn("f1_f3", df["v"] + 101)
            .withColumn("f2_f1", df["v"] + 11)
            .withColumn("f2_f3", df["v"] + 110)
            .withColumn("f3_f1", df["v"] + 101)
            .withColumn("f3_f2", df["v"] + 110)
            .withColumn("f1_f2_f3", df["v"] + 111)
            .withColumn("f1_f3_f2", df["v"] + 111)
            .withColumn("f2_f1_f3", df["v"] + 111)
            .withColumn("f2_f3_f1", df["v"] + 111)
            .withColumn("f3_f1_f2", df["v"] + 111)
            .withColumn("f3_f2_f1", df["v"] + 111)
            .collect()
        )

        for f3 in [f3s, f3i]:
            df1 = (
                df.withColumn("f1", f1(df["v"]))
                .withColumn("f2", f2(df["v"]))
                .withColumn("f3", f3(df["v"]))
                .withColumn("f1_f2", f1(f2(df["v"])))
                .withColumn("f1_f3", f1(f3(df["v"])))
                .withColumn("f2_f1", f2(f1(df["v"])))
                .withColumn("f2_f3", f2(f3(df["v"])))
                .withColumn("f3_f1", f3(f1(df["v"])))
                .withColumn("f3_f2", f3(f2(df["v"])))
                .withColumn("f1_f2_f3", f1(f2(f3(df["v"]))))
                .withColumn("f1_f3_f2", f1(f3(f2(df["v"]))))
                .withColumn("f2_f1_f3", f2(f1(f3(df["v"]))))
                .withColumn("f2_f3_f1", f2(f3(f1(df["v"]))))
                .withColumn("f3_f1_f2", f3(f1(f2(df["v"]))))
                .withColumn("f3_f2_f1", f3(f2(f1(df["v"]))))
            )

            self.assertEqual(expected, df1.collect())

    # SPARK-24721
    @unittest.skipIf(not test_compiled, test_not_compiled_message)  # type: ignore
    def test_datasource_with_udf(self):
        # Same as SQLTests.test_datasource_with_udf, but with Pandas UDF
        # This needs to a separate test because Arrow dependency is optional
        import numpy as np

        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(1).write.mode("overwrite").format("csv").save(path)
            filesource_df = self.spark.read.option("inferSchema", True).csv(path).toDF("i")
            datasource_df = (
                self.spark.read.format("org.apache.spark.sql.sources.SimpleScanSource")
                .option("from", 0)
                .option("to", 1)
                .load()
                .toDF("i")
            )
            datasource_v2_df = (
                self.spark.read.format("org.apache.spark.sql.connector.SimpleDataSourceV2")
                .load()
                .toDF("i", "j")
            )

            c1 = pandas_udf(lambda x: x + 1, "int")(lit(1))
            c2 = pandas_udf(lambda x: x + 1, "int")(col("i"))

            f1 = pandas_udf(lambda x: pd.Series(np.repeat(False, len(x))), "boolean")(lit(1))
            f2 = pandas_udf(lambda x: pd.Series(np.repeat(False, len(x))), "boolean")(col("i"))

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn("c", c1)
                expected = df.withColumn("c", lit(2))
                self.assertEqual(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn("c", c2)
                expected = df.withColumn("c", col("i") + 1)
                self.assertEqual(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                for f in [f1, f2]:
                    result = df.filter(f)
                    self.assertEqual(0, result.count())
        finally:
            shutil.rmtree(path)

    # SPARK-33277
    def test_pandas_udf_with_column_vector(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(0, 200000, 1, 1).write.parquet(path)

            @pandas_udf(LongType())
            def udf(x):
                return pd.Series([0] * len(x))

            for offheap in ["true", "false"]:
                with self.sql_conf({"spark.sql.columnVector.offheap.enabled": offheap}):
                    self.assertEqual(self.spark.read.parquet(path).select(udf("id")).head(), Row(0))
        finally:
            shutil.rmtree(path)

    def test_named_arguments(self):
        @pandas_udf("int")
        def test_udf(a, b):
            return a + 10 * b

        self.spark.udf.register("test_udf", test_udf)

        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(a=col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(b=col("id") * 10, a=col("id"))),
                self.spark.sql("SELECT test_udf(id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(101)])

    def test_named_arguments_negative(self):
        @pandas_udf("int")
        def test_udf(a, b):
            return a + b

        self.spark.udf.register("test_udf", test_udf)

        with self.assertRaisesRegex(
            AnalysisException,
            "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
        ):
            self.spark.sql("SELECT test_udf(a => id, a => id * 10) FROM range(2)").show()

        with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
            self.spark.sql("SELECT test_udf(a => id, id * 10) FROM range(2)").show()

        with self.assertRaisesRegex(
            PythonException, r"test_udf\(\) got an unexpected keyword argument 'c'"
        ):
            self.spark.sql("SELECT test_udf(c => 'x') FROM range(2)").show()

    def test_kwargs(self):
        @pandas_udf("int")
        def test_udf(a, **kwargs):
            return a + 10 * kwargs["b"]

        self.spark.udf.register("test_udf", test_udf)

        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(a=col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(b=col("id") * 10, a=col("id"))),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(101)])

    def test_named_arguments_and_defaults(self):
        @pandas_udf("int")
        def test_udf(a, b=0):
            return a + 10 * b

        self.spark.udf.register("test_udf", test_udf)

        # without "b"
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(col("id"))),
                self.spark.range(2).select(test_udf(a=col("id"))),
                self.spark.sql("SELECT test_udf(id) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(with_b=False, query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(1)])

        # with "b"
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(a=col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(b=col("id") * 10, a=col("id"))),
                self.spark.sql("SELECT test_udf(id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(with_b=True, query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(101)])


class ScalarPandasUDFTests(ScalarPandasUDFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        ReusedSQLTestCase.tearDownClass()


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_udf_scalar import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
