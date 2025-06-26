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
import time
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterator, Tuple

from pyspark.util import PythonEvalType

# TODO: import arrow_udf from public API
from pyspark.sql.pandas.functions import arrow_udf, ArrowUDFType
from pyspark.sql import functions as F
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
    MapType,
    BinaryType,
)
from pyspark.errors import AnalysisException, PythonException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ScalarArrowUDFTestsMixin:
    @property
    def nondeterministic_arrow_udf(self):
        import pyarrow as pa
        import numpy as np

        @arrow_udf("double")
        def random_udf(v):
            return pa.array(np.random.random(len(v)))

        return random_udf.asNondeterministic()

    @property
    def nondeterministic_arrow_iter_udf(self):
        import pyarrow as pa
        import numpy as np

        @arrow_udf("double", ArrowUDFType.SCALAR_ITER)
        def random_udf(it):
            for v in it:
                yield pa.array(np.random.random(len(v)))

        return random_udf.asNondeterministic()

    def test_arrow_udf_tokenize(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])

        tokenize = arrow_udf(
            lambda s: pa.compute.ascii_split_whitespace(s),
            ArrayType(StringType()),
        )

        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual(tokenize.returnType, ArrayType(StringType()))
        self.assertEqual([Row(hi=["hi", "boo"]), Row(hi=["bye", "boo"])], result.collect())

    def test_arrow_udf_output_nested_arrays(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])

        tokenize = arrow_udf(
            lambda s: pa.array([pa.compute.ascii_split_whitespace(s).to_pylist()]),
            ArrayType(ArrayType(StringType())),
        )

        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual(tokenize.returnType, ArrayType(ArrayType(StringType())))
        self.assertEqual([Row(hi=[["hi", "boo"]]), Row(hi=[["bye", "boo"]])], result.collect())

    def test_arrow_udf_output_structs(self):
        import pyarrow as pa

        df = self.spark.range(10).select("id", F.lit("foo").alias("name"))

        create_struct = arrow_udf(
            lambda x, y: pa.StructArray.from_arrays([x, y], names=["c1", "c2"]),
            "struct<c1:long, c2:string>",
        )

        self.assertEqual(
            df.select(create_struct("id", "name").alias("res")).first(),
            Row(res=Row(c1=0, c2="foo")),
        )

    def test_arrow_udf_output_nested_structs(self):
        import pyarrow as pa

        df = self.spark.range(10).select("id", F.lit("foo").alias("name"))

        create_struct = arrow_udf(
            lambda x, y: pa.StructArray.from_arrays(
                [x, pa.StructArray.from_arrays([x, y], names=["c3", "c4"])], names=["c1", "c2"]
            ),
            "struct<c1:long, c2:struct<c3:long, c4:string>>",
        )

        self.assertEqual(
            df.select(create_struct("id", "name").alias("res")).first(),
            Row(res=Row(c1=0, c2=Row(c3=0, c4="foo"))),
        )

    def test_arrow_udf_input_output_nested_structs(self):
        # root
        # |-- s: struct (nullable = false)
        # |    |-- a: integer (nullable = false)
        # |    |-- y: struct (nullable = false)
        # |    |    |-- b: integer (nullable = false)
        # |    |    |-- x: struct (nullable = false)
        # |    |    |    |-- c: integer (nullable = false)
        # |    |    |    |-- d: integer (nullable = false)
        df = self.spark.sql(
            """
            SELECT STRUCT(a, STRUCT(b, STRUCT(c, d) AS x) AS y) AS s
            FROM VALUES
            (1, 2, 3, 4),
            (-1, -2, -3, -4)
            AS tab(a, b, c, d)
        """
        )

        schema = StructType(
            [
                StructField("a", IntegerType(), False),
                StructField(
                    "y",
                    StructType(
                        [
                            StructField("b", IntegerType(), False),
                            StructField(
                                "x",
                                StructType(
                                    [
                                        StructField("c", IntegerType(), False),
                                        StructField("d", IntegerType(), False),
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

        extract_y = arrow_udf(lambda s: s.field("y"), schema["y"].dataType)
        result = df.select(extract_y("s").alias("y"))

        self.assertEqual(
            [Row(y=Row(b=2, x=Row(c=3, d=4))), Row(y=Row(b=-2, x=Row(c=-3, d=-4)))],
            result.collect(),
        )

    def test_arrow_udf_input_nested_maps(self):
        import pyarrow as pa

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "attributes", MapType(StringType(), MapType(StringType(), StringType())), True
                ),
            ]
        )
        data = [("1", {"personal": {"name": "John", "city": "New York"}})]
        # root
        # |-- id: string (nullable = true)
        # |-- attributes: map (nullable = true)
        # |    |-- key: string
        # |    |-- value: map (valueContainsNull = true)
        # |    |    |-- key: string
        # |    |    |-- value: string (valueContainsNull = true)
        df = self.spark.createDataFrame(data, schema)

        str_repr = arrow_udf(lambda s: pa.array(str(x.as_py()) for x in s), StringType())
        result = df.select(str_repr("attributes").alias("s"))

        self.assertEqual(
            [Row(s="[('personal', [('name', 'John'), ('city', 'New York')])]")],
            result.collect(),
        )

    def test_arrow_udf_input_nested_arrays(self):
        import pyarrow as pa

        # root
        # |-- a: array (nullable = false)
        # |    |-- element: array (containsNull = false)
        # |    |    |-- element: integer (containsNull = true)
        df = self.spark.sql(
            """
            SELECT ARRAY(ARRAY(1,2,3),ARRAY(4,NULL),ARRAY(5,6,NULL)) AS arr
            """
        )

        str_repr = arrow_udf(lambda s: pa.array(str(x.as_py()) for x in s), StringType())
        result = df.select(str_repr("arr").alias("s"))

        self.assertEqual(
            [Row(s="[[1, 2, 3], [4, None], [5, 6, None]]")],
            result.collect(),
        )

    def test_arrow_udf_input_arrow_array_struct(self):
        import pyarrow as pa

        df = self.spark.createDataFrame(
            [[[("a", 2, 3.0), ("a", 2, 3.0)]], [[("b", 5, 6.0), ("b", 5, 6.0)]]],
            "array_struct_col array<struct<col1:string, col2:long, col3:double>>",
        )

        @arrow_udf("array<struct<col1:string, col2:long, col3:double>>")
        def return_cols(cols):
            assert isinstance(cols, pa.Array)
            return cols

        result = df.select(return_cols("array_struct_col"))

        self.assertEqual(
            [
                Row(output=[Row(col1="a", col2=2, col3=3.0), Row(col1="a", col2=2, col3=3.0)]),
                Row(output=[Row(col1="b", col2=5, col3=6.0), Row(col1="b", col2=5, col3=6.0)]),
            ],
            result.collect(),
        )

    def test_arrow_udf_input_dates(self):
        import pyarrow as pa

        df = self.spark.sql(
            """
            SELECT * FROM VALUES
            (1, DATE('2022-02-22')),
            (2, DATE('2023-02-22')),
            (3, DATE('2024-02-22'))
            AS tab(i, date)
            """
        )

        @arrow_udf("int")
        def extract_year(d):
            assert isinstance(d, pa.Array)
            assert isinstance(d, pa.Date32Array)
            return pa.array([x.as_py().year for x in d], pa.int32())

        result = df.select(extract_year("date").alias("year"))
        self.assertEqual(
            [Row(year=2022), Row(year=2023), Row(year=2024)],
            result.collect(),
        )

    def test_arrow_udf_output_dates(self):
        import pyarrow as pa

        df = self.spark.sql(
            """
            SELECT * FROM VALUES
            (2022, 1, 5),
            (2023, 2, 6),
            (2024, 3, 7)
            AS tab(y, m, d)
            """
        )

        @arrow_udf("date")
        def build_date(y, m, d):
            assert all(isinstance(x, pa.Array) for x in [y, m, d])
            dates = [
                date(int(y[i].as_py()), int(m[i].as_py()), int(d[i].as_py())) for i in range(len(y))
            ]
            return pa.array(dates, pa.date32())

        result = df.select(build_date("y", "m", "d").alias("date"))
        self.assertEqual(
            [
                Row(date=date(2022, 1, 5)),
                Row(date=date(2023, 2, 6)),
                Row(date=date(2024, 3, 7)),
            ],
            result.collect(),
        )

    def test_arrow_udf_input_timestamps(self):
        import pyarrow as pa

        df = self.spark.sql(
            """
            SELECT * FROM VALUES
            (1, TIMESTAMP('2019-04-12 15:50:01')),
            (2, TIMESTAMP('2020-04-12 15:50:02')),
            (3, TIMESTAMP('2021-04-12 15:50:03'))
            AS tab(i, ts)
            """
        )

        @arrow_udf("int")
        def extract_second(d):
            assert isinstance(d, pa.Array)
            assert isinstance(d, pa.TimestampArray)
            return pa.array([x.as_py().second for x in d], pa.int32())

        result = df.select(extract_second("ts").alias("second"))
        self.assertEqual(
            [Row(second=1), Row(second=2), Row(second=3)],
            result.collect(),
        )

    def test_arrow_udf_output_timestamps_ltz(self):
        import pyarrow as pa

        df = self.spark.sql(
            """
            SELECT * FROM VALUES
            (2022, 1, 5, 15, 0, 1),
            (2023, 2, 6, 16, 1, 2),
            (2024, 3, 7, 17, 2, 3)
            AS tab(y, m, d, h, mi, s)
            """
        )

        @arrow_udf("timestamp")
        def build_ts(y, m, d, h, mi, s):
            assert all(isinstance(x, pa.Array) for x in [y, m, d, h, mi, s])
            dates = [
                datetime(
                    int(y[i].as_py()),
                    int(m[i].as_py()),
                    int(d[i].as_py()),
                    int(h[i].as_py()),
                    int(mi[i].as_py()),
                    int(s[i].as_py()),
                    tzinfo=timezone.utc,
                )
                for i in range(len(y))
            ]
            return pa.array(dates, pa.timestamp("us", "UTC"))

        result = df.select(build_ts("y", "m", "d", "h", "mi", "s").alias("ts"))
        self.assertEqual(
            [
                Row(ts=datetime(2022, 1, 5, 7, 0, 1)),
                Row(ts=datetime(2023, 2, 6, 8, 1, 2)),
                Row(ts=datetime(2024, 3, 7, 9, 2, 3)),
            ],
            result.collect(),
        )

    def test_arrow_udf_output_timestamps_ntz(self):
        import pyarrow as pa

        df = self.spark.sql(
            """
            SELECT * FROM VALUES
            (2022, 1, 5, 15, 0, 1),
            (2023, 2, 6, 16, 1, 2),
            (2024, 3, 7, 17, 2, 3)
            AS tab(y, m, d, h, mi, s)
            """
        )

        @arrow_udf("timestamp_ntz")
        def build_ts(y, m, d, h, mi, s):
            assert all(isinstance(x, pa.Array) for x in [y, m, d, h, mi, s])
            dates = [
                datetime(
                    int(y[i].as_py()),
                    int(m[i].as_py()),
                    int(d[i].as_py()),
                    int(h[i].as_py()),
                    int(mi[i].as_py()),
                    int(s[i].as_py()),
                )
                for i in range(len(y))
            ]
            return pa.array(dates, pa.timestamp("us"))

        result = df.select(build_ts("y", "m", "d", "h", "mi", "s").alias("ts"))
        self.assertEqual(
            [
                Row(ts=datetime(2022, 1, 5, 15, 0, 1)),
                Row(ts=datetime(2023, 2, 6, 16, 1, 2)),
                Row(ts=datetime(2024, 3, 7, 17, 2, 3)),
            ],
            result.collect(),
        )

    def test_arrow_udf_null_boolean(self):
        data = [(True,), (True,), (None,), (False,)]
        schema = StructType().add("bool", BooleanType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            bool_f = arrow_udf(lambda x: x, BooleanType(), udf_type)
            res = df.select(bool_f(F.col("bool")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_byte(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("byte", ByteType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            byte_f = arrow_udf(lambda x: x, ByteType(), udf_type)
            res = df.select(byte_f(F.col("byte")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_short(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("short", ShortType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            short_f = arrow_udf(lambda x: x, ShortType(), udf_type)
            res = df.select(short_f(F.col("short")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_int(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("int", IntegerType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            int_f = arrow_udf(lambda x: x, IntegerType(), udf_type)
            res = df.select(int_f(F.col("int")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_long(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("long", LongType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            long_f = arrow_udf(lambda x: x, LongType(), udf_type)
            res = df.select(long_f(F.col("long")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_float(self):
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("float", FloatType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            float_f = arrow_udf(lambda x: x, FloatType(), udf_type)
            res = df.select(float_f(F.col("float")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_double(self):
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("double", DoubleType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            double_f = arrow_udf(lambda x: x, DoubleType(), udf_type)
            res = df.select(double_f(F.col("double")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_decimal(self):
        data = [(Decimal(3.0),), (Decimal(5.0),), (Decimal(-1.0),), (None,)]
        schema = StructType().add("decimal", DecimalType(38, 18))
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            decimal_f = arrow_udf(lambda x: x, DecimalType(38, 18), udf_type)
            res = df.select(decimal_f(F.col("decimal")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_string(self):
        data = [("foo",), (None,), ("bar",), ("bar",)]
        schema = StructType().add("str", StringType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            str_f = arrow_udf(lambda x: x, StringType(), udf_type)
            res = df.select(str_f(F.col("str")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_binary(self):
        data = [(bytearray(b"a"),), (None,), (bytearray(b"bb"),), (bytearray(b"ccc"),)]
        schema = StructType().add("binary", BinaryType())
        df = self.spark.createDataFrame(data, schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            binary_f = arrow_udf(lambda x: x, BinaryType(), udf_type)
            res = df.select(binary_f(F.col("binary")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_null_array(self):
        data = [([1, 2],), (None,), (None,), ([3, 4],), (None,)]
        array_schema = StructType([StructField("array", ArrayType(IntegerType()))])
        df = self.spark.createDataFrame(data, schema=array_schema)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            array_f = arrow_udf(lambda x: x, ArrayType(IntegerType()), udf_type)
            result = df.select(array_f(F.col("array")))
            self.assertEqual(df.collect(), result.collect())

    def test_arrow_udf_empty_partition(self):
        df = self.spark.createDataFrame([Row(id=1)]).repartition(2)
        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            f = arrow_udf(lambda x: x, LongType(), udf_type)
            res = df.select(f(F.col("id")))
            self.assertEqual(df.collect(), res.collect())

    def test_arrow_udf_datatype_string(self):
        df = self.spark.range(10).select(
            F.col("id").cast("string").alias("str"),
            F.col("id").cast("int").alias("int"),
            F.col("id").alias("long"),
            F.col("id").cast("float").alias("float"),
            F.col("id").cast("double").alias("double"),
            F.col("id").cast("decimal").alias("decimal1"),
            F.col("id").cast("decimal(10, 0)").alias("decimal2"),
            F.col("id").cast("decimal(38, 18)").alias("decimal3"),
            F.col("id").cast("boolean").alias("bool"),
        )

        def f(x):
            return x

        for udf_type in [ArrowUDFType.SCALAR, ArrowUDFType.SCALAR_ITER]:
            str_f = arrow_udf(f, "string", udf_type)
            int_f = arrow_udf(f, "integer", udf_type)
            long_f = arrow_udf(f, "long", udf_type)
            float_f = arrow_udf(f, "float", udf_type)
            double_f = arrow_udf(f, "double", udf_type)
            decimal1_f = arrow_udf(f, "decimal", udf_type)
            decimal2_f = arrow_udf(f, "decimal(10, 0)", udf_type)
            decimal3_f = arrow_udf(f, "decimal(38, 18)", udf_type)
            bool_f = arrow_udf(f, "boolean", udf_type)
            res = df.select(
                str_f(F.col("str")),
                int_f(F.col("int")),
                long_f(F.col("long")),
                float_f(F.col("float")),
                double_f(F.col("double")),
                decimal1_f("decimal1"),
                decimal2_f("decimal2"),
                decimal3_f("decimal3"),
                bool_f(F.col("bool")),
            )
            self.assertEqual(df.collect(), res.collect())

    def test_udf_register_arrow_udf_basic(self):
        import pyarrow as pa

        scalar_original_add = arrow_udf(
            lambda x, y: pa.compute.add(x, y).cast(pa.int32()), IntegerType()
        )
        self.assertEqual(scalar_original_add.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)
        self.assertEqual(scalar_original_add.deterministic, True)

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS add1")
        new_add = self.spark.udf.register("add1", scalar_original_add)

        self.assertEqual(new_add.deterministic, True)
        self.assertEqual(new_add.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        df = self.spark.range(10).select(
            F.col("id").cast("int").alias("a"), F.col("id").cast("int").alias("b")
        )
        res1 = df.select(new_add(F.col("a"), F.col("b")))
        res2 = self.spark.sql(
            "SELECT add1(t.a, t.b) FROM (SELECT id as a, id as b FROM range(10)) t"
        )
        expected = df.select(F.expr("a + b"))
        self.assertEqual(expected.collect(), res1.collect())
        self.assertEqual(expected.collect(), res2.collect())

    def test_catalog_register_arrow_udf_basic(self):
        import pyarrow as pa

        scalar_original_add = arrow_udf(
            lambda x, y: pa.compute.add(x, y).cast(pa.int32()), IntegerType()
        )
        self.assertEqual(scalar_original_add.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)
        self.assertEqual(scalar_original_add.deterministic, True)

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS add1")
        new_add = self.spark.catalog.registerFunction("add1", scalar_original_add)

        self.assertEqual(new_add.deterministic, True)
        self.assertEqual(new_add.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        df = self.spark.range(10).select(
            F.col("id").cast("int").alias("a"), F.col("id").cast("int").alias("b")
        )
        res1 = df.select(new_add(F.col("a"), F.col("b")))
        res2 = self.spark.sql(
            "SELECT add1(t.a, t.b) FROM (SELECT id as a, id as b FROM range(10)) t"
        )
        expected = df.select(F.expr("a + b"))
        self.assertEqual(expected.collect(), res1.collect())
        self.assertEqual(expected.collect(), res2.collect())

    def test_udf_register_nondeterministic_arrow_udf(self):
        import pyarrow as pa

        random_arrow_udf = arrow_udf(
            lambda x: pa.compute.add(x, random.randint(6, 6)), LongType()
        ).asNondeterministic()
        self.assertEqual(random_arrow_udf.deterministic, False)
        self.assertEqual(random_arrow_udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS randomArrowUDF")
        nondeterministic_arrow_udf = self.spark.udf.register("randomArrowUDF", random_arrow_udf)

        self.assertEqual(nondeterministic_arrow_udf.deterministic, False)
        self.assertEqual(nondeterministic_arrow_udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)
        [row] = self.spark.sql("SELECT randomArrowUDF(1)").collect()
        self.assertEqual(row[0], 7)

    def test_catalog_register_nondeterministic_arrow_udf(self):
        import pyarrow as pa

        random_arrow_udf = arrow_udf(
            lambda x: pa.compute.add(x, random.randint(6, 6)), LongType()
        ).asNondeterministic()
        self.assertEqual(random_arrow_udf.deterministic, False)
        self.assertEqual(random_arrow_udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS randomArrowUDF")
        nondeterministic_arrow_udf = self.spark.catalog.registerFunction(
            "randomArrowUDF", random_arrow_udf
        )

        self.assertEqual(nondeterministic_arrow_udf.deterministic, False)
        self.assertEqual(nondeterministic_arrow_udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)
        [row] = self.spark.sql("SELECT randomArrowUDF(1)").collect()
        self.assertEqual(row[0], 7)

    def test_nondeterministic_arrow_udf(self):
        import pyarrow as pa

        # Test that nondeterministic UDFs are evaluated only once in chained UDF evaluations
        @arrow_udf("double")
        def scalar_plus_ten(v):
            return pa.compute.add(v, 10)

        @arrow_udf("double", ArrowUDFType.SCALAR_ITER)
        def iter_plus_ten(it):
            for v in it:
                yield pa.compute.add(v, 10)

        for plus_ten in [scalar_plus_ten, iter_plus_ten]:
            random_udf = self.nondeterministic_arrow_udf

            df = self.spark.range(10).withColumn("rand", random_udf("id"))
            result1 = df.withColumn("plus_ten(rand)", plus_ten(df["rand"])).toPandas()

            self.assertEqual(random_udf.deterministic, False)
            self.assertTrue(result1["plus_ten(rand)"].equals(result1["rand"] + 10))

    def test_nondeterministic_arrow_udf_in_aggregate(self):
        with self.quiet():
            df = self.spark.range(10)
            for random_udf in [
                self.nondeterministic_arrow_udf,
                self.nondeterministic_arrow_iter_udf,
            ]:
                with self.assertRaisesRegex(AnalysisException, "Non-deterministic"):
                    df.groupby("id").agg(F.sum(random_udf("id"))).collect()
                with self.assertRaisesRegex(AnalysisException, "Non-deterministic"):
                    df.agg(F.sum(random_udf("id"))).collect()

    def test_arrow_udf_chained(self):
        import pyarrow.compute as pc

        scalar_f = arrow_udf(lambda x: pc.add(x, 1), LongType())
        scalar_g = arrow_udf(lambda x: pc.subtract(x, 1), LongType())

        iter_f = arrow_udf(
            lambda it: map(lambda x: pc.add(x, 1), it),
            LongType(),
            ArrowUDFType.SCALAR_ITER,
        )
        iter_g = arrow_udf(
            lambda it: map(lambda x: pc.subtract(x, 1), it),
            LongType(),
            ArrowUDFType.SCALAR_ITER,
        )

        df = self.spark.range(10)
        expected = df.select(F.col("id").alias("res")).collect()

        for f, g in [
            (scalar_f, scalar_g),
            (iter_f, iter_g),
            (scalar_f, iter_g),
            (iter_f, scalar_g),
        ]:
            res = df.select(g(f(F.col("id"))).alias("res"))
            self.assertEqual(expected, res.collect())

    def test_arrow_udf_chained_ii(self):
        import pyarrow.compute as pc

        scalar_f = arrow_udf(lambda x: pc.add(x, 1), LongType())

        iter_f = arrow_udf(
            lambda it: map(lambda x: pc.add(x, 1), it),
            LongType(),
            ArrowUDFType.SCALAR_ITER,
        )

        df = self.spark.range(10)
        expected = df.select((F.col("id") + 3).alias("res")).collect()

        for f1, f2, f3 in [
            (scalar_f, scalar_f, scalar_f),
            (iter_f, iter_f, iter_f),
            (scalar_f, iter_f, scalar_f),
            (iter_f, scalar_f, scalar_f),
            (iter_f, iter_f, iter_f),
        ]:
            res = df.select(f1(f2(f3(F.col("id")))).alias("res"))
            self.assertEqual(expected, res.collect())

    def test_arrow_udf_chained_iii(self):
        import pyarrow as pa

        scalar_f = arrow_udf(lambda x: pa.compute.add(x, 1), LongType())
        scalar_g = arrow_udf(lambda x: pa.compute.subtract(x, 1), LongType())
        scalar_m = arrow_udf(lambda x, y: pa.compute.multiply(x, y), LongType())

        iter_f = arrow_udf(
            lambda it: map(lambda x: pa.compute.add(x, 1), it),
            LongType(),
            ArrowUDFType.SCALAR_ITER,
        )
        iter_g = arrow_udf(
            lambda it: map(lambda x: pa.compute.subtract(x, 1), it),
            LongType(),
            ArrowUDFType.SCALAR_ITER,
        )

        @arrow_udf(LongType())
        def iter_m(it: Iterator[Tuple[pa.Array, pa.Array]]) -> Iterator[pa.Array]:
            for a, b in it:
                yield pa.compute.multiply(a, b)

        df = self.spark.range(10)
        expected = df.select(((F.col("id") + 1) * (F.col("id") - 1)).alias("res")).collect()

        for f, g, m in [
            (scalar_f, scalar_g, scalar_m),
            (iter_f, iter_g, iter_m),
            (scalar_f, iter_g, scalar_m),
            (iter_f, scalar_g, scalar_m),
            (iter_f, scalar_g, iter_m),
        ]:
            res = df.select(m(f(F.col("id")), g(F.col("id"))).alias("res"))
            self.assertEqual(expected, res.collect())

    def test_arrow_udf_chained_struct_type(self):
        import pyarrow as pa

        return_type = StructType([StructField("id", LongType()), StructField("str", StringType())])

        @arrow_udf(return_type)
        def scalar_f(id):
            return pa.StructArray.from_arrays([id, id.cast(pa.string())], names=["id", "str"])

        @arrow_udf(return_type)
        def iter_f(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for id in it:
                yield pa.StructArray.from_arrays([id, id.cast(pa.string())], names=["id", "str"])

        scalar_g = arrow_udf(lambda x: x, return_type)
        iter_g = arrow_udf(lambda x: x, return_type, ArrowUDFType.SCALAR_ITER)

        df = self.spark.range(10)
        expected = df.select(
            F.struct(F.col("id"), F.col("id").cast("string").alias("str")).alias("res")
        ).collect()

        for f, g in [
            (scalar_f, scalar_g),
            (iter_f, iter_g),
            (scalar_f, iter_g),
            (iter_f, scalar_g),
        ]:
            res = df.select(g(f(F.col("id"))).alias("res"))
            self.assertEqual(expected, res.collect())

    def test_arrow_udf_named_arguments(self):
        import pyarrow as pa

        @arrow_udf("int")
        def test_udf(a, b):
            return pa.compute.add(a, pa.compute.multiply(b, 10)).cast(pa.int32())

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS test_udf")
        self.spark.udf.register("test_udf", test_udf)

        expected = [Row(0), Row(101)]
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(F.col("id"), b=F.col("id") * 10)),
                self.spark.range(2).select(test_udf(a=F.col("id"), b=F.col("id") * 10)),
                self.spark.range(2).select(test_udf(b=F.col("id") * 10, a=F.col("id"))),
                self.spark.sql("SELECT test_udf(id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(query_no=i):
                self.assertEqual(expected, df.collect())

    def test_arrow_udf_named_arguments_negative(self):
        import pyarrow as pa

        @arrow_udf("int")
        def test_udf(a, b):
            return pa.compute.add(a, b).cast(pa.int32())

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS test_udf")
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

    def test_arrow_udf_named_arguments_and_defaults(self):
        import pyarrow as pa

        @arrow_udf("int")
        def test_udf(a, b=0):
            return pa.compute.add(a, pa.compute.multiply(b, 10)).cast(pa.int32())

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS test_udf")
        self.spark.udf.register("test_udf", test_udf)

        # without "b"
        expected = [Row(0), Row(1)]
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(F.col("id"))),
                self.spark.range(2).select(test_udf(a=F.col("id"))),
                self.spark.sql("SELECT test_udf(id) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(with_b=False, query_no=i):
                self.assertEqual(expected, df.collect())

        # with "b"
        expected = [Row(0), Row(101)]
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(F.col("id"), b=F.col("id") * 10)),
                self.spark.range(2).select(test_udf(a=F.col("id"), b=F.col("id") * 10)),
                self.spark.range(2).select(test_udf(b=F.col("id") * 10, a=F.col("id"))),
                self.spark.sql("SELECT test_udf(id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(with_b=True, query_no=i):
                self.assertEqual(expected, df.collect())

    def test_arrow_udf_kwargs(self):
        import pyarrow as pa

        @arrow_udf("int")
        def test_udf(a, **kwargs):
            return pa.compute.add(a, pa.compute.multiply(kwargs["b"], 10)).cast(pa.int32())

        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS test_udf")
        self.spark.udf.register("test_udf", test_udf)

        expected = [Row(0), Row(101)]
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(a=F.col("id"), b=F.col("id") * 10)),
                self.spark.range(2).select(test_udf(b=F.col("id") * 10, a=F.col("id"))),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(query_no=i):
                self.assertEqual(expected, df.collect())

    def test_arrow_iter_udf_single_column(self):
        import pyarrow as pa

        @arrow_udf(LongType())
        def add_one(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for s in it:
                yield pa.compute.add(s, 1)

        df = self.spark.range(10)
        expected = df.select((F.col("id") + 1).alias("res")).collect()

        result = df.select(add_one("id").alias("res"))
        self.assertEqual(expected, result.collect())

    def test_arrow_iter_udf_two_columns(self):
        import pyarrow as pa

        @arrow_udf(LongType())
        def multiple(it: Iterator[Tuple[pa.Array, pa.Array]]) -> Iterator[pa.Array]:
            for a, b in it:
                yield pa.compute.multiply(a, b)

        df = self.spark.range(10).select(
            F.col("id").alias("a"),
            (F.col("id") + 1).alias("b"),
        )

        expected = df.select((F.col("a") * F.col("b")).alias("res")).collect()

        result = df.select(multiple("a", "b").alias("res"))
        self.assertEqual(expected, result.collect())

    def test_arrow_iter_udf_three_columns(self):
        import pyarrow as pa

        @arrow_udf(LongType())
        def multiple(it: Iterator[Tuple[pa.Array, pa.Array, pa.Array]]) -> Iterator[pa.Array]:
            for a, b, c in it:
                yield pa.compute.multiply(pa.compute.multiply(a, b), c)

        df = self.spark.range(10).select(
            F.col("id").alias("a"),
            (F.col("id") + 1).alias("b"),
            (F.col("id") + 2).alias("c"),
        )

        expected = df.select((F.col("a") * F.col("b") * F.col("c")).alias("res")).collect()

        result = df.select(multiple("a", "b", "c").alias("res"))
        self.assertEqual(expected, result.collect())


class ScalarArrowUDFTests(ScalarArrowUDFTestsMixin, ReusedSQLTestCase):
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
    from pyspark.sql.tests.arrow.test_arrow_udf_scalar import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
