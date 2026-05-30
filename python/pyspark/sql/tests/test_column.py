# -*- encoding: utf-8 -*-
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

from enum import Enum
from itertools import chain
import datetime
import unittest
import uuid

from pyspark.sql import Column, Row
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import have_pandas, pandas_requirement_message


class ColumnTestsMixin:
    def test_column_name_encoding(self):
        """Ensure that created columns has `str` type consistently."""
        columns = self.spark.createDataFrame([("Alice", 1)], ["name", "age"]).columns
        self.assertEqual(columns, ["name", "age"])
        self.assertTrue(isinstance(columns[0], str))
        self.assertTrue(isinstance(columns[1], str))

    def test_and_in_expression(self):
        self.assertEqual(4, self.df.filter((self.df.key <= 10) & (self.df.value <= "2")).count())
        self.assertRaises(ValueError, lambda: (self.df.key <= 10) and (self.df.value <= "2"))
        self.assertEqual(14, self.df.filter((self.df.key <= 3) | (self.df.value < "2")).count())
        self.assertRaises(ValueError, lambda: self.df.key <= 3 or self.df.value < "2")
        self.assertEqual(99, self.df.filter(~(self.df.key == 1)).count())
        self.assertRaises(ValueError, lambda: not self.df.key == 1)

    def test_validate_column_types(self):
        from pyspark.sql.functions import udf, to_json
        from pyspark.sql.classic.column import _to_java_column

        self.assertTrue("Column" in _to_java_column("a").getClass().toString())
        self.assertTrue("Column" in _to_java_column("a").getClass().toString())
        self.assertTrue("Column" in _to_java_column(self.spark.range(1).id).getClass().toString())

        with self.assertRaises(PySparkTypeError) as pe:
            _to_java_column(1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "Column or str",
                "arg_name": "col",
                "arg_type": "int",
            },
        )

        class A:
            pass

        self.assertRaises(TypeError, lambda: _to_java_column(A()))
        self.assertRaises(TypeError, lambda: _to_java_column([]))

        with self.assertRaises(PySparkTypeError) as pe:
            udf(lambda x: x)(None)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "Column or str",
                "arg_name": "col",
                "arg_type": "NoneType",
            },
        )
        self.assertRaises(TypeError, lambda: to_json(1))

    def test_column_operators(self):
        ci = self.df.key
        cs = self.df.value
        ci == cs
        self.assertTrue(isinstance((-ci - 1 - 2) % 3 * 2.5 / 3.5, Column))
        rcc = (1 + ci), (1 - ci), (1 * ci), (1 / ci), (1 % ci), (1**ci), (ci**1)
        self.assertTrue(all(isinstance(c, Column) for c in rcc))
        cb = [ci == 5, ci != 0, ci > 3, ci < 4, ci >= 0, ci <= 7]
        self.assertTrue(all(isinstance(c, Column) for c in cb))
        cbool = (ci & ci), (ci | ci), (~ci)
        self.assertTrue(all(isinstance(c, Column) for c in cbool))
        css = (
            cs.contains("a"),
            cs.like("a"),
            cs.rlike("a"),
            cs.ilike("A"),
            cs.asc(),
            cs.desc(),
            cs.startswith("a"),
            cs.endswith("a"),
            ci.eqNullSafe(cs),
            sf.col("b") & sf.lit(True),
            sf.col("b") & True,
            sf.lit(True) & sf.col("b"),
            True & sf.col("b"),
            sf.col("b") | sf.lit(True),
            sf.col("b") | True,
            sf.lit(True) | sf.col("b"),
            True | sf.col("b"),
        )
        self.assertTrue(all(isinstance(c, Column) for c in css))
        self.assertTrue(isinstance(ci.cast(LongType()), Column))
        self.assertRaisesRegex(
            ValueError, "Cannot apply 'in' operator against a column", lambda: 1 in cs
        )

    def test_column_date_time_op(self):
        query = """
            SELECT * FROM VALUES
            (TIME('00:00:00'), 1),
            (TIME('01:02:03'), 2),
            (TIME('11:12:13'), 3)
            AS tab(t, i)
            """

        df = self.spark.sql(query)

        res1 = df.select("i").where(sf.col("t") < datetime.time(3, 0, 0))
        self.assertEqual([r.i for r in res1.collect()], [1, 2])

        res2 = df.select("i").where(sf.col("t") > datetime.time(1, 0, 0))
        self.assertEqual([r.i for r in res2.collect()], [2, 3])

        res3 = df.select("i").where(sf.col("t") == datetime.time(0, 0, 0))
        self.assertEqual([r.i for r in res3.collect()], [1])

    def test_column_accessor(self):
        from pyspark.sql.functions import col

        self.assertIsInstance(col("foo")[1:3], Column)
        self.assertIsInstance(col("foo")[0], Column)
        self.assertIsInstance(col("foo")["bar"], Column)
        self.assertRaises(ValueError, lambda: col("foo")[0:10:2])

    def test_column_select(self):
        df = self.df
        self.assertEqual(self.testData, df.select("*").collect())
        self.assertEqual(self.testData, df.select(df.key, df.value).collect())
        self.assertEqual([Row(value="1")], df.where(df.key == 1).select(df.value).collect())

    def test_access_column(self):
        df = self.df
        self.assertTrue(isinstance(df.key, Column))
        self.assertTrue(isinstance(df["key"], Column))
        self.assertTrue(isinstance(df[0], Column))
        self.assertRaises(IndexError, lambda: df[2])
        self.assertRaises(TypeError, lambda: df[{}])
        self.assertRaises(AnalysisException, lambda: df.select(df["bad_key"]).schema)

    def test_column_name_with_non_ascii(self):
        columnName = "数量"
        self.assertTrue(isinstance(columnName, str))
        schema = StructType([StructField(columnName, LongType(), True)])
        df = self.spark.createDataFrame([(1,)], schema)
        self.assertEqual(schema, df.schema)
        self.assertEqual("DataFrame[数量: bigint]", str(df))
        self.assertEqual([("数量", "bigint")], df.dtypes)
        self.assertEqual(1, df.select("数量").first()[0])
        self.assertEqual(1, df.select(df["数量"]).first()[0])
        self.assertTrue(columnName in repr(df[columnName]))

    def test_field_accessor(self):
        df = self.spark.createDataFrame([Row(l=[1], r=Row(a=1, b="b"), d={"k": "v"})])
        self.assertEqual(1, df.select(df.l[0]).first()[0])
        self.assertEqual(1, df.select(df.r["a"]).first()[0])
        self.assertEqual(1, df.select(df["r.a"]).first()[0])
        self.assertEqual("b", df.select(df.r["b"]).first()[0])
        self.assertEqual("b", df.select(df["r.b"]).first()[0])
        self.assertEqual("v", df.select(df.d["k"]).first()[0])

    def test_bitwise_operations(self):
        from pyspark.sql import functions

        row = Row(a=170, b=75)
        df = self.spark.createDataFrame([row])
        result = df.select(df.a.bitwiseAND(df.b)).collect()[0].asDict()
        self.assertEqual(170 & 75, result["(a & b)"])
        result = df.select(df.a.bitwiseOR(df.b)).collect()[0].asDict()
        self.assertEqual(170 | 75, result["(a | b)"])
        result = df.select(df.a.bitwiseXOR(df.b)).collect()[0].asDict()
        self.assertEqual(170 ^ 75, result["(a ^ b)"])
        result = df.select(functions.bitwiseNOT(df.b)).collect()[0].asDict()
        self.assertEqual(~75, result["~b"])
        result = df.select(functions.bitwise_not(df.b)).collect()[0].asDict()
        self.assertEqual(~75, result["~b"])

    def test_with_field(self):
        from pyspark.sql.functions import lit, col

        df = self.spark.createDataFrame([Row(a=Row(b=1, c=2))])
        self.assertIsInstance(df["a"].withField("b", lit(3)), Column)
        self.assertIsInstance(df["a"].withField("d", lit(3)), Column)
        result = df.withColumn("a", df["a"].withField("d", lit(3))).collect()[0].asDict()
        self.assertEqual(3, result["a"]["d"])
        result = df.withColumn("a", df["a"].withField("b", lit(3))).collect()[0].asDict()
        self.assertEqual(3, result["a"]["b"])

        with self.assertRaises(PySparkTypeError) as pe:
            df["a"].withField("b", 3)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={"expected_type": "Column", "arg_name": "col", "arg_type": "int"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df["a"].withField(col("b"), lit(3))

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "str",
                "arg_name": "fieldName",
                "arg_type": "Column",
            },
        )

    def test_drop_fields(self):
        df = self.spark.createDataFrame([Row(a=Row(b=1, c=2, d=Row(e=3, f=4)))])
        self.assertIsInstance(df["a"].dropFields("b"), Column)
        self.assertIsInstance(df["a"].dropFields("b", "c"), Column)
        self.assertIsInstance(df["a"].dropFields("d.e"), Column)

        result = (
            df.select(
                df["a"].dropFields("b").alias("a1"),
                df["a"].dropFields("d.e").alias("a2"),
            )
            .first()
            .asDict(True)
        )

        self.assertTrue("b" not in result["a1"] and "c" in result["a1"] and "d" in result["a1"])

        self.assertTrue("e" not in result["a2"]["d"] and "f" in result["a2"]["d"])

    def test_getitem_column(self):
        mapping = {"A": "20", "B": "28", "C": "34"}
        mapping_expr = sf.create_map([sf.lit(x) for x in chain(*mapping.items())])
        df = self.spark.createDataFrame(
            data=[["A", "10"], ["B", "14"], ["C", "17"]],
            schema=["key", "value"],
        ).withColumn("square_value", mapping_expr[sf.col("key")])
        self.assertEqual(df.count(), 3)

    def test_alias_metadata(self):
        df = self.spark.createDataFrame([("",)], ["a"])
        df = df.withMetadata("a", {"foo": "bar"})
        self.assertEqual(df.schema["a"].metadata, {"foo": "bar"})

        # SPARK-51426: Ensure setting metadata to '{]' clears it
        df = df.select([sf.col("a").alias("a", metadata={})])
        self.assertEqual(df.schema["a"].metadata, {})

        df = df.withMetadata("a", {"baz": "burr"})
        self.assertEqual(df.schema["a"].metadata, {"baz": "burr"})

        df = df.withMetadata("a", {})
        self.assertEqual(df.schema["a"].metadata, {})

    def test_alias_negative(self):
        with self.assertRaises(PySparkValueError) as pe:
            self.spark.range(1).id.alias("a", "b", metadata={})

        self.check_error(
            exception=pe.exception,
            errorClass="ONLY_ALLOWED_FOR_SINGLE_COLUMN",
            messageParameters={"arg_name": "metadata"},
        )

    def test_cast_str_representation(self):
        self.assertEqual(str(sf.col("a").cast("int")), "Column<'CAST(a AS INT)'>")
        self.assertEqual(str(sf.col("a").cast("INT")), "Column<'CAST(a AS INT)'>")
        self.assertEqual(str(sf.col("a").cast(IntegerType())), "Column<'CAST(a AS INT)'>")
        self.assertEqual(str(sf.col("a").cast(LongType())), "Column<'CAST(a AS BIGINT)'>")

        self.assertEqual(str(sf.col("a").try_cast("int")), "Column<'TRY_CAST(a AS INT)'>")
        self.assertEqual(str(sf.col("a").try_cast("INT")), "Column<'TRY_CAST(a AS INT)'>")
        self.assertEqual(str(sf.col("a").try_cast(IntegerType())), "Column<'TRY_CAST(a AS INT)'>")
        self.assertEqual(str(sf.col("a").try_cast(LongType())), "Column<'TRY_CAST(a AS BIGINT)'>")

    def test_cast_negative(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.range(1).id.cast(123)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "DataType or str",
                "arg_name": "dataType",
                "arg_type": "int",
            },
        )

    def test_over_negative(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.range(1).id.over(123)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "WindowSpec",
                "arg_name": "window",
                "arg_type": "int",
            },
        )

    def test_eqnullsafe_classmethod_usage(self):
        df = self.spark.range(1)
        self.assertEqual(df.select(Column.eqNullSafe(df.id, df.id)).first()[0], True)

    def test_isinstance_dataframe(self):
        self.assertIsInstance(self.spark.range(1).id, Column)

    def test_expr_str_representation(self):
        expression = sf.expr("foo")
        when_cond = sf.when(expression, sf.lit(None))
        self.assertEqual(str(when_cond), "Column<'CASE WHEN foo THEN NULL END'>")

    def test_col_field_ops_representation(self):
        # SPARK-49894: Test string representation of columns
        c = sf.col("c")

        # getField
        self.assertEqual(str(c.x), "Column<'c['x']'>")
        self.assertEqual(str(c.x.y), "Column<'c['x']['y']'>")
        self.assertEqual(str(c.x.y.z), "Column<'c['x']['y']['z']'>")

        self.assertEqual(str(c["x"]), "Column<'c['x']'>")
        self.assertEqual(str(c["x"]["y"]), "Column<'c['x']['y']'>")
        self.assertEqual(str(c["x"]["y"]["z"]), "Column<'c['x']['y']['z']'>")

        self.assertEqual(str(c.getField("x")), "Column<'c['x']'>")
        self.assertEqual(
            str(c.getField("x").getField("y")),
            "Column<'c['x']['y']'>",
        )
        self.assertEqual(
            str(c.getField("x").getField("y").getField("z")),
            "Column<'c['x']['y']['z']'>",
        )

        self.assertEqual(str(c.getItem("x")), "Column<'c['x']'>")
        self.assertEqual(
            str(c.getItem("x").getItem("y")),
            "Column<'c['x']['y']'>",
        )
        self.assertEqual(
            str(c.getItem("x").getItem("y").getItem("z")),
            "Column<'c['x']['y']['z']'>",
        )

        self.assertEqual(
            str(c.x["y"].getItem("z")),
            "Column<'c['x']['y']['z']'>",
        )
        self.assertEqual(
            str(c["x"].getField("y").getItem("z")),
            "Column<'c['x']['y']['z']'>",
        )
        self.assertEqual(
            str(c.getField("x").getItem("y").z),
            "Column<'c['x']['y']['z']'>",
        )
        self.assertEqual(
            str(c["x"].y.getField("z")),
            "Column<'c['x']['y']['z']'>",
        )

        # WithField
        self.assertEqual(
            str(c.withField("x", sf.col("y"))),
            "Column<'update_field(c, x, y)'>",
        )
        self.assertEqual(
            str(c.withField("x", sf.col("y")).withField("x", sf.col("z"))),
            "Column<'update_field(update_field(c, x, y), x, z)'>",
        )

        # DropFields
        self.assertEqual(str(c.dropFields("x")), "Column<'drop_field(c, x)'>")
        self.assertEqual(
            str(c.dropFields("x", "y")),
            "Column<'drop_field(drop_field(c, x), y)'>",
        )
        self.assertEqual(
            str(c.dropFields("x", "y", "z")),
            "Column<'drop_field(drop_field(drop_field(c, x), y), z)'>",
        )

    def test_lit_time_representation(self):
        dt = datetime.date(2021, 3, 4)
        self.assertEqual(str(sf.lit(dt)), "Column<'2021-03-04'>")

        ts = datetime.datetime(2021, 3, 4, 12, 34, 56, 1234)
        self.assertEqual(str(sf.lit(ts)), "Column<'2021-03-04 12:34:56.001234'>")

        ts = datetime.time(12, 34, 56, 1234)
        self.assertEqual(str(sf.lit(ts)), "Column<'12:34:56.001234'>")

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_lit_delta_representation(self):
        for delta in [
            datetime.timedelta(days=1),
            datetime.timedelta(hours=2),
            datetime.timedelta(minutes=3),
            datetime.timedelta(seconds=4),
            datetime.timedelta(microseconds=5),
            datetime.timedelta(days=2, hours=21, microseconds=908),
            datetime.timedelta(days=1, minutes=-3, microseconds=-1001),
            datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=5),
        ]:
            import pandas as pd

            # Column<'PT69H0.000908S'> or Column<'P2DT21H0M0.000908S'>
            s = str(sf.lit(delta))

            # Parse the ISO string representation and compare
            self.assertTrue(pd.Timedelta(s[8:-2]).to_pytimedelta() == delta)

    def test_enum_literals(self):
        class IntEnum(Enum):
            X = 1
            Y = 2
            Z = 3

        class BoolEnum(Enum):
            T = True

        class StrEnum(Enum):
            X = "x"

        id = sf.col("id")
        s = sf.col("s")
        b = sf.col("b")

        cols, expected = list(
            zip(
                (id + IntEnum.X, 2),
                (id - IntEnum.X, 0),
                (id * IntEnum.X, 1),
                (id / IntEnum.X, 1.0),
                (id % IntEnum.X, 0),
                (IntEnum.X + id, 2),
                (IntEnum.X - id, 0),
                (IntEnum.X * id, 1),
                (IntEnum.X / id, 1.0),
                (IntEnum.X % id, 0),
                (id**IntEnum.X, 1.0),
                (IntEnum.X**id, 1, 0),
                (id == IntEnum.X, True),
                (IntEnum.X == id, True),
                (id < IntEnum.X, False),
                (id <= IntEnum.X, True),
                (id >= IntEnum.X, True),
                (id > IntEnum.X, False),
                (id.eqNullSafe(IntEnum.X), True),
                (b & BoolEnum.T, True),
                (b | BoolEnum.T, True),
                (BoolEnum.T & b, True),
                (BoolEnum.T | b, True),
                (id.bitwiseOR(IntEnum.X), 1),
                (id.bitwiseAND(IntEnum.X), 1),
                (id.bitwiseXOR(IntEnum.X), 0),
                (id.contains(IntEnum.X), True),
                (s.startswith(StrEnum.X), False),
                (s.endswith(StrEnum.X), False),
                (s.like(StrEnum.X), False),
                (s.rlike(StrEnum.X), False),
                (s.ilike(StrEnum.X), False),
                (s.substr(IntEnum.X, IntEnum.Y), "1"),
                (sf.when(b, IntEnum.X).when(~b, IntEnum.Y).otherwise(IntEnum.Z), 1),
            )
        )
        result = (
            self.spark.range(1, 2)
            .select(id, id.astype("string").alias("s"), id.astype("boolean").alias("b"))
            .select(*cols)
            .first()
        )

        for r, c, e in zip(result, cols, expected):
            self.assertEqual(r, e, str(c))

    def test_transform(self):
        # Test with built-in functions
        df = self.spark.createDataFrame([("  hello  ",), ("  world  ",)], ["text"])
        result = df.select(df.text.transform(sf.trim).transform(sf.upper)).collect()
        self.assertEqual(result[0][0], "HELLO")
        self.assertEqual(result[1][0], "WORLD")

        # Test with lambda functions
        df = self.spark.createDataFrame([(10,), (20,), (30,)], ["value"])
        result = df.select(
            df.value.transform(lambda c: c + 5)
            .transform(lambda c: c * 2)
            .transform(lambda c: c - 10)
        ).collect()
        self.assertEqual(result[0][0], 20)
        self.assertEqual(result[1][0], 40)
        self.assertEqual(result[2][0], 60)

    def test_self_join(self):
        df1 = self.spark.range(10).withColumn("a", sf.lit(0))
        df2 = df1.withColumnRenamed("a", "b")
        df = df1.join(df2, df1["a"] == df2["b"])
        self.assertTrue(df.count() == 100)
        df = df2.join(df1, df2["b"] == df1["a"])
        self.assertTrue(df.count() == 100)

    def test_self_join_II(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], schema=["a", "b"])
        df2 = df.select(df.a.alias("aa"), df.b)
        df3 = df2.join(df, df2.b == df.b)
        self.assertTrue(df3.columns, ["aa", "b", "a", "b"])
        self.assertTrue(df3.count() == 2)

    def test_self_join_III(self):
        df1 = self.spark.range(10).withColumn("value", sf.lit(1))
        df2 = df1.union(df1)
        df3 = df1.join(df2, df1.id == df2.id, "left")
        self.assertTrue(df3.columns, ["id", "value", "id", "value"])
        self.assertTrue(df3.count() == 20)

    def test_self_join_IV(self):
        df1 = self.spark.range(10).withColumn("value", sf.lit(1))
        df2 = df1.withColumn("value", sf.lit(2)).union(df1.withColumn("value", sf.lit(3)))
        df3 = df1.join(df2, df1.id == df2.id, "right")
        self.assertTrue(df3.columns, ["id", "value", "id", "value"])
        self.assertTrue(df3.count() == 20)

    def test_select_join_keys(self):
        df1 = self.spark.range(10).withColumn("v1", sf.lit(1))
        df2 = self.spark.range(10).withColumn("v2", sf.lit(2))
        for how in ["inner", "left", "right", "full", "cross"]:
            self.assertTrue(df1.join(df2, "id", how).select(df1["id"]).count() >= 0, how)
            self.assertTrue(df1.join(df2, "id", how).select(df2["id"]).count() >= 0, how)

    def test_select_regular_column_with_reused_dataframe_hidden_in_natural_join(self):
        # A DataFrame appears both as a direct join side and inside a natural/USING
        # join that hides one of its columns into `metadataOutput`. When resolving
        # `df2["id"]`, two candidates match the plan id: one from `p.output` (the
        # direct join side) and one only visible via `p.metadataOutput` (the reused
        # `df2` nested under the USING-join wrapper). We should prefer the regular
        # candidate and not throw AMBIGUOUS_COLUMN_REFERENCE.
        df1 = self.spark.createDataFrame([(10, "T1"), (20, "T2")], ["key", "val"])
        df2 = self.spark.createDataFrame([(10,), (20,), (30,)], ["id"])
        # The second row's id (99) does not match any df2 row, so the USING
        # left-join in `enriched` produces NULL on the df2 side for val "T2".
        # If `df2["id"]` were resolved to the hidden (USING-wrapper) candidate,
        # the second row would yield NULL instead of 20, and the assertion below
        # would fail. This pins resolution to the direct-side `id`.
        df3 = self.spark.createDataFrame([(10, "T1"), (99, "T2")], ["id", "val"])
        enriched = df3.join(df2, "id", "left")
        result = (
            df1.join(df2, df1["key"] == df2["id"], "left")
            .join(enriched, "val", "full_outer")
            .sort("val")
            .select(df2["id"])
        )
        self.assertEqual(
            [r["id"] for r in result.collect()],
            [10, 20],
        )

    def test_drop_notexistent_col(self):
        df1 = self.spark.createDataFrame(
            [("a", "b", "c")],
            schema="colA string, colB string, colC string",
        )
        df2 = self.spark.createDataFrame(
            [("c", "d", "e")],
            schema="colC string, colD string, colE string",
        )
        df3 = df1.join(df2, df1["colC"] == df2["colC"]).withColumn(
            "colB",
            sf.when(
                df1["colB"] == "b", sf.concat(df1["colB"].cast("string"), sf.lit("x"))
            ).otherwise(df1["colB"]),
        )
        df4 = df3.drop(df1["colB"])

        self.assertEqual(df4.columns, ["colA", "colB", "colC", "colC", "colD", "colE"])
        self.assertEqual(df4.count(), 1)

    # --- Mixed-surface layered DataFrame programs ---------------------------
    #
    # These tests chain multiple DataFrame transformations - semi-joins
    # (for SQL EXISTS/IN), window functions, cube aggregations, UDFs and
    # struct field access - into 4-5 layer pipelines, then reference the
    # final layered DataFrame's columns via ``layered.col`` in both filter
    # and select at the outermost surface. The goal is to catch regressions
    # in plan-id propagation across analyzer rules that single-operator
    # tests miss when rules interact.

    def test_layered_semijoin_groupby_window(self):
        # 4-layer DataFrame pipeline: filter -> semi-join -> groupBy/agg
        # -> window functions. ``layered.col`` references appear in both
        # filter and select at the outermost surface.
        events_data = [
            (1, 1, "Books", 100.0, 2, True),
            (2, 1, "Books", 50.0, 3, True),
            (3, 2, "Electronics", 200.0, 1, True),
            (4, 2, "Electronics", 300.0, 2, True),
            (5, 3, "Home", 80.0, 4, True),
            (6, 4, "Books", 60.0, 1, False),
        ]
        users_data = [(1, 25), (2, 30), (3, 22), (4, 18)]
        events_cols = ["id", "user_id", "category", "amount", "quantity", "is_active"]
        users_cols = ["id", "age"]

        events = self.spark.createDataFrame(events_data, events_cols)
        users = self.spark.createDataFrame(users_data, users_cols)
        # Layer 1: filter + semi-join (DataFrame-API equivalent of
        # WHERE is_active AND EXISTS (user with age > 20)).
        active = events.where(events.is_active).join(
            users.where(users.age > 20),
            events.user_id == users.id,
            "left_semi",
        )
        # Layer 2: groupBy + agg, then post-agg filter (HAVING equivalent).
        agg = active.groupBy("category").agg(
            sf.sum(active.amount * active.quantity * sf.lit(0.1)).alias("total_amt"),
            sf.sum(active.amount).alias("amount_sum"),
        )
        totals = agg.where(agg.amount_sum > 50).select("category", "total_amt")
        # Layer 3: window functions on top of the aggregate.
        running = Window.orderBy("total_amt").rowsBetween(-1, 1)
        ranking = Window.orderBy(totals.total_amt.desc())
        windowed = totals.select(
            "category",
            "total_amt",
            sf.avg(totals.total_amt).over(running).alias("running_avg"),
            sf.rank().over(ranking).alias("rank_num"),
        )
        # Layer 4: outer filter.
        layered = windowed.where(windowed.rank_num <= 5)

        rows = (
            layered.filter(layered.rank_num <= 3)
            .select(
                layered.category,
                layered.total_amt,
                layered.running_avg,
                layered.rank_num,
            )
            .collect()
        )
        result = sorted((r.category, r.rank_num) for r in rows)
        self.assertEqual(result, [("Books", 2), ("Electronics", 1), ("Home", 3)])

    def test_layered_struct_semijoin_cube_ntile(self):
        # 5-layer DataFrame pipeline: filter -> semi-join -> struct field
        # access -> cube aggregation -> window NTILE. ``layered.col``
        # references appear in both filter and select at the outermost
        # surface.
        events_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("category", StringType()),
                StructField("status", StringType()),
                StructField("amount", IntegerType()),
                StructField("quantity", IntegerType()),
                StructField(
                    "detail",
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("nested", StructType([StructField("x", IntegerType())])),
                        ]
                    ),
                ),
            ]
        )
        events_data = [
            (1, "Books", "A", 100, 5, ("alpha", (1,))),
            (2, "Electronics", "B", 200, 3, ("beta", (2,))),
            (3, "Books", "A", 50, 7, ("alpha", (1,))),
            (4, "Electronics", "B", 300, 4, ("beta", (2,))),
            (5, "Home", "C", 80, 2, ("gamma", (3,))),
        ]
        categories_data = [("Books", 1), ("Electronics", 2), ("Home", 3), ("Toys", 5)]
        categories_cols = ["name", "priority"]

        events = self.spark.createDataFrame(events_data, events_schema)
        categories = self.spark.createDataFrame(categories_data, categories_cols)
        # Layer 1: filter + semi-join (DataFrame-API equivalent of
        # WHERE quantity > 1 AND category IN (SELECT ...)).
        filtered = events.where(events.quantity > 1).join(
            categories.where(categories.priority <= 3),
            events.category == categories.name,
            "left_semi",
        )
        # Layer 2: project with struct field access (struct subfields use
        # bracket access since ``detail.name`` would hit ``Column.name``).
        base = filtered.select(
            filtered.id,
            filtered.category,
            filtered.status,
            filtered.amount,
            filtered.detail["name"].alias("detail_name"),
            filtered.detail["nested"]["x"].alias("nx"),
        )
        # Layer 3: cube aggregation (mixed grouping levels - similar
        # surface area to SQL GROUPING SETS without an exact equivalent
        # in the DataFrame API).
        agg = base.cube("category", "status", "detail_name").agg(
            sf.sum(base.amount).alias("total"), sf.count(sf.lit(1)).alias("cnt")
        )
        grouped = agg.where(agg.category.isNotNull() & agg.status.isNotNull())
        # Layer 4: NTILE window.
        tiled = grouped.withColumn("tile", sf.ntile(2).over(Window.orderBy(grouped.total.desc())))
        # Layer 5: outer filter.
        layered = tiled.where(tiled.tile <= 2)

        rows = (
            layered.filter(layered.tile >= 1)
            .select(
                layered.category,
                layered.status,
                layered.detail_name,
                layered.total,
                layered.cnt,
                layered.tile,
            )
            .collect()
        )
        # Cube emits one (category, status, detail_name) group per distinct
        # combination plus one (category, status, NULL) rollup per distinct
        # (category, status) pair. The where filter keeps both.
        self.assertEqual(len(rows), 6)
        self.assertEqual({r.category for r in rows}, {"Books", "Electronics", "Home"})
        self.assertEqual({r.total for r in rows}, {80, 150, 500})
        self.assertEqual({r.tile for r in rows}, {1, 2})

    def test_layered_window_window_udf(self):
        # 4-layer DataFrame pipeline: filter -> running-total window ->
        # per-partition max window -> UDF wrap. ``layered.col`` references
        # appear in both filter and select at the outermost surface.
        data = [
            (1, "A", 100),
            (2, "A", 200),
            (3, "B", 150),
            (4, "B", 250),
            (5, "C", 50),
        ]
        cols = ["id", "category", "amount"]

        df = self.spark.createDataFrame(data, cols)
        # Layer 1: filter (replaces WHERE EXISTS amount > 0).
        filtered = df.where(df.amount > 0)
        # Layer 2: running total window.
        run_w = Window.partitionBy("category").orderBy("id")
        with_run = filtered.withColumn("run_amt", sf.sum(filtered.amount).over(run_w))
        # Layer 3: per-category max window (replaces correlated subquery
        # for cat_max).
        cat_w = Window.partitionBy("category")
        with_max = with_run.withColumn("cat_max", sf.max(with_run.amount).over(cat_w))
        # Layer 4: UDF.
        double = sf.udf(lambda x: x * 2 if x is not None else None, IntegerType())
        layered = with_max.withColumn("doubled_amt", double(with_max.amount))

        rows = (
            layered.filter(layered.amount > 0)
            .select(
                layered.id,
                layered.category,
                layered.amount,
                layered.run_amt,
                layered.cat_max,
                layered.doubled_amt,
            )
            .collect()
        )
        result = sorted(
            (r.id, r.category, r.amount, r.run_amt, r.cat_max, r.doubled_amt) for r in rows
        )
        self.assertEqual(
            result,
            [
                (1, "A", 100, 100, 200, 200),
                (2, "A", 200, 300, 200, 400),
                (3, "B", 150, 150, 250, 300),
                (4, "B", 250, 400, 250, 500),
                (5, "C", 50, 50, 50, 100),
            ],
        )

    # --- Tagged DataFrame column resolution --------------------------------
    #
    # ``df.col`` / ``df["col"]`` carries the source DataFrame's plan id. These
    # tests pin how that tagged reference resolves after assorted operators.
    # The behavior is shared across Spark Classic and Spark Connect (both
    # ``spark.sql.analyzer.strictDataFrameColumnResolution`` modes) except for
    # a few diverging cases, which are overridden in the Connect parity suites
    # (``ColumnParityTests`` / ``...WithNonStrictDFColResolution``):
    #
    #   * the shadowing trio - Classic and Connect strict raise, Connect
    #     lenient resolves the shadowed name via name-based fallback;
    #   * union - Classic resolves via attribute-id propagation, Connect
    #     raises in both modes.

    def test_resolve_after_chained_withcolumn_shadow(self):
        # Two consecutive withColumn calls each shadow `c` with a new
        # attribute of the same name, so the original `c` leaves the
        # projection and the tagged `df.c` cannot resolve.
        # Connect lenient diverges: name-based fallback resolves the
        # shadowed name (overridden in the lenient parity suite).
        df = self.spark.sql("SELECT 1 AS c")
        with self.assertRaises(AnalysisException):
            df.withColumn("c", sf.col("c").cast("string")).withColumn(
                "c", sf.col("c").cast("int")
            ).select(df.c).collect()

    def test_resolve_after_select_alias_shadow(self):
        # Same shadowing shape as withColumn but via select + alias.
        # Connect lenient diverges: name-based fallback resolves the
        # shadowed name (overridden in the lenient parity suite).
        df = self.spark.sql("SELECT 1 AS c")
        with self.assertRaises(AnalysisException):
            df.select(df.c.cast("string").alias("c")).select(df.c).collect()

    def test_resolve_after_withcolumnrenamed(self):
        # withColumnRenamed drops the original `c` attribute and projects it
        # as `c2`; the tagged `df.c` matches neither the original attribute
        # nor a current column named `c`, so all modes raise.
        df = self.spark.sql("SELECT 1 AS c")
        with self.assertRaises(AnalysisException):
            df.withColumnRenamed("c", "c2").select(df.c).collect()

    def test_resolve_after_drop(self):
        # drop("c") removes the column entirely; the tagged `df.c` cannot
        # resolve under any mode.
        df = self.spark.sql("SELECT 1 AS c, 2 AS d")
        with self.assertRaises(AnalysisException):
            df.drop("c").select(df.c).collect()

    def test_resolve_through_filter(self):
        # filter is a pass-through operator: the child Project's attributes
        # flow through unchanged, so the tagged reference resolves.
        df = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
        rows = df.filter(df.c > 0).select(df.c).collect()
        self.assertEqual(sorted(r.c for r in rows), [1, 2])

    def test_resolve_through_sort(self):
        # sort is also a pass-through operator.
        df = self.spark.sql("SELECT 2 AS c UNION ALL SELECT 1 AS c")
        rows = df.sort(df.c).select(df.c).collect()
        self.assertEqual([r.c for r in rows], [1, 2])

    def test_resolve_through_distinct(self):
        # distinct preserves attribute identity for column resolution.
        df = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 1 AS c")
        rows = df.distinct().select(df.c).collect()
        self.assertEqual([r.c for r in rows], [1])

    def test_resolve_after_groupby_count(self):
        # groupBy("c").count() preserves the grouping key's attribute id, so
        # the tagged reference resolves.
        df = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 1 AS c UNION ALL SELECT 2 AS c")
        rows = df.groupBy("c").count().select(df.c).collect()
        self.assertEqual(sorted(r.c for r in rows), [1, 2])

    def test_resolve_after_agg_alias_shadow(self):
        # An aggregate output aliased `c` collides by name with the source
        # `c`, but the tagged `df.c` still references the aggregated-away
        # source attribute, so it cannot resolve.
        # Connect lenient diverges: name-based fallback resolves the
        # aliased name (overridden in the lenient parity suite).
        df = self.spark.sql("SELECT 1 AS c")
        with self.assertRaises(AnalysisException):
            df.groupBy().agg(sf.sum("c").alias("c")).select(df.c).collect()

    def test_resolve_after_pivot(self):
        # pivot preserves the grouping key's attribute id, so the tagged
        # reference resolves.
        df = self.spark.sql(
            "SELECT 1 AS c, 'a' AS k, 10 AS v UNION ALL SELECT 2 AS c, 'b' AS k, 20 AS v"
        )
        rows = df.groupBy("c").pivot("k").sum("v").select(df.c).collect()
        self.assertEqual(sorted(r.c for r in rows), [1, 2])

    def test_resolve_after_union(self):
        # Union emits new attribute ids. Classic resolves the tagged
        # left-side reference by attribute-id propagation and succeeds;
        # Connect treats Union as a leaf when walking the plan tree for
        # plan-id resolution and raises in both modes (overridden there).
        df1 = self.spark.sql("SELECT 1 AS c")
        df2 = self.spark.sql("SELECT 2 AS c")
        rows = df1.union(df2).select(df1.c).collect()
        self.assertEqual(sorted(r.c for r in rows), [1, 2])

    def test_resolve_after_intersect(self):
        # intersect, like union, emits new attribute ids, but the propagated
        # id is retained in the output, so the tagged reference resolves in
        # all modes.
        df1 = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
        df2 = self.spark.sql("SELECT 2 AS c UNION ALL SELECT 3 AS c")
        rows = df1.intersect(df2).select(df1.c).collect()
        self.assertEqual([r.c for r in rows], [2])

    def test_resolve_self_join_alias(self):
        # Both self-join sides originate from the same plan-id-tagged
        # ancestor, yielding two equal-depth candidates with the same
        # attribute id. Disambiguation cannot tiebreak and all modes raise
        # an ambiguous-reference error.
        df = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
        a, b = df.alias("a"), df.alias("b")
        with self.assertRaises(AnalysisException):
            a.join(b, a.c == b.c).select(df.c).collect()

    def test_resolve_after_subquery_view(self):
        # Persisting the DataFrame as a temp view and reading it back via
        # table() produces a new plan; the tagged reference still resolves in
        # all modes.
        view = f"v_{uuid.uuid4().hex}"
        df = self.spark.sql("SELECT 1 AS c")
        df.createOrReplaceTempView(view)
        try:
            rows = self.spark.table(view).select(df.c).collect()
            self.assertEqual([r.c for r in rows], [1])
        finally:
            self.spark.sql(f"DROP VIEW IF EXISTS {view}")

    def test_resolve_cross_dataframe_illegal_reference(self):
        # Referencing a column from a DataFrame whose plan id is not an
        # ancestor of the target plan (`df1.select(df2.id)`) fails in all
        # modes; the strict / lenient switch does not gate this throw.
        df1 = self.spark.range(3)
        df2 = self.spark.range(5)
        with self.assertRaises(AnalysisException):
            df1.select(df2.id).collect()

    def test_resolve_df_star(self):
        # `df["*"]` is an UnresolvedDataFrameStar carrying df's plan id; the
        # analyzer expands it to the matched node's output in all modes.
        df = self.spark.sql(
            "SELECT 'Books' AS c, 100 AS v UNION ALL SELECT 'Electronics' AS c, 200 AS v"
        )
        rows = df.select(df["*"]).collect()
        self.assertEqual(sorted((r.c, r.v) for r in rows), [("Books", 100), ("Electronics", 200)])

    def test_resolve_self_join_withcolumnrenamed(self):
        # Documented ColumnResolutionHelper case: df1 = range(10) + col `a`;
        # df2 = df1 renamed `a` -> `b`; df1.join(df2, df1.a == df2.b). The
        # node with df1's plan id is found on both Join sides; the right
        # candidate is filtered out because its `a` is not in the renaming
        # Project's output, so disambiguation succeeds in all modes.
        df1 = self.spark.range(10).withColumn("a", sf.col("id"))
        df2 = df1.withColumnRenamed("a", "b")
        rows = df1.join(df2, df1.a == df2.b).select(df1.a, df2.b).collect()
        self.assertEqual(len(rows), 10)

    def test_resolve_sort_missing_attr_recovery(self):
        # Documented ColumnResolutionHelper case: df.select(df.v).sort(df.id)
        # where df.id is not in the select's output. The analyzer descends
        # through the Project, resolves df.id via plan id at the source, and
        # adds it back to the upstream projection. Works in all modes.
        df = self.spark.range(10).withColumn("v", sf.col("id") + 1)
        rows = df.select(df.v).sort(df.id).collect()
        self.assertEqual(len(rows), 10)


class ColumnTests(ColumnTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
