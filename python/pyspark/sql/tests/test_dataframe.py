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

import pydoc
import shutil
import tempfile
import unittest
from typing import cast
import io
from contextlib import redirect_stdout

from pyspark.sql import Row, functions, DataFrame
from pyspark.sql.functions import col, lit, count, struct
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    StructType,
    StructField,
)
from pyspark.storagelevel import StorageLevel
from pyspark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkTypeError,
    PySparkValueError,
)
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    have_pandas,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


class DataFrameTestsMixin:
    def test_range(self):
        self.assertEqual(self.spark.range(1, 1).count(), 0)
        self.assertEqual(self.spark.range(1, 0, -1).count(), 1)
        self.assertEqual(self.spark.range(0, 1 << 40, 1 << 39).count(), 2)
        self.assertEqual(self.spark.range(-2).count(), 0)
        self.assertEqual(self.spark.range(3).count(), 3)

    def test_table(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.table(None)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={"arg_name": "tableName", "arg_type": "NoneType"},
        )

    def test_dataframe_star(self):
        df1 = self.spark.createDataFrame([{"a": 1}])
        df2 = self.spark.createDataFrame([{"a": 1, "b": "v"}])
        df3 = df2.withColumnsRenamed({"a": "x", "b": "y"})

        df = df1.join(df2)
        self.assertEqual(df.columns, ["a", "a", "b"])
        self.assertEqual(df.select(df1["*"]).columns, ["a"])
        self.assertEqual(df.select(df2["*"]).columns, ["a", "b"])

        df = df1.join(df2).withColumn("c", lit(0))
        self.assertEqual(df.columns, ["a", "a", "b", "c"])
        self.assertEqual(df.select(df1["*"]).columns, ["a"])
        self.assertEqual(df.select(df2["*"]).columns, ["a", "b"])

        df = df1.join(df2, "a")
        self.assertEqual(df.columns, ["a", "b"])
        self.assertEqual(df.select(df1["*"]).columns, ["a"])
        self.assertEqual(df.select(df2["*"]).columns, ["a", "b"])

        df = df1.join(df2, "a").withColumn("c", lit(0))
        self.assertEqual(df.columns, ["a", "b", "c"])
        self.assertEqual(df.select(df1["*"]).columns, ["a"])
        self.assertEqual(df.select(df2["*"]).columns, ["a", "b"])

        df = df2.join(df3)
        self.assertEqual(df.columns, ["a", "b", "x", "y"])
        self.assertEqual(df.select(df2["*"]).columns, ["a", "b"])
        self.assertEqual(df.select(df3["*"]).columns, ["x", "y"])

        df = df2.join(df3).withColumn("c", lit(0))
        self.assertEqual(df.columns, ["a", "b", "x", "y", "c"])
        self.assertEqual(df.select(df2["*"]).columns, ["a", "b"])
        self.assertEqual(df.select(df3["*"]).columns, ["x", "y"])

    def test_count_star(self):
        df1 = self.spark.createDataFrame([{"a": 1}])
        df2 = self.spark.createDataFrame([{"a": 1, "b": "v"}])
        df3 = df2.select(struct("a", "b").alias("s"))

        self.assertEqual(df1.select(count(df1["*"])).columns, ["count(1)"])
        self.assertEqual(df1.select(count(col("*"))).columns, ["count(1)"])

        self.assertEqual(df2.select(count(df2["*"])).columns, ["count(1)"])
        self.assertEqual(df2.select(count(col("*"))).columns, ["count(1)"])

        self.assertEqual(df3.select(count(df3["*"])).columns, ["count(1)"])
        self.assertEqual(df3.select(count(col("*"))).columns, ["count(1)"])

    def test_self_join(self):
        df1 = self.spark.range(10).withColumn("a", lit(0))
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
        df1 = self.spark.range(10).withColumn("value", lit(1))
        df2 = df1.union(df1)
        df3 = df1.join(df2, df1.id == df2.id, "left")
        self.assertTrue(df3.columns, ["id", "value", "id", "value"])
        self.assertTrue(df3.count() == 20)

    def test_self_join_IV(self):
        df1 = self.spark.range(10).withColumn("value", lit(1))
        df2 = df1.withColumn("value", lit(2)).union(df1.withColumn("value", lit(3)))
        df3 = df1.join(df2, df1.id == df2.id, "right")
        self.assertTrue(df3.columns, ["id", "value", "id", "value"])
        self.assertTrue(df3.count() == 20)

    def test_duplicated_column_names(self):
        df = self.spark.createDataFrame([(1, 2)], ["c", "c"])
        row = df.select("*").first()
        self.assertEqual(1, row[0])
        self.assertEqual(2, row[1])
        self.assertEqual("Row(c=1, c=2)", str(row))
        # Cannot access columns
        self.assertRaises(AnalysisException, lambda: df.select(df[0]).first())
        self.assertRaises(AnalysisException, lambda: df.select(df.c).first())
        self.assertRaises(AnalysisException, lambda: df.select(df["c"]).first())

    def test_help_command(self):
        # Regression test for SPARK-5464
        rdd = self.sc.parallelize(['{"foo":"bar"}', '{"foo":"baz"}'])
        df = self.spark.read.json(rdd)
        # render_doc() reproduces the help() exception without printing output
        self.check_help_command(df)

    def check_help_command(self, df):
        pydoc.render_doc(df)
        pydoc.render_doc(df.foo)
        pydoc.render_doc(df.take(1))

    def test_drop(self):
        df = self.spark.createDataFrame([("A", 50, "Y"), ("B", 60, "Y")], ["name", "age", "active"])
        self.assertEqual(df.drop("active").columns, ["name", "age"])
        self.assertEqual(df.drop("active", "nonexistent_column").columns, ["name", "age"])
        self.assertEqual(df.drop("name", "age", "active").columns, [])
        self.assertEqual(df.drop(col("name")).columns, ["age", "active"])
        self.assertEqual(df.drop(col("name"), col("age")).columns, ["active"])
        self.assertEqual(df.drop(col("name"), col("age"), col("random")).columns, ["active"])

    def test_drop_join(self):
        left_df = self.spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")],
            ["join_key", "value1"],
        )
        right_df = self.spark.createDataFrame(
            [(1, "aa"), (2, "bb"), (4, "dd")],
            ["join_key", "value2"],
        )
        joined_df = left_df.join(
            right_df,
            on=left_df["join_key"] == right_df["join_key"],
            how="left",
        )

        dropped_1 = joined_df.drop(left_df["join_key"])
        self.assertEqual(dropped_1.columns, ["value1", "join_key", "value2"])
        self.assertEqual(
            dropped_1.sort("value1").collect(),
            [
                Row(value1="a", join_key=1, value2="aa"),
                Row(value1="b", join_key=2, value2="bb"),
                Row(value1="c", join_key=None, value2=None),
            ],
        )

        dropped_2 = joined_df.drop(right_df["join_key"])
        self.assertEqual(dropped_2.columns, ["join_key", "value1", "value2"])
        self.assertEqual(
            dropped_2.sort("value1").collect(),
            [
                Row(join_key=1, value1="a", value2="aa"),
                Row(join_key=2, value1="b", value2="bb"),
                Row(join_key=3, value1="c", value2=None),
            ],
        )

    def test_with_columns_renamed(self):
        df = self.spark.createDataFrame([("Alice", 50), ("Alice", 60)], ["name", "age"])

        # rename both columns
        renamed_df1 = df.withColumnsRenamed({"name": "naam", "age": "leeftijd"})
        self.assertEqual(renamed_df1.columns, ["naam", "leeftijd"])

        # rename one column with one missing name
        renamed_df2 = df.withColumnsRenamed({"name": "naam", "address": "adres"})
        self.assertEqual(renamed_df2.columns, ["naam", "age"])

        # negative test for incorrect type
        with self.assertRaises(PySparkTypeError) as pe:
            df.withColumnsRenamed(("name", "x"))

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_DICT",
            messageParameters={"arg_name": "colsMap", "arg_type": "tuple"},
        )

    def test_with_columns_renamed_with_duplicated_names(self):
        df1 = self.spark.createDataFrame([(1, "v1")], ["id", "value"])
        df2 = self.spark.createDataFrame([(1, "x", "v2")], ["id", "a", "value"])
        join = df2.join(df1, on=["id"], how="left")

        self.assertEqual(
            join.withColumnRenamed("id", "value").columns,
            join.withColumnsRenamed({"id": "value"}).columns,
        )
        self.assertEqual(
            join.withColumnRenamed("a", "b").columns,
            join.withColumnsRenamed({"a": "b"}).columns,
        )
        self.assertEqual(
            join.withColumnRenamed("value", "new_value").columns,
            join.withColumnsRenamed({"value": "new_value"}).columns,
        )
        self.assertEqual(
            join.withColumnRenamed("x", "y").columns,
            join.withColumnsRenamed({"x": "y"}).columns,
        )

    def test_ordering_of_with_columns_renamed(self):
        df = self.spark.range(10)

        df1 = df.withColumnsRenamed({"id": "a", "a": "b"})
        self.assertEqual(df1.columns, ["b"])

        df2 = df.withColumnsRenamed({"a": "b", "id": "a"})
        self.assertEqual(df2.columns, ["a"])

    def test_drop_duplicates(self):
        # SPARK-36034 test that drop duplicates throws a type error when in correct type provided
        df = self.spark.createDataFrame([("Alice", 50), ("Alice", 60)], ["name", "age"])

        # shouldn't drop a non-null row
        self.assertEqual(df.dropDuplicates().count(), 2)

        self.assertEqual(df.dropDuplicates(["name"]).count(), 1)

        self.assertEqual(df.dropDuplicates(["name", "age"]).count(), 2)

        with self.assertRaises(PySparkTypeError) as pe:
            df.dropDuplicates("name")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_LIST_OR_TUPLE",
            messageParameters={"arg_name": "subset", "arg_type": "str"},
        )

        # Should raise proper error when taking non-string values
        with self.assertRaises(PySparkTypeError) as pe:
            df.dropDuplicates([None]).show()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={"arg_name": "subset", "arg_type": "NoneType"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.dropDuplicates([1]).show()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={"arg_name": "subset", "arg_type": "int"},
        )

    def test_drop_duplicates_with_ambiguous_reference(self):
        df1 = self.spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        df2 = self.spark.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")])
        df3 = df1.join(df2, df1.name == df2.name, "inner")

        self.assertEqual(df3.drop("name", "age").columns, ["height"])
        self.assertEqual(df3.drop("name", df3.age, "unknown").columns, ["height"])
        self.assertEqual(df3.drop("name", "age", df3.height).columns, [])

    def test_drop_empty_column(self):
        df = self.spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

        self.assertEqual(df.drop().columns, ["age", "name"])
        self.assertEqual(df.drop(*[]).columns, ["age", "name"])

    def test_drop_column_name_with_dot(self):
        df = (
            self.spark.range(1, 3)
            .withColumn("first.name", lit("Peter"))
            .withColumn("city.name", lit("raleigh"))
            .withColumn("state", lit("nc"))
        )

        self.assertEqual(df.drop("first.name").columns, ["id", "city.name", "state"])
        self.assertEqual(df.drop("city.name").columns, ["id", "first.name", "state"])
        self.assertEqual(df.drop("first.name", "city.name").columns, ["id", "state"])
        self.assertEqual(
            df.drop("first.name", "city.name", "unknown.unknown").columns, ["id", "state"]
        )
        self.assertEqual(
            df.drop("unknown.unknown").columns, ["id", "first.name", "city.name", "state"]
        )

    def test_with_column_with_existing_name(self):
        keys = self.df.withColumn("key", self.df.key).select("key").collect()
        self.assertEqual([r.key for r in keys], list(range(100)))

    # regression test for SPARK-10417
    def test_column_iterator(self):
        def foo():
            for x in self.df.key:
                break

        self.assertRaises(TypeError, foo)

    def test_with_columns(self):
        # With single column
        keys = self.df.withColumns({"key": self.df.key}).select("key").collect()
        self.assertEqual([r.key for r in keys], list(range(100)))

        # With key and value columns
        kvs = (
            self.df.withColumns({"key": self.df.key, "value": self.df.value})
            .select("key", "value")
            .collect()
        )
        self.assertEqual([(r.key, r.value) for r in kvs], [(i, str(i)) for i in range(100)])

        # Columns rename
        kvs = (
            self.df.withColumns({"key_alias": self.df.key, "value_alias": self.df.value})
            .select("key_alias", "value_alias")
            .collect()
        )
        self.assertEqual(
            [(r.key_alias, r.value_alias) for r in kvs], [(i, str(i)) for i in range(100)]
        )

        # Type check
        self.assertRaises(TypeError, self.df.withColumns, ["key"])
        self.assertRaises(Exception, self.df.withColumns)

    def test_generic_hints(self):
        df1 = self.spark.range(10e10).toDF("id")
        df2 = self.spark.range(10e10).toDF("id")

        self.assertIsInstance(df1.hint("broadcast"), type(df1))

        # Dummy rules
        self.assertIsInstance(df1.hint("broadcast", "foo", "bar"), type(df1))

        with io.StringIO() as buf, redirect_stdout(buf):
            df1.join(df2.hint("broadcast"), "id").explain(True)
            self.assertEqual(1, buf.getvalue().count("BroadcastHashJoin"))

    def test_coalesce_hints_with_string_parameter(self):
        with self.sql_conf({"spark.sql.adaptive.coalescePartitions.enabled": False}):
            df = self.spark.createDataFrame(
                zip(["A", "B"] * 2**9, range(2**10)),
                StructType([StructField("a", StringType()), StructField("n", IntegerType())]),
            )
            with io.StringIO() as buf, redirect_stdout(buf):
                # COALESCE
                coalesce = df.hint("coalesce", 2)
                coalesce.explain(True)
                output = buf.getvalue()
                self.assertGreaterEqual(output.count("Coalesce 2"), 1)
                buf.truncate(0)
                buf.seek(0)

                # REPARTITION_BY_RANGE
                range_partitioned = df.hint("REPARTITION_BY_RANGE", 2, "a")
                range_partitioned.explain(True)
                output = buf.getvalue()
                self.assertGreaterEqual(output.count("REPARTITION_BY_NUM"), 1)
                buf.truncate(0)
                buf.seek(0)

                # REBALANCE
                rebalanced1 = df.hint("REBALANCE", "a")  # just check this doesn't error
                rebalanced1.explain(True)
                rebalanced2 = df.hint("REBALANCE", 2)
                rebalanced2.explain(True)
                rebalanced3 = df.hint("REBALANCE", 2, "a")
                rebalanced3.explain(True)
                rebalanced4 = df.hint("REBALANCE", functions.col("a"))
                rebalanced4.explain(True)
                output = buf.getvalue()
                self.assertGreaterEqual(output.count("REBALANCE_PARTITIONS_BY_NONE"), 1)
                self.assertGreaterEqual(output.count("REBALANCE_PARTITIONS_BY_COL"), 3)

    # add tests for SPARK-23647 (test more types for hint)
    def test_extended_hint_types(self):
        df = self.spark.range(10e10).toDF("id")
        such_a_nice_list = ["itworks1", "itworks2", "itworks3"]
        int_list = [1, 2, 3]
        hinted_df = df.hint("my awesome hint", 1.2345, "what", such_a_nice_list, int_list)

        self.assertIsInstance(df.hint("broadcast", []), type(df))
        self.assertIsInstance(df.hint("broadcast", ["foo", "bar"]), type(df))

        with io.StringIO() as buf, redirect_stdout(buf):
            hinted_df.explain(True)
            explain_output = buf.getvalue()
            self.assertGreaterEqual(explain_output.count("1.2345"), 1)
            self.assertGreaterEqual(explain_output.count("what"), 1)
            self.assertGreaterEqual(explain_output.count("itworks"), 1)

    def test_sample(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.range(1).sample()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_BOOL_OR_FLOAT_OR_INT",
            messageParameters={
                "arg_name": "withReplacement (optional), fraction (required) and seed (optional)",
                "arg_type": "NoneType, NoneType, NoneType",
            },
        )

        self.assertRaises(TypeError, lambda: self.spark.range(1).sample("a"))

        self.assertRaises(TypeError, lambda: self.spark.range(1).sample(seed="abc"))

        self.assertRaises(
            IllegalArgumentException, lambda: self.spark.range(1).sample(-1.0).count()
        )

    def test_sample_with_random_seed(self):
        df = self.spark.range(10000).sample(0.1)
        cnts = [df.count() for i in range(10)]
        self.assertEqual(1, len(set(cnts)))

    def test_toDF_with_string(self):
        df = self.spark.createDataFrame([("John", 30), ("Alice", 25), ("Bob", 28)])
        data = [("John", 30), ("Alice", 25), ("Bob", 28)]

        result = df.toDF("key", "value")
        self.assertEqual(result.schema.simpleString(), "struct<key:string,value:bigint>")
        self.assertEqual(result.collect(), data)

        with self.assertRaises(PySparkTypeError) as pe:
            df.toDF("key", None)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_LIST_OF_STR",
            messageParameters={"arg_name": "cols", "arg_type": "NoneType"},
        )

    def test_toDF_with_schema_string(self):
        data = [Row(key=i, value=str(i)) for i in range(100)]
        rdd = self.sc.parallelize(data, 5)

        df = rdd.toDF("key: int, value: string")
        self.assertEqual(df.schema.simpleString(), "struct<key:int,value:string>")
        self.assertEqual(df.collect(), data)

        # different but compatible field types can be used.
        df = rdd.toDF("key: string, value: string")
        self.assertEqual(df.schema.simpleString(), "struct<key:string,value:string>")
        self.assertEqual(df.collect(), [Row(key=str(i), value=str(i)) for i in range(100)])

        # field names can differ.
        df = rdd.toDF(" a: int, b: string ")
        self.assertEqual(df.schema.simpleString(), "struct<a:int,b:string>")
        self.assertEqual(df.collect(), data)

        # number of fields must match.
        self.assertRaisesRegex(
            Exception, "FIELD_STRUCT_LENGTH_MISMATCH", lambda: rdd.toDF("key: int").collect()
        )

        # field types mismatch will cause exception at runtime.
        self.assertRaisesRegex(
            Exception,
            "FIELD_DATA_TYPE_UNACCEPTABLE",
            lambda: rdd.toDF("key: float, value: string").collect(),
        )

        # flat schema values will be wrapped into row.
        df = rdd.map(lambda row: row.key).toDF("int")
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")
        self.assertEqual(df.collect(), [Row(key=i) for i in range(100)])

        # users can use DataType directly instead of data type string.
        df = rdd.map(lambda row: row.key).toDF(IntegerType())
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")
        self.assertEqual(df.collect(), [Row(key=i) for i in range(100)])

    def test_create_df_with_collation(self):
        schema = StructType([StructField("name", StringType("UNICODE_CI"), True)])
        df = self.spark.createDataFrame([("Alice",), ("alice",)], schema)

        self.assertEqual(df.select("name").distinct().count(), 1)

    def test_print_schema(self):
        df = self.spark.createDataFrame([(1, (2, 2))], ["a", "b"])

        with io.StringIO() as buf, redirect_stdout(buf):
            df.printSchema(1)
            self.assertEqual(1, buf.getvalue().count("long"))
            self.assertEqual(0, buf.getvalue().count("_1"))
            self.assertEqual(0, buf.getvalue().count("_2"))

            buf.truncate(0)
            buf.seek(0)

            df.printSchema(2)
            self.assertEqual(3, buf.getvalue().count("long"))
            self.assertEqual(1, buf.getvalue().count("_1"))
            self.assertEqual(1, buf.getvalue().count("_2"))

    def test_join_without_on(self):
        df1 = self.spark.range(1).toDF("a")
        df2 = self.spark.range(1).toDF("b")

        with self.sql_conf({"spark.sql.crossJoin.enabled": False}):
            self.assertRaises(AnalysisException, lambda: df1.join(df2, how="inner").collect())

        with self.sql_conf({"spark.sql.crossJoin.enabled": True}):
            actual = df1.join(df2, how="inner").collect()
            expected = [Row(a=0, b=0)]
            self.assertEqual(actual, expected)

    # Regression test for invalid join methods when on is None, Spark-14761
    def test_invalid_join_method(self):
        df1 = self.spark.createDataFrame([("Alice", 5), ("Bob", 8)], ["name", "age"])
        df2 = self.spark.createDataFrame([("Alice", 80), ("Bob", 90)], ["name", "height"])
        self.assertRaises(AnalysisException, lambda: df1.join(df2, how="invalid-join-type"))

    # Cartesian products require cross join syntax
    def test_require_cross(self):
        df1 = self.spark.createDataFrame([(1, "1")], ("key", "value"))
        df2 = self.spark.createDataFrame([(1, "1")], ("key", "value"))

        with self.sql_conf({"spark.sql.crossJoin.enabled": False}):
            # joins without conditions require cross join syntax
            self.assertRaises(AnalysisException, lambda: df1.join(df2).collect())

            # works with crossJoin
            self.assertEqual(1, df1.crossJoin(df2).count())

    def test_cache_dataframe(self):
        df = self.spark.createDataFrame([(2, 2), (3, 3)])
        try:
            self.assertEqual(df.storageLevel, StorageLevel.NONE)

            df.cache()
            self.assertEqual(df.storageLevel, StorageLevel.MEMORY_AND_DISK_DESER)

            df.unpersist()
            self.assertEqual(df.storageLevel, StorageLevel.NONE)

            df.persist()
            self.assertEqual(df.storageLevel, StorageLevel.MEMORY_AND_DISK_DESER)

            df.unpersist(blocking=True)
            self.assertEqual(df.storageLevel, StorageLevel.NONE)

            df.persist(StorageLevel.DISK_ONLY)
            self.assertEqual(df.storageLevel, StorageLevel.DISK_ONLY)
        finally:
            df.unpersist()
            self.assertEqual(df.storageLevel, StorageLevel.NONE)

    def test_cache_table(self):
        spark = self.spark
        tables = ["tab1", "tab2", "tab3"]
        with self.tempView(*tables):
            for i, tab in enumerate(tables):
                spark.createDataFrame([(2, i), (3, i)]).createOrReplaceTempView(tab)
                self.assertFalse(spark.catalog.isCached(tab))
            spark.catalog.cacheTable("tab1")
            spark.catalog.cacheTable("tab3", StorageLevel.OFF_HEAP)
            self.assertTrue(spark.catalog.isCached("tab1"))
            self.assertFalse(spark.catalog.isCached("tab2"))
            self.assertTrue(spark.catalog.isCached("tab3"))
            spark.catalog.cacheTable("tab2")
            spark.catalog.uncacheTable("tab1")
            spark.catalog.uncacheTable("tab3")
            self.assertFalse(spark.catalog.isCached("tab1"))
            self.assertTrue(spark.catalog.isCached("tab2"))
            self.assertFalse(spark.catalog.isCached("tab3"))
            spark.catalog.clearCache()
            self.assertFalse(spark.catalog.isCached("tab1"))
            self.assertFalse(spark.catalog.isCached("tab2"))
            self.assertFalse(spark.catalog.isCached("tab3"))
            self.assertRaisesRegex(
                AnalysisException,
                "does_not_exist",
                lambda: spark.catalog.isCached("does_not_exist"),
            )
            self.assertRaisesRegex(
                AnalysisException,
                "does_not_exist",
                lambda: spark.catalog.cacheTable("does_not_exist"),
            )
            self.assertRaisesRegex(
                AnalysisException,
                "does_not_exist",
                lambda: spark.catalog.uncacheTable("does_not_exist"),
            )

    def test_repr_behaviors(self):
        import re

        pattern = re.compile(r"^ *\|", re.MULTILINE)
        df = self.spark.createDataFrame([(1, "1"), (22222, "22222")], ("key", "value"))

        # test when eager evaluation is enabled and _repr_html_ will not be called
        with self.sql_conf({"spark.sql.repl.eagerEval.enabled": True}):
            expected1 = """+-----+-----+
                ||  key|value|
                |+-----+-----+
                ||    1|    1|
                ||22222|22222|
                |+-----+-----+
                |"""
            self.assertEqual(re.sub(pattern, "", expected1), df.__repr__())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                expected2 = """+---+-----+
                ||key|value|
                |+---+-----+
                ||  1|    1|
                ||222|  222|
                |+---+-----+
                |"""
                self.assertEqual(re.sub(pattern, "", expected2), df.__repr__())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    expected3 = """+---+-----+
                    ||key|value|
                    |+---+-----+
                    ||  1|    1|
                    |+---+-----+
                    |only showing top 1 row
                    |"""
                    self.assertEqual(re.sub(pattern, "", expected3), df.__repr__())

        # test when eager evaluation is enabled and _repr_html_ will be called
        with self.sql_conf({"spark.sql.repl.eagerEval.enabled": True}):
            expected1 = """<table border='1'>
                |<tr><th>key</th><th>value</th></tr>
                |<tr><td>1</td><td>1</td></tr>
                |<tr><td>22222</td><td>22222</td></tr>
                |</table>
                |"""
            self.assertEqual(re.sub(pattern, "", expected1), df._repr_html_())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                expected2 = """<table border='1'>
                    |<tr><th>key</th><th>value</th></tr>
                    |<tr><td>1</td><td>1</td></tr>
                    |<tr><td>222</td><td>222</td></tr>
                    |</table>
                    |"""
                self.assertEqual(re.sub(pattern, "", expected2), df._repr_html_())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    expected3 = """<table border='1'>
                        |<tr><th>key</th><th>value</th></tr>
                        |<tr><td>1</td><td>1</td></tr>
                        |</table>
                        |only showing top 1 row
                        |"""
                    self.assertEqual(re.sub(pattern, "", expected3), df._repr_html_())

        # test when eager evaluation is disabled and _repr_html_ will be called
        with self.sql_conf({"spark.sql.repl.eagerEval.enabled": False}):
            expected = "DataFrame[key: bigint, value: string]"
            self.assertEqual(None, df._repr_html_())
            self.assertEqual(expected, df.__repr__())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                self.assertEqual(None, df._repr_html_())
                self.assertEqual(expected, df.__repr__())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    self.assertEqual(None, df._repr_html_())
                    self.assertEqual(expected, df.__repr__())

    def test_same_semantics_error(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.range(10).sameSemantics(1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_DATAFRAME",
            messageParameters={"arg_name": "other", "arg_type": "int"},
        )

    def test_input_files(self):
        tpath = tempfile.mkdtemp()
        shutil.rmtree(tpath)
        try:
            self.spark.range(1, 100, 1, 10).write.parquet(tpath)
            # read parquet file and get the input files list
            input_files_list = self.spark.read.parquet(tpath).inputFiles()

            # input files list should contain 10 entries
            self.assertEqual(len(input_files_list), 10)
            # all file paths in list must contain tpath
            for file_path in input_files_list:
                self.assertTrue(tpath in file_path)
        finally:
            shutil.rmtree(tpath)

    def test_df_show(self):
        # SPARK-35408: ensure better diagnostics if incorrect parameters are passed
        # to DataFrame.show

        df = self.spark.createDataFrame([("foo",)])
        df.show(5)
        df.show(5, True)
        df.show(5, 1, True)
        df.show(n=5, truncate="1", vertical=False)
        df.show(n=5, truncate=1.5, vertical=False)

        with self.assertRaises(PySparkTypeError) as pe:
            df.show(True)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_INT",
            messageParameters={"arg_name": "n", "arg_type": "bool"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.show(vertical="foo")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_BOOL",
            messageParameters={"arg_name": "vertical", "arg_type": "str"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.show(truncate="foo")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_BOOL",
            messageParameters={"arg_name": "truncate", "arg_type": "str"},
        )

    def test_df_merge_into(self):
        try:
            # InMemoryRowLevelOperationTableCatalog is a test catalog that is included in the
            # catalyst-test package. If Spark complains that it can't find this class, make sure
            # the catalyst-test JAR does exist in "SPARK_HOME/assembly/target/scala-x.xx/jars"
            # directory. If not, build it with `build/sbt test:assembly` and copy it over.
            self.spark.conf.set(
                "spark.sql.catalog.testcat",
                "org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog",
            )
            with self.table("testcat.ns1.target"):

                def reset_target_table():
                    self.spark.createDataFrame(
                        [(1, "Alice"), (2, "Bob")], ["id", "name"]
                    ).write.mode("overwrite").saveAsTable("testcat.ns1.target")

                source = self.spark.createDataFrame(
                    [(1, "Charlie"), (3, "David")], ["id", "name"]
                )  # type: DataFrame

                from pyspark.sql.functions import col

                # Match -> update, NotMatch -> insert, NotMatchedBySource -> delete
                reset_target_table()
                # fmt: off
                source.mergeInto("testcat.ns1.target", source.id == col("target.id")) \
                    .whenMatched(source.id == 1).update({"name": source.name}) \
                    .whenNotMatched().insert({"id": source.id, "name": source.name}) \
                    .whenNotMatchedBySource().delete() \
                    .merge()
                # fmt: on
                self.assertEqual(
                    self.spark.table("testcat.ns1.target").orderBy("id").collect(),
                    [Row(id=1, name="Charlie"), Row(id=3, name="David")],
                )

                # Match -> updateAll, NotMatch -> insertAll, NotMatchedBySource -> update
                reset_target_table()
                # fmt: off
                source.mergeInto("testcat.ns1.target", source.id == col("target.id")) \
                    .whenMatched(source.id == 1).updateAll() \
                    .whenNotMatched(source.id == 3).insertAll() \
                    .whenNotMatchedBySource(col("target.id") == lit(2)) \
                      .update({"name": lit("not_matched")}) \
                    .merge()
                # fmt: on
                self.assertEqual(
                    self.spark.table("testcat.ns1.target").orderBy("id").collect(),
                    [
                        Row(id=1, name="Charlie"),
                        Row(id=2, name="not_matched"),
                        Row(id=3, name="David"),
                    ],
                )

                # Match -> delete, NotMatchedBySource -> delete
                reset_target_table()
                # fmt: off
                self.spark.createDataFrame([(1, "AliceJr")], ["id", "name"]) \
                    .write.mode("append").saveAsTable("testcat.ns1.target")
                source.mergeInto("testcat.ns1.target", source.id == col("target.id")) \
                    .whenMatched(col("target.name") != lit("AliceJr")).delete() \
                    .whenNotMatchedBySource().delete() \
                    .merge()
                # fmt: on
                self.assertEqual(
                    self.spark.table("testcat.ns1.target").orderBy("id").collect(),
                    [Row(id=1, name="AliceJr")],
                )
        finally:
            self.spark.conf.unset("spark.sql.catalog.testcat")

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_pandas_api(self):
        import pandas as pd
        from pandas.testing import assert_frame_equal

        sdf = self.spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["Col1", "Col2"])
        psdf_from_sdf = sdf.pandas_api()
        psdf_from_sdf_with_index = sdf.pandas_api(index_col="Col1")
        pdf = pd.DataFrame({"Col1": ["a", "b", "c"], "Col2": [1, 2, 3]})
        pdf_with_index = pdf.set_index("Col1")

        assert_frame_equal(pdf, psdf_from_sdf.to_pandas())
        assert_frame_equal(pdf_with_index, psdf_from_sdf_with_index.to_pandas())

    def test_to(self):
        schema = StructType(
            [StructField("i", StringType(), True), StructField("j", IntegerType(), True)]
        )
        df = self.spark.createDataFrame([("a", 1)], schema)

        schema1 = StructType([StructField("j", StringType()), StructField("i", StringType())])
        df1 = df.to(schema1)
        self.assertEqual(schema1, df1.schema)
        self.assertEqual(df.count(), df1.count())

        schema2 = StructType([StructField("j", LongType())])
        df2 = df.to(schema2)
        self.assertEqual(schema2, df2.schema)
        self.assertEqual(df.count(), df2.count())

        schema3 = StructType([StructField("struct", schema1, False)])
        df3 = df.select(struct("i", "j").alias("struct")).to(schema3)
        self.assertEqual(schema3, df3.schema)
        self.assertEqual(df.count(), df3.count())

        # incompatible field nullability
        schema4 = StructType([StructField("j", LongType(), False)])
        self.assertRaisesRegex(
            AnalysisException, "NULLABLE_COLUMN_OR_FIELD", lambda: df.to(schema4).count()
        )

        # field cannot upcast
        schema5 = StructType([StructField("i", LongType())])
        self.assertRaisesRegex(
            AnalysisException, "INVALID_COLUMN_OR_FIELD_DATA_TYPE", lambda: df.to(schema5).count()
        )

    def test_colregex(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.range(10).colRegex(10)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={"arg_name": "colName", "arg_type": "int"},
        )

    def test_where(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.range(10).where(10)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "condition", "arg_type": "int"},
        )

    def test_duplicate_field_names(self):
        data = [
            Row(Row("a", 1), Row(2, 3, "b", 4, "c", "d")),
            Row(Row("w", 6), Row(7, 8, "x", 9, "y", "z")),
        ]
        schema = (
            StructType()
            .add("struct", StructType().add("x", StringType()).add("x", IntegerType()))
            .add(
                "struct",
                StructType()
                .add("a", IntegerType())
                .add("x", IntegerType())
                .add("x", StringType())
                .add("y", IntegerType())
                .add("y", StringType())
                .add("x", StringType()),
            )
        )
        df = self.spark.createDataFrame(data, schema=schema)

        self.assertEqual(df.schema, schema)
        self.assertEqual(df.collect(), data)

    def test_union_classmethod_usage(self):
        df = self.spark.range(1)
        self.assertEqual(DataFrame.union(df, df).collect(), [Row(id=0), Row(id=0)])

    def test_isinstance_dataframe(self):
        self.assertIsInstance(self.spark.range(1), DataFrame)

    def test_checkpoint_dataframe(self):
        with io.StringIO() as buf, redirect_stdout(buf):
            self.spark.range(1).localCheckpoint().explain()
            self.assertIn("ExistingRDD", buf.getvalue())

    def test_transpose(self):
        df = self.spark.createDataFrame([{"a": "x", "b": "y", "c": "z"}])

        # default index column
        transposed_df = df.transpose()
        expected_schema = StructType(
            [StructField("key", StringType(), False), StructField("x", StringType(), True)]
        )
        expected_data = [Row(key="b", x="y"), Row(key="c", x="z")]
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)
        assertDataFrameEqual(transposed_df, expected_df, checkRowOrder=True)

        # specified index column
        transposed_df = df.transpose("c")
        expected_schema = StructType(
            [StructField("key", StringType(), False), StructField("z", StringType(), True)]
        )
        expected_data = [Row(key="a", z="x"), Row(key="b", z="y")]
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)
        assertDataFrameEqual(transposed_df, expected_df, checkRowOrder=True)

        # enforce transpose max values
        with self.sql_conf({"spark.sql.transposeMaxValues": 0}):
            with self.assertRaises(AnalysisException) as pe:
                df.transpose().collect()
            self.check_error(
                exception=pe.exception,
                errorClass="TRANSPOSE_EXCEED_ROW_LIMIT",
                messageParameters={"maxValues": "0", "config": "spark.sql.transposeMaxValues"},
            )

        # enforce ascending order based on index column values for transposed columns
        df = self.spark.createDataFrame([{"a": "z"}, {"a": "y"}, {"a": "x"}])
        transposed_df = df.transpose()
        expected_schema = StructType(
            [
                StructField("key", StringType(), False),
                StructField("x", StringType(), True),
                StructField("y", StringType(), True),
                StructField("z", StringType(), True),
            ]
        )  # z, y, x -> x, y, z
        expected_df = self.spark.createDataFrame([], schema=expected_schema)
        assertDataFrameEqual(transposed_df, expected_df, checkRowOrder=True)

        # enforce AtomicType Attribute for index column values
        df = self.spark.createDataFrame([{"a": ["x", "x"], "b": "y", "c": "z"}])
        with self.assertRaises(AnalysisException) as pe:
            df.transpose().collect()
        self.check_error(
            exception=pe.exception,
            errorClass="TRANSPOSE_INVALID_INDEX_COLUMN",
            messageParameters={
                "reason": "Index column must be of atomic type, "
                "but found: ArrayType(StringType,true)"
            },
        )

        # enforce least common type for non-index columns
        df = self.spark.createDataFrame([{"a": "x", "b": "y", "c": 1}])
        with self.assertRaises(AnalysisException) as pe:
            df.transpose().collect()
        self.check_error(
            exception=pe.exception,
            errorClass="TRANSPOSE_NO_LEAST_COMMON_TYPE",
            messageParameters={"dt1": "STRING", "dt2": "BIGINT"},
        )


class DataFrameTests(DataFrameTestsMixin, ReusedSQLTestCase):
    def test_query_execution_unsupported_in_classic(self):
        with self.assertRaises(PySparkValueError) as pe:
            self.spark.range(1).executionInfo

        self.check_error(
            exception=pe.exception,
            errorClass="CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF",
            messageParameters={"member": "queryExecution"},
        )


if __name__ == "__main__":
    from pyspark.sql.tests.test_dataframe import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
