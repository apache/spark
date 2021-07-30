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
import pydoc
import shutil
import tempfile
import time
import unittest

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, count, sum, mean
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField, \
    BooleanType, DateType, TimestampType, FloatType
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from pyspark.testing.sqlutils import ReusedSQLTestCase, SQLTestUtils, have_pyarrow, have_pandas, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest


class DataFrameTests(ReusedSQLTestCase):

    def test_range(self):
        self.assertEqual(self.spark.range(1, 1).count(), 0)
        self.assertEqual(self.spark.range(1, 0, -1).count(), 1)
        self.assertEqual(self.spark.range(0, 1 << 40, 1 << 39).count(), 2)
        self.assertEqual(self.spark.range(-2).count(), 0)
        self.assertEqual(self.spark.range(3).count(), 3)

    def test_duplicated_column_names(self):
        df = self.spark.createDataFrame([(1, 2)], ["c", "c"])
        row = df.select('*').first()
        self.assertEqual(1, row[0])
        self.assertEqual(2, row[1])
        self.assertEqual("Row(c=1, c=2)", str(row))
        # Cannot access columns
        self.assertRaises(AnalysisException, lambda: df.select(df[0]).first())
        self.assertRaises(AnalysisException, lambda: df.select(df.c).first())
        self.assertRaises(AnalysisException, lambda: df.select(df["c"]).first())

    def test_freqItems(self):
        vals = [Row(a=1, b=-2.0) if i % 2 == 0 else Row(a=i, b=i * 1.0) for i in range(100)]
        df = self.sc.parallelize(vals).toDF()
        items = df.stat.freqItems(("a", "b"), 0.4).collect()[0]
        self.assertTrue(1 in items[0])
        self.assertTrue(-2.0 in items[1])

    def test_help_command(self):
        # Regression test for SPARK-5464
        rdd = self.sc.parallelize(['{"foo":"bar"}', '{"foo":"baz"}'])
        df = self.spark.read.json(rdd)
        # render_doc() reproduces the help() exception without printing output
        pydoc.render_doc(df)
        pydoc.render_doc(df.foo)
        pydoc.render_doc(df.take(1))

    def test_drop_duplicates(self):
        # SPARK-36034 test that drop duplicates throws a type error when in correct type provided
        df = self.spark.createDataFrame(
            [("Alice", 50), ("Alice", 60)],
            ["name", "age"]
        )

        # shouldn't drop a non-null row
        self.assertEqual(df.dropDuplicates().count(), 2)

        self.assertEqual(df.dropDuplicates(["name"]).count(), 1)

        self.assertEqual(df.dropDuplicates(["name", "age"]).count(), 2)

        type_error_msg = "Parameter 'subset' must be a list of columns"
        with self.assertRaisesRegex(TypeError, type_error_msg):
            df.dropDuplicates("name")

    def test_dropna(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)])

        # shouldn't drop a non-null row
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', 50, 80.1)], schema).dropna().count(),
            1)

        # dropping rows with a single null value
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna().count(),
            0)
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna(how='any').count(),
            0)

        # if how = 'all', only drop rows if all values are null
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna(how='all').count(),
            1)
        self.assertEqual(self.spark.createDataFrame(
            [(None, None, None)], schema).dropna(how='all').count(),
            0)

        # how and subset
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', 50, None)], schema).dropna(how='any', subset=['name', 'age']).count(),
            1)
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', None, None)], schema).dropna(how='any', subset=['name', 'age']).count(),
            0)

        # threshold
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna(thresh=2).count(),
            1)
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', None, None)], schema).dropna(thresh=2).count(),
            0)

        # threshold and subset
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', 50, None)], schema).dropna(thresh=2, subset=['name', 'age']).count(),
            1)
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', None, 180.9)], schema).dropna(thresh=2, subset=['name', 'age']).count(),
            0)

        # thresh should take precedence over how
        self.assertEqual(self.spark.createDataFrame(
            [(u'Alice', 50, None)], schema).dropna(
                how='any', thresh=2, subset=['name', 'age']).count(),
            1)

    def test_fillna(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True),
            StructField("spy", BooleanType(), True)])

        # fillna shouldn't change non-null values
        row = self.spark.createDataFrame([(u'Alice', 10, 80.1, True)], schema).fillna(50).first()
        self.assertEqual(row.age, 10)

        # fillna with int
        row = self.spark.createDataFrame([(u'Alice', None, None, None)], schema).fillna(50).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.0)

        # fillna with double
        row = self.spark.createDataFrame(
            [(u'Alice', None, None, None)], schema).fillna(50.1).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.1)

        # fillna with bool
        row = self.spark.createDataFrame(
            [(u'Alice', None, None, None)], schema).fillna(True).first()
        self.assertEqual(row.age, None)
        self.assertEqual(row.spy, True)

        # fillna with string
        row = self.spark.createDataFrame([(None, None, None, None)], schema).fillna("hello").first()
        self.assertEqual(row.name, u"hello")
        self.assertEqual(row.age, None)

        # fillna with subset specified for numeric cols
        row = self.spark.createDataFrame(
            [(None, None, None, None)], schema).fillna(50, subset=['name', 'age']).first()
        self.assertEqual(row.name, None)
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, None)
        self.assertEqual(row.spy, None)

        # fillna with subset specified for string cols
        row = self.spark.createDataFrame(
            [(None, None, None, None)], schema).fillna("haha", subset=['name', 'age']).first()
        self.assertEqual(row.name, "haha")
        self.assertEqual(row.age, None)
        self.assertEqual(row.height, None)
        self.assertEqual(row.spy, None)

        # fillna with subset specified for bool cols
        row = self.spark.createDataFrame(
            [(None, None, None, None)], schema).fillna(True, subset=['name', 'spy']).first()
        self.assertEqual(row.name, None)
        self.assertEqual(row.age, None)
        self.assertEqual(row.height, None)
        self.assertEqual(row.spy, True)

        # fillna with dictionary for boolean types
        row = self.spark.createDataFrame([Row(a=None), Row(a=True)]).fillna({"a": True}).first()
        self.assertEqual(row.a, True)

    def test_repartitionByRange_dataframe(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)])

        df1 = self.spark.createDataFrame(
            [(u'Bob', 27, 66.0), (u'Alice', 10, 10.0), (u'Bob', 10, 66.0)], schema)
        df2 = self.spark.createDataFrame(
            [(u'Alice', 10, 10.0), (u'Bob', 10, 66.0), (u'Bob', 27, 66.0)], schema)

        # test repartitionByRange(numPartitions, *cols)
        df3 = df1.repartitionByRange(2, "name", "age")
        self.assertEqual(df3.rdd.getNumPartitions(), 2)
        self.assertEqual(df3.rdd.first(), df2.rdd.first())
        self.assertEqual(df3.rdd.take(3), df2.rdd.take(3))

        # test repartitionByRange(numPartitions, *cols)
        df4 = df1.repartitionByRange(3, "name", "age")
        self.assertEqual(df4.rdd.getNumPartitions(), 3)
        self.assertEqual(df4.rdd.first(), df2.rdd.first())
        self.assertEqual(df4.rdd.take(3), df2.rdd.take(3))

        # test repartitionByRange(*cols)
        df5 = df1.repartitionByRange(5, "name", "age")
        self.assertEqual(df5.rdd.first(), df2.rdd.first())
        self.assertEqual(df5.rdd.take(3), df2.rdd.take(3))

    def test_replace(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)])

        # replace with int
        row = self.spark.createDataFrame([(u'Alice', 10, 10.0)], schema).replace(10, 20).first()
        self.assertEqual(row.age, 20)
        self.assertEqual(row.height, 20.0)

        # replace with double
        row = self.spark.createDataFrame(
            [(u'Alice', 80, 80.0)], schema).replace(80.0, 82.1).first()
        self.assertEqual(row.age, 82)
        self.assertEqual(row.height, 82.1)

        # replace with string
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace(u'Alice', u'Ann').first()
        self.assertEqual(row.name, u"Ann")
        self.assertEqual(row.age, 10)

        # replace with subset specified by a string of a column name w/ actual change
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace(10, 20, subset='age').first()
        self.assertEqual(row.age, 20)

        # replace with subset specified by a string of a column name w/o actual change
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace(10, 20, subset='height').first()
        self.assertEqual(row.age, 10)

        # replace with subset specified with one column replaced, another column not in subset
        # stays unchanged.
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 10.0)], schema).replace(10, 20, subset=['name', 'age']).first()
        self.assertEqual(row.name, u'Alice')
        self.assertEqual(row.age, 20)
        self.assertEqual(row.height, 10.0)

        # replace with subset specified but no column will be replaced
        row = self.spark.createDataFrame(
            [(u'Alice', 10, None)], schema).replace(10, 20, subset=['name', 'height']).first()
        self.assertEqual(row.name, u'Alice')
        self.assertEqual(row.age, 10)
        self.assertEqual(row.height, None)

        # replace with lists
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace([u'Alice'], [u'Ann']).first()
        self.assertTupleEqual(row, (u'Ann', 10, 80.1))

        # replace with dict
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace({10: 11}).first()
        self.assertTupleEqual(row, (u'Alice', 11, 80.1))

        # test backward compatibility with dummy value
        dummy_value = 1
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace({'Alice': 'Bob'}, dummy_value).first()
        self.assertTupleEqual(row, (u'Bob', 10, 80.1))

        # test dict with mixed numerics
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace({10: -10, 80.1: 90.5}).first()
        self.assertTupleEqual(row, (u'Alice', -10, 90.5))

        # replace with tuples
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace((u'Alice', ), (u'Bob', )).first()
        self.assertTupleEqual(row, (u'Bob', 10, 80.1))

        # replace multiple columns
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.0)], schema).replace((10, 80.0), (20, 90)).first()
        self.assertTupleEqual(row, (u'Alice', 20, 90.0))

        # test for mixed numerics
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.0)], schema).replace((10, 80), (20, 90.5)).first()
        self.assertTupleEqual(row, (u'Alice', 20, 90.5))

        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.0)], schema).replace({10: 20, 80: 90.5}).first()
        self.assertTupleEqual(row, (u'Alice', 20, 90.5))

        # replace with boolean
        row = (self
               .spark.createDataFrame([(u'Alice', 10, 80.0)], schema)
               .selectExpr("name = 'Bob'", 'age <= 15')
               .replace(False, True).first())
        self.assertTupleEqual(row, (True, True))

        # replace string with None and then drop None rows
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.0)], schema).replace(u'Alice', None).dropna()
        self.assertEqual(row.count(), 0)

        # replace with number and None
        row = self.spark.createDataFrame(
            [(u'Alice', 10, 80.0)], schema).replace([10, 80], [20, None]).first()
        self.assertTupleEqual(row, (u'Alice', 20, None))

        # should fail if subset is not list, tuple or None
        with self.assertRaises(TypeError):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.1)], schema).replace({10: 11}, subset=1).first()

        # should fail if to_replace and value have different length
        with self.assertRaises(ValueError):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.1)], schema).replace(["Alice", "Bob"], ["Eve"]).first()

        # should fail if when received unexpected type
        with self.assertRaises(TypeError):
            from datetime import datetime
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.1)], schema).replace(datetime.now(), datetime.now()).first()

        # should fail if provided mixed type replacements
        with self.assertRaises(ValueError):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.1)], schema).replace(["Alice", 10], ["Eve", 20]).first()

        with self.assertRaises(ValueError):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.1)], schema).replace({u"Alice": u"Bob", 10: 20}).first()

        with self.assertRaisesRegex(
                TypeError,
                'value argument is required when to_replace is not a dictionary.'):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.0)], schema).replace(["Alice", "Bob"]).first()

    def test_with_column_with_existing_name(self):
        keys = self.df.withColumn("key", self.df.key).select("key").collect()
        self.assertEqual([r.key for r in keys], list(range(100)))

    # regression test for SPARK-10417
    def test_column_iterator(self):

        def foo():
            for x in self.df.key:
                break

        self.assertRaises(TypeError, foo)

    def test_generic_hints(self):
        from pyspark.sql import DataFrame

        df1 = self.spark.range(10e10).toDF("id")
        df2 = self.spark.range(10e10).toDF("id")

        self.assertIsInstance(df1.hint("broadcast"), DataFrame)
        self.assertIsInstance(df1.hint("broadcast", []), DataFrame)

        # Dummy rules
        self.assertIsInstance(df1.hint("broadcast", "foo", "bar"), DataFrame)
        self.assertIsInstance(df1.hint("broadcast", ["foo", "bar"]), DataFrame)

        plan = df1.join(df2.hint("broadcast"), "id")._jdf.queryExecution().executedPlan()
        self.assertEqual(1, plan.toString().count("BroadcastHashJoin"))

    # add tests for SPARK-23647 (test more types for hint)
    def test_extended_hint_types(self):
        df = self.spark.range(10e10).toDF("id")
        such_a_nice_list = ["itworks1", "itworks2", "itworks3"]
        hinted_df = df.hint("my awesome hint", 1.2345, "what", such_a_nice_list)
        logical_plan = hinted_df._jdf.queryExecution().logical()

        self.assertEqual(1, logical_plan.toString().count("1.2345"))
        self.assertEqual(1, logical_plan.toString().count("what"))
        self.assertEqual(3, logical_plan.toString().count("itworks"))

    def test_observe(self):
        # SPARK-36263: tests the DataFrame.observe(Observation, *Column) method
        from pyspark.sql import Observation

        df = SparkSession(self.sc).createDataFrame([
            (1, 1.0, 'one'),
            (2, 2.0, 'two'),
            (3, 3.0, 'three'),
        ], ['id', 'val', 'label'])

        unnamed_observation = Observation()
        named_observation = Observation("metric")
        observed = df.orderBy('id').observe(
            named_observation,
            count(lit(1)).alias('cnt'),
            sum(col("id")).alias('sum'),
            mean(col("val")).alias('mean')
        ).observe(unnamed_observation, count(lit(1)).alias('rows'))

        # test that observe works transparently
        actual = observed.collect()
        self.assertEqual([
            {'id': 1, 'val': 1.0, 'label': 'one'},
            {'id': 2, 'val': 2.0, 'label': 'two'},
            {'id': 3, 'val': 3.0, 'label': 'three'},
        ], [row.asDict() for row in actual])

        # test that we retrieve the metrics
        self.assertEqual(named_observation.get, Row(cnt=3, sum=6, mean=2.0))
        self.assertEqual(unnamed_observation.get, Row(rows=3))

        # observation requires name (if given) to be non empty string
        with self.assertRaisesRegex(TypeError, 'name should be a string'):
            Observation(123)
        with self.assertRaisesRegex(ValueError, 'name should not be empty'):
            Observation('')

        # dataframe.observe requires at least one expr
        with self.assertRaisesRegex(AssertionError, 'exprs should not be empty'):
            df.observe(Observation())

        # dataframe.observe requires non-None Columns
        for args in [(None,), ('id',),
                     (lit(1), None), (lit(1), 'id')]:
            with self.subTest(args=args):
                with self.assertRaisesRegex(AssertionError, 'all exprs should be Column'):
                    df.observe(Observation(), *args)

    def test_sample(self):
        self.assertRaisesRegex(
            TypeError,
            "should be a bool, float and number",
            lambda: self.spark.range(1).sample())

        self.assertRaises(
            TypeError,
            lambda: self.spark.range(1).sample("a"))

        self.assertRaises(
            TypeError,
            lambda: self.spark.range(1).sample(seed="abc"))

        self.assertRaises(
            IllegalArgumentException,
            lambda: self.spark.range(1).sample(-1.0))

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
        self.assertRaisesRegex(Exception, "Length of object",
                               lambda: rdd.toDF("key: int").collect())

        # field types mismatch will cause exception at runtime.
        self.assertRaisesRegex(Exception, "FloatType can not accept",
                               lambda: rdd.toDF("key: float, value: string").collect())

        # flat schema values will be wrapped into row.
        df = rdd.map(lambda row: row.key).toDF("int")
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")
        self.assertEqual(df.collect(), [Row(key=i) for i in range(100)])

        # users can use DataType directly instead of data type string.
        df = rdd.map(lambda row: row.key).toDF(IntegerType())
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")
        self.assertEqual(df.collect(), [Row(key=i) for i in range(100)])

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
        self.assertRaises(IllegalArgumentException, lambda: df1.join(df2, how="invalid-join-type"))

    # Cartesian products require cross join syntax
    def test_require_cross(self):

        df1 = self.spark.createDataFrame([(1, "1")], ("key", "value"))
        df2 = self.spark.createDataFrame([(1, "1")], ("key", "value"))

        with self.sql_conf({"spark.sql.crossJoin.enabled": False}):
            # joins without conditions require cross join syntax
            self.assertRaises(AnalysisException, lambda: df1.join(df2).collect())

            # works with crossJoin
            self.assertEqual(1, df1.crossJoin(df2).count())

    def test_cache(self):
        spark = self.spark
        with self.tempView("tab1", "tab2"):
            spark.createDataFrame([(2, 2), (3, 3)]).createOrReplaceTempView("tab1")
            spark.createDataFrame([(2, 2), (3, 3)]).createOrReplaceTempView("tab2")
            self.assertFalse(spark.catalog.isCached("tab1"))
            self.assertFalse(spark.catalog.isCached("tab2"))
            spark.catalog.cacheTable("tab1")
            self.assertTrue(spark.catalog.isCached("tab1"))
            self.assertFalse(spark.catalog.isCached("tab2"))
            spark.catalog.cacheTable("tab2")
            spark.catalog.uncacheTable("tab1")
            self.assertFalse(spark.catalog.isCached("tab1"))
            self.assertTrue(spark.catalog.isCached("tab2"))
            spark.catalog.clearCache()
            self.assertFalse(spark.catalog.isCached("tab1"))
            self.assertFalse(spark.catalog.isCached("tab2"))
            self.assertRaisesRegex(
                AnalysisException,
                "does_not_exist",
                lambda: spark.catalog.isCached("does_not_exist"))
            self.assertRaisesRegex(
                AnalysisException,
                "does_not_exist",
                lambda: spark.catalog.cacheTable("does_not_exist"))
            self.assertRaisesRegex(
                AnalysisException,
                "does_not_exist",
                lambda: spark.catalog.uncacheTable("does_not_exist"))

    def _to_pandas(self):
        from datetime import datetime, date
        schema = StructType().add("a", IntegerType()).add("b", StringType())\
                             .add("c", BooleanType()).add("d", FloatType())\
                             .add("dt", DateType()).add("ts", TimestampType())
        data = [
            (1, "foo", True, 3.0, date(1969, 1, 1), datetime(1969, 1, 1, 1, 1, 1)),
            (2, "foo", True, 5.0, None, None),
            (3, "bar", False, -1.0, date(2012, 3, 3), datetime(2012, 3, 3, 3, 3, 3)),
            (4, "bar", False, 6.0, date(2100, 4, 4), datetime(2100, 4, 4, 4, 4, 4)),
        ]
        df = self.spark.createDataFrame(data, schema)
        return df.toPandas()

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas(self):
        import numpy as np
        pdf = self._to_pandas()
        types = pdf.dtypes
        self.assertEqual(types[0], np.int32)
        self.assertEqual(types[1], np.object)
        self.assertEqual(types[2], np.bool)
        self.assertEqual(types[3], np.float32)
        self.assertEqual(types[4], np.object)  # datetime.date
        self.assertEqual(types[5], 'datetime64[ns]')

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_with_duplicated_column_names(self):
        import numpy as np

        sql = "select 1 v, 1 v"
        for arrowEnabled in [False, True]:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrowEnabled}):
                df = self.spark.sql(sql)
                pdf = df.toPandas()
                types = pdf.dtypes
                self.assertEqual(types.iloc[0], np.int32)
                self.assertEqual(types.iloc[1], np.int32)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_on_cross_join(self):
        import numpy as np

        sql = """
        select t1.*, t2.* from (
          select explode(sequence(1, 3)) v
        ) t1 left join (
          select explode(sequence(1, 3)) v
        ) t2
        """
        for arrowEnabled in [False, True]:
            with self.sql_conf({"spark.sql.crossJoin.enabled": True,
                                "spark.sql.execution.arrow.pyspark.enabled": arrowEnabled}):
                df = self.spark.sql(sql)
                pdf = df.toPandas()
                types = pdf.dtypes
                self.assertEqual(types.iloc[0], np.int32)
                self.assertEqual(types.iloc[1], np.int32)

    @unittest.skipIf(have_pandas, "Required Pandas was found.")
    def test_to_pandas_required_pandas_not_found(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegex(ImportError, 'Pandas >= .* must be installed'):
                self._to_pandas()

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_avoid_astype(self):
        import numpy as np
        schema = StructType().add("a", IntegerType()).add("b", StringType())\
                             .add("c", IntegerType())
        data = [(1, "foo", 16777220), (None, "bar", None)]
        df = self.spark.createDataFrame(data, schema)
        types = df.toPandas().dtypes
        self.assertEqual(types[0], np.float64)  # doesn't convert to np.int32 due to NaN value.
        self.assertEqual(types[1], np.object)
        self.assertEqual(types[2], np.float64)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_from_empty_dataframe(self):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            # SPARK-29188 test that toPandas() on an empty dataframe has the correct dtypes
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
            CAST('2019-01-01' AS TIMESTAMP) AS timestamp
            """
            dtypes_when_nonempty_df = self.spark.sql(sql).toPandas().dtypes
            dtypes_when_empty_df = self.spark.sql(sql).filter("False").toPandas().dtypes
            self.assertTrue(np.all(dtypes_when_empty_df == dtypes_when_nonempty_df))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_from_null_dataframe(self):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            # SPARK-29188 test that toPandas() on a dataframe with only nulls has correct dtypes
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
            CAST(NULL AS TIMESTAMP) AS timestamp
            """
            pdf = self.spark.sql(sql).toPandas()
            types = pdf.dtypes
            self.assertEqual(types[0], np.float64)
            self.assertEqual(types[1], np.float64)
            self.assertEqual(types[2], np.float64)
            self.assertEqual(types[3], np.float64)
            self.assertEqual(types[4], np.float32)
            self.assertEqual(types[5], np.float64)
            self.assertEqual(types[6], np.object)
            self.assertEqual(types[7], np.object)
            self.assertTrue(np.can_cast(np.datetime64, types[8]))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_to_pandas_from_mixed_dataframe(self):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            # SPARK-29188 test that toPandas() on a dataframe with some nulls has correct dtypes
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
            timestamp_seconds(col9) AS timestamp
            FROM VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1),
                        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """
            pdf_with_some_nulls = self.spark.sql(sql).toPandas()
            pdf_with_only_nulls = self.spark.sql(sql).filter('tinyint is null').toPandas()
            self.assertTrue(np.all(pdf_with_only_nulls.dtypes == pdf_with_some_nulls.dtypes))

    def test_create_dataframe_from_array_of_long(self):
        import array
        data = [Row(longarray=array.array('l', [-9223372036854775808, 0, 9223372036854775807]))]
        df = self.spark.createDataFrame(data)
        self.assertEqual(df.first(), Row(longarray=[-9223372036854775808, 0, 9223372036854775807]))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_create_dataframe_from_pandas_with_timestamp(self):
        import pandas as pd
        from datetime import datetime
        pdf = pd.DataFrame({"ts": [datetime(2017, 10, 31, 1, 1, 1)],
                            "d": [pd.Timestamp.now().date()]}, columns=["d", "ts"])
        # test types are inferred correctly without specifying schema
        df = self.spark.createDataFrame(pdf)
        self.assertTrue(isinstance(df.schema['ts'].dataType, TimestampType))
        self.assertTrue(isinstance(df.schema['d'].dataType, DateType))
        # test with schema will accept pdf as input
        df = self.spark.createDataFrame(pdf, schema="d date, ts timestamp")
        self.assertTrue(isinstance(df.schema['ts'].dataType, TimestampType))
        self.assertTrue(isinstance(df.schema['d'].dataType, DateType))

    @unittest.skipIf(have_pandas, "Required Pandas was found.")
    def test_create_dataframe_required_pandas_not_found(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                    ImportError,
                    "(Pandas >= .* must be installed|No module named '?pandas'?)"):
                import pandas as pd
                from datetime import datetime
                pdf = pd.DataFrame({"ts": [datetime(2017, 10, 31, 1, 1, 1)],
                                    "d": [pd.Timestamp.now().date()]})
                self.spark.createDataFrame(pdf)

    # Regression test for SPARK-23360
    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_create_dataframe_from_pandas_with_dst(self):
        import pandas as pd
        from pandas.testing import assert_frame_equal
        from datetime import datetime

        pdf = pd.DataFrame({'time': [datetime(2015, 10, 31, 22, 30)]})

        df = self.spark.createDataFrame(pdf)
        assert_frame_equal(pdf, df.toPandas())

        orig_env_tz = os.environ.get('TZ', None)
        try:
            tz = 'America/Los_Angeles'
            os.environ['TZ'] = tz
            time.tzset()
            with self.sql_conf({'spark.sql.session.timeZone': tz}):
                df = self.spark.createDataFrame(pdf)
                assert_frame_equal(pdf, df.toPandas())
        finally:
            del os.environ['TZ']
            if orig_env_tz is not None:
                os.environ['TZ'] = orig_env_tz
            time.tzset()

    def test_repr_behaviors(self):
        import re
        pattern = re.compile(r'^ *\|', re.MULTILINE)
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
            self.assertEqual(re.sub(pattern, '', expected1), df.__repr__())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                expected2 = """+---+-----+
                ||key|value|
                |+---+-----+
                ||  1|    1|
                ||222|  222|
                |+---+-----+
                |"""
                self.assertEqual(re.sub(pattern, '', expected2), df.__repr__())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    expected3 = """+---+-----+
                    ||key|value|
                    |+---+-----+
                    ||  1|    1|
                    |+---+-----+
                    |only showing top 1 row
                    |"""
                    self.assertEqual(re.sub(pattern, '', expected3), df.__repr__())

        # test when eager evaluation is enabled and _repr_html_ will be called
        with self.sql_conf({"spark.sql.repl.eagerEval.enabled": True}):
            expected1 = """<table border='1'>
                |<tr><th>key</th><th>value</th></tr>
                |<tr><td>1</td><td>1</td></tr>
                |<tr><td>22222</td><td>22222</td></tr>
                |</table>
                |"""
            self.assertEqual(re.sub(pattern, '', expected1), df._repr_html_())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                expected2 = """<table border='1'>
                    |<tr><th>key</th><th>value</th></tr>
                    |<tr><td>1</td><td>1</td></tr>
                    |<tr><td>222</td><td>222</td></tr>
                    |</table>
                    |"""
                self.assertEqual(re.sub(pattern, '', expected2), df._repr_html_())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    expected3 = """<table border='1'>
                        |<tr><th>key</th><th>value</th></tr>
                        |<tr><td>1</td><td>1</td></tr>
                        |</table>
                        |only showing top 1 row
                        |"""
                    self.assertEqual(re.sub(pattern, '', expected3), df._repr_html_())

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
        # SPARK-23961: toLocalIterator throws exception when not fully consumed
        # Create a DataFrame large enough so that write to socket will eventually block
        df = self.spark.range(1 << 20, numPartitions=2)
        it = df.toLocalIterator()
        self.assertEqual(df.take(1)[0], next(it))
        with QuietTest(self.sc):
            it = None  # remove iterator from scope, socket is closed when cleaned up
            # Make sure normal df operations still work
            result = []
            for i, row in enumerate(df.toLocalIterator()):
                result.append(row)
                if i == 7:
                    break
            self.assertEqual(df.take(8), result)

    def test_same_semantics_error(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegex(TypeError, "should be of DataFrame.*int"):
                self.spark.range(10).sameSemantics(1)

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

        df = self.spark.createDataFrame([('foo',)])
        df.show(5)
        df.show(5, True)
        df.show(5, 1, True)
        df.show(n=5, truncate='1', vertical=False)
        df.show(n=5, truncate=1.5, vertical=False)

        with self.assertRaisesRegex(TypeError, "Parameter 'n'"):
            df.show(True)
        with self.assertRaisesRegex(TypeError, "Parameter 'vertical'"):
            df.show(vertical='foo')
        with self.assertRaisesRegex(TypeError, "Parameter 'truncate=foo'"):
            df.show(truncate='foo')

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message)  # type: ignore
    def test_to_pandas_on_spark(self):
        import pandas as pd
        from pandas.testing import assert_frame_equal

        sdf = self.spark.createDataFrame([("a", 1), ("b", 2), ("c",  3)], ["Col1", "Col2"])
        psdf_from_sdf = sdf.to_pandas_on_spark()
        psdf_from_sdf_with_index = sdf.to_pandas_on_spark(index_col="Col1")
        pdf = pd.DataFrame({"Col1": ["a", "b", "c"], "Col2": [1, 2, 3]})
        pdf_with_index = pdf.set_index("Col1")

        assert_frame_equal(pdf, psdf_from_sdf.to_pandas())
        assert_frame_equal(pdf_with_index, psdf_from_sdf_with_index.to_pandas())


class QueryExecutionListenerTests(unittest.TestCase, SQLTestUtils):
    # These tests are separate because it uses 'spark.sql.queryExecutionListeners' which is
    # static and immutable. This can't be set or unset, for example, via `spark.conf`.

    @classmethod
    def setUpClass(cls):
        import glob
        from pyspark.find_spark_home import _find_spark_home

        SPARK_HOME = _find_spark_home()
        filename_pattern = (
            "sql/core/target/scala-*/test-classes/org/apache/spark/sql/"
            "TestQueryExecutionListener.class")
        cls.has_listener = bool(glob.glob(os.path.join(SPARK_HOME, filename_pattern)))

        if cls.has_listener:
            # Note that 'spark.sql.queryExecutionListeners' is a static immutable configuration.
            cls.spark = SparkSession.builder \
                .master("local[4]") \
                .appName(cls.__name__) \
                .config(
                    "spark.sql.queryExecutionListeners",
                    "org.apache.spark.sql.TestQueryExecutionListener") \
                .getOrCreate()

    def setUp(self):
        if not self.has_listener:
            raise self.skipTest(
                "'org.apache.spark.sql.TestQueryExecutionListener' is not "
                "available. Will skip the related tests.")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def tearDown(self):
        self.spark._jvm.OnSuccessCall.clear()

    def test_query_execution_listener_on_collect(self):
        self.assertFalse(
            self.spark._jvm.OnSuccessCall.isCalled(),
            "The callback from the query execution listener should not be called before 'collect'")
        self.spark.sql("SELECT * FROM range(1)").collect()
        self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
        self.assertTrue(
            self.spark._jvm.OnSuccessCall.isCalled(),
            "The callback from the query execution listener should be called after 'collect'")

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message)  # type: ignore
    def test_query_execution_listener_on_collect_with_arrow(self):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": True}):
            self.assertFalse(
                self.spark._jvm.OnSuccessCall.isCalled(),
                "The callback from the query execution listener should not be "
                "called before 'toPandas'")
            self.spark.sql("SELECT * FROM range(1)").toPandas()
            self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
            self.assertTrue(
                self.spark._jvm.OnSuccessCall.isCalled(),
                "The callback from the query execution listener should be called after 'toPandas'")


if __name__ == "__main__":
    from pyspark.sql.tests.test_dataframe import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
