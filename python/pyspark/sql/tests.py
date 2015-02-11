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

"""
Unit tests for pyspark.sql; additional tests are implemented as doctests in
individual modules.
"""
import os
import sys
import pydoc
import shutil
import tempfile

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.sql import SQLContext, HiveContext, Column
from pyspark.sql.types import IntegerType, Row, ArrayType, StructType, StructField, \
    UserDefinedType, DoubleType, LongType, StringType
from pyspark.tests import ReusedPySparkTestCase


class ExamplePointUDT(UserDefinedType):
    """
    User-defined type (UDT) for ExamplePoint.
    """

    @classmethod
    def sqlType(self):
        return ArrayType(DoubleType(), False)

    @classmethod
    def module(cls):
        return 'pyspark.tests'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.test.ExamplePointUDT'

    def serialize(self, obj):
        return [obj.x, obj.y]

    def deserialize(self, datum):
        return ExamplePoint(datum[0], datum[1])


class ExamplePoint:
    """
    An example class to demonstrate UDT in Scala, Java, and Python.
    """

    __UDT__ = ExamplePointUDT()

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return "ExamplePoint(%s,%s)" % (self.x, self.y)

    def __str__(self):
        return "(%s,%s)" % (self.x, self.y)

    def __eq__(self, other):
        return isinstance(other, ExamplePoint) and \
            other.x == self.x and other.y == self.y


class SQLTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.sqlCtx = SQLContext(cls.sc)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        rdd = cls.sc.parallelize(cls.testData)
        cls.df = cls.sqlCtx.inferSchema(rdd)

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def test_udf(self):
        self.sqlCtx.registerFunction("twoArgs", lambda x, y: len(x) + y, IntegerType())
        [row] = self.sqlCtx.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], 5)

    def test_udf2(self):
        self.sqlCtx.registerFunction("strlen", lambda string: len(string), IntegerType())
        self.sqlCtx.inferSchema(self.sc.parallelize([Row(a="test")])).registerTempTable("test")
        [res] = self.sqlCtx.sql("SELECT strlen(a) FROM test WHERE strlen(a) > 1").collect()
        self.assertEqual(4, res[0])

    def test_udf_with_array_type(self):
        d = [Row(l=range(3), d={"key": range(5)})]
        rdd = self.sc.parallelize(d)
        self.sqlCtx.inferSchema(rdd).registerTempTable("test")
        self.sqlCtx.registerFunction("copylist", lambda l: list(l), ArrayType(IntegerType()))
        self.sqlCtx.registerFunction("maplen", lambda d: len(d), IntegerType())
        [(l1, l2)] = self.sqlCtx.sql("select copylist(l), maplen(d) from test").collect()
        self.assertEqual(range(3), l1)
        self.assertEqual(1, l2)

    def test_broadcast_in_udf(self):
        bar = {"a": "aa", "b": "bb", "c": "abc"}
        foo = self.sc.broadcast(bar)
        self.sqlCtx.registerFunction("MYUDF", lambda x: foo.value[x] if x else '')
        [res] = self.sqlCtx.sql("SELECT MYUDF('c')").collect()
        self.assertEqual("abc", res[0])
        [res] = self.sqlCtx.sql("SELECT MYUDF('')").collect()
        self.assertEqual("", res[0])

    def test_basic_functions(self):
        rdd = self.sc.parallelize(['{"foo":"bar"}', '{"foo":"baz"}'])
        df = self.sqlCtx.jsonRDD(rdd)
        df.count()
        df.collect()
        df.schema()

        # cache and checkpoint
        self.assertFalse(df.is_cached)
        df.persist()
        df.unpersist()
        df.cache()
        self.assertTrue(df.is_cached)
        self.assertEqual(2, df.count())

        df.registerTempTable("temp")
        df = self.sqlCtx.sql("select foo from temp")
        df.count()
        df.collect()

    def test_apply_schema_to_row(self):
        df = self.sqlCtx.jsonRDD(self.sc.parallelize(["""{"a":2}"""]))
        df2 = self.sqlCtx.applySchema(df.map(lambda x: x), df.schema())
        self.assertEqual(df.collect(), df2.collect())

        rdd = self.sc.parallelize(range(10)).map(lambda x: Row(a=x))
        df3 = self.sqlCtx.applySchema(rdd, df.schema())
        self.assertEqual(10, df3.count())

    def test_serialize_nested_array_and_map(self):
        d = [Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")})]
        rdd = self.sc.parallelize(d)
        df = self.sqlCtx.inferSchema(rdd)
        row = df.head()
        self.assertEqual(1, len(row.l))
        self.assertEqual(1, row.l[0].a)
        self.assertEqual("2", row.d["key"].d)

        l = df.map(lambda x: x.l).first()
        self.assertEqual(1, len(l))
        self.assertEqual('s', l[0].b)

        d = df.map(lambda x: x.d).first()
        self.assertEqual(1, len(d))
        self.assertEqual(1.0, d["key"].c)

        row = df.map(lambda x: x.d["key"]).first()
        self.assertEqual(1.0, row.c)
        self.assertEqual("2", row.d)

    def test_infer_schema(self):
        d = [Row(l=[], d={}),
             Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")}, s="")]
        rdd = self.sc.parallelize(d)
        df = self.sqlCtx.inferSchema(rdd)
        self.assertEqual([], df.map(lambda r: r.l).first())
        self.assertEqual([None, ""], df.map(lambda r: r.s).collect())
        df.registerTempTable("test")
        result = self.sqlCtx.sql("SELECT l[0].a from test where d['key'].d = '2'")
        self.assertEqual(1, result.head()[0])

        df2 = self.sqlCtx.inferSchema(rdd, 1.0)
        self.assertEqual(df.schema(), df2.schema())
        self.assertEqual({}, df2.map(lambda r: r.d).first())
        self.assertEqual([None, ""], df2.map(lambda r: r.s).collect())
        df2.registerTempTable("test2")
        result = self.sqlCtx.sql("SELECT l[0].a from test2 where d['key'].d = '2'")
        self.assertEqual(1, result.head()[0])

    def test_struct_in_map(self):
        d = [Row(m={Row(i=1): Row(s="")})]
        rdd = self.sc.parallelize(d)
        df = self.sqlCtx.inferSchema(rdd)
        k, v = df.head().m.items()[0]
        self.assertEqual(1, k.i)
        self.assertEqual("", v.s)

    def test_convert_row_to_dict(self):
        row = Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")})
        self.assertEqual(1, row.asDict()['l'][0].a)
        rdd = self.sc.parallelize([row])
        df = self.sqlCtx.inferSchema(rdd)
        df.registerTempTable("test")
        row = self.sqlCtx.sql("select l, d from test").head()
        self.assertEqual(1, row.asDict()["l"][0].a)
        self.assertEqual(1.0, row.asDict()['d']['key'].c)

    def test_infer_schema_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        rdd = self.sc.parallelize([row])
        df = self.sqlCtx.inferSchema(rdd)
        schema = df.schema()
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), ExamplePointUDT)
        df.registerTempTable("labeled_point")
        point = self.sqlCtx.sql("SELECT point FROM labeled_point").head().point
        self.assertEqual(point, ExamplePoint(1.0, 2.0))

    def test_apply_schema_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = (1.0, ExamplePoint(1.0, 2.0))
        rdd = self.sc.parallelize([row])
        schema = StructType([StructField("label", DoubleType(), False),
                             StructField("point", ExamplePointUDT(), False)])
        df = self.sqlCtx.applySchema(rdd, schema)
        point = df.head().point
        self.assertEquals(point, ExamplePoint(1.0, 2.0))

    def test_parquet_with_udt(self):
        from pyspark.sql.tests import ExamplePoint
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        rdd = self.sc.parallelize([row])
        df0 = self.sqlCtx.inferSchema(rdd)
        output_dir = os.path.join(self.tempdir.name, "labeled_point")
        df0.saveAsParquetFile(output_dir)
        df1 = self.sqlCtx.parquetFile(output_dir)
        point = df1.head().point
        self.assertEquals(point, ExamplePoint(1.0, 2.0))

    def test_column_operators(self):
        ci = self.df.key
        cs = self.df.value
        c = ci == cs
        self.assertTrue(isinstance((- ci - 1 - 2) % 3 * 2.5 / 3.5, Column))
        rcc = (1 + ci), (1 - ci), (1 * ci), (1 / ci), (1 % ci)
        self.assertTrue(all(isinstance(c, Column) for c in rcc))
        cb = [ci == 5, ci != 0, ci > 3, ci < 4, ci >= 0, ci <= 7, ci and cs, ci or cs]
        self.assertTrue(all(isinstance(c, Column) for c in cb))
        cbool = (ci & ci), (ci | ci), (~ci)
        self.assertTrue(all(isinstance(c, Column) for c in cbool))
        css = cs.like('a'), cs.rlike('a'), cs.asc(), cs.desc(), cs.startswith('a'), cs.endswith('a')
        self.assertTrue(all(isinstance(c, Column) for c in css))
        self.assertTrue(isinstance(ci.cast(LongType()), Column))

    def test_column_select(self):
        df = self.df
        self.assertEqual(self.testData, df.select("*").collect())
        self.assertEqual(self.testData, df.select(df.key, df.value).collect())
        self.assertEqual([Row(value='1')], df.where(df.key == 1).select(df.value).collect())

    def test_aggregator(self):
        df = self.df
        g = df.groupBy()
        self.assertEqual([99, 100], sorted(g.agg({'key': 'max', 'value': 'count'}).collect()[0]))
        self.assertEqual([Row(**{"AVG(key#0)": 49.5})], g.mean().collect())

        from pyspark.sql import Dsl
        self.assertEqual((0, u'99'), tuple(g.agg(Dsl.first(df.key), Dsl.last(df.value)).first()))
        self.assertTrue(95 < g.agg(Dsl.approxCountDistinct(df.key)).first()[0])
        self.assertEqual(100, g.agg(Dsl.countDistinct(df.value)).first()[0])

    def test_save_and_load(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.save(tmpPath, "org.apache.spark.sql.json", "error")
        actual = self.sqlCtx.load(tmpPath, "org.apache.spark.sql.json")
        self.assertTrue(sorted(df.collect()) == sorted(actual.collect()))

        schema = StructType([StructField("value", StringType(), True)])
        actual = self.sqlCtx.load(tmpPath, "org.apache.spark.sql.json", schema)
        self.assertTrue(sorted(df.select("value").collect()) == sorted(actual.collect()))

        df.save(tmpPath, "org.apache.spark.sql.json", "overwrite")
        actual = self.sqlCtx.load(tmpPath, "org.apache.spark.sql.json")
        self.assertTrue(sorted(df.collect()) == sorted(actual.collect()))

        df.save(source="org.apache.spark.sql.json", mode="overwrite", path=tmpPath,
                noUse="this options will not be used in save.")
        actual = self.sqlCtx.load(source="org.apache.spark.sql.json", path=tmpPath,
                                  noUse="this options will not be used in load.")
        self.assertTrue(sorted(df.collect()) == sorted(actual.collect()))

        defaultDataSourceName = self.sqlCtx.getConf("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.sqlCtx.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        actual = self.sqlCtx.load(path=tmpPath)
        self.assertTrue(sorted(df.collect()) == sorted(actual.collect()))
        self.sqlCtx.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

    def test_help_command(self):
        # Regression test for SPARK-5464
        rdd = self.sc.parallelize(['{"foo":"bar"}', '{"foo":"baz"}'])
        df = self.sqlCtx.jsonRDD(rdd)
        # render_doc() reproduces the help() exception without printing output
        pydoc.render_doc(df)
        pydoc.render_doc(df.foo)
        pydoc.render_doc(df.take(1))


class HiveContextSQLTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        print "type", type(cls.sc)
        print "type", type(cls.sc._jsc)
        _scala_HiveContext =\
            cls.sc._jvm.org.apache.spark.sql.hive.test.TestHiveContext(cls.sc._jsc.sc())
        cls.sqlCtx = HiveContext(cls.sc, _scala_HiveContext)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        rdd = cls.sc.parallelize(cls.testData)
        cls.df = cls.sqlCtx.inferSchema(rdd)

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def test_save_and_load_table(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.saveAsTable("savedJsonTable", "org.apache.spark.sql.json", "append", path=tmpPath)
        actual = self.sqlCtx.createExternalTable("externalJsonTable", tmpPath,
                                                 "org.apache.spark.sql.json")
        self.assertTrue(
            sorted(df.collect()) ==
            sorted(self.sqlCtx.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertTrue(
            sorted(df.collect()) ==
            sorted(self.sqlCtx.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertTrue(sorted(df.collect()) == sorted(actual.collect()))
        self.sqlCtx.sql("DROP TABLE externalJsonTable")

        df.saveAsTable("savedJsonTable", "org.apache.spark.sql.json", "overwrite", path=tmpPath)
        schema = StructType([StructField("value", StringType(), True)])
        actual = self.sqlCtx.createExternalTable("externalJsonTable",
                                                 source="org.apache.spark.sql.json",
                                                 schema=schema, path=tmpPath,
                                                 noUse="this options will not be used")
        self.assertTrue(
            sorted(df.collect()) ==
            sorted(self.sqlCtx.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertTrue(
            sorted(df.select("value").collect()) ==
            sorted(self.sqlCtx.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertTrue(sorted(df.select("value").collect()) == sorted(actual.collect()))
        self.sqlCtx.sql("DROP TABLE savedJsonTable")
        self.sqlCtx.sql("DROP TABLE externalJsonTable")

        defaultDataSourceName = self.sqlCtx.getConf("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.sqlCtx.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        df.saveAsTable("savedJsonTable", path=tmpPath, mode="overwrite")
        actual = self.sqlCtx.createExternalTable("externalJsonTable", path=tmpPath)
        self.assertTrue(
            sorted(df.collect()) ==
            sorted(self.sqlCtx.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertTrue(
            sorted(df.collect()) ==
            sorted(self.sqlCtx.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertTrue(sorted(df.collect()) == sorted(actual.collect()))
        self.sqlCtx.sql("DROP TABLE savedJsonTable")
        self.sqlCtx.sql("DROP TABLE externalJsonTable")
        self.sqlCtx.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

if __name__ == "__main__":
    unittest.main()
