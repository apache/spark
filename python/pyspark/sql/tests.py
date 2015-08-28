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

"""
Unit tests for pyspark.sql; additional tests are implemented as doctests in
individual modules.
"""
import os
import sys
import pydoc
import shutil
import tempfile
import pickle
import functools
import time
import datetime

import py4j

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.sql import SQLContext, HiveContext, Column, Row
from pyspark.sql.types import *
from pyspark.sql.types import UserDefinedType, _infer_type
from pyspark.tests import ReusedPySparkTestCase
from pyspark.sql.functions import UserDefinedFunction, sha2
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException, IllegalArgumentException


class UTC(datetime.tzinfo):
    """UTC"""
    ZERO = datetime.timedelta(0)

    def utcoffset(self, dt):
        return self.ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return self.ZERO


class ExamplePointUDT(UserDefinedType):
    """
    User-defined type (UDT) for ExamplePoint.
    """

    @classmethod
    def sqlType(self):
        return ArrayType(DoubleType(), False)

    @classmethod
    def module(cls):
        return 'pyspark.sql.tests'

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
        return isinstance(other, self.__class__) and \
            other.x == self.x and other.y == self.y


class PythonOnlyUDT(UserDefinedType):
    """
    User-defined type (UDT) for ExamplePoint.
    """

    @classmethod
    def sqlType(self):
        return ArrayType(DoubleType(), False)

    @classmethod
    def module(cls):
        return '__main__'

    def serialize(self, obj):
        return [obj.x, obj.y]

    def deserialize(self, datum):
        return PythonOnlyPoint(datum[0], datum[1])

    @staticmethod
    def foo():
        pass

    @property
    def props(self):
        return {}


class PythonOnlyPoint(ExamplePoint):
    """
    An example class to demonstrate UDT in only Python
    """
    __UDT__ = PythonOnlyUDT()


class MyObject(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value


class DataTypeTests(unittest.TestCase):
    # regression test for SPARK-6055
    def test_data_type_eq(self):
        lt = LongType()
        lt2 = pickle.loads(pickle.dumps(LongType()))
        self.assertEquals(lt, lt2)

    # regression test for SPARK-7978
    def test_decimal_type(self):
        t1 = DecimalType()
        t2 = DecimalType(10, 2)
        self.assertTrue(t2 is not t1)
        self.assertNotEqual(t1, t2)
        t3 = DecimalType(8)
        self.assertNotEqual(t2, t3)


class SQLTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.sqlCtx = SQLContext(cls.sc)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        rdd = cls.sc.parallelize(cls.testData, 2)
        cls.df = rdd.toDF()

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def test_row_should_be_read_only(self):
        row = Row(a=1, b=2)
        self.assertEqual(1, row.a)

        def foo():
            row.a = 3
        self.assertRaises(Exception, foo)

        row2 = self.sqlCtx.range(10).first()
        self.assertEqual(0, row2.id)

        def foo2():
            row2.id = 2
        self.assertRaises(Exception, foo2)

    def test_range(self):
        self.assertEqual(self.sqlCtx.range(1, 1).count(), 0)
        self.assertEqual(self.sqlCtx.range(1, 0, -1).count(), 1)
        self.assertEqual(self.sqlCtx.range(0, 1 << 40, 1 << 39).count(), 2)
        self.assertEqual(self.sqlCtx.range(-2).count(), 0)
        self.assertEqual(self.sqlCtx.range(3).count(), 3)

    def test_duplicated_column_names(self):
        df = self.sqlCtx.createDataFrame([(1, 2)], ["c", "c"])
        row = df.select('*').first()
        self.assertEqual(1, row[0])
        self.assertEqual(2, row[1])
        self.assertEqual("Row(c=1, c=2)", str(row))
        # Cannot access columns
        self.assertRaises(AnalysisException, lambda: df.select(df[0]).first())
        self.assertRaises(AnalysisException, lambda: df.select(df.c).first())
        self.assertRaises(AnalysisException, lambda: df.select(df["c"]).first())

    def test_explode(self):
        from pyspark.sql.functions import explode
        d = [Row(a=1, intlist=[1, 2, 3], mapfield={"a": "b"})]
        rdd = self.sc.parallelize(d)
        data = self.sqlCtx.createDataFrame(rdd)

        result = data.select(explode(data.intlist).alias("a")).select("a").collect()
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[1][0], 2)
        self.assertEqual(result[2][0], 3)

        result = data.select(explode(data.mapfield).alias("a", "b")).select("a", "b").collect()
        self.assertEqual(result[0][0], "a")
        self.assertEqual(result[0][1], "b")

    def test_and_in_expression(self):
        self.assertEqual(4, self.df.filter((self.df.key <= 10) & (self.df.value <= "2")).count())
        self.assertRaises(ValueError, lambda: (self.df.key <= 10) and (self.df.value <= "2"))
        self.assertEqual(14, self.df.filter((self.df.key <= 3) | (self.df.value < "2")).count())
        self.assertRaises(ValueError, lambda: self.df.key <= 3 or self.df.value < "2")
        self.assertEqual(99, self.df.filter(~(self.df.key == 1)).count())
        self.assertRaises(ValueError, lambda: not self.df.key == 1)

    def test_udf_with_callable(self):
        d = [Row(number=i, squared=i**2) for i in range(10)]
        rdd = self.sc.parallelize(d)
        data = self.sqlCtx.createDataFrame(rdd)

        class PlusFour:
            def __call__(self, col):
                if col is not None:
                    return col + 4

        call = PlusFour()
        pudf = UserDefinedFunction(call, LongType())
        res = data.select(pudf(data['number']).alias('plus_four'))
        self.assertEqual(res.agg({'plus_four': 'sum'}).collect()[0][0], 85)

    def test_udf_with_partial_function(self):
        d = [Row(number=i, squared=i**2) for i in range(10)]
        rdd = self.sc.parallelize(d)
        data = self.sqlCtx.createDataFrame(rdd)

        def some_func(col, param):
            if col is not None:
                return col + param

        pfunc = functools.partial(some_func, param=4)
        pudf = UserDefinedFunction(pfunc, LongType())
        res = data.select(pudf(data['number']).alias('plus_four'))
        self.assertEqual(res.agg({'plus_four': 'sum'}).collect()[0][0], 85)

    def test_udf(self):
        self.sqlCtx.registerFunction("twoArgs", lambda x, y: len(x) + y, IntegerType())
        [row] = self.sqlCtx.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], 5)

    def test_udf2(self):
        self.sqlCtx.registerFunction("strlen", lambda string: len(string), IntegerType())
        self.sqlCtx.createDataFrame(self.sc.parallelize([Row(a="test")])).registerTempTable("test")
        [res] = self.sqlCtx.sql("SELECT strlen(a) FROM test WHERE strlen(a) > 1").collect()
        self.assertEqual(4, res[0])

    def test_udf_with_array_type(self):
        d = [Row(l=list(range(3)), d={"key": list(range(5))})]
        rdd = self.sc.parallelize(d)
        self.sqlCtx.createDataFrame(rdd).registerTempTable("test")
        self.sqlCtx.registerFunction("copylist", lambda l: list(l), ArrayType(IntegerType()))
        self.sqlCtx.registerFunction("maplen", lambda d: len(d), IntegerType())
        [(l1, l2)] = self.sqlCtx.sql("select copylist(l), maplen(d) from test").collect()
        self.assertEqual(list(range(3)), l1)
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
        df.schema

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
        df2 = self.sqlCtx.createDataFrame(df.map(lambda x: x), df.schema)
        self.assertEqual(df.collect(), df2.collect())

        rdd = self.sc.parallelize(range(10)).map(lambda x: Row(a=x))
        df3 = self.sqlCtx.createDataFrame(rdd, df.schema)
        self.assertEqual(10, df3.count())

    def test_serialize_nested_array_and_map(self):
        d = [Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")})]
        rdd = self.sc.parallelize(d)
        df = self.sqlCtx.createDataFrame(rdd)
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
        d = [Row(l=[], d={}, s=None),
             Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")}, s="")]
        rdd = self.sc.parallelize(d)
        df = self.sqlCtx.createDataFrame(rdd)
        self.assertEqual([], df.map(lambda r: r.l).first())
        self.assertEqual([None, ""], df.map(lambda r: r.s).collect())
        df.registerTempTable("test")
        result = self.sqlCtx.sql("SELECT l[0].a from test where d['key'].d = '2'")
        self.assertEqual(1, result.head()[0])

        df2 = self.sqlCtx.createDataFrame(rdd, samplingRatio=1.0)
        self.assertEqual(df.schema, df2.schema)
        self.assertEqual({}, df2.map(lambda r: r.d).first())
        self.assertEqual([None, ""], df2.map(lambda r: r.s).collect())
        df2.registerTempTable("test2")
        result = self.sqlCtx.sql("SELECT l[0].a from test2 where d['key'].d = '2'")
        self.assertEqual(1, result.head()[0])

    def test_infer_nested_schema(self):
        NestedRow = Row("f1", "f2")
        nestedRdd1 = self.sc.parallelize([NestedRow([1, 2], {"row1": 1.0}),
                                          NestedRow([2, 3], {"row2": 2.0})])
        df = self.sqlCtx.inferSchema(nestedRdd1)
        self.assertEqual(Row(f1=[1, 2], f2={u'row1': 1.0}), df.collect()[0])

        nestedRdd2 = self.sc.parallelize([NestedRow([[1, 2], [2, 3]], [1, 2]),
                                          NestedRow([[2, 3], [3, 4]], [2, 3])])
        df = self.sqlCtx.inferSchema(nestedRdd2)
        self.assertEqual(Row(f1=[[1, 2], [2, 3]], f2=[1, 2]), df.collect()[0])

        from collections import namedtuple
        CustomRow = namedtuple('CustomRow', 'field1 field2')
        rdd = self.sc.parallelize([CustomRow(field1=1, field2="row1"),
                                   CustomRow(field1=2, field2="row2"),
                                   CustomRow(field1=3, field2="row3")])
        df = self.sqlCtx.inferSchema(rdd)
        self.assertEquals(Row(field1=1, field2=u'row1'), df.first())

    def test_create_dataframe_from_objects(self):
        data = [MyObject(1, "1"), MyObject(2, "2")]
        df = self.sqlCtx.createDataFrame(data)
        self.assertEqual(df.dtypes, [("key", "bigint"), ("value", "string")])
        self.assertEqual(df.first(), Row(key=1, value="1"))

    def test_select_null_literal(self):
        df = self.sqlCtx.sql("select null as col")
        self.assertEquals(Row(col=None), df.first())

    def test_apply_schema(self):
        from datetime import date, datetime
        rdd = self.sc.parallelize([(127, -128, -32768, 32767, 2147483647, 1.0,
                                    date(2010, 1, 1), datetime(2010, 1, 1, 1, 1, 1),
                                    {"a": 1}, (2,), [1, 2, 3], None)])
        schema = StructType([
            StructField("byte1", ByteType(), False),
            StructField("byte2", ByteType(), False),
            StructField("short1", ShortType(), False),
            StructField("short2", ShortType(), False),
            StructField("int1", IntegerType(), False),
            StructField("float1", FloatType(), False),
            StructField("date1", DateType(), False),
            StructField("time1", TimestampType(), False),
            StructField("map1", MapType(StringType(), IntegerType(), False), False),
            StructField("struct1", StructType([StructField("b", ShortType(), False)]), False),
            StructField("list1", ArrayType(ByteType(), False), False),
            StructField("null1", DoubleType(), True)])
        df = self.sqlCtx.createDataFrame(rdd, schema)
        results = df.map(lambda x: (x.byte1, x.byte2, x.short1, x.short2, x.int1, x.float1, x.date1,
                                    x.time1, x.map1["a"], x.struct1.b, x.list1, x.null1))
        r = (127, -128, -32768, 32767, 2147483647, 1.0, date(2010, 1, 1),
             datetime(2010, 1, 1, 1, 1, 1), 1, 2, [1, 2, 3], None)
        self.assertEqual(r, results.first())

        df.registerTempTable("table2")
        r = self.sqlCtx.sql("SELECT byte1 - 1 AS byte1, byte2 + 1 AS byte2, " +
                            "short1 + 1 AS short1, short2 - 1 AS short2, int1 - 1 AS int1, " +
                            "float1 + 1.5 as float1 FROM table2").first()

        self.assertEqual((126, -127, -32767, 32766, 2147483646, 2.5), tuple(r))

        from pyspark.sql.types import _parse_schema_abstract, _infer_schema_type
        rdd = self.sc.parallelize([(127, -32768, 1.0, datetime(2010, 1, 1, 1, 1, 1),
                                    {"a": 1}, (2,), [1, 2, 3])])
        abstract = "byte1 short1 float1 time1 map1{} struct1(b) list1[]"
        schema = _parse_schema_abstract(abstract)
        typedSchema = _infer_schema_type(rdd.first(), schema)
        df = self.sqlCtx.createDataFrame(rdd, typedSchema)
        r = (127, -32768, 1.0, datetime(2010, 1, 1, 1, 1, 1), {"a": 1}, Row(b=2), [1, 2, 3])
        self.assertEqual(r, tuple(df.first()))

    def test_struct_in_map(self):
        d = [Row(m={Row(i=1): Row(s="")})]
        df = self.sc.parallelize(d).toDF()
        k, v = list(df.head().m.items())[0]
        self.assertEqual(1, k.i)
        self.assertEqual("", v.s)

    def test_convert_row_to_dict(self):
        row = Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")})
        self.assertEqual(1, row.asDict()['l'][0].a)
        df = self.sc.parallelize([row]).toDF()
        df.registerTempTable("test")
        row = self.sqlCtx.sql("select l, d from test").head()
        self.assertEqual(1, row.asDict()["l"][0].a)
        self.assertEqual(1.0, row.asDict()['d']['key'].c)

    def test_udt(self):
        from pyspark.sql.types import _parse_datatype_json_string, _infer_type, _verify_type
        from pyspark.sql.tests import ExamplePointUDT, ExamplePoint

        def check_datatype(datatype):
            pickled = pickle.loads(pickle.dumps(datatype))
            assert datatype == pickled
            scala_datatype = self.sqlCtx._ssql_ctx.parseDataType(datatype.json())
            python_datatype = _parse_datatype_json_string(scala_datatype.json())
            assert datatype == python_datatype

        check_datatype(ExamplePointUDT())
        structtype_with_udt = StructType([StructField("label", DoubleType(), False),
                                          StructField("point", ExamplePointUDT(), False)])
        check_datatype(structtype_with_udt)
        p = ExamplePoint(1.0, 2.0)
        self.assertEqual(_infer_type(p), ExamplePointUDT())
        _verify_type(ExamplePoint(1.0, 2.0), ExamplePointUDT())
        self.assertRaises(ValueError, lambda: _verify_type([1.0, 2.0], ExamplePointUDT()))

        check_datatype(PythonOnlyUDT())
        structtype_with_udt = StructType([StructField("label", DoubleType(), False),
                                          StructField("point", PythonOnlyUDT(), False)])
        check_datatype(structtype_with_udt)
        p = PythonOnlyPoint(1.0, 2.0)
        self.assertEqual(_infer_type(p), PythonOnlyUDT())
        _verify_type(PythonOnlyPoint(1.0, 2.0), PythonOnlyUDT())
        self.assertRaises(ValueError, lambda: _verify_type([1.0, 2.0], PythonOnlyUDT()))

    def test_infer_schema_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df = self.sqlCtx.createDataFrame([row])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), ExamplePointUDT)
        df.registerTempTable("labeled_point")
        point = self.sqlCtx.sql("SELECT point FROM labeled_point").head().point
        self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df = self.sqlCtx.createDataFrame([row])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), PythonOnlyUDT)
        df.registerTempTable("labeled_point")
        point = self.sqlCtx.sql("SELECT point FROM labeled_point").head().point
        self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_apply_schema_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = (1.0, ExamplePoint(1.0, 2.0))
        schema = StructType([StructField("label", DoubleType(), False),
                             StructField("point", ExamplePointUDT(), False)])
        df = self.sqlCtx.createDataFrame([row], schema)
        point = df.head().point
        self.assertEquals(point, ExamplePoint(1.0, 2.0))

        row = (1.0, PythonOnlyPoint(1.0, 2.0))
        schema = StructType([StructField("label", DoubleType(), False),
                             StructField("point", PythonOnlyUDT(), False)])
        df = self.sqlCtx.createDataFrame([row], schema)
        point = df.head().point
        self.assertEquals(point, PythonOnlyPoint(1.0, 2.0))

    def test_udf_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df = self.sqlCtx.createDataFrame([row])
        self.assertEqual(1.0, df.map(lambda r: r.point.x).first())
        udf = UserDefinedFunction(lambda p: p.y, DoubleType())
        self.assertEqual(2.0, df.select(udf(df.point)).first()[0])
        udf2 = UserDefinedFunction(lambda p: ExamplePoint(p.x + 1, p.y + 1), ExamplePointUDT())
        self.assertEqual(ExamplePoint(2.0, 3.0), df.select(udf2(df.point)).first()[0])

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df = self.sqlCtx.createDataFrame([row])
        self.assertEqual(1.0, df.map(lambda r: r.point.x).first())
        udf = UserDefinedFunction(lambda p: p.y, DoubleType())
        self.assertEqual(2.0, df.select(udf(df.point)).first()[0])
        udf2 = UserDefinedFunction(lambda p: PythonOnlyPoint(p.x + 1, p.y + 1), PythonOnlyUDT())
        self.assertEqual(PythonOnlyPoint(2.0, 3.0), df.select(udf2(df.point)).first()[0])

    def test_parquet_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df0 = self.sqlCtx.createDataFrame([row])
        output_dir = os.path.join(self.tempdir.name, "labeled_point")
        df0.write.parquet(output_dir)
        df1 = self.sqlCtx.parquetFile(output_dir)
        point = df1.head().point
        self.assertEquals(point, ExamplePoint(1.0, 2.0))

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df0 = self.sqlCtx.createDataFrame([row])
        df0.write.parquet(output_dir, mode='overwrite')
        df1 = self.sqlCtx.parquetFile(output_dir)
        point = df1.head().point
        self.assertEquals(point, PythonOnlyPoint(1.0, 2.0))

    def test_column_operators(self):
        ci = self.df.key
        cs = self.df.value
        c = ci == cs
        self.assertTrue(isinstance((- ci - 1 - 2) % 3 * 2.5 / 3.5, Column))
        rcc = (1 + ci), (1 - ci), (1 * ci), (1 / ci), (1 % ci)
        self.assertTrue(all(isinstance(c, Column) for c in rcc))
        cb = [ci == 5, ci != 0, ci > 3, ci < 4, ci >= 0, ci <= 7]
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

    def test_freqItems(self):
        vals = [Row(a=1, b=-2.0) if i % 2 == 0 else Row(a=i, b=i * 1.0) for i in range(100)]
        df = self.sc.parallelize(vals).toDF()
        items = df.stat.freqItems(("a", "b"), 0.4).collect()[0]
        self.assertTrue(1 in items[0])
        self.assertTrue(-2.0 in items[1])

    def test_aggregator(self):
        df = self.df
        g = df.groupBy()
        self.assertEqual([99, 100], sorted(g.agg({'key': 'max', 'value': 'count'}).collect()[0]))
        self.assertEqual([Row(**{"AVG(key#0)": 49.5})], g.mean().collect())

        from pyspark.sql import functions
        self.assertEqual((0, u'99'),
                         tuple(g.agg(functions.first(df.key), functions.last(df.value)).first()))
        self.assertTrue(95 < g.agg(functions.approxCountDistinct(df.key)).first()[0])
        self.assertEqual(100, g.agg(functions.countDistinct(df.value)).first()[0])

    def test_corr(self):
        import math
        df = self.sc.parallelize([Row(a=i, b=math.sqrt(i)) for i in range(10)]).toDF()
        corr = df.stat.corr("a", "b")
        self.assertTrue(abs(corr - 0.95734012) < 1e-6)

    def test_cov(self):
        df = self.sc.parallelize([Row(a=i, b=2 * i) for i in range(10)]).toDF()
        cov = df.stat.cov("a", "b")
        self.assertTrue(abs(cov - 55.0 / 3) < 1e-6)

    def test_crosstab(self):
        df = self.sc.parallelize([Row(a=i % 3, b=i % 2) for i in range(1, 7)]).toDF()
        ct = df.stat.crosstab("a", "b").collect()
        ct = sorted(ct, key=lambda x: x[0])
        for i, row in enumerate(ct):
            self.assertEqual(row[0], str(i))
            self.assertTrue(row[1], 1)
            self.assertTrue(row[2], 1)

    def test_math_functions(self):
        df = self.sc.parallelize([Row(a=i, b=2 * i) for i in range(10)]).toDF()
        from pyspark.sql import functions
        import math

        def get_values(l):
            return [j[0] for j in l]

        def assert_close(a, b):
            c = get_values(b)
            diff = [abs(v - c[k]) < 1e-6 for k, v in enumerate(a)]
            return sum(diff) == len(a)
        assert_close([math.cos(i) for i in range(10)],
                     df.select(functions.cos(df.a)).collect())
        assert_close([math.cos(i) for i in range(10)],
                     df.select(functions.cos("a")).collect())
        assert_close([math.sin(i) for i in range(10)],
                     df.select(functions.sin(df.a)).collect())
        assert_close([math.sin(i) for i in range(10)],
                     df.select(functions.sin(df['a'])).collect())
        assert_close([math.pow(i, 2 * i) for i in range(10)],
                     df.select(functions.pow(df.a, df.b)).collect())
        assert_close([math.pow(i, 2) for i in range(10)],
                     df.select(functions.pow(df.a, 2)).collect())
        assert_close([math.pow(i, 2) for i in range(10)],
                     df.select(functions.pow(df.a, 2.0)).collect())
        assert_close([math.hypot(i, 2 * i) for i in range(10)],
                     df.select(functions.hypot(df.a, df.b)).collect())

    def test_rand_functions(self):
        df = self.df
        from pyspark.sql import functions
        rnd = df.select('key', functions.rand()).collect()
        for row in rnd:
            assert row[1] >= 0.0 and row[1] <= 1.0, "got: %s" % row[1]
        rndn = df.select('key', functions.randn(5)).collect()
        for row in rndn:
            assert row[1] >= -4.0 and row[1] <= 4.0, "got: %s" % row[1]

        # If the specified seed is 0, we should use it.
        # https://issues.apache.org/jira/browse/SPARK-9691
        rnd1 = df.select('key', functions.rand(0)).collect()
        rnd2 = df.select('key', functions.rand(0)).collect()
        self.assertEqual(sorted(rnd1), sorted(rnd2))

        rndn1 = df.select('key', functions.randn(0)).collect()
        rndn2 = df.select('key', functions.randn(0)).collect()
        self.assertEqual(sorted(rndn1), sorted(rndn2))

    def test_between_function(self):
        df = self.sc.parallelize([
            Row(a=1, b=2, c=3),
            Row(a=2, b=1, c=3),
            Row(a=4, b=1, c=4)]).toDF()
        self.assertEqual([Row(a=2, b=1, c=3), Row(a=4, b=1, c=4)],
                         df.filter(df.a.between(df.b, df.c)).collect())

    def test_struct_type(self):
        from pyspark.sql.types import StructType, StringType, StructField
        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        struct2 = StructType([StructField("f1", StringType(), True),
                              StructField("f2", StringType(), True, None)])
        self.assertEqual(struct1, struct2)

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        struct2 = StructType([StructField("f1", StringType(), True)])
        self.assertNotEqual(struct1, struct2)

        struct1 = (StructType().add(StructField("f1", StringType(), True))
                   .add(StructField("f2", StringType(), True, None)))
        struct2 = StructType([StructField("f1", StringType(), True),
                              StructField("f2", StringType(), True, None)])
        self.assertEqual(struct1, struct2)

        struct1 = (StructType().add(StructField("f1", StringType(), True))
                   .add(StructField("f2", StringType(), True, None)))
        struct2 = StructType([StructField("f1", StringType(), True)])
        self.assertNotEqual(struct1, struct2)

        # Catch exception raised during improper construction
        try:
            struct1 = StructType().add("name")
            self.assertEqual(1, 0)
        except ValueError:
            self.assertEqual(1, 1)

    def test_save_and_load(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.json(tmpPath)
        actual = self.sqlCtx.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        schema = StructType([StructField("value", StringType(), True)])
        actual = self.sqlCtx.read.json(tmpPath, schema)
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

        df.write.json(tmpPath, "overwrite")
        actual = self.sqlCtx.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        df.write.save(format="json", mode="overwrite", path=tmpPath,
                      noUse="this options will not be used in save.")
        actual = self.sqlCtx.read.load(format="json", path=tmpPath,
                                       noUse="this options will not be used in load.")
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        defaultDataSourceName = self.sqlCtx.getConf("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.sqlCtx.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        actual = self.sqlCtx.load(path=tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.sqlCtx.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

    def test_save_and_load_builder(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.json(tmpPath)
        actual = self.sqlCtx.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        schema = StructType([StructField("value", StringType(), True)])
        actual = self.sqlCtx.read.json(tmpPath, schema)
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

        df.write.mode("overwrite").json(tmpPath)
        actual = self.sqlCtx.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        df.write.mode("overwrite").options(noUse="this options will not be used in save.")\
                .option("noUse", "this option will not be used in save.")\
                .format("json").save(path=tmpPath)
        actual =\
            self.sqlCtx.read.format("json")\
                            .load(path=tmpPath, noUse="this options will not be used in load.")
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        defaultDataSourceName = self.sqlCtx.getConf("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.sqlCtx.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        actual = self.sqlCtx.load(path=tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
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

    def test_access_column(self):
        df = self.df
        self.assertTrue(isinstance(df.key, Column))
        self.assertTrue(isinstance(df['key'], Column))
        self.assertTrue(isinstance(df[0], Column))
        self.assertRaises(IndexError, lambda: df[2])
        self.assertRaises(AnalysisException, lambda: df["bad_key"])
        self.assertRaises(TypeError, lambda: df[{}])

    def test_column_name_with_non_ascii(self):
        df = self.sqlCtx.createDataFrame([(1,)], ["数量"])
        self.assertEqual(StructType([StructField("数量", LongType(), True)]), df.schema)
        self.assertEqual("DataFrame[数量: bigint]", str(df))
        self.assertEqual([("数量", 'bigint')], df.dtypes)
        self.assertEqual(1, df.select("数量").first()[0])
        self.assertEqual(1, df.select(df["数量"]).first()[0])

    def test_access_nested_types(self):
        df = self.sc.parallelize([Row(l=[1], r=Row(a=1, b="b"), d={"k": "v"})]).toDF()
        self.assertEqual(1, df.select(df.l[0]).first()[0])
        self.assertEqual(1, df.select(df.l.getItem(0)).first()[0])
        self.assertEqual(1, df.select(df.r.a).first()[0])
        self.assertEqual("b", df.select(df.r.getField("b")).first()[0])
        self.assertEqual("v", df.select(df.d["k"]).first()[0])
        self.assertEqual("v", df.select(df.d.getItem("k")).first()[0])

    def test_field_accessor(self):
        df = self.sc.parallelize([Row(l=[1], r=Row(a=1, b="b"), d={"k": "v"})]).toDF()
        self.assertEqual(1, df.select(df.l[0]).first()[0])
        self.assertEqual(1, df.select(df.r["a"]).first()[0])
        self.assertEqual(1, df.select(df["r.a"]).first()[0])
        self.assertEqual("b", df.select(df.r["b"]).first()[0])
        self.assertEqual("b", df.select(df["r.b"]).first()[0])
        self.assertEqual("v", df.select(df.d["k"]).first()[0])

    def test_infer_long_type(self):
        longrow = [Row(f1='a', f2=100000000000000)]
        df = self.sc.parallelize(longrow).toDF()
        self.assertEqual(df.schema.fields[1].dataType, LongType())

        # this saving as Parquet caused issues as well.
        output_dir = os.path.join(self.tempdir.name, "infer_long_type")
        df.saveAsParquetFile(output_dir)
        df1 = self.sqlCtx.parquetFile(output_dir)
        self.assertEquals('a', df1.first().f1)
        self.assertEquals(100000000000000, df1.first().f2)

        self.assertEqual(_infer_type(1), LongType())
        self.assertEqual(_infer_type(2**10), LongType())
        self.assertEqual(_infer_type(2**20), LongType())
        self.assertEqual(_infer_type(2**31 - 1), LongType())
        self.assertEqual(_infer_type(2**31), LongType())
        self.assertEqual(_infer_type(2**61), LongType())
        self.assertEqual(_infer_type(2**71), LongType())

    def test_filter_with_datetime(self):
        time = datetime.datetime(2015, 4, 17, 23, 1, 2, 3000)
        date = time.date()
        row = Row(date=date, time=time)
        df = self.sqlCtx.createDataFrame([row])
        self.assertEqual(1, df.filter(df.date == date).count())
        self.assertEqual(1, df.filter(df.time == time).count())
        self.assertEqual(0, df.filter(df.date > date).count())
        self.assertEqual(0, df.filter(df.time > time).count())

    def test_time_with_timezone(self):
        day = datetime.date.today()
        now = datetime.datetime.now()
        ts = time.mktime(now.timetuple())
        # class in __main__ is not serializable
        from pyspark.sql.tests import UTC
        utc = UTC()
        utcnow = datetime.datetime.utcfromtimestamp(ts)  # without microseconds
        # add microseconds to utcnow (keeping year,month,day,hour,minute,second)
        utcnow = datetime.datetime(*(utcnow.timetuple()[:6] + (now.microsecond, utc)))
        df = self.sqlCtx.createDataFrame([(day, now, utcnow)])
        day1, now1, utcnow1 = df.first()
        self.assertEqual(day1, day)
        self.assertEqual(now, now1)
        self.assertEqual(now, utcnow1)

    def test_decimal(self):
        from decimal import Decimal
        schema = StructType([StructField("decimal", DecimalType(10, 5))])
        df = self.sqlCtx.createDataFrame([(Decimal("3.14159"),)], schema)
        row = df.select(df.decimal + 1).first()
        self.assertEqual(row[0], Decimal("4.14159"))
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.parquet(tmpPath)
        df2 = self.sqlCtx.read.parquet(tmpPath)
        row = df2.first()
        self.assertEqual(row[0], Decimal("3.14159"))

    def test_dropna(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)])

        # shouldn't drop a non-null row
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', 50, 80.1)], schema).dropna().count(),
            1)

        # dropping rows with a single null value
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna().count(),
            0)
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna(how='any').count(),
            0)

        # if how = 'all', only drop rows if all values are null
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna(how='all').count(),
            1)
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(None, None, None)], schema).dropna(how='all').count(),
            0)

        # how and subset
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', 50, None)], schema).dropna(how='any', subset=['name', 'age']).count(),
            1)
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', None, None)], schema).dropna(how='any', subset=['name', 'age']).count(),
            0)

        # threshold
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', None, 80.1)], schema).dropna(thresh=2).count(),
            1)
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', None, None)], schema).dropna(thresh=2).count(),
            0)

        # threshold and subset
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', 50, None)], schema).dropna(thresh=2, subset=['name', 'age']).count(),
            1)
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', None, 180.9)], schema).dropna(thresh=2, subset=['name', 'age']).count(),
            0)

        # thresh should take precedence over how
        self.assertEqual(self.sqlCtx.createDataFrame(
            [(u'Alice', 50, None)], schema).dropna(
                how='any', thresh=2, subset=['name', 'age']).count(),
            1)

    def test_fillna(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)])

        # fillna shouldn't change non-null values
        row = self.sqlCtx.createDataFrame([(u'Alice', 10, 80.1)], schema).fillna(50).first()
        self.assertEqual(row.age, 10)

        # fillna with int
        row = self.sqlCtx.createDataFrame([(u'Alice', None, None)], schema).fillna(50).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.0)

        # fillna with double
        row = self.sqlCtx.createDataFrame([(u'Alice', None, None)], schema).fillna(50.1).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.1)

        # fillna with string
        row = self.sqlCtx.createDataFrame([(None, None, None)], schema).fillna("hello").first()
        self.assertEqual(row.name, u"hello")
        self.assertEqual(row.age, None)

        # fillna with subset specified for numeric cols
        row = self.sqlCtx.createDataFrame(
            [(None, None, None)], schema).fillna(50, subset=['name', 'age']).first()
        self.assertEqual(row.name, None)
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, None)

        # fillna with subset specified for numeric cols
        row = self.sqlCtx.createDataFrame(
            [(None, None, None)], schema).fillna("haha", subset=['name', 'age']).first()
        self.assertEqual(row.name, "haha")
        self.assertEqual(row.age, None)
        self.assertEqual(row.height, None)

    def test_bitwise_operations(self):
        from pyspark.sql import functions
        row = Row(a=170, b=75)
        df = self.sqlCtx.createDataFrame([row])
        result = df.select(df.a.bitwiseAND(df.b)).collect()[0].asDict()
        self.assertEqual(170 & 75, result['(a & b)'])
        result = df.select(df.a.bitwiseOR(df.b)).collect()[0].asDict()
        self.assertEqual(170 | 75, result['(a | b)'])
        result = df.select(df.a.bitwiseXOR(df.b)).collect()[0].asDict()
        self.assertEqual(170 ^ 75, result['(a ^ b)'])
        result = df.select(functions.bitwiseNOT(df.b)).collect()[0].asDict()
        self.assertEqual(~75, result['~b'])

    def test_expr(self):
        from pyspark.sql import functions
        row = Row(a="length string", b=75)
        df = self.sqlCtx.createDataFrame([row])
        result = df.select(functions.expr("length(a)")).collect()[0].asDict()
        self.assertEqual(13, result["'length(a)"])

    def test_replace(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)])

        # replace with int
        row = self.sqlCtx.createDataFrame([(u'Alice', 10, 10.0)], schema).replace(10, 20).first()
        self.assertEqual(row.age, 20)
        self.assertEqual(row.height, 20.0)

        # replace with double
        row = self.sqlCtx.createDataFrame(
            [(u'Alice', 80, 80.0)], schema).replace(80.0, 82.1).first()
        self.assertEqual(row.age, 82)
        self.assertEqual(row.height, 82.1)

        # replace with string
        row = self.sqlCtx.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace(u'Alice', u'Ann').first()
        self.assertEqual(row.name, u"Ann")
        self.assertEqual(row.age, 10)

        # replace with subset specified by a string of a column name w/ actual change
        row = self.sqlCtx.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace(10, 20, subset='age').first()
        self.assertEqual(row.age, 20)

        # replace with subset specified by a string of a column name w/o actual change
        row = self.sqlCtx.createDataFrame(
            [(u'Alice', 10, 80.1)], schema).replace(10, 20, subset='height').first()
        self.assertEqual(row.age, 10)

        # replace with subset specified with one column replaced, another column not in subset
        # stays unchanged.
        row = self.sqlCtx.createDataFrame(
            [(u'Alice', 10, 10.0)], schema).replace(10, 20, subset=['name', 'age']).first()
        self.assertEqual(row.name, u'Alice')
        self.assertEqual(row.age, 20)
        self.assertEqual(row.height, 10.0)

        # replace with subset specified but no column will be replaced
        row = self.sqlCtx.createDataFrame(
            [(u'Alice', 10, None)], schema).replace(10, 20, subset=['name', 'height']).first()
        self.assertEqual(row.name, u'Alice')
        self.assertEqual(row.age, 10)
        self.assertEqual(row.height, None)

    def test_capture_analysis_exception(self):
        self.assertRaises(AnalysisException, lambda: self.sqlCtx.sql("select abc"))
        self.assertRaises(AnalysisException, lambda: self.df.selectExpr("a + b"))
        # RuntimeException should not be captured
        self.assertRaises(py4j.protocol.Py4JJavaError, lambda: self.sqlCtx.sql("abc"))

    def test_capture_illegalargument_exception(self):
        self.assertRaisesRegexp(IllegalArgumentException, "Setting negative mapred.reduce.tasks",
                                lambda: self.sqlCtx.sql("SET mapred.reduce.tasks=-1"))
        df = self.sqlCtx.createDataFrame([(1, 2)], ["a", "b"])
        self.assertRaisesRegexp(IllegalArgumentException, "1024 is not in the permitted values",
                                lambda: df.select(sha2(df.a, 1024)).collect())

    def test_with_column_with_existing_name(self):
        keys = self.df.withColumn("key", self.df.key).select("key").collect()
        self.assertEqual([r.key for r in keys], list(range(100)))


class HiveContextSQLTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        try:
            cls.sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        except py4j.protocol.Py4JError:
            cls.tearDownClass()
            raise unittest.SkipTest("Hive is not available")
        except TypeError:
            cls.tearDownClass()
            raise unittest.SkipTest("Hive is not available")
        os.unlink(cls.tempdir.name)
        _scala_HiveContext =\
            cls.sc._jvm.org.apache.spark.sql.hive.test.TestHiveContext(cls.sc._jsc.sc())
        cls.sqlCtx = HiveContext(cls.sc, _scala_HiveContext)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.sc.parallelize(cls.testData).toDF()

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def test_save_and_load_table(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.saveAsTable("savedJsonTable", "json", "append", path=tmpPath)
        actual = self.sqlCtx.createExternalTable("externalJsonTable", tmpPath, "json")
        self.assertEqual(sorted(df.collect()),
                         sorted(self.sqlCtx.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.collect()),
                         sorted(self.sqlCtx.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.sqlCtx.sql("DROP TABLE externalJsonTable")

        df.write.saveAsTable("savedJsonTable", "json", "overwrite", path=tmpPath)
        schema = StructType([StructField("value", StringType(), True)])
        actual = self.sqlCtx.createExternalTable("externalJsonTable", source="json",
                                                 schema=schema, path=tmpPath,
                                                 noUse="this options will not be used")
        self.assertEqual(sorted(df.collect()),
                         sorted(self.sqlCtx.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.select("value").collect()),
                         sorted(self.sqlCtx.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))
        self.sqlCtx.sql("DROP TABLE savedJsonTable")
        self.sqlCtx.sql("DROP TABLE externalJsonTable")

        defaultDataSourceName = self.sqlCtx.getConf("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.sqlCtx.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        df.write.saveAsTable("savedJsonTable", path=tmpPath, mode="overwrite")
        actual = self.sqlCtx.createExternalTable("externalJsonTable", path=tmpPath)
        self.assertEqual(sorted(df.collect()),
                         sorted(self.sqlCtx.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.collect()),
                         sorted(self.sqlCtx.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.sqlCtx.sql("DROP TABLE savedJsonTable")
        self.sqlCtx.sql("DROP TABLE externalJsonTable")
        self.sqlCtx.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

    def test_window_functions(self):
        df = self.sqlCtx.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.partitionBy("value").orderBy("key")
        from pyspark.sql import functions as F
        sel = df.select(df.value, df.key,
                        F.max("key").over(w.rowsBetween(0, 1)),
                        F.min("key").over(w.rowsBetween(0, 1)),
                        F.count("key").over(w.rowsBetween(float('-inf'), float('inf'))),
                        F.rowNumber().over(w),
                        F.rank().over(w),
                        F.denseRank().over(w),
                        F.ntile(2).over(w))
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 1, 1, 1, 1, 1),
            ("2", 1, 1, 1, 3, 1, 1, 1, 1),
            ("2", 1, 2, 1, 3, 2, 1, 1, 1),
            ("2", 2, 2, 2, 3, 3, 3, 2, 2)
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

    def test_window_functions_without_partitionBy(self):
        df = self.sqlCtx.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.orderBy("key", df.value)
        from pyspark.sql import functions as F
        sel = df.select(df.value, df.key,
                        F.max("key").over(w.rowsBetween(0, 1)),
                        F.min("key").over(w.rowsBetween(0, 1)),
                        F.count("key").over(w.rowsBetween(float('-inf'), float('inf'))),
                        F.rowNumber().over(w),
                        F.rank().over(w),
                        F.denseRank().over(w),
                        F.ntile(2).over(w))
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 4, 1, 1, 1, 1),
            ("2", 1, 1, 1, 4, 2, 2, 2, 1),
            ("2", 1, 2, 1, 4, 3, 2, 2, 2),
            ("2", 2, 2, 2, 4, 4, 4, 3, 2)
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])


if __name__ == "__main__":
    unittest.main()
