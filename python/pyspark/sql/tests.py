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
import subprocess
import pydoc
import shutil
import tempfile
import pickle
import functools
import time
import datetime

import py4j
try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.sql import SparkSession, HiveContext, Column, Row
from pyspark.sql.types import *
from pyspark.sql.types import UserDefinedType, _infer_type
from pyspark.tests import ReusedPySparkTestCase, SparkSubmitTests
from pyspark.sql.functions import UserDefinedFunction, sha2
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException, ParseException, IllegalArgumentException


class UTCOffsetTimezone(datetime.tzinfo):
    """
    Specifies timezone in UTC offset
    """

    def __init__(self, offset=0):
        self.ZERO = datetime.timedelta(hours=offset)

    def utcoffset(self, dt):
        return self.ZERO

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
        self.assertEqual(lt, lt2)

    # regression test for SPARK-7978
    def test_decimal_type(self):
        t1 = DecimalType()
        t2 = DecimalType(10, 2)
        self.assertTrue(t2 is not t1)
        self.assertNotEqual(t1, t2)
        t3 = DecimalType(8)
        self.assertNotEqual(t2, t3)

    # regression test for SPARK-10392
    def test_datetype_equal_zero(self):
        dt = DateType()
        self.assertEqual(dt.fromInternal(0), datetime.date(1970, 1, 1))

    # regression test for SPARK-17035
    def test_timestamp_microsecond(self):
        tst = TimestampType()
        self.assertEqual(tst.toInternal(datetime.datetime.max) % 1000000, 999999)

    def test_empty_row(self):
        row = Row()
        self.assertEqual(len(row), 0)


class SQLTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.spark = SparkSession(cls.sc)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.spark.createDataFrame(cls.testData)

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        cls.spark.stop()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def test_row_should_be_read_only(self):
        row = Row(a=1, b=2)
        self.assertEqual(1, row.a)

        def foo():
            row.a = 3
        self.assertRaises(Exception, foo)

        row2 = self.spark.range(10).first()
        self.assertEqual(0, row2.id)

        def foo2():
            row2.id = 2
        self.assertRaises(Exception, foo2)

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

    def test_column_name_encoding(self):
        """Ensure that created columns has `str` type consistently."""
        columns = self.spark.createDataFrame([('Alice', 1)], ['name', u'age']).columns
        self.assertEqual(columns, ['name', 'age'])
        self.assertTrue(isinstance(columns[0], str))
        self.assertTrue(isinstance(columns[1], str))

    def test_explode(self):
        from pyspark.sql.functions import explode
        d = [Row(a=1, intlist=[1, 2, 3], mapfield={"a": "b"})]
        rdd = self.sc.parallelize(d)
        data = self.spark.createDataFrame(rdd)

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
        data = self.spark.createDataFrame(rdd)

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
        data = self.spark.createDataFrame(rdd)

        def some_func(col, param):
            if col is not None:
                return col + param

        pfunc = functools.partial(some_func, param=4)
        pudf = UserDefinedFunction(pfunc, LongType())
        res = data.select(pudf(data['number']).alias('plus_four'))
        self.assertEqual(res.agg({'plus_four': 'sum'}).collect()[0][0], 85)

    def test_udf(self):
        self.spark.catalog.registerFunction("twoArgs", lambda x, y: len(x) + y, IntegerType())
        [row] = self.spark.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], 5)

    def test_udf2(self):
        self.spark.catalog.registerFunction("strlen", lambda string: len(string), IntegerType())
        self.spark.createDataFrame(self.sc.parallelize([Row(a="test")]))\
            .createOrReplaceTempView("test")
        [res] = self.spark.sql("SELECT strlen(a) FROM test WHERE strlen(a) > 1").collect()
        self.assertEqual(4, res[0])

    def test_chained_udf(self):
        self.spark.catalog.registerFunction("double", lambda x: x + x, IntegerType())
        [row] = self.spark.sql("SELECT double(1)").collect()
        self.assertEqual(row[0], 2)
        [row] = self.spark.sql("SELECT double(double(1))").collect()
        self.assertEqual(row[0], 4)
        [row] = self.spark.sql("SELECT double(double(1) + 1)").collect()
        self.assertEqual(row[0], 6)

    def test_multiple_udfs(self):
        self.spark.catalog.registerFunction("double", lambda x: x * 2, IntegerType())
        [row] = self.spark.sql("SELECT double(1), double(2)").collect()
        self.assertEqual(tuple(row), (2, 4))
        [row] = self.spark.sql("SELECT double(double(1)), double(double(2) + 2)").collect()
        self.assertEqual(tuple(row), (4, 12))
        self.spark.catalog.registerFunction("add", lambda x, y: x + y, IntegerType())
        [row] = self.spark.sql("SELECT double(add(1, 2)), add(double(2), 1)").collect()
        self.assertEqual(tuple(row), (6, 5))

    def test_udf_in_filter_on_top_of_outer_join(self):
        from pyspark.sql.functions import udf
        left = self.spark.createDataFrame([Row(a=1)])
        right = self.spark.createDataFrame([Row(a=1)])
        df = left.join(right, on='a', how='left_outer')
        df = df.withColumn('b', udf(lambda x: 'x')(df.a))
        self.assertEqual(df.filter('b = "x"').collect(), [Row(a=1, b='x')])

    def test_udf_without_arguments(self):
        self.spark.catalog.registerFunction("foo", lambda: "bar")
        [row] = self.spark.sql("SELECT foo()").collect()
        self.assertEqual(row[0], "bar")

    def test_udf_with_array_type(self):
        d = [Row(l=list(range(3)), d={"key": list(range(5))})]
        rdd = self.sc.parallelize(d)
        self.spark.createDataFrame(rdd).createOrReplaceTempView("test")
        self.spark.catalog.registerFunction("copylist", lambda l: list(l), ArrayType(IntegerType()))
        self.spark.catalog.registerFunction("maplen", lambda d: len(d), IntegerType())
        [(l1, l2)] = self.spark.sql("select copylist(l), maplen(d) from test").collect()
        self.assertEqual(list(range(3)), l1)
        self.assertEqual(1, l2)

    def test_broadcast_in_udf(self):
        bar = {"a": "aa", "b": "bb", "c": "abc"}
        foo = self.sc.broadcast(bar)
        self.spark.catalog.registerFunction("MYUDF", lambda x: foo.value[x] if x else '')
        [res] = self.spark.sql("SELECT MYUDF('c')").collect()
        self.assertEqual("abc", res[0])
        [res] = self.spark.sql("SELECT MYUDF('')").collect()
        self.assertEqual("", res[0])

    def test_udf_with_aggregate_function(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        from pyspark.sql.functions import udf, col, sum
        from pyspark.sql.types import BooleanType

        my_filter = udf(lambda a: a == 1, BooleanType())
        sel = df.select(col("key")).distinct().filter(my_filter(col("key")))
        self.assertEqual(sel.collect(), [Row(key=1)])

        my_copy = udf(lambda x: x, IntegerType())
        my_add = udf(lambda a, b: int(a + b), IntegerType())
        my_strlen = udf(lambda x: len(x), IntegerType())
        sel = df.groupBy(my_copy(col("key")).alias("k"))\
            .agg(sum(my_strlen(col("value"))).alias("s"))\
            .select(my_add(col("k"), col("s")).alias("t"))
        self.assertEqual(sel.collect(), [Row(t=4), Row(t=3)])

    def test_udf_in_generate(self):
        from pyspark.sql.functions import udf, explode
        df = self.spark.range(5)
        f = udf(lambda x: list(range(x)), ArrayType(LongType()))
        row = df.select(explode(f(*df))).groupBy().sum().first()
        self.assertEqual(row[0], 10)

    def test_udf_with_order_by_and_limit(self):
        from pyspark.sql.functions import udf
        my_copy = udf(lambda x: x, IntegerType())
        df = self.spark.range(10).orderBy("id")
        res = df.select(df.id, my_copy(df.id).alias("copy")).limit(1)
        res.explain(True)
        self.assertEqual(res.collect(), [Row(id=0, copy=0)])

    def test_basic_functions(self):
        rdd = self.sc.parallelize(['{"foo":"bar"}', '{"foo":"baz"}'])
        df = self.spark.read.json(rdd)
        df.count()
        df.collect()
        df.schema

        # cache and checkpoint
        self.assertFalse(df.is_cached)
        df.persist()
        df.unpersist(True)
        df.cache()
        self.assertTrue(df.is_cached)
        self.assertEqual(2, df.count())

        df.createOrReplaceTempView("temp")
        df = self.spark.sql("select foo from temp")
        df.count()
        df.collect()

    def test_apply_schema_to_row(self):
        df = self.spark.read.json(self.sc.parallelize(["""{"a":2}"""]))
        df2 = self.spark.createDataFrame(df.rdd.map(lambda x: x), df.schema)
        self.assertEqual(df.collect(), df2.collect())

        rdd = self.sc.parallelize(range(10)).map(lambda x: Row(a=x))
        df3 = self.spark.createDataFrame(rdd, df.schema)
        self.assertEqual(10, df3.count())

    def test_infer_schema_to_local(self):
        input = [{"a": 1}, {"b": "coffee"}]
        rdd = self.sc.parallelize(input)
        df = self.spark.createDataFrame(input)
        df2 = self.spark.createDataFrame(rdd, samplingRatio=1.0)
        self.assertEqual(df.schema, df2.schema)

        rdd = self.sc.parallelize(range(10)).map(lambda x: Row(a=x, b=None))
        df3 = self.spark.createDataFrame(rdd, df.schema)
        self.assertEqual(10, df3.count())

    def test_apply_schema_to_dict_and_rows(self):
        schema = StructType().add("b", StringType()).add("a", IntegerType())
        input = [{"a": 1}, {"b": "coffee"}]
        rdd = self.sc.parallelize(input)
        for verify in [False, True]:
            df = self.spark.createDataFrame(input, schema, verifySchema=verify)
            df2 = self.spark.createDataFrame(rdd, schema, verifySchema=verify)
            self.assertEqual(df.schema, df2.schema)

            rdd = self.sc.parallelize(range(10)).map(lambda x: Row(a=x, b=None))
            df3 = self.spark.createDataFrame(rdd, schema, verifySchema=verify)
            self.assertEqual(10, df3.count())
            input = [Row(a=x, b=str(x)) for x in range(10)]
            df4 = self.spark.createDataFrame(input, schema, verifySchema=verify)
            self.assertEqual(10, df4.count())

    def test_create_dataframe_schema_mismatch(self):
        input = [Row(a=1)]
        rdd = self.sc.parallelize(range(3)).map(lambda i: Row(a=i))
        schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
        df = self.spark.createDataFrame(rdd, schema)
        self.assertRaises(Exception, lambda: df.show())

    def test_serialize_nested_array_and_map(self):
        d = [Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")})]
        rdd = self.sc.parallelize(d)
        df = self.spark.createDataFrame(rdd)
        row = df.head()
        self.assertEqual(1, len(row.l))
        self.assertEqual(1, row.l[0].a)
        self.assertEqual("2", row.d["key"].d)

        l = df.rdd.map(lambda x: x.l).first()
        self.assertEqual(1, len(l))
        self.assertEqual('s', l[0].b)

        d = df.rdd.map(lambda x: x.d).first()
        self.assertEqual(1, len(d))
        self.assertEqual(1.0, d["key"].c)

        row = df.rdd.map(lambda x: x.d["key"]).first()
        self.assertEqual(1.0, row.c)
        self.assertEqual("2", row.d)

    def test_infer_schema(self):
        d = [Row(l=[], d={}, s=None),
             Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")}, s="")]
        rdd = self.sc.parallelize(d)
        df = self.spark.createDataFrame(rdd)
        self.assertEqual([], df.rdd.map(lambda r: r.l).first())
        self.assertEqual([None, ""], df.rdd.map(lambda r: r.s).collect())
        df.createOrReplaceTempView("test")
        result = self.spark.sql("SELECT l[0].a from test where d['key'].d = '2'")
        self.assertEqual(1, result.head()[0])

        df2 = self.spark.createDataFrame(rdd, samplingRatio=1.0)
        self.assertEqual(df.schema, df2.schema)
        self.assertEqual({}, df2.rdd.map(lambda r: r.d).first())
        self.assertEqual([None, ""], df2.rdd.map(lambda r: r.s).collect())
        df2.createOrReplaceTempView("test2")
        result = self.spark.sql("SELECT l[0].a from test2 where d['key'].d = '2'")
        self.assertEqual(1, result.head()[0])

    def test_infer_nested_schema(self):
        NestedRow = Row("f1", "f2")
        nestedRdd1 = self.sc.parallelize([NestedRow([1, 2], {"row1": 1.0}),
                                          NestedRow([2, 3], {"row2": 2.0})])
        df = self.spark.createDataFrame(nestedRdd1)
        self.assertEqual(Row(f1=[1, 2], f2={u'row1': 1.0}), df.collect()[0])

        nestedRdd2 = self.sc.parallelize([NestedRow([[1, 2], [2, 3]], [1, 2]),
                                          NestedRow([[2, 3], [3, 4]], [2, 3])])
        df = self.spark.createDataFrame(nestedRdd2)
        self.assertEqual(Row(f1=[[1, 2], [2, 3]], f2=[1, 2]), df.collect()[0])

        from collections import namedtuple
        CustomRow = namedtuple('CustomRow', 'field1 field2')
        rdd = self.sc.parallelize([CustomRow(field1=1, field2="row1"),
                                   CustomRow(field1=2, field2="row2"),
                                   CustomRow(field1=3, field2="row3")])
        df = self.spark.createDataFrame(rdd)
        self.assertEqual(Row(field1=1, field2=u'row1'), df.first())

    def test_create_dataframe_from_objects(self):
        data = [MyObject(1, "1"), MyObject(2, "2")]
        df = self.spark.createDataFrame(data)
        self.assertEqual(df.dtypes, [("key", "bigint"), ("value", "string")])
        self.assertEqual(df.first(), Row(key=1, value="1"))

    def test_select_null_literal(self):
        df = self.spark.sql("select null as col")
        self.assertEqual(Row(col=None), df.first())

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
        df = self.spark.createDataFrame(rdd, schema)
        results = df.rdd.map(lambda x: (x.byte1, x.byte2, x.short1, x.short2, x.int1, x.float1,
                             x.date1, x.time1, x.map1["a"], x.struct1.b, x.list1, x.null1))
        r = (127, -128, -32768, 32767, 2147483647, 1.0, date(2010, 1, 1),
             datetime(2010, 1, 1, 1, 1, 1), 1, 2, [1, 2, 3], None)
        self.assertEqual(r, results.first())

        df.createOrReplaceTempView("table2")
        r = self.spark.sql("SELECT byte1 - 1 AS byte1, byte2 + 1 AS byte2, " +
                           "short1 + 1 AS short1, short2 - 1 AS short2, int1 - 1 AS int1, " +
                           "float1 + 1.5 as float1 FROM table2").first()

        self.assertEqual((126, -127, -32767, 32766, 2147483646, 2.5), tuple(r))

        from pyspark.sql.types import _parse_schema_abstract, _infer_schema_type
        rdd = self.sc.parallelize([(127, -32768, 1.0, datetime(2010, 1, 1, 1, 1, 1),
                                    {"a": 1}, (2,), [1, 2, 3])])
        abstract = "byte1 short1 float1 time1 map1{} struct1(b) list1[]"
        schema = _parse_schema_abstract(abstract)
        typedSchema = _infer_schema_type(rdd.first(), schema)
        df = self.spark.createDataFrame(rdd, typedSchema)
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
        df.createOrReplaceTempView("test")
        row = self.spark.sql("select l, d from test").head()
        self.assertEqual(1, row.asDict()["l"][0].a)
        self.assertEqual(1.0, row.asDict()['d']['key'].c)

    def test_udt(self):
        from pyspark.sql.types import _parse_datatype_json_string, _infer_type, _verify_type
        from pyspark.sql.tests import ExamplePointUDT, ExamplePoint

        def check_datatype(datatype):
            pickled = pickle.loads(pickle.dumps(datatype))
            assert datatype == pickled
            scala_datatype = self.spark._jsparkSession.parseDataType(datatype.json())
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

    def test_simple_udt_in_df(self):
        schema = StructType().add("key", LongType()).add("val", PythonOnlyUDT())
        df = self.spark.createDataFrame(
            [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)],
            schema=schema)
        df.show()

    def test_nested_udt_in_df(self):
        schema = StructType().add("key", LongType()).add("val", ArrayType(PythonOnlyUDT()))
        df = self.spark.createDataFrame(
            [(i % 3, [PythonOnlyPoint(float(i), float(i))]) for i in range(10)],
            schema=schema)
        df.collect()

        schema = StructType().add("key", LongType()).add("val",
                                                         MapType(LongType(), PythonOnlyUDT()))
        df = self.spark.createDataFrame(
            [(i % 3, {i % 3: PythonOnlyPoint(float(i + 1), float(i + 1))}) for i in range(10)],
            schema=schema)
        df.collect()

    def test_complex_nested_udt_in_df(self):
        from pyspark.sql.functions import udf

        schema = StructType().add("key", LongType()).add("val", PythonOnlyUDT())
        df = self.spark.createDataFrame(
            [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)],
            schema=schema)
        df.collect()

        gd = df.groupby("key").agg({"val": "collect_list"})
        gd.collect()
        udf = udf(lambda k, v: [(k, v[0])], ArrayType(df.schema))
        gd.select(udf(*gd)).collect()

    def test_udt_with_none(self):
        df = self.spark.range(0, 10, 1, 1)

        def myudf(x):
            if x > 0:
                return PythonOnlyPoint(float(x), float(x))

        self.spark.catalog.registerFunction("udf", myudf, PythonOnlyUDT())
        rows = [r[0] for r in df.selectExpr("udf(id)").take(2)]
        self.assertEqual(rows, [None, PythonOnlyPoint(1, 1)])

    def test_infer_schema_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), ExamplePointUDT)
        df.createOrReplaceTempView("labeled_point")
        point = self.spark.sql("SELECT point FROM labeled_point").head().point
        self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), PythonOnlyUDT)
        df.createOrReplaceTempView("labeled_point")
        point = self.spark.sql("SELECT point FROM labeled_point").head().point
        self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_apply_schema_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = (1.0, ExamplePoint(1.0, 2.0))
        schema = StructType([StructField("label", DoubleType(), False),
                             StructField("point", ExamplePointUDT(), False)])
        df = self.spark.createDataFrame([row], schema)
        point = df.head().point
        self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = (1.0, PythonOnlyPoint(1.0, 2.0))
        schema = StructType([StructField("label", DoubleType(), False),
                             StructField("point", PythonOnlyUDT(), False)])
        df = self.spark.createDataFrame([row], schema)
        point = df.head().point
        self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_udf_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        self.assertEqual(1.0, df.rdd.map(lambda r: r.point.x).first())
        udf = UserDefinedFunction(lambda p: p.y, DoubleType())
        self.assertEqual(2.0, df.select(udf(df.point)).first()[0])
        udf2 = UserDefinedFunction(lambda p: ExamplePoint(p.x + 1, p.y + 1), ExamplePointUDT())
        self.assertEqual(ExamplePoint(2.0, 3.0), df.select(udf2(df.point)).first()[0])

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        self.assertEqual(1.0, df.rdd.map(lambda r: r.point.x).first())
        udf = UserDefinedFunction(lambda p: p.y, DoubleType())
        self.assertEqual(2.0, df.select(udf(df.point)).first()[0])
        udf2 = UserDefinedFunction(lambda p: PythonOnlyPoint(p.x + 1, p.y + 1), PythonOnlyUDT())
        self.assertEqual(PythonOnlyPoint(2.0, 3.0), df.select(udf2(df.point)).first()[0])

    def test_parquet_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df0 = self.spark.createDataFrame([row])
        output_dir = os.path.join(self.tempdir.name, "labeled_point")
        df0.write.parquet(output_dir)
        df1 = self.spark.read.parquet(output_dir)
        point = df1.head().point
        self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df0 = self.spark.createDataFrame([row])
        df0.write.parquet(output_dir, mode='overwrite')
        df1 = self.spark.read.parquet(output_dir)
        point = df1.head().point
        self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_union_with_udt(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
        row1 = (1.0, ExamplePoint(1.0, 2.0))
        row2 = (2.0, ExamplePoint(3.0, 4.0))
        schema = StructType([StructField("label", DoubleType(), False),
                             StructField("point", ExamplePointUDT(), False)])
        df1 = self.spark.createDataFrame([row1], schema)
        df2 = self.spark.createDataFrame([row2], schema)

        result = df1.union(df2).orderBy("label").collect()
        self.assertEqual(
            result,
            [
                Row(label=1.0, point=ExamplePoint(1.0, 2.0)),
                Row(label=2.0, point=ExamplePoint(3.0, 4.0))
            ]
        )

    def test_column_operators(self):
        ci = self.df.key
        cs = self.df.value
        c = ci == cs
        self.assertTrue(isinstance((- ci - 1 - 2) % 3 * 2.5 / 3.5, Column))
        rcc = (1 + ci), (1 - ci), (1 * ci), (1 / ci), (1 % ci), (1 ** ci), (ci ** 1)
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

    def test_first_last_ignorenulls(self):
        from pyspark.sql import functions
        df = self.spark.range(0, 100)
        df2 = df.select(functions.when(df.id % 3 == 0, None).otherwise(df.id).alias("id"))
        df3 = df2.select(functions.first(df2.id, False).alias('a'),
                         functions.first(df2.id, True).alias('b'),
                         functions.last(df2.id, False).alias('c'),
                         functions.last(df2.id, True).alias('d'))
        self.assertEqual([Row(a=None, b=1, c=None, d=98)], df3.collect())

    def test_approxQuantile(self):
        df = self.sc.parallelize([Row(a=i) for i in range(10)]).toDF()
        aq = df.stat.approxQuantile("a", [0.1, 0.5, 0.9], 0.1)
        self.assertTrue(isinstance(aq, list))
        self.assertEqual(len(aq), 3)
        self.assertTrue(all(isinstance(q, float) for q in aq))

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
        with self.assertRaises(ValueError):
            struct1 = StructType().add("name")

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        for field in struct1:
            self.assertIsInstance(field, StructField)

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        self.assertEqual(len(struct1), 2)

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        self.assertIs(struct1["f1"], struct1.fields[0])
        self.assertIs(struct1[0], struct1.fields[0])
        self.assertEqual(struct1[0:1], StructType(struct1.fields[0:1]))
        with self.assertRaises(KeyError):
            not_a_field = struct1["f9"]
        with self.assertRaises(IndexError):
            not_a_field = struct1[9]
        with self.assertRaises(TypeError):
            not_a_field = struct1[9.9]

    def test_metadata_null(self):
        from pyspark.sql.types import StructType, StringType, StructField
        schema = StructType([StructField("f1", StringType(), True, None),
                             StructField("f2", StringType(), True, {'a': None})])
        rdd = self.sc.parallelize([["a", "b"], ["c", "d"]])
        self.spark.createDataFrame(rdd, schema)

    def test_save_and_load(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.json(tmpPath)
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        schema = StructType([StructField("value", StringType(), True)])
        actual = self.spark.read.json(tmpPath, schema)
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

        df.write.json(tmpPath, "overwrite")
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        df.write.save(format="json", mode="overwrite", path=tmpPath,
                      noUse="this options will not be used in save.")
        actual = self.spark.read.load(format="json", path=tmpPath,
                                      noUse="this options will not be used in load.")
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        defaultDataSourceName = self.spark.conf.get("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.spark.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        actual = self.spark.read.load(path=tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        csvpath = os.path.join(tempfile.mkdtemp(), 'data')
        df.write.option('quote', None).format('csv').save(csvpath)

        shutil.rmtree(tmpPath)

    def test_save_and_load_builder(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.json(tmpPath)
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        schema = StructType([StructField("value", StringType(), True)])
        actual = self.spark.read.json(tmpPath, schema)
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

        df.write.mode("overwrite").json(tmpPath)
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        df.write.mode("overwrite").options(noUse="this options will not be used in save.")\
                .option("noUse", "this option will not be used in save.")\
                .format("json").save(path=tmpPath)
        actual =\
            self.spark.read.format("json")\
                           .load(path=tmpPath, noUse="this options will not be used in load.")
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        defaultDataSourceName = self.spark.conf.get("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.spark.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        actual = self.spark.read.load(path=tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

    def test_stream_trigger_takes_keyword_args(self):
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
        try:
            df.writeStream.trigger('5 seconds')
            self.fail("Should have thrown an exception")
        except TypeError:
            # should throw error
            pass

    def test_stream_read_options(self):
        schema = StructType([StructField("data", StringType(), False)])
        df = self.spark.readStream\
            .format('text')\
            .option('path', 'python/test_support/sql/streaming')\
            .schema(schema)\
            .load()
        self.assertTrue(df.isStreaming)
        self.assertEqual(df.schema.simpleString(), "struct<data:string>")

    def test_stream_read_options_overwrite(self):
        bad_schema = StructType([StructField("test", IntegerType(), False)])
        schema = StructType([StructField("data", StringType(), False)])
        df = self.spark.readStream.format('csv').option('path', 'python/test_support/sql/fake') \
            .schema(bad_schema)\
            .load(path='python/test_support/sql/streaming', schema=schema, format='text')
        self.assertTrue(df.isStreaming)
        self.assertEqual(df.schema.simpleString(), "struct<data:string>")

    def test_stream_save_options(self):
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
        for q in self.spark._wrapped.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, 'out')
        chk = os.path.join(tmpPath, 'chk')
        q = df.writeStream.option('checkpointLocation', chk).queryName('this_query') \
            .format('parquet').outputMode('append').option('path', out).start()
        try:
            self.assertEqual(q.name, 'this_query')
            self.assertTrue(q.isActive)
            q.processAllAvailable()
            output_files = []
            for _, _, files in os.walk(out):
                output_files.extend([f for f in files if not f.startswith('.')])
            self.assertTrue(len(output_files) > 0)
            self.assertTrue(len(os.listdir(chk)) > 0)
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_stream_save_options_overwrite(self):
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
        for q in self.spark._wrapped.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, 'out')
        chk = os.path.join(tmpPath, 'chk')
        fake1 = os.path.join(tmpPath, 'fake1')
        fake2 = os.path.join(tmpPath, 'fake2')
        q = df.writeStream.option('checkpointLocation', fake1)\
            .format('memory').option('path', fake2) \
            .queryName('fake_query').outputMode('append') \
            .start(path=out, format='parquet', queryName='this_query', checkpointLocation=chk)

        try:
            self.assertEqual(q.name, 'this_query')
            self.assertTrue(q.isActive)
            q.processAllAvailable()
            output_files = []
            for _, _, files in os.walk(out):
                output_files.extend([f for f in files if not f.startswith('.')])
            self.assertTrue(len(output_files) > 0)
            self.assertTrue(len(os.listdir(chk)) > 0)
            self.assertFalse(os.path.isdir(fake1))  # should not have been created
            self.assertFalse(os.path.isdir(fake2))  # should not have been created
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_stream_await_termination(self):
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
        for q in self.spark._wrapped.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, 'out')
        chk = os.path.join(tmpPath, 'chk')
        q = df.writeStream\
            .start(path=out, format='parquet', queryName='this_query', checkpointLocation=chk)
        try:
            self.assertTrue(q.isActive)
            try:
                q.awaitTermination("hello")
                self.fail("Expected a value exception")
            except ValueError:
                pass
            now = time.time()
            # test should take at least 2 seconds
            res = q.awaitTermination(2.6)
            duration = time.time() - now
            self.assertTrue(duration >= 2)
            self.assertFalse(res)
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_query_manager_await_termination(self):
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
        for q in self.spark._wrapped.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, 'out')
        chk = os.path.join(tmpPath, 'chk')
        q = df.writeStream\
            .start(path=out, format='parquet', queryName='this_query', checkpointLocation=chk)
        try:
            self.assertTrue(q.isActive)
            try:
                self.spark._wrapped.streams.awaitAnyTermination("hello")
                self.fail("Expected a value exception")
            except ValueError:
                pass
            now = time.time()
            # test should take at least 2 seconds
            res = self.spark._wrapped.streams.awaitAnyTermination(2.6)
            duration = time.time() - now
            self.assertTrue(duration >= 2)
            self.assertFalse(res)
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_help_command(self):
        # Regression test for SPARK-5464
        rdd = self.sc.parallelize(['{"foo":"bar"}', '{"foo":"baz"}'])
        df = self.spark.read.json(rdd)
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
        if sys.version >= '3':
            columnName = "数量"
            self.assertTrue(isinstance(columnName, str))
        else:
            columnName = unicode("数量", "utf-8")
            self.assertTrue(isinstance(columnName, unicode))
        schema = StructType([StructField(columnName, LongType(), True)])
        df = self.spark.createDataFrame([(1,)], schema)
        self.assertEqual(schema, df.schema)
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
        df.write.parquet(output_dir)
        df1 = self.spark.read.parquet(output_dir)
        self.assertEqual('a', df1.first().f1)
        self.assertEqual(100000000000000, df1.first().f2)

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
        df = self.spark.createDataFrame([row])
        self.assertEqual(1, df.filter(df.date == date).count())
        self.assertEqual(1, df.filter(df.time == time).count())
        self.assertEqual(0, df.filter(df.date > date).count())
        self.assertEqual(0, df.filter(df.time > time).count())

    def test_filter_with_datetime_timezone(self):
        dt1 = datetime.datetime(2015, 4, 17, 23, 1, 2, 3000, tzinfo=UTCOffsetTimezone(0))
        dt2 = datetime.datetime(2015, 4, 17, 23, 1, 2, 3000, tzinfo=UTCOffsetTimezone(1))
        row = Row(date=dt1)
        df = self.spark.createDataFrame([row])
        self.assertEqual(0, df.filter(df.date == dt2).count())
        self.assertEqual(1, df.filter(df.date > dt2).count())
        self.assertEqual(0, df.filter(df.date < dt2).count())

    def test_time_with_timezone(self):
        day = datetime.date.today()
        now = datetime.datetime.now()
        ts = time.mktime(now.timetuple())
        # class in __main__ is not serializable
        from pyspark.sql.tests import UTCOffsetTimezone
        utc = UTCOffsetTimezone()
        utcnow = datetime.datetime.utcfromtimestamp(ts)  # without microseconds
        # add microseconds to utcnow (keeping year,month,day,hour,minute,second)
        utcnow = datetime.datetime(*(utcnow.timetuple()[:6] + (now.microsecond, utc)))
        df = self.spark.createDataFrame([(day, now, utcnow)])
        day1, now1, utcnow1 = df.first()
        self.assertEqual(day1, day)
        self.assertEqual(now, now1)
        self.assertEqual(now, utcnow1)

    def test_decimal(self):
        from decimal import Decimal
        schema = StructType([StructField("decimal", DecimalType(10, 5))])
        df = self.spark.createDataFrame([(Decimal("3.14159"),)], schema)
        row = df.select(df.decimal + 1).first()
        self.assertEqual(row[0], Decimal("4.14159"))
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.parquet(tmpPath)
        df2 = self.spark.read.parquet(tmpPath)
        row = df2.first()
        self.assertEqual(row[0], Decimal("3.14159"))

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
            StructField("height", DoubleType(), True)])

        # fillna shouldn't change non-null values
        row = self.spark.createDataFrame([(u'Alice', 10, 80.1)], schema).fillna(50).first()
        self.assertEqual(row.age, 10)

        # fillna with int
        row = self.spark.createDataFrame([(u'Alice', None, None)], schema).fillna(50).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.0)

        # fillna with double
        row = self.spark.createDataFrame([(u'Alice', None, None)], schema).fillna(50.1).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.1)

        # fillna with string
        row = self.spark.createDataFrame([(None, None, None)], schema).fillna("hello").first()
        self.assertEqual(row.name, u"hello")
        self.assertEqual(row.age, None)

        # fillna with subset specified for numeric cols
        row = self.spark.createDataFrame(
            [(None, None, None)], schema).fillna(50, subset=['name', 'age']).first()
        self.assertEqual(row.name, None)
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, None)

        # fillna with subset specified for numeric cols
        row = self.spark.createDataFrame(
            [(None, None, None)], schema).fillna("haha", subset=['name', 'age']).first()
        self.assertEqual(row.name, "haha")
        self.assertEqual(row.age, None)
        self.assertEqual(row.height, None)

    def test_bitwise_operations(self):
        from pyspark.sql import functions
        row = Row(a=170, b=75)
        df = self.spark.createDataFrame([row])
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
        df = self.spark.createDataFrame([row])
        result = df.select(functions.expr("length(a)")).collect()[0].asDict()
        self.assertEqual(13, result["length(a)"])

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

    def test_capture_analysis_exception(self):
        self.assertRaises(AnalysisException, lambda: self.spark.sql("select abc"))
        self.assertRaises(AnalysisException, lambda: self.df.selectExpr("a + b"))

    def test_capture_parse_exception(self):
        self.assertRaises(ParseException, lambda: self.spark.sql("abc"))

    def test_capture_illegalargument_exception(self):
        self.assertRaisesRegexp(IllegalArgumentException, "Setting negative mapred.reduce.tasks",
                                lambda: self.spark.sql("SET mapred.reduce.tasks=-1"))
        df = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        self.assertRaisesRegexp(IllegalArgumentException, "1024 is not in the permitted values",
                                lambda: df.select(sha2(df.a, 1024)).collect())
        try:
            df.select(sha2(df.a, 1024)).collect()
        except IllegalArgumentException as e:
            self.assertRegexpMatches(e.desc, "1024 is not in the permitted values")
            self.assertRegexpMatches(e.stackTrace,
                                     "org.apache.spark.sql.functions")

    def test_with_column_with_existing_name(self):
        keys = self.df.withColumn("key", self.df.key).select("key").collect()
        self.assertEqual([r.key for r in keys], list(range(100)))

    # regression test for SPARK-10417
    def test_column_iterator(self):

        def foo():
            for x in self.df.key:
                break

        self.assertRaises(TypeError, foo)

    # add test for SPARK-10577 (test broadcast join hint)
    def test_functions_broadcast(self):
        from pyspark.sql.functions import broadcast

        df1 = self.spark.createDataFrame([(1, "1"), (2, "2")], ("key", "value"))
        df2 = self.spark.createDataFrame([(1, "1"), (2, "2")], ("key", "value"))

        # equijoin - should be converted into broadcast join
        plan1 = df1.join(broadcast(df2), "key")._jdf.queryExecution().executedPlan()
        self.assertEqual(1, plan1.toString().count("BroadcastHashJoin"))

        # no join key -- should not be a broadcast join
        plan2 = df1.join(broadcast(df2))._jdf.queryExecution().executedPlan()
        self.assertEqual(0, plan2.toString().count("BroadcastHashJoin"))

        # planner should not crash without a join
        broadcast(df1)._jdf.queryExecution().executedPlan()

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
        self.assertRaisesRegexp(Exception, "Length of object",
                                lambda: rdd.toDF("key: int").collect())

        # field types mismatch will cause exception at runtime.
        self.assertRaisesRegexp(Exception, "FloatType can not accept",
                                lambda: rdd.toDF("key: float, value: string").collect())

        # flat schema values will be wrapped into row.
        df = rdd.map(lambda row: row.key).toDF("int")
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")
        self.assertEqual(df.collect(), [Row(key=i) for i in range(100)])

        # users can use DataType directly instead of data type string.
        df = rdd.map(lambda row: row.key).toDF(IntegerType())
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")
        self.assertEqual(df.collect(), [Row(key=i) for i in range(100)])

    def test_conf(self):
        spark = self.spark
        spark.conf.set("bogo", "sipeo")
        self.assertEqual(spark.conf.get("bogo"), "sipeo")
        spark.conf.set("bogo", "ta")
        self.assertEqual(spark.conf.get("bogo"), "ta")
        self.assertEqual(spark.conf.get("bogo", "not.read"), "ta")
        self.assertEqual(spark.conf.get("not.set", "ta"), "ta")
        self.assertRaisesRegexp(Exception, "not.set", lambda: spark.conf.get("not.set"))
        spark.conf.unset("bogo")
        self.assertEqual(spark.conf.get("bogo", "colombia"), "colombia")

    def test_current_database(self):
        spark = self.spark
        spark.catalog._reset()
        self.assertEquals(spark.catalog.currentDatabase(), "default")
        spark.sql("CREATE DATABASE some_db")
        spark.catalog.setCurrentDatabase("some_db")
        self.assertEquals(spark.catalog.currentDatabase(), "some_db")
        self.assertRaisesRegexp(
            AnalysisException,
            "does_not_exist",
            lambda: spark.catalog.setCurrentDatabase("does_not_exist"))

    def test_list_databases(self):
        spark = self.spark
        spark.catalog._reset()
        databases = [db.name for db in spark.catalog.listDatabases()]
        self.assertEquals(databases, ["default"])
        spark.sql("CREATE DATABASE some_db")
        databases = [db.name for db in spark.catalog.listDatabases()]
        self.assertEquals(sorted(databases), ["default", "some_db"])

    def test_list_tables(self):
        from pyspark.sql.catalog import Table
        spark = self.spark
        spark.catalog._reset()
        spark.sql("CREATE DATABASE some_db")
        self.assertEquals(spark.catalog.listTables(), [])
        self.assertEquals(spark.catalog.listTables("some_db"), [])
        spark.createDataFrame([(1, 1)]).createOrReplaceTempView("temp_tab")
        spark.sql("CREATE TABLE tab1 (name STRING, age INT)")
        spark.sql("CREATE TABLE some_db.tab2 (name STRING, age INT)")
        tables = sorted(spark.catalog.listTables(), key=lambda t: t.name)
        tablesDefault = sorted(spark.catalog.listTables("default"), key=lambda t: t.name)
        tablesSomeDb = sorted(spark.catalog.listTables("some_db"), key=lambda t: t.name)
        self.assertEquals(tables, tablesDefault)
        self.assertEquals(len(tables), 2)
        self.assertEquals(len(tablesSomeDb), 2)
        self.assertEquals(tables[0], Table(
            name="tab1",
            database="default",
            description=None,
            tableType="MANAGED",
            isTemporary=False))
        self.assertEquals(tables[1], Table(
            name="temp_tab",
            database=None,
            description=None,
            tableType="TEMPORARY",
            isTemporary=True))
        self.assertEquals(tablesSomeDb[0], Table(
            name="tab2",
            database="some_db",
            description=None,
            tableType="MANAGED",
            isTemporary=False))
        self.assertEquals(tablesSomeDb[1], Table(
            name="temp_tab",
            database=None,
            description=None,
            tableType="TEMPORARY",
            isTemporary=True))
        self.assertRaisesRegexp(
            AnalysisException,
            "does_not_exist",
            lambda: spark.catalog.listTables("does_not_exist"))

    def test_list_functions(self):
        from pyspark.sql.catalog import Function
        spark = self.spark
        spark.catalog._reset()
        spark.sql("CREATE DATABASE some_db")
        functions = dict((f.name, f) for f in spark.catalog.listFunctions())
        functionsDefault = dict((f.name, f) for f in spark.catalog.listFunctions("default"))
        self.assertTrue(len(functions) > 200)
        self.assertTrue("+" in functions)
        self.assertTrue("like" in functions)
        self.assertTrue("month" in functions)
        self.assertTrue("to_unix_timestamp" in functions)
        self.assertTrue("current_database" in functions)
        self.assertEquals(functions["+"], Function(
            name="+",
            description=None,
            className="org.apache.spark.sql.catalyst.expressions.Add",
            isTemporary=True))
        self.assertEquals(functions, functionsDefault)
        spark.catalog.registerFunction("temp_func", lambda x: str(x))
        spark.sql("CREATE FUNCTION func1 AS 'org.apache.spark.data.bricks'")
        spark.sql("CREATE FUNCTION some_db.func2 AS 'org.apache.spark.data.bricks'")
        newFunctions = dict((f.name, f) for f in spark.catalog.listFunctions())
        newFunctionsSomeDb = dict((f.name, f) for f in spark.catalog.listFunctions("some_db"))
        self.assertTrue(set(functions).issubset(set(newFunctions)))
        self.assertTrue(set(functions).issubset(set(newFunctionsSomeDb)))
        self.assertTrue("temp_func" in newFunctions)
        self.assertTrue("func1" in newFunctions)
        self.assertTrue("func2" not in newFunctions)
        self.assertTrue("temp_func" in newFunctionsSomeDb)
        self.assertTrue("func1" not in newFunctionsSomeDb)
        self.assertTrue("func2" in newFunctionsSomeDb)
        self.assertRaisesRegexp(
            AnalysisException,
            "does_not_exist",
            lambda: spark.catalog.listFunctions("does_not_exist"))

    def test_list_columns(self):
        from pyspark.sql.catalog import Column
        spark = self.spark
        spark.catalog._reset()
        spark.sql("CREATE DATABASE some_db")
        spark.sql("CREATE TABLE tab1 (name STRING, age INT)")
        spark.sql("CREATE TABLE some_db.tab2 (nickname STRING, tolerance FLOAT)")
        columns = sorted(spark.catalog.listColumns("tab1"), key=lambda c: c.name)
        columnsDefault = sorted(spark.catalog.listColumns("tab1", "default"), key=lambda c: c.name)
        self.assertEquals(columns, columnsDefault)
        self.assertEquals(len(columns), 2)
        self.assertEquals(columns[0], Column(
            name="age",
            description=None,
            dataType="int",
            nullable=True,
            isPartition=False,
            isBucket=False))
        self.assertEquals(columns[1], Column(
            name="name",
            description=None,
            dataType="string",
            nullable=True,
            isPartition=False,
            isBucket=False))
        columns2 = sorted(spark.catalog.listColumns("tab2", "some_db"), key=lambda c: c.name)
        self.assertEquals(len(columns2), 2)
        self.assertEquals(columns2[0], Column(
            name="nickname",
            description=None,
            dataType="string",
            nullable=True,
            isPartition=False,
            isBucket=False))
        self.assertEquals(columns2[1], Column(
            name="tolerance",
            description=None,
            dataType="float",
            nullable=True,
            isPartition=False,
            isBucket=False))
        self.assertRaisesRegexp(
            AnalysisException,
            "tab2",
            lambda: spark.catalog.listColumns("tab2"))
        self.assertRaisesRegexp(
            AnalysisException,
            "does_not_exist",
            lambda: spark.catalog.listColumns("does_not_exist"))

    def test_cache(self):
        spark = self.spark
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
        self.assertRaisesRegexp(
            AnalysisException,
            "does_not_exist",
            lambda: spark.catalog.isCached("does_not_exist"))
        self.assertRaisesRegexp(
            AnalysisException,
            "does_not_exist",
            lambda: spark.catalog.cacheTable("does_not_exist"))
        self.assertRaisesRegexp(
            AnalysisException,
            "does_not_exist",
            lambda: spark.catalog.uncacheTable("does_not_exist"))

    def test_BinaryType_serialization(self):
        # Pyrolite version <= 4.9 could not serialize BinaryType with Python3 SPARK-17808
        schema = StructType([StructField('mybytes', BinaryType())])
        data = [[bytearray(b'here is my data')],
                [bytearray(b'and here is some more')],]
        df = self.spark.createDataFrame(data, schema=schema)
        df.collect()


class HiveSparkSubmitTests(SparkSubmitTests):

    def test_hivecontext(self):
        # This test checks that HiveContext is using Hive metastore (SPARK-16224).
        # It sets a metastore url and checks if there is a derby dir created by
        # Hive metastore. If this derby dir exists, HiveContext is using
        # Hive metastore.
        metastore_path = os.path.join(tempfile.mkdtemp(), "spark16224_metastore_db")
        metastore_URL = "jdbc:derby:;databaseName=" + metastore_path + ";create=true"
        hive_site_dir = os.path.join(self.programDir, "conf")
        hive_site_file = self.createTempFile("hive-site.xml", ("""
            |<configuration>
            |  <property>
            |  <name>javax.jdo.option.ConnectionURL</name>
            |  <value>%s</value>
            |  </property>
            |</configuration>
            """ % metastore_URL).lstrip(), "conf")
        script = self.createTempFile("test.py", """
            |import os
            |
            |from pyspark.conf import SparkConf
            |from pyspark.context import SparkContext
            |from pyspark.sql import HiveContext
            |
            |conf = SparkConf()
            |sc = SparkContext(conf=conf)
            |hive_context = HiveContext(sc)
            |print(hive_context.sql("show databases").collect())
            """)
        proc = subprocess.Popen(
            [self.sparkSubmit, "--master", "local-cluster[1,1,1024]",
             "--driver-class-path", hive_site_dir, script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("default", out.decode('utf-8'))
        self.assertTrue(os.path.exists(metastore_path))


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
        cls.spark = HiveContext._createForTesting(cls.sc)
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
        actual = self.spark.createExternalTable("externalJsonTable", tmpPath, "json")
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE externalJsonTable")

        df.write.saveAsTable("savedJsonTable", "json", "overwrite", path=tmpPath)
        schema = StructType([StructField("value", StringType(), True)])
        actual = self.spark.createExternalTable("externalJsonTable", source="json",
                                                schema=schema, path=tmpPath,
                                                noUse="this options will not be used")
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.select("value").collect()),
                         sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE savedJsonTable")
        self.spark.sql("DROP TABLE externalJsonTable")

        defaultDataSourceName = self.spark.getConf("spark.sql.sources.default",
                                                   "org.apache.spark.sql.parquet")
        self.spark.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        df.write.saveAsTable("savedJsonTable", path=tmpPath, mode="overwrite")
        actual = self.spark.createExternalTable("externalJsonTable", path=tmpPath)
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE savedJsonTable")
        self.spark.sql("DROP TABLE externalJsonTable")
        self.spark.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

    def test_window_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.partitionBy("value").orderBy("key")
        from pyspark.sql import functions as F
        sel = df.select(df.value, df.key,
                        F.max("key").over(w.rowsBetween(0, 1)),
                        F.min("key").over(w.rowsBetween(0, 1)),
                        F.count("key").over(w.rowsBetween(float('-inf'), float('inf'))),
                        F.row_number().over(w),
                        F.rank().over(w),
                        F.dense_rank().over(w),
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
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.orderBy("key", df.value)
        from pyspark.sql import functions as F
        sel = df.select(df.value, df.key,
                        F.max("key").over(w.rowsBetween(0, 1)),
                        F.min("key").over(w.rowsBetween(0, 1)),
                        F.count("key").over(w.rowsBetween(float('-inf'), float('inf'))),
                        F.row_number().over(w),
                        F.rank().over(w),
                        F.dense_rank().over(w),
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

    def test_collect_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        from pyspark.sql import functions

        self.assertEqual(
            sorted(df.select(functions.collect_set(df.key).alias('r')).collect()[0].r),
            [1, 2])
        self.assertEqual(
            sorted(df.select(functions.collect_list(df.key).alias('r')).collect()[0].r),
            [1, 1, 1, 2])
        self.assertEqual(
            sorted(df.select(functions.collect_set(df.value).alias('r')).collect()[0].r),
            ["1", "2"])
        self.assertEqual(
            sorted(df.select(functions.collect_list(df.value).alias('r')).collect()[0].r),
            ["1", "2", "2", "2"])

    def test_limit_and_take(self):
        df = self.spark.range(1, 1000, numPartitions=10)

        def assert_runs_only_one_job_stage_and_task(job_group_name, f):
            tracker = self.sc.statusTracker()
            self.sc.setJobGroup(job_group_name, description="")
            f()
            jobs = tracker.getJobIdsForGroup(job_group_name)
            self.assertEqual(1, len(jobs))
            stages = tracker.getJobInfo(jobs[0]).stageIds
            self.assertEqual(1, len(stages))
            self.assertEqual(1, tracker.getStageInfo(stages[0]).numTasks)

        # Regression test for SPARK-10731: take should delegate to Scala implementation
        assert_runs_only_one_job_stage_and_task("take", lambda: df.take(1))
        # Regression test for SPARK-17514: limit(n).collect() should the perform same as take(n)
        assert_runs_only_one_job_stage_and_task("collect_limit", lambda: df.limit(1).collect())


if __name__ == "__main__":
    from pyspark.sql.tests import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'))
    else:
        unittest.main()
