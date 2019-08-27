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
import threading
import pickle
import functools
import time
import datetime
import array
import ctypes
import warnings
import py4j
from contextlib import contextmanager

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

from pyspark.util import _exception_message

_pandas_requirement_message = None
try:
    from pyspark.sql.utils import require_minimum_pandas_version
    require_minimum_pandas_version()
except ImportError as e:
    # If Pandas version requirement is not satisfied, skip related tests.
    _pandas_requirement_message = _exception_message(e)

_pyarrow_requirement_message = None
try:
    from pyspark.sql.utils import require_minimum_pyarrow_version
    require_minimum_pyarrow_version()
except ImportError as e:
    # If Arrow version requirement is not satisfied, skip related tests.
    _pyarrow_requirement_message = _exception_message(e)

_test_not_compiled_message = None
try:
    from pyspark.sql.utils import require_test_compiled
    require_test_compiled()
except Exception as e:
    _test_not_compiled_message = _exception_message(e)

_have_pandas = _pandas_requirement_message is None
_have_pyarrow = _pyarrow_requirement_message is None
_test_compiled = _test_not_compiled_message is None

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Column, Row
from pyspark.sql.types import *
from pyspark.sql.types import UserDefinedType, _infer_type, _make_type_verifier
from pyspark.sql.types import _array_signed_int_typecode_ctype_mappings, _array_type_mappings
from pyspark.sql.types import _array_unsigned_int_typecode_ctype_mappings
from pyspark.sql.types import _merge_type
from pyspark.tests import QuietTest, ReusedPySparkTestCase, PySparkTestCase, SparkSubmitTests
from pyspark.sql.functions import UserDefinedFunction, sha2, lit, input_file_name
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


class SQLTestUtils(object):
    """
    This util assumes the instance of this to have 'spark' attribute, having a spark session.
    It is usually used with 'ReusedSQLTestCase' class but can be used if you feel sure the
    the implementation of this class has 'spark' attribute.
    """

    @contextmanager
    def sql_conf(self, pairs):
        """
        A convenient context manager to test some configuration specific logic. This sets
        `value` to the configuration `key` and then restores it back when it exits.
        """
        assert isinstance(pairs, dict), "pairs should be a dictionary."
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        keys = pairs.keys()
        new_values = pairs.values()
        old_values = [self.spark.conf.get(key, None) for key in keys]
        for key, new_value in zip(keys, new_values):
            self.spark.conf.set(key, new_value)
        try:
            yield
        finally:
            for key, old_value in zip(keys, old_values):
                if old_value is None:
                    self.spark.conf.unset(key)
                else:
                    self.spark.conf.set(key, old_value)


class ReusedSQLTestCase(ReusedPySparkTestCase, SQLTestUtils):
    @classmethod
    def setUpClass(cls):
        super(ReusedSQLTestCase, cls).setUpClass()
        cls.spark = SparkSession(cls.sc)

    @classmethod
    def tearDownClass(cls):
        super(ReusedSQLTestCase, cls).tearDownClass()
        cls.spark.stop()

    def assertPandasEqual(self, expected, result):
        msg = ("DataFrames are not equal: " +
               "\n\nExpected:\n%s\n%s" % (expected, expected.dtypes) +
               "\n\nResult:\n%s\n%s" % (result, result.dtypes))
        self.assertTrue(expected.equals(result), msg=msg)


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

    def test_struct_field_type_name(self):
        struct_field = StructField("a", IntegerType())
        self.assertRaises(TypeError, struct_field.typeName)

    def test_invalid_create_row(self):
        row_class = Row("c1", "c2")
        self.assertRaises(ValueError, lambda: row_class(1, 2, 3))


class SQLTests(ReusedSQLTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.spark.createDataFrame(cls.testData)

    @classmethod
    def tearDownClass(cls):
        ReusedSQLTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def test_sqlcontext_reuses_sparksession(self):
        sqlContext1 = SQLContext(self.sc)
        sqlContext2 = SQLContext(self.sc)
        self.assertTrue(sqlContext1.sparkSession is sqlContext2.sparkSession)

    def tearDown(self):
        super(SQLTests, self).tearDown()

        # tear down test_bucketed_write state
        self.spark.sql("DROP TABLE IF EXISTS pyspark_bucket")

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
        from pyspark.sql.functions import explode, explode_outer, posexplode_outer
        d = [
            Row(a=1, intlist=[1, 2, 3], mapfield={"a": "b"}),
            Row(a=1, intlist=[], mapfield={}),
            Row(a=1, intlist=None, mapfield=None),
        ]
        rdd = self.sc.parallelize(d)
        data = self.spark.createDataFrame(rdd)

        result = data.select(explode(data.intlist).alias("a")).select("a").collect()
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[1][0], 2)
        self.assertEqual(result[2][0], 3)

        result = data.select(explode(data.mapfield).alias("a", "b")).select("a", "b").collect()
        self.assertEqual(result[0][0], "a")
        self.assertEqual(result[0][1], "b")

        result = [tuple(x) for x in data.select(posexplode_outer("intlist")).collect()]
        self.assertEqual(result, [(0, 1), (1, 2), (2, 3), (None, None), (None, None)])

        result = [tuple(x) for x in data.select(posexplode_outer("mapfield")).collect()]
        self.assertEqual(result, [(0, 'a', 'b'), (None, None, None), (None, None, None)])

        result = [x[0] for x in data.select(explode_outer("intlist")).collect()]
        self.assertEqual(result, [1, 2, 3, None, None])

        result = [tuple(x) for x in data.select(explode_outer("mapfield")).collect()]
        self.assertEqual(result, [('a', 'b'), (None, None), (None, None)])

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

        # This is to check if a deprecated 'SQLContext.registerFunction' can call its alias.
        sqlContext = self.spark._wrapped
        sqlContext.registerFunction("oneArg", lambda x: len(x), IntegerType())
        [row] = sqlContext.sql("SELECT oneArg('test')").collect()
        self.assertEqual(row[0], 4)

    def test_udf2(self):
        self.spark.catalog.registerFunction("strlen", lambda string: len(string), IntegerType())
        self.spark.createDataFrame(self.sc.parallelize([Row(a="test")]))\
            .createOrReplaceTempView("test")
        [res] = self.spark.sql("SELECT strlen(a) FROM test WHERE strlen(a) > 1").collect()
        self.assertEqual(4, res[0])

    def test_udf3(self):
        two_args = self.spark.catalog.registerFunction(
            "twoArgs", UserDefinedFunction(lambda x, y: len(x) + y))
        self.assertEqual(two_args.deterministic, True)
        [row] = self.spark.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], u'5')

    def test_udf_registration_return_type_none(self):
        two_args = self.spark.catalog.registerFunction(
            "twoArgs", UserDefinedFunction(lambda x, y: len(x) + y, "integer"), None)
        self.assertEqual(two_args.deterministic, True)
        [row] = self.spark.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], 5)

    def test_udf_registration_return_type_not_none(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(TypeError, "Invalid returnType"):
                self.spark.catalog.registerFunction(
                    "f", UserDefinedFunction(lambda x, y: len(x) + y, StringType()), StringType())

    def test_nondeterministic_udf(self):
        # Test that nondeterministic UDFs are evaluated only once in chained UDF evaluations
        from pyspark.sql.functions import udf
        import random
        udf_random_col = udf(lambda: int(100 * random.random()), IntegerType()).asNondeterministic()
        self.assertEqual(udf_random_col.deterministic, False)
        df = self.spark.createDataFrame([Row(1)]).select(udf_random_col().alias('RAND'))
        udf_add_ten = udf(lambda rand: rand + 10, IntegerType())
        [row] = df.withColumn('RAND_PLUS_TEN', udf_add_ten('RAND')).collect()
        self.assertEqual(row[0] + 10, row[1])

    def test_nondeterministic_udf2(self):
        import random
        from pyspark.sql.functions import udf
        random_udf = udf(lambda: random.randint(6, 6), IntegerType()).asNondeterministic()
        self.assertEqual(random_udf.deterministic, False)
        random_udf1 = self.spark.catalog.registerFunction("randInt", random_udf)
        self.assertEqual(random_udf1.deterministic, False)
        [row] = self.spark.sql("SELECT randInt()").collect()
        self.assertEqual(row[0], 6)
        [row] = self.spark.range(1).select(random_udf1()).collect()
        self.assertEqual(row[0], 6)
        [row] = self.spark.range(1).select(random_udf()).collect()
        self.assertEqual(row[0], 6)
        # render_doc() reproduces the help() exception without printing output
        pydoc.render_doc(udf(lambda: random.randint(6, 6), IntegerType()))
        pydoc.render_doc(random_udf)
        pydoc.render_doc(random_udf1)
        pydoc.render_doc(udf(lambda x: x).asNondeterministic)

    def test_nondeterministic_udf3(self):
        # regression test for SPARK-23233
        from pyspark.sql.functions import udf
        f = udf(lambda x: x)
        # Here we cache the JVM UDF instance.
        self.spark.range(1).select(f("id"))
        # This should reset the cache to set the deterministic status correctly.
        f = f.asNondeterministic()
        # Check the deterministic status of udf.
        df = self.spark.range(1).select(f("id"))
        deterministic = df._jdf.logicalPlan().projectList().head().deterministic()
        self.assertFalse(deterministic)

    def test_nondeterministic_udf_in_aggregate(self):
        from pyspark.sql.functions import udf, sum
        import random
        udf_random_col = udf(lambda: int(100 * random.random()), 'int').asNondeterministic()
        df = self.spark.range(10)

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(AnalysisException, "nondeterministic"):
                df.groupby('id').agg(sum(udf_random_col())).collect()
            with self.assertRaisesRegexp(AnalysisException, "nondeterministic"):
                df.agg(sum(udf_random_col())).collect()

    def test_chained_udf(self):
        self.spark.catalog.registerFunction("double", lambda x: x + x, IntegerType())
        [row] = self.spark.sql("SELECT double(1)").collect()
        self.assertEqual(row[0], 2)
        [row] = self.spark.sql("SELECT double(double(1))").collect()
        self.assertEqual(row[0], 4)
        [row] = self.spark.sql("SELECT double(double(1) + 1)").collect()
        self.assertEqual(row[0], 6)

    def test_single_udf_with_repeated_argument(self):
        # regression test for SPARK-20685
        self.spark.catalog.registerFunction("add", lambda x, y: x + y, IntegerType())
        row = self.spark.sql("SELECT add(1, 1)").first()
        self.assertEqual(tuple(row), (2, ))

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

    def test_udf_in_filter_on_top_of_join(self):
        # regression test for SPARK-18589
        from pyspark.sql.functions import udf
        left = self.spark.createDataFrame([Row(a=1)])
        right = self.spark.createDataFrame([Row(b=1)])
        f = udf(lambda a, b: a == b, BooleanType())
        df = left.crossJoin(right).filter(f("a", "b"))
        self.assertEqual(df.collect(), [Row(a=1, b=1)])

    def test_udf_in_join_condition(self):
        # regression test for SPARK-25314
        from pyspark.sql.functions import udf
        left = self.spark.createDataFrame([Row(a=1)])
        right = self.spark.createDataFrame([Row(b=1)])
        f = udf(lambda a, b: a == b, BooleanType())
        df = left.join(right, f("a", "b"))
        with self.assertRaisesRegexp(AnalysisException, 'Detected implicit cartesian product'):
            df.collect()
        with self.sql_conf({"spark.sql.crossJoin.enabled": True}):
            self.assertEqual(df.collect(), [Row(a=1, b=1)])

    def test_udf_in_left_outer_join_condition(self):
        # regression test for SPARK-26147
        from pyspark.sql.functions import udf, col
        left = self.spark.createDataFrame([Row(a=1)])
        right = self.spark.createDataFrame([Row(b=1)])
        f = udf(lambda a: str(a), StringType())
        # The join condition can't be pushed down, as it refers to attributes from both sides.
        # The Python UDF only refer to attributes from one side, so it's evaluable.
        df = left.join(right, f("a") == col("b").cast("string"), how="left_outer")
        with self.sql_conf({"spark.sql.crossJoin.enabled": True}):
            self.assertEqual(df.collect(), [Row(a=1, b=1)])

    def test_udf_and_common_filter_in_join_condition(self):
        # regression test for SPARK-25314
        # test the complex scenario with both udf and common filter
        from pyspark.sql.functions import udf
        left = self.spark.createDataFrame([Row(a=1, a1=1, a2=1), Row(a=2, a1=2, a2=2)])
        right = self.spark.createDataFrame([Row(b=1, b1=1, b2=1), Row(b=1, b1=3, b2=1)])
        f = udf(lambda a, b: a == b, BooleanType())
        df = left.join(right, [f("a", "b"), left.a1 == right.b1])
        # do not need spark.sql.crossJoin.enabled=true for udf is not the only join condition.
        self.assertEqual(df.collect(), [Row(a=1, a1=1, a2=1, b=1, b1=1, b2=1)])

    def test_udf_not_supported_in_join_condition(self):
        # regression test for SPARK-25314
        # test python udf is not supported in join type except inner join.
        from pyspark.sql.functions import udf
        left = self.spark.createDataFrame([Row(a=1, a1=1, a2=1), Row(a=2, a1=2, a2=2)])
        right = self.spark.createDataFrame([Row(b=1, b1=1, b2=1), Row(b=1, b1=3, b2=1)])
        f = udf(lambda a, b: a == b, BooleanType())

        def runWithJoinType(join_type, type_string):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    'Using PythonUDF.*%s is not supported.' % type_string):
                left.join(right, [f("a", "b"), left.a1 == right.b1], join_type).collect()
        runWithJoinType("full", "FullOuter")
        runWithJoinType("left", "LeftOuter")
        runWithJoinType("right", "RightOuter")
        runWithJoinType("leftanti", "LeftAnti")
        runWithJoinType("leftsemi", "LeftSemi")

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

    def test_udf_with_filter_function(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        from pyspark.sql.functions import udf, col
        from pyspark.sql.types import BooleanType

        my_filter = udf(lambda a: a < 2, BooleanType())
        sel = df.select(col("key"), col("value")).filter((my_filter(col("key"))) & (df.value < "2"))
        self.assertEqual(sel.collect(), [Row(key=1, value='1')])

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

        df = self.spark.range(3)
        res = df.select("id", explode(f(df.id))).collect()
        self.assertEqual(res[0][0], 1)
        self.assertEqual(res[0][1], 0)
        self.assertEqual(res[1][0], 2)
        self.assertEqual(res[1][1], 0)
        self.assertEqual(res[2][0], 2)
        self.assertEqual(res[2][1], 1)

        range_udf = udf(lambda value: list(range(value - 1, value + 1)), ArrayType(IntegerType()))
        res = df.select("id", explode(range_udf(df.id))).collect()
        self.assertEqual(res[0][0], 0)
        self.assertEqual(res[0][1], -1)
        self.assertEqual(res[1][0], 0)
        self.assertEqual(res[1][1], 0)
        self.assertEqual(res[2][0], 1)
        self.assertEqual(res[2][1], 0)
        self.assertEqual(res[3][0], 1)
        self.assertEqual(res[3][1], 1)

    def test_udf_with_order_by_and_limit(self):
        from pyspark.sql.functions import udf
        my_copy = udf(lambda x: x, IntegerType())
        df = self.spark.range(10).orderBy("id")
        res = df.select(df.id, my_copy(df.id).alias("copy")).limit(1)
        res.explain(True)
        self.assertEqual(res.collect(), [Row(id=0, copy=0)])

    def test_udf_registration_returns_udf(self):
        df = self.spark.range(10)
        add_three = self.spark.udf.register("add_three", lambda x: x + 3, IntegerType())

        self.assertListEqual(
            df.selectExpr("add_three(id) AS plus_three").collect(),
            df.select(add_three("id").alias("plus_three")).collect()
        )

        # This is to check if a 'SQLContext.udf' can call its alias.
        sqlContext = self.spark._wrapped
        add_four = sqlContext.udf.register("add_four", lambda x: x + 4, IntegerType())

        self.assertListEqual(
            df.selectExpr("add_four(id) AS plus_four").collect(),
            df.select(add_four("id").alias("plus_four")).collect()
        )

    def test_non_existed_udf(self):
        spark = self.spark
        self.assertRaisesRegexp(AnalysisException, "Can not load class non_existed_udf",
                                lambda: spark.udf.registerJavaFunction("udf1", "non_existed_udf"))

        # This is to check if a deprecated 'SQLContext.registerJavaFunction' can call its alias.
        sqlContext = spark._wrapped
        self.assertRaisesRegexp(AnalysisException, "Can not load class non_existed_udf",
                                lambda: sqlContext.registerJavaFunction("udf1", "non_existed_udf"))

    def test_non_existed_udaf(self):
        spark = self.spark
        self.assertRaisesRegexp(AnalysisException, "Can not load class non_existed_udaf",
                                lambda: spark.udf.registerJavaUDAF("udaf1", "non_existed_udaf"))

    def test_linesep_text(self):
        df = self.spark.read.text("python/test_support/sql/ages_newlines.csv", lineSep=",")
        expected = [Row(value=u'Joe'), Row(value=u'20'), Row(value=u'"Hi'),
                    Row(value=u'\nI am Jeo"\nTom'), Row(value=u'30'),
                    Row(value=u'"My name is Tom"\nHyukjin'), Row(value=u'25'),
                    Row(value=u'"I am Hyukjin\n\nI love Spark!"\n')]
        self.assertEqual(df.collect(), expected)

        tpath = tempfile.mkdtemp()
        shutil.rmtree(tpath)
        try:
            df.write.text(tpath, lineSep="!")
            expected = [Row(value=u'Joe!20!"Hi!'), Row(value=u'I am Jeo"'),
                        Row(value=u'Tom!30!"My name is Tom"'),
                        Row(value=u'Hyukjin!25!"I am Hyukjin'),
                        Row(value=u''), Row(value=u'I love Spark!"'),
                        Row(value=u'!')]
            readback = self.spark.read.text(tpath)
            self.assertEqual(readback.collect(), expected)
        finally:
            shutil.rmtree(tpath)

    def test_multiline_json(self):
        people1 = self.spark.read.json("python/test_support/sql/people.json")
        people_array = self.spark.read.json("python/test_support/sql/people_array.json",
                                            multiLine=True)
        self.assertEqual(people1.collect(), people_array.collect())

    def test_encoding_json(self):
        people_array = self.spark.read\
            .json("python/test_support/sql/people_array_utf16le.json",
                  multiLine=True, encoding="UTF-16LE")
        expected = [Row(age=30, name=u'Andy'), Row(age=19, name=u'Justin')]
        self.assertEqual(people_array.collect(), expected)

    def test_linesep_json(self):
        df = self.spark.read.json("python/test_support/sql/people.json", lineSep=",")
        expected = [Row(_corrupt_record=None, name=u'Michael'),
                    Row(_corrupt_record=u' "age":30}\n{"name":"Justin"', name=None),
                    Row(_corrupt_record=u' "age":19}\n', name=None)]
        self.assertEqual(df.collect(), expected)

        tpath = tempfile.mkdtemp()
        shutil.rmtree(tpath)
        try:
            df = self.spark.read.json("python/test_support/sql/people.json")
            df.write.json(tpath, lineSep="!!")
            readback = self.spark.read.json(tpath, lineSep="!!")
            self.assertEqual(readback.collect(), df.collect())
        finally:
            shutil.rmtree(tpath)

    def test_multiline_csv(self):
        ages_newlines = self.spark.read.csv(
            "python/test_support/sql/ages_newlines.csv", multiLine=True)
        expected = [Row(_c0=u'Joe', _c1=u'20', _c2=u'Hi,\nI am Jeo'),
                    Row(_c0=u'Tom', _c1=u'30', _c2=u'My name is Tom'),
                    Row(_c0=u'Hyukjin', _c1=u'25', _c2=u'I am Hyukjin\n\nI love Spark!')]
        self.assertEqual(ages_newlines.collect(), expected)

    def test_ignorewhitespace_csv(self):
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.spark.createDataFrame([[" a", "b  ", " c "]]).write.csv(
            tmpPath,
            ignoreLeadingWhiteSpace=False,
            ignoreTrailingWhiteSpace=False)

        expected = [Row(value=u' a,b  , c ')]
        readback = self.spark.read.text(tmpPath)
        self.assertEqual(readback.collect(), expected)
        shutil.rmtree(tmpPath)

    def test_read_multiple_orc_file(self):
        df = self.spark.read.orc(["python/test_support/sql/orc_partitioned/b=0/c=0",
                                  "python/test_support/sql/orc_partitioned/b=1/c=1"])
        self.assertEqual(2, df.count())

    def test_udf_with_input_file_name(self):
        from pyspark.sql.functions import udf, input_file_name
        sourceFile = udf(lambda path: path, StringType())
        filePath = "python/test_support/sql/people1.json"
        row = self.spark.read.json(filePath).select(sourceFile(input_file_name())).first()
        self.assertTrue(row[0].find("people1.json") != -1)

    def test_udf_with_input_file_name_for_hadooprdd(self):
        from pyspark.sql.functions import udf, input_file_name

        def filename(path):
            return path

        sameText = udf(filename, StringType())

        rdd = self.sc.textFile('python/test_support/sql/people.json')
        df = self.spark.read.json(rdd).select(input_file_name().alias('file'))
        row = df.select(sameText(df['file'])).first()
        self.assertTrue(row[0].find("people.json") != -1)

        rdd2 = self.sc.newAPIHadoopFile(
            'python/test_support/sql/people.json',
            'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
            'org.apache.hadoop.io.LongWritable',
            'org.apache.hadoop.io.Text')

        df2 = self.spark.read.json(rdd2).select(input_file_name().alias('file'))
        row2 = df2.select(sameText(df2['file'])).first()
        self.assertTrue(row2[0].find("people.json") != -1)

    def test_input_file_name_reset_for_rdd(self):
        rdd = self.sc.textFile('python/test_support/hello/hello.txt').map(lambda x: {'data': x})
        df = self.spark.createDataFrame(rdd, "data STRING")
        df.select(input_file_name().alias('file')).collect()

        non_file_df = self.spark.range(100).select(input_file_name())

        results = non_file_df.collect()
        self.assertTrue(len(results) == 100)

        # [SPARK-24605]: if everything was properly reset after the last job, this should return
        # empty string rather than the file read in the last job.
        for result in results:
            self.assertEqual(result[0], '')

    def test_input_file_name_udf(self):
        from pyspark.sql.functions import udf, input_file_name

        df = self.spark.read.text('python/test_support/hello/hello.txt')
        df = df.select(udf(lambda x: x)("value"), input_file_name().alias('file'))
        file_name = df.collect()[0].file
        self.assertTrue("python/test_support/hello/hello.txt" in file_name)

    def test_udf_defers_judf_initialization(self):
        # This is separate of  UDFInitializationTests
        # to avoid context initialization
        # when udf is called

        from pyspark.sql.functions import UserDefinedFunction

        f = UserDefinedFunction(lambda x: x, StringType())

        self.assertIsNone(
            f._judf_placeholder,
            "judf should not be initialized before the first call."
        )

        self.assertIsInstance(f("foo"), Column, "UDF call should return a Column.")

        self.assertIsNotNone(
            f._judf_placeholder,
            "judf should be initialized after UDF has been called."
        )

    def test_udf_with_string_return_type(self):
        from pyspark.sql.functions import UserDefinedFunction

        add_one = UserDefinedFunction(lambda x: x + 1, "integer")
        make_pair = UserDefinedFunction(lambda x: (-x, x), "struct<x:integer,y:integer>")
        make_array = UserDefinedFunction(
            lambda x: [float(x) for x in range(x, x + 3)], "array<double>")

        expected = (2, Row(x=-1, y=1), [1.0, 2.0, 3.0])
        actual = (self.spark.range(1, 2).toDF("x")
                  .select(add_one("x"), make_pair("x"), make_array("x"))
                  .first())

        self.assertTupleEqual(expected, actual)

    def test_udf_shouldnt_accept_noncallable_object(self):
        from pyspark.sql.functions import UserDefinedFunction

        non_callable = None
        self.assertRaises(TypeError, UserDefinedFunction, non_callable, StringType())

    def test_udf_with_decorator(self):
        from pyspark.sql.functions import lit, udf
        from pyspark.sql.types import IntegerType, DoubleType

        @udf(IntegerType())
        def add_one(x):
            if x is not None:
                return x + 1

        @udf(returnType=DoubleType())
        def add_two(x):
            if x is not None:
                return float(x + 2)

        @udf
        def to_upper(x):
            if x is not None:
                return x.upper()

        @udf()
        def to_lower(x):
            if x is not None:
                return x.lower()

        @udf
        def substr(x, start, end):
            if x is not None:
                return x[start:end]

        @udf("long")
        def trunc(x):
            return int(x)

        @udf(returnType="double")
        def as_double(x):
            return float(x)

        df = (
            self.spark
                .createDataFrame(
                    [(1, "Foo", "foobar", 3.0)], ("one", "Foo", "foobar", "float"))
                .select(
                    add_one("one"), add_two("one"),
                    to_upper("Foo"), to_lower("Foo"),
                    substr("foobar", lit(0), lit(3)),
                    trunc("float"), as_double("one")))

        self.assertListEqual(
            [tpe for _, tpe in df.dtypes],
            ["int", "double", "string", "string", "string", "bigint", "double"]
        )

        self.assertListEqual(
            list(df.first()),
            [2, 3.0, "FOO", "foo", "foo", 3, 1.0]
        )

    def test_udf_wrapper(self):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import IntegerType

        def f(x):
            """Identity"""
            return x

        return_type = IntegerType()
        f_ = udf(f, return_type)

        self.assertTrue(f.__doc__ in f_.__doc__)
        self.assertEqual(f, f_.func)
        self.assertEqual(return_type, f_.returnType)

        class F(object):
            """Identity"""
            def __call__(self, x):
                return x

        f = F()
        return_type = IntegerType()
        f_ = udf(f, return_type)

        self.assertTrue(f.__doc__ in f_.__doc__)
        self.assertEqual(f, f_.func)
        self.assertEqual(return_type, f_.returnType)

        f = functools.partial(f, x=1)
        return_type = IntegerType()
        f_ = udf(f, return_type)

        self.assertTrue(f.__doc__ in f_.__doc__)
        self.assertEqual(f, f_.func)
        self.assertEqual(return_type, f_.returnType)

    def test_validate_column_types(self):
        from pyspark.sql.functions import udf, to_json
        from pyspark.sql.column import _to_java_column

        self.assertTrue("Column" in _to_java_column("a").getClass().toString())
        self.assertTrue("Column" in _to_java_column(u"a").getClass().toString())
        self.assertTrue("Column" in _to_java_column(self.spark.range(1).id).getClass().toString())

        self.assertRaisesRegexp(
            TypeError,
            "Invalid argument, not a string or column",
            lambda: _to_java_column(1))

        class A():
            pass

        self.assertRaises(TypeError, lambda: _to_java_column(A()))
        self.assertRaises(TypeError, lambda: _to_java_column([]))

        self.assertRaisesRegexp(
            TypeError,
            "Invalid argument, not a string or column",
            lambda: udf(lambda x: x)(None))
        self.assertRaises(TypeError, lambda: to_json(1))

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

    def test_infer_schema_not_enough_names(self):
        df = self.spark.createDataFrame([["a", "b"]], ["col1"])
        self.assertEqual(df.columns, ['col1', '_2'])

    def test_infer_schema_fails(self):
        with self.assertRaisesRegexp(TypeError, 'field a'):
            self.spark.createDataFrame(self.spark.sparkContext.parallelize([[1, 1], ["x", 1]]),
                                       schema=["a", "b"], samplingRatio=0.99)

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

    def test_create_dataframe_from_dict_respects_schema(self):
        df = self.spark.createDataFrame([{'a': 1}], ["b"])
        self.assertEqual(df.columns, ['b'])

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
        from pyspark.sql.types import _parse_datatype_json_string, _infer_type, _make_type_verifier
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
        _make_type_verifier(ExamplePointUDT())(ExamplePoint(1.0, 2.0))
        self.assertRaises(ValueError, lambda: _make_type_verifier(ExamplePointUDT())([1.0, 2.0]))

        check_datatype(PythonOnlyUDT())
        structtype_with_udt = StructType([StructField("label", DoubleType(), False),
                                          StructField("point", PythonOnlyUDT(), False)])
        check_datatype(structtype_with_udt)
        p = PythonOnlyPoint(1.0, 2.0)
        self.assertEqual(_infer_type(p), PythonOnlyUDT())
        _make_type_verifier(PythonOnlyUDT())(PythonOnlyPoint(1.0, 2.0))
        self.assertRaises(
            ValueError,
            lambda: _make_type_verifier(PythonOnlyUDT())([1.0, 2.0]))

    def test_simple_udt_in_df(self):
        schema = StructType().add("key", LongType()).add("val", PythonOnlyUDT())
        df = self.spark.createDataFrame(
            [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)],
            schema=schema)
        df.collect()

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

    def test_nonparam_udf_with_aggregate(self):
        import pyspark.sql.functions as f

        df = self.spark.createDataFrame([(1, 2), (1, 2)])
        f_udf = f.udf(lambda: "const_str")
        rows = df.distinct().withColumn("a", f_udf()).collect()
        self.assertEqual(rows, [Row(_1=1, _2=2, a=u'const_str')])

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

    def test_cast_to_string_with_udt(self):
        from pyspark.sql.tests import ExamplePointUDT, ExamplePoint
        from pyspark.sql.functions import col
        row = (ExamplePoint(1.0, 2.0), PythonOnlyPoint(3.0, 4.0))
        schema = StructType([StructField("point", ExamplePointUDT(), False),
                             StructField("pypoint", PythonOnlyUDT(), False)])
        df = self.spark.createDataFrame([row], schema)

        result = df.select(col('point').cast('string'), col('pypoint').cast('string')).head()
        self.assertEqual(result, Row(point=u'(1.0, 2.0)', pypoint=u'[3.0, 4.0]'))

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
        css = cs.contains('a'), cs.like('a'), cs.rlike('a'), cs.asc(), cs.desc(),\
            cs.startswith('a'), cs.endswith('a'), ci.eqNullSafe(cs)
        self.assertTrue(all(isinstance(c, Column) for c in css))
        self.assertTrue(isinstance(ci.cast(LongType()), Column))
        self.assertRaisesRegexp(ValueError,
                                "Cannot apply 'in' operator against a column",
                                lambda: 1 in cs)

    def test_column_getitem(self):
        from pyspark.sql.functions import col

        self.assertIsInstance(col("foo")[1:3], Column)
        self.assertIsInstance(col("foo")[0], Column)
        self.assertIsInstance(col("foo")["bar"], Column)
        self.assertRaises(ValueError, lambda: col("foo")[0:10:2])

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
        df = self.sc.parallelize([Row(a=i, b=i+10) for i in range(10)]).toDF()
        for f in ["a", u"a"]:
            aq = df.stat.approxQuantile(f, [0.1, 0.5, 0.9], 0.1)
            self.assertTrue(isinstance(aq, list))
            self.assertEqual(len(aq), 3)
        self.assertTrue(all(isinstance(q, float) for q in aq))
        aqs = df.stat.approxQuantile(["a", u"b"], [0.1, 0.5, 0.9], 0.1)
        self.assertTrue(isinstance(aqs, list))
        self.assertEqual(len(aqs), 2)
        self.assertTrue(isinstance(aqs[0], list))
        self.assertEqual(len(aqs[0]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqs[0]))
        self.assertTrue(isinstance(aqs[1], list))
        self.assertEqual(len(aqs[1]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqs[1]))
        aqt = df.stat.approxQuantile((u"a", "b"), [0.1, 0.5, 0.9], 0.1)
        self.assertTrue(isinstance(aqt, list))
        self.assertEqual(len(aqt), 2)
        self.assertTrue(isinstance(aqt[0], list))
        self.assertEqual(len(aqt[0]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqt[0]))
        self.assertTrue(isinstance(aqt[1], list))
        self.assertEqual(len(aqt[1]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqt[1]))
        self.assertRaises(ValueError, lambda: df.stat.approxQuantile(123, [0.1, 0.9], 0.1))
        self.assertRaises(ValueError, lambda: df.stat.approxQuantile(("a", 123), [0.1, 0.9], 0.1))
        self.assertRaises(ValueError, lambda: df.stat.approxQuantile(["a", 123], [0.1, 0.9], 0.1))

    def test_corr(self):
        import math
        df = self.sc.parallelize([Row(a=i, b=math.sqrt(i)) for i in range(10)]).toDF()
        corr = df.stat.corr(u"a", "b")
        self.assertTrue(abs(corr - 0.95734012) < 1e-6)

    def test_sampleby(self):
        df = self.sc.parallelize([Row(a=i, b=(i % 3)) for i in range(10)]).toDF()
        sampled = df.stat.sampleBy(u"b", fractions={0: 0.5, 1: 0.5}, seed=0)
        self.assertTrue(sampled.count() == 3)

    def test_cov(self):
        df = self.sc.parallelize([Row(a=i, b=2 * i) for i in range(10)]).toDF()
        cov = df.stat.cov(u"a", "b")
        self.assertTrue(abs(cov - 55.0 / 3) < 1e-6)

    def test_crosstab(self):
        df = self.sc.parallelize([Row(a=i % 3, b=i % 2) for i in range(1, 7)]).toDF()
        ct = df.stat.crosstab(u"a", "b").collect()
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

    def test_string_functions(self):
        from pyspark.sql.functions import col, lit
        df = self.spark.createDataFrame([['nick']], schema=['name'])
        self.assertRaisesRegexp(
            TypeError,
            "must be the same type",
            lambda: df.select(col('name').substr(0, lit(1))))
        if sys.version_info.major == 2:
            self.assertRaises(
                TypeError,
                lambda: df.select(col('name').substr(long(0), long(1))))

    def test_array_contains_function(self):
        from pyspark.sql.functions import array_contains

        df = self.spark.createDataFrame([(["1", "2", "3"],), ([],)], ['data'])
        actual = df.select(array_contains(df.data, "1").alias('b')).collect()
        self.assertEqual([Row(b=True), Row(b=False)], actual)

    def test_between_function(self):
        df = self.sc.parallelize([
            Row(a=1, b=2, c=3),
            Row(a=2, b=1, c=3),
            Row(a=4, b=1, c=4)]).toDF()
        self.assertEqual([Row(a=2, b=1, c=3), Row(a=4, b=1, c=4)],
                         df.filter(df.a.between(df.b, df.c)).collect())

    def test_struct_type(self):
        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        struct2 = StructType([StructField("f1", StringType(), True),
                              StructField("f2", StringType(), True, None)])
        self.assertEqual(struct1.fieldNames(), struct2.names)
        self.assertEqual(struct1, struct2)

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        struct2 = StructType([StructField("f1", StringType(), True)])
        self.assertNotEqual(struct1.fieldNames(), struct2.names)
        self.assertNotEqual(struct1, struct2)

        struct1 = (StructType().add(StructField("f1", StringType(), True))
                   .add(StructField("f2", StringType(), True, None)))
        struct2 = StructType([StructField("f1", StringType(), True),
                              StructField("f2", StringType(), True, None)])
        self.assertEqual(struct1.fieldNames(), struct2.names)
        self.assertEqual(struct1, struct2)

        struct1 = (StructType().add(StructField("f1", StringType(), True))
                   .add(StructField("f2", StringType(), True, None)))
        struct2 = StructType([StructField("f1", StringType(), True)])
        self.assertNotEqual(struct1.fieldNames(), struct2.names)
        self.assertNotEqual(struct1, struct2)

        # Catch exception raised during improper construction
        self.assertRaises(ValueError, lambda: StructType().add("name"))

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        for field in struct1:
            self.assertIsInstance(field, StructField)

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        self.assertEqual(len(struct1), 2)

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        self.assertIs(struct1["f1"], struct1.fields[0])
        self.assertIs(struct1[0], struct1.fields[0])
        self.assertEqual(struct1[0:1], StructType(struct1.fields[0:1]))
        self.assertRaises(KeyError, lambda: struct1["f9"])
        self.assertRaises(IndexError, lambda: struct1[9])
        self.assertRaises(TypeError, lambda: struct1[9.9])

    def test_parse_datatype_string(self):
        from pyspark.sql.types import _all_atomic_types, _parse_datatype_string
        for k, t in _all_atomic_types.items():
            if t != NullType:
                self.assertEqual(t(), _parse_datatype_string(k))
        self.assertEqual(IntegerType(), _parse_datatype_string("int"))
        self.assertEqual(DecimalType(1, 1), _parse_datatype_string("decimal(1  ,1)"))
        self.assertEqual(DecimalType(10, 1), _parse_datatype_string("decimal( 10,1 )"))
        self.assertEqual(DecimalType(11, 1), _parse_datatype_string("decimal(11,1)"))
        self.assertEqual(
            ArrayType(IntegerType()),
            _parse_datatype_string("array<int >"))
        self.assertEqual(
            MapType(IntegerType(), DoubleType()),
            _parse_datatype_string("map< int, double  >"))
        self.assertEqual(
            StructType([StructField("a", IntegerType()), StructField("c", DoubleType())]),
            _parse_datatype_string("struct<a:int, c:double >"))
        self.assertEqual(
            StructType([StructField("a", IntegerType()), StructField("c", DoubleType())]),
            _parse_datatype_string("a:int, c:double"))
        self.assertEqual(
            StructType([StructField("a", IntegerType()), StructField("c", DoubleType())]),
            _parse_datatype_string("a INT, c DOUBLE"))

    def test_metadata_null(self):
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

    def test_stream_trigger(self):
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')

        # Should take at least one arg
        try:
            df.writeStream.trigger()
        except ValueError:
            pass

        # Should not take multiple args
        try:
            df.writeStream.trigger(once=True, processingTime='5 seconds')
        except ValueError:
            pass

        # Should not take multiple args
        try:
            df.writeStream.trigger(processingTime='5 seconds', continuous='1 second')
        except ValueError:
            pass

        # Should take only keyword args
        try:
            df.writeStream.trigger('5 seconds')
            self.fail("Should have thrown an exception")
        except TypeError:
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
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming') \
            .withColumn('id', lit(1))
        for q in self.spark._wrapped.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, 'out')
        chk = os.path.join(tmpPath, 'chk')
        q = df.writeStream.option('checkpointLocation', chk).queryName('this_query') \
            .format('parquet').partitionBy('id').outputMode('append').option('path', out).start()
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

    def test_stream_status_and_progress(self):
        df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
        for q in self.spark._wrapped.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, 'out')
        chk = os.path.join(tmpPath, 'chk')

        def func(x):
            time.sleep(1)
            return x

        from pyspark.sql.functions import col, udf
        sleep_udf = udf(func)

        # Use "sleep_udf" to delay the progress update so that we can test `lastProgress` when there
        # were no updates.
        q = df.select(sleep_udf(col("value")).alias('value')).writeStream \
            .start(path=out, format='parquet', queryName='this_query', checkpointLocation=chk)
        try:
            # "lastProgress" will return None in most cases. However, as it may be flaky when
            # Jenkins is very slow, we don't assert it. If there is something wrong, "lastProgress"
            # may throw error with a high chance and make this test flaky, so we should still be
            # able to detect broken codes.
            q.lastProgress

            q.processAllAvailable()
            lastProgress = q.lastProgress
            recentProgress = q.recentProgress
            status = q.status
            self.assertEqual(lastProgress['name'], q.name)
            self.assertEqual(lastProgress['id'], q.id)
            self.assertTrue(any(p == lastProgress for p in recentProgress))
            self.assertTrue(
                "message" in status and
                "isDataAvailable" in status and
                "isTriggerActive" in status)
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

    def test_stream_exception(self):
        sdf = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
        sq = sdf.writeStream.format('memory').queryName('query_explain').start()
        try:
            sq.processAllAvailable()
            self.assertEqual(sq.exception(), None)
        finally:
            sq.stop()

        from pyspark.sql.functions import col, udf
        from pyspark.sql.utils import StreamingQueryException
        bad_udf = udf(lambda x: 1 / 0)
        sq = sdf.select(bad_udf(col("value")))\
            .writeStream\
            .format('memory')\
            .queryName('this_query')\
            .start()
        try:
            # Process some data to fail the query
            sq.processAllAvailable()
            self.fail("bad udf should fail the query")
        except StreamingQueryException as e:
            # This is expected
            self.assertTrue("ZeroDivisionError" in e.desc)
        finally:
            sq.stop()
        self.assertTrue(type(sq.exception()) is StreamingQueryException)
        self.assertTrue("ZeroDivisionError" in sq.exception().desc)

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

    class ForeachWriterTester:

        def __init__(self, spark):
            self.spark = spark

        def write_open_event(self, partitionId, epochId):
            self._write_event(
                self.open_events_dir,
                {'partition': partitionId, 'epoch': epochId})

        def write_process_event(self, row):
            self._write_event(self.process_events_dir, {'value': 'text'})

        def write_close_event(self, error):
            self._write_event(self.close_events_dir, {'error': str(error)})

        def write_input_file(self):
            self._write_event(self.input_dir, "text")

        def open_events(self):
            return self._read_events(self.open_events_dir, 'partition INT, epoch INT')

        def process_events(self):
            return self._read_events(self.process_events_dir, 'value STRING')

        def close_events(self):
            return self._read_events(self.close_events_dir, 'error STRING')

        def run_streaming_query_on_writer(self, writer, num_files):
            self._reset()
            try:
                sdf = self.spark.readStream.format('text').load(self.input_dir)
                sq = sdf.writeStream.foreach(writer).start()
                for i in range(num_files):
                    self.write_input_file()
                    sq.processAllAvailable()
            finally:
                self.stop_all()

        def assert_invalid_writer(self, writer, msg=None):
            self._reset()
            try:
                sdf = self.spark.readStream.format('text').load(self.input_dir)
                sq = sdf.writeStream.foreach(writer).start()
                self.write_input_file()
                sq.processAllAvailable()
                self.fail("invalid writer %s did not fail the query" % str(writer))  # not expected
            except Exception as e:
                if msg:
                    assert msg in str(e), "%s not in %s" % (msg, str(e))

            finally:
                self.stop_all()

        def stop_all(self):
            for q in self.spark._wrapped.streams.active:
                q.stop()

        def _reset(self):
            self.input_dir = tempfile.mkdtemp()
            self.open_events_dir = tempfile.mkdtemp()
            self.process_events_dir = tempfile.mkdtemp()
            self.close_events_dir = tempfile.mkdtemp()

        def _read_events(self, dir, json):
            rows = self.spark.read.schema(json).json(dir).collect()
            dicts = [row.asDict() for row in rows]
            return dicts

        def _write_event(self, dir, event):
            import uuid
            with open(os.path.join(dir, str(uuid.uuid4())), 'w') as f:
                f.write("%s\n" % str(event))

        def __getstate__(self):
            return (self.open_events_dir, self.process_events_dir, self.close_events_dir)

        def __setstate__(self, state):
            self.open_events_dir, self.process_events_dir, self.close_events_dir = state

    def test_streaming_foreach_with_simple_function(self):
        tester = self.ForeachWriterTester(self.spark)

        def foreach_func(row):
            tester.write_process_event(row)

        tester.run_streaming_query_on_writer(foreach_func, 2)
        self.assertEqual(len(tester.process_events()), 2)

    def test_streaming_foreach_with_basic_open_process_close(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def open(self, partitionId, epochId):
                tester.write_open_event(partitionId, epochId)
                return True

            def process(self, row):
                tester.write_process_event(row)

            def close(self, error):
                tester.write_close_event(error)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)

        open_events = tester.open_events()
        self.assertEqual(len(open_events), 2)
        self.assertSetEqual(set([e['epoch'] for e in open_events]), {0, 1})

        self.assertEqual(len(tester.process_events()), 2)

        close_events = tester.close_events()
        self.assertEqual(len(close_events), 2)
        self.assertSetEqual(set([e['error'] for e in close_events]), {'None'})

    def test_streaming_foreach_with_open_returning_false(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def open(self, partition_id, epoch_id):
                tester.write_open_event(partition_id, epoch_id)
                return False

            def process(self, row):
                tester.write_process_event(row)

            def close(self, error):
                tester.write_close_event(error)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)

        self.assertEqual(len(tester.open_events()), 2)

        self.assertEqual(len(tester.process_events()), 0)  # no row was processed

        close_events = tester.close_events()
        self.assertEqual(len(close_events), 2)
        self.assertSetEqual(set([e['error'] for e in close_events]), {'None'})

    def test_streaming_foreach_without_open_method(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def process(self, row):
                tester.write_process_event(row)

            def close(self, error):
                tester.write_close_event(error)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)
        self.assertEqual(len(tester.open_events()), 0)  # no open events
        self.assertEqual(len(tester.process_events()), 2)
        self.assertEqual(len(tester.close_events()), 2)

    def test_streaming_foreach_without_close_method(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def open(self, partition_id, epoch_id):
                tester.write_open_event(partition_id, epoch_id)
                return True

            def process(self, row):
                tester.write_process_event(row)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)
        self.assertEqual(len(tester.open_events()), 2)  # no open events
        self.assertEqual(len(tester.process_events()), 2)
        self.assertEqual(len(tester.close_events()), 0)

    def test_streaming_foreach_without_open_and_close_methods(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def process(self, row):
                tester.write_process_event(row)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)
        self.assertEqual(len(tester.open_events()), 0)  # no open events
        self.assertEqual(len(tester.process_events()), 2)
        self.assertEqual(len(tester.close_events()), 0)

    def test_streaming_foreach_with_process_throwing_error(self):
        from pyspark.sql.utils import StreamingQueryException

        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def process(self, row):
                raise Exception("test error")

            def close(self, error):
                tester.write_close_event(error)

        try:
            tester.run_streaming_query_on_writer(ForeachWriter(), 1)
            self.fail("bad writer did not fail the query")  # this is not expected
        except StreamingQueryException as e:
            # TODO: Verify whether original error message is inside the exception
            pass

        self.assertEqual(len(tester.process_events()), 0)  # no row was processed
        close_events = tester.close_events()
        self.assertEqual(len(close_events), 1)
        # TODO: Verify whether original error message is inside the exception

    def test_streaming_foreach_with_invalid_writers(self):

        tester = self.ForeachWriterTester(self.spark)

        def func_with_iterator_input(iter):
            for x in iter:
                print(x)

        tester.assert_invalid_writer(func_with_iterator_input)

        class WriterWithoutProcess:
            def open(self, partition):
                pass

        tester.assert_invalid_writer(WriterWithoutProcess(), "does not have a 'process'")

        class WriterWithNonCallableProcess():
            process = True

        tester.assert_invalid_writer(WriterWithNonCallableProcess(),
                                     "'process' in provided object is not callable")

        class WriterWithNoParamProcess():
            def process(self):
                pass

        tester.assert_invalid_writer(WriterWithNoParamProcess())

        # Abstract class for tests below
        class WithProcess():
            def process(self, row):
                pass

        class WriterWithNonCallableOpen(WithProcess):
            open = True

        tester.assert_invalid_writer(WriterWithNonCallableOpen(),
                                     "'open' in provided object is not callable")

        class WriterWithNoParamOpen(WithProcess):
            def open(self):
                pass

        tester.assert_invalid_writer(WriterWithNoParamOpen())

        class WriterWithNonCallableClose(WithProcess):
            close = True

        tester.assert_invalid_writer(WriterWithNonCallableClose(),
                                     "'close' in provided object is not callable")

    def test_streaming_foreachBatch(self):
        q = None
        collected = dict()

        def collectBatch(batch_df, batch_id):
            collected[batch_id] = batch_df.collect()

        try:
            df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
            q = df.writeStream.foreachBatch(collectBatch).start()
            q.processAllAvailable()
            self.assertTrue(0 in collected)
            self.assertTrue(len(collected[0]), 2)
        finally:
            if q:
                q.stop()

    def test_streaming_foreachBatch_propagates_python_errors(self):
        from pyspark.sql.utils import StreamingQueryException

        q = None

        def collectBatch(df, id):
            raise Exception("this should fail the query")

        try:
            df = self.spark.readStream.format('text').load('python/test_support/sql/streaming')
            q = df.writeStream.foreachBatch(collectBatch).start()
            q.processAllAvailable()
            self.fail("Expected a failure")
        except StreamingQueryException as e:
            self.assertTrue("this should fail" in str(e))
        finally:
            if q:
                q.stop()

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

    def test_merge_type(self):
        self.assertEqual(_merge_type(LongType(), NullType()), LongType())
        self.assertEqual(_merge_type(NullType(), LongType()), LongType())

        self.assertEqual(_merge_type(LongType(), LongType()), LongType())

        self.assertEqual(_merge_type(
            ArrayType(LongType()),
            ArrayType(LongType())
        ), ArrayType(LongType()))
        with self.assertRaisesRegexp(TypeError, 'element in array'):
            _merge_type(ArrayType(LongType()), ArrayType(DoubleType()))

        self.assertEqual(_merge_type(
            MapType(StringType(), LongType()),
            MapType(StringType(), LongType())
        ), MapType(StringType(), LongType()))
        with self.assertRaisesRegexp(TypeError, 'key of map'):
            _merge_type(
                MapType(StringType(), LongType()),
                MapType(DoubleType(), LongType()))
        with self.assertRaisesRegexp(TypeError, 'value of map'):
            _merge_type(
                MapType(StringType(), LongType()),
                MapType(StringType(), DoubleType()))

        self.assertEqual(_merge_type(
            StructType([StructField("f1", LongType()), StructField("f2", StringType())]),
            StructType([StructField("f1", LongType()), StructField("f2", StringType())])
        ), StructType([StructField("f1", LongType()), StructField("f2", StringType())]))
        with self.assertRaisesRegexp(TypeError, 'field f1'):
            _merge_type(
                StructType([StructField("f1", LongType()), StructField("f2", StringType())]),
                StructType([StructField("f1", DoubleType()), StructField("f2", StringType())]))

        self.assertEqual(_merge_type(
            StructType([StructField("f1", StructType([StructField("f2", LongType())]))]),
            StructType([StructField("f1", StructType([StructField("f2", LongType())]))])
        ), StructType([StructField("f1", StructType([StructField("f2", LongType())]))]))
        with self.assertRaisesRegexp(TypeError, 'field f2 in field f1'):
            _merge_type(
                StructType([StructField("f1", StructType([StructField("f2", LongType())]))]),
                StructType([StructField("f1", StructType([StructField("f2", StringType())]))]))

        self.assertEqual(_merge_type(
            StructType([StructField("f1", ArrayType(LongType())), StructField("f2", StringType())]),
            StructType([StructField("f1", ArrayType(LongType())), StructField("f2", StringType())])
        ), StructType([StructField("f1", ArrayType(LongType())), StructField("f2", StringType())]))
        with self.assertRaisesRegexp(TypeError, 'element in array field f1'):
            _merge_type(
                StructType([
                    StructField("f1", ArrayType(LongType())),
                    StructField("f2", StringType())]),
                StructType([
                    StructField("f1", ArrayType(DoubleType())),
                    StructField("f2", StringType())]))

        self.assertEqual(_merge_type(
            StructType([
                StructField("f1", MapType(StringType(), LongType())),
                StructField("f2", StringType())]),
            StructType([
                StructField("f1", MapType(StringType(), LongType())),
                StructField("f2", StringType())])
        ), StructType([
            StructField("f1", MapType(StringType(), LongType())),
            StructField("f2", StringType())]))
        with self.assertRaisesRegexp(TypeError, 'value of map field f1'):
            _merge_type(
                StructType([
                    StructField("f1", MapType(StringType(), LongType())),
                    StructField("f2", StringType())]),
                StructType([
                    StructField("f1", MapType(StringType(), DoubleType())),
                    StructField("f2", StringType())]))

        self.assertEqual(_merge_type(
            StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]),
            StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))])
        ), StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]))
        with self.assertRaisesRegexp(TypeError, 'key of map element in array field f1'):
            _merge_type(
                StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]),
                StructType([StructField("f1", ArrayType(MapType(DoubleType(), LongType())))])
            )

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

    # regression test for SPARK-19561
    def test_datetime_at_epoch(self):
        epoch = datetime.datetime.fromtimestamp(0)
        df = self.spark.createDataFrame([Row(date=epoch)])
        first = df.select('date', lit(epoch).alias('lit_date')).first()
        self.assertEqual(first['date'], epoch)
        self.assertEqual(first['lit_date'], epoch)

    def test_dayofweek(self):
        from pyspark.sql.functions import dayofweek
        dt = datetime.datetime(2017, 11, 6)
        df = self.spark.createDataFrame([Row(date=dt)])
        row = df.select(dayofweek(df.date)).first()
        self.assertEqual(row[0], 2)

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
        df5 = df1.repartitionByRange("name", "age")
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
        with self.assertRaises(ValueError):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.1)], schema).replace({10: 11}, subset=1).first()

        # should fail if to_replace and value have different length
        with self.assertRaises(ValueError):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.1)], schema).replace(["Alice", "Bob"], ["Eve"]).first()

        # should fail if when received unexpected type
        with self.assertRaises(ValueError):
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

        with self.assertRaisesRegexp(
                TypeError,
                'value argument is required when to_replace is not a dictionary.'):
            self.spark.createDataFrame(
                [(u'Alice', 10, 80.0)], schema).replace(["Alice", "Bob"]).first()

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
        plan2 = df1.crossJoin(broadcast(df2))._jdf.queryExecution().executedPlan()
        self.assertEqual(0, plan2.toString().count("BroadcastHashJoin"))

        # planner should not crash without a join
        broadcast(df1)._jdf.queryExecution().executedPlan()

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

    def test_sample(self):
        self.assertRaisesRegexp(
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
        from pyspark.sql.functions import broadcast

        df1 = self.spark.createDataFrame([(1, "1")], ("key", "value"))
        df2 = self.spark.createDataFrame([(1, "1")], ("key", "value"))

        # joins without conditions require cross join syntax
        self.assertRaises(AnalysisException, lambda: df1.join(df2).collect())

        # works with crossJoin
        self.assertEqual(1, df1.crossJoin(df2).count())

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

        self.assertEqual(spark.conf.get("hyukjin", None), None)

        # This returns 'STATIC' because it's the default value of
        # 'spark.sql.sources.partitionOverwriteMode', and `defaultValue` in
        # `spark.conf.get` is unset.
        self.assertEqual(spark.conf.get("spark.sql.sources.partitionOverwriteMode"), "STATIC")

        # This returns None because 'spark.sql.sources.partitionOverwriteMode' is unset, but
        # `defaultValue` in `spark.conf.get` is set to None.
        self.assertEqual(spark.conf.get("spark.sql.sources.partitionOverwriteMode", None), None)

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
        spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
        spark.sql("CREATE TABLE some_db.tab2 (name STRING, age INT) USING parquet")
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
        self.assertTrue("to_date" in functions)
        self.assertTrue("to_timestamp" in functions)
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
        spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
        spark.sql("CREATE TABLE some_db.tab2 (nickname STRING, tolerance FLOAT) USING parquet")
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

    def test_read_text_file_list(self):
        df = self.spark.read.text(['python/test_support/sql/text-test.txt',
                                   'python/test_support/sql/text-test.txt'])
        count = df.count()
        self.assertEquals(count, 4)

    def test_BinaryType_serialization(self):
        # Pyrolite version <= 4.9 could not serialize BinaryType with Python3 SPARK-17808
        # The empty bytearray is test for SPARK-21534.
        schema = StructType([StructField('mybytes', BinaryType())])
        data = [[bytearray(b'here is my data')],
                [bytearray(b'and here is some more')],
                [bytearray(b'')]]
        df = self.spark.createDataFrame(data, schema=schema)
        df.collect()

    # test for SPARK-16542
    def test_array_types(self):
        # This test need to make sure that the Scala type selected is at least
        # as large as the python's types. This is necessary because python's
        # array types depend on C implementation on the machine. Therefore there
        # is no machine independent correspondence between python's array types
        # and Scala types.
        # See: https://docs.python.org/2/library/array.html

        def assertCollectSuccess(typecode, value):
            row = Row(myarray=array.array(typecode, [value]))
            df = self.spark.createDataFrame([row])
            self.assertEqual(df.first()["myarray"][0], value)

        # supported string types
        #
        # String types in python's array are "u" for Py_UNICODE and "c" for char.
        # "u" will be removed in python 4, and "c" is not supported in python 3.
        supported_string_types = []
        if sys.version_info[0] < 4:
            supported_string_types += ['u']
            # test unicode
            assertCollectSuccess('u', u'a')
        if sys.version_info[0] < 3:
            supported_string_types += ['c']
            # test string
            assertCollectSuccess('c', 'a')

        # supported float and double
        #
        # Test max, min, and precision for float and double, assuming IEEE 754
        # floating-point format.
        supported_fractional_types = ['f', 'd']
        assertCollectSuccess('f', ctypes.c_float(1e+38).value)
        assertCollectSuccess('f', ctypes.c_float(1e-38).value)
        assertCollectSuccess('f', ctypes.c_float(1.123456).value)
        assertCollectSuccess('d', sys.float_info.max)
        assertCollectSuccess('d', sys.float_info.min)
        assertCollectSuccess('d', sys.float_info.epsilon)

        # supported signed int types
        #
        # The size of C types changes with implementation, we need to make sure
        # that there is no overflow error on the platform running this test.
        supported_signed_int_types = list(
            set(_array_signed_int_typecode_ctype_mappings.keys())
            .intersection(set(_array_type_mappings.keys())))
        for t in supported_signed_int_types:
            ctype = _array_signed_int_typecode_ctype_mappings[t]
            max_val = 2 ** (ctypes.sizeof(ctype) * 8 - 1)
            assertCollectSuccess(t, max_val - 1)
            assertCollectSuccess(t, -max_val)

        # supported unsigned int types
        #
        # JVM does not have unsigned types. We need to be very careful to make
        # sure that there is no overflow error.
        supported_unsigned_int_types = list(
            set(_array_unsigned_int_typecode_ctype_mappings.keys())
            .intersection(set(_array_type_mappings.keys())))
        for t in supported_unsigned_int_types:
            ctype = _array_unsigned_int_typecode_ctype_mappings[t]
            assertCollectSuccess(t, 2 ** (ctypes.sizeof(ctype) * 8) - 1)

        # all supported types
        #
        # Make sure the types tested above:
        # 1. are all supported types
        # 2. cover all supported types
        supported_types = (supported_string_types +
                           supported_fractional_types +
                           supported_signed_int_types +
                           supported_unsigned_int_types)
        self.assertEqual(set(supported_types), set(_array_type_mappings.keys()))

        # all unsupported types
        #
        # Keys in _array_type_mappings is a complete list of all supported types,
        # and types not in _array_type_mappings are considered unsupported.
        # `array.typecodes` are not supported in python 2.
        if sys.version_info[0] < 3:
            all_types = set(['c', 'b', 'B', 'u', 'h', 'H', 'i', 'I', 'l', 'L', 'f', 'd'])
        else:
            all_types = set(array.typecodes)
        unsupported_types = all_types - set(supported_types)
        # test unsupported types
        for t in unsupported_types:
            with self.assertRaises(TypeError):
                a = array.array(t)
                self.spark.createDataFrame([Row(myarray=a)]).collect()

    def test_bucketed_write(self):
        data = [
            (1, "foo", 3.0), (2, "foo", 5.0),
            (3, "bar", -1.0), (4, "bar", 6.0),
        ]
        df = self.spark.createDataFrame(data, ["x", "y", "z"])

        def count_bucketed_cols(names, table="pyspark_bucket"):
            """Given a sequence of column names and a table name
            query the catalog and return number o columns which are
            used for bucketing
            """
            cols = self.spark.catalog.listColumns(table)
            num = len([c for c in cols if c.name in names and c.isBucket])
            return num

        # Test write with one bucketing column
        df.write.bucketBy(3, "x").mode("overwrite").saveAsTable("pyspark_bucket")
        self.assertEqual(count_bucketed_cols(["x"]), 1)
        self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

        # Test write two bucketing columns
        df.write.bucketBy(3, "x", "y").mode("overwrite").saveAsTable("pyspark_bucket")
        self.assertEqual(count_bucketed_cols(["x", "y"]), 2)
        self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

        # Test write with bucket and sort
        df.write.bucketBy(2, "x").sortBy("z").mode("overwrite").saveAsTable("pyspark_bucket")
        self.assertEqual(count_bucketed_cols(["x"]), 1)
        self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

        # Test write with a list of columns
        df.write.bucketBy(3, ["x", "y"]).mode("overwrite").saveAsTable("pyspark_bucket")
        self.assertEqual(count_bucketed_cols(["x", "y"]), 2)
        self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

        # Test write with bucket and sort with a list of columns
        (df.write.bucketBy(2, "x")
            .sortBy(["y", "z"])
            .mode("overwrite").saveAsTable("pyspark_bucket"))
        self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

        # Test write with bucket and sort with multiple columns
        (df.write.bucketBy(2, "x")
            .sortBy("y", "z")
            .mode("overwrite").saveAsTable("pyspark_bucket"))
        self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

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

    @unittest.skipIf(not _have_pandas, _pandas_requirement_message)
    def test_to_pandas(self):
        import numpy as np
        pdf = self._to_pandas()
        types = pdf.dtypes
        self.assertEquals(types[0], np.int32)
        self.assertEquals(types[1], np.object)
        self.assertEquals(types[2], np.bool)
        self.assertEquals(types[3], np.float32)
        self.assertEquals(types[4], np.object)  # datetime.date
        self.assertEquals(types[5], 'datetime64[ns]')

    @unittest.skipIf(_have_pandas, "Required Pandas was found.")
    def test_to_pandas_required_pandas_not_found(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(ImportError, 'Pandas >= .* must be installed'):
                self._to_pandas()

    @unittest.skipIf(not _have_pandas, _pandas_requirement_message)
    def test_to_pandas_avoid_astype(self):
        import numpy as np
        schema = StructType().add("a", IntegerType()).add("b", StringType())\
                             .add("c", IntegerType())
        data = [(1, "foo", 16777220), (None, "bar", None)]
        df = self.spark.createDataFrame(data, schema)
        types = df.toPandas().dtypes
        self.assertEquals(types[0], np.float64)  # doesn't convert to np.int32 due to NaN value.
        self.assertEquals(types[1], np.object)
        self.assertEquals(types[2], np.float64)

    def test_create_dataframe_from_array_of_long(self):
        import array
        data = [Row(longarray=array.array('l', [-9223372036854775808, 0, 9223372036854775807]))]
        df = self.spark.createDataFrame(data)
        self.assertEqual(df.first(), Row(longarray=[-9223372036854775808, 0, 9223372036854775807]))

    @unittest.skipIf(not _have_pandas, _pandas_requirement_message)
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

    @unittest.skipIf(_have_pandas, "Required Pandas was found.")
    def test_create_dataframe_required_pandas_not_found(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    ImportError,
                    "(Pandas >= .* must be installed|No module named '?pandas'?)"):
                import pandas as pd
                from datetime import datetime
                pdf = pd.DataFrame({"ts": [datetime(2017, 10, 31, 1, 1, 1)],
                                    "d": [pd.Timestamp.now().date()]})
                self.spark.createDataFrame(pdf)

    # Regression test for SPARK-23360
    @unittest.skipIf(not _have_pandas, _pandas_requirement_message)
    def test_create_dateframe_from_pandas_with_dst(self):
        import pandas as pd
        from datetime import datetime

        pdf = pd.DataFrame({'time': [datetime(2015, 10, 31, 22, 30)]})

        df = self.spark.createDataFrame(pdf)
        self.assertPandasEqual(pdf, df.toPandas())

        orig_env_tz = os.environ.get('TZ', None)
        try:
            tz = 'America/Los_Angeles'
            os.environ['TZ'] = tz
            time.tzset()
            with self.sql_conf({'spark.sql.session.timeZone': tz}):
                df = self.spark.createDataFrame(pdf)
                self.assertPandasEqual(pdf, df.toPandas())
        finally:
            del os.environ['TZ']
            if orig_env_tz is not None:
                os.environ['TZ'] = orig_env_tz
            time.tzset()

    def test_sort_with_nulls_order(self):
        from pyspark.sql import functions

        df = self.spark.createDataFrame(
            [('Tom', 80), (None, 60), ('Alice', 50)], ["name", "height"])
        self.assertEquals(
            df.select(df.name).orderBy(functions.asc_nulls_first('name')).collect(),
            [Row(name=None), Row(name=u'Alice'), Row(name=u'Tom')])
        self.assertEquals(
            df.select(df.name).orderBy(functions.asc_nulls_last('name')).collect(),
            [Row(name=u'Alice'), Row(name=u'Tom'), Row(name=None)])
        self.assertEquals(
            df.select(df.name).orderBy(functions.desc_nulls_first('name')).collect(),
            [Row(name=None), Row(name=u'Tom'), Row(name=u'Alice')])
        self.assertEquals(
            df.select(df.name).orderBy(functions.desc_nulls_last('name')).collect(),
            [Row(name=u'Tom'), Row(name=u'Alice'), Row(name=None)])

    def test_json_sampling_ratio(self):
        rdd = self.spark.sparkContext.range(0, 100, 1, 1) \
            .map(lambda x: '{"a":0.1}' if x == 1 else '{"a":%s}' % str(x))
        schema = self.spark.read.option('inferSchema', True) \
            .option('samplingRatio', 0.5) \
            .json(rdd).schema
        self.assertEquals(schema, StructType([StructField("a", LongType(), True)]))

    def test_csv_sampling_ratio(self):
        rdd = self.spark.sparkContext.range(0, 100, 1, 1) \
            .map(lambda x: '0.1' if x == 1 else str(x))
        schema = self.spark.read.option('inferSchema', True)\
            .csv(rdd, samplingRatio=0.5).schema
        self.assertEquals(schema, StructType([StructField("_c0", IntegerType(), True)]))

    def test_checking_csv_header(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)
        try:
            self.spark.createDataFrame([[1, 1000], [2000, 2]])\
                .toDF('f1', 'f2').write.option("header", "true").csv(path)
            schema = StructType([
                StructField('f2', IntegerType(), nullable=True),
                StructField('f1', IntegerType(), nullable=True)])
            df = self.spark.read.option('header', 'true').schema(schema)\
                .csv(path, enforceSchema=False)
            self.assertRaisesRegexp(
                Exception,
                "CSV header does not conform to the schema",
                lambda: df.collect())
        finally:
            shutil.rmtree(path)

    def test_ignore_column_of_all_nulls(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)
        try:
            df = self.spark.createDataFrame([["""{"a":null, "b":1, "c":3.0}"""],
                                             ["""{"a":null, "b":null, "c":"string"}"""],
                                             ["""{"a":null, "b":null, "c":null}"""]])
            df.write.text(path)
            schema = StructType([
                StructField('b', LongType(), nullable=True),
                StructField('c', StringType(), nullable=True)])
            readback = self.spark.read.json(path, dropFieldIfAllNull=True)
            self.assertEquals(readback.schema, schema)
        finally:
            shutil.rmtree(path)

    # SPARK-24721
    @unittest.skipIf(not _test_compiled, _test_not_compiled_message)
    def test_datasource_with_udf(self):
        from pyspark.sql.functions import udf, lit, col

        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(1).write.mode("overwrite").format('csv').save(path)
            filesource_df = self.spark.read.option('inferSchema', True).csv(path).toDF('i')
            datasource_df = self.spark.read \
                .format("org.apache.spark.sql.sources.SimpleScanSource") \
                .option('from', 0).option('to', 1).load().toDF('i')
            datasource_v2_df = self.spark.read \
                .format("org.apache.spark.sql.sources.v2.SimpleDataSourceV2") \
                .load().toDF('i', 'j')

            c1 = udf(lambda x: x + 1, 'int')(lit(1))
            c2 = udf(lambda x: x + 1, 'int')(col('i'))

            f1 = udf(lambda x: False, 'boolean')(lit(1))
            f2 = udf(lambda x: False, 'boolean')(col('i'))

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn('c', c1)
                expected = df.withColumn('c', lit(2))
                self.assertEquals(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn('c', c2)
                expected = df.withColumn('c', col('i') + 1)
                self.assertEquals(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                for f in [f1, f2]:
                    result = df.filter(f)
                    self.assertEquals(0, result.count())
        finally:
            shutil.rmtree(path)

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
            self.assertEquals(re.sub(pattern, '', expected1), df.__repr__())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                expected2 = """+---+-----+
                ||key|value|
                |+---+-----+
                ||  1|    1|
                ||222|  222|
                |+---+-----+
                |"""
                self.assertEquals(re.sub(pattern, '', expected2), df.__repr__())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    expected3 = """+---+-----+
                    ||key|value|
                    |+---+-----+
                    ||  1|    1|
                    |+---+-----+
                    |only showing top 1 row
                    |"""
                    self.assertEquals(re.sub(pattern, '', expected3), df.__repr__())

        # test when eager evaluation is enabled and _repr_html_ will be called
        with self.sql_conf({"spark.sql.repl.eagerEval.enabled": True}):
            expected1 = """<table border='1'>
                |<tr><th>key</th><th>value</th></tr>
                |<tr><td>1</td><td>1</td></tr>
                |<tr><td>22222</td><td>22222</td></tr>
                |</table>
                |"""
            self.assertEquals(re.sub(pattern, '', expected1), df._repr_html_())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                expected2 = """<table border='1'>
                    |<tr><th>key</th><th>value</th></tr>
                    |<tr><td>1</td><td>1</td></tr>
                    |<tr><td>222</td><td>222</td></tr>
                    |</table>
                    |"""
                self.assertEquals(re.sub(pattern, '', expected2), df._repr_html_())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    expected3 = """<table border='1'>
                        |<tr><th>key</th><th>value</th></tr>
                        |<tr><td>1</td><td>1</td></tr>
                        |</table>
                        |only showing top 1 row
                        |"""
                    self.assertEquals(re.sub(pattern, '', expected3), df._repr_html_())

        # test when eager evaluation is disabled and _repr_html_ will be called
        with self.sql_conf({"spark.sql.repl.eagerEval.enabled": False}):
            expected = "DataFrame[key: bigint, value: string]"
            self.assertEquals(None, df._repr_html_())
            self.assertEquals(expected, df.__repr__())
            with self.sql_conf({"spark.sql.repl.eagerEval.truncate": 3}):
                self.assertEquals(None, df._repr_html_())
                self.assertEquals(expected, df.__repr__())
                with self.sql_conf({"spark.sql.repl.eagerEval.maxNumRows": 1}):
                    self.assertEquals(None, df._repr_html_())
                    self.assertEquals(expected, df.__repr__())

    # SPARK-25591
    def test_same_accumulator_in_udfs(self):
        from pyspark.sql.functions import udf

        data_schema = StructType([StructField("a", IntegerType(), True),
                                  StructField("b", IntegerType(), True)])
        data = self.spark.createDataFrame([[1, 2]], schema=data_schema)

        test_accum = self.sc.accumulator(0)

        def first_udf(x):
            test_accum.add(1)
            return x

        def second_udf(x):
            test_accum.add(100)
            return x

        func_udf = udf(first_udf, IntegerType())
        func_udf2 = udf(second_udf, IntegerType())
        data = data.withColumn("out1", func_udf(data["a"]))
        data = data.withColumn("out2", func_udf2(data["b"]))
        data.collect()
        self.assertEqual(test_accum.value, 101)


class HiveSparkSubmitTests(SparkSubmitTests):

    @classmethod
    def setUpClass(cls):
        # get a SparkContext to check for availability of Hive
        sc = SparkContext('local[4]', cls.__name__)
        cls.hive_available = True
        try:
            sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        except py4j.protocol.Py4JError:
            cls.hive_available = False
        except TypeError:
            cls.hive_available = False
        finally:
            # we don't need this SparkContext for the test
            sc.stop()

    def setUp(self):
        super(HiveSparkSubmitTests, self).setUp()
        if not self.hive_available:
            self.skipTest("Hive is not available.")

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
            self.sparkSubmit + ["--master", "local-cluster[1,1,1024]",
                                "--driver-class-path", hive_site_dir, script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("default", out.decode('utf-8'))
        self.assertTrue(os.path.exists(metastore_path))


class SQLTests2(ReusedSQLTestCase):

    # We can't include this test into SQLTests because we will stop class's SparkContext and cause
    # other tests failed.
    def test_sparksession_with_stopped_sparkcontext(self):
        self.sc.stop()
        sc = SparkContext('local[4]', self.sc.appName)
        spark = SparkSession.builder.getOrCreate()
        try:
            df = spark.createDataFrame([(1, 2)], ["c", "c"])
            df.collect()
        finally:
            spark.stop()
            sc.stop()


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
        not _have_pandas or not _have_pyarrow,
        _pandas_requirement_message or _pyarrow_requirement_message)
    def test_query_execution_listener_on_collect_with_arrow(self):
        with self.sql_conf({"spark.sql.execution.arrow.enabled": True}):
            self.assertFalse(
                self.spark._jvm.OnSuccessCall.isCalled(),
                "The callback from the query execution listener should not be "
                "called before 'toPandas'")
            self.spark.sql("SELECT * FROM range(1)").toPandas()
            self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
            self.assertTrue(
                self.spark._jvm.OnSuccessCall.isCalled(),
                "The callback from the query execution listener should be called after 'toPandas'")


class SparkSessionTests(PySparkTestCase):

    # This test is separate because it's closely related with session's start and stop.
    # See SPARK-23228.
    def test_set_jvm_default_session(self):
        spark = SparkSession.builder.getOrCreate()
        try:
            self.assertTrue(spark._jvm.SparkSession.getDefaultSession().isDefined())
        finally:
            spark.stop()
            self.assertTrue(spark._jvm.SparkSession.getDefaultSession().isEmpty())

    def test_jvm_default_session_already_set(self):
        # Here, we assume there is the default session already set in JVM.
        jsession = self.sc._jvm.SparkSession(self.sc._jsc.sc())
        self.sc._jvm.SparkSession.setDefaultSession(jsession)

        spark = SparkSession.builder.getOrCreate()
        try:
            self.assertTrue(spark._jvm.SparkSession.getDefaultSession().isDefined())
            # The session should be the same with the exiting one.
            self.assertTrue(jsession.equals(spark._jvm.SparkSession.getDefaultSession().get()))
        finally:
            spark.stop()


class UDFInitializationTests(unittest.TestCase):
    def tearDown(self):
        if SparkSession._instantiatedSession is not None:
            SparkSession._instantiatedSession.stop()

        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

    def test_udf_init_shouldnt_initialize_context(self):
        from pyspark.sql.functions import UserDefinedFunction

        UserDefinedFunction(lambda x: x, StringType())

        self.assertIsNone(
            SparkContext._active_spark_context,
            "SparkContext shouldn't be initialized when UserDefinedFunction is created."
        )
        self.assertIsNone(
            SparkSession._instantiatedSession,
            "SparkSession shouldn't be initialized when UserDefinedFunction is created."
        )


class HiveContextSQLTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        cls.hive_available = True
        try:
            cls.sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        except py4j.protocol.Py4JError:
            cls.hive_available = False
        except TypeError:
            cls.hive_available = False
        os.unlink(cls.tempdir.name)
        if cls.hive_available:
            cls.spark = HiveContext._createForTesting(cls.sc)
            cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
            cls.df = cls.sc.parallelize(cls.testData).toDF()

    def setUp(self):
        if not self.hive_available:
            self.skipTest("Hive is not available.")

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

    def test_window_functions_cumulative_sum(self):
        df = self.spark.createDataFrame([("one", 1), ("two", 2)], ["key", "value"])
        from pyspark.sql import functions as F

        # Test cumulative sum
        sel = df.select(
            df.key,
            F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding, 0)))
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

        # Test boundary values less than JVM's Long.MinValue and make sure we don't overflow
        sel = df.select(
            df.key,
            F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding - 1, 0)))
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

        # Test boundary values greater than JVM's Long.MaxValue and make sure we don't overflow
        frame_end = Window.unboundedFollowing + 1
        sel = df.select(
            df.key,
            F.sum(df.value).over(Window.rowsBetween(Window.currentRow, frame_end)))
        rs = sorted(sel.collect())
        expected = [("one", 3), ("two", 2)]
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

    def test_datetime_functions(self):
        from pyspark.sql import functions
        from datetime import date, datetime
        df = self.spark.range(1).selectExpr("'2017-01-22' as dateCol")
        parse_result = df.select(functions.to_date(functions.col("dateCol"))).first()
        self.assertEquals(date(2017, 1, 22), parse_result['to_date(`dateCol`)'])

    @unittest.skipIf(sys.version_info < (3, 3), "Unittest < 3.3 doesn't support mocking")
    def test_unbounded_frames(self):
        from unittest.mock import patch
        from pyspark.sql import functions as F
        from pyspark.sql import window
        import importlib

        df = self.spark.range(0, 3)

        def rows_frame_match():
            return "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in df.select(
                F.count("*").over(window.Window.rowsBetween(-sys.maxsize, sys.maxsize))
            ).columns[0]

        def range_frame_match():
            return "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in df.select(
                F.count("*").over(window.Window.rangeBetween(-sys.maxsize, sys.maxsize))
            ).columns[0]

        with patch("sys.maxsize", 2 ** 31 - 1):
            importlib.reload(window)
            self.assertTrue(rows_frame_match())
            self.assertTrue(range_frame_match())

        with patch("sys.maxsize", 2 ** 63 - 1):
            importlib.reload(window)
            self.assertTrue(rows_frame_match())
            self.assertTrue(range_frame_match())

        with patch("sys.maxsize", 2 ** 127 - 1):
            importlib.reload(window)
            self.assertTrue(rows_frame_match())
            self.assertTrue(range_frame_match())

        importlib.reload(window)


class DataTypeVerificationTests(unittest.TestCase):

    def test_verify_type_exception_msg(self):
        self.assertRaisesRegexp(
            ValueError,
            "test_name",
            lambda: _make_type_verifier(StringType(), nullable=False, name="test_name")(None))

        schema = StructType([StructField('a', StructType([StructField('b', IntegerType())]))])
        self.assertRaisesRegexp(
            TypeError,
            "field b in field a",
            lambda: _make_type_verifier(schema)([["data"]]))

    def test_verify_type_ok_nullable(self):
        obj = None
        types = [IntegerType(), FloatType(), StringType(), StructType([])]
        for data_type in types:
            try:
                _make_type_verifier(data_type, nullable=True)(obj)
            except Exception:
                self.fail("verify_type(%s, %s, nullable=True)" % (obj, data_type))

    def test_verify_type_not_nullable(self):
        import array
        import datetime
        import decimal

        schema = StructType([
            StructField('s', StringType(), nullable=False),
            StructField('i', IntegerType(), nullable=True)])

        class MyObj:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        # obj, data_type
        success_spec = [
            # String
            ("", StringType()),
            (u"", StringType()),
            (1, StringType()),
            (1.0, StringType()),
            ([], StringType()),
            ({}, StringType()),

            # UDT
            (ExamplePoint(1.0, 2.0), ExamplePointUDT()),

            # Boolean
            (True, BooleanType()),

            # Byte
            (-(2**7), ByteType()),
            (2**7 - 1, ByteType()),

            # Short
            (-(2**15), ShortType()),
            (2**15 - 1, ShortType()),

            # Integer
            (-(2**31), IntegerType()),
            (2**31 - 1, IntegerType()),

            # Long
            (2**64, LongType()),

            # Float & Double
            (1.0, FloatType()),
            (1.0, DoubleType()),

            # Decimal
            (decimal.Decimal("1.0"), DecimalType()),

            # Binary
            (bytearray([1, 2]), BinaryType()),

            # Date/Timestamp
            (datetime.date(2000, 1, 2), DateType()),
            (datetime.datetime(2000, 1, 2, 3, 4), DateType()),
            (datetime.datetime(2000, 1, 2, 3, 4), TimestampType()),

            # Array
            ([], ArrayType(IntegerType())),
            (["1", None], ArrayType(StringType(), containsNull=True)),
            ([1, 2], ArrayType(IntegerType())),
            ((1, 2), ArrayType(IntegerType())),
            (array.array('h', [1, 2]), ArrayType(IntegerType())),

            # Map
            ({}, MapType(StringType(), IntegerType())),
            ({"a": 1}, MapType(StringType(), IntegerType())),
            ({"a": None}, MapType(StringType(), IntegerType(), valueContainsNull=True)),

            # Struct
            ({"s": "a", "i": 1}, schema),
            ({"s": "a", "i": None}, schema),
            ({"s": "a"}, schema),
            ({"s": "a", "f": 1.0}, schema),
            (Row(s="a", i=1), schema),
            (Row(s="a", i=None), schema),
            (Row(s="a", i=1, f=1.0), schema),
            (["a", 1], schema),
            (["a", None], schema),
            (("a", 1), schema),
            (MyObj(s="a", i=1), schema),
            (MyObj(s="a", i=None), schema),
            (MyObj(s="a"), schema),
        ]

        # obj, data_type, exception class
        failure_spec = [
            # String (match anything but None)
            (None, StringType(), ValueError),

            # UDT
            (ExamplePoint(1.0, 2.0), PythonOnlyUDT(), ValueError),

            # Boolean
            (1, BooleanType(), TypeError),
            ("True", BooleanType(), TypeError),
            ([1], BooleanType(), TypeError),

            # Byte
            (-(2**7) - 1, ByteType(), ValueError),
            (2**7, ByteType(), ValueError),
            ("1", ByteType(), TypeError),
            (1.0, ByteType(), TypeError),

            # Short
            (-(2**15) - 1, ShortType(), ValueError),
            (2**15, ShortType(), ValueError),

            # Integer
            (-(2**31) - 1, IntegerType(), ValueError),
            (2**31, IntegerType(), ValueError),

            # Float & Double
            (1, FloatType(), TypeError),
            (1, DoubleType(), TypeError),

            # Decimal
            (1.0, DecimalType(), TypeError),
            (1, DecimalType(), TypeError),
            ("1.0", DecimalType(), TypeError),

            # Binary
            (1, BinaryType(), TypeError),

            # Date/Timestamp
            ("2000-01-02", DateType(), TypeError),
            (946811040, TimestampType(), TypeError),

            # Array
            (["1", None], ArrayType(StringType(), containsNull=False), ValueError),
            ([1, "2"], ArrayType(IntegerType()), TypeError),

            # Map
            ({"a": 1}, MapType(IntegerType(), IntegerType()), TypeError),
            ({"a": "1"}, MapType(StringType(), IntegerType()), TypeError),
            ({"a": None}, MapType(StringType(), IntegerType(), valueContainsNull=False),
             ValueError),

            # Struct
            ({"s": "a", "i": "1"}, schema, TypeError),
            (Row(s="a"), schema, ValueError),     # Row can't have missing field
            (Row(s="a", i="1"), schema, TypeError),
            (["a"], schema, ValueError),
            (["a", "1"], schema, TypeError),
            (MyObj(s="a", i="1"), schema, TypeError),
            (MyObj(s=None, i="1"), schema, ValueError),
        ]

        # Check success cases
        for obj, data_type in success_spec:
            try:
                _make_type_verifier(data_type, nullable=False)(obj)
            except Exception:
                self.fail("verify_type(%s, %s, nullable=False)" % (obj, data_type))

        # Check failure cases
        for obj, data_type, exp in failure_spec:
            msg = "verify_type(%s, %s, nullable=False) == %s" % (obj, data_type, exp)
            with self.assertRaises(exp, msg=msg):
                _make_type_verifier(data_type, nullable=False)(obj)


@unittest.skipIf(
    not _have_pandas or not _have_pyarrow,
    _pandas_requirement_message or _pyarrow_requirement_message)
class ArrowTests(ReusedSQLTestCase):

    @classmethod
    def setUpClass(cls):
        from datetime import date, datetime
        from decimal import Decimal
        from distutils.version import LooseVersion
        import pyarrow as pa
        super(ArrowTests, cls).setUpClass()
        cls.warnings_lock = threading.Lock()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.spark.conf.set("spark.sql.session.timeZone", tz)
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        # Disable fallback by default to easily detect the failures.
        cls.spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")
        cls.schema = StructType([
            StructField("1_str_t", StringType(), True),
            StructField("2_int_t", IntegerType(), True),
            StructField("3_long_t", LongType(), True),
            StructField("4_float_t", FloatType(), True),
            StructField("5_double_t", DoubleType(), True),
            StructField("6_decimal_t", DecimalType(38, 18), True),
            StructField("7_date_t", DateType(), True),
            StructField("8_timestamp_t", TimestampType(), True)])
        cls.data = [(u"a", 1, 10, 0.2, 2.0, Decimal("2.0"),
                     date(1969, 1, 1), datetime(1969, 1, 1, 1, 1, 1)),
                    (u"b", 2, 20, 0.4, 4.0, Decimal("4.0"),
                     date(2012, 2, 2), datetime(2012, 2, 2, 2, 2, 2)),
                    (u"c", 3, 30, 0.8, 6.0, Decimal("6.0"),
                     date(2100, 3, 3), datetime(2100, 3, 3, 3, 3, 3))]

        # TODO: remove version check once minimum pyarrow version is 0.10.0
        if LooseVersion("0.10.0") <= LooseVersion(pa.__version__):
            cls.schema.add(StructField("9_binary_t", BinaryType(), True))
            cls.data[0] = cls.data[0] + (bytearray(b"a"),)
            cls.data[1] = cls.data[1] + (bytearray(b"bb"),)
            cls.data[2] = cls.data[2] + (bytearray(b"ccc"),)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        super(ArrowTests, cls).tearDownClass()

    def create_pandas_data_frame(self):
        import pandas as pd
        import numpy as np
        data_dict = {}
        for j, name in enumerate(self.schema.names):
            data_dict[name] = [self.data[i][j] for i in range(len(self.data))]
        # need to convert these to numpy types first
        data_dict["2_int_t"] = np.int32(data_dict["2_int_t"])
        data_dict["4_float_t"] = np.float32(data_dict["4_float_t"])
        return pd.DataFrame(data=data_dict)

    def test_toPandas_fallback_enabled(self):
        import pandas as pd

        with self.sql_conf({"spark.sql.execution.arrow.fallback.enabled": True}):
            schema = StructType([StructField("map", MapType(StringType(), IntegerType()), True)])
            df = self.spark.createDataFrame([({u'a': 1},)], schema=schema)
            with QuietTest(self.sc):
                with self.warnings_lock:
                    with warnings.catch_warnings(record=True) as warns:
                        # we want the warnings to appear even if this test is run from a subclass
                        warnings.simplefilter("always")
                        pdf = df.toPandas()
                        # Catch and check the last UserWarning.
                        user_warns = [
                            warn.message for warn in warns if isinstance(warn.message, UserWarning)]
                        self.assertTrue(len(user_warns) > 0)
                        self.assertTrue(
                            "Attempting non-optimization" in _exception_message(user_warns[-1]))
                        self.assertPandasEqual(pdf, pd.DataFrame({u'map': [{u'a': 1}]}))

    def test_toPandas_fallback_disabled(self):
        from distutils.version import LooseVersion
        import pyarrow as pa

        schema = StructType([StructField("map", MapType(StringType(), IntegerType()), True)])
        df = self.spark.createDataFrame([(None,)], schema=schema)
        with QuietTest(self.sc):
            with self.warnings_lock:
                with self.assertRaisesRegexp(Exception, 'Unsupported type'):
                    df.toPandas()

        # TODO: remove BinaryType check once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            schema = StructType([StructField("binary", BinaryType(), True)])
            df = self.spark.createDataFrame([(None,)], schema=schema)
            with QuietTest(self.sc):
                with self.assertRaisesRegexp(Exception, 'Unsupported type.*BinaryType'):
                    df.toPandas()

    def test_null_conversion(self):
        df_null = self.spark.createDataFrame([tuple([None for _ in range(len(self.data[0]))])] +
                                             self.data)
        pdf = df_null.toPandas()
        null_counts = pdf.isnull().sum().tolist()
        self.assertTrue(all([c == 1 for c in null_counts]))

    def _toPandas_arrow_toggle(self, df):
        with self.sql_conf({"spark.sql.execution.arrow.enabled": False}):
            pdf = df.toPandas()

        pdf_arrow = df.toPandas()

        return pdf, pdf_arrow

    def test_toPandas_arrow_toggle(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
        expected = self.create_pandas_data_frame()
        self.assertPandasEqual(expected, pdf)
        self.assertPandasEqual(expected, pdf_arrow)

    def test_toPandas_respect_session_timezone(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)

        timezone = "America/New_York"
        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": False,
                "spark.sql.session.timeZone": timezone}):
            pdf_la, pdf_arrow_la = self._toPandas_arrow_toggle(df)
            self.assertPandasEqual(pdf_arrow_la, pdf_la)

        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": True,
                "spark.sql.session.timeZone": timezone}):
            pdf_ny, pdf_arrow_ny = self._toPandas_arrow_toggle(df)
            self.assertPandasEqual(pdf_arrow_ny, pdf_ny)

            self.assertFalse(pdf_ny.equals(pdf_la))

            from pyspark.sql.types import _check_series_convert_timestamps_local_tz
            pdf_la_corrected = pdf_la.copy()
            for field in self.schema:
                if isinstance(field.dataType, TimestampType):
                    pdf_la_corrected[field.name] = _check_series_convert_timestamps_local_tz(
                        pdf_la_corrected[field.name], timezone)
            self.assertPandasEqual(pdf_ny, pdf_la_corrected)

    def test_pandas_round_trip(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf_arrow = df.toPandas()
        self.assertPandasEqual(pdf_arrow, pdf)

    def test_filtered_frame(self):
        df = self.spark.range(3).toDF("i")
        pdf = df.filter("i < 0").toPandas()
        self.assertEqual(len(pdf.columns), 1)
        self.assertEqual(pdf.columns[0], "i")
        self.assertTrue(pdf.empty)

    def _createDataFrame_toggle(self, pdf, schema=None):
        with self.sql_conf({"spark.sql.execution.arrow.enabled": False}):
            df_no_arrow = self.spark.createDataFrame(pdf, schema=schema)

        df_arrow = self.spark.createDataFrame(pdf, schema=schema)

        return df_no_arrow, df_arrow

    def test_createDataFrame_toggle(self):
        pdf = self.create_pandas_data_frame()
        df_no_arrow, df_arrow = self._createDataFrame_toggle(pdf, schema=self.schema)
        self.assertEquals(df_no_arrow.collect(), df_arrow.collect())

    def test_createDataFrame_respect_session_timezone(self):
        from datetime import timedelta
        pdf = self.create_pandas_data_frame()
        timezone = "America/New_York"
        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": False,
                "spark.sql.session.timeZone": timezone}):
            df_no_arrow_la, df_arrow_la = self._createDataFrame_toggle(pdf, schema=self.schema)
            result_la = df_no_arrow_la.collect()
            result_arrow_la = df_arrow_la.collect()
            self.assertEqual(result_la, result_arrow_la)

        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": True,
                "spark.sql.session.timeZone": timezone}):
            df_no_arrow_ny, df_arrow_ny = self._createDataFrame_toggle(pdf, schema=self.schema)
            result_ny = df_no_arrow_ny.collect()
            result_arrow_ny = df_arrow_ny.collect()
            self.assertEqual(result_ny, result_arrow_ny)

            self.assertNotEqual(result_ny, result_la)

            # Correct result_la by adjusting 3 hours difference between Los Angeles and New York
            result_la_corrected = [Row(**{k: v - timedelta(hours=3) if k == '8_timestamp_t' else v
                                          for k, v in row.asDict().items()})
                                   for row in result_la]
            self.assertEqual(result_ny, result_la_corrected)

    def test_createDataFrame_with_schema(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertEquals(self.schema, df.schema)
        pdf_arrow = df.toPandas()
        self.assertPandasEqual(pdf_arrow, pdf)

    def test_createDataFrame_with_incorrect_schema(self):
        pdf = self.create_pandas_data_frame()
        fields = list(self.schema)
        fields[0], fields[7] = fields[7], fields[0]  # swap str with timestamp
        wrong_schema = StructType(fields)
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, ".*No cast.*string.*timestamp.*"):
                self.spark.createDataFrame(pdf, schema=wrong_schema)

    def test_createDataFrame_with_names(self):
        pdf = self.create_pandas_data_frame()
        new_names = list(map(str, range(len(self.schema.fieldNames()))))
        # Test that schema as a list of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=list(new_names))
        self.assertEquals(df.schema.fieldNames(), new_names)
        # Test that schema as tuple of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=tuple(new_names))
        self.assertEquals(df.schema.fieldNames(), new_names)

    def test_createDataFrame_column_name_encoding(self):
        import pandas as pd
        pdf = pd.DataFrame({u'a': [1]})
        columns = self.spark.createDataFrame(pdf).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEquals(columns[0], 'a')
        columns = self.spark.createDataFrame(pdf, [u'b']).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEquals(columns[0], 'b')

    def test_createDataFrame_with_single_data_type(self):
        import pandas as pd
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(ValueError, ".*IntegerType.*not supported.*"):
                self.spark.createDataFrame(pd.DataFrame({"a": [1]}), schema="int")

    def test_createDataFrame_does_not_modify_input(self):
        import pandas as pd
        # Some series get converted for Spark to consume, this makes sure input is unchanged
        pdf = self.create_pandas_data_frame()
        # Use a nanosecond value to make sure it is not truncated
        pdf.ix[0, '8_timestamp_t'] = pd.Timestamp(1)
        # Integers with nulls will get NaNs filled with 0 and will be casted
        pdf.ix[1, '2_int_t'] = None
        pdf_copy = pdf.copy(deep=True)
        self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertTrue(pdf.equals(pdf_copy))

    def test_schema_conversion_roundtrip(self):
        from pyspark.sql.types import from_arrow_schema, to_arrow_schema
        arrow_schema = to_arrow_schema(self.schema)
        schema_rt = from_arrow_schema(arrow_schema)
        self.assertEquals(self.schema, schema_rt)

    def test_createDataFrame_with_array_type(self):
        import pandas as pd
        pdf = pd.DataFrame({"a": [[1, 2], [3, 4]], "b": [[u"x", u"y"], [u"y", u"z"]]})
        df, df_arrow = self._createDataFrame_toggle(pdf)
        result = df.collect()
        result_arrow = df_arrow.collect()
        expected = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result_arrow[r][e] and
                                result[r][e] == result_arrow[r][e])

    def test_toPandas_with_array_type(self):
        expected = [([1, 2], [u"x", u"y"]), ([3, 4], [u"y", u"z"])]
        array_schema = StructType([StructField("a", ArrayType(IntegerType())),
                                   StructField("b", ArrayType(StringType()))])
        df = self.spark.createDataFrame(expected, schema=array_schema)
        pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
        result = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        result_arrow = [tuple(list(e) for e in rec) for rec in pdf_arrow.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result_arrow[r][e] and
                                result[r][e] == result_arrow[r][e])

    def test_createDataFrame_with_int_col_names(self):
        import numpy as np
        import pandas as pd
        pdf = pd.DataFrame(np.random.rand(4, 2))
        df, df_arrow = self._createDataFrame_toggle(pdf)
        pdf_col_names = [str(c) for c in pdf.columns]
        self.assertEqual(pdf_col_names, df.columns)
        self.assertEqual(pdf_col_names, df_arrow.columns)

    def test_createDataFrame_fallback_enabled(self):
        import pandas as pd

        with QuietTest(self.sc):
            with self.sql_conf({"spark.sql.execution.arrow.fallback.enabled": True}):
                with warnings.catch_warnings(record=True) as warns:
                    # we want the warnings to appear even if this test is run from a subclass
                    warnings.simplefilter("always")
                    df = self.spark.createDataFrame(
                        pd.DataFrame([[{u'a': 1}]]), "a: map<string, int>")
                    # Catch and check the last UserWarning.
                    user_warns = [
                        warn.message for warn in warns if isinstance(warn.message, UserWarning)]
                    self.assertTrue(len(user_warns) > 0)
                    self.assertTrue(
                        "Attempting non-optimization" in _exception_message(user_warns[-1]))
                    self.assertEqual(df.collect(), [Row(a={u'a': 1})])

    def test_createDataFrame_fallback_disabled(self):
        from distutils.version import LooseVersion
        import pandas as pd
        import pyarrow as pa

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(TypeError, 'Unsupported type'):
                self.spark.createDataFrame(
                    pd.DataFrame([[{u'a': 1}]]), "a: map<string, int>")

        # TODO: remove BinaryType check once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            with QuietTest(self.sc):
                with self.assertRaisesRegexp(TypeError, 'Unsupported type.*BinaryType'):
                    self.spark.createDataFrame(
                        pd.DataFrame([[{'a': b'aaa'}]]), "a: binary")

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        import pandas as pd
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [datetime.datetime(2015, 11, 1, 0, 30),
              datetime.datetime(2015, 11, 1, 1, 30),
              datetime.datetime(2015, 11, 1, 2, 30)]
        pdf = pd.DataFrame({'time': dt})

        df_from_python = self.spark.createDataFrame(dt, 'timestamp').toDF('time')
        df_from_pandas = self.spark.createDataFrame(pdf)

        self.assertPandasEqual(pdf, df_from_python.toPandas())
        self.assertPandasEqual(pdf, df_from_pandas.toPandas())


@unittest.skipIf(
    not _have_pandas or not _have_pyarrow,
    _pandas_requirement_message or _pyarrow_requirement_message)
class MaxResultArrowTests(unittest.TestCase):
    # These tests are separate as 'spark.driver.maxResultSize' configuration
    # is a static configuration to Spark context.

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession(SparkContext(
            'local[4]', cls.__name__, conf=SparkConf().set("spark.driver.maxResultSize", "10k")))

        # Explicitly enable Arrow and disable fallback.
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        cls.spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_exception_by_max_results(self):
        with self.assertRaisesRegexp(Exception, "is bigger than"):
            self.spark.range(0, 10000, 1, 100).toPandas()


class EncryptionArrowTests(ArrowTests):

    @classmethod
    def conf(cls):
        return super(EncryptionArrowTests, cls).conf().set("spark.io.encryption.enabled", "true")


@unittest.skipIf(
    not _have_pandas or not _have_pyarrow,
    _pandas_requirement_message or _pyarrow_requirement_message)
class PandasUDFTests(ReusedSQLTestCase):

    def test_pandas_udf_basic(self):
        from pyspark.rdd import PythonEvalType
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        udf = pandas_udf(lambda x: x, DoubleType())
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, DoubleType(), PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, 'double', PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, StructType([StructField("v", DoubleType())]),
                         PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, 'v double', PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, 'v double',
                         functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, returnType='v double',
                         functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

    def test_pandas_udf_decorator(self):
        from pyspark.rdd import PythonEvalType
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        from pyspark.sql.types import StructType, StructField, DoubleType

        @pandas_udf(DoubleType())
        def foo(x):
            return x
        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        @pandas_udf(returnType=DoubleType())
        def foo(x):
            return x
        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        schema = StructType([StructField("v", DoubleType())])

        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf('v double', PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf(returnType='double', functionType=PandasUDFType.SCALAR)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        @pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

    def test_udf_wrong_arg(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        with QuietTest(self.sc):
            with self.assertRaises(ParseException):
                @pandas_udf('blah')
                def foo(x):
                    return x
            with self.assertRaisesRegexp(ValueError, 'Invalid returnType.*None'):
                @pandas_udf(functionType=PandasUDFType.SCALAR)
                def foo(x):
                    return x
            with self.assertRaisesRegexp(ValueError, 'Invalid functionType'):
                @pandas_udf('double', 100)
                def foo(x):
                    return x

            with self.assertRaisesRegexp(ValueError, '0-arg pandas_udfs.*not.*supported'):
                pandas_udf(lambda: 1, LongType(), PandasUDFType.SCALAR)
            with self.assertRaisesRegexp(ValueError, '0-arg pandas_udfs.*not.*supported'):
                @pandas_udf(LongType(), PandasUDFType.SCALAR)
                def zero_with_type():
                    return 1

            with self.assertRaisesRegexp(TypeError, 'Invalid returnType'):
                @pandas_udf(returnType=PandasUDFType.GROUPED_MAP)
                def foo(df):
                    return df
            with self.assertRaisesRegexp(TypeError, 'Invalid returnType'):
                @pandas_udf(returnType='double', functionType=PandasUDFType.GROUPED_MAP)
                def foo(df):
                    return df
            with self.assertRaisesRegexp(ValueError, 'Invalid function'):
                @pandas_udf(returnType='k int, v double', functionType=PandasUDFType.GROUPED_MAP)
                def foo(k, v, w):
                    return k

    def test_stopiteration_in_udf(self):
        from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
        from py4j.protocol import Py4JJavaError

        def foo(x):
            raise StopIteration()

        def foofoo(x, y):
            raise StopIteration()

        exc_message = "Caught StopIteration thrown from user's code; failing the task"
        df = self.spark.range(0, 100)

        # plain udf (test for SPARK-23754)
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.withColumn('v', udf(foo)('id')).collect
        )

        # pandas scalar udf
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.withColumn(
                'v', pandas_udf(foo, 'double', PandasUDFType.SCALAR)('id')
            ).collect
        )

        # pandas grouped map
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.groupBy('id').apply(
                pandas_udf(foo, df.schema, PandasUDFType.GROUPED_MAP)
            ).collect
        )

        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.groupBy('id').apply(
                pandas_udf(foofoo, df.schema, PandasUDFType.GROUPED_MAP)
            ).collect
        )

        # pandas grouped agg
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.groupBy('id').agg(
                pandas_udf(foo, 'double', PandasUDFType.GROUPED_AGG)('id')
            ).collect
        )


@unittest.skipIf(
    not _have_pandas or not _have_pyarrow,
    _pandas_requirement_message or _pyarrow_requirement_message)
class ScalarPandasUDFTests(ReusedSQLTestCase):

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

    @property
    def nondeterministic_vectorized_udf(self):
        from pyspark.sql.functions import pandas_udf

        @pandas_udf('double')
        def random_udf(v):
            import pandas as pd
            import numpy as np
            return pd.Series(np.random.random(len(v)))
        random_udf = random_udf.asNondeterministic()
        return random_udf

    def test_pandas_udf_tokenize(self):
        from pyspark.sql.functions import pandas_udf
        tokenize = pandas_udf(lambda s: s.apply(lambda str: str.split(' ')),
                              ArrayType(StringType()))
        self.assertEqual(tokenize.returnType, ArrayType(StringType()))
        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual([Row(hi=[u'hi', u'boo']), Row(hi=[u'bye', u'boo'])], result.collect())

    def test_pandas_udf_nested_arrays(self):
        from pyspark.sql.functions import pandas_udf
        tokenize = pandas_udf(lambda s: s.apply(lambda str: [str.split(' ')]),
                              ArrayType(ArrayType(StringType())))
        self.assertEqual(tokenize.returnType, ArrayType(ArrayType(StringType())))
        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual([Row(hi=[[u'hi', u'boo']]), Row(hi=[[u'bye', u'boo']])], result.collect())

    def test_vectorized_udf_basic(self):
        from pyspark.sql.functions import pandas_udf, col, array
        df = self.spark.range(10).select(
            col('id').cast('string').alias('str'),
            col('id').cast('int').alias('int'),
            col('id').alias('long'),
            col('id').cast('float').alias('float'),
            col('id').cast('double').alias('double'),
            col('id').cast('decimal').alias('decimal'),
            col('id').cast('boolean').alias('bool'),
            array(col('id')).alias('array_long'))
        f = lambda x: x
        str_f = pandas_udf(f, StringType())
        int_f = pandas_udf(f, IntegerType())
        long_f = pandas_udf(f, LongType())
        float_f = pandas_udf(f, FloatType())
        double_f = pandas_udf(f, DoubleType())
        decimal_f = pandas_udf(f, DecimalType())
        bool_f = pandas_udf(f, BooleanType())
        array_long_f = pandas_udf(f, ArrayType(LongType()))
        res = df.select(str_f(col('str')), int_f(col('int')),
                        long_f(col('long')), float_f(col('float')),
                        double_f(col('double')), decimal_f('decimal'),
                        bool_f(col('bool')), array_long_f('array_long'))
        self.assertEquals(df.collect(), res.collect())

    def test_register_nondeterministic_vectorized_udf_basic(self):
        from pyspark.sql.functions import pandas_udf
        from pyspark.rdd import PythonEvalType
        import random
        random_pandas_udf = pandas_udf(
            lambda x: random.randint(6, 6) + x, IntegerType()).asNondeterministic()
        self.assertEqual(random_pandas_udf.deterministic, False)
        self.assertEqual(random_pandas_udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
        nondeterministic_pandas_udf = self.spark.catalog.registerFunction(
            "randomPandasUDF", random_pandas_udf)
        self.assertEqual(nondeterministic_pandas_udf.deterministic, False)
        self.assertEqual(nondeterministic_pandas_udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
        [row] = self.spark.sql("SELECT randomPandasUDF(1)").collect()
        self.assertEqual(row[0], 7)

    def test_vectorized_udf_null_boolean(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [(True,), (True,), (None,), (False,)]
        schema = StructType().add("bool", BooleanType())
        df = self.spark.createDataFrame(data, schema)
        bool_f = pandas_udf(lambda x: x, BooleanType())
        res = df.select(bool_f(col('bool')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_byte(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("byte", ByteType())
        df = self.spark.createDataFrame(data, schema)
        byte_f = pandas_udf(lambda x: x, ByteType())
        res = df.select(byte_f(col('byte')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_short(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("short", ShortType())
        df = self.spark.createDataFrame(data, schema)
        short_f = pandas_udf(lambda x: x, ShortType())
        res = df.select(short_f(col('short')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_int(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("int", IntegerType())
        df = self.spark.createDataFrame(data, schema)
        int_f = pandas_udf(lambda x: x, IntegerType())
        res = df.select(int_f(col('int')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_long(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("long", LongType())
        df = self.spark.createDataFrame(data, schema)
        long_f = pandas_udf(lambda x: x, LongType())
        res = df.select(long_f(col('long')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_float(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("float", FloatType())
        df = self.spark.createDataFrame(data, schema)
        float_f = pandas_udf(lambda x: x, FloatType())
        res = df.select(float_f(col('float')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_double(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("double", DoubleType())
        df = self.spark.createDataFrame(data, schema)
        double_f = pandas_udf(lambda x: x, DoubleType())
        res = df.select(double_f(col('double')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_decimal(self):
        from decimal import Decimal
        from pyspark.sql.functions import pandas_udf, col
        data = [(Decimal(3.0),), (Decimal(5.0),), (Decimal(-1.0),), (None,)]
        schema = StructType().add("decimal", DecimalType(38, 18))
        df = self.spark.createDataFrame(data, schema)
        decimal_f = pandas_udf(lambda x: x, DecimalType(38, 18))
        res = df.select(decimal_f(col('decimal')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_string(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [("foo",), (None,), ("bar",), ("bar",)]
        schema = StructType().add("str", StringType())
        df = self.spark.createDataFrame(data, schema)
        str_f = pandas_udf(lambda x: x, StringType())
        res = df.select(str_f(col('str')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_string_in_udf(self):
        from pyspark.sql.functions import pandas_udf, col
        import pandas as pd
        df = self.spark.range(10)
        str_f = pandas_udf(lambda x: pd.Series(map(str, x)), StringType())
        actual = df.select(str_f(col('id')))
        expected = df.select(col('id').cast('string'))
        self.assertEquals(expected.collect(), actual.collect())

    def test_vectorized_udf_datatype_string(self):
        from pyspark.sql.functions import pandas_udf, col
        df = self.spark.range(10).select(
            col('id').cast('string').alias('str'),
            col('id').cast('int').alias('int'),
            col('id').alias('long'),
            col('id').cast('float').alias('float'),
            col('id').cast('double').alias('double'),
            col('id').cast('decimal').alias('decimal'),
            col('id').cast('boolean').alias('bool'))
        f = lambda x: x
        str_f = pandas_udf(f, 'string')
        int_f = pandas_udf(f, 'integer')
        long_f = pandas_udf(f, 'long')
        float_f = pandas_udf(f, 'float')
        double_f = pandas_udf(f, 'double')
        decimal_f = pandas_udf(f, 'decimal(38, 18)')
        bool_f = pandas_udf(f, 'boolean')
        res = df.select(str_f(col('str')), int_f(col('int')),
                        long_f(col('long')), float_f(col('float')),
                        double_f(col('double')), decimal_f('decimal'),
                        bool_f(col('bool')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_binary(self):
        from distutils.version import LooseVersion
        import pyarrow as pa
        from pyspark.sql.functions import pandas_udf, col
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            with QuietTest(self.sc):
                with self.assertRaisesRegexp(
                        NotImplementedError,
                        'Invalid returnType.*scalar Pandas UDF.*BinaryType'):
                    pandas_udf(lambda x: x, BinaryType())
        else:
            data = [(bytearray(b"a"),), (None,), (bytearray(b"bb"),), (bytearray(b"ccc"),)]
            schema = StructType().add("binary", BinaryType())
            df = self.spark.createDataFrame(data, schema)
            str_f = pandas_udf(lambda x: x, BinaryType())
            res = df.select(str_f(col('binary')))
            self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_array_type(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [([1, 2],), ([3, 4],)]
        array_schema = StructType([StructField("array", ArrayType(IntegerType()))])
        df = self.spark.createDataFrame(data, schema=array_schema)
        array_f = pandas_udf(lambda x: x, ArrayType(IntegerType()))
        result = df.select(array_f(col('array')))
        self.assertEquals(df.collect(), result.collect())

    def test_vectorized_udf_null_array(self):
        from pyspark.sql.functions import pandas_udf, col
        data = [([1, 2],), (None,), (None,), ([3, 4],), (None,)]
        array_schema = StructType([StructField("array", ArrayType(IntegerType()))])
        df = self.spark.createDataFrame(data, schema=array_schema)
        array_f = pandas_udf(lambda x: x, ArrayType(IntegerType()))
        result = df.select(array_f(col('array')))
        self.assertEquals(df.collect(), result.collect())

    def test_vectorized_udf_complex(self):
        from pyspark.sql.functions import pandas_udf, col, expr
        df = self.spark.range(10).select(
            col('id').cast('int').alias('a'),
            col('id').cast('int').alias('b'),
            col('id').cast('double').alias('c'))
        add = pandas_udf(lambda x, y: x + y, IntegerType())
        power2 = pandas_udf(lambda x: 2 ** x, IntegerType())
        mul = pandas_udf(lambda x, y: x * y, DoubleType())
        res = df.select(add(col('a'), col('b')), power2(col('a')), mul(col('b'), col('c')))
        expected = df.select(expr('a + b'), expr('power(2, a)'), expr('b * c'))
        self.assertEquals(expected.collect(), res.collect())

    def test_vectorized_udf_exception(self):
        from pyspark.sql.functions import pandas_udf, col
        df = self.spark.range(10)
        raise_exception = pandas_udf(lambda x: x * (1 / 0), LongType())
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, 'division( or modulo)? by zero'):
                df.select(raise_exception(col('id'))).collect()

    def test_vectorized_udf_invalid_length(self):
        from pyspark.sql.functions import pandas_udf, col
        import pandas as pd
        df = self.spark.range(10)
        raise_exception = pandas_udf(lambda _: pd.Series(1), LongType())
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    Exception,
                    'Result vector from pandas_udf was not the required length'):
                df.select(raise_exception(col('id'))).collect()

    def test_vectorized_udf_chained(self):
        from pyspark.sql.functions import pandas_udf, col
        df = self.spark.range(10)
        f = pandas_udf(lambda x: x + 1, LongType())
        g = pandas_udf(lambda x: x - 1, LongType())
        res = df.select(g(f(col('id'))))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_wrong_return_type(self):
        from pyspark.sql.functions import pandas_udf, col
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid returnType.*scalar Pandas UDF.*MapType'):
                pandas_udf(lambda x: x * 1.0, MapType(LongType(), LongType()))

    def test_vectorized_udf_return_scalar(self):
        from pyspark.sql.functions import pandas_udf, col
        df = self.spark.range(10)
        f = pandas_udf(lambda x: 1.0, DoubleType())
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, 'Return.*type.*Series'):
                df.select(f(col('id'))).collect()

    def test_vectorized_udf_decorator(self):
        from pyspark.sql.functions import pandas_udf, col
        df = self.spark.range(10)

        @pandas_udf(returnType=LongType())
        def identity(x):
            return x
        res = df.select(identity(col('id')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_empty_partition(self):
        from pyspark.sql.functions import pandas_udf, col
        df = self.spark.createDataFrame(self.sc.parallelize([Row(id=1)], 2))
        f = pandas_udf(lambda x: x, LongType())
        res = df.select(f(col('id')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_varargs(self):
        from pyspark.sql.functions import pandas_udf, col
        df = self.spark.createDataFrame(self.sc.parallelize([Row(id=1)], 2))
        f = pandas_udf(lambda *v: v[0], LongType())
        res = df.select(f(col('id')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_unsupported_types(self):
        from pyspark.sql.functions import pandas_udf
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid returnType.*scalar Pandas UDF.*MapType'):
                pandas_udf(lambda x: x, MapType(StringType(), IntegerType()))

    def test_vectorized_udf_dates(self):
        from pyspark.sql.functions import pandas_udf, col
        from datetime import date
        schema = StructType().add("idx", LongType()).add("date", DateType())
        data = [(0, date(1969, 1, 1),),
                (1, date(2012, 2, 2),),
                (2, None,),
                (3, date(2100, 4, 4),)]
        df = self.spark.createDataFrame(data, schema=schema)

        date_copy = pandas_udf(lambda t: t, returnType=DateType())
        df = df.withColumn("date_copy", date_copy(col("date")))

        @pandas_udf(returnType=StringType())
        def check_data(idx, date, date_copy):
            import pandas as pd
            msgs = []
            is_equal = date.isnull()
            for i in range(len(idx)):
                if (is_equal[i] and data[idx[i]][1] is None) or \
                        date[i] == data[idx[i]][1]:
                    msgs.append(None)
                else:
                    msgs.append(
                        "date values are not equal (date='%s': data[%d][1]='%s')"
                        % (date[i], idx[i], data[idx[i]][1]))
            return pd.Series(msgs)

        result = df.withColumn("check_data",
                               check_data(col("idx"), col("date"), col("date_copy"))).collect()

        self.assertEquals(len(data), len(result))
        for i in range(len(result)):
            self.assertEquals(data[i][1], result[i][1])  # "date" col
            self.assertEquals(data[i][1], result[i][2])  # "date_copy" col
            self.assertIsNone(result[i][3])  # "check_data" col

    def test_vectorized_udf_timestamps(self):
        from pyspark.sql.functions import pandas_udf, col
        from datetime import datetime
        schema = StructType([
            StructField("idx", LongType(), True),
            StructField("timestamp", TimestampType(), True)])
        data = [(0, datetime(1969, 1, 1, 1, 1, 1)),
                (1, datetime(2012, 2, 2, 2, 2, 2)),
                (2, None),
                (3, datetime(2100, 3, 3, 3, 3, 3))]

        df = self.spark.createDataFrame(data, schema=schema)

        # Check that a timestamp passed through a pandas_udf will not be altered by timezone calc
        f_timestamp_copy = pandas_udf(lambda t: t, returnType=TimestampType())
        df = df.withColumn("timestamp_copy", f_timestamp_copy(col("timestamp")))

        @pandas_udf(returnType=StringType())
        def check_data(idx, timestamp, timestamp_copy):
            import pandas as pd
            msgs = []
            is_equal = timestamp.isnull()  # use this array to check values are equal
            for i in range(len(idx)):
                # Check that timestamps are as expected in the UDF
                if (is_equal[i] and data[idx[i]][1] is None) or \
                        timestamp[i].to_pydatetime() == data[idx[i]][1]:
                    msgs.append(None)
                else:
                    msgs.append(
                        "timestamp values are not equal (timestamp='%s': data[%d][1]='%s')"
                        % (timestamp[i], idx[i], data[idx[i]][1]))
            return pd.Series(msgs)

        result = df.withColumn("check_data", check_data(col("idx"), col("timestamp"),
                                                        col("timestamp_copy"))).collect()
        # Check that collection values are correct
        self.assertEquals(len(data), len(result))
        for i in range(len(result)):
            self.assertEquals(data[i][1], result[i][1])  # "timestamp" col
            self.assertEquals(data[i][1], result[i][2])  # "timestamp_copy" col
            self.assertIsNone(result[i][3])  # "check_data" col

    def test_vectorized_udf_return_timestamp_tz(self):
        from pyspark.sql.functions import pandas_udf, col
        import pandas as pd
        df = self.spark.range(10)

        @pandas_udf(returnType=TimestampType())
        def gen_timestamps(id):
            ts = [pd.Timestamp(i, unit='D', tz='America/Los_Angeles') for i in id]
            return pd.Series(ts)

        result = df.withColumn("ts", gen_timestamps(col("id"))).collect()
        spark_ts_t = TimestampType()
        for r in result:
            i, ts = r
            ts_tz = pd.Timestamp(i, unit='D', tz='America/Los_Angeles').to_pydatetime()
            expected = spark_ts_t.fromInternal(spark_ts_t.toInternal(ts_tz))
            self.assertEquals(expected, ts)

    def test_vectorized_udf_check_config(self):
        from pyspark.sql.functions import pandas_udf, col
        import pandas as pd
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 3}):
            df = self.spark.range(10, numPartitions=1)

            @pandas_udf(returnType=LongType())
            def check_records_per_batch(x):
                return pd.Series(x.size).repeat(x.size)

            result = df.select(check_records_per_batch(col("id"))).collect()
            for (r,) in result:
                self.assertTrue(r <= 3)

    def test_vectorized_udf_timestamps_respect_session_timezone(self):
        from pyspark.sql.functions import pandas_udf, col
        from datetime import datetime
        import pandas as pd
        schema = StructType([
            StructField("idx", LongType(), True),
            StructField("timestamp", TimestampType(), True)])
        data = [(1, datetime(1969, 1, 1, 1, 1, 1)),
                (2, datetime(2012, 2, 2, 2, 2, 2)),
                (3, None),
                (4, datetime(2100, 3, 3, 3, 3, 3))]
        df = self.spark.createDataFrame(data, schema=schema)

        f_timestamp_copy = pandas_udf(lambda ts: ts, TimestampType())
        internal_value = pandas_udf(
            lambda ts: ts.apply(lambda ts: ts.value if ts is not pd.NaT else None), LongType())

        timezone = "America/New_York"
        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": False,
                "spark.sql.session.timeZone": timezone}):
            df_la = df.withColumn("tscopy", f_timestamp_copy(col("timestamp"))) \
                .withColumn("internal_value", internal_value(col("timestamp")))
            result_la = df_la.select(col("idx"), col("internal_value")).collect()
            # Correct result_la by adjusting 3 hours difference between Los Angeles and New York
            diff = 3 * 60 * 60 * 1000 * 1000 * 1000
            result_la_corrected = \
                df_la.select(col("idx"), col("tscopy"), col("internal_value") + diff).collect()

        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": True,
                "spark.sql.session.timeZone": timezone}):
            df_ny = df.withColumn("tscopy", f_timestamp_copy(col("timestamp"))) \
                .withColumn("internal_value", internal_value(col("timestamp")))
            result_ny = df_ny.select(col("idx"), col("tscopy"), col("internal_value")).collect()

            self.assertNotEqual(result_ny, result_la)
            self.assertEqual(result_ny, result_la_corrected)

    def test_nondeterministic_vectorized_udf(self):
        # Test that nondeterministic UDFs are evaluated only once in chained UDF evaluations
        from pyspark.sql.functions import udf, pandas_udf, col

        @pandas_udf('double')
        def plus_ten(v):
            return v + 10
        random_udf = self.nondeterministic_vectorized_udf

        df = self.spark.range(10).withColumn('rand', random_udf(col('id')))
        result1 = df.withColumn('plus_ten(rand)', plus_ten(df['rand'])).toPandas()

        self.assertEqual(random_udf.deterministic, False)
        self.assertTrue(result1['plus_ten(rand)'].equals(result1['rand'] + 10))

    def test_nondeterministic_vectorized_udf_in_aggregate(self):
        from pyspark.sql.functions import pandas_udf, sum

        df = self.spark.range(10)
        random_udf = self.nondeterministic_vectorized_udf

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(AnalysisException, 'nondeterministic'):
                df.groupby(df.id).agg(sum(random_udf(df.id))).collect()
            with self.assertRaisesRegexp(AnalysisException, 'nondeterministic'):
                df.agg(sum(random_udf(df.id))).collect()

    def test_register_vectorized_udf_basic(self):
        from pyspark.rdd import PythonEvalType
        from pyspark.sql.functions import pandas_udf, col, expr
        df = self.spark.range(10).select(
            col('id').cast('int').alias('a'),
            col('id').cast('int').alias('b'))
        original_add = pandas_udf(lambda x, y: x + y, IntegerType())
        self.assertEqual(original_add.deterministic, True)
        self.assertEqual(original_add.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
        new_add = self.spark.catalog.registerFunction("add1", original_add)
        res1 = df.select(new_add(col('a'), col('b')))
        res2 = self.spark.sql(
            "SELECT add1(t.a, t.b) FROM (SELECT id as a, id as b FROM range(10)) t")
        expected = df.select(expr('a + b'))
        self.assertEquals(expected.collect(), res1.collect())
        self.assertEquals(expected.collect(), res2.collect())

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        from pyspark.sql.functions import pandas_udf
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [datetime.datetime(2015, 11, 1, 0, 30),
              datetime.datetime(2015, 11, 1, 1, 30),
              datetime.datetime(2015, 11, 1, 2, 30)]
        df = self.spark.createDataFrame(dt, 'timestamp').toDF('time')
        foo_udf = pandas_udf(lambda x: x, 'timestamp')
        result = df.withColumn('time', foo_udf(df.time))
        self.assertEquals(df.collect(), result.collect())

    @unittest.skipIf(sys.version_info[:2] < (3, 5), "Type hints are supported from Python 3.5.")
    def test_type_annotation(self):
        from pyspark.sql.functions import pandas_udf
        # Regression test to check if type hints can be used. See SPARK-23569.
        # Note that it throws an error during compilation in lower Python versions if 'exec'
        # is not used. Also, note that we explicitly use another dictionary to avoid modifications
        # in the current 'locals()'.
        #
        # Hyukjin: I think it's an ugly way to test issues about syntax specific in
        # higher versions of Python, which we shouldn't encourage. This was the last resort
        # I could come up with at that time.
        _locals = {}
        exec(
            "import pandas as pd\ndef noop(col: pd.Series) -> pd.Series: return col",
            _locals)
        df = self.spark.range(1).select(pandas_udf(f=_locals['noop'], returnType='bigint')('id'))
        self.assertEqual(df.first()[0], 0)

    def test_mixed_udf(self):
        import pandas as pd
        from pyspark.sql.functions import col, udf, pandas_udf

        df = self.spark.range(0, 1).toDF('v')

        # Test mixture of multiple UDFs and Pandas UDFs.

        @udf('int')
        def f1(x):
            assert type(x) == int
            return x + 1

        @pandas_udf('int')
        def f2(x):
            assert type(x) == pd.Series
            return x + 10

        @udf('int')
        def f3(x):
            assert type(x) == int
            return x + 100

        @pandas_udf('int')
        def f4(x):
            assert type(x) == pd.Series
            return x + 1000

        # Test single expression with chained UDFs
        df_chained_1 = df.withColumn('f2_f1', f2(f1(df['v'])))
        df_chained_2 = df.withColumn('f3_f2_f1', f3(f2(f1(df['v']))))
        df_chained_3 = df.withColumn('f4_f3_f2_f1', f4(f3(f2(f1(df['v'])))))
        df_chained_4 = df.withColumn('f4_f2_f1', f4(f2(f1(df['v']))))
        df_chained_5 = df.withColumn('f4_f3_f1', f4(f3(f1(df['v']))))

        expected_chained_1 = df.withColumn('f2_f1', df['v'] + 11)
        expected_chained_2 = df.withColumn('f3_f2_f1', df['v'] + 111)
        expected_chained_3 = df.withColumn('f4_f3_f2_f1', df['v'] + 1111)
        expected_chained_4 = df.withColumn('f4_f2_f1', df['v'] + 1011)
        expected_chained_5 = df.withColumn('f4_f3_f1', df['v'] + 1101)

        self.assertEquals(expected_chained_1.collect(), df_chained_1.collect())
        self.assertEquals(expected_chained_2.collect(), df_chained_2.collect())
        self.assertEquals(expected_chained_3.collect(), df_chained_3.collect())
        self.assertEquals(expected_chained_4.collect(), df_chained_4.collect())
        self.assertEquals(expected_chained_5.collect(), df_chained_5.collect())

        # Test multiple mixed UDF expressions in a single projection
        df_multi_1 = df \
            .withColumn('f1', f1(col('v'))) \
            .withColumn('f2', f2(col('v'))) \
            .withColumn('f3', f3(col('v'))) \
            .withColumn('f4', f4(col('v'))) \
            .withColumn('f2_f1', f2(col('f1'))) \
            .withColumn('f3_f1', f3(col('f1'))) \
            .withColumn('f4_f1', f4(col('f1'))) \
            .withColumn('f3_f2', f3(col('f2'))) \
            .withColumn('f4_f2', f4(col('f2'))) \
            .withColumn('f4_f3', f4(col('f3'))) \
            .withColumn('f3_f2_f1', f3(col('f2_f1'))) \
            .withColumn('f4_f2_f1', f4(col('f2_f1'))) \
            .withColumn('f4_f3_f1', f4(col('f3_f1'))) \
            .withColumn('f4_f3_f2', f4(col('f3_f2'))) \
            .withColumn('f4_f3_f2_f1', f4(col('f3_f2_f1')))

        # Test mixed udfs in a single expression
        df_multi_2 = df \
            .withColumn('f1', f1(col('v'))) \
            .withColumn('f2', f2(col('v'))) \
            .withColumn('f3', f3(col('v'))) \
            .withColumn('f4', f4(col('v'))) \
            .withColumn('f2_f1', f2(f1(col('v')))) \
            .withColumn('f3_f1', f3(f1(col('v')))) \
            .withColumn('f4_f1', f4(f1(col('v')))) \
            .withColumn('f3_f2', f3(f2(col('v')))) \
            .withColumn('f4_f2', f4(f2(col('v')))) \
            .withColumn('f4_f3', f4(f3(col('v')))) \
            .withColumn('f3_f2_f1', f3(f2(f1(col('v'))))) \
            .withColumn('f4_f2_f1', f4(f2(f1(col('v'))))) \
            .withColumn('f4_f3_f1', f4(f3(f1(col('v'))))) \
            .withColumn('f4_f3_f2', f4(f3(f2(col('v'))))) \
            .withColumn('f4_f3_f2_f1', f4(f3(f2(f1(col('v'))))))

        expected = df \
            .withColumn('f1', df['v'] + 1) \
            .withColumn('f2', df['v'] + 10) \
            .withColumn('f3', df['v'] + 100) \
            .withColumn('f4', df['v'] + 1000) \
            .withColumn('f2_f1', df['v'] + 11) \
            .withColumn('f3_f1', df['v'] + 101) \
            .withColumn('f4_f1', df['v'] + 1001) \
            .withColumn('f3_f2', df['v'] + 110) \
            .withColumn('f4_f2', df['v'] + 1010) \
            .withColumn('f4_f3', df['v'] + 1100) \
            .withColumn('f3_f2_f1', df['v'] + 111) \
            .withColumn('f4_f2_f1', df['v'] + 1011) \
            .withColumn('f4_f3_f1', df['v'] + 1101) \
            .withColumn('f4_f3_f2', df['v'] + 1110) \
            .withColumn('f4_f3_f2_f1', df['v'] + 1111)

        self.assertEquals(expected.collect(), df_multi_1.collect())
        self.assertEquals(expected.collect(), df_multi_2.collect())

    def test_mixed_udf_and_sql(self):
        import pandas as pd
        from pyspark.sql import Column
        from pyspark.sql.functions import udf, pandas_udf

        df = self.spark.range(0, 1).toDF('v')

        # Test mixture of UDFs, Pandas UDFs and SQL expression.

        @udf('int')
        def f1(x):
            assert type(x) == int
            return x + 1

        def f2(x):
            assert type(x) == Column
            return x + 10

        @pandas_udf('int')
        def f3(x):
            assert type(x) == pd.Series
            return x + 100

        df1 = df.withColumn('f1', f1(df['v'])) \
            .withColumn('f2', f2(df['v'])) \
            .withColumn('f3', f3(df['v'])) \
            .withColumn('f1_f2', f1(f2(df['v']))) \
            .withColumn('f1_f3', f1(f3(df['v']))) \
            .withColumn('f2_f1', f2(f1(df['v']))) \
            .withColumn('f2_f3', f2(f3(df['v']))) \
            .withColumn('f3_f1', f3(f1(df['v']))) \
            .withColumn('f3_f2', f3(f2(df['v']))) \
            .withColumn('f1_f2_f3', f1(f2(f3(df['v'])))) \
            .withColumn('f1_f3_f2', f1(f3(f2(df['v'])))) \
            .withColumn('f2_f1_f3', f2(f1(f3(df['v'])))) \
            .withColumn('f2_f3_f1', f2(f3(f1(df['v'])))) \
            .withColumn('f3_f1_f2', f3(f1(f2(df['v'])))) \
            .withColumn('f3_f2_f1', f3(f2(f1(df['v']))))

        expected = df.withColumn('f1', df['v'] + 1) \
            .withColumn('f2', df['v'] + 10) \
            .withColumn('f3', df['v'] + 100) \
            .withColumn('f1_f2', df['v'] + 11) \
            .withColumn('f1_f3', df['v'] + 101) \
            .withColumn('f2_f1', df['v'] + 11) \
            .withColumn('f2_f3', df['v'] + 110) \
            .withColumn('f3_f1', df['v'] + 101) \
            .withColumn('f3_f2', df['v'] + 110) \
            .withColumn('f1_f2_f3', df['v'] + 111) \
            .withColumn('f1_f3_f2', df['v'] + 111) \
            .withColumn('f2_f1_f3', df['v'] + 111) \
            .withColumn('f2_f3_f1', df['v'] + 111) \
            .withColumn('f3_f1_f2', df['v'] + 111) \
            .withColumn('f3_f2_f1', df['v'] + 111)

        self.assertEquals(expected.collect(), df1.collect())

    # SPARK-24721
    @unittest.skipIf(not _test_compiled, _test_not_compiled_message)
    def test_datasource_with_udf(self):
        # Same as SQLTests.test_datasource_with_udf, but with Pandas UDF
        # This needs to a separate test because Arrow dependency is optional
        import pandas as pd
        import numpy as np
        from pyspark.sql.functions import pandas_udf, lit, col

        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(1).write.mode("overwrite").format('csv').save(path)
            filesource_df = self.spark.read.option('inferSchema', True).csv(path).toDF('i')
            datasource_df = self.spark.read \
                .format("org.apache.spark.sql.sources.SimpleScanSource") \
                .option('from', 0).option('to', 1).load().toDF('i')
            datasource_v2_df = self.spark.read \
                .format("org.apache.spark.sql.sources.v2.SimpleDataSourceV2") \
                .load().toDF('i', 'j')

            c1 = pandas_udf(lambda x: x + 1, 'int')(lit(1))
            c2 = pandas_udf(lambda x: x + 1, 'int')(col('i'))

            f1 = pandas_udf(lambda x: pd.Series(np.repeat(False, len(x))), 'boolean')(lit(1))
            f2 = pandas_udf(lambda x: pd.Series(np.repeat(False, len(x))), 'boolean')(col('i'))

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn('c', c1)
                expected = df.withColumn('c', lit(2))
                self.assertEquals(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn('c', c2)
                expected = df.withColumn('c', col('i') + 1)
                self.assertEquals(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                for f in [f1, f2]:
                    result = df.filter(f)
                    self.assertEquals(0, result.count())
        finally:
            shutil.rmtree(path)


@unittest.skipIf(
    not _have_pandas or not _have_pyarrow,
    _pandas_requirement_message or _pyarrow_requirement_message)
class GroupedMapPandasUDFTests(ReusedSQLTestCase):

    @property
    def data(self):
        from pyspark.sql.functions import array, explode, col, lit
        return self.spark.range(10).toDF('id') \
            .withColumn("vs", array([lit(i) for i in range(20, 30)])) \
            .withColumn("v", explode(col('vs'))).drop('vs')

    def test_supported_types(self):
        from decimal import Decimal
        from distutils.version import LooseVersion
        import pyarrow as pa
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        values = [
            1, 2, 3,
            4, 5, 1.1,
            2.2, Decimal(1.123),
            [1, 2, 2], True, 'hello'
        ]
        output_fields = [
            ('id', IntegerType()), ('byte', ByteType()), ('short', ShortType()),
            ('int', IntegerType()), ('long', LongType()), ('float', FloatType()),
            ('double', DoubleType()), ('decim', DecimalType(10, 3)),
            ('array', ArrayType(IntegerType())), ('bool', BooleanType()), ('str', StringType())
        ]

        # TODO: Add BinaryType to variables above once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) >= LooseVersion("0.10.0"):
            values.append(bytearray([0x01, 0x02]))
            output_fields.append(('bin', BinaryType()))

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
                str=pdf.str + 'there',
                array=pdf.array,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP
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
                str=pdf.str + 'there',
                array=pdf.array,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP
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
                str=pdf.str + 'there',
                array=pdf.array,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP
        )

        result1 = df.groupby('id').apply(udf1).sort('id').toPandas()
        expected1 = df.toPandas().groupby('id').apply(udf1.func).reset_index(drop=True)

        result2 = df.groupby('id').apply(udf2).sort('id').toPandas()
        expected2 = expected1

        result3 = df.groupby('id').apply(udf3).sort('id').toPandas()
        expected3 = expected1

        self.assertPandasEqual(expected1, result1)
        self.assertPandasEqual(expected2, result2)
        self.assertPandasEqual(expected3, result3)

    def test_array_type_correct(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType, array, col

        df = self.data.withColumn("arr", array(col("id"))).repartition(1, "id")

        output_schema = StructType(
            [StructField('id', LongType()),
             StructField('v', IntegerType()),
             StructField('arr', ArrayType(LongType()))])

        udf = pandas_udf(
            lambda pdf: pdf,
            output_schema,
            PandasUDFType.GROUPED_MAP
        )

        result = df.groupby('id').apply(udf).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(udf.func).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

    def test_register_grouped_map_udf(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        foo_udf = pandas_udf(lambda x: x, "id long", PandasUDFType.GROUPED_MAP)
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    ValueError,
                    'f.*SQL_BATCHED_UDF.*SQL_SCALAR_PANDAS_UDF.*SQL_GROUPED_AGG_PANDAS_UDF.*'):
                self.spark.catalog.registerFunction("foo_udf", foo_udf)

    def test_decorator(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        df = self.data

        @pandas_udf(
            'id long, v int, v1 double, v2 long',
            PandasUDFType.GROUPED_MAP
        )
        def foo(pdf):
            return pdf.assign(v1=pdf.v * pdf.id * 1.0, v2=pdf.v + pdf.id)

        result = df.groupby('id').apply(foo).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(foo.func).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

    def test_coerce(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        df = self.data

        foo = pandas_udf(
            lambda pdf: pdf,
            'id long, v double',
            PandasUDFType.GROUPED_MAP
        )

        result = df.groupby('id').apply(foo).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(foo.func).reset_index(drop=True)
        expected = expected.assign(v=expected.v.astype('float64'))
        self.assertPandasEqual(expected, result)

    def test_complex_groupby(self):
        from pyspark.sql.functions import pandas_udf, col, PandasUDFType
        df = self.data

        @pandas_udf(
            'id long, v int, norm double',
            PandasUDFType.GROUPED_MAP
        )
        def normalize(pdf):
            v = pdf.v
            return pdf.assign(norm=(v - v.mean()) / v.std())

        result = df.groupby(col('id') % 2 == 0).apply(normalize).sort('id', 'v').toPandas()
        pdf = df.toPandas()
        expected = pdf.groupby(pdf['id'] % 2 == 0).apply(normalize.func)
        expected = expected.sort_values(['id', 'v']).reset_index(drop=True)
        expected = expected.assign(norm=expected.norm.astype('float64'))
        self.assertPandasEqual(expected, result)

    def test_empty_groupby(self):
        from pyspark.sql.functions import pandas_udf, col, PandasUDFType
        df = self.data

        @pandas_udf(
            'id long, v int, norm double',
            PandasUDFType.GROUPED_MAP
        )
        def normalize(pdf):
            v = pdf.v
            return pdf.assign(norm=(v - v.mean()) / v.std())

        result = df.groupby().apply(normalize).sort('id', 'v').toPandas()
        pdf = df.toPandas()
        expected = normalize.func(pdf)
        expected = expected.sort_values(['id', 'v']).reset_index(drop=True)
        expected = expected.assign(norm=expected.norm.astype('float64'))
        self.assertPandasEqual(expected, result)

    def test_datatype_string(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        df = self.data

        foo_udf = pandas_udf(
            lambda pdf: pdf.assign(v1=pdf.v * pdf.id * 1.0, v2=pdf.v + pdf.id),
            'id long, v int, v1 double, v2 long',
            PandasUDFType.GROUPED_MAP
        )

        result = df.groupby('id').apply(foo_udf).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(foo_udf.func).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

    def test_wrong_return_type(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid returnType.*grouped map Pandas UDF.*MapType'):
                pandas_udf(
                    lambda pdf: pdf,
                    'id long, v map<int, int>',
                    PandasUDFType.GROUPED_MAP)

    def test_wrong_args(self):
        from pyspark.sql.functions import udf, pandas_udf, sum, PandasUDFType
        df = self.data

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(lambda x: x)
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(udf(lambda x: x, DoubleType()))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(sum(df.v))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(df.v + 1)
            with self.assertRaisesRegexp(ValueError, 'Invalid function'):
                df.groupby('id').apply(
                    pandas_udf(lambda: 1, StructType([StructField("d", DoubleType())])))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(pandas_udf(lambda x, y: x, DoubleType()))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf.*GROUPED_MAP'):
                df.groupby('id').apply(
                    pandas_udf(lambda x, y: x, DoubleType(), PandasUDFType.SCALAR))

    def test_unsupported_types(self):
        from distutils.version import LooseVersion
        import pyarrow as pa
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        common_err_msg = 'Invalid returnType.*grouped map Pandas UDF.*'
        unsupported_types = [
            StructField('map', MapType(StringType(), IntegerType())),
            StructField('arr_ts', ArrayType(TimestampType())),
            StructField('null', NullType()),
        ]

        # TODO: Remove this if-statement once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            unsupported_types.append(StructField('bin', BinaryType()))

        for unsupported_type in unsupported_types:
            schema = StructType([StructField('id', LongType(), True), unsupported_type])
            with QuietTest(self.sc):
                with self.assertRaisesRegexp(NotImplementedError, common_err_msg):
                    pandas_udf(lambda x: x, schema, PandasUDFType.GROUPED_MAP)

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [datetime.datetime(2015, 11, 1, 0, 30),
              datetime.datetime(2015, 11, 1, 1, 30),
              datetime.datetime(2015, 11, 1, 2, 30)]
        df = self.spark.createDataFrame(dt, 'timestamp').toDF('time')
        foo_udf = pandas_udf(lambda pdf: pdf, 'time timestamp', PandasUDFType.GROUPED_MAP)
        result = df.groupby('time').apply(foo_udf).sort('time')
        self.assertPandasEqual(df.toPandas(), result.toPandas())

    def test_udf_with_key(self):
        from pyspark.sql.functions import pandas_udf, col, PandasUDFType
        df = self.data
        pdf = df.toPandas()

        def foo1(key, pdf):
            import numpy as np
            assert type(key) == tuple
            assert type(key[0]) == np.int64

            return pdf.assign(v1=key[0],
                              v2=pdf.v * key[0],
                              v3=pdf.v * pdf.id,
                              v4=pdf.v * pdf.id.mean())

        def foo2(key, pdf):
            import numpy as np
            assert type(key) == tuple
            assert type(key[0]) == np.int64
            assert type(key[1]) == np.int32

            return pdf.assign(v1=key[0],
                              v2=key[1],
                              v3=pdf.v * key[0],
                              v4=pdf.v + key[1])

        def foo3(key, pdf):
            assert type(key) == tuple
            assert len(key) == 0
            return pdf.assign(v1=pdf.v * pdf.id)

        # v2 is int because numpy.int64 * pd.Series<int32> results in pd.Series<int32>
        # v3 is long because pd.Series<int64> * pd.Series<int32> results in pd.Series<int64>
        udf1 = pandas_udf(
            foo1,
            'id long, v int, v1 long, v2 int, v3 long, v4 double',
            PandasUDFType.GROUPED_MAP)

        udf2 = pandas_udf(
            foo2,
            'id long, v int, v1 long, v2 int, v3 int, v4 int',
            PandasUDFType.GROUPED_MAP)

        udf3 = pandas_udf(
            foo3,
            'id long, v int, v1 long',
            PandasUDFType.GROUPED_MAP)

        # Test groupby column
        result1 = df.groupby('id').apply(udf1).sort('id', 'v').toPandas()
        expected1 = pdf.groupby('id')\
            .apply(lambda x: udf1.func((x.id.iloc[0],), x))\
            .sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected1, result1)

        # Test groupby expression
        result2 = df.groupby(df.id % 2).apply(udf1).sort('id', 'v').toPandas()
        expected2 = pdf.groupby(pdf.id % 2)\
            .apply(lambda x: udf1.func((x.id.iloc[0] % 2,), x))\
            .sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected2, result2)

        # Test complex groupby
        result3 = df.groupby(df.id, df.v % 2).apply(udf2).sort('id', 'v').toPandas()
        expected3 = pdf.groupby([pdf.id, pdf.v % 2])\
            .apply(lambda x: udf2.func((x.id.iloc[0], (x.v % 2).iloc[0],), x))\
            .sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected3, result3)

        # Test empty groupby
        result4 = df.groupby().apply(udf3).sort('id', 'v').toPandas()
        expected4 = udf3.func((), pdf)
        self.assertPandasEqual(expected4, result4)

    def test_column_order(self):
        from collections import OrderedDict
        import pandas as pd
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        # Helper function to set column names from a list
        def rename_pdf(pdf, names):
            pdf.rename(columns={old: new for old, new in
                                zip(pd_result.columns, names)}, inplace=True)

        df = self.data
        grouped_df = df.groupby('id')
        grouped_pdf = df.toPandas().groupby('id')

        # Function returns a pdf with required column names, but order could be arbitrary using dict
        def change_col_order(pdf):
            # Constructing a DataFrame from a dict should result in the same order,
            # but use from_items to ensure the pdf column order is different than schema
            return pd.DataFrame.from_items([
                ('id', pdf.id),
                ('u', pdf.v * 2),
                ('v', pdf.v)])

        ordered_udf = pandas_udf(
            change_col_order,
            'id long, v int, u int',
            PandasUDFType.GROUPED_MAP
        )

        # The UDF result should assign columns by name from the pdf
        result = grouped_df.apply(ordered_udf).sort('id', 'v')\
            .select('id', 'u', 'v').toPandas()
        pd_result = grouped_pdf.apply(change_col_order)
        expected = pd_result.sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

        # Function returns a pdf with positional columns, indexed by range
        def range_col_order(pdf):
            # Create a DataFrame with positional columns, fix types to long
            return pd.DataFrame(list(zip(pdf.id, pdf.v * 3, pdf.v)), dtype='int64')

        range_udf = pandas_udf(
            range_col_order,
            'id long, u long, v long',
            PandasUDFType.GROUPED_MAP
        )

        # The UDF result uses positional columns from the pdf
        result = grouped_df.apply(range_udf).sort('id', 'v') \
            .select('id', 'u', 'v').toPandas()
        pd_result = grouped_pdf.apply(range_col_order)
        rename_pdf(pd_result, ['id', 'u', 'v'])
        expected = pd_result.sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

        # Function returns a pdf with columns indexed with integers
        def int_index(pdf):
            return pd.DataFrame(OrderedDict([(0, pdf.id), (1, pdf.v * 4), (2, pdf.v)]))

        int_index_udf = pandas_udf(
            int_index,
            'id long, u int, v int',
            PandasUDFType.GROUPED_MAP
        )

        # The UDF result should assign columns by position of integer index
        result = grouped_df.apply(int_index_udf).sort('id', 'v') \
            .select('id', 'u', 'v').toPandas()
        pd_result = grouped_pdf.apply(int_index)
        rename_pdf(pd_result, ['id', 'u', 'v'])
        expected = pd_result.sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

        @pandas_udf('id long, v int', PandasUDFType.GROUPED_MAP)
        def column_name_typo(pdf):
            return pd.DataFrame({'iid': pdf.id, 'v': pdf.v})

        @pandas_udf('id long, v int', PandasUDFType.GROUPED_MAP)
        def invalid_positional_types(pdf):
            return pd.DataFrame([(u'a', 1.2)])

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, "KeyError: 'id'"):
                grouped_df.apply(column_name_typo).collect()
            from distutils.version import LooseVersion
            import pyarrow as pa
            if LooseVersion(pa.__version__) < LooseVersion("0.11.0"):
                # TODO: see ARROW-1949. Remove when the minimum PyArrow version becomes 0.11.0.
                with self.assertRaisesRegexp(Exception, "No cast implemented"):
                    grouped_df.apply(invalid_positional_types).collect()
            else:
                with self.assertRaisesRegexp(Exception, "an integer is required"):
                    grouped_df.apply(invalid_positional_types).collect()

    def test_positional_assignment_conf(self):
        import pandas as pd
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        with self.sql_conf({
                "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": False}):

            @pandas_udf("a string, b float", PandasUDFType.GROUPED_MAP)
            def foo(_):
                return pd.DataFrame([('hi', 1)], columns=['x', 'y'])

            df = self.data
            result = df.groupBy('id').apply(foo).select('a', 'b').collect()
            for r in result:
                self.assertEqual(r.a, 'hi')
                self.assertEqual(r.b, 1)

    def test_self_join_with_pandas(self):
        import pyspark.sql.functions as F

        @F.pandas_udf('key long, col string', F.PandasUDFType.GROUPED_MAP)
        def dummy_pandas_udf(df):
            return df[['key', 'col']]

        df = self.spark.createDataFrame([Row(key=1, col='A'), Row(key=1, col='B'),
                                         Row(key=2, col='C')])
        df_with_pandas = df.groupBy('key').apply(dummy_pandas_udf)

        # this was throwing an AnalysisException before SPARK-24208
        res = df_with_pandas.alias('temp0').join(df_with_pandas.alias('temp1'),
                                                 F.col('temp0.key') == F.col('temp1.key'))
        self.assertEquals(res.count(), 5)

    def test_mixed_scalar_udfs_followed_by_grouby_apply(self):
        import pandas as pd
        from pyspark.sql.functions import udf, pandas_udf, PandasUDFType

        df = self.spark.range(0, 10).toDF('v1')
        df = df.withColumn('v2', udf(lambda x: x + 1, 'int')(df['v1'])) \
            .withColumn('v3', pandas_udf(lambda x: x + 2, 'int')(df['v1']))

        result = df.groupby() \
            .apply(pandas_udf(lambda x: pd.DataFrame([x.sum().sum()]),
                              'sum int',
                              PandasUDFType.GROUPED_MAP))

        self.assertEquals(result.collect()[0]['sum'], 165)


@unittest.skipIf(
    not _have_pandas or not _have_pyarrow,
    _pandas_requirement_message or _pyarrow_requirement_message)
class GroupedAggPandasUDFTests(ReusedSQLTestCase):

    @property
    def data(self):
        from pyspark.sql.functions import array, explode, col, lit
        return self.spark.range(10).toDF('id') \
            .withColumn("vs", array([lit(i * 1.0) + col('id') for i in range(20, 30)])) \
            .withColumn("v", explode(col('vs'))) \
            .drop('vs') \
            .withColumn('w', lit(1.0))

    @property
    def python_plus_one(self):
        from pyspark.sql.functions import udf

        @udf('double')
        def plus_one(v):
            assert isinstance(v, (int, float))
            return v + 1
        return plus_one

    @property
    def pandas_scalar_plus_two(self):
        import pandas as pd
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.SCALAR)
        def plus_two(v):
            assert isinstance(v, pd.Series)
            return v + 2
        return plus_two

    @property
    def pandas_agg_mean_udf(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def avg(v):
            return v.mean()
        return avg

    @property
    def pandas_agg_sum_udf(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def sum(v):
            return v.sum()
        return sum

    @property
    def pandas_agg_weighted_mean_udf(self):
        import numpy as np
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def weighted_mean(v, w):
            return np.average(v, weights=w)
        return weighted_mean

    def test_manual(self):
        from pyspark.sql.functions import pandas_udf, array

        df = self.data
        sum_udf = self.pandas_agg_sum_udf
        mean_udf = self.pandas_agg_mean_udf
        mean_arr_udf = pandas_udf(
            self.pandas_agg_mean_udf.func,
            ArrayType(self.pandas_agg_mean_udf.returnType),
            self.pandas_agg_mean_udf.evalType)

        result1 = df.groupby('id').agg(
            sum_udf(df.v),
            mean_udf(df.v),
            mean_arr_udf(array(df.v))).sort('id')
        expected1 = self.spark.createDataFrame(
            [[0, 245.0, 24.5, [24.5]],
             [1, 255.0, 25.5, [25.5]],
             [2, 265.0, 26.5, [26.5]],
             [3, 275.0, 27.5, [27.5]],
             [4, 285.0, 28.5, [28.5]],
             [5, 295.0, 29.5, [29.5]],
             [6, 305.0, 30.5, [30.5]],
             [7, 315.0, 31.5, [31.5]],
             [8, 325.0, 32.5, [32.5]],
             [9, 335.0, 33.5, [33.5]]],
            ['id', 'sum(v)', 'avg(v)', 'avg(array(v))'])

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())

    def test_basic(self):
        from pyspark.sql.functions import col, lit, sum, mean

        df = self.data
        weighted_mean_udf = self.pandas_agg_weighted_mean_udf

        # Groupby one column and aggregate one UDF with literal
        result1 = df.groupby('id').agg(weighted_mean_udf(df.v, lit(1.0))).sort('id')
        expected1 = df.groupby('id').agg(mean(df.v).alias('weighted_mean(v, 1.0)')).sort('id')
        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())

        # Groupby one expression and aggregate one UDF with literal
        result2 = df.groupby((col('id') + 1)).agg(weighted_mean_udf(df.v, lit(1.0)))\
            .sort(df.id + 1)
        expected2 = df.groupby((col('id') + 1))\
            .agg(mean(df.v).alias('weighted_mean(v, 1.0)')).sort(df.id + 1)
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())

        # Groupby one column and aggregate one UDF without literal
        result3 = df.groupby('id').agg(weighted_mean_udf(df.v, df.w)).sort('id')
        expected3 = df.groupby('id').agg(mean(df.v).alias('weighted_mean(v, w)')).sort('id')
        self.assertPandasEqual(expected3.toPandas(), result3.toPandas())

        # Groupby one expression and aggregate one UDF without literal
        result4 = df.groupby((col('id') + 1).alias('id'))\
            .agg(weighted_mean_udf(df.v, df.w))\
            .sort('id')
        expected4 = df.groupby((col('id') + 1).alias('id'))\
            .agg(mean(df.v).alias('weighted_mean(v, w)'))\
            .sort('id')
        self.assertPandasEqual(expected4.toPandas(), result4.toPandas())

    def test_unsupported_types(self):
        from pyspark.sql.types import DoubleType, MapType
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(NotImplementedError, 'not supported'):
                pandas_udf(
                    lambda x: x,
                    ArrayType(ArrayType(TimestampType())),
                    PandasUDFType.GROUPED_AGG)

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(NotImplementedError, 'not supported'):
                @pandas_udf('mean double, std double', PandasUDFType.GROUPED_AGG)
                def mean_and_std_udf(v):
                    return v.mean(), v.std()

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(NotImplementedError, 'not supported'):
                @pandas_udf(MapType(DoubleType(), DoubleType()), PandasUDFType.GROUPED_AGG)
                def mean_and_std_udf(v):
                    return {v.mean(): v.std()}

    def test_alias(self):
        from pyspark.sql.functions import mean

        df = self.data
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.groupby('id').agg(mean_udf(df.v).alias('mean_alias'))
        expected1 = df.groupby('id').agg(mean(df.v).alias('mean_alias'))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())

    def test_mixed_sql(self):
        """
        Test mixing group aggregate pandas UDF with sql expression.
        """
        from pyspark.sql.functions import sum, mean

        df = self.data
        sum_udf = self.pandas_agg_sum_udf

        # Mix group aggregate pandas UDF with sql expression
        result1 = (df.groupby('id')
                   .agg(sum_udf(df.v) + 1)
                   .sort('id'))
        expected1 = (df.groupby('id')
                     .agg(sum(df.v) + 1)
                     .sort('id'))

        # Mix group aggregate pandas UDF with sql expression (order swapped)
        result2 = (df.groupby('id')
                     .agg(sum_udf(df.v + 1))
                     .sort('id'))

        expected2 = (df.groupby('id')
                       .agg(sum(df.v + 1))
                       .sort('id'))

        # Wrap group aggregate pandas UDF with two sql expressions
        result3 = (df.groupby('id')
                   .agg(sum_udf(df.v + 1) + 2)
                   .sort('id'))
        expected3 = (df.groupby('id')
                     .agg(sum(df.v + 1) + 2)
                     .sort('id'))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())
        self.assertPandasEqual(expected3.toPandas(), result3.toPandas())

    def test_mixed_udfs(self):
        """
        Test mixing group aggregate pandas UDF with python UDF and scalar pandas UDF.
        """
        from pyspark.sql.functions import sum, mean

        df = self.data
        plus_one = self.python_plus_one
        plus_two = self.pandas_scalar_plus_two
        sum_udf = self.pandas_agg_sum_udf

        # Mix group aggregate pandas UDF and python UDF
        result1 = (df.groupby('id')
                   .agg(plus_one(sum_udf(df.v)))
                   .sort('id'))
        expected1 = (df.groupby('id')
                     .agg(plus_one(sum(df.v)))
                     .sort('id'))

        # Mix group aggregate pandas UDF and python UDF (order swapped)
        result2 = (df.groupby('id')
                   .agg(sum_udf(plus_one(df.v)))
                   .sort('id'))
        expected2 = (df.groupby('id')
                     .agg(sum(plus_one(df.v)))
                     .sort('id'))

        # Mix group aggregate pandas UDF and scalar pandas UDF
        result3 = (df.groupby('id')
                   .agg(sum_udf(plus_two(df.v)))
                   .sort('id'))
        expected3 = (df.groupby('id')
                     .agg(sum(plus_two(df.v)))
                     .sort('id'))

        # Mix group aggregate pandas UDF and scalar pandas UDF (order swapped)
        result4 = (df.groupby('id')
                   .agg(plus_two(sum_udf(df.v)))
                   .sort('id'))
        expected4 = (df.groupby('id')
                     .agg(plus_two(sum(df.v)))
                     .sort('id'))

        # Wrap group aggregate pandas UDF with two python UDFs and use python UDF in groupby
        result5 = (df.groupby(plus_one(df.id))
                   .agg(plus_one(sum_udf(plus_one(df.v))))
                   .sort('plus_one(id)'))
        expected5 = (df.groupby(plus_one(df.id))
                     .agg(plus_one(sum(plus_one(df.v))))
                     .sort('plus_one(id)'))

        # Wrap group aggregate pandas UDF with two scala pandas UDF and user scala pandas UDF in
        # groupby
        result6 = (df.groupby(plus_two(df.id))
                   .agg(plus_two(sum_udf(plus_two(df.v))))
                   .sort('plus_two(id)'))
        expected6 = (df.groupby(plus_two(df.id))
                     .agg(plus_two(sum(plus_two(df.v))))
                     .sort('plus_two(id)'))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())
        self.assertPandasEqual(expected3.toPandas(), result3.toPandas())
        self.assertPandasEqual(expected4.toPandas(), result4.toPandas())
        self.assertPandasEqual(expected5.toPandas(), result5.toPandas())
        self.assertPandasEqual(expected6.toPandas(), result6.toPandas())

    def test_multiple_udfs(self):
        """
        Test multiple group aggregate pandas UDFs in one agg function.
        """
        from pyspark.sql.functions import col, lit, sum, mean

        df = self.data
        mean_udf = self.pandas_agg_mean_udf
        sum_udf = self.pandas_agg_sum_udf
        weighted_mean_udf = self.pandas_agg_weighted_mean_udf

        result1 = (df.groupBy('id')
                   .agg(mean_udf(df.v),
                        sum_udf(df.v),
                        weighted_mean_udf(df.v, df.w))
                   .sort('id')
                   .toPandas())
        expected1 = (df.groupBy('id')
                     .agg(mean(df.v),
                          sum(df.v),
                          mean(df.v).alias('weighted_mean(v, w)'))
                     .sort('id')
                     .toPandas())

        self.assertPandasEqual(expected1, result1)

    def test_complex_groupby(self):
        from pyspark.sql.functions import lit, sum

        df = self.data
        sum_udf = self.pandas_agg_sum_udf
        plus_one = self.python_plus_one
        plus_two = self.pandas_scalar_plus_two

        # groupby one expression
        result1 = df.groupby(df.v % 2).agg(sum_udf(df.v))
        expected1 = df.groupby(df.v % 2).agg(sum(df.v))

        # empty groupby
        result2 = df.groupby().agg(sum_udf(df.v))
        expected2 = df.groupby().agg(sum(df.v))

        # groupby one column and one sql expression
        result3 = df.groupby(df.id, df.v % 2).agg(sum_udf(df.v)).orderBy(df.id, df.v % 2)
        expected3 = df.groupby(df.id, df.v % 2).agg(sum(df.v)).orderBy(df.id, df.v % 2)

        # groupby one python UDF
        result4 = df.groupby(plus_one(df.id)).agg(sum_udf(df.v))
        expected4 = df.groupby(plus_one(df.id)).agg(sum(df.v))

        # groupby one scalar pandas UDF
        result5 = df.groupby(plus_two(df.id)).agg(sum_udf(df.v))
        expected5 = df.groupby(plus_two(df.id)).agg(sum(df.v))

        # groupby one expression and one python UDF
        result6 = df.groupby(df.v % 2, plus_one(df.id)).agg(sum_udf(df.v))
        expected6 = df.groupby(df.v % 2, plus_one(df.id)).agg(sum(df.v))

        # groupby one expression and one scalar pandas UDF
        result7 = df.groupby(df.v % 2, plus_two(df.id)).agg(sum_udf(df.v)).sort('sum(v)')
        expected7 = df.groupby(df.v % 2, plus_two(df.id)).agg(sum(df.v)).sort('sum(v)')

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())
        self.assertPandasEqual(expected3.toPandas(), result3.toPandas())
        self.assertPandasEqual(expected4.toPandas(), result4.toPandas())
        self.assertPandasEqual(expected5.toPandas(), result5.toPandas())
        self.assertPandasEqual(expected6.toPandas(), result6.toPandas())
        self.assertPandasEqual(expected7.toPandas(), result7.toPandas())

    def test_complex_expressions(self):
        from pyspark.sql.functions import col, sum

        df = self.data
        plus_one = self.python_plus_one
        plus_two = self.pandas_scalar_plus_two
        sum_udf = self.pandas_agg_sum_udf

        # Test complex expressions with sql expression, python UDF and
        # group aggregate pandas UDF
        result1 = (df.withColumn('v1', plus_one(df.v))
                   .withColumn('v2', df.v + 2)
                   .groupby(df.id, df.v % 2)
                   .agg(sum_udf(col('v')),
                        sum_udf(col('v1') + 3),
                        sum_udf(col('v2')) + 5,
                        plus_one(sum_udf(col('v1'))),
                        sum_udf(plus_one(col('v2'))))
                   .sort('id')
                   .toPandas())

        expected1 = (df.withColumn('v1', df.v + 1)
                     .withColumn('v2', df.v + 2)
                     .groupby(df.id, df.v % 2)
                     .agg(sum(col('v')),
                          sum(col('v1') + 3),
                          sum(col('v2')) + 5,
                          plus_one(sum(col('v1'))),
                          sum(plus_one(col('v2'))))
                     .sort('id')
                     .toPandas())

        # Test complex expressions with sql expression, scala pandas UDF and
        # group aggregate pandas UDF
        result2 = (df.withColumn('v1', plus_one(df.v))
                   .withColumn('v2', df.v + 2)
                   .groupby(df.id, df.v % 2)
                   .agg(sum_udf(col('v')),
                        sum_udf(col('v1') + 3),
                        sum_udf(col('v2')) + 5,
                        plus_two(sum_udf(col('v1'))),
                        sum_udf(plus_two(col('v2'))))
                   .sort('id')
                   .toPandas())

        expected2 = (df.withColumn('v1', df.v + 1)
                     .withColumn('v2', df.v + 2)
                     .groupby(df.id, df.v % 2)
                     .agg(sum(col('v')),
                          sum(col('v1') + 3),
                          sum(col('v2')) + 5,
                          plus_two(sum(col('v1'))),
                          sum(plus_two(col('v2'))))
                     .sort('id')
                     .toPandas())

        # Test sequential groupby aggregate
        result3 = (df.groupby('id')
                   .agg(sum_udf(df.v).alias('v'))
                   .groupby('id')
                   .agg(sum_udf(col('v')))
                   .sort('id')
                   .toPandas())

        expected3 = (df.groupby('id')
                     .agg(sum(df.v).alias('v'))
                     .groupby('id')
                     .agg(sum(col('v')))
                     .sort('id')
                     .toPandas())

        self.assertPandasEqual(expected1, result1)
        self.assertPandasEqual(expected2, result2)
        self.assertPandasEqual(expected3, result3)

    def test_retain_group_columns(self):
        from pyspark.sql.functions import sum, lit, col
        with self.sql_conf({"spark.sql.retainGroupColumns": False}):
            df = self.data
            sum_udf = self.pandas_agg_sum_udf

            result1 = df.groupby(df.id).agg(sum_udf(df.v))
            expected1 = df.groupby(df.id).agg(sum(df.v))
            self.assertPandasEqual(expected1.toPandas(), result1.toPandas())

    def test_array_type(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        df = self.data

        array_udf = pandas_udf(lambda x: [1.0, 2.0], 'array<double>', PandasUDFType.GROUPED_AGG)
        result1 = df.groupby('id').agg(array_udf(df['v']).alias('v2'))
        self.assertEquals(result1.first()['v2'], [1.0, 2.0])

    def test_invalid_args(self):
        from pyspark.sql.functions import mean

        df = self.data
        plus_one = self.python_plus_one
        mean_udf = self.pandas_agg_mean_udf

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    'nor.*aggregate function'):
                df.groupby(df.id).agg(plus_one(df.v)).collect()

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    'aggregate function.*argument.*aggregate function'):
                df.groupby(df.id).agg(mean_udf(mean_udf(df.v))).collect()

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    'mixture.*aggregate function.*group aggregate pandas UDF'):
                df.groupby(df.id).agg(mean_udf(df.v), mean(df.v)).collect()

    def test_register_vectorized_udf_basic(self):
        from pyspark.sql.functions import pandas_udf
        from pyspark.rdd import PythonEvalType

        sum_pandas_udf = pandas_udf(
            lambda v: v.sum(), "integer", PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)

        self.assertEqual(sum_pandas_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)
        group_agg_pandas_udf = self.spark.udf.register("sum_pandas_udf", sum_pandas_udf)
        self.assertEqual(group_agg_pandas_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)
        q = "SELECT sum_pandas_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
        actual = sorted(map(lambda r: r[0], self.spark.sql(q).collect()))
        expected = [1, 5]
        self.assertEqual(actual, expected)


@unittest.skipIf(
    not _have_pandas or not _have_pyarrow,
    _pandas_requirement_message or _pyarrow_requirement_message)
class WindowPandasUDFTests(ReusedSQLTestCase):
    @property
    def data(self):
        from pyspark.sql.functions import array, explode, col, lit
        return self.spark.range(10).toDF('id') \
            .withColumn("vs", array([lit(i * 1.0) + col('id') for i in range(20, 30)])) \
            .withColumn("v", explode(col('vs'))) \
            .drop('vs') \
            .withColumn('w', lit(1.0))

    @property
    def python_plus_one(self):
        from pyspark.sql.functions import udf
        return udf(lambda v: v + 1, 'double')

    @property
    def pandas_scalar_time_two(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        return pandas_udf(lambda v: v * 2, 'double')

    @property
    def pandas_agg_mean_udf(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def avg(v):
            return v.mean()
        return avg

    @property
    def pandas_agg_max_udf(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def max(v):
            return v.max()
        return max

    @property
    def pandas_agg_min_udf(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def min(v):
            return v.min()
        return min

    @property
    def unbounded_window(self):
        return Window.partitionBy('id') \
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    @property
    def ordered_window(self):
        return Window.partitionBy('id').orderBy('v')

    @property
    def unpartitioned_window(self):
        return Window.partitionBy()

    def test_simple(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType, percent_rank, mean, max

        df = self.data
        w = self.unbounded_window

        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn('mean_v', mean_udf(df['v']).over(w))
        expected1 = df.withColumn('mean_v', mean(df['v']).over(w))

        result2 = df.select(mean_udf(df['v']).over(w))
        expected2 = df.select(mean(df['v']).over(w))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())

    def test_multiple_udfs(self):
        from pyspark.sql.functions import max, min, mean

        df = self.data
        w = self.unbounded_window

        result1 = df.withColumn('mean_v', self.pandas_agg_mean_udf(df['v']).over(w)) \
                    .withColumn('max_v', self.pandas_agg_max_udf(df['v']).over(w)) \
                    .withColumn('min_w', self.pandas_agg_min_udf(df['w']).over(w))

        expected1 = df.withColumn('mean_v', mean(df['v']).over(w)) \
                      .withColumn('max_v', max(df['v']).over(w)) \
                      .withColumn('min_w', min(df['w']).over(w))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())

    def test_replace_existing(self):
        from pyspark.sql.functions import mean

        df = self.data
        w = self.unbounded_window

        result1 = df.withColumn('v', self.pandas_agg_mean_udf(df['v']).over(w))
        expected1 = df.withColumn('v', mean(df['v']).over(w))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())

    def test_mixed_sql(self):
        from pyspark.sql.functions import mean

        df = self.data
        w = self.unbounded_window
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn('v', mean_udf(df['v'] * 2).over(w) + 1)
        expected1 = df.withColumn('v', mean(df['v'] * 2).over(w) + 1)

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())

    def test_mixed_udf(self):
        from pyspark.sql.functions import mean

        df = self.data
        w = self.unbounded_window

        plus_one = self.python_plus_one
        time_two = self.pandas_scalar_time_two
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn(
            'v2',
            plus_one(mean_udf(plus_one(df['v'])).over(w)))
        expected1 = df.withColumn(
            'v2',
            plus_one(mean(plus_one(df['v'])).over(w)))

        result2 = df.withColumn(
            'v2',
            time_two(mean_udf(time_two(df['v'])).over(w)))
        expected2 = df.withColumn(
            'v2',
            time_two(mean(time_two(df['v'])).over(w)))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())

    def test_without_partitionBy(self):
        from pyspark.sql.functions import mean

        df = self.data
        w = self.unpartitioned_window
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn('v2', mean_udf(df['v']).over(w))
        expected1 = df.withColumn('v2', mean(df['v']).over(w))

        result2 = df.select(mean_udf(df['v']).over(w))
        expected2 = df.select(mean(df['v']).over(w))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())

    def test_mixed_sql_and_udf(self):
        from pyspark.sql.functions import max, min, rank, col

        df = self.data
        w = self.unbounded_window
        ow = self.ordered_window
        max_udf = self.pandas_agg_max_udf
        min_udf = self.pandas_agg_min_udf

        result1 = df.withColumn('v_diff', max_udf(df['v']).over(w) - min_udf(df['v']).over(w))
        expected1 = df.withColumn('v_diff', max(df['v']).over(w) - min(df['v']).over(w))

        # Test mixing sql window function and window udf in the same expression
        result2 = df.withColumn('v_diff', max_udf(df['v']).over(w) - min(df['v']).over(w))
        expected2 = expected1

        # Test chaining sql aggregate function and udf
        result3 = df.withColumn('max_v', max_udf(df['v']).over(w)) \
                    .withColumn('min_v', min(df['v']).over(w)) \
                    .withColumn('v_diff', col('max_v') - col('min_v')) \
                    .drop('max_v', 'min_v')
        expected3 = expected1

        # Test mixing sql window function and udf
        result4 = df.withColumn('max_v', max_udf(df['v']).over(w)) \
                    .withColumn('rank', rank().over(ow))
        expected4 = df.withColumn('max_v', max(df['v']).over(w)) \
                      .withColumn('rank', rank().over(ow))

        self.assertPandasEqual(expected1.toPandas(), result1.toPandas())
        self.assertPandasEqual(expected2.toPandas(), result2.toPandas())
        self.assertPandasEqual(expected3.toPandas(), result3.toPandas())
        self.assertPandasEqual(expected4.toPandas(), result4.toPandas())

    def test_array_type(self):
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        df = self.data
        w = self.unbounded_window

        array_udf = pandas_udf(lambda x: [1.0, 2.0], 'array<double>', PandasUDFType.GROUPED_AGG)
        result1 = df.withColumn('v2', array_udf(df['v']).over(w))
        self.assertEquals(result1.first()['v2'], [1.0, 2.0])

    def test_invalid_args(self):
        from pyspark.sql.functions import mean, pandas_udf, PandasUDFType

        df = self.data
        w = self.unbounded_window
        ow = self.ordered_window
        mean_udf = self.pandas_agg_mean_udf

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    '.*not supported within a window function'):
                foo_udf = pandas_udf(lambda x: x, 'v double', PandasUDFType.GROUPED_MAP)
                df.withColumn('v2', foo_udf(df['v']).over(w))

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    '.*Only unbounded window frame is supported.*'):
                df.withColumn('mean_v', mean_udf(df['v']).over(ow))


if __name__ == "__main__":
    from pyspark.sql.tests import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'), verbosity=2)
    else:
        unittest.main(verbosity=2)
