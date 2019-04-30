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
import os
import random
import shutil
import sys
import tempfile
import time
import unittest

if sys.version >= '3':
    unicode = str

from datetime import date, datetime
from decimal import Decimal

from pyspark.rdd import PythonEvalType
from pyspark.sql import Column
from pyspark.sql.functions import array, col, expr, lit, sum, struct, udf, pandas_udf
from pyspark.sql.types import Row
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.testing.sqlutils import ReusedSQLTestCase, test_compiled,\
    test_not_compiled_message, have_pandas, have_pyarrow, pandas_requirement_message, \
    pyarrow_requirement_message
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd

if have_pyarrow:
    import pyarrow as pa


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
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
        import numpy as np

        @pandas_udf('double')
        def random_udf(v):
            return pd.Series(np.random.random(len(v)))
        random_udf = random_udf.asNondeterministic()
        return random_udf

    def test_pandas_udf_tokenize(self):
        tokenize = pandas_udf(lambda s: s.apply(lambda str: str.split(' ')),
                              ArrayType(StringType()))
        self.assertEqual(tokenize.returnType, ArrayType(StringType()))
        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual([Row(hi=[u'hi', u'boo']), Row(hi=[u'bye', u'boo'])], result.collect())

    def test_pandas_udf_nested_arrays(self):
        tokenize = pandas_udf(lambda s: s.apply(lambda str: [str.split(' ')]),
                              ArrayType(ArrayType(StringType())))
        self.assertEqual(tokenize.returnType, ArrayType(ArrayType(StringType())))
        df = self.spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
        result = df.select(tokenize("vals").alias("hi"))
        self.assertEqual([Row(hi=[[u'hi', u'boo']]), Row(hi=[[u'bye', u'boo']])], result.collect())

    def test_vectorized_udf_basic(self):
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
        data = [(True,), (True,), (None,), (False,)]
        schema = StructType().add("bool", BooleanType())
        df = self.spark.createDataFrame(data, schema)
        bool_f = pandas_udf(lambda x: x, BooleanType())
        res = df.select(bool_f(col('bool')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_byte(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("byte", ByteType())
        df = self.spark.createDataFrame(data, schema)
        byte_f = pandas_udf(lambda x: x, ByteType())
        res = df.select(byte_f(col('byte')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_short(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("short", ShortType())
        df = self.spark.createDataFrame(data, schema)
        short_f = pandas_udf(lambda x: x, ShortType())
        res = df.select(short_f(col('short')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_int(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("int", IntegerType())
        df = self.spark.createDataFrame(data, schema)
        int_f = pandas_udf(lambda x: x, IntegerType())
        res = df.select(int_f(col('int')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_long(self):
        data = [(None,), (2,), (3,), (4,)]
        schema = StructType().add("long", LongType())
        df = self.spark.createDataFrame(data, schema)
        long_f = pandas_udf(lambda x: x, LongType())
        res = df.select(long_f(col('long')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_float(self):
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("float", FloatType())
        df = self.spark.createDataFrame(data, schema)
        float_f = pandas_udf(lambda x: x, FloatType())
        res = df.select(float_f(col('float')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_double(self):
        data = [(3.0,), (5.0,), (-1.0,), (None,)]
        schema = StructType().add("double", DoubleType())
        df = self.spark.createDataFrame(data, schema)
        double_f = pandas_udf(lambda x: x, DoubleType())
        res = df.select(double_f(col('double')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_decimal(self):
        data = [(Decimal(3.0),), (Decimal(5.0),), (Decimal(-1.0),), (None,)]
        schema = StructType().add("decimal", DecimalType(38, 18))
        df = self.spark.createDataFrame(data, schema)
        decimal_f = pandas_udf(lambda x: x, DecimalType(38, 18))
        res = df.select(decimal_f(col('decimal')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_null_string(self):
        data = [("foo",), (None,), ("bar",), ("bar",)]
        schema = StructType().add("str", StringType())
        df = self.spark.createDataFrame(data, schema)
        str_f = pandas_udf(lambda x: x, StringType())
        res = df.select(str_f(col('str')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_string_in_udf(self):
        df = self.spark.range(10)
        str_f = pandas_udf(lambda x: pd.Series(map(str, x)), StringType())
        actual = df.select(str_f(col('id')))
        expected = df.select(col('id').cast('string'))
        self.assertEquals(expected.collect(), actual.collect())

    def test_vectorized_udf_datatype_string(self):
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
        data = [(bytearray(b"a"),), (None,), (bytearray(b"bb"),), (bytearray(b"ccc"),)]
        schema = StructType().add("binary", BinaryType())
        df = self.spark.createDataFrame(data, schema)
        str_f = pandas_udf(lambda x: x, BinaryType())
        res = df.select(str_f(col('binary')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_array_type(self):
        data = [([1, 2],), ([3, 4],)]
        array_schema = StructType([StructField("array", ArrayType(IntegerType()))])
        df = self.spark.createDataFrame(data, schema=array_schema)
        array_f = pandas_udf(lambda x: x, ArrayType(IntegerType()))
        result = df.select(array_f(col('array')))
        self.assertEquals(df.collect(), result.collect())

    def test_vectorized_udf_null_array(self):
        data = [([1, 2],), (None,), (None,), ([3, 4],), (None,)]
        array_schema = StructType([StructField("array", ArrayType(IntegerType()))])
        df = self.spark.createDataFrame(data, schema=array_schema)
        array_f = pandas_udf(lambda x: x, ArrayType(IntegerType()))
        result = df.select(array_f(col('array')))
        self.assertEquals(df.collect(), result.collect())

    def test_vectorized_udf_struct_type(self):
        df = self.spark.range(10)
        return_type = StructType([
            StructField('id', LongType()),
            StructField('str', StringType())])

        def func(id):
            return pd.DataFrame({'id': id, 'str': id.apply(unicode)})

        f = pandas_udf(func, returnType=return_type)

        expected = df.select(struct(col('id'), col('id').cast('string').alias('str'))
                             .alias('struct')).collect()

        actual = df.select(f(col('id')).alias('struct')).collect()
        self.assertEqual(expected, actual)

        g = pandas_udf(func, 'id: long, str: string')
        actual = df.select(g(col('id')).alias('struct')).collect()
        self.assertEqual(expected, actual)

        struct_f = pandas_udf(lambda x: x, return_type)
        actual = df.select(struct_f(struct(col('id'), col('id').cast('string').alias('str'))))
        self.assertEqual(expected, actual.collect())

    def test_vectorized_udf_struct_complex(self):
        df = self.spark.range(10)
        return_type = StructType([
            StructField('ts', TimestampType()),
            StructField('arr', ArrayType(LongType()))])

        @pandas_udf(returnType=return_type)
        def f(id):
            return pd.DataFrame({'ts': id.apply(lambda i: pd.Timestamp(i)),
                                 'arr': id.apply(lambda i: [i, i + 1])})

        actual = df.withColumn('f', f(col('id'))).collect()
        for i, row in enumerate(actual):
            id, f = row
            self.assertEqual(i, id)
            self.assertEqual(pd.Timestamp(i).to_pydatetime(), f[0])
            self.assertListEqual([i, i + 1], f[1])

    def test_vectorized_udf_nested_struct(self):
        nested_type = StructType([
            StructField('id', IntegerType()),
            StructField('nested', StructType([
                StructField('foo', StringType()),
                StructField('bar', FloatType())
            ]))
        ])

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    Exception,
                    'Invalid returnType with scalar Pandas UDFs'):
                pandas_udf(lambda x: x, returnType=nested_type)

    def test_vectorized_udf_complex(self):
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
        df = self.spark.range(10)
        raise_exception = pandas_udf(lambda x: x * (1 / 0), LongType())
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, 'division( or modulo)? by zero'):
                df.select(raise_exception(col('id'))).collect()

    def test_vectorized_udf_invalid_length(self):
        df = self.spark.range(10)
        raise_exception = pandas_udf(lambda _: pd.Series(1), LongType())
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    Exception,
                    'Result vector from pandas_udf was not the required length'):
                df.select(raise_exception(col('id'))).collect()

    def test_vectorized_udf_chained(self):
        df = self.spark.range(10)
        f = pandas_udf(lambda x: x + 1, LongType())
        g = pandas_udf(lambda x: x - 1, LongType())
        res = df.select(g(f(col('id'))))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_chained_struct_type(self):
        df = self.spark.range(10)
        return_type = StructType([
            StructField('id', LongType()),
            StructField('str', StringType())])

        @pandas_udf(return_type)
        def f(id):
            return pd.DataFrame({'id': id, 'str': id.apply(unicode)})

        g = pandas_udf(lambda x: x, return_type)

        expected = df.select(struct(col('id'), col('id').cast('string').alias('str'))
                             .alias('struct')).collect()

        actual = df.select(g(f(col('id'))).alias('struct')).collect()
        self.assertEqual(expected, actual)

    def test_vectorized_udf_wrong_return_type(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid returnType.*scalar Pandas UDF.*MapType'):
                pandas_udf(lambda x: x * 1.0, MapType(LongType(), LongType()))

    def test_vectorized_udf_return_scalar(self):
        df = self.spark.range(10)
        f = pandas_udf(lambda x: 1.0, DoubleType())
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, 'Return.*type.*Series'):
                df.select(f(col('id'))).collect()

    def test_vectorized_udf_decorator(self):
        df = self.spark.range(10)

        @pandas_udf(returnType=LongType())
        def identity(x):
            return x
        res = df.select(identity(col('id')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_empty_partition(self):
        df = self.spark.createDataFrame(self.sc.parallelize([Row(id=1)], 2))
        f = pandas_udf(lambda x: x, LongType())
        res = df.select(f(col('id')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_struct_with_empty_partition(self):
        df = self.spark.createDataFrame(self.sc.parallelize([Row(id=1)], 2))\
            .withColumn('name', lit('John Doe'))

        @pandas_udf("first string, last string")
        def split_expand(n):
            return n.str.split(expand=True)

        result = df.select(split_expand('name')).collect()
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual('John', row[0]['first'])
        self.assertEqual('Doe', row[0]['last'])

    def test_vectorized_udf_varargs(self):
        df = self.spark.createDataFrame(self.sc.parallelize([Row(id=1)], 2))
        f = pandas_udf(lambda *v: v[0], LongType())
        res = df.select(f(col('id')))
        self.assertEquals(df.collect(), res.collect())

    def test_vectorized_udf_unsupported_types(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid returnType.*scalar Pandas UDF.*MapType'):
                pandas_udf(lambda x: x, MapType(StringType(), IntegerType()))
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid returnType.*scalar Pandas UDF.*ArrayType.StructType'):
                pandas_udf(lambda x: x, ArrayType(StructType([StructField('a', IntegerType())])))

    def test_vectorized_udf_dates(self):
        schema = StructType().add("idx", LongType()).add("date", DateType())
        data = [(0, date(1969, 1, 1),),
                (1, date(2012, 2, 2),),
                (2, None,),
                (3, date(2100, 4, 4),),
                (4, date(2262, 4, 12),)]
        df = self.spark.createDataFrame(data, schema=schema)

        date_copy = pandas_udf(lambda t: t, returnType=DateType())
        df = df.withColumn("date_copy", date_copy(col("date")))

        @pandas_udf(returnType=StringType())
        def check_data(idx, date, date_copy):
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
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 3}):
            df = self.spark.range(10, numPartitions=1)

            @pandas_udf(returnType=LongType())
            def check_records_per_batch(x):
                return pd.Series(x.size).repeat(x.size)

            result = df.select(check_records_per_batch(col("id"))).collect()
            for (r,) in result:
                self.assertTrue(r <= 3)

    def test_vectorized_udf_timestamps_respect_session_timezone(self):
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
        @pandas_udf('double')
        def plus_ten(v):
            return v + 10
        random_udf = self.nondeterministic_vectorized_udf

        df = self.spark.range(10).withColumn('rand', random_udf(col('id')))
        result1 = df.withColumn('plus_ten(rand)', plus_ten(df['rand'])).toPandas()

        self.assertEqual(random_udf.deterministic, False)
        self.assertTrue(result1['plus_ten(rand)'].equals(result1['rand'] + 10))

    def test_nondeterministic_vectorized_udf_in_aggregate(self):
        df = self.spark.range(10)
        random_udf = self.nondeterministic_vectorized_udf

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(AnalysisException, 'nondeterministic'):
                df.groupby(df.id).agg(sum(random_udf(df.id))).collect()
            with self.assertRaisesRegexp(AnalysisException, 'nondeterministic'):
                df.agg(sum(random_udf(df.id))).collect()

    def test_register_vectorized_udf_basic(self):
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
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [datetime(2015, 11, 1, 0, 30),
              datetime(2015, 11, 1, 1, 30),
              datetime(2015, 11, 1, 2, 30)]
        df = self.spark.createDataFrame(dt, 'timestamp').toDF('time')
        foo_udf = pandas_udf(lambda x: x, 'timestamp')
        result = df.withColumn('time', foo_udf(df.time))
        self.assertEquals(df.collect(), result.collect())

    @unittest.skipIf(sys.version_info[:2] < (3, 5), "Type hints are supported from Python 3.5.")
    def test_type_annotation(self):
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
    @unittest.skipIf(not test_compiled, test_not_compiled_message)
    def test_datasource_with_udf(self):
        # Same as SQLTests.test_datasource_with_udf, but with Pandas UDF
        # This needs to a separate test because Arrow dependency is optional
        import numpy as np

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


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_udf_scalar import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
