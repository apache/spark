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
import shutil
import sys
import tempfile
import time
import unittest

from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.window import Window
from pyspark.testing.sqlutils import ReusedSQLTestCase, test_compiled, test_not_compiled_message, \
    have_pandas, have_pyarrow, pandas_requirement_message, pyarrow_requirement_message
from pyspark.tests import QuietTest


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
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
        from pyspark.sql.functions import pandas_udf
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
        from pyspark.sql.functions import pandas_udf, col

        @pandas_udf('double')
        def plus_ten(v):
            return v + 10
        random_udf = self.nondeterministic_vectorized_udf

        df = self.spark.range(10).withColumn('rand', random_udf(col('id')))
        result1 = df.withColumn('plus_ten(rand)', plus_ten(df['rand'])).toPandas()

        self.assertEqual(random_udf.deterministic, False)
        self.assertTrue(result1['plus_ten(rand)'].equals(result1['rand'] + 10))

    def test_nondeterministic_vectorized_udf_in_aggregate(self):
        from pyspark.sql.functions import sum

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
    @unittest.skipIf(not test_compiled, test_not_compiled_message)
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
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
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
        from pyspark.sql.functions import pandas_udf, PandasUDFType
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
        from pyspark.sql.functions import pandas_udf, PandasUDFType
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
            with self.assertRaisesRegexp(Exception, "No cast implemented"):
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
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
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
        from pyspark.sql.functions import col, lit, mean

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
        from pyspark.sql.functions import sum

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
        from pyspark.sql.functions import sum

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
        from pyspark.sql.functions import sum, mean

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
        from pyspark.sql.functions import sum

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
        from pyspark.sql.functions import sum
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
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
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
        from pyspark.sql.functions import pandas_udf
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
        from pyspark.sql.functions import mean

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
        from pyspark.sql.functions import pandas_udf, PandasUDFType

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
    try:
        import xmlrunner
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'), verbosity=2)
    except ImportError:
        unittest.main(verbosity=2)
