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
import sys

from pyspark.sql import Row
from pyspark.sql.functions import udf, input_file_name
from pyspark.testing.sqlutils import ReusedSQLTestCase


class FunctionsTests(ReusedSQLTestCase):

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

        with self.tempView("temp"):
            df.createOrReplaceTempView("temp")
            df = self.spark.sql("select foo from temp")
            df.count()
            df.collect()

    def test_corr(self):
        import math
        df = self.sc.parallelize([Row(a=i, b=math.sqrt(i)) for i in range(10)]).toDF()
        corr = df.stat.corr(u"a", "b")
        self.assertTrue(abs(corr - 0.95734012) < 1e-6)

    def test_sampleby(self):
        df = self.sc.parallelize([Row(a=i, b=(i % 3)) for i in range(100)]).toDF()
        sampled = df.stat.sampleBy(u"b", fractions={0: 0.5, 1: 0.5}, seed=0)
        self.assertTrue(sampled.count() == 35)

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
        assert_close([math.hypot(i, 2 * i) for i in range(10)],
                     df.select(functions.hypot("a", u"b")).collect())
        assert_close([math.hypot(i, 2) for i in range(10)],
                     df.select(functions.hypot("a", 2)).collect())
        assert_close([math.hypot(i, 2) for i in range(10)],
                     df.select(functions.hypot(df.a, 2)).collect())

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
        from pyspark.sql import functions
        from pyspark.sql.functions import col, lit, _string_functions
        df = self.spark.createDataFrame([['nick']], schema=['name'])
        self.assertRaisesRegexp(
            TypeError,
            "must be the same type",
            lambda: df.select(col('name').substr(0, lit(1))))
        if sys.version_info.major == 2:
            self.assertRaises(
                TypeError,
                lambda: df.select(col('name').substr(long(0), long(1))))

        for name in _string_functions.keys():
            self.assertEqual(
                df.select(getattr(functions, name)("name")).first()[0],
                df.select(getattr(functions, name)(col("name"))).first()[0])

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

    def test_dayofweek(self):
        from pyspark.sql.functions import dayofweek
        dt = datetime.datetime(2017, 11, 6)
        df = self.spark.createDataFrame([Row(date=dt)])
        row = df.select(dayofweek(df.date)).first()
        self.assertEqual(row[0], 2)

    def test_expr(self):
        from pyspark.sql import functions
        row = Row(a="length string", b=75)
        df = self.spark.createDataFrame([row])
        result = df.select(functions.expr("length(a)")).collect()[0].asDict()
        self.assertEqual(13, result["length(a)"])

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

    def test_array_repeat(self):
        from pyspark.sql.functions import array_repeat, lit

        df = self.spark.range(1)

        self.assertEquals(
            df.select(array_repeat("id", 3)).toDF("val").collect(),
            df.select(array_repeat("id", lit(3))).toDF("val").collect(),
        )

    def test_input_file_name_udf(self):
        df = self.spark.read.text('python/test_support/hello/hello.txt')
        df = df.select(udf(lambda x: x)("value"), input_file_name().alias('file'))
        file_name = df.collect()[0].file
        self.assertTrue("python/test_support/hello/hello.txt" in file_name)

    def test_overlay(self):
        from pyspark.sql.functions import col, lit, overlay
        from itertools import chain
        import re

        actual = list(chain.from_iterable([
            re.findall("(overlay\\(.*\\))", str(x)) for x in [
                overlay(col("foo"), col("bar"), 1),
                overlay("x", "y", 3),
                overlay(col("x"), col("y"), 1, 3),
                overlay("x", "y", 2, 5),
                overlay("x", "y", lit(11)),
                overlay("x", "y", lit(2), lit(5)),
            ]
        ]))

        expected = [
            "overlay(foo, bar, 1, -1)",
            "overlay(x, y, 3, -1)",
            "overlay(x, y, 1, 3)",
            "overlay(x, y, 2, 5)",
            "overlay(x, y, 11, -1)",
            "overlay(x, y, 2, 5)",
        ]

        self.assertListEqual(actual, expected)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_functions import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
