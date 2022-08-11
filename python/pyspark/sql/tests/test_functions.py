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
from inspect import getmembers, isfunction
from itertools import chain
import re
import math

from py4j.protocol import Py4JJavaError
from pyspark.sql import Row, Window, types
from pyspark.sql.functions import (
    udf,
    input_file_name,
    col,
    percentile_approx,
    lit,
    assert_true,
    sum_distinct,
    sumDistinct,
    shiftleft,
    shiftLeft,
    shiftRight,
    shiftright,
    shiftrightunsigned,
    shiftRightUnsigned,
    octet_length,
    bit_length,
    sec,
    csc,
    cot,
    make_date,
    date_add,
    date_sub,
    add_months,
    array_repeat,
    size,
    slice,
    least,
)
from pyspark.sql import functions
from pyspark.testing.sqlutils import ReusedSQLTestCase, SQLTestUtils


class FunctionsTests(ReusedSQLTestCase):
    def test_function_parity(self):
        # This test compares the available list of functions in pyspark.sql.functions with those
        # available in the Scala/Java DataFrame API in org.apache.spark.sql.functions.
        #
        # NOTE FOR DEVELOPERS:
        # If this test fails one of the following needs to happen
        # * If a function was added to org.apache.spark.sql.functions it either needs to be added to
        #     pyspark.sql.functions or added to the below expected_missing_in_py set.
        # * If a function was added to pyspark.sql.functions that was already in
        #     org.apache.spark.sql.functions then it needs to be removed from expected_missing_in_py
        #     below. If the function has a different name it needs to be added to py_equiv_jvm
        #     mapping.
        # * If it's not related to an added/removed function then likely the exclusion list
        #     jvm_excluded_fn needs to be updated.

        jvm_fn_set = {name for (name, value) in getmembers(self.sc._jvm.functions)}
        py_fn_set = {name for (name, value) in getmembers(functions, isfunction) if name[0] != "_"}

        # Functions on the JVM side we do not expect to be available in python because they are
        # depreciated, irrelevant to python, or have equivalents.
        jvm_excluded_fn = [
            "callUDF",  # depreciated, use call_udf
            "typedlit",  # Scala only
            "typedLit",  # Scala only
            "monotonicallyIncreasingId",  # depreciated, use monotonically_increasing_id
            "negate",  # equivalent to python -expression
            "not",  # equivalent to python ~expression
            "udaf",  # used for creating UDAF's which are not supported in PySpark
        ]

        jvm_fn_set.difference_update(jvm_excluded_fn)

        # For functions that are named differently in pyspark this is the mapping of their
        # python name to the JVM equivalent
        py_equiv_jvm = {"create_map": "map"}
        for py_name, jvm_name in py_equiv_jvm.items():
            if py_name in py_fn_set:
                py_fn_set.remove(py_name)
                py_fn_set.add(jvm_name)

        missing_in_py = jvm_fn_set.difference(py_fn_set)

        # Functions that we expect to be missing in python until they are added to pyspark
        expected_missing_in_py = set()

        self.assertEqual(
            expected_missing_in_py, missing_in_py, "Missing functions in pyspark not as expected"
        )

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
        self.assertEqual(result, [(0, "a", "b"), (None, None, None), (None, None, None)])

        result = [x[0] for x in data.select(explode_outer("intlist")).collect()]
        self.assertEqual(result, [1, 2, 3, None, None])

        result = [tuple(x) for x in data.select(explode_outer("mapfield")).collect()]
        self.assertEqual(result, [("a", "b"), (None, None), (None, None)])

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
        corr = df.stat.corr("a", "b")
        self.assertTrue(abs(corr - 0.95734012) < 1e-6)

    def test_sampleby(self):
        df = self.sc.parallelize([Row(a=i, b=(i % 3)) for i in range(100)]).toDF()
        sampled = df.stat.sampleBy("b", fractions={0: 0.5, 1: 0.5}, seed=0)
        self.assertTrue(sampled.count() == 35)

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

        SQLTestUtils.assert_close(
            [math.cos(i) for i in range(10)], df.select(functions.cos(df.a)).collect()
        )
        SQLTestUtils.assert_close(
            [math.cos(i) for i in range(10)], df.select(functions.cos("a")).collect()
        )
        SQLTestUtils.assert_close(
            [math.sin(i) for i in range(10)], df.select(functions.sin(df.a)).collect()
        )
        SQLTestUtils.assert_close(
            [math.sin(i) for i in range(10)], df.select(functions.sin(df["a"])).collect()
        )
        SQLTestUtils.assert_close(
            [math.pow(i, 2 * i) for i in range(10)], df.select(functions.pow(df.a, df.b)).collect()
        )
        SQLTestUtils.assert_close(
            [math.pow(i, 2) for i in range(10)], df.select(functions.pow(df.a, 2)).collect()
        )
        SQLTestUtils.assert_close(
            [math.pow(i, 2) for i in range(10)], df.select(functions.pow(df.a, 2.0)).collect()
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2 * i) for i in range(10)],
            df.select(functions.hypot(df.a, df.b)).collect(),
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2 * i) for i in range(10)],
            df.select(functions.hypot("a", "b")).collect(),
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2) for i in range(10)], df.select(functions.hypot("a", 2)).collect()
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2) for i in range(10)], df.select(functions.hypot(df.a, 2)).collect()
        )

    def test_inverse_trig_functions(self):
        from pyspark.sql import functions

        funs = [
            (functions.acosh, "ACOSH"),
            (functions.asinh, "ASINH"),
            (functions.atanh, "ATANH"),
        ]

        cols = ["a", functions.col("a")]

        for f, alias in funs:
            for c in cols:
                self.assertIn(f"{alias}(a)", repr(f(c)))

    def test_reciprocal_trig_functions(self):
        # SPARK-36683: Tests for reciprocal trig functions (SEC, CSC and COT)
        lst = [
            0.0,
            math.pi / 6,
            math.pi / 4,
            math.pi / 3,
            math.pi / 2,
            math.pi,
            3 * math.pi / 2,
            2 * math.pi,
        ]

        df = self.spark.createDataFrame(lst, types.DoubleType())

        def to_reciprocal_trig(func):
            return [1.0 / func(i) if func(i) != 0 else math.inf for i in lst]

        SQLTestUtils.assert_close(to_reciprocal_trig(math.cos), df.select(sec(df.value)).collect())
        SQLTestUtils.assert_close(to_reciprocal_trig(math.sin), df.select(csc(df.value)).collect())
        SQLTestUtils.assert_close(to_reciprocal_trig(math.tan), df.select(cot(df.value)).collect())

    def test_rand_functions(self):
        df = self.df
        from pyspark.sql import functions

        rnd = df.select("key", functions.rand()).collect()
        for row in rnd:
            assert row[1] >= 0.0 and row[1] <= 1.0, "got: %s" % row[1]
        rndn = df.select("key", functions.randn(5)).collect()
        for row in rndn:
            assert row[1] >= -4.0 and row[1] <= 4.0, "got: %s" % row[1]

        # If the specified seed is 0, we should use it.
        # https://issues.apache.org/jira/browse/SPARK-9691
        rnd1 = df.select("key", functions.rand(0)).collect()
        rnd2 = df.select("key", functions.rand(0)).collect()
        self.assertEqual(sorted(rnd1), sorted(rnd2))

        rndn1 = df.select("key", functions.randn(0)).collect()
        rndn2 = df.select("key", functions.randn(0)).collect()
        self.assertEqual(sorted(rndn1), sorted(rndn2))

    def test_string_functions(self):
        from pyspark.sql import functions
        from pyspark.sql.functions import col, lit

        string_functions = [
            "upper",
            "lower",
            "ascii",
            "base64",
            "unbase64",
            "ltrim",
            "rtrim",
            "trim",
        ]

        df = self.spark.createDataFrame([["nick"]], schema=["name"])
        self.assertRaisesRegex(
            TypeError, "must be the same type", lambda: df.select(col("name").substr(0, lit(1)))
        )

        for name in string_functions:
            self.assertEqual(
                df.select(getattr(functions, name)("name")).first()[0],
                df.select(getattr(functions, name)(col("name"))).first()[0],
            )

    def test_octet_length_function(self):
        # SPARK-36751: add octet length api for python
        df = self.spark.createDataFrame([("cat",), ("\U0001F408",)], ["cat"])
        actual = df.select(octet_length("cat")).collect()
        self.assertEqual([Row(3), Row(4)], actual)

    def test_bit_length_function(self):
        # SPARK-36751: add bit length api for python
        df = self.spark.createDataFrame([("cat",), ("\U0001F408",)], ["cat"])
        actual = df.select(bit_length("cat")).collect()
        self.assertEqual([Row(24), Row(32)], actual)

    def test_array_contains_function(self):
        from pyspark.sql.functions import array_contains

        df = self.spark.createDataFrame([(["1", "2", "3"],), ([],)], ["data"])
        actual = df.select(array_contains(df.data, "1").alias("b")).collect()
        self.assertEqual([Row(b=True), Row(b=False)], actual)

    def test_between_function(self):
        df = self.sc.parallelize(
            [Row(a=1, b=2, c=3), Row(a=2, b=1, c=3), Row(a=4, b=1, c=4)]
        ).toDF()
        self.assertEqual(
            [Row(a=2, b=1, c=3), Row(a=4, b=1, c=4)], df.filter(df.a.between(df.b, df.c)).collect()
        )

    def test_dayofweek(self):
        from pyspark.sql.functions import dayofweek

        dt = datetime.datetime(2017, 11, 6)
        df = self.spark.createDataFrame([Row(date=dt)])
        row = df.select(dayofweek(df.date)).first()
        self.assertEqual(row[0], 2)

    # Test added for SPARK-37738; change Python API to accept both col & int as input
    def test_date_add_function(self):
        dt = datetime.date(2021, 12, 27)

        # Note; number var in Python gets converted to LongType column;
        # this is not supported by the function, so cast to Integer explicitly
        df = self.spark.createDataFrame([Row(date=dt, add=2)], "date date, add integer")

        self.assertTrue(
            all(
                df.select(
                    date_add(df.date, df.add) == datetime.date(2021, 12, 29),
                    date_add(df.date, "add") == datetime.date(2021, 12, 29),
                    date_add(df.date, 3) == datetime.date(2021, 12, 30),
                ).first()
            )
        )

    # Test added for SPARK-37738; change Python API to accept both col & int as input
    def test_date_sub_function(self):
        dt = datetime.date(2021, 12, 27)

        # Note; number var in Python gets converted to LongType column;
        # this is not supported by the function, so cast to Integer explicitly
        df = self.spark.createDataFrame([Row(date=dt, sub=2)], "date date, sub integer")

        self.assertTrue(
            all(
                df.select(
                    date_sub(df.date, df.sub) == datetime.date(2021, 12, 25),
                    date_sub(df.date, "sub") == datetime.date(2021, 12, 25),
                    date_sub(df.date, 3) == datetime.date(2021, 12, 24),
                ).first()
            )
        )

    # Test added for SPARK-37738; change Python API to accept both col & int as input
    def test_add_months_function(self):
        dt = datetime.date(2021, 12, 27)

        # Note; number in Python gets converted to LongType column;
        # this is not supported by the function, so cast to Integer explicitly
        df = self.spark.createDataFrame([Row(date=dt, add=2)], "date date, add integer")

        self.assertTrue(
            all(
                df.select(
                    add_months(df.date, df.add) == datetime.date(2022, 2, 27),
                    add_months(df.date, "add") == datetime.date(2022, 2, 27),
                    add_months(df.date, 3) == datetime.date(2022, 3, 27),
                ).first()
            )
        )

    def test_make_date(self):
        # SPARK-36554: expose make_date expression
        df = self.spark.createDataFrame([(2020, 6, 26)], ["Y", "M", "D"])
        row_from_col = df.select(make_date(df.Y, df.M, df.D)).first()
        self.assertEqual(row_from_col[0], datetime.date(2020, 6, 26))
        row_from_name = df.select(make_date("Y", "M", "D")).first()
        self.assertEqual(row_from_name[0], datetime.date(2020, 6, 26))

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
        df3 = df2.select(
            functions.first(df2.id, False).alias("a"),
            functions.first(df2.id, True).alias("b"),
            functions.last(df2.id, False).alias("c"),
            functions.last(df2.id, True).alias("d"),
        )
        self.assertEqual([Row(a=None, b=1, c=None, d=98)], df3.collect())

    def test_approxQuantile(self):
        df = self.sc.parallelize([Row(a=i, b=i + 10) for i in range(10)]).toDF()
        for f in ["a", "a"]:
            aq = df.stat.approxQuantile(f, [0.1, 0.5, 0.9], 0.1)
            self.assertTrue(isinstance(aq, list))
            self.assertEqual(len(aq), 3)
        self.assertTrue(all(isinstance(q, float) for q in aq))
        aqs = df.stat.approxQuantile(["a", "b"], [0.1, 0.5, 0.9], 0.1)
        self.assertTrue(isinstance(aqs, list))
        self.assertEqual(len(aqs), 2)
        self.assertTrue(isinstance(aqs[0], list))
        self.assertEqual(len(aqs[0]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqs[0]))
        self.assertTrue(isinstance(aqs[1], list))
        self.assertEqual(len(aqs[1]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqs[1]))
        aqt = df.stat.approxQuantile(("a", "b"), [0.1, 0.5, 0.9], 0.1)
        self.assertTrue(isinstance(aqt, list))
        self.assertEqual(len(aqt), 2)
        self.assertTrue(isinstance(aqt[0], list))
        self.assertEqual(len(aqt[0]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqt[0]))
        self.assertTrue(isinstance(aqt[1], list))
        self.assertEqual(len(aqt[1]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqt[1]))
        self.assertRaises(TypeError, lambda: df.stat.approxQuantile(123, [0.1, 0.9], 0.1))
        self.assertRaises(TypeError, lambda: df.stat.approxQuantile(("a", 123), [0.1, 0.9], 0.1))
        self.assertRaises(TypeError, lambda: df.stat.approxQuantile(["a", 123], [0.1, 0.9], 0.1))

    def test_sorting_functions_with_column(self):
        from pyspark.sql import functions
        from pyspark.sql.column import Column

        funs = [
            functions.asc_nulls_first,
            functions.asc_nulls_last,
            functions.desc_nulls_first,
            functions.desc_nulls_last,
        ]
        exprs = [col("x"), "x"]

        for fun in funs:
            for expr in exprs:
                res = fun(expr)
                self.assertIsInstance(res, Column)
                self.assertIn(f"""'x {fun.__name__.replace("_", " ").upper()}'""", str(res))

        for expr in exprs:
            res = functions.asc(expr)
            self.assertIsInstance(res, Column)
            self.assertIn("""'x ASC NULLS FIRST'""", str(res))

        for expr in exprs:
            res = functions.desc(expr)
            self.assertIsInstance(res, Column)
            self.assertIn("""'x DESC NULLS LAST'""", str(res))

    def test_sort_with_nulls_order(self):
        from pyspark.sql import functions

        df = self.spark.createDataFrame(
            [("Tom", 80), (None, 60), ("Alice", 50)], ["name", "height"]
        )
        self.assertEqual(
            df.select(df.name).orderBy(functions.asc_nulls_first("name")).collect(),
            [Row(name=None), Row(name="Alice"), Row(name="Tom")],
        )
        self.assertEqual(
            df.select(df.name).orderBy(functions.asc_nulls_last("name")).collect(),
            [Row(name="Alice"), Row(name="Tom"), Row(name=None)],
        )
        self.assertEqual(
            df.select(df.name).orderBy(functions.desc_nulls_first("name")).collect(),
            [Row(name=None), Row(name="Tom"), Row(name="Alice")],
        )
        self.assertEqual(
            df.select(df.name).orderBy(functions.desc_nulls_last("name")).collect(),
            [Row(name="Tom"), Row(name="Alice"), Row(name=None)],
        )

    def test_input_file_name_reset_for_rdd(self):
        rdd = self.sc.textFile("python/test_support/hello/hello.txt").map(lambda x: {"data": x})
        df = self.spark.createDataFrame(rdd, "data STRING")
        df.select(input_file_name().alias("file")).collect()

        non_file_df = self.spark.range(100).select(input_file_name())

        results = non_file_df.collect()
        self.assertTrue(len(results) == 100)

        # [SPARK-24605]: if everything was properly reset after the last job, this should return
        # empty string rather than the file read in the last job.
        for result in results:
            self.assertEqual(result[0], "")

    def test_slice(self):
        df = self.spark.createDataFrame(
            [
                (
                    [1, 2, 3],
                    2,
                    2,
                ),
                (
                    [4, 5],
                    2,
                    2,
                ),
            ],
            ["x", "index", "len"],
        )

        expected = [Row(sliced=[2, 3]), Row(sliced=[5])]
        self.assertTrue(
            all(
                [
                    df.select(slice(df.x, 2, 2).alias("sliced")).collect() == expected,
                    df.select(slice(df.x, lit(2), lit(2)).alias("sliced")).collect() == expected,
                    df.select(slice("x", "index", "len").alias("sliced")).collect() == expected,
                ]
            )
        )

        self.assertEqual(
            df.select(slice(df.x, size(df.x) - 1, lit(1)).alias("sliced")).collect(),
            [Row(sliced=[2]), Row(sliced=[4])],
        )
        self.assertEqual(
            df.select(slice(df.x, lit(1), size(df.x) - 1).alias("sliced")).collect(),
            [Row(sliced=[1, 2]), Row(sliced=[4])],
        )

    def test_array_repeat(self):
        df = self.spark.range(1)
        df = df.withColumn("repeat_n", lit(3))

        expected = [Row(val=[0, 0, 0])]
        self.assertTrue(
            all(
                [
                    df.select(array_repeat("id", 3).alias("val")).collect() == expected,
                    df.select(array_repeat("id", lit(3)).alias("val")).collect() == expected,
                    df.select(array_repeat("id", "repeat_n").alias("val")).collect() == expected,
                ]
            )
        )

    def test_input_file_name_udf(self):
        df = self.spark.read.text("python/test_support/hello/hello.txt")
        df = df.select(udf(lambda x: x)("value"), input_file_name().alias("file"))
        file_name = df.collect()[0].file
        self.assertTrue("python/test_support/hello/hello.txt" in file_name)

    def test_least(self):
        df = self.spark.createDataFrame([(1, 4, 3)], ["a", "b", "c"])

        expected = [Row(least=1)]
        self.assertTrue(
            all(
                [
                    df.select(least(df.a, df.b, df.c).alias("least")).collect() == expected,
                    df.select(least(lit(3), lit(5), lit(1)).alias("least")).collect() == expected,
                    df.select(least("a", "b", "c").alias("least")).collect() == expected,
                ]
            )
        )

    def test_overlay(self):
        from pyspark.sql.functions import col, lit, overlay
        from itertools import chain
        import re

        actual = list(
            chain.from_iterable(
                [
                    re.findall("(overlay\\(.*\\))", str(x))
                    for x in [
                        overlay(col("foo"), col("bar"), 1),
                        overlay("x", "y", 3),
                        overlay(col("x"), col("y"), 1, 3),
                        overlay("x", "y", 2, 5),
                        overlay("x", "y", lit(11)),
                        overlay("x", "y", lit(2), lit(5)),
                    ]
                ]
            )
        )

        expected = [
            "overlay(foo, bar, 1, -1)",
            "overlay(x, y, 3, -1)",
            "overlay(x, y, 1, 3)",
            "overlay(x, y, 2, 5)",
            "overlay(x, y, 11, -1)",
            "overlay(x, y, 2, 5)",
        ]

        self.assertListEqual(actual, expected)

        df = self.spark.createDataFrame([("SPARK_SQL", "CORE", 7, 0)], ("x", "y", "pos", "len"))

        exp = [Row(ol="SPARK_CORESQL")]
        self.assertTrue(
            all(
                [
                    df.select(overlay(df.x, df.y, 7, 0).alias("ol")).collect() == exp,
                    df.select(overlay(df.x, df.y, lit(7), lit(0)).alias("ol")).collect() == exp,
                    df.select(overlay("x", "y", "pos", "len").alias("ol")).collect() == exp,
                ]
            )
        )

    def test_percentile_approx(self):
        actual = list(
            chain.from_iterable(
                [
                    re.findall("(percentile_approx\\(.*\\))", str(x))
                    for x in [
                        percentile_approx(col("foo"), lit(0.5)),
                        percentile_approx(col("bar"), 0.25, 42),
                        percentile_approx(col("bar"), [0.25, 0.5, 0.75]),
                        percentile_approx(col("foo"), (0.05, 0.95), 100),
                        percentile_approx("foo", 0.5),
                        percentile_approx("bar", [0.1, 0.9], lit(10)),
                    ]
                ]
            )
        )

        expected = [
            "percentile_approx(foo, 0.5, 10000)",
            "percentile_approx(bar, 0.25, 42)",
            "percentile_approx(bar, array(0.25, 0.5, 0.75), 10000)",
            "percentile_approx(foo, array(0.05, 0.95), 100)",
            "percentile_approx(foo, 0.5, 10000)",
            "percentile_approx(bar, array(0.1, 0.9), 10)",
        ]

        self.assertListEqual(actual, expected)

    def test_nth_value(self):
        from pyspark.sql import Window
        from pyspark.sql.functions import nth_value

        df = self.spark.createDataFrame(
            [
                ("a", 0, None),
                ("a", 1, "x"),
                ("a", 2, "y"),
                ("a", 3, "z"),
                ("a", 4, None),
                ("b", 1, None),
                ("b", 2, None),
            ],
            schema=("key", "order", "value"),
        )
        w = Window.partitionBy("key").orderBy("order")

        rs = df.select(
            df.key,
            df.order,
            nth_value("value", 2).over(w),
            nth_value("value", 2, False).over(w),
            nth_value("value", 2, True).over(w),
        ).collect()

        expected = [
            ("a", 0, None, None, None),
            ("a", 1, "x", "x", None),
            ("a", 2, "x", "x", "y"),
            ("a", 3, "x", "x", "y"),
            ("a", 4, "x", "x", "y"),
            ("b", 1, None, None, None),
            ("b", 2, None, None, None),
        ]

        for r, ex in zip(sorted(rs), sorted(expected)):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_higher_order_function_failures(self):
        from pyspark.sql.functions import col, transform

        # Should fail with varargs
        with self.assertRaises(ValueError):
            transform(col("foo"), lambda *x: lit(1))

        # Should fail with kwargs
        with self.assertRaises(ValueError):
            transform(col("foo"), lambda **x: lit(1))

        # Should fail with nullary function
        with self.assertRaises(ValueError):
            transform(col("foo"), lambda: lit(1))

        # Should fail with quaternary function
        with self.assertRaises(ValueError):
            transform(col("foo"), lambda x1, x2, x3, x4: lit(1))

        # Should fail if function doesn't return Column
        with self.assertRaises(ValueError):
            transform(col("foo"), lambda x: 1)

    def test_nested_higher_order_function(self):
        # SPARK-35382: lambda vars must be resolved properly in nested higher order functions
        from pyspark.sql.functions import flatten, struct, transform

        df = self.spark.sql("SELECT array(1, 2, 3) as numbers, array('a', 'b', 'c') as letters")

        actual = df.select(
            flatten(
                transform(
                    "numbers",
                    lambda number: transform(
                        "letters", lambda letter: struct(number.alias("n"), letter.alias("l"))
                    ),
                )
            )
        ).first()[0]

        expected = [
            (1, "a"),
            (1, "b"),
            (1, "c"),
            (2, "a"),
            (2, "b"),
            (2, "c"),
            (3, "a"),
            (3, "b"),
            (3, "c"),
        ]

        self.assertEquals(actual, expected)

    def test_window_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.partitionBy("value").orderBy("key")
        from pyspark.sql import functions as F

        sel = df.select(
            df.value,
            df.key,
            F.max("key").over(w.rowsBetween(0, 1)),
            F.min("key").over(w.rowsBetween(0, 1)),
            F.count("key").over(w.rowsBetween(float("-inf"), float("inf"))),
            F.row_number().over(w),
            F.rank().over(w),
            F.dense_rank().over(w),
            F.ntile(2).over(w),
        )
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 1, 1, 1, 1, 1),
            ("2", 1, 1, 1, 3, 1, 1, 1, 1),
            ("2", 1, 2, 1, 3, 2, 1, 1, 1),
            ("2", 2, 2, 2, 3, 3, 3, 2, 2),
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_window_functions_without_partitionBy(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.orderBy("key", df.value)
        from pyspark.sql import functions as F

        sel = df.select(
            df.value,
            df.key,
            F.max("key").over(w.rowsBetween(0, 1)),
            F.min("key").over(w.rowsBetween(0, 1)),
            F.count("key").over(w.rowsBetween(float("-inf"), float("inf"))),
            F.row_number().over(w),
            F.rank().over(w),
            F.dense_rank().over(w),
            F.ntile(2).over(w),
        )
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 4, 1, 1, 1, 1),
            ("2", 1, 1, 1, 4, 2, 2, 2, 1),
            ("2", 1, 2, 1, 4, 3, 2, 2, 2),
            ("2", 2, 2, 2, 4, 4, 4, 3, 2),
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_window_functions_cumulative_sum(self):
        df = self.spark.createDataFrame([("one", 1), ("two", 2)], ["key", "value"])
        from pyspark.sql import functions as F

        # Test cumulative sum
        sel = df.select(
            df.key, F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding, 0))
        )
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

        # Test boundary values less than JVM's Long.MinValue and make sure we don't overflow
        sel = df.select(
            df.key, F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding - 1, 0))
        )
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

        # Test boundary values greater than JVM's Long.MaxValue and make sure we don't overflow
        frame_end = Window.unboundedFollowing + 1
        sel = df.select(
            df.key, F.sum(df.value).over(Window.rowsBetween(Window.currentRow, frame_end))
        )
        rs = sorted(sel.collect())
        expected = [("one", 3), ("two", 2)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_collect_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        from pyspark.sql import functions

        self.assertEqual(
            sorted(df.select(functions.collect_set(df.key).alias("r")).collect()[0].r), [1, 2]
        )
        self.assertEqual(
            sorted(df.select(functions.collect_list(df.key).alias("r")).collect()[0].r),
            [1, 1, 1, 2],
        )
        self.assertEqual(
            sorted(df.select(functions.collect_set(df.value).alias("r")).collect()[0].r), ["1", "2"]
        )
        self.assertEqual(
            sorted(df.select(functions.collect_list(df.value).alias("r")).collect()[0].r),
            ["1", "2", "2", "2"],
        )

    def test_datetime_functions(self):
        from pyspark.sql import functions
        from datetime import date

        df = self.spark.range(1).selectExpr("'2017-01-22' as dateCol")
        parse_result = df.select(functions.to_date(functions.col("dateCol"))).first()
        self.assertEqual(date(2017, 1, 22), parse_result["to_date(dateCol)"])

    def test_assert_true(self):
        from pyspark.sql.functions import assert_true

        df = self.spark.range(3)

        self.assertEqual(
            df.select(assert_true(df.id < 3)).toDF("val").collect(),
            [Row(val=None), Row(val=None), Row(val=None)],
        )

        with self.assertRaises(Py4JJavaError) as cm:
            df.select(assert_true(df.id < 2, "too big")).toDF("val").collect()
        self.assertIn("java.lang.RuntimeException", str(cm.exception))
        self.assertIn("too big", str(cm.exception))

        with self.assertRaises(Py4JJavaError) as cm:
            df.select(assert_true(df.id < 2, df.id * 1e6)).toDF("val").collect()
        self.assertIn("java.lang.RuntimeException", str(cm.exception))
        self.assertIn("2000000", str(cm.exception))

        with self.assertRaises(TypeError) as cm:
            df.select(assert_true(df.id < 2, 5))
        self.assertEqual("errMsg should be a Column or a str, got <class 'int'>", str(cm.exception))

    def test_raise_error(self):
        from pyspark.sql.functions import raise_error

        df = self.spark.createDataFrame([Row(id="foobar")])

        with self.assertRaises(Py4JJavaError) as cm:
            df.select(raise_error(df.id)).collect()
        self.assertIn("java.lang.RuntimeException", str(cm.exception))
        self.assertIn("foobar", str(cm.exception))

        with self.assertRaises(Py4JJavaError) as cm:
            df.select(raise_error("barfoo")).collect()
        self.assertIn("java.lang.RuntimeException", str(cm.exception))
        self.assertIn("barfoo", str(cm.exception))

        with self.assertRaises(TypeError) as cm:
            df.select(raise_error(None))
        self.assertEqual(
            "errMsg should be a Column or a str, got <class 'NoneType'>", str(cm.exception)
        )

    def test_sum_distinct(self):
        self.spark.range(10).select(
            assert_true(sum_distinct(col("id")) == sumDistinct(col("id")))
        ).collect()

    def test_shiftleft(self):
        self.spark.range(10).select(
            assert_true(shiftLeft(col("id"), 2) == shiftleft(col("id"), 2))
        ).collect()

    def test_shiftright(self):
        self.spark.range(10).select(
            assert_true(shiftRight(col("id"), 2) == shiftright(col("id"), 2))
        ).collect()

    def test_shiftrightunsigned(self):
        self.spark.range(10).select(
            assert_true(shiftRightUnsigned(col("id"), 2) == shiftrightunsigned(col("id"), 2))
        ).collect()

    def test_lit_day_time_interval(self):
        td = datetime.timedelta(days=1, hours=12, milliseconds=123)
        actual = self.spark.range(1).select(lit(td)).first()[0]
        self.assertEqual(actual, td)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
