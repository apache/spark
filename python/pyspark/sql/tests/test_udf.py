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

import functools
import platform
import pydoc
import shutil
import tempfile
import unittest
import datetime
import io
from contextlib import redirect_stdout

from pyspark.sql import SparkSession, Column, Row
from pyspark.sql.functions import col, udf, assert_true, lit, rand
from pyspark.sql.udf import UserDefinedFunction
from pyspark.sql.types import (
    StringType,
    IntegerType,
    BooleanType,
    DoubleType,
    LongType,
    ArrayType,
    MapType,
    StructType,
    StructField,
    TimestampNTZType,
    DayTimeIntervalType,
    VariantType,
    VariantVal,
)
from pyspark.errors import AnalysisException, PythonException, PySparkTypeError
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    test_compiled,
    test_not_compiled_message,
)
from pyspark.testing.utils import assertDataFrameEqual


class BaseUDFTestsMixin(object):
    def test_udf_with_callable(self):
        data = self.spark.createDataFrame([(i, i**2) for i in range(10)], ["number", "squared"])

        class PlusFour:
            def __call__(self, col):
                if col is not None:
                    return col + 4

        call = PlusFour()
        pudf = UserDefinedFunction(call, LongType())
        res = data.select(pudf(data["number"]).alias("plus_four"))
        self.assertEqual(res.agg({"plus_four": "sum"}).collect()[0][0], 85)

    def test_udf_with_partial_function(self):
        data = self.spark.createDataFrame([(i, i**2) for i in range(10)], ["number", "squared"])

        def some_func(col, param):
            if col is not None:
                return col + param

        pfunc = functools.partial(some_func, param=4)
        pudf = UserDefinedFunction(pfunc, LongType())
        res = data.select(pudf(data["number"]).alias("plus_four"))
        self.assertEqual(res.agg({"plus_four": "sum"}).collect()[0][0], 85)

    def test_udf(self):
        self.spark.catalog.registerFunction("twoArgs", lambda x, y: len(x) + y, IntegerType())
        [row] = self.spark.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], 5)

    def test_udf_on_sql_context(self):
        from pyspark import SQLContext

        # This is to check if a deprecated 'SQLContext.registerFunction' can call its alias.
        sqlContext = SQLContext.getOrCreate(self.spark.sparkContext)
        sqlContext.registerFunction("oneArg", lambda x: len(x), IntegerType())
        [row] = sqlContext.sql("SELECT oneArg('test')").collect()
        self.assertEqual(row[0], 4)

    def test_udf2(self):
        with self.tempView("test"):
            self.spark.catalog.registerFunction("strlen", lambda string: len(string), IntegerType())
            self.spark.createDataFrame([("test",)], ["a"]).createOrReplaceTempView("test")
            [res] = self.spark.sql("SELECT strlen(a) FROM test WHERE strlen(a) > 1").collect()
            self.assertEqual(4, res[0])

    def test_udf3(self):
        two_args = self.spark.catalog.registerFunction(
            "twoArgs", UserDefinedFunction(lambda x, y: len(x) + y)
        )
        self.assertEqual(two_args.deterministic, True)
        [row] = self.spark.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], "5")

    def test_udf_registration_return_type_none(self):
        two_args = self.spark.catalog.registerFunction(
            "twoArgs", UserDefinedFunction(lambda x, y: len(x) + y, "integer"), None
        )
        self.assertEqual(two_args.deterministic, True)
        [row] = self.spark.sql("SELECT twoArgs('test', 1)").collect()
        self.assertEqual(row[0], 5)

    def test_udf_registration_return_type_not_none(self):
        with self.quiet():
            self.check_udf_registration_return_type_not_none()

    def check_udf_registration_return_type_not_none(self):
        # negative test for incorrect type
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.catalog.registerFunction(
                "f", UserDefinedFunction(lambda x, y: len(x) + y, StringType()), StringType()
            )

        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_SPECIFY_RETURN_TYPE_FOR_UDF",
            messageParameters={"arg_name": "f", "return_type": "StringType()"},
        )

    def test_nondeterministic_udf(self):
        # Test that nondeterministic UDFs are evaluated only once in chained UDF evaluations
        import random

        udf_random_col = udf(lambda: int(100 * random.random()), IntegerType()).asNondeterministic()
        self.assertEqual(udf_random_col.deterministic, False)
        df = self.spark.createDataFrame([Row(1)]).select(udf_random_col().alias("RAND"))
        udf_add_ten = udf(lambda rand: rand + 10, IntegerType())
        [row] = df.withColumn("RAND_PLUS_TEN", udf_add_ten("RAND")).collect()
        self.assertEqual(row[0] + 10, row[1])

    def test_nondeterministic_udf2(self):
        import random

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
        with self.quiet():
            self.check_nondeterministic_udf_in_aggregate()

    def check_nondeterministic_udf_in_aggregate(self):
        from pyspark.sql.functions import sum
        import random

        udf_random_col = udf(lambda: int(100 * random.random()), "int").asNondeterministic()
        df = self.spark.range(10)

        with self.assertRaisesRegex(AnalysisException, "Non-deterministic"):
            df.groupby("id").agg(sum(udf_random_col())).collect()
        with self.assertRaisesRegex(AnalysisException, "Non-deterministic"):
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
        self.assertEqual(tuple(row), (2,))

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
        left = self.spark.createDataFrame([Row(a=1)])
        right = self.spark.createDataFrame([Row(a=1)])
        df = left.join(right, on="a", how="left_outer")
        df = df.withColumn("b", udf(lambda x: "x")(df.a))
        self.assertEqual(df.filter('b = "x"').collect(), [Row(a=1, b="x")])

    def test_udf_in_filter_on_top_of_join(self):
        # regression test for SPARK-18589
        left = self.spark.createDataFrame([Row(a=1)])
        right = self.spark.createDataFrame([Row(b=1)])
        f = udf(lambda a, b: a == b, BooleanType())
        df = left.crossJoin(right).filter(f("a", "b"))
        self.assertEqual(df.collect(), [Row(a=1, b=1)])

    def test_udf_in_join_condition(self):
        # regression test for SPARK-25314
        left = self.spark.createDataFrame([Row(a=1)])
        right = self.spark.createDataFrame([Row(b=1)])
        f = udf(lambda a, b: a == b, BooleanType())
        # The udf uses attributes from both sides of join, so it is pulled out as Filter +
        # Cross join.
        df = left.join(right, f("a", "b"))
        with self.sql_conf({"spark.sql.crossJoin.enabled": False}):
            with self.assertRaisesRegex(AnalysisException, "Detected implicit cartesian product"):
                df.collect()
        with self.sql_conf({"spark.sql.crossJoin.enabled": True}):
            self.assertEqual(df.collect(), [Row(a=1, b=1)])

    def test_udf_in_left_outer_join_condition(self):
        # regression test for SPARK-26147
        from pyspark.sql.functions import col

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
        left = self.spark.createDataFrame([Row(a=1, a1=1, a2=1), Row(a=2, a1=2, a2=2)])
        right = self.spark.createDataFrame([Row(b=1, b1=1, b2=1), Row(b=1, b1=3, b2=1)])
        f = udf(lambda a, b: a == b, BooleanType())
        df = left.join(right, [f("a", "b"), left.a1 == right.b1])
        # do not need spark.sql.crossJoin.enabled=true for udf is not the only join condition.
        self.assertEqual(df.collect(), [Row(a=1, a1=1, a2=1, b=1, b1=1, b2=1)])

    def test_udf_not_supported_in_join_condition(self):
        # regression test for SPARK-25314
        # test python udf is not supported in join type except inner join.
        left = self.spark.createDataFrame([Row(a=1, a1=1, a2=1), Row(a=2, a1=2, a2=2)])
        right = self.spark.createDataFrame([Row(b=1, b1=1, b2=1), Row(b=1, b1=3, b2=1)])
        f = udf(lambda a, b: a == b, BooleanType())

        def runWithJoinType(join_type, type_string):
            with self.assertRaisesRegex(
                AnalysisException,
                """Python UDF in the ON clause of a %s JOIN.""" % type_string,
            ):
                left.join(right, [f("a", "b"), left.a1 == right.b1], join_type).collect()

        runWithJoinType("full", "FULL OUTER")
        runWithJoinType("left", "LEFT OUTER")
        runWithJoinType("right", "RIGHT OUTER")
        runWithJoinType("leftanti", "LEFT ANTI")
        runWithJoinType("leftsemi", "LEFT SEMI")

    def test_udf_as_join_condition(self):
        left = self.spark.createDataFrame([Row(a=1, a1=1, a2=1), Row(a=2, a1=2, a2=2)])
        right = self.spark.createDataFrame([Row(b=1, b1=1, b2=1), Row(b=1, b1=3, b2=1)])
        f = udf(lambda a: a, IntegerType())

        df = left.join(right, [f("a") == f("b"), left.a1 == right.b1])
        self.assertEqual(df.collect(), [Row(a=1, a1=1, a2=1, b=1, b1=1, b2=1)])

    def test_udf_without_arguments(self):
        self.spark.catalog.registerFunction("foo", lambda: "bar")
        [row] = self.spark.sql("SELECT foo()").collect()
        self.assertEqual(row[0], "bar")

    def test_udf_with_array_type(self):
        with self.tempView("test"):
            self.spark.createDataFrame(
                [
                    ([0, 1, 2], {"key": [0, 1, 2, 3, 4]}),
                ],
                ["l", "d"],
            ).createOrReplaceTempView("test")
            self.spark.catalog.registerFunction(
                "copylist", lambda l: list(l), ArrayType(IntegerType())
            )
            self.spark.catalog.registerFunction("maplen", lambda d: len(d), IntegerType())
            [(l1, l2)] = self.spark.sql("select copylist(l), maplen(d) from test").collect()
            self.assertEqual(list(range(3)), l1)
            self.assertEqual(1, l2)

    def test_broadcast_in_udf(self):
        bar = {"a": "aa", "b": "bb", "c": "abc"}
        foo = self.sc.broadcast(bar)
        self.spark.catalog.registerFunction("MYUDF", lambda x: foo.value[x] if x else "")
        [res] = self.spark.sql("SELECT MYUDF('c')").collect()
        self.assertEqual("abc", res[0])
        [res] = self.spark.sql("SELECT MYUDF('')").collect()
        self.assertEqual("", res[0])

    def test_udf_with_filter_function(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])

        my_filter = udf(lambda a: a < 2, BooleanType())
        sel = df.select(col("key"), col("value")).filter((my_filter(col("key"))) & (df.value < "2"))
        self.assertEqual(sel.collect(), [Row(key=1, value="1")])

    def test_udf_with_variant_input(self):
        df = self.spark.range(0, 10).selectExpr("parse_json(cast(id as string)) v")

        u = udf(lambda u: str(u), StringType())
        with self.assertRaises(AnalysisException) as ae:
            df.select(u(col("v"))).collect()

        self.check_error(
            exception=ae.exception,
            errorClass="DATATYPE_MISMATCH.UNSUPPORTED_UDF_INPUT_TYPE",
            messageParameters={"sqlExpr": '"<lambda>(v)"', "dataType": "VARIANT"},
        )

    def test_udf_with_complex_variant_input(self):
        df = self.spark.range(0, 10).selectExpr(
            "named_struct('v', parse_json(cast(id as string))) struct_of_v"
        )

        u = udf(lambda u: str(u), StringType())

        with self.assertRaises(AnalysisException) as ae:
            df.select(u(col("struct_of_v"))).collect()

        self.check_error(
            exception=ae.exception,
            errorClass="DATATYPE_MISMATCH.UNSUPPORTED_UDF_INPUT_TYPE",
            messageParameters={
                "sqlExpr": '"<lambda>(struct_of_v)"',
                "dataType": "STRUCT<v: VARIANT NOT NULL>",
            },
        )

    def test_udf_with_variant_output(self):
        # The variant value returned corresponds to a JSON string of {"a": "b"}.
        u = udf(
            lambda: VariantVal(bytes([2, 1, 0, 0, 2, 5, 98]), bytes([1, 1, 0, 1, 97])),
            VariantType(),
        )

        with self.assertRaises(AnalysisException) as ae:
            self.spark.range(0, 10).select(u()).collect()

        self.check_error(
            exception=ae.exception,
            errorClass="DATATYPE_MISMATCH.UNSUPPORTED_UDF_OUTPUT_TYPE",
            messageParameters={"sqlExpr": '"<lambda>()"', "dataType": "VARIANT"},
        )

    def test_udf_with_complex_variant_output(self):
        # The variant value returned corresponds to a JSON string of {"a": "b"}.
        u = udf(
            lambda: {"v", VariantVal(bytes([2, 1, 0, 0, 2, 5, 98]), bytes([1, 1, 0, 1, 97]))},
            MapType(StringType(), VariantType()),
        )

        with self.assertRaises(AnalysisException) as ae:
            self.spark.range(0, 10).select(u()).collect()

        self.check_error(
            exception=ae.exception,
            errorClass="DATATYPE_MISMATCH.UNSUPPORTED_UDF_OUTPUT_TYPE",
            messageParameters={"sqlExpr": '"<lambda>()"', "dataType": "MAP<STRING, VARIANT>"},
        )

    def test_udf_with_aggregate_function(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        from pyspark.sql.functions import col, sum

        my_filter = udf(lambda a: a == 1, BooleanType())
        sel = df.select(col("key")).distinct().filter(my_filter(col("key")))
        self.assertEqual(sel.collect(), [Row(key=1)])

        my_copy = udf(lambda x: x, IntegerType())
        my_add = udf(lambda a, b: int(a + b), IntegerType())
        my_strlen = udf(lambda x: len(x), IntegerType())
        sel = (
            df.groupBy(my_copy(col("key")).alias("k"))
            .agg(sum(my_strlen(col("value"))).alias("s"))
            .select(my_add(col("k"), col("s")).alias("t"))
        )
        self.assertEqual(sel.collect(), [Row(t=4), Row(t=3)])

    def test_udf_in_generate(self):
        from pyspark.sql.functions import explode

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
        my_copy = udf(lambda x: x, IntegerType())
        df = self.spark.range(10).orderBy("id")
        res = df.select(df.id, my_copy(df.id).alias("copy")).limit(1)
        self.assertEqual(res.collect(), [Row(id=0, copy=0)])

    def test_udf_registration_returns_udf(self):
        df = self.spark.range(10)
        add_three = self.spark.udf.register("add_three", lambda x: x + 3, IntegerType())
        self.assertListEqual(
            df.selectExpr("add_three(id) AS plus_three").collect(),
            df.select(add_three("id").alias("plus_three")).collect(),
        )

        add_three_str = self.spark.udf.register("add_three_str", lambda x: x + 3)
        self.assertListEqual(
            df.selectExpr("add_three_str(id) AS plus_three").collect(),
            df.select(add_three_str("id").alias("plus_three")).collect(),
        )

    def test_udf_registration_returns_udf_on_sql_context(self):
        from pyspark import SQLContext

        df = self.spark.range(10)

        # This is to check if a 'SQLContext.udf' can call its alias.
        sqlContext = SQLContext.getOrCreate(self.spark.sparkContext)
        add_four = sqlContext.udf.register("add_four", lambda x: x + 4, IntegerType())

        self.assertListEqual(
            df.selectExpr("add_four(id) AS plus_four").collect(),
            df.select(add_four("id").alias("plus_four")).collect(),
        )

    @unittest.skipIf(not test_compiled, test_not_compiled_message)  # type: ignore
    def test_register_java_function(self):
        self.spark.udf.registerJavaFunction(
            "javaStringLength", "test.org.apache.spark.sql.JavaStringLength", IntegerType()
        )
        [value] = self.spark.sql("SELECT javaStringLength('test')").first()
        self.assertEqual(value, 4)

        self.spark.udf.registerJavaFunction(
            "javaStringLength2", "test.org.apache.spark.sql.JavaStringLength"
        )
        [value] = self.spark.sql("SELECT javaStringLength2('test')").first()
        self.assertEqual(value, 4)

        self.spark.udf.registerJavaFunction(
            "javaStringLength3", "test.org.apache.spark.sql.JavaStringLength", "integer"
        )
        [value] = self.spark.sql("SELECT javaStringLength3('test')").first()
        self.assertEqual(value, 4)

    @unittest.skipIf(not test_compiled, test_not_compiled_message)  # type: ignore
    def test_register_java_udaf(self):
        self.spark.udf.registerJavaUDAF("javaUDAF", "test.org.apache.spark.sql.MyDoubleAvg")
        df = self.spark.createDataFrame([(1, "a"), (2, "b"), (3, "a")], ["id", "name"])
        df.createOrReplaceTempView("df")
        row = self.spark.sql(
            "SELECT name, javaUDAF(id) as avg from df group by name order by name desc"
        ).first()
        self.assertEqual(row.asDict(), Row(name="b", avg=102.0).asDict())

    def test_err_udf_registration(self):
        with self.quiet():
            self.check_err_udf_registration()

    def check_err_udf_registration(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.udf.register("f", UserDefinedFunction("x", StringType()), "int")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_CALLABLE",
            messageParameters={"arg_name": "func", "arg_type": "str"},
        )

    def test_non_existed_udf(self):
        spark = self.spark
        self.assertRaisesRegex(
            AnalysisException,
            "Can not load class non_existed_udf",
            lambda: spark.udf.registerJavaFunction("udf1", "non_existed_udf"),
        )

    def test_non_existed_udf_with_sql_context(self):
        from pyspark import SQLContext

        # This is to check if a deprecated 'SQLContext.registerJavaFunction' can call its alias.
        sqlContext = SQLContext.getOrCreate(self.spark.sparkContext)
        self.assertRaisesRegex(
            AnalysisException,
            "Can not load class non_existed_udf",
            lambda: sqlContext.registerJavaFunction("udf1", "non_existed_udf"),
        )

    def test_non_existed_udaf(self):
        spark = self.spark
        self.assertRaisesRegex(
            AnalysisException,
            "Can not load class non_existed_udaf",
            lambda: spark.udf.registerJavaUDAF("udaf1", "non_existed_udaf"),
        )

    def test_udf_with_input_file_name(self):
        from pyspark.sql.functions import input_file_name

        sourceFile = udf(lambda path: path, StringType())
        filePath = "python/test_support/sql/people1.json"
        row = self.spark.read.json(filePath).select(sourceFile(input_file_name())).first()
        self.assertTrue(row[0].find("people1.json") != -1)

    def test_udf_with_input_file_name_for_hadooprdd(self):
        from pyspark.sql.functions import input_file_name

        def filename(path):
            return path

        sameText = udf(filename, StringType())

        rdd = self.sc.textFile("python/test_support/sql/people.json")
        df = self.spark.read.json(rdd).select(input_file_name().alias("file"))
        row = df.select(sameText(df["file"])).first()
        self.assertTrue(row[0].find("people.json") != -1)

        rdd2 = self.sc.newAPIHadoopFile(
            "python/test_support/sql/people.json",
            "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
            "org.apache.hadoop.io.LongWritable",
            "org.apache.hadoop.io.Text",
        )

        df2 = self.spark.read.json(rdd2).select(input_file_name().alias("file"))
        row2 = df2.select(sameText(df2["file"])).first()
        self.assertTrue(row2[0].find("people.json") != -1)

    def test_udf_defers_judf_initialization(self):
        # This is separate of  UDFInitializationTests
        # to avoid context initialization
        # when udf is called
        f = UserDefinedFunction(lambda x: x, StringType())

        self.assertIsNone(
            f._judf_placeholder, "judf should not be initialized before the first call."
        )

        self.assertIsInstance(f("foo"), Column, "UDF call should return a Column.")

        self.assertIsNotNone(
            f._judf_placeholder, "judf should be initialized after UDF has been called."
        )

    def test_udf_with_string_return_type(self):
        add_one = UserDefinedFunction(lambda x: x + 1, "integer")
        make_pair = UserDefinedFunction(lambda x: (-x, x), "struct<x:integer,y:integer>")
        make_array = UserDefinedFunction(
            lambda x: [float(x) for x in range(x, x + 3)], "array<double>"
        )

        expected = (2, Row(x=-1, y=1), [1.0, 2.0, 3.0])
        actual = (
            self.spark.range(1, 2)
            .toDF("x")
            .select(add_one("x"), make_pair("x"), make_array("x"))
            .first()
        )

        self.assertTupleEqual(expected, actual)

    def test_udf_should_not_accept_noncallable_object(self):
        non_callable = None
        self.assertRaises(TypeError, UserDefinedFunction, non_callable, StringType())

    def test_udf_with_decorator(self):
        from pyspark.sql.functions import lit

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

        df = self.spark.createDataFrame(
            [(1, "Foo", "foobar", 3.0)], ("one", "Foo", "foobar", "float")
        ).select(
            add_one("one"),
            add_two("one"),
            to_upper("Foo"),
            to_lower("Foo"),
            substr("foobar", lit(0), lit(3)),
            trunc("float"),
            as_double("one"),
        )

        self.assertListEqual(
            [tpe for _, tpe in df.dtypes],
            ["int", "double", "string", "string", "string", "bigint", "double"],
        )

        self.assertListEqual(list(df.first()), [2, 3.0, "FOO", "foo", "foo", 3, 1.0])

    def test_udf_wrapper(self):
        def f(x):
            """Identity"""
            return x

        return_type = IntegerType()
        f_ = udf(f, return_type)

        self.assertTrue(f.__doc__ in f_.__doc__)
        self.assertEqual(f, f_.func)
        self.assertEqual(return_type, f_.returnType)

        class F:
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

    def test_udf_timestamp_ntz(self):
        # SPARK-36626: Test TimestampNTZ in Python UDF
        @udf(TimestampNTZType())
        def noop(x):
            assert x == datetime.datetime(1970, 1, 1, 0, 0)
            return x

        with self.sql_conf({"spark.sql.session.timeZone": "Pacific/Honolulu"}):
            df = self.spark.createDataFrame(
                [(datetime.datetime(1970, 1, 1, 0, 0),)], schema="dt timestamp_ntz"
            ).select(noop("dt").alias("dt"))

            df.selectExpr("assert_true('1970-01-01 00:00:00' == CAST(dt AS STRING))").collect()
            self.assertEqual(df.schema[0].dataType.typeName(), "timestamp_ntz")
            self.assertEqual(df.first()[0], datetime.datetime(1970, 1, 1, 0, 0))

    def test_udf_daytime_interval(self):
        # SPARK-37277: Support DayTimeIntervalType in Python UDF
        @udf(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))
        def noop(x):
            assert x == datetime.timedelta(microseconds=123)
            return x

        df = self.spark.createDataFrame(
            [(datetime.timedelta(microseconds=123),)], schema="td interval day to second"
        ).select(noop("td").alias("td"))

        df.select(
            assert_true(lit("INTERVAL '0 00:00:00.000123' DAY TO SECOND") == df.td.cast("string"))
        ).collect()
        self.assertEqual(df.schema[0].dataType.simpleString(), "interval day to second")
        self.assertEqual(df.first()[0], datetime.timedelta(microseconds=123))

    def test_nonparam_udf_with_aggregate(self):
        import pyspark.sql.functions as f

        df = self.spark.createDataFrame([(1, 2), (1, 2)])
        f_udf = f.udf(lambda: "const_str")
        rows = df.distinct().withColumn("a", f_udf()).collect()
        self.assertEqual(rows, [Row(_1=1, _2=2, a="const_str")])

    # SPARK-24721
    @unittest.skipIf(not test_compiled, test_not_compiled_message)  # type: ignore
    def test_datasource_with_udf(self):
        from pyspark.sql.functions import lit, col

        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(1).write.mode("overwrite").format("csv").save(path)
            filesource_df = self.spark.read.option("inferSchema", True).csv(path).toDF("i")
            datasource_df = (
                self.spark.read.format("org.apache.spark.sql.sources.SimpleScanSource")
                .option("from", 0)
                .option("to", 1)
                .load()
                .toDF("i")
            )
            datasource_v2_df = (
                self.spark.read.format("org.apache.spark.sql.connector.SimpleDataSourceV2")
                .load()
                .toDF("i", "j")
            )

            c1 = udf(lambda x: x + 1, "int")(lit(1))
            c2 = udf(lambda x: x + 1, "int")(col("i"))

            f1 = udf(lambda x: False, "boolean")(lit(1))
            f2 = udf(lambda x: False, "boolean")(col("i"))

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn("c", c1)
                expected = df.withColumn("c", lit(2))
                self.assertEqual(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                result = df.withColumn("c", c2)
                expected = df.withColumn("c", col("i") + 1)
                self.assertEqual(expected.collect(), result.collect())

            for df in [filesource_df, datasource_df, datasource_v2_df]:
                for f in [f1, f2]:
                    result = df.filter(f)
                    self.assertEqual(0, result.count())
        finally:
            shutil.rmtree(path)

    # SPARK-42134
    def test_file_dsv2_with_udf_filter(self):
        from pyspark.sql.functions import lit

        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            with self.sql_conf({"spark.sql.sources.useV1SourceList": ""}):
                self.spark.range(1).write.mode("overwrite").format("parquet").save(path)
                df = self.spark.read.parquet(path).toDF("i")
                f = udf(lambda x: False, "boolean")(lit(1))
                result = df.filter(f)
                self.assertEqual(0, result.count())

        finally:
            shutil.rmtree(path)

    # SPARK-25591
    def test_same_accumulator_in_udfs(self):
        data_schema = StructType(
            [StructField("a", IntegerType(), True), StructField("b", IntegerType(), True)]
        )
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

    # SPARK-26293
    def test_udf_in_subquery(self):
        f = udf(lambda x: x, "long")
        with self.tempView("v"):
            self.spark.range(1).filter(f("id") >= 0).createTempView("v")
            result = self.spark.sql(
                "select i from values(0L) as data(i) where i in (select id from v)"
            )
            self.assertEqual(result.collect(), [Row(i=0)])

    def test_udf_globals_not_overwritten(self):
        @udf("string")
        def f():
            assert "itertools" not in str(map)

        self.spark.range(1).select(f()).collect()

    def test_worker_original_stdin_closed(self):
        # Test if it closes the original standard input of worker inherited from the daemon,
        # and replaces it with '/dev/null'.  See SPARK-26175.
        def task(iterator):
            import sys

            res = sys.stdin.read()
            # Because the standard input is '/dev/null', it reaches to EOF.
            assert res == "", "Expect read EOF from stdin."
            return iterator

        self.sc.parallelize(range(1), 1).mapPartitions(task).count()

    def test_udf_with_256_args(self):
        N = 256
        data = [["data-%d" % i for i in range(N)]] * 5
        df = self.spark.createDataFrame(data)

        def f(*a):
            return "success"

        fUdf = udf(f, StringType())

        r = df.select(fUdf(*df.columns))
        self.assertEqual(r.first()[0], "success")

    def test_udf_cache(self):
        def func(x):
            return x

        df = self.spark.range(1)
        df.select(udf(func)("id")).cache()

        with io.StringIO() as buf, redirect_stdout(buf):
            df.select(udf(func)("id")).explain()
            # == Physical Plan ==
            # InMemoryTableScan [func(id)#30]
            #    +- InMemoryRelation [func(id)#30], StorageLevel(...)
            #          +- *(2) Project [pythonUDF0#5 AS func(id)#3]
            #             +- BatchEvalPython [func(id#0L)#2], [pythonUDF0#5]
            #                +- *(1) Range (0, 1, step=1, splits=12)
            self.assertEqual(1, buf.getvalue().count("InMemoryTableScan"))
            self.assertEqual(1, buf.getvalue().count("InMemoryRelation"))

    # SPARK-34545
    def test_udf_input_serialization_valuecompare_disabled(self):
        def f(e):
            return e[0]

        df = self.spark.createDataFrame([((1.0, 1.0), (1, 1))], ["c1", "c2"])
        result = df.select(
            "*", udf(f, DoubleType())("c1").alias("c3"), udf(f, IntegerType())("c2").alias("c4")
        )
        self.assertEqual(
            result.collect(), [Row(c1=Row(_1=1.0, _2=1.0), c2=Row(_1=1, _2=1), c3=1.0, c4=1)]
        )

    # SPARK-33277
    def test_udf_with_column_vector(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(0, 100000, 1, 1).write.parquet(path)

            def f(x):
                return 0

            fUdf = udf(f, LongType())

            for offheap in ["true", "false"]:
                with self.sql_conf({"spark.sql.columnVector.offheap.enabled": offheap}):
                    self.assertEqual(
                        self.spark.read.parquet(path).select(fUdf("id")).head(), Row(0)
                    )
        finally:
            shutil.rmtree(path)

    def test_udf_with_rand(self):
        # SPARK-40121: rand() with Python UDF.
        self.assertEqual(
            len(self.spark.range(10).select(udf(lambda x: x, DoubleType())(rand())).collect()), 10
        )

    def test_nested_struct(self):
        df = self.spark.range(1).selectExpr(
            "struct(1, struct('John', 30, ('value', 10))) as nested_struct"
        )
        # Input
        row = df.select(udf(lambda x: str(x))("nested_struct")).first()
        self.assertEqual(
            row[0], "Row(col1=1, col2=Row(col1='John', col2=30, col3=Row(col1='value', col2=10)))"
        )
        # Output
        row = df.select(udf(lambda x: x, returnType=df.dtypes[0][1])("nested_struct")).first()
        self.assertEqual(
            row[0], Row(col1=1, col2=Row(col1="John", col2=30, col3=Row(col1="value", col2=10)))
        )

    def test_nested_map(self):
        df = self.spark.range(1).selectExpr("map('a', map('b', 'c')) as nested_map")
        # Input
        row = df.select(udf(lambda x: str(x))("nested_map")).first()
        self.assertEqual(row[0], "{'a': {'b': 'c'}}")
        # Output

        @udf(returnType=df.dtypes[0][1])
        def f(x):
            x["a"]["b"] = "d"
            return x

        row = df.select(f("nested_map")).first()
        self.assertEqual(row[0], {"a": {"b": "d"}})

    def test_nested_array(self):
        df = self.spark.range(1).selectExpr("array(array(1, 2), array(3, 4)) as nested_array")
        # Input
        row = df.select(udf(lambda x: str(x))("nested_array")).first()
        self.assertIn(
            row[0], ["[[1, 2], [3, 4]]", "[[np.int32(1), np.int32(2)], [np.int32(3), np.int32(4)]]"]
        )
        # Output

        @udf(returnType=df.dtypes[0][1])
        def f(x):
            x.append([4, 5])
            return x

        row = df.select(f("nested_array")).first()
        self.assertEqual(row[0], [[1, 2], [3, 4], [4, 5]])

    def test_complex_return_types(self):
        row = (
            self.spark.range(1)
            .selectExpr("array(1, 2, 3) as array", "map('a', 'b') as map", "struct(1, 2) as struct")
            .select(
                udf(lambda x: x, "array<int>")("array"),
                udf(lambda x: x, "map<string,string>")("map"),
                udf(lambda x: x, "struct<col1:int,col2:int>")("struct"),
            )
            .first()
        )

        self.assertEqual(row[0], [1, 2, 3])
        self.assertEqual(row[1], {"a": "b"})
        self.assertEqual(row[2], Row(col1=1, col2=2))

    def test_named_arguments(self):
        @udf("int")
        def test_udf(a, b):
            return a + 10 * b

        self.spark.udf.register("test_udf", test_udf)

        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(a=col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(b=col("id") * 10, a=col("id"))),
                self.spark.sql("SELECT test_udf(id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(101)])

    def test_named_arguments_negative(self):
        @udf("int")
        def test_udf(a, b):
            return a + b

        self.spark.udf.register("test_udf", test_udf)

        with self.assertRaisesRegex(
            AnalysisException,
            "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
        ):
            self.spark.sql("SELECT test_udf(a => id, a => id * 10) FROM range(2)").show()

        with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
            self.spark.sql("SELECT test_udf(a => id, id * 10) FROM range(2)").show()

        with self.assertRaisesRegex(
            PythonException, r"test_udf\(\) got an unexpected keyword argument 'c'"
        ):
            self.spark.sql("SELECT test_udf(c => 'x') FROM range(2)").show()

        with self.assertRaisesRegex(
            PythonException, r"test_udf\(\) got multiple values for argument 'a'"
        ):
            self.spark.sql("SELECT test_udf(id, a => id * 10) FROM range(2)").show()

    def test_kwargs(self):
        @udf("int")
        def test_udf(**kwargs):
            return kwargs["a"] + 10 * kwargs["b"]

        self.spark.udf.register("test_udf", test_udf)

        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(a=col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(b=col("id") * 10, a=col("id"))),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(101)])

        # negative
        with self.assertRaisesRegex(
            AnalysisException,
            "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
        ):
            self.spark.sql("SELECT test_udf(a => id, a => id * 10) FROM range(2)").show()

        with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
            self.spark.sql("SELECT test_udf(a => id, id * 10) FROM range(2)").show()

    def test_named_arguments_and_defaults(self):
        @udf("int")
        def test_udf(a, b=0):
            return a + 10 * b

        self.spark.udf.register("test_udf", test_udf)

        # without "b"
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(col("id"))),
                self.spark.range(2).select(test_udf(a=col("id"))),
                self.spark.sql("SELECT test_udf(id) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(with_b=False, query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(1)])

        # with "b"
        for i, df in enumerate(
            [
                self.spark.range(2).select(test_udf(col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(a=col("id"), b=col("id") * 10)),
                self.spark.range(2).select(test_udf(b=col("id") * 10, a=col("id"))),
                self.spark.sql("SELECT test_udf(id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(a => id, b => id * 10) FROM range(2)"),
                self.spark.sql("SELECT test_udf(b => id * 10, a => id) FROM range(2)"),
            ]
        ):
            with self.subTest(with_b=True, query_no=i):
                assertDataFrameEqual(df, [Row(0), Row(101)])

    def test_raise_stop_iteration(self):
        @udf("int")
        def test_udf(a):
            if a < 5:
                return a
            else:
                raise StopIteration()

        assertDataFrameEqual(
            self.spark.range(5).select(test_udf(col("id"))), [Row(i) for i in range(5)]
        )

        with self.assertRaisesRegex(PythonException, "StopIteration"):
            self.spark.range(10).select(test_udf(col("id"))).show()

    @unittest.skipIf(
        "pypy" in platform.python_implementation().lower(), "cannot run in environment pypy"
    )
    def test_python_udf_segfault(self):
        with self.sql_conf({"spark.sql.execution.pyspark.udf.faulthandler.enabled": True}):
            with self.assertRaisesRegex(Exception, "Segmentation fault"):
                import ctypes

                self.spark.range(1).select(udf(lambda x: ctypes.string_at(0))("id")).collect()

    def test_err_udf_init(self):
        with self.quiet():
            self.check_err_udf_init()

    def check_err_udf_init(self):
        with self.assertRaises(PySparkTypeError) as pe:
            UserDefinedFunction("x", StringType())

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_CALLABLE",
            messageParameters={"arg_name": "func", "arg_type": "str"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            UserDefinedFunction(lambda x: x, 1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_DATATYPE_OR_STR",
            messageParameters={"arg_name": "returnType", "arg_type": "int"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            UserDefinedFunction(lambda x: x, StringType(), evalType="SQL_BATCHED_UDF")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_INT",
            messageParameters={"arg_name": "evalType", "arg_type": "str"},
        )


class UDFTests(BaseUDFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseUDFTestsMixin, cls).setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")


class UDFInitializationTests(unittest.TestCase):
    def tearDown(self):
        from pyspark import SparkContext

        if SparkSession._instantiatedSession is not None:
            SparkSession._instantiatedSession.stop()

        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

    def test_udf_init_should_not_initialize_context(self):
        from pyspark import SparkContext

        UserDefinedFunction(lambda x: x, StringType())

        self.assertIsNone(
            SparkContext._active_spark_context,
            "SparkContext shouldn't be initialized when UserDefinedFunction is created.",
        )
        self.assertIsNone(
            SparkSession._instantiatedSession,
            "SparkSession shouldn't be initialized when UserDefinedFunction is created.",
        )

    def test_err_parse_type_when_no_sc(self):
        with self.assertRaisesRegex(
            RuntimeError,
            "SparkContext or SparkSession should be created first",
        ):
            udf(lambda x: x, "integer")


if __name__ == "__main__":
    from pyspark.sql.tests.test_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
