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

import unittest

from pyspark.sql import Row
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    StructType,
    StructField,
    BooleanType,
)
from pyspark.errors import (
    AnalysisException,
    PySparkTypeError,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase


class DataFrameStatTestsMixin:
    def test_freqItems(self):
        vals = [Row(a=1, b=-2.0) if i % 2 == 0 else Row(a=i, b=i * 1.0) for i in range(100)]
        df = self.spark.createDataFrame(vals)
        items = df.stat.freqItems(("a", "b"), 0.4).collect()[0]
        self.assertTrue(1 in items[0])
        self.assertTrue(-2.0 in items[1])

    def test_dropna(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("height", DoubleType(), True),
            ]
        )

        # shouldn't drop a non-null row
        self.assertEqual(
            self.spark.createDataFrame([("Alice", 50, 80.1)], schema).dropna().count(), 1
        )

        # dropping rows with a single null value
        self.assertEqual(
            self.spark.createDataFrame([("Alice", None, 80.1)], schema).dropna().count(), 0
        )
        self.assertEqual(
            self.spark.createDataFrame([("Alice", None, 80.1)], schema).dropna(how="any").count(), 0
        )

        # if how = 'all', only drop rows if all values are null
        self.assertEqual(
            self.spark.createDataFrame([("Alice", None, 80.1)], schema).dropna(how="all").count(), 1
        )
        self.assertEqual(
            self.spark.createDataFrame([(None, None, None)], schema).dropna(how="all").count(), 0
        )

        # how and subset
        self.assertEqual(
            self.spark.createDataFrame([("Alice", 50, None)], schema)
            .dropna(how="any", subset=["name", "age"])
            .count(),
            1,
        )
        self.assertEqual(
            self.spark.createDataFrame([("Alice", None, None)], schema)
            .dropna(how="any", subset=["name", "age"])
            .count(),
            0,
        )

        # threshold
        self.assertEqual(
            self.spark.createDataFrame([("Alice", None, 80.1)], schema).dropna(thresh=2).count(), 1
        )
        self.assertEqual(
            self.spark.createDataFrame([("Alice", None, None)], schema).dropna(thresh=2).count(), 0
        )

        # threshold and subset
        self.assertEqual(
            self.spark.createDataFrame([("Alice", 50, None)], schema)
            .dropna(thresh=2, subset=["name", "age"])
            .count(),
            1,
        )
        self.assertEqual(
            self.spark.createDataFrame([("Alice", None, 180.9)], schema)
            .dropna(thresh=2, subset=["name", "age"])
            .count(),
            0,
        )

        # thresh should take precedence over how
        self.assertEqual(
            self.spark.createDataFrame([("Alice", 50, None)], schema)
            .dropna(how="any", thresh=2, subset=["name", "age"])
            .count(),
            1,
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame([("Alice", 50, None)], schema).dropna(subset=10)

        self.check_error(
            exception=pe.exception,
            error_class="NOT_LIST_OR_STR_OR_TUPLE",
            message_parameters={"arg_name": "subset", "arg_type": "int"},
        )

    def test_fillna(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("height", DoubleType(), True),
                StructField("spy", BooleanType(), True),
            ]
        )

        # fillna shouldn't change non-null values
        row = self.spark.createDataFrame([("Alice", 10, 80.1, True)], schema).fillna(50).first()
        self.assertEqual(row.age, 10)

        # fillna with int
        row = self.spark.createDataFrame([("Alice", None, None, None)], schema).fillna(50).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.0)

        # fillna with double
        row = self.spark.createDataFrame([("Alice", None, None, None)], schema).fillna(50.1).first()
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, 50.1)

        # fillna with bool
        row = self.spark.createDataFrame([("Alice", None, None, None)], schema).fillna(True).first()
        self.assertEqual(row.age, None)
        self.assertEqual(row.spy, True)

        # fillna with string
        row = self.spark.createDataFrame([(None, None, None, None)], schema).fillna("hello").first()
        self.assertEqual(row.name, "hello")
        self.assertEqual(row.age, None)

        # fillna with subset specified for numeric cols
        row = (
            self.spark.createDataFrame([(None, None, None, None)], schema)
            .fillna(50, subset=["name", "age"])
            .first()
        )
        self.assertEqual(row.name, None)
        self.assertEqual(row.age, 50)
        self.assertEqual(row.height, None)
        self.assertEqual(row.spy, None)

        # fillna with subset specified for string cols
        row = (
            self.spark.createDataFrame([(None, None, None, None)], schema)
            .fillna("haha", subset=["name", "age"])
            .first()
        )
        self.assertEqual(row.name, "haha")
        self.assertEqual(row.age, None)
        self.assertEqual(row.height, None)
        self.assertEqual(row.spy, None)

        # fillna with subset specified for bool cols
        row = (
            self.spark.createDataFrame([(None, None, None, None)], schema)
            .fillna(True, subset=["name", "spy"])
            .first()
        )
        self.assertEqual(row.name, None)
        self.assertEqual(row.age, None)
        self.assertEqual(row.height, None)
        self.assertEqual(row.spy, True)

        # fillna with dictionary for boolean types
        row = self.spark.createDataFrame([Row(a=None), Row(a=True)]).fillna({"a": True}).first()
        self.assertEqual(row.a, True)

        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame([Row(a=None), Row(a=True)]).fillna(["a", True])

        self.check_error(
            exception=pe.exception,
            error_class="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR",
            message_parameters={"arg_name": "value", "arg_type": "list"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame([Row(a=None), Row(a=True)]).fillna(50, subset=10)

        self.check_error(
            exception=pe.exception,
            error_class="NOT_LIST_OR_TUPLE",
            message_parameters={"arg_name": "subset", "arg_type": "int"},
        )

    def test_replace(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("height", DoubleType(), True),
            ]
        )

        # replace with int
        row = self.spark.createDataFrame([("Alice", 10, 10.0)], schema).replace(10, 20).first()
        self.assertEqual(row.age, 20)
        self.assertEqual(row.height, 20.0)

        # replace with double
        row = self.spark.createDataFrame([("Alice", 80, 80.0)], schema).replace(80.0, 82.1).first()
        self.assertEqual(row.age, 82)
        self.assertEqual(row.height, 82.1)

        # replace with string
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema)
            .replace("Alice", "Ann")
            .first()
        )
        self.assertEqual(row.name, "Ann")
        self.assertEqual(row.age, 10)

        # replace with subset specified by a string of a column name w/ actual change
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema)
            .replace(10, 20, subset="age")
            .first()
        )
        self.assertEqual(row.age, 20)

        # replace with subset specified by a string of a column name w/o actual change
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema)
            .replace(10, 20, subset="height")
            .first()
        )
        self.assertEqual(row.age, 10)

        # replace with subset specified with one column replaced, another column not in subset
        # stays unchanged.
        row = (
            self.spark.createDataFrame([("Alice", 10, 10.0)], schema)
            .replace(10, 20, subset=["name", "age"])
            .first()
        )
        self.assertEqual(row.name, "Alice")
        self.assertEqual(row.age, 20)
        self.assertEqual(row.height, 10.0)

        # replace with subset specified but no column will be replaced
        row = (
            self.spark.createDataFrame([("Alice", 10, None)], schema)
            .replace(10, 20, subset=["name", "height"])
            .first()
        )
        self.assertEqual(row.name, "Alice")
        self.assertEqual(row.age, 10)
        self.assertEqual(row.height, None)

        # replace with lists
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema)
            .replace(["Alice"], ["Ann"])
            .first()
        )
        self.assertTupleEqual(row, ("Ann", 10, 80.1))

        # replace with dict
        row = self.spark.createDataFrame([("Alice", 10, 80.1)], schema).replace({10: 11}).first()
        self.assertTupleEqual(row, ("Alice", 11, 80.1))

        # test backward compatibility with dummy value
        dummy_value = 1
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema)
            .replace({"Alice": "Bob"}, dummy_value)
            .first()
        )
        self.assertTupleEqual(row, ("Bob", 10, 80.1))

        # test dict with mixed numerics
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema)
            .replace({10: -10, 80.1: 90.5})
            .first()
        )
        self.assertTupleEqual(row, ("Alice", -10, 90.5))

        # replace with tuples
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema)
            .replace(("Alice",), ("Bob",))
            .first()
        )
        self.assertTupleEqual(row, ("Bob", 10, 80.1))

        # replace multiple columns
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema)
            .replace((10, 80.0), (20, 90))
            .first()
        )
        self.assertTupleEqual(row, ("Alice", 20, 90.0))

        # test for mixed numerics
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema)
            .replace((10, 80), (20, 90.5))
            .first()
        )
        self.assertTupleEqual(row, ("Alice", 20, 90.5))

        row = (
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema)
            .replace({10: 20, 80: 90.5})
            .first()
        )
        self.assertTupleEqual(row, ("Alice", 20, 90.5))

        # replace with boolean
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema)
            .selectExpr("name = 'Bob'", "age <= 15")
            .replace(False, True)
            .first()
        )
        self.assertTupleEqual(row, (True, True))

        # replace string with None and then drop None rows
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema)
            .replace("Alice", None)
            .dropna()
        )
        self.assertEqual(row.count(), 0)

        # replace with number and None
        row = (
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema)
            .replace([10, 80], [20, None])
            .first()
        )
        self.assertTupleEqual(row, ("Alice", 20, None))

        # should fail if subset is not list, tuple or None
        with self.assertRaises(TypeError):
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema).replace(
                {10: 11}, subset=1
            ).first()

        # should fail if to_replace and value have different length
        with self.assertRaises(ValueError):
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema).replace(
                ["Alice", "Bob"], ["Eve"]
            ).first()

        # should fail if when received unexpected type
        with self.assertRaises(TypeError):
            from datetime import datetime

            self.spark.createDataFrame([("Alice", 10, 80.1)], schema).replace(
                datetime.now(), datetime.now()
            ).first()

        # should fail if provided mixed type replacements
        with self.assertRaises(ValueError):
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema).replace(
                ["Alice", 10], ["Eve", 20]
            ).first()

        with self.assertRaises(ValueError):
            self.spark.createDataFrame([("Alice", 10, 80.1)], schema).replace(
                {"Alice": "Bob", 10: 20}
            ).first()

        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema).replace(["Alice", "Bob"])

        self.check_error(
            exception=pe.exception,
            error_class="ARGUMENT_REQUIRED",
            message_parameters={"arg_name": "value", "condition": "`to_replace` is dict"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame([("Alice", 10, 80.0)], schema).replace(lambda x: x + 1, 10)

        self.check_error(
            exception=pe.exception,
            error_class="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
            message_parameters={"arg_name": "to_replace", "arg_type": "function"},
        )

    def test_unpivot(self):
        # SPARK-39877: test the DataFrame.unpivot method
        df = self.spark.createDataFrame(
            [
                (1, 10, 1.0, "one"),
                (2, 20, 2.0, "two"),
                (3, 30, 3.0, "three"),
            ],
            ["id", "int", "double", "str"],
        )

        with self.subTest(desc="with none identifier"):
            with self.assertRaisesRegex(AssertionError, "ids must not be None"):
                df.unpivot(None, ["int", "double"], "var", "val")

        with self.subTest(desc="with no identifier"):
            for id in [[], ()]:
                with self.subTest(ids=id):
                    actual = df.unpivot(id, ["int", "double"], "var", "val")
                    self.assertEqual(actual.schema.simpleString(), "struct<var:string,val:double>")
                    self.assertEqual(
                        actual.collect(),
                        [
                            Row(var="int", value=10.0),
                            Row(var="double", value=1.0),
                            Row(var="int", value=20.0),
                            Row(var="double", value=2.0),
                            Row(var="int", value=30.0),
                            Row(var="double", value=3.0),
                        ],
                    )

        with self.subTest(desc="with single identifier column"):
            for id in ["id", ["id"], ("id",)]:
                with self.subTest(ids=id):
                    actual = df.unpivot(id, ["int", "double"], "var", "val")
                    self.assertEqual(
                        actual.schema.simpleString(),
                        "struct<id:bigint,var:string,val:double>",
                    )
                    self.assertEqual(
                        actual.collect(),
                        [
                            Row(id=1, var="int", value=10.0),
                            Row(id=1, var="double", value=1.0),
                            Row(id=2, var="int", value=20.0),
                            Row(id=2, var="double", value=2.0),
                            Row(id=3, var="int", value=30.0),
                            Row(id=3, var="double", value=3.0),
                        ],
                    )

        with self.subTest(desc="with multiple identifier columns"):
            for ids in [["id", "double"], ("id", "double")]:
                with self.subTest(ids=ids):
                    actual = df.unpivot(ids, ["int", "double"], "var", "val")
                    self.assertEqual(
                        actual.schema.simpleString(),
                        "struct<id:bigint,double:double,var:string,val:double>",
                    )
                    self.assertEqual(
                        actual.collect(),
                        [
                            Row(id=1, double=1.0, var="int", value=10.0),
                            Row(id=1, double=1.0, var="double", value=1.0),
                            Row(id=2, double=2.0, var="int", value=20.0),
                            Row(id=2, double=2.0, var="double", value=2.0),
                            Row(id=3, double=3.0, var="int", value=30.0),
                            Row(id=3, double=3.0, var="double", value=3.0),
                        ],
                    )

        with self.subTest(desc="with no identifier columns but none value columns"):
            # select only columns that have common data type (double)
            actual = df.select("id", "int", "double").unpivot([], None, "var", "val")
            self.assertEqual(actual.schema.simpleString(), "struct<var:string,val:double>")
            self.assertEqual(
                actual.collect(),
                [
                    Row(var="id", value=1.0),
                    Row(var="int", value=10.0),
                    Row(var="double", value=1.0),
                    Row(var="id", value=2.0),
                    Row(var="int", value=20.0),
                    Row(var="double", value=2.0),
                    Row(var="id", value=3.0),
                    Row(var="int", value=30.0),
                    Row(var="double", value=3.0),
                ],
            )

        with self.subTest(desc="with single identifier columns but none value columns"):
            for ids in ["id", ["id"], ("id",)]:
                with self.subTest(ids=ids):
                    # select only columns that have common data type (double)
                    actual = df.select("id", "int", "double").unpivot(ids, None, "var", "val")
                    self.assertEqual(
                        actual.schema.simpleString(), "struct<id:bigint,var:string,val:double>"
                    )
                    self.assertEqual(
                        actual.collect(),
                        [
                            Row(id=1, var="int", value=10.0),
                            Row(id=1, var="double", value=1.0),
                            Row(id=2, var="int", value=20.0),
                            Row(id=2, var="double", value=2.0),
                            Row(id=3, var="int", value=30.0),
                            Row(id=3, var="double", value=3.0),
                        ],
                    )

        with self.subTest(desc="with multiple identifier columns but none given value columns"):
            for ids in [["id", "str"], ("id", "str")]:
                with self.subTest(ids=ids):
                    actual = df.unpivot(ids, None, "var", "val")
                    self.assertEqual(
                        actual.schema.simpleString(),
                        "struct<id:bigint,str:string,var:string,val:double>",
                    )
                    self.assertEqual(
                        actual.collect(),
                        [
                            Row(id=1, str="one", var="int", val=10.0),
                            Row(id=1, str="one", var="double", val=1.0),
                            Row(id=2, str="two", var="int", val=20.0),
                            Row(id=2, str="two", var="double", val=2.0),
                            Row(id=3, str="three", var="int", val=30.0),
                            Row(id=3, str="three", var="double", val=3.0),
                        ],
                    )

        with self.subTest(desc="with single value column"):
            for values in ["int", ["int"], ("int",)]:
                with self.subTest(values=values):
                    actual = df.unpivot("id", values, "var", "val")
                    self.assertEqual(
                        actual.schema.simpleString(), "struct<id:bigint,var:string,val:bigint>"
                    )
                    self.assertEqual(
                        actual.collect(),
                        [
                            Row(id=1, var="int", val=10),
                            Row(id=2, var="int", val=20),
                            Row(id=3, var="int", val=30),
                        ],
                    )

        with self.subTest(desc="with multiple value columns"):
            for values in [["int", "double"], ("int", "double")]:
                with self.subTest(values=values):
                    actual = df.unpivot("id", values, "var", "val")
                    self.assertEqual(
                        actual.schema.simpleString(), "struct<id:bigint,var:string,val:double>"
                    )
                    self.assertEqual(
                        actual.collect(),
                        [
                            Row(id=1, var="int", val=10.0),
                            Row(id=1, var="double", val=1.0),
                            Row(id=2, var="int", val=20.0),
                            Row(id=2, var="double", val=2.0),
                            Row(id=3, var="int", val=30.0),
                            Row(id=3, var="double", val=3.0),
                        ],
                    )

        with self.subTest(desc="with columns"):
            for id in [df.id, [df.id], (df.id,)]:
                for values in [[df.int, df.double], (df.int, df.double)]:
                    with self.subTest(ids=id, values=values):
                        self.assertEqual(
                            df.unpivot(id, values, "var", "val").collect(),
                            df.unpivot("id", ["int", "double"], "var", "val").collect(),
                        )

        with self.subTest(desc="with column names and columns"):
            for ids in [[df.id, "str"], (df.id, "str")]:
                for values in [[df.int, "double"], (df.int, "double")]:
                    with self.subTest(ids=ids, values=values):
                        self.assertEqual(
                            df.unpivot(ids, values, "var", "val").collect(),
                            df.unpivot(["id", "str"], ["int", "double"], "var", "val").collect(),
                        )

        with self.subTest(desc="melt alias"):
            self.assertEqual(
                df.unpivot("id", ["int", "double"], "var", "val").collect(),
                df.melt("id", ["int", "double"], "var", "val").collect(),
            )

    def test_unpivot_negative(self):
        # SPARK-39877: test the DataFrame.unpivot method
        df = self.spark.createDataFrame(
            [
                (1, 10, 1.0, "one"),
                (2, 20, 2.0, "two"),
                (3, 30, 3.0, "three"),
            ],
            ["id", "int", "double", "str"],
        )

        with self.subTest(desc="with no value columns"):
            for values in [[], ()]:
                with self.subTest(values=values):
                    with self.assertRaisesRegex(
                        AnalysisException,
                        r"\[UNPIVOT_REQUIRES_VALUE_COLUMNS] At least one value column "
                        r"needs to be specified for UNPIVOT, all columns specified as ids.*",
                    ):
                        df.unpivot("id", values, "var", "val").collect()

        with self.subTest(desc="with value columns without common data type"):
            with self.assertRaisesRegex(
                AnalysisException,
                r"\[UNPIVOT_VALUE_DATA_TYPE_MISMATCH\] Unpivot value columns must share "
                r"a least common type, some types do not: .*",
            ):
                df.unpivot("id", ["int", "str"], "var", "val").collect()

    def test_melt_groupby(self):
        df = self.spark.createDataFrame(
            [(1, 2, 3, 4, 5, 6)],
            ["f1", "f2", "label", "pred", "model_version", "ts"],
        )
        self.assertEqual(
            df.melt(
                "model_version",
                ["label", "f2"],
                "f1",
                "f2",
            )
            .groupby("f1")
            .count()
            .count(),
            2,
        )


class DataFrameStatTests(
    DataFrameStatTestsMixin,
    ReusedSQLTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_stat import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
