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

import array
import datetime
import unittest
import random
import string

from pyspark.errors import PySparkValueError
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    MapType,
    ArrayType,
    Row,
)
from pyspark.testing.sqlutils import MyObject, PythonOnlyUDT

from pyspark.testing.connectutils import should_test_connect
from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase

if should_test_connect:
    import pandas as pd
    import numpy as np
    from pyspark.sql import functions as SF
    from pyspark.sql.connect import functions as CF
    from pyspark.errors.exceptions.connect import ParseException


class SparkConnectCreationTests(SparkConnectSQLTestCase):
    def test_with_local_data(self):
        """SPARK-41114: Test creating a dataframe using local data"""
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        df = self.connect.createDataFrame(pdf)
        rows = df.filter(df.a == CF.lit(3)).collect()
        self.assertTrue(len(rows) == 1)
        self.assertEqual(rows[0][0], 3)
        self.assertEqual(rows[0][1], "c")

        # Check correct behavior for empty DataFrame
        pdf = pd.DataFrame({"a": []})
        with self.assertRaises(ValueError):
            self.connect.createDataFrame(pdf)

    def test_with_local_ndarray(self):
        """SPARK-41446: Test creating a dataframe using local list"""
        data = np.array([[1, 2, 3, 4], [5, 6, 7, 8]])

        sdf = self.spark.createDataFrame(data)
        cdf = self.connect.createDataFrame(data)
        self.assertEqual(sdf.schema, cdf.schema)
        self.assert_eq(sdf.toPandas(), cdf.toPandas())

        for schema in [
            StructType(
                [
                    StructField("col1", IntegerType(), True),
                    StructField("col2", IntegerType(), True),
                    StructField("col3", IntegerType(), True),
                    StructField("col4", IntegerType(), True),
                ]
            ),
            "struct<col1 int, col2 int, col3 int, col4 int>",
            "col1 int, col2 int, col3 int, col4 int",
            "col1 int, col2 long, col3 string, col4 long",
            "col1 int, col2 string, col3 short, col4 long",
            ["a", "b", "c", "d"],
            ("x1", "x2", "x3", "x4"),
        ]:
            with self.subTest(schema=schema):
                sdf = self.spark.createDataFrame(data, schema=schema)
                cdf = self.connect.createDataFrame(data, schema=schema)

                self.assertEqual(sdf.schema, cdf.schema)
                self.assert_eq(sdf.toPandas(), cdf.toPandas())

        with self.assertRaises(PySparkValueError) as pe:
            self.connect.createDataFrame(data, ["a", "b", "c", "d", "e"])

        self.check_error(
            exception=pe.exception,
            errorClass="AXIS_LENGTH_MISMATCH",
            messageParameters={"expected_length": "5", "actual_length": "4"},
        )

        with self.assertRaises(ParseException):
            self.connect.createDataFrame(data, "col1 magic_type, col2 int, col3 int, col4 int")

        with self.assertRaises(PySparkValueError) as pe:
            self.connect.createDataFrame(data, "col1 int, col2 int, col3 int")

        self.check_error(
            exception=pe.exception,
            errorClass="AXIS_LENGTH_MISMATCH",
            messageParameters={"expected_length": "3", "actual_length": "4"},
        )

        # test 1 dim ndarray
        data = np.array([1.0, 2.0, np.nan, 3.0, 4.0, float("NaN"), 5.0])
        self.assertEqual(data.ndim, 1)

        sdf = self.spark.createDataFrame(data)
        cdf = self.connect.createDataFrame(data)
        self.assertEqual(sdf.schema, cdf.schema)
        self.assert_eq(sdf.toPandas(), cdf.toPandas())

    def test_with_local_list(self):
        """SPARK-41446: Test creating a dataframe using local list"""
        data = [[1, 2, 3, 4]]

        sdf = self.spark.createDataFrame(data)
        cdf = self.connect.createDataFrame(data)
        self.assertEqual(sdf.schema, cdf.schema)
        self.assert_eq(sdf.toPandas(), cdf.toPandas())

        for schema in [
            "struct<col1 int, col2 int, col3 int, col4 int>",
            "col1 int, col2 int, col3 int, col4 int",
            "col1 int, col2 long, col3 string, col4 long",
            "col1 int, col2 string, col3 short, col4 long",
            ["a", "b", "c", "d"],
            ("x1", "x2", "x3", "x4"),
        ]:
            sdf = self.spark.createDataFrame(data, schema=schema)
            cdf = self.connect.createDataFrame(data, schema=schema)

            self.assertEqual(sdf.schema, cdf.schema)
            self.assert_eq(sdf.toPandas(), cdf.toPandas())

        with self.assertRaises(PySparkValueError) as pe:
            self.connect.createDataFrame(data, ["a", "b", "c", "d", "e"])

        self.check_error(
            exception=pe.exception,
            errorClass="AXIS_LENGTH_MISMATCH",
            messageParameters={"expected_length": "5", "actual_length": "4"},
        )

        with self.assertRaises(ParseException):
            self.connect.createDataFrame(data, "col1 magic_type, col2 int, col3 int, col4 int")

        with self.assertRaises(PySparkValueError) as pe:
            self.connect.createDataFrame(data, "col1 int, col2 int, col3 int")

        self.check_error(
            exception=pe.exception,
            errorClass="AXIS_LENGTH_MISMATCH",
            messageParameters={"expected_length": "3", "actual_length": "4"},
        )

    def test_with_local_rows(self):
        # SPARK-41789, SPARK-41810: Test creating a dataframe with list of rows and dictionaries
        rows = [
            Row(course="dotNET", year=2012, earnings=10000),
            Row(course="Java", year=2012, earnings=20000),
            Row(course="dotNET", year=2012, earnings=5000),
            Row(course="dotNET", year=2013, earnings=48000),
            Row(course="Java", year=2013, earnings=30000),
            Row(course="Scala", year=2022, earnings=None),
        ]
        dicts = [row.asDict() for row in rows]

        for data in [rows, dicts]:
            sdf = self.spark.createDataFrame(data)
            cdf = self.connect.createDataFrame(data)

            self.assertEqual(sdf.schema, cdf.schema)
            self.assert_eq(sdf.toPandas(), cdf.toPandas())

            # test with rename
            sdf = self.spark.createDataFrame(data, schema=["a", "b", "c"])
            cdf = self.connect.createDataFrame(data, schema=["a", "b", "c"])

            self.assertEqual(sdf.schema, cdf.schema)
            self.assert_eq(sdf.toPandas(), cdf.toPandas())

    def test_streaming_local_relation(self):
        threshold_conf = "spark.sql.session.localRelationCacheThreshold"
        old_threshold = self.connect.conf.get(threshold_conf)
        threshold = 1024 * 1024
        self.connect.conf.set(threshold_conf, threshold)
        try:
            suffix = "abcdef"
            letters = string.ascii_lowercase
            str = "".join(random.choice(letters) for i in range(threshold)) + suffix
            data = [[0, str], [1, str]]
            for i in range(0, 2):
                cdf = self.connect.createDataFrame(data, ["a", "b"])
                self.assert_eq(cdf.count(), len(data))
                self.assert_eq(cdf.filter(f"endsWith(b, '{suffix}')").isEmpty(), False)
        finally:
            self.connect.conf.set(threshold_conf, old_threshold)

    def test_with_atom_type(self):
        for data in [[(1), (2), (3)], [1, 2, 3]]:
            for schema in ["long", "int", "short"]:
                sdf = self.spark.createDataFrame(data, schema=schema)
                cdf = self.connect.createDataFrame(data, schema=schema)

                self.assertEqual(sdf.schema, cdf.schema)
                self.assert_eq(sdf.toPandas(), cdf.toPandas())

    def test_with_none_and_nan(self):
        # SPARK-41855: make createDataFrame support None and NaN
        # SPARK-41814: test with eqNullSafe
        data1 = [Row(id=1, value=float("NaN")), Row(id=2, value=42.0), Row(id=3, value=None)]
        data2 = [Row(id=1, value=np.nan), Row(id=2, value=42.0), Row(id=3, value=None)]
        data3 = [
            {"id": 1, "value": float("NaN")},
            {"id": 2, "value": 42.0},
            {"id": 3, "value": None},
        ]
        data4 = [{"id": 1, "value": np.nan}, {"id": 2, "value": 42.0}, {"id": 3, "value": None}]
        data5 = [(1, float("NaN")), (2, 42.0), (3, None)]
        data6 = [(1, np.nan), (2, 42.0), (3, None)]
        data7 = np.array([[1, float("NaN")], [2, 42.0], [3, None]])
        data8 = np.array([[1, np.nan], [2, 42.0], [3, None]])

        # +---+-----+
        # | id|value|
        # +---+-----+
        # |  1|  NaN|
        # |  2| 42.0|
        # |  3| NULL|
        # +---+-----+

        for data in [data1, data2, data3, data4, data5, data6, data7, data8]:
            if isinstance(data[0], (Row, dict)):
                # data1, data2, data3, data4
                cdf = self.connect.createDataFrame(data)
                sdf = self.spark.createDataFrame(data)
            else:
                # data5, data6, data7, data8
                cdf = self.connect.createDataFrame(data, schema=["id", "value"])
                sdf = self.spark.createDataFrame(data, schema=["id", "value"])

            self.assert_eq(cdf.toPandas(), sdf.toPandas())

            self.assert_eq(
                cdf.select(
                    cdf["value"].eqNullSafe(None),
                    cdf["value"].eqNullSafe(float("NaN")),
                    cdf["value"].eqNullSafe(42.0),
                ).toPandas(),
                sdf.select(
                    sdf["value"].eqNullSafe(None),
                    sdf["value"].eqNullSafe(float("NaN")),
                    sdf["value"].eqNullSafe(42.0),
                ).toPandas(),
            )

        # SPARK-41851: test with nanvl
        data = [(1.0, float("nan")), (float("nan"), 2.0)]

        cdf = self.connect.createDataFrame(data, ("a", "b"))
        sdf = self.spark.createDataFrame(data, ("a", "b"))

        self.assert_eq(cdf.toPandas(), sdf.toPandas())

        self.assert_eq(
            cdf.select(
                CF.nanvl("a", "b").alias("r1"), CF.nanvl(cdf.a, cdf.b).alias("r2")
            ).toPandas(),
            sdf.select(
                SF.nanvl("a", "b").alias("r1"), SF.nanvl(sdf.a, sdf.b).alias("r2")
            ).toPandas(),
        )

        # SPARK-41852: test with pmod
        data = [
            (1.0, float("nan")),
            (float("nan"), 2.0),
            (10.0, 3.0),
            (float("nan"), float("nan")),
            (-3.0, 4.0),
            (-10.0, 3.0),
            (-5.0, -6.0),
            (7.0, -8.0),
            (1.0, 2.0),
        ]

        cdf = self.connect.createDataFrame(data, ("a", "b"))
        sdf = self.spark.createDataFrame(data, ("a", "b"))

        self.assert_eq(cdf.toPandas(), sdf.toPandas())

        self.assert_eq(
            cdf.select(CF.pmod("a", "b")).toPandas(),
            sdf.select(SF.pmod("a", "b")).toPandas(),
        )

    def test_cast_with_ddl(self):
        data = [Row(date=datetime.date(2021, 12, 27), add=2)]

        cdf = self.connect.createDataFrame(data, "date date, add integer")
        sdf = self.spark.createDataFrame(data, "date date, add integer")

        self.assertEqual(cdf.schema, sdf.schema)

    def test_create_empty_df(self):
        for schema in [
            "STRING",
            "x STRING",
            "x STRING, y INTEGER",
            StringType(),
            StructType(
                [
                    StructField("x", StringType(), True),
                    StructField("y", IntegerType(), True),
                ]
            ),
        ]:
            cdf = self.connect.createDataFrame(data=[], schema=schema)
            sdf = self.spark.createDataFrame(data=[], schema=schema)

            self.assert_eq(cdf.toPandas(), sdf.toPandas())

        # check error
        with self.assertRaises(PySparkValueError) as pe:
            self.connect.createDataFrame(data=[])

        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_INFER_EMPTY_SCHEMA",
            messageParameters={},
        )

    def test_create_dataframe_from_arrays(self):
        # SPARK-42021: createDataFrame support array.array
        data1 = [Row(a=1, b=array.array("i", [1, 2, 3]), c=array.array("d", [4, 5, 6]))]
        data2 = [(array.array("d", [1, 2, 3]), 2, "3")]
        data3 = [{"a": 1, "b": array.array("i", [1, 2, 3])}]

        for data in [data1, data2, data3]:
            cdf = self.connect.createDataFrame(data)
            sdf = self.spark.createDataFrame(data)

            self.assertEqual(cdf.schema, sdf.schema)
            self.assertEqual(cdf.collect(), sdf.collect())

    def test_timestampe_create_from_rows(self):
        data = [(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)]

        cdf = self.connect.createDataFrame(data, ["date", "val"])
        sdf = self.spark.createDataFrame(data, ["date", "val"])

        self.assertEqual(cdf.schema, sdf.schema)
        self.assertEqual(cdf.collect(), sdf.collect())

    def test_create_dataframe_with_coercion(self):
        data1 = [[1.33, 1], ["2.1", 1]]
        data2 = [[True, 1], ["false", 1]]

        for data in [data1, data2]:
            cdf = self.connect.createDataFrame(data, ["a", "b"])
            sdf = self.spark.createDataFrame(data, ["a", "b"])

            self.assertEqual(cdf.schema, sdf.schema)
            self.assertEqual(cdf.collect(), sdf.collect())

    def test_nested_type_create_from_rows(self):
        data1 = [Row(a=1, b=Row(c=2, d=Row(e=3, f=Row(g=4, h=Row(i=5)))))]
        # root
        # |-- a: long (nullable = true)
        # |-- b: struct (nullable = true)
        # |    |-- c: long (nullable = true)
        # |    |-- d: struct (nullable = true)
        # |    |    |-- e: long (nullable = true)
        # |    |    |-- f: struct (nullable = true)
        # |    |    |    |-- g: long (nullable = true)
        # |    |    |    |-- h: struct (nullable = true)
        # |    |    |    |    |-- i: long (nullable = true)

        data2 = [
            (
                1,
                "a",
                Row(
                    a=1,
                    b=[1, 2, 3],
                    c={"a": "b"},
                    d=Row(x=1, y="y", z=Row(o=1, p=2, q=Row(g=1.5))),
                ),
            )
        ]
        # root
        # |-- _1: long (nullable = true)
        # |-- _2: string (nullable = true)
        # |-- _3: struct (nullable = true)
        # |    |-- a: long (nullable = true)
        # |    |-- b: array (nullable = true)
        # |    |    |-- element: long (containsNull = true)
        # |    |-- c: map (nullable = true)
        # |    |    |-- key: string
        # |    |    |-- value: string (valueContainsNull = true)
        # |    |-- d: struct (nullable = true)
        # |    |    |-- x: long (nullable = true)
        # |    |    |-- y: string (nullable = true)
        # |    |    |-- z: struct (nullable = true)
        # |    |    |    |-- o: long (nullable = true)
        # |    |    |    |-- p: long (nullable = true)
        # |    |    |    |-- q: struct (nullable = true)
        # |    |    |    |    |-- g: double (nullable = true)

        data3 = [
            Row(
                a=1,
                b=[1, 2, 3],
                c={"a": "b"},
                d=Row(x=1, y="y", z=Row(1, 2, 3)),
                e=list("hello connect"),
            )
        ]
        # root
        # |-- a: long (nullable = true)
        # |-- b: array (nullable = true)
        # |    |-- element: long (containsNull = true)
        # |-- c: map (nullable = true)
        # |    |-- key: string
        # |    |-- value: string (valueContainsNull = true)
        # |-- d: struct (nullable = true)
        # |    |-- x: long (nullable = true)
        # |    |-- y: string (nullable = true)
        # |    |-- z: struct (nullable = true)
        # |    |    |-- _1: long (nullable = true)
        # |    |    |-- _2: long (nullable = true)
        # |    |    |-- _3: long (nullable = true)
        # |-- e: array (nullable = true)
        # |    |-- element: string (containsNull = true)

        data4 = [
            {
                "a": 1,
                "b": Row(x=1, y=Row(z=2)),
                "c": {"x": -1, "y": 2},
                "d": [1, 2, 3, 4, 5],
            }
        ]
        # root
        # |-- a: long (nullable = true)
        # |-- b: struct (nullable = true)
        # |    |-- x: long (nullable = true)
        # |    |-- y: struct (nullable = true)
        # |    |    |-- z: long (nullable = true)
        # |-- c: map (nullable = true)
        # |    |-- key: string
        # |    |-- value: long (valueContainsNull = true)
        # |-- d: array (nullable = true)
        # |    |-- element: long (containsNull = true)

        data5 = [
            {
                "a": [Row(x=1, y="2"), Row(x=-1, y="-2")],
                "b": [[1, 2, 3], [4, 5], [6]],
                "c": {3: {4: {5: 6}}, 7: {8: {9: 0}}},
            }
        ]
        # root
        # |-- a: array (nullable = true)
        # |    |-- element: struct (containsNull = true)
        # |    |    |-- x: long (nullable = true)
        # |    |    |-- y: string (nullable = true)
        # |-- b: array (nullable = true)
        # |    |-- element: array (containsNull = true)
        # |    |    |-- element: long (containsNull = true)
        # |-- c: map (nullable = true)
        # |    |-- key: long
        # |    |-- value: map (valueContainsNull = true)
        # |    |    |-- key: long
        # |    |    |-- value: map (valueContainsNull = true)
        # |    |    |    |-- key: long
        # |    |    |    |-- value: long (valueContainsNull = true)

        for data in [data1, data2, data3, data4, data5]:
            with self.subTest(data=data):
                cdf = self.connect.createDataFrame(data)
                sdf = self.spark.createDataFrame(data)

                self.assertEqual(cdf.schema, sdf.schema)
                self.assertEqual(cdf.collect(), sdf.collect())

    def test_create_df_from_objects(self):
        data = [MyObject(1, "1"), MyObject(2, "2")]

        # +---+-----+
        # |key|value|
        # +---+-----+
        # |  1|    1|
        # |  2|    2|
        # +---+-----+

        cdf = self.connect.createDataFrame(data)
        sdf = self.spark.createDataFrame(data)

        self.assertEqual(cdf.schema, sdf.schema)
        self.assertEqual(cdf.collect(), sdf.collect())

    def test_create_df_nullability(self):
        data = [("asd", None)]
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField("age", IntegerType(), nullable=False),
            ]
        )

        with self.assertRaises(PySparkValueError):
            self.spark.createDataFrame(data, schema)

    def test_create_dataframe_from_pandas_with_ns_timestamp(self):
        """Truncate the timestamps for nanoseconds."""
        from datetime import datetime, timezone, timedelta
        from pandas import Timestamp
        import pandas as pd

        pdf = pd.DataFrame(
            {
                "naive": [datetime(2019, 1, 1, 0)],
                "aware": [
                    Timestamp(
                        year=2019, month=1, day=1, nanosecond=500, tz=timezone(timedelta(hours=-8))
                    )
                ],
            }
        )

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            self.assertEqual(
                self.connect.createDataFrame(pdf).collect(),
                self.spark.createDataFrame(pdf).collect(),
            )

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": True}):
            self.assertEqual(
                self.connect.createDataFrame(pdf).collect(),
                self.spark.createDataFrame(pdf).collect(),
            )

    def test_schema_has_nullable(self):
        schema_false = StructType().add("id", IntegerType(), False)
        cdf1 = self.connect.createDataFrame([[1]], schema=schema_false)
        sdf1 = self.spark.createDataFrame([[1]], schema=schema_false)
        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertEqual(cdf1.collect(), sdf1.collect())

        schema_true = StructType().add("id", IntegerType(), True)
        cdf2 = self.connect.createDataFrame([[1]], schema=schema_true)
        sdf2 = self.spark.createDataFrame([[1]], schema=schema_true)
        self.assertEqual(cdf2.schema, sdf2.schema)
        self.assertEqual(cdf2.collect(), sdf2.collect())

        pdf1 = cdf1.toPandas()
        cdf3 = self.connect.createDataFrame(pdf1, cdf1.schema)
        sdf3 = self.spark.createDataFrame(pdf1, sdf1.schema)
        self.assertEqual(cdf3.schema, sdf3.schema)
        self.assertEqual(cdf3.collect(), sdf3.collect())

        pdf2 = cdf2.toPandas()
        cdf4 = self.connect.createDataFrame(pdf2, cdf2.schema)
        sdf4 = self.spark.createDataFrame(pdf2, sdf2.schema)
        self.assertEqual(cdf4.schema, sdf4.schema)
        self.assertEqual(cdf4.collect(), sdf4.collect())

    def test_array_has_nullable(self):
        for schemas, data in [
            (
                [StructType().add("arr", ArrayType(IntegerType(), False), True)],
                [Row([1, 2]), Row([3]), Row(None)],
            ),
            (
                [
                    StructType().add("arr", ArrayType(IntegerType(), True), True),
                    "arr array<integer>",
                ],
                [Row([1, None]), Row([3]), Row(None)],
            ),
            (
                [StructType().add("arr", ArrayType(IntegerType(), False), False)],
                [Row([1, 2]), Row([3])],
            ),
            (
                [
                    StructType().add("arr", ArrayType(IntegerType(), True), False),
                    "arr array<integer> not null",
                ],
                [Row([1, None]), Row([3])],
            ),
        ]:
            for schema in schemas:
                with self.subTest(schema=schema):
                    cdf = self.connect.createDataFrame(data, schema=schema)
                    sdf = self.spark.createDataFrame(data, schema=schema)
                    self.assertEqual(cdf.schema, sdf.schema)
                    self.assertEqual(cdf.collect(), sdf.collect())

    def test_map_has_nullable(self):
        for schemas, data in [
            (
                [StructType().add("map", MapType(StringType(), IntegerType(), False), True)],
                [Row({"a": 1, "b": 2}), Row({"a": 3}), Row(None)],
            ),
            (
                [
                    StructType().add("map", MapType(StringType(), IntegerType(), True), True),
                    "map map<string, integer>",
                ],
                [Row({"a": 1, "b": None}), Row({"a": 3}), Row(None)],
            ),
            (
                [StructType().add("map", MapType(StringType(), IntegerType(), False), False)],
                [Row({"a": 1, "b": 2}), Row({"a": 3})],
            ),
            (
                [
                    StructType().add("map", MapType(StringType(), IntegerType(), True), False),
                    "map map<string, integer> not null",
                ],
                [Row({"a": 1, "b": None}), Row({"a": 3})],
            ),
        ]:
            for schema in schemas:
                with self.subTest(schema=schema):
                    cdf = self.connect.createDataFrame(data, schema=schema)
                    sdf = self.spark.createDataFrame(data, schema=schema)
                    self.assertEqual(cdf.schema, sdf.schema)
                    self.assertEqual(cdf.collect(), sdf.collect())

    def test_struct_has_nullable(self):
        for schemas, data in [
            (
                [
                    StructType().add("struct", StructType().add("i", IntegerType(), False), True),
                    "struct struct<i: integer not null>",
                ],
                [Row(Row(1)), Row(Row(2)), Row(None)],
            ),
            (
                [
                    StructType().add("struct", StructType().add("i", IntegerType(), True), True),
                    "struct struct<i: integer>",
                ],
                [Row(Row(1)), Row(Row(2)), Row(Row(None)), Row(None)],
            ),
            (
                [
                    StructType().add("struct", StructType().add("i", IntegerType(), False), False),
                    "struct struct<i: integer not null> not null",
                ],
                [Row(Row(1)), Row(Row(2))],
            ),
            (
                [
                    StructType().add("struct", StructType().add("i", IntegerType(), True), False),
                    "struct struct<i: integer> not null",
                ],
                [Row(Row(1)), Row(Row(2)), Row(Row(None))],
            ),
        ]:
            for schema in schemas:
                with self.subTest(schema=schema):
                    cdf = self.connect.createDataFrame(data, schema=schema)
                    sdf = self.spark.createDataFrame(data, schema=schema)
                    self.assertEqual(cdf.schema, sdf.schema)
                    self.assertEqual(cdf.collect(), sdf.collect())

    def test_large_client_data(self):
        # SPARK-42816 support more than 4MB message size.
        # ~200bytes
        cols = ["abcdefghijklmnoprstuvwxyz" for x in range(10)]
        # 100k rows => 20MB
        row_count = 100 * 1000
        rows = [cols] * row_count
        self.assertEqual(row_count, self.connect.createDataFrame(data=rows).count())

    def test_simple_udt(self):
        from pyspark.ml.linalg import MatrixUDT, VectorUDT

        for schema in [
            StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            StructType().add("key", LongType()).add("val", ArrayType(PythonOnlyUDT())),
            StructType().add("key", LongType()).add("val", MapType(LongType(), PythonOnlyUDT())),
            StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            StructType().add("key", LongType()).add("vec", VectorUDT()),
            StructType().add("key", LongType()).add("mat", MatrixUDT()),
        ]:
            cdf = self.connect.createDataFrame(data=[], schema=schema)
            sdf = self.spark.createDataFrame(data=[], schema=schema)

            self.assertEqual(cdf.schema, sdf.schema)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_creation import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
