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

import array
import ctypes
import datetime
import os
import pickle
import sys
import unittest
from dataclasses import dataclass, asdict

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.errors import (
    AnalysisException,
    PySparkTypeError,
    PySparkValueError,
    PySparkRuntimeError,
)
from pyspark.sql.types import (
    DataType,
    ByteType,
    ShortType,
    IntegerType,
    FloatType,
    DateType,
    TimestampType,
    DayTimeIntervalType,
    YearMonthIntervalType,
    CalendarIntervalType,
    MapType,
    StringType,
    CharType,
    VarcharType,
    StructType,
    StructField,
    ArrayType,
    DoubleType,
    LongType,
    DecimalType,
    BinaryType,
    BooleanType,
    NullType,
    VariantType,
    VariantVal,
)
from pyspark.sql.types import (
    _array_signed_int_typecode_ctype_mappings,
    _array_type_mappings,
    _array_unsigned_int_typecode_ctype_mappings,
    _infer_type,
    _make_type_verifier,
    _merge_type,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    ExamplePointUDT,
    PythonOnlyUDT,
    ExamplePoint,
    PythonOnlyPoint,
    MyObject,
)
from pyspark.testing.utils import PySparkErrorTestUtils


class TypesTestsMixin:
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
        schema = StructType().add("a", IntegerType()).add("b", StringType())
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
        rdd = self.sc.parallelize(range(3)).map(lambda i: Row(a=i))
        schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
        df = self.spark.createDataFrame(rdd, schema)
        self.assertRaises(Exception, lambda: df.show())

    def test_infer_schema(self):
        d = [Row(l=[], d={}, s=None), Row(l=[Row(a=1, b="s")], d={"key": Row(c=1.0, d="2")}, s="")]
        rdd = self.sc.parallelize(d)

        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame(rdd, schema=123)

        self.check_error(
            exception=pe.exception,
            error_class="NOT_LIST_OR_NONE_OR_STRUCT",
            message_parameters={"arg_name": "schema", "arg_type": "int"},
        )

        df = self.spark.createDataFrame(rdd)
        self.assertEqual([], df.rdd.map(lambda r: r.l).first())
        self.assertEqual([None, ""], df.rdd.map(lambda r: r.s).collect())

        with self.tempView("test"):
            df.createOrReplaceTempView("test")
            result = self.spark.sql("SELECT l from test")
            self.assertEqual([], result.head()[0])
            # We set `spark.sql.ansi.enabled` to False for this case
            # since it occurs an error in ANSI mode if there is a list index
            # or key that does not exist.
            with self.sql_conf({"spark.sql.ansi.enabled": False}):
                result = self.spark.sql("SELECT l[0].a from test where d['key'].d = '2'")
                self.assertEqual(1, result.head()[0])

        df2 = self.spark.createDataFrame(rdd, samplingRatio=1.0)
        self.assertEqual(df.schema, df2.schema)
        self.assertEqual({}, df2.rdd.map(lambda r: r.d).first())
        self.assertEqual([None, ""], df2.rdd.map(lambda r: r.s).collect())

        with self.tempView("test2"):
            df2.createOrReplaceTempView("test2")
            result = self.spark.sql("SELECT l from test2")
            self.assertEqual([], result.head()[0])
            # We set `spark.sql.ansi.enabled` to False for this case
            # since it occurs an error in ANSI mode if there is a list index
            # or key that does not exist.
            with self.sql_conf({"spark.sql.ansi.enabled": False}):
                result = self.spark.sql("SELECT l[0].a from test2 where d['key'].d = '2'")
                self.assertEqual(1, result.head()[0])

    def test_infer_schema_specification(self):
        from decimal import Decimal

        class A:
            def __init__(self):
                self.a = 1

        data = [
            True,
            1,
            "a",
            "a",
            datetime.date(1970, 1, 1),
            datetime.datetime(1970, 1, 1, 0, 0),
            datetime.timedelta(microseconds=123456678),
            1.0,
            array.array("d", [1]),
            [1],
            (1,),
            {"a": 1},
            bytearray(1),
            Decimal(1),
            Row(a=1),
            Row("a")(1),
            A(),
        ]

        df = self.spark.createDataFrame([data])
        actual = list(map(lambda x: x.dataType.simpleString(), df.schema))
        expected = [
            "boolean",
            "bigint",
            "string",
            "string",
            "date",
            "timestamp",
            "interval day to second",
            "double",
            "array<double>",
            "array<bigint>",
            "struct<_1:bigint>",
            "map<string,bigint>",
            "binary",
            "decimal(38,18)",
            "struct<a:bigint>",
            "struct<a:bigint>",
            "struct<a:bigint>",
        ]
        self.assertEqual(actual, expected)

        actual = list(df.first())
        expected = [
            True,
            1,
            "a",
            "a",
            datetime.date(1970, 1, 1),
            datetime.datetime(1970, 1, 1, 0, 0),
            datetime.timedelta(microseconds=123456678),
            1.0,
            [1.0],
            [1],
            Row(_1=1),
            {"a": 1},
            bytearray(b"\x00"),
            Decimal("1.000000000000000000"),
            Row(a=1),
            Row(a=1),
            Row(a=1),
        ]
        self.assertEqual(actual, expected)

        with self.sql_conf({"spark.sql.timestampType": "TIMESTAMP_NTZ"}):
            with self.sql_conf({"spark.sql.session.timeZone": "America/Sao_Paulo"}):
                df = self.spark.createDataFrame([(datetime.datetime(1970, 1, 1, 0, 0),)])
                self.assertEqual(list(df.schema)[0].dataType.simpleString(), "timestamp_ntz")
                self.assertEqual(df.first()[0], datetime.datetime(1970, 1, 1, 0, 0))

            df = self.spark.createDataFrame(
                [
                    (datetime.datetime(1970, 1, 1, 0, 0),),
                    (datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),),
                ]
            )
            self.assertEqual(list(df.schema)[0].dataType.simpleString(), "timestamp")

    def test_infer_schema_not_enough_names(self):
        df = self.spark.createDataFrame([["a", "b"]], ["col1"])
        self.assertEqual(df.columns, ["col1", "_2"])

    def test_infer_schema_upcast_int_to_string(self):
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([[1, 1], ["x", 1]]),
            schema=["a", "b"],
            samplingRatio=0.99,
        )
        self.assertEqual([Row(a="1", b=1), Row(a="x", b=1)], df.collect())

    def test_infer_schema_upcast_float_to_string(self):
        df = self.spark.createDataFrame([[1.33, 1], ["2.1", 1]], schema=["a", "b"])
        self.assertEqual([Row(a="1.33", b=1), Row(a="2.1", b=1)], df.collect())

    def test_infer_schema_upcast_boolean_to_string(self):
        df = self.spark.createDataFrame([[True, 1], ["false", 1]], schema=["a", "b"])
        self.assertEqual([Row(a="true", b=1), Row(a="false", b=1)], df.collect())

    def test_infer_nested_schema(self):
        NestedRow = Row("f1", "f2")
        nestedRdd1 = self.sc.parallelize(
            [NestedRow([1, 2], {"row1": 1.0}), NestedRow([2, 3], {"row2": 2.0})]
        )
        df = self.spark.createDataFrame(nestedRdd1)
        self.assertEqual(Row(f1=[1, 2], f2={"row1": 1.0}), df.collect()[0])

        nestedRdd2 = self.sc.parallelize(
            [NestedRow([[1, 2], [2, 3]], [1, 2]), NestedRow([[2, 3], [3, 4]], [2, 3])]
        )
        df = self.spark.createDataFrame(nestedRdd2)
        self.assertEqual(Row(f1=[[1, 2], [2, 3]], f2=[1, 2]), df.collect()[0])

        from collections import namedtuple

        CustomRow = namedtuple("CustomRow", "field1 field2")
        rdd = self.sc.parallelize(
            [
                CustomRow(field1=1, field2="row1"),
                CustomRow(field1=2, field2="row2"),
                CustomRow(field1=3, field2="row3"),
            ]
        )
        df = self.spark.createDataFrame(rdd)
        self.assertEqual(Row(field1=1, field2="row1"), df.first())

    def test_infer_nested_dict_as_struct(self):
        # SPARK-35929: Test inferring nested dict as a struct type.
        NestedRow = Row("f1", "f2")

        with self.sql_conf({"spark.sql.pyspark.inferNestedDictAsStruct.enabled": True}):
            data = [
                NestedRow([{"payment": 200.5, "name": "A"}], [1, 2]),
                NestedRow([{"payment": 100.5, "name": "B"}], [2, 3]),
            ]

            df = self.spark.createDataFrame(data)
            self.assertEqual(Row(f1=[Row(payment=200.5, name="A")], f2=[1, 2]), df.first())

    def test_infer_nested_dict_as_struct_with_rdd(self):
        # SPARK-35929: Test inferring nested dict as a struct type.
        NestedRow = Row("f1", "f2")

        with self.sql_conf({"spark.sql.pyspark.inferNestedDictAsStruct.enabled": True}):
            data = [
                NestedRow([{"payment": 200.5, "name": "A"}], [1, 2]),
                NestedRow([{"payment": 100.5, "name": "B"}], [2, 3]),
            ]

            nestedRdd = self.sc.parallelize(data)
            df = self.spark.createDataFrame(nestedRdd)
            self.assertEqual(Row(f1=[Row(payment=200.5, name="A")], f2=[1, 2]), df.first())

    def test_infer_array_merge_element_types(self):
        # SPARK-39168: Test inferring array element type from all values in array
        ArrayRow = Row("f1", "f2")

        data = [ArrayRow([1, None], [None, 2])]

        df = self.spark.createDataFrame(data)
        self.assertEqual(Row(f1=[1, None], f2=[None, 2]), df.first())

        # Test legacy behavior inferring only from the first element
        with self.sql_conf(
            {"spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled": True}
        ):
            # Legacy: f2 schema inferred as an array of nulls, should raise error
            self.assertRaises(ValueError, lambda: self.spark.createDataFrame(data))

        # an array with only null values should raise an error
        data2 = [ArrayRow([1], [None])]
        self.assertRaises(ValueError, lambda: self.spark.createDataFrame(data2))

        # an array with no values should raise an error
        data3 = [ArrayRow([1], [])]
        self.assertRaises(ValueError, lambda: self.spark.createDataFrame(data3))

        # an array with conflicting types should raise an error
        # in this case this is ArrayType(StringType) and ArrayType(NullType)
        data4 = [ArrayRow([1, "1"], [None])]
        with self.assertRaisesRegex(ValueError, "types cannot be determined after inferring"):
            self.spark.createDataFrame(data4)

    def test_infer_array_merge_element_types_with_rdd(self):
        # SPARK-39168: Test inferring array element type from all values in array
        ArrayRow = Row("f1", "f2")

        data = [ArrayRow([1, None], [None, 2])]

        rdd = self.sc.parallelize(data)
        df = self.spark.createDataFrame(rdd)
        self.assertEqual(Row(f1=[1, None], f2=[None, 2]), df.first())

    def test_infer_array_element_type_empty(self):
        # SPARK-39168: Test inferring array element type from all rows
        ArrayRow = Row("f1")

        data = [ArrayRow([]), ArrayRow([None]), ArrayRow([1])]

        rdd = self.sc.parallelize(data)
        df = self.spark.createDataFrame(rdd)
        rows = df.collect()
        self.assertEqual(Row(f1=[]), rows[0])
        self.assertEqual(Row(f1=[None]), rows[1])
        self.assertEqual(Row(f1=[1]), rows[2])

        df = self.spark.createDataFrame(data)
        rows = df.collect()
        self.assertEqual(Row(f1=[]), rows[0])
        self.assertEqual(Row(f1=[None]), rows[1])
        self.assertEqual(Row(f1=[1]), rows[2])

    def test_infer_array_element_type_with_struct(self):
        # SPARK-39168: Test inferring array of struct type from all struct values
        NestedRow = Row("f1")

        with self.sql_conf({"spark.sql.pyspark.inferNestedDictAsStruct.enabled": True}):
            data = [NestedRow([{"payment": 200.5}, {"name": "A"}])]

            nestedRdd = self.sc.parallelize(data)
            df = self.spark.createDataFrame(nestedRdd)
            self.assertEqual(
                Row(f1=[Row(payment=200.5, name=None), Row(payment=None, name="A")]), df.first()
            )

            df = self.spark.createDataFrame(data)
            self.assertEqual(
                Row(f1=[Row(payment=200.5, name=None), Row(payment=None, name="A")]), df.first()
            )

            # Test legacy behavior inferring only from the first element; excludes "name" field
            with self.sql_conf(
                {"spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled": True}
            ):
                df = self.spark.createDataFrame(data)
                self.assertEqual(Row(f1=[Row(payment=200.5), Row(payment=None)]), df.first())

    def test_create_dataframe_from_dict_respects_schema(self):
        df = self.spark.createDataFrame([{"a": 1}], ["b"])
        self.assertEqual(df.columns, ["b"])

    def test_create_dataframe_from_dataclasses(self):
        @dataclass
        class User:
            name: str
            age: int
            is_active: bool

        user = User(name="John", age=30, is_active=True)
        r = self.spark.createDataFrame([user]).first()
        self.assertEqual(asdict(user), r.asDict())

    def test_negative_decimal(self):
        try:
            self.spark.sql("set spark.sql.legacy.allowNegativeScaleOfDecimal=true")
            df = self.spark.createDataFrame([(1,), (11,)], ["value"])
            ret = df.select(F.col("value").cast(DecimalType(1, -1))).collect()
            actual = list(map(lambda r: int(r.value), ret))
            self.assertEqual(actual, [0, 10])
        finally:
            self.spark.sql("set spark.sql.legacy.allowNegativeScaleOfDecimal=false")

    def test_create_dataframe_from_objects(self):
        data = [MyObject(1, "1"), MyObject(2, "2")]
        df = self.spark.createDataFrame(data)
        self.assertEqual(df.dtypes, [("key", "bigint"), ("value", "string")])
        self.assertEqual(df.first(), Row(key=1, value="1"))

    def test_apply_schema(self):
        from datetime import date, datetime, timedelta

        rdd = self.sc.parallelize(
            [
                (
                    127,
                    -128,
                    -32768,
                    32767,
                    2147483647,
                    1.0,
                    date(2010, 1, 1),
                    datetime(2010, 1, 1, 1, 1, 1),
                    timedelta(days=1),
                    {"a": 1},
                    (2,),
                    [1, 2, 3],
                    None,
                )
            ]
        )
        schema = StructType(
            [
                StructField("byte1", ByteType(), False),
                StructField("byte2", ByteType(), False),
                StructField("short1", ShortType(), False),
                StructField("short2", ShortType(), False),
                StructField("int1", IntegerType(), False),
                StructField("float1", FloatType(), False),
                StructField("date1", DateType(), False),
                StructField("time1", TimestampType(), False),
                StructField("daytime1", DayTimeIntervalType(), False),
                StructField("map1", MapType(StringType(), IntegerType(), False), False),
                StructField("struct1", StructType([StructField("b", ShortType(), False)]), False),
                StructField("list1", ArrayType(ByteType(), False), False),
                StructField("null1", DoubleType(), True),
            ]
        )
        df = self.spark.createDataFrame(rdd, schema)
        results = df.rdd.map(
            lambda x: (
                x.byte1,
                x.byte2,
                x.short1,
                x.short2,
                x.int1,
                x.float1,
                x.date1,
                x.time1,
                x.daytime1,
                x.map1["a"],
                x.struct1.b,
                x.list1,
                x.null1,
            )
        )
        r = (
            127,
            -128,
            -32768,
            32767,
            2147483647,
            1.0,
            date(2010, 1, 1),
            datetime(2010, 1, 1, 1, 1, 1),
            timedelta(days=1),
            1,
            2,
            [1, 2, 3],
            None,
        )
        self.assertEqual(r, results.first())

        with self.tempView("table2"):
            df.createOrReplaceTempView("table2")
            r = self.spark.sql(
                "SELECT byte1 - 1 AS byte1, byte2 + 1 AS byte2, "
                + "short1 + 1 AS short1, short2 - 1 AS short2, int1 - 1 AS int1, "
                + "float1 + 1.5 as float1 FROM table2"
            ).first()

            self.assertEqual((126, -127, -32767, 32766, 2147483646, 2.5), tuple(r))

    def test_convert_row_to_dict(self):
        row = Row(l=[Row(a=1, b="s")], d={"key": Row(c=1.0, d="2")})
        self.assertEqual(1, row.asDict()["l"][0].a)
        df = self.spark.createDataFrame([row])

        with self.tempView("test"):
            df.createOrReplaceTempView("test")
            row = self.spark.sql("select l, d from test").head()
            self.assertEqual(1, row.asDict()["l"][0].a)
            self.assertEqual(1.0, row.asDict()["d"]["key"].c)

    def test_convert_list_to_str(self):
        data = [[[123], 120]]
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("income", LongType(), True),
            ]
        )
        df = self.spark.createDataFrame(data, schema)
        self.assertEqual(df.schema, schema)
        self.assertEqual(df.count(), 1)
        self.assertEqual(df.head(), Row(name="[123]", income=120))

    def test_udt(self):
        from pyspark.sql.types import _parse_datatype_json_string, _infer_type, _make_type_verifier

        def check_datatype(datatype):
            pickled = pickle.loads(pickle.dumps(datatype))
            assert datatype == pickled
            scala_datatype = self.spark._jsparkSession.parseDataType(datatype.json())
            python_datatype = _parse_datatype_json_string(scala_datatype.json())
            assert datatype == python_datatype

        check_datatype(ExamplePointUDT())
        structtype_with_udt = StructType(
            [
                StructField("label", DoubleType(), False),
                StructField("point", ExamplePointUDT(), False),
            ]
        )
        check_datatype(structtype_with_udt)
        p = ExamplePoint(1.0, 2.0)
        self.assertEqual(_infer_type(p), ExamplePointUDT())
        _make_type_verifier(ExamplePointUDT())(ExamplePoint(1.0, 2.0))
        self.assertRaises(ValueError, lambda: _make_type_verifier(ExamplePointUDT())([1.0, 2.0]))

        check_datatype(PythonOnlyUDT())
        structtype_with_udt = StructType(
            [
                StructField("label", DoubleType(), False),
                StructField("point", PythonOnlyUDT(), False),
            ]
        )
        check_datatype(structtype_with_udt)
        p = PythonOnlyPoint(1.0, 2.0)
        self.assertEqual(_infer_type(p), PythonOnlyUDT())
        _make_type_verifier(PythonOnlyUDT())(PythonOnlyPoint(1.0, 2.0))
        self.assertRaises(ValueError, lambda: _make_type_verifier(PythonOnlyUDT())([1.0, 2.0]))

    def test_simple_udt_in_df(self):
        schema = StructType().add("key", LongType()).add("val", PythonOnlyUDT())
        df = self.spark.createDataFrame(
            [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)], schema=schema
        )
        df.collect()

    def test_nested_udt_in_df(self):
        schema = StructType().add("key", LongType()).add("val", ArrayType(PythonOnlyUDT()))
        df = self.spark.createDataFrame(
            [(i % 3, [PythonOnlyPoint(float(i), float(i))]) for i in range(10)], schema=schema
        )
        df.collect()

        schema = (
            StructType().add("key", LongType()).add("val", MapType(LongType(), PythonOnlyUDT()))
        )
        df = self.spark.createDataFrame(
            [(i % 3, {i % 3: PythonOnlyPoint(float(i + 1), float(i + 1))}) for i in range(10)],
            schema=schema,
        )
        df.collect()

    def test_complex_nested_udt_in_df(self):
        schema = StructType().add("key", LongType()).add("val", PythonOnlyUDT())
        df = self.spark.createDataFrame(
            [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)], schema=schema
        )
        df.collect()

        gd = df.groupby("key").agg({"val": "collect_list"})
        gd.collect()
        udf = F.udf(lambda k, v: [(k, v[0])], ArrayType(df.schema))
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
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), ExamplePointUDT)

        with self.tempView("labeled_point"):
            df.createOrReplaceTempView("labeled_point")
            point = self.spark.sql("SELECT point FROM labeled_point").head().point
            self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), PythonOnlyUDT)

        with self.tempView("labeled_point"):
            df.createOrReplaceTempView("labeled_point")
            point = self.spark.sql("SELECT point FROM labeled_point").head().point
            self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_infer_schema_with_udt_with_column_names(self):
        row = (1.0, ExamplePoint(1.0, 2.0))
        df = self.spark.createDataFrame([row], ["label", "point"])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), ExamplePointUDT)

        with self.tempView("labeled_point"):
            df.createOrReplaceTempView("labeled_point")
            point = self.spark.sql("SELECT point FROM labeled_point").head().point
            self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = (1.0, PythonOnlyPoint(1.0, 2.0))
        df = self.spark.createDataFrame([row], ["label", "point"])
        schema = df.schema
        field = [f for f in schema.fields if f.name == "point"][0]
        self.assertEqual(type(field.dataType), PythonOnlyUDT)

        with self.tempView("labeled_point"):
            df.createOrReplaceTempView("labeled_point")
            point = self.spark.sql("SELECT point FROM labeled_point").head().point
            self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_apply_schema_with_udt(self):
        row = (1.0, ExamplePoint(1.0, 2.0))
        schema = StructType(
            [
                StructField("label", DoubleType(), False),
                StructField("point", ExamplePointUDT(), False),
            ]
        )
        df = self.spark.createDataFrame([row], schema)
        point = df.head().point
        self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = (1.0, PythonOnlyPoint(1.0, 2.0))
        schema = StructType(
            [
                StructField("label", DoubleType(), False),
                StructField("point", PythonOnlyUDT(), False),
            ]
        )
        df = self.spark.createDataFrame([row], schema)
        point = df.head().point
        self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_apply_schema_with_nullable_udt(self):
        rows = [(1.0, ExamplePoint(1.0, 2.0)), (2.0, None)]
        schema = StructType(
            [
                StructField("label", DoubleType(), False),
                StructField("point", ExamplePointUDT(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema)
        points = [row.point for row in df.collect()]
        self.assertEqual(points, [ExamplePoint(1.0, 2.0), None])

        rows = [(1.0, PythonOnlyPoint(1.0, 2.0)), (2.0, None)]
        schema = StructType(
            [
                StructField("label", DoubleType(), False),
                StructField("point", PythonOnlyUDT(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema)
        points = [row.point for row in df.collect()]
        self.assertEqual(points, [PythonOnlyPoint(1.0, 2.0), None])

    def test_udf_with_udt(self):
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        udf = F.udf(lambda p: p.y, DoubleType())
        self.assertEqual(2.0, df.select(udf(df.point)).first()[0])
        udf2 = F.udf(lambda p: ExamplePoint(p.x + 1, p.y + 1), ExamplePointUDT())
        self.assertEqual(ExamplePoint(2.0, 3.0), df.select(udf2(df.point)).first()[0])

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        udf = F.udf(lambda p: p.y, DoubleType())
        self.assertEqual(2.0, df.select(udf(df.point)).first()[0])
        udf2 = F.udf(lambda p: PythonOnlyPoint(p.x + 1, p.y + 1), PythonOnlyUDT())
        self.assertEqual(PythonOnlyPoint(2.0, 3.0), df.select(udf2(df.point)).first()[0])

    def test_rdd_with_udt(self):
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        self.assertEqual(1.0, df.rdd.map(lambda r: r.point.x).first())

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        self.assertEqual(1.0, df.rdd.map(lambda r: r.point.x).first())

    def test_parquet_with_udt(self):
        row = Row(label=1.0, point=ExamplePoint(1.0, 2.0))
        df0 = self.spark.createDataFrame([row])
        output_dir = os.path.join(self.tempdir.name, "labeled_point")
        df0.write.parquet(output_dir)
        df1 = self.spark.read.parquet(output_dir)
        point = df1.head().point
        self.assertEqual(point, ExamplePoint(1.0, 2.0))

        row = Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))
        df0 = self.spark.createDataFrame([row])
        df0.write.parquet(output_dir, mode="overwrite")
        df1 = self.spark.read.parquet(output_dir)
        point = df1.head().point
        self.assertEqual(point, PythonOnlyPoint(1.0, 2.0))

    def test_union_with_udt(self):
        row1 = (1.0, ExamplePoint(1.0, 2.0))
        row2 = (2.0, ExamplePoint(3.0, 4.0))
        schema = StructType(
            [
                StructField("label", DoubleType(), False),
                StructField("point", ExamplePointUDT(), False),
            ]
        )
        df1 = self.spark.createDataFrame([row1], schema)
        df2 = self.spark.createDataFrame([row2], schema)

        result = df1.union(df2).orderBy("label").collect()
        self.assertEqual(
            result,
            [
                Row(label=1.0, point=ExamplePoint(1.0, 2.0)),
                Row(label=2.0, point=ExamplePoint(3.0, 4.0)),
            ],
        )

    def test_cast_to_string_with_udt(self):
        row = (ExamplePoint(1.0, 2.0), PythonOnlyPoint(3.0, 4.0))
        schema = StructType(
            [
                StructField("point", ExamplePointUDT(), False),
                StructField("pypoint", PythonOnlyUDT(), False),
            ]
        )
        df = self.spark.createDataFrame([row], schema)

        result = df.select(F.col("point").cast("string"), F.col("pypoint").cast("string")).head()
        self.assertEqual(result, Row(point="(1.0, 2.0)", pypoint="[3.0, 4.0]"))

    def test_cast_to_udt_with_udt(self):
        row = Row(point=ExamplePoint(1.0, 2.0), python_only_point=PythonOnlyPoint(1.0, 2.0))
        df = self.spark.createDataFrame([row])
        with self.assertRaises(AnalysisException):
            df.select(F.col("point").cast(PythonOnlyUDT())).collect()
        with self.assertRaises(AnalysisException):
            df.select(F.col("python_only_point").cast(ExamplePointUDT())).collect()

    def test_struct_type(self):
        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        struct2 = StructType(
            [StructField("f1", StringType(), True), StructField("f2", StringType(), True, None)]
        )
        self.assertEqual(struct1.fieldNames(), struct2.names)
        self.assertEqual(struct1, struct2)

        struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        struct2 = StructType([StructField("f1", StringType(), True)])
        self.assertNotEqual(struct1.fieldNames(), struct2.names)
        self.assertNotEqual(struct1, struct2)

        struct1 = (
            StructType()
            .add(StructField("f1", StringType(), True))
            .add(StructField("f2", StringType(), True, None))
        )
        struct2 = StructType(
            [StructField("f1", StringType(), True), StructField("f2", StringType(), True, None)]
        )
        self.assertEqual(struct1.fieldNames(), struct2.names)
        self.assertEqual(struct1, struct2)

        struct1 = (
            StructType()
            .add(StructField("f1", StringType(), True))
            .add(StructField("f2", StringType(), True, None))
        )
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
            if k != "varchar" and k != "char":
                self.assertEqual(t(), _parse_datatype_string(k))
        self.assertEqual(IntegerType(), _parse_datatype_string("int"))
        self.assertEqual(StringType(), _parse_datatype_string("string"))
        self.assertEqual(StringType(), _parse_datatype_string("string collate UTF8_BINARY"))
        self.assertEqual(StringType(), _parse_datatype_string("string COLLATE UTF8_BINARY"))
        self.assertEqual(
            StringType.fromCollationId(0), _parse_datatype_string("string COLLATE   UTF8_BINARY")
        )
        self.assertEqual(
            StringType.fromCollationId(1),
            _parse_datatype_string("string COLLATE UTF8_BINARY_LCASE"),
        )
        self.assertEqual(
            StringType.fromCollationId(2), _parse_datatype_string("string COLLATE UNICODE")
        )
        self.assertEqual(
            StringType.fromCollationId(2), _parse_datatype_string("string COLLATE `UNICODE`")
        )
        self.assertEqual(
            StringType.fromCollationId(3), _parse_datatype_string("string COLLATE UNICODE_CI")
        )
        self.assertEqual(
            StringType.fromCollationId(3), _parse_datatype_string("string COLLATE `UNICODE_CI`")
        )
        self.assertEqual(CharType(1), _parse_datatype_string("char(1)"))
        self.assertEqual(CharType(10), _parse_datatype_string("char( 10   )"))
        self.assertEqual(CharType(11), _parse_datatype_string("char( 11)"))
        self.assertEqual(VarcharType(1), _parse_datatype_string("varchar(1)"))
        self.assertEqual(VarcharType(10), _parse_datatype_string("varchar( 10   )"))
        self.assertEqual(VarcharType(11), _parse_datatype_string("varchar( 11)"))
        self.assertEqual(DecimalType(1, 1), _parse_datatype_string("decimal(1  ,1)"))
        self.assertEqual(DecimalType(10, 1), _parse_datatype_string("decimal( 10,1 )"))
        self.assertEqual(DecimalType(11, 1), _parse_datatype_string("decimal(11,1)"))
        self.assertEqual(ArrayType(IntegerType()), _parse_datatype_string("array<int >"))
        self.assertEqual(
            MapType(IntegerType(), DoubleType()), _parse_datatype_string("map< int, double  >")
        )
        self.assertEqual(
            StructType([StructField("a", IntegerType()), StructField("c", DoubleType())]),
            _parse_datatype_string("struct<a:int, c:double >"),
        )
        self.assertEqual(
            StructType([StructField("a", IntegerType()), StructField("c", DoubleType())]),
            _parse_datatype_string("a:int, c:double"),
        )
        self.assertEqual(
            StructType([StructField("a", IntegerType()), StructField("c", DoubleType())]),
            _parse_datatype_string("a INT, c DOUBLE"),
        )
        self.assertEqual(VariantType(), _parse_datatype_string("variant"))

    def test_metadata_null(self):
        schema = StructType(
            [
                StructField("f1", StringType(), True, None),
                StructField("f2", StringType(), True, {"a": None}),
            ]
        )
        self.spark.createDataFrame([["a", "b"], ["c", "d"]], schema)

    def test_access_nested_types(self):
        df = self.spark.createDataFrame([Row(l=[1], r=Row(a=1, b="b"), d={"k": "v"})])
        self.assertEqual(1, df.select(df.l[0]).first()[0])
        self.assertEqual(1, df.select(df.l.getItem(0)).first()[0])
        self.assertEqual(1, df.select(df.r.a).first()[0])
        self.assertEqual("b", df.select(df.r.getField("b")).first()[0])
        self.assertEqual("v", df.select(df.d["k"]).first()[0])
        self.assertEqual("v", df.select(df.d.getItem("k")).first()[0])

        # Deprecated behaviors
        map_col = F.create_map(F.lit(0), F.lit(100), F.lit(1), F.lit(200))
        self.assertEqual(
            self.spark.range(1).withColumn("mapped", map_col.getItem(F.col("id"))).first()[1], 100
        )

        struct_col = F.struct(F.lit(0), F.lit(100), F.lit(1), F.lit(200))
        self.assertEqual(
            self.spark.range(1).withColumn("struct", struct_col.getField(F.lit("col1"))).first()[1],
            0,
        )

    def test_infer_long_type(self):
        longrow = [Row(f1="a", f2=100000000000000)]
        df = self.sc.parallelize(longrow).toDF()
        self.assertEqual(df.schema.fields[1].dataType, LongType())

        # this saving as Parquet caused issues as well.
        output_dir = os.path.join(self.tempdir.name, "infer_long_type")
        df.write.parquet(output_dir)
        df1 = self.spark.read.parquet(output_dir)
        self.assertEqual("a", df1.first().f1)
        self.assertEqual(100000000000000, df1.first().f2)

        self.assertEqual(_infer_type(1), LongType())
        self.assertEqual(_infer_type(2**10), LongType())
        self.assertEqual(_infer_type(2**20), LongType())
        self.assertEqual(_infer_type(2**31 - 1), LongType())
        self.assertEqual(_infer_type(2**31), LongType())
        self.assertEqual(_infer_type(2**61), LongType())
        self.assertEqual(_infer_type(2**71), LongType())

    def test_infer_binary_type(self):
        binaryrow = [Row(f1="a", f2=b"abcd")]
        df = self.sc.parallelize(binaryrow).toDF()
        self.assertEqual(df.schema.fields[1].dataType, BinaryType())

        # this saving as Parquet caused issues as well.
        output_dir = os.path.join(self.tempdir.name, "infer_binary_type")
        df.write.parquet(output_dir)
        df1 = self.spark.read.parquet(output_dir)
        self.assertEqual("a", df1.first().f1)
        self.assertEqual(b"abcd", df1.first().f2)

        self.assertEqual(_infer_type(b""), BinaryType())
        self.assertEqual(_infer_type(b"1234"), BinaryType())

    def test_merge_type(self):
        self.assertEqual(_merge_type(LongType(), NullType()), LongType())
        self.assertEqual(_merge_type(NullType(), LongType()), LongType())

        self.assertEqual(_merge_type(LongType(), LongType()), LongType())

        self.assertEqual(
            _merge_type(ArrayType(LongType()), ArrayType(LongType())), ArrayType(LongType())
        )
        with self.assertRaises(PySparkTypeError) as pe:
            _merge_type(ArrayType(LongType()), ArrayType(DoubleType()))
        self.check_error(
            exception=pe.exception,
            error_class="CANNOT_MERGE_TYPE",
            message_parameters={"data_type1": "LongType", "data_type2": "DoubleType"},
        )

        self.assertEqual(
            _merge_type(MapType(StringType(), LongType()), MapType(StringType(), LongType())),
            MapType(StringType(), LongType()),
        )

        self.assertEqual(
            _merge_type(MapType(StringType(), LongType()), MapType(DoubleType(), LongType())),
            MapType(StringType(), LongType()),
        )

        with self.assertRaises(PySparkTypeError) as pe:
            _merge_type(MapType(StringType(), LongType()), MapType(StringType(), DoubleType()))
        self.check_error(
            exception=pe.exception,
            error_class="CANNOT_MERGE_TYPE",
            message_parameters={"data_type1": "LongType", "data_type2": "DoubleType"},
        )

        self.assertEqual(
            _merge_type(
                StructType([StructField("f1", LongType()), StructField("f2", StringType())]),
                StructType([StructField("f1", LongType()), StructField("f2", StringType())]),
            ),
            StructType([StructField("f1", LongType()), StructField("f2", StringType())]),
        )
        with self.assertRaises(PySparkTypeError) as pe:
            _merge_type(
                StructType([StructField("f1", LongType()), StructField("f2", StringType())]),
                StructType([StructField("f1", DoubleType()), StructField("f2", StringType())]),
            )
        self.check_error(
            exception=pe.exception,
            error_class="CANNOT_MERGE_TYPE",
            message_parameters={"data_type1": "LongType", "data_type2": "DoubleType"},
        )

        self.assertEqual(
            _merge_type(
                StructType([StructField("f1", StructType([StructField("f2", LongType())]))]),
                StructType([StructField("f1", StructType([StructField("f2", LongType())]))]),
            ),
            StructType([StructField("f1", StructType([StructField("f2", LongType())]))]),
        )
        self.assertEqual(
            _merge_type(
                StructType([StructField("f1", StructType([StructField("f2", LongType())]))]),
                StructType([StructField("f1", StructType([StructField("f2", StringType())]))]),
            ),
            StructType([StructField("f1", StructType([StructField("f2", StringType())]))]),
        )

        self.assertEqual(
            _merge_type(
                StructType(
                    [StructField("f1", ArrayType(LongType())), StructField("f2", StringType())]
                ),
                StructType(
                    [StructField("f1", ArrayType(LongType())), StructField("f2", StringType())]
                ),
            ),
            StructType([StructField("f1", ArrayType(LongType())), StructField("f2", StringType())]),
        )
        with self.assertRaises(PySparkTypeError) as pe:
            _merge_type(
                StructType(
                    [StructField("f1", ArrayType(LongType())), StructField("f2", StringType())]
                ),
                StructType(
                    [StructField("f1", ArrayType(DoubleType())), StructField("f2", StringType())]
                ),
            )
        self.check_error(
            exception=pe.exception,
            error_class="CANNOT_MERGE_TYPE",
            message_parameters={"data_type1": "LongType", "data_type2": "DoubleType"},
        )

        self.assertEqual(
            _merge_type(
                StructType(
                    [
                        StructField("f1", MapType(StringType(), LongType())),
                        StructField("f2", StringType()),
                    ]
                ),
                StructType(
                    [
                        StructField("f1", MapType(StringType(), LongType())),
                        StructField("f2", StringType()),
                    ]
                ),
            ),
            StructType(
                [
                    StructField("f1", MapType(StringType(), LongType())),
                    StructField("f2", StringType()),
                ]
            ),
        )
        with self.assertRaises(PySparkTypeError) as pe:
            _merge_type(
                StructType(
                    [
                        StructField("f1", MapType(StringType(), LongType())),
                        StructField("f2", StringType()),
                    ]
                ),
                StructType(
                    [
                        StructField("f1", MapType(StringType(), DoubleType())),
                        StructField("f2", StringType()),
                    ]
                ),
            )
        self.check_error(
            exception=pe.exception,
            error_class="CANNOT_MERGE_TYPE",
            message_parameters={"data_type1": "LongType", "data_type2": "DoubleType"},
        )

        self.assertEqual(
            _merge_type(
                StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]),
                StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]),
            ),
            StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]),
        )
        self.assertEqual(
            _merge_type(
                StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]),
                StructType([StructField("f1", ArrayType(MapType(DoubleType(), LongType())))]),
            ),
            StructType([StructField("f1", ArrayType(MapType(StringType(), LongType())))]),
        )

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
            supported_string_types += ["u"]
            # test unicode
            assertCollectSuccess("u", "a")

        # supported float and double
        #
        # Test max, min, and precision for float and double, assuming IEEE 754
        # floating-point format.
        supported_fractional_types = ["f", "d"]
        assertCollectSuccess("f", ctypes.c_float(1e38).value)
        assertCollectSuccess("f", ctypes.c_float(1e-38).value)
        assertCollectSuccess("f", ctypes.c_float(1.123456).value)
        assertCollectSuccess("d", sys.float_info.max)
        assertCollectSuccess("d", sys.float_info.min)
        assertCollectSuccess("d", sys.float_info.epsilon)

        # supported signed int types
        #
        # The size of C types changes with implementation, we need to make sure
        # that there is no overflow error on the platform running this test.
        supported_signed_int_types = list(
            set(_array_signed_int_typecode_ctype_mappings.keys()).intersection(
                set(_array_type_mappings.keys())
            )
        )
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
            set(_array_unsigned_int_typecode_ctype_mappings.keys()).intersection(
                set(_array_type_mappings.keys())
            )
        )
        for t in supported_unsigned_int_types:
            ctype = _array_unsigned_int_typecode_ctype_mappings[t]
            assertCollectSuccess(t, 2 ** (ctypes.sizeof(ctype) * 8) - 1)

        # all supported types
        #
        # Make sure the types tested above:
        # 1. are all supported types
        # 2. cover all supported types
        supported_types = (
            supported_string_types
            + supported_fractional_types
            + supported_signed_int_types
            + supported_unsigned_int_types
        )
        self.assertEqual(set(supported_types), set(_array_type_mappings.keys()))

        # all unsupported types
        #
        # Keys in _array_type_mappings is a complete list of all supported types,
        # and types not in _array_type_mappings are considered unsupported.
        # PyPy seems not having array.typecodes.
        all_types = set(["b", "B", "u", "h", "H", "i", "I", "l", "L", "q", "Q", "f", "d"])
        unsupported_types = all_types - set(supported_types)
        # test unsupported types
        for t in unsupported_types:
            with self.assertRaises(PySparkTypeError) as pe:
                a = array.array(t)
                self.spark.createDataFrame([Row(myarray=a)]).collect()

            self.check_error(
                exception=pe.exception,
                error_class="CANNOT_INFER_TYPE_FOR_FIELD",
                message_parameters={"field_name": "myarray"},
            )

    def test_repr(self):
        instances = [
            NullType(),
            StringType(),
            StringType("UTF8_BINARY"),
            StringType("UTF8_BINARY_LCASE"),
            StringType("UNICODE"),
            StringType("UNICODE_CI"),
            CharType(10),
            VarcharType(10),
            BinaryType(),
            BooleanType(),
            DateType(),
            TimestampType(),
            DecimalType(),
            DoubleType(),
            FloatType(),
            ByteType(),
            IntegerType(),
            LongType(),
            ShortType(),
            CalendarIntervalType(),
            ArrayType(StringType()),
            MapType(StringType(), IntegerType()),
            StructField("f1", StringType(), True),
            StructType([StructField("f1", StringType(), True)]),
            VariantType(),
        ]
        for instance in instances:
            self.assertEqual(eval(repr(instance)), instance)

    def test_daytime_interval_type_constructor(self):
        # SPARK-37277: Test constructors in day time interval.
        self.assertEqual(DayTimeIntervalType().simpleString(), "interval day to second")
        self.assertEqual(
            DayTimeIntervalType(DayTimeIntervalType.DAY).simpleString(), "interval day"
        )
        self.assertEqual(
            DayTimeIntervalType(
                DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND
            ).simpleString(),
            "interval hour to second",
        )

        with self.assertRaises(PySparkRuntimeError) as pe:
            DayTimeIntervalType(endField=DayTimeIntervalType.SECOND)

        self.check_error(
            exception=pe.exception,
            error_class="INVALID_INTERVAL_CASTING",
            message_parameters={"start_field": "None", "end_field": "3"},
        )

        with self.assertRaises(PySparkRuntimeError) as pe:
            DayTimeIntervalType(123)

        self.check_error(
            exception=pe.exception,
            error_class="INVALID_INTERVAL_CASTING",
            message_parameters={"start_field": "123", "end_field": "123"},
        )

        with self.assertRaises(PySparkRuntimeError) as pe:
            DayTimeIntervalType(DayTimeIntervalType.DAY, 321)

        self.check_error(
            exception=pe.exception,
            error_class="INVALID_INTERVAL_CASTING",
            message_parameters={"start_field": "0", "end_field": "321"},
        )

    def test_daytime_interval_type(self):
        # SPARK-37277: Support DayTimeIntervalType in createDataFrame
        timedetlas = [
            (datetime.timedelta(microseconds=123),),
            (
                datetime.timedelta(
                    days=1, seconds=23, microseconds=123, milliseconds=4, minutes=5, hours=11
                ),
            ),
            (datetime.timedelta(microseconds=-123),),
            (datetime.timedelta(days=-1),),
            (datetime.timedelta(microseconds=388629894454999981),),
            (datetime.timedelta(days=-1, seconds=86399, microseconds=999999),),  # -1 microsecond
        ]
        df = self.spark.createDataFrame(timedetlas, schema="td interval day to second")
        self.assertEqual(set(r.td for r in df.collect()), set(set(r[0] for r in timedetlas)))

        exprs = [
            "INTERVAL '1 02:03:04' DAY TO SECOND AS a",
            "INTERVAL '1 02:03' DAY TO MINUTE AS b",
            "INTERVAL '1 02' DAY TO HOUR AS c",
            "INTERVAL '1' DAY AS d",
            "INTERVAL '26:03:04' HOUR TO SECOND AS e",
            "INTERVAL '26:03' HOUR TO MINUTE AS f",
            "INTERVAL '26' HOUR AS g",
            "INTERVAL '1563:04' MINUTE TO SECOND AS h",
            "INTERVAL '1563' MINUTE AS i",
            "INTERVAL '93784' SECOND AS j",
        ]
        df = self.spark.range(1).selectExpr(exprs)

        actual = list(df.first())
        expected = [
            datetime.timedelta(days=1, hours=2, minutes=3, seconds=4),
            datetime.timedelta(days=1, hours=2, minutes=3),
            datetime.timedelta(days=1, hours=2),
            datetime.timedelta(days=1),
            datetime.timedelta(hours=26, minutes=3, seconds=4),
            datetime.timedelta(hours=26, minutes=3),
            datetime.timedelta(hours=26),
            datetime.timedelta(minutes=1563, seconds=4),
            datetime.timedelta(minutes=1563),
            datetime.timedelta(seconds=93784),
        ]

        for n, (a, e) in enumerate(zip(actual, expected)):
            self.assertEqual(a, e, "%s does not match with %s" % (exprs[n], expected[n]))

    def test_yearmonth_interval_type_constructor(self):
        self.assertEqual(YearMonthIntervalType().simpleString(), "interval year to month")
        self.assertEqual(
            YearMonthIntervalType(YearMonthIntervalType.YEAR).simpleString(), "interval year"
        )
        self.assertEqual(
            YearMonthIntervalType(
                YearMonthIntervalType.YEAR, YearMonthIntervalType.MONTH
            ).simpleString(),
            "interval year to month",
        )

        with self.assertRaises(PySparkRuntimeError) as pe:
            YearMonthIntervalType(endField=3)

        self.check_error(
            exception=pe.exception,
            error_class="INVALID_INTERVAL_CASTING",
            message_parameters={"start_field": "None", "end_field": "3"},
        )

        with self.assertRaises(PySparkRuntimeError) as pe:
            YearMonthIntervalType(123)

        self.check_error(
            exception=pe.exception,
            error_class="INVALID_INTERVAL_CASTING",
            message_parameters={"start_field": "123", "end_field": "123"},
        )

        with self.assertRaises(PySparkRuntimeError) as pe:
            YearMonthIntervalType(YearMonthIntervalType.YEAR, 321)

        self.check_error(
            exception=pe.exception,
            error_class="INVALID_INTERVAL_CASTING",
            message_parameters={"start_field": "0", "end_field": "321"},
        )

    def test_yearmonth_interval_type(self):
        schema1 = self.spark.sql("SELECT INTERVAL '10-8' YEAR TO MONTH AS interval").schema
        self.assertEqual(schema1.fields[0].dataType, YearMonthIntervalType(0, 1))

        schema2 = self.spark.sql("SELECT INTERVAL '10' YEAR AS interval").schema
        self.assertEqual(schema2.fields[0].dataType, YearMonthIntervalType(0, 0))

        schema3 = self.spark.sql("SELECT INTERVAL '8' MONTH AS interval").schema
        self.assertEqual(schema3.fields[0].dataType, YearMonthIntervalType(1, 1))

    def test_calendar_interval_type_constructor(self):
        self.assertEqual(CalendarIntervalType().simpleString(), "interval")

        with self.assertRaisesRegex(TypeError, "takes 1 positional argument but 2 were given"):
            CalendarIntervalType(3)

    def test_calendar_interval_type(self):
        schema1 = self.spark.sql("SELECT make_interval(100, 11, 1, 1, 12, 30, 01.001001)").schema
        self.assertEqual(schema1.fields[0].dataType, CalendarIntervalType())

    def test_calendar_interval_type_with_sf(self):
        schema1 = self.spark.range(1).select(F.make_interval(F.lit(1))).schema
        self.assertEqual(schema1.fields[0].dataType, CalendarIntervalType())

    def test_variant_type(self):
        from decimal import Decimal

        self.assertEqual(VariantType().simpleString(), "variant")

        # Holds a tuple of (key, json string value, python value)
        expected_values = [
            ("str", '"%s"' % ("0123456789" * 10), "0123456789" * 10),
            ("short_str", '"abc"', "abc"),
            ("null", "null", None),
            ("true", "true", True),
            ("false", "false", False),
            ("int1", "1", 1),
            ("-int1", "-5", -5),
            ("int2", "257", 257),
            ("-int2", "-124", -124),
            ("int4", "65793", 65793),
            ("-int4", "-69633", -69633),
            ("int8", "4295033089", 4295033089),
            ("-int8", "-4294967297", -4294967297),
            ("float4", "1.23456789e-30", 1.23456789e-30),
            ("-float4", "-4.56789e+29", -4.56789e29),
            ("dec4", "123.456", Decimal("123.456")),
            ("-dec4", "-321.654", Decimal("-321.654")),
            ("dec8", "429.4967297", Decimal("429.4967297")),
            ("-dec8", "-5.678373902", Decimal("-5.678373902")),
            ("dec16", "467440737095.51617", Decimal("467440737095.51617")),
            ("-dec16", "-67.849438003827263", Decimal("-67.849438003827263")),
            ("arr", '[1.1,"2",[3],{"4":5}]', [Decimal("1.1"), "2", [3], {"4": 5}]),
            ("obj", '{"a":["123",{"b":2}],"c":3}', {"a": ["123", {"b": 2}], "c": 3}),
        ]
        json_str = "{%s}" % ",".join(['"%s": %s' % (t[0], t[1]) for t in expected_values])

        df = self.spark.createDataFrame([({"json": json_str})])
        row = df.select(
            F.parse_json(df.json).alias("v"),
            F.array([F.parse_json(F.lit('{"a": 1}'))]).alias("a"),
            F.struct([F.parse_json(F.lit('{"b": "2"}'))]).alias("s"),
            F.create_map([F.lit("k"), F.parse_json(F.lit('{"c": true}'))]).alias("m"),
        ).collect()[0]
        variants = [row["v"], row["a"][0], row["s"]["col1"], row["m"]["k"]]
        for v in variants:
            self.assertEqual(type(v), VariantVal)

        # check str
        as_string = str(variants[0])
        for key, expected, _ in expected_values:
            self.assertTrue('"%s":%s' % (key, expected) in as_string)
        self.assertEqual(str(variants[1]), '{"a":1}')
        self.assertEqual(str(variants[2]), '{"b":"2"}')
        self.assertEqual(str(variants[3]), '{"c":true}')

        # check toPython
        as_python = variants[0].toPython()
        for key, _, obj in expected_values:
            self.assertEqual(as_python[key], obj)
        self.assertEqual(variants[1].toPython(), {"a": 1})
        self.assertEqual(variants[2].toPython(), {"b": "2"})
        self.assertEqual(variants[3].toPython(), {"c": True})

        # check repr
        self.assertEqual(str(variants[0]), str(eval(repr(variants[0]))))

    def test_from_ddl(self):
        self.assertEqual(DataType.fromDDL("long"), LongType())
        self.assertEqual(
            DataType.fromDDL("a: int, b: string"),
            StructType([StructField("a", IntegerType()), StructField("b", StringType())]),
        )
        self.assertEqual(
            DataType.fromDDL("a int, b string"),
            StructType([StructField("a", IntegerType()), StructField("b", StringType())]),
        )
        self.assertEqual(
            DataType.fromDDL("a int, v variant"),
            StructType([StructField("a", IntegerType()), StructField("v", VariantType())]),
        )

    def test_collated_string(self):
        dfs = [
            self.spark.sql("SELECT 'abc' collate UTF8_BINARY_LCASE"),
            self.spark.createDataFrame(
                [], StructType([StructField("id", StringType("UTF8_BINARY_LCASE"))])
            ),
        ]
        for df in dfs:
            # performs both datatype -> proto & proto -> datatype conversions
            self.assertEqual(
                df.to(StructType([StructField("new", StringType("UTF8_BINARY_LCASE"))]))
                .schema[0]
                .dataType,
                StringType("UTF8_BINARY_LCASE"),
            )


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

    def test_char_type(self):
        v1 = CharType(10)
        v2 = CharType(20)
        self.assertTrue(v2 is not v1)
        self.assertNotEqual(v1, v2)
        v3 = CharType(10)
        self.assertEqual(v1, v3)
        self.assertFalse(v1 is v3)

    def test_varchar_type(self):
        v1 = VarcharType(10)
        v2 = VarcharType(20)
        self.assertTrue(v2 is not v1)
        self.assertNotEqual(v1, v2)
        v3 = VarcharType(10)
        self.assertEqual(v1, v3)
        self.assertFalse(v1 is v3)

    # regression test for SPARK-10392
    def test_datetype_equal_zero(self):
        dt = DateType()
        self.assertEqual(dt.fromInternal(0), datetime.date(1970, 1, 1))

    # regression test for SPARK-17035
    def test_timestamp_microsecond(self):
        tst = TimestampType()
        self.assertEqual(tst.toInternal(datetime.datetime.max) % 1000000, 999999)

    # regression test for SPARK-23299
    def test_row_without_column_name(self):
        row = Row("Alice", 11)
        self.assertEqual(repr(row), "<Row('Alice', 11)>")

        # test __repr__ with unicode values
        self.assertEqual(repr(Row("数", "量")), "<Row('数', '量')>")

    # SPARK-44643: test __repr__ with empty Row
    def test_row_repr_with_empty_row(self):
        self.assertEqual(repr(Row(a=Row())), "Row(a=<Row()>)")
        self.assertEqual(repr(Row(Row())), "<Row(<Row()>)>")

        EmptyRow = Row()
        self.assertEqual(repr(Row(a=EmptyRow())), "Row(a=Row())")
        self.assertEqual(repr(Row(EmptyRow())), "<Row(Row())>")

    def test_empty_row(self):
        row = Row()
        self.assertEqual(len(row), 0)

    def test_struct_field_type_name(self):
        struct_field = StructField("a", IntegerType())
        self.assertRaises(TypeError, struct_field.typeName)

    def test_invalid_create_row(self):
        row_class = Row("c1", "c2")
        self.assertRaises(ValueError, lambda: row_class(1, 2, 3))


class DataTypeVerificationTests(unittest.TestCase, PySparkErrorTestUtils):
    def test_verify_type_exception_msg(self):
        with self.assertRaises(PySparkValueError) as pe:
            _make_type_verifier(StringType(), nullable=False, name="test_name")(None)

        self.check_error(
            exception=pe.exception,
            error_class="FIELD_NOT_NULLABLE_WITH_NAME",
            message_parameters={
                "field_name": "test_name",
            },
        )

        schema = StructType([StructField("a", StructType([StructField("b", IntegerType())]))])
        with self.assertRaises(PySparkTypeError) as pe:
            _make_type_verifier(schema)([["data"]])

        self.check_error(
            exception=pe.exception,
            error_class="FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME",
            message_parameters={
                "data_type": "IntegerType()",
                "field_name": "field b in field a",
                "obj": "'data'",
                "obj_type": "<class 'str'>",
            },
        )

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

        schema = StructType(
            [
                StructField("s", StringType(), nullable=False),
                StructField("i", IntegerType(), nullable=True),
            ]
        )

        class MyObj:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        # obj, data_type
        success_spec = [
            # String
            ("", StringType()),
            (1, StringType()),
            (1.0, StringType()),
            ([], StringType()),
            ({}, StringType()),
            ("", StringType("UTF8_BINARY_LCASE")),
            # Char
            ("", CharType(10)),
            (1, CharType(10)),
            (1.0, CharType(10)),
            ([], CharType(10)),
            ({}, CharType(10)),
            # Varchar
            ("", VarcharType(10)),
            (1, VarcharType(10)),
            (1.0, VarcharType(10)),
            ([], VarcharType(10)),
            ({}, VarcharType(10)),
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
            (-(2**63), LongType()),
            (2**63 - 1, LongType()),
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
            (array.array("h", [1, 2]), ArrayType(IntegerType())),
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
            (None, StringType("UTF8_BINARY_LCASE"), ValueError),
            # CharType (match anything but None)
            (None, CharType(10), ValueError),
            # VarcharType (match anything but None)
            (None, VarcharType(10), ValueError),
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
            (
                {"a": None},
                MapType(StringType(), IntegerType(), valueContainsNull=False),
                ValueError,
            ),
            # Struct
            ({"s": "a", "i": "1"}, schema, TypeError),
            (Row(s="a"), schema, ValueError),  # Row can't have missing field
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

    def test_row_without_field_sorting(self):
        r = Row(b=1, a=2)
        TestRow = Row("b", "a")
        expected = TestRow(1, 2)

        self.assertEqual(r, expected)
        self.assertEqual(repr(r), "Row(b=1, a=2)")

    def test_struct_field_from_json(self):
        # SPARK-40820: fromJson with only name and type
        json = {"name": "c1", "type": "string"}
        struct_field = StructField.fromJson(json)

        self.assertEqual(repr(struct_field), "StructField('c1', StringType(), True)")


class TypesTests(TypesTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_types import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
