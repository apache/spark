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

from pyspark.errors import PySparkValueError
from pyspark.sql.conversion import (
    ArrowTableToRowsConversion,
    LocalDataToArrowConversion,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    GeographyType,
    GeometryType,
    IntegerType,
    MapType,
    NullType,
    Row,
    StringType,
    StructField,
    StructType,
    UserDefinedType,
)
from pyspark.testing.objects import ExamplePoint, ExamplePointUDT
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message


class ScoreUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        return IntegerType()

    def serialize(self, obj):
        return obj.score

    def deserialize(self, datum):
        return Score(datum)


class Score:
    __UDT__ = ScoreUDT()

    def __init__(self, score):
        self.score = score

    def __eq__(self, other):
        return self.score == other.score


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ConversionTests(unittest.TestCase):
    def test_conversion(self):
        data = [
            # Schema, Test cases (Before, After_If_Different)
            (NullType(), (None,)),
            (IntegerType(), (1,), (None,)),
            ((IntegerType(), {"nullable": False}), (1,)),
            (StringType(), ("a",)),
            (BinaryType(), (b"a",)),
            (GeographyType("ANY"), (None,)),
            (GeometryType("ANY"), (None,)),
            (ArrayType(IntegerType()), ([1, None],)),
            (ArrayType(IntegerType(), containsNull=False), ([1, 2],)),
            (ArrayType(BinaryType()), ([b"a", b"b"],)),
            (MapType(StringType(), IntegerType()), ({"a": 1, "b": None},)),
            (
                MapType(StringType(), IntegerType(), valueContainsNull=False),
                ({"a": 1},),
            ),
            (MapType(StringType(), BinaryType()), ({"a": b"a"},)),
            (
                StructType(
                    [
                        StructField("i", IntegerType()),
                        StructField("i_n", IntegerType()),
                        StructField("ii", IntegerType(), nullable=False),
                        StructField("s", StringType()),
                        StructField("b", BinaryType()),
                    ]
                ),
                ((1, None, 1, "a", b"a"), Row(i=1, i_n=None, ii=1, s="a", b=b"a")),
                (
                    {"b": b"a", "s": "a", "ii": 1, "in": None, "i": 1},
                    Row(i=1, i_n=None, ii=1, s="a", b=b"a"),
                ),
            ),
            (ExamplePointUDT(), (ExamplePoint(1.0, 1.0),)),
            (ScoreUDT(), (Score(1),)),
        ]

        schema = StructType()

        input_row = []
        expected = []

        index = 0
        for row_schema, *tests in data:
            if isinstance(row_schema, tuple):
                row_schema, kwargs = row_schema
            else:
                kwargs = {}
            for test in tests:
                if len(test) == 1:
                    before, after = test[0], test[0]
                else:
                    before, after = test
                schema.add(f"{row_schema.simpleString()}_{index}", row_schema, **kwargs)
                input_row.append(before)
                expected.append(after)
                index += 1

        tbl = LocalDataToArrowConversion.convert(
            [tuple(input_row)], schema, use_large_var_types=False
        )
        actual = ArrowTableToRowsConversion.convert(tbl, schema)

        for a, e in zip(
            actual[0],
            expected,
        ):
            with self.subTest(expected=e):
                self.assertEqual(a, e)

    def test_none_as_row(self):
        schema = StructType([StructField("x", IntegerType())])
        tbl = LocalDataToArrowConversion.convert([None], schema, use_large_var_types=False)
        actual = ArrowTableToRowsConversion.convert(tbl, schema)
        self.assertEqual(actual[0], Row(x=None))

    def test_return_as_tuples(self):
        schema = StructType([StructField("x", IntegerType())])
        tbl = LocalDataToArrowConversion.convert([(1,)], schema, use_large_var_types=False)
        actual = ArrowTableToRowsConversion.convert(tbl, schema, return_as_tuples=True)
        self.assertEqual(actual[0], (1,))

        schema = StructType()
        tbl = LocalDataToArrowConversion.convert([tuple()], schema, use_large_var_types=False)
        actual = ArrowTableToRowsConversion.convert(tbl, schema, return_as_tuples=True)
        self.assertEqual(actual[0], tuple())

    def test_binary_as_bytes_conversion(self):
        data = [
            (
                str(i).encode(),  # simple binary
                [str(j).encode() for j in range(3)],  # array of binary
                {str(j): str(j).encode() for j in range(2)},  # map with binary values
                {"b": str(i).encode()},  # struct with binary
            )
            for i in range(2)
        ]
        schema = (
            StructType()
            .add("b", BinaryType())
            .add("arr_b", ArrayType(BinaryType()))
            .add("map_b", MapType(StringType(), BinaryType()))
            .add("struct_b", StructType().add("b", BinaryType()))
        )

        tbl = LocalDataToArrowConversion.convert(data, schema, use_large_var_types=False)

        for binary_as_bytes, expected_type in [(True, bytes), (False, bytearray)]:
            actual = ArrowTableToRowsConversion.convert(
                tbl, schema, binary_as_bytes=binary_as_bytes
            )

            for row in actual:
                # Simple binary field
                self.assertIsInstance(row.b, expected_type)
                # Array elements
                for elem in row.arr_b:
                    self.assertIsInstance(elem, expected_type)
                # Map values
                for value in row.map_b.values():
                    self.assertIsInstance(value, expected_type)
                # Struct field
                self.assertIsInstance(row.struct_b.b, expected_type)

    def test_invalid_conversion(self):
        data = [
            (NullType(), 1),
            (ArrayType(IntegerType(), containsNull=False), [1, None]),
            (ArrayType(ScoreUDT(), containsNull=False), [None]),
        ]

        for row_schema, value in data:
            schema = StructType([StructField("x", row_schema)])
            with self.assertRaises(PySparkValueError):
                LocalDataToArrowConversion.convert([(value,)], schema, use_large_var_types=False)


if __name__ == "__main__":
    from pyspark.sql.tests.test_conversion import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
