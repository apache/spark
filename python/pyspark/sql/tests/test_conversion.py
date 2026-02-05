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
import unittest
from zoneinfo import ZoneInfo

from pyspark.errors import PySparkValueError
from pyspark.sql.conversion import (
    ArrowTableToRowsConversion,
    LocalDataToArrowConversion,
    ArrowArrayConversion,
    ArrowBatchTransformer,
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
class ArrowBatchTransformerTests(unittest.TestCase):
    def test_flatten_struct_basic(self):
        """Test flattening a struct column into separate columns."""
        import pyarrow as pa

        struct_array = pa.StructArray.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["x", "y"],
        )
        batch = pa.RecordBatch.from_arrays([struct_array], ["_0"])

        flattened = ArrowBatchTransformer.flatten_struct(batch)

        self.assertEqual(flattened.num_columns, 2)
        self.assertEqual(flattened.column(0).to_pylist(), [1, 2, 3])
        self.assertEqual(flattened.column(1).to_pylist(), ["a", "b", "c"])
        self.assertEqual(flattened.schema.names, ["x", "y"])

    def test_flatten_struct_empty_batch(self):
        """Test flattening an empty batch."""
        import pyarrow as pa

        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        struct_array = pa.array([], type=struct_type)
        batch = pa.RecordBatch.from_arrays([struct_array], ["_0"])

        flattened = ArrowBatchTransformer.flatten_struct(batch)

        self.assertEqual(flattened.num_rows, 0)
        self.assertEqual(flattened.num_columns, 2)

    def test_wrap_struct_basic(self):
        """Test wrapping columns into a struct."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["x", "y"],
        )

        wrapped = ArrowBatchTransformer.wrap_struct(batch)

        self.assertEqual(wrapped.num_columns, 1)
        self.assertEqual(wrapped.schema.names, ["_0"])

        struct_col = wrapped.column(0)
        self.assertEqual(len(struct_col), 3)
        self.assertEqual(struct_col.field(0).to_pylist(), [1, 2, 3])
        self.assertEqual(struct_col.field(1).to_pylist(), ["a", "b", "c"])

    def test_wrap_struct_empty_columns(self):
        """Test wrapping a batch with no columns."""
        import pyarrow as pa

        schema = pa.schema([])
        batch = pa.RecordBatch.from_arrays([], schema=schema)

        wrapped = ArrowBatchTransformer.wrap_struct(batch)

        self.assertEqual(wrapped.num_columns, 1)
        self.assertEqual(wrapped.num_rows, 0)

    def test_wrap_struct_empty_batch(self):
        """Test wrapping an empty batch with schema."""
        import pyarrow as pa

        schema = pa.schema([("x", pa.int64()), ("y", pa.string())])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
            schema=schema,
        )

        wrapped = ArrowBatchTransformer.wrap_struct(batch)

        self.assertEqual(wrapped.num_rows, 0)
        self.assertEqual(wrapped.num_columns, 1)


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

    def test_arrow_array_localize_tz(self):
        import pyarrow as pa

        tz1 = ZoneInfo("Asia/Singapore")
        tz2 = ZoneInfo("America/Los_Angeles")
        tz3 = ZoneInfo("UTC")

        ts0 = datetime.datetime(2026, 1, 5, 15, 0, 1)
        ts1 = datetime.datetime(2026, 1, 5, 15, 0, 1, tzinfo=tz1)
        ts2 = datetime.datetime(2026, 1, 5, 15, 0, 1, tzinfo=tz2)
        ts3 = datetime.datetime(2026, 1, 5, 15, 0, 1, tzinfo=tz3)

        # non-timestampe types
        for arr in [
            pa.array([1, 2]),
            pa.array([["x", "y"]]),
            pa.array([[[3.0, 4.0]]]),
            pa.StructArray.from_arrays([pa.array([1, 2]), pa.array(["x", "y"])], names=["a", "b"]),
            pa.array([{1: None, 2: "x"}], type=pa.map_(pa.int32(), pa.string())),
        ]:
            output = ArrowArrayConversion.localize_tz(arr)
            self.assertTrue(output is arr, f"MUST not generate a new array {output.tolist()}")

        # timestampe types
        for arr, expected in [
            (pa.array([ts0, None]), pa.array([ts0, None])),  # ts-ntz
            (pa.array([ts1, None]), pa.array([ts0, None])),  # ts-ltz
            (pa.array([[ts2, None]]), pa.array([[ts0, None]])),  # array<ts-ltz>
            (pa.array([[[ts3, None]]]), pa.array([[[ts0, None]]])),  # array<array<ts-ltz>>
            (
                pa.StructArray.from_arrays(
                    [pa.array([1, 2]), pa.array([ts0, None]), pa.array([ts1, None])],
                    names=["a", "b", "c"],
                ),
                pa.StructArray.from_arrays(
                    [pa.array([1, 2]), pa.array([ts0, None]), pa.array([ts0, None])],
                    names=["a", "b", "c"],
                ),
            ),  # struct<int, ts-ntz, ts-ltz>
            (
                pa.StructArray.from_arrays(
                    [pa.array([1, 2]), pa.array([[ts2], [None]])], names=["a", "b"]
                ),
                pa.StructArray.from_arrays(
                    [pa.array([1, 2]), pa.array([[ts0], [None]])], names=["a", "b"]
                ),
            ),  # struct<int, array<ts-ltz>>
            (
                pa.StructArray.from_arrays(
                    [
                        pa.array([ts2, None]),
                        pa.StructArray.from_arrays(
                            [pa.array(["a", "b"]), pa.array([[ts3], [None]])], names=["x", "y"]
                        ),
                    ],
                    names=["a", "b"],
                ),
                pa.StructArray.from_arrays(
                    [
                        pa.array([ts0, None]),
                        pa.StructArray.from_arrays(
                            [pa.array(["a", "b"]), pa.array([[ts0], [None]])], names=["x", "y"]
                        ),
                    ],
                    names=["a", "b"],
                ),
            ),  # struct<ts-ltz, struct<str, array<ts-ltz>>>
            (
                pa.array(
                    [{1: None, 2: ts1}],
                    type=pa.map_(pa.int32(), pa.timestamp("us", tz=tz1)),
                ),
                pa.array(
                    [{1: None, 2: ts0}],
                    type=pa.map_(pa.int32(), pa.timestamp("us")),
                ),
            ),  # map<int, ts-ltz>
            (
                pa.array(
                    [{1: [None], 2: [ts2, None]}],
                    type=pa.map_(pa.int32(), pa.list_(pa.timestamp("us", tz=tz2))),
                ),
                pa.array(
                    [{1: [None], 2: [ts0, None]}],
                    type=pa.map_(pa.int32(), pa.list_(pa.timestamp("us"))),
                ),
            ),  # map<int, array<ts-ltz>>
        ]:
            output = ArrowArrayConversion.localize_tz(arr)
            self.assertEqual(output, expected, f"{output.tolist()} != {expected.tolist()}")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
