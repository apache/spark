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

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.sql.conversion import (
    ArrowArrayToPandasConversion,
    ArrowTableToRowsConversion,
    LocalDataToArrowConversion,
    ArrowArrayConversion,
    ArrowBatchTransformer,
    PandasToArrowConversion,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    Geography,
    GeographyType,
    Geometry,
    GeometryType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    Row,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimeType,
    TimestampNTZType,
    TimestampType,
    UserDefinedType,
    VariantType,
    VariantVal,
    YearMonthIntervalType,
)
from pyspark.testing.objects import ExamplePoint, ExamplePointUDT, PythonOnlyPoint, PythonOnlyUDT
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


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

    def test_enforce_schema_nested_cast(self):
        """Nested struct and list types are cast recursively by Arrow."""
        import pyarrow as pa

        inner = pa.struct([("a", pa.int32()), ("b", pa.float32())])
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([{"a": 1, "b": 2.0}], type=inner),
                pa.array([[1, 2]], type=pa.list_(pa.int32())),
            ],
            names=["s", "l"],
        )
        target = pa.schema(
            [
                ("s", pa.struct([("a", pa.int64()), ("b", pa.float64())])),
                ("l", pa.list_(pa.int64())),
            ]
        )
        result = ArrowBatchTransformer.enforce_schema(batch, target)
        self.assertEqual(result.schema, target)

    def test_enforce_schema_arrow_cast_false(self):
        """arrow_cast=False raises on type mismatch instead of casting."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int32())], names=["x"])
        target = pa.schema([("x", pa.int64())])
        with self.assertRaises(PySparkRuntimeError) as cm:
            ArrowBatchTransformer.enforce_schema(batch, target, arrow_cast=False)
        self.assertEqual(cm.exception.getCondition(), "RESULT_COLUMN_TYPES_MISMATCH")

    def test_enforce_schema_safecheck(self):
        """safecheck=True rejects overflow; safecheck=False allows it."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([999], type=pa.int64())], names=["x"])
        target = pa.schema([("x", pa.int8())])
        with self.assertRaises(PySparkRuntimeError) as cm:
            ArrowBatchTransformer.enforce_schema(batch, target, safecheck=True)
        self.assertEqual(cm.exception.getCondition(), "RESULT_COLUMN_TYPES_MISMATCH")
        result = ArrowBatchTransformer.enforce_schema(batch, target, safecheck=False)
        self.assertEqual(result.schema, target)

    def test_enforce_schema_missing_column(self):
        """Missing column raises RESULT_COLUMN_NAMES_MISMATCH."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1])], names=["a"])
        with self.assertRaises(PySparkRuntimeError) as cm:
            ArrowBatchTransformer.enforce_schema(batch, pa.schema([("missing", pa.int64())]))
        self.assertEqual(cm.exception.getCondition(), "RESULT_COLUMN_NAMES_MISMATCH")

    def test_enforce_schema_extra_column(self):
        """Extra column raises RESULT_COLUMN_NAMES_MISMATCH with the extra name listed."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1]), pa.array([2])], names=["a", "b"])
        with self.assertRaises(PySparkRuntimeError) as cm:
            ArrowBatchTransformer.enforce_schema(batch, pa.schema([("a", pa.int64())]))
        self.assertEqual(cm.exception.getCondition(), "RESULT_COLUMN_NAMES_MISMATCH")
        self.assertIn("b", str(cm.exception))

    def test_enforce_schema_reorder_by_name(self):
        """reorder_by_name=True reorders input columns to match target schema order."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array(["x"]), pa.array([1])], names=["b", "a"])
        target = pa.schema([("a", pa.int64()), ("b", pa.string())])
        result = ArrowBatchTransformer.enforce_schema(batch, target)
        self.assertEqual(result.schema.names, ["a", "b"])
        self.assertEqual(result.column(0).to_pylist(), [1])
        self.assertEqual(result.column(1).to_pylist(), ["x"])

    def test_enforce_schema_positional(self):
        """reorder_by_name=False matches columns by index, preserving input names."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1]), pa.array(["x"])], names=["foo", "bar"])
        target = pa.schema([("a", pa.int64()), ("b", pa.string())])
        result = ArrowBatchTransformer.enforce_schema(batch, target, reorder_by_name=False)
        # Input column names are preserved
        self.assertEqual(result.schema.names, ["foo", "bar"])
        self.assertEqual(result.column(0).to_pylist(), [1])
        self.assertEqual(result.column(1).to_pylist(), ["x"])

    def test_enforce_schema_positional_count_mismatch(self):
        """reorder_by_name=False with wrong column count raises RESULT_COLUMN_SCHEMA_MISMATCH."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1])], names=["a"])
        target = pa.schema([("x", pa.int64()), ("y", pa.int64())])
        with self.assertRaises(PySparkRuntimeError) as cm:
            ArrowBatchTransformer.enforce_schema(batch, target, reorder_by_name=False)
        self.assertEqual(cm.exception.getCondition(), "RESULT_COLUMN_SCHEMA_MISMATCH")

    def test_enforce_schema_table_input(self):
        """enforce_schema accepts pa.Table and returns pa.Table."""
        import pyarrow as pa

        table = pa.table({"x": pa.array([1], type=pa.int32())})
        target = pa.schema([("x", pa.int64())])
        result = ArrowBatchTransformer.enforce_schema(table, target)
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.schema, target)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
@unittest.skipIf(not have_pandas, pandas_requirement_message)
class PandasToArrowConversionTests(unittest.TestCase):
    def test_convert(self):
        """Test basic DataFrame/Series to Arrow RecordBatch conversion."""
        import pandas as pd
        import pyarrow as pa

        # Basic DataFrame conversion
        df = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        schema = StructType([StructField("a", IntegerType()), StructField("b", DoubleType())])
        result = PandasToArrowConversion.convert(df, schema)
        self.assertIsInstance(result, pa.RecordBatch)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.num_columns, 2)
        self.assertEqual(result.schema.names, ["a", "b"])

        # List of Series input
        series_list = [pd.Series([1, 2, 3]), pd.Series([1.0, 2.0, 3.0])]
        result = PandasToArrowConversion.convert(series_list, schema)
        self.assertEqual(result.num_rows, 3)

        # With nulls
        df = pd.DataFrame({"a": [1, None, 3], "b": [1.0, 2.0, None]})
        result = PandasToArrowConversion.convert(df, schema)
        self.assertEqual(result.column(0).to_pylist(), [1, None, 3])

        # Empty DataFrame (0 rows)
        df = pd.DataFrame({"a": pd.Series([], dtype=int), "b": pd.Series([], dtype=float)})
        result = PandasToArrowConversion.convert(df, schema)
        self.assertEqual(result.num_rows, 0)

        # Empty schema (0 columns) should preserve row count
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        result = PandasToArrowConversion.convert(df, StructType([]))
        self.assertEqual(result.num_columns, 0)
        self.assertEqual(result.num_rows, 3)

    def test_convert_assign_cols_by_name(self):
        """Test assign_cols_by_name reorders columns to match schema."""
        import pandas as pd

        # DataFrame columns in different order than schema
        df = pd.DataFrame({"b": ["x", "y", "z"], "a": [1, 2, 3]})
        schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])

        # With assign_cols_by_name=True - reorders columns to match schema field names
        result = PandasToArrowConversion.convert(df, schema, assign_cols_by_name=True)
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])  # a
        self.assertEqual(result.column(1).to_pylist(), ["x", "y", "z"])  # b

        # Without assign_cols_by_name - uses positional order (b first, a second)
        df = pd.DataFrame({"b": [10, 20, 30], "a": [1.0, 2.0, 3.0]})
        schema = StructType([StructField("x", IntegerType()), StructField("y", DoubleType())])
        result = PandasToArrowConversion.convert(df, schema, assign_cols_by_name=False)
        self.assertEqual(result.column(0).to_pylist(), [10, 20, 30])  # positional: b -> x
        self.assertEqual(result.column(1).to_pylist(), [1.0, 2.0, 3.0])  # positional: a -> y

    def test_convert_timezone(self):
        """Test timezone handling for timestamp conversion."""
        import pandas as pd

        # Create DataFrame with timezone-naive timestamps
        df = pd.DataFrame({"ts": pd.to_datetime(["2023-01-01 12:00:00", "2023-01-02 12:00:00"])})
        schema = StructType([StructField("ts", TimestampType())])

        # Convert with timezone
        result = PandasToArrowConversion.convert(df, schema, timezone="UTC")
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.num_columns, 1)

    def test_convert_arrow_cast(self):
        """Test arrow_cast allows type coercion on mismatch."""
        import pandas as pd

        # DataFrame with int32, schema expects int64
        df = pd.DataFrame({"a": pd.array([1, 2, 3], dtype="int32")})
        schema = StructType([StructField("a", LongType())])

        # With arrow_cast=True, should allow the conversion
        result = PandasToArrowConversion.convert(df, schema, arrow_cast=True)
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])

    def test_convert_decimal(self):
        """Test int to decimal coercion."""
        import pandas as pd
        from decimal import Decimal

        # DataFrame with integers, schema expects decimal
        df = pd.DataFrame({"a": [1, 2, 3]})
        schema = StructType([StructField("a", DecimalType(10, 2))])

        # With int_to_decimal_coercion_enabled=True
        result = PandasToArrowConversion.convert(df, schema, int_to_decimal_coercion_enabled=True)
        self.assertEqual(result.num_rows, 3)
        # Values should be converted to decimal
        values = result.column(0).to_pylist()
        self.assertEqual(values, [Decimal("1.00"), Decimal("2.00"), Decimal("3.00")])

    def test_convert_struct(self):
        """Test struct type conversion via nested DataFrame columns."""
        import pandas as pd
        import pyarrow as pa

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField(
                    "info",
                    StructType([StructField("x", IntegerType()), StructField("y", DoubleType())]),
                ),
            ]
        )
        # List input: second element is a DataFrame (struct column)
        data = [pd.Series([1, 2]), pd.DataFrame({"x": [10, 20], "y": [1.1, 2.2]})]
        result = PandasToArrowConversion.convert(data, schema)
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.num_columns, 2)
        # Struct column should be a StructArray
        self.assertTrue(pa.types.is_struct(result.column(1).type))

        # Empty DataFrame for struct type
        data = [
            pd.Series([], dtype=int),
            pd.DataFrame({"x": pd.Series([], dtype=int), "y": pd.Series([], dtype=float)}),
        ]
        result = PandasToArrowConversion.convert(data, schema)
        self.assertEqual(result.num_rows, 0)

    def test_convert_error_messages(self):
        """Test error messages include series name from schema field."""
        import pandas as pd

        schema = StructType([StructField("age", IntegerType()), StructField("name", StringType())])

        # Type mismatch: string data for integer column
        data = [pd.Series(["not_int", "bad"]), pd.Series(["a", "b"])]
        with self.assertRaises((PySparkValueError, PySparkTypeError)) as ctx:
            PandasToArrowConversion.convert(data, schema)
        # Error message should use the new format and reference the schema field name
        self.assertIn("age", str(ctx.exception))

    def test_convert_is_legacy(self):
        """Test is_legacy=True uses the legacy error format."""
        import pandas as pd

        schema = StructType([StructField("val", DoubleType())])
        data = [pd.Series(["not_a_number", "bad"])]

        # ValueError path (string -> double)
        with self.assertRaises(PySparkValueError) as ctx:
            PandasToArrowConversion.convert(data, schema, is_legacy=True)
        self.assertIn("Exception thrown when converting pandas.Series", str(ctx.exception))
        self.assertIn("val", str(ctx.exception))

        # TypeError path (int -> struct): ArrowTypeError inherits from TypeError.
        # ignore_unexpected_complex_type_values=True lets the bad value pass through
        # to Arrow, which raises ArrowTypeError (a TypeError subclass).
        struct_schema = StructType(
            [StructField("x", StructType([StructField("a", IntegerType())]))]
        )
        data = [pd.Series([0, 1])]
        with self.assertRaises(PySparkTypeError) as ctx:
            PandasToArrowConversion.convert(
                data,
                struct_schema,
                is_legacy=True,
                ignore_unexpected_complex_type_values=True,
            )
        self.assertIn("Exception thrown when converting pandas.Series", str(ctx.exception))
        self.assertIn("x", str(ctx.exception))

    def test_convert_prefers_large_types(self):
        """Test prefers_large_types produces large Arrow types."""
        import pandas as pd
        import pyarrow as pa

        df = pd.DataFrame({"s": ["hello", "world"]})
        schema = StructType([StructField("s", StringType())])

        result = PandasToArrowConversion.convert(df, schema, prefers_large_types=True)
        self.assertEqual(result.column(0).type, pa.large_string())

        result = PandasToArrowConversion.convert(df, schema, prefers_large_types=False)
        self.assertEqual(result.column(0).type, pa.string())

    def test_convert_categorical(self):
        """Test CategoricalDtype series is correctly converted."""
        import pandas as pd

        cat_series = pd.Series(pd.Categorical(["a", "b", "a", "c"]))
        schema = StructType([StructField("cat", StringType())])
        result = PandasToArrowConversion.convert([cat_series], schema)
        self.assertEqual(result.column(0).to_pylist(), ["a", "b", "a", "c"])


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


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowArrayToPandasConversionTests(unittest.TestCase):
    def test_udt_convert_numpy(self):
        import pyarrow as pa

        udt = ExamplePointUDT()

        # basic conversion with nulls
        arr = pa.array([[1.0, 2.0], None, [3.0, 4.0]], type=pa.list_(pa.float64()))
        result = ArrowArrayToPandasConversion.convert_numpy(arr, udt, ser_name="my_point")
        self.assertIsInstance(result.iloc[0], ExamplePoint)
        self.assertEqual(result.iloc[0], ExamplePoint(1.0, 2.0))
        self.assertIsNone(result.iloc[1])
        self.assertEqual(result.iloc[2], ExamplePoint(3.0, 4.0))
        self.assertEqual(result.name, "my_point")

        # empty
        result = ArrowArrayToPandasConversion.convert_numpy(
            pa.array([], type=pa.list_(pa.float64())), udt
        )
        self.assertEqual(len(result), 0)

        # PythonOnlyUDT
        result = ArrowArrayToPandasConversion.convert_numpy(
            pa.array([[5.0, 6.0]], type=pa.list_(pa.float64())), PythonOnlyUDT()
        )
        self.assertIsInstance(result.iloc[0], PythonOnlyPoint)
        self.assertEqual(result.iloc[0], PythonOnlyPoint(5.0, 6.0))

    def test_udt_chunked_array(self):
        import pyarrow as pa

        chunk1 = pa.array([[1.0, 2.0]], type=pa.list_(pa.float64()))
        chunk2 = pa.array([[3.0, 4.0]], type=pa.list_(pa.float64()))
        chunked = pa.chunked_array([chunk1, chunk2])
        result = ArrowArrayToPandasConversion.convert_numpy(chunked, ExamplePointUDT())
        self.assertEqual(result.iloc[0], ExamplePoint(1.0, 2.0))
        self.assertEqual(result.iloc[1], ExamplePoint(3.0, 4.0))

    def test_variant_convert_numpy(self):
        import pyarrow as pa

        variant_type = pa.struct(
            [
                pa.field("value", pa.binary(), nullable=False),
                pa.field("metadata", pa.binary(), nullable=False, metadata={b"variant": b"true"}),
            ]
        )

        # basic conversion with nulls
        arr = pa.array(
            [
                {"value": b"\x01", "metadata": b"\x02"},
                None,
                {"value": b"\x03", "metadata": b"\x04"},
            ],
            type=variant_type,
        )
        result = ArrowArrayToPandasConversion.convert_numpy(arr, VariantType(), ser_name="v")
        self.assertIsInstance(result.iloc[0], VariantVal)
        self.assertEqual(result.iloc[0].value, b"\x01")
        self.assertEqual(result.iloc[0].metadata, b"\x02")
        self.assertIsNone(result.iloc[1])
        self.assertEqual(result.iloc[2].value, b"\x03")
        self.assertEqual(result.iloc[2].metadata, b"\x04")
        self.assertEqual(result.name, "v")

        # empty
        result = ArrowArrayToPandasConversion.convert_numpy(
            pa.array([], type=variant_type), VariantType()
        )
        self.assertEqual(len(result), 0)

    def test_convert_pyarrow(self):
        import pyarrow as pa
        import pandas as pd

        from decimal import Decimal

        # Cases where input data equals expected output
        cases = [
            ([None, None], pa.null(), NullType()),
            ([b"\x01", None], pa.binary(), BinaryType()),
            ([True, None, False], pa.bool_(), BooleanType()),
            ([1.0, None], pa.float32(), FloatType()),
            ([1.0, None], pa.float64(), DoubleType()),
            ([1, None, 3], pa.int8(), ByteType()),
            ([1, None, 3], pa.int16(), ShortType()),
            ([1, None, 3], pa.int32(), IntegerType()),
            ([1, None, 3], pa.int64(), LongType()),
            ([Decimal("1.23"), None], pa.decimal128(10, 2), DecimalType(10, 2)),
            (["a", None, "c"], pa.string(), StringType()),
            ([1, None], pa.int32(), YearMonthIntervalType()),
        ]
        for data, arrow_type, spark_type in cases:
            arr = pa.array(data, type=arrow_type)
            result = ArrowArrayToPandasConversion.convert_pyarrow(arr, spark_type)
            self.assertIsInstance(result.dtype, pd.ArrowDtype, f"Failed for {spark_type}")
            for i, val in enumerate(data):
                msg = f"Failed for {spark_type} at index {i}: expected {val}, got {result.iloc[i]}"
                if val is None:
                    self.assertTrue(pd.isna(result.iloc[i]), msg)
                else:
                    self.assertEqual(result.iloc[i], val, msg)

    def test_convert_pyarrow_temporal(self):
        import pyarrow as pa
        import pandas as pd

        cases = [
            ([1, None], pa.date32(), DateType(), [datetime.date(1970, 1, 2), None]),
            ([1000000, None], pa.time64("us"), TimeType(), [datetime.time(0, 0, 1), None]),
            (
                [1000000, None],
                pa.timestamp("us", tz="UTC"),
                TimestampType(),
                [datetime.datetime(1970, 1, 1, 0, 0, 1), None],
            ),
            (
                [1000000, None],
                pa.timestamp("us"),
                TimestampNTZType(),
                [datetime.datetime(1970, 1, 1, 0, 0, 1), None],
            ),
            (
                [1000000, None],
                pa.duration("us"),
                DayTimeIntervalType(),
                [datetime.timedelta(seconds=1), None],
            ),
        ]
        for data, arrow_type, spark_type, expected in cases:
            arr = pa.array(data, type=arrow_type)
            result = ArrowArrayToPandasConversion.convert_pyarrow(arr, spark_type)
            self.assertIsInstance(result.dtype, pd.ArrowDtype, f"Failed for {spark_type}")
            for i, exp in enumerate(expected):
                msg = f"Failed for {spark_type} at index {i}: expected {exp}, got {result.iloc[i]}"
                if exp is None:
                    self.assertTrue(pd.isna(result.iloc[i]), msg)
                else:
                    self.assertEqual(result.iloc[i], exp, msg)

    def test_convert_pyarrow_ser_name(self):
        import pyarrow as pa
        import pandas as pd

        # explicit ser_name
        arr = pa.array([1, 2, 3], type=pa.int64())
        result = ArrowArrayToPandasConversion.convert_pyarrow(arr, LongType(), ser_name="col")
        self.assertEqual(result.name, "col")
        self.assertIsInstance(result.dtype, pd.ArrowDtype)

        # default name from arrow array (set via RecordBatch column extraction)
        batch = pa.record_batch({"my_col": [1, 2, 3]})
        arr = batch.column("my_col")
        result = ArrowArrayToPandasConversion.convert_pyarrow(arr, LongType())
        self.assertEqual(result.name, "my_col")

    def test_convert_arrow_dtype(self):
        """Test that arrow_dtype routes supported types to convert_pyarrow."""
        import pyarrow as pa
        import pandas as pd

        arr = pa.array([1, 2, 3], type=pa.int64())

        # arrow_dtype=True with a supported type: ArrowDtype-backed
        result = ArrowArrayToPandasConversion.convert(arr, LongType(), arrow_dtype=True)
        self.assertIsInstance(result.dtype, pd.ArrowDtype)

        # arrow_dtype=False (default): numpy-backed
        result = ArrowArrayToPandasConversion.convert(arr, LongType())
        self.assertNotIsInstance(result.dtype, pd.ArrowDtype)

    def test_convert_arrow_dtype_unsupported_type_falls_through(self):
        """arrow_dtype=True with a type not in ARROW_DTYPE_TYPES falls through
        to convert_numpy/convert_legacy."""
        import pyarrow as pa
        import pandas as pd

        # ArrayType is not in ARROW_DTYPE_TYPES, so arrow_dtype=True should fall through
        arr = pa.array([[1, 2], [3]], type=pa.list_(pa.int64()))
        result = ArrowArrayToPandasConversion.convert(
            arr, ArrayType(IntegerType()), arrow_dtype=True
        )
        self.assertNotIsInstance(result.dtype, pd.ArrowDtype)

    def test_convert_arrow_dtype_with_df_for_struct(self):
        """arrow_dtype=True with df_for_struct=True falls through to legacy
        (caller wants a DataFrame, not an ArrowDtype Series)."""
        import pyarrow as pa
        import pandas as pd

        struct_type = pa.struct([("a", pa.int64()), ("b", pa.string())])
        arr = pa.array([{"a": 1, "b": "x"}], type=struct_type)
        spark_type = StructType([StructField("a", LongType()), StructField("b", StringType())])

        # df_for_struct=True with arrow_dtype=True: returns a DataFrame, not a Series
        result = ArrowArrayToPandasConversion.convert(
            arr,
            spark_type,
            arrow_dtype=True,
            df_for_struct=True,
        )
        self.assertIsInstance(result, pd.DataFrame)

    def test_geography_convert_numpy(self):
        import pyarrow as pa

        geography_type = pa.struct(
            [
                pa.field("srid", pa.int32(), nullable=False),
                pa.field(
                    "wkb",
                    pa.binary(),
                    nullable=False,
                    metadata={b"geography": b"true", b"srid": b"4326"},
                ),
            ]
        )

        # basic conversion with nulls
        # POINT(1.0, 2.0) and POINT(17.0, 7.0) in WKB format
        wkb1 = bytes.fromhex("0101000000000000000000F03F0000000000000040")
        wkb2 = bytes.fromhex("010100000000000000000031400000000000001c40")
        arr = pa.array(
            [
                {"srid": 4326, "wkb": wkb1},
                None,
                {"srid": 4326, "wkb": wkb2},
            ],
            type=geography_type,
        )
        result = ArrowArrayToPandasConversion.convert_numpy(arr, GeographyType(4326), ser_name="g")
        self.assertEqual(result.iloc[0], Geography(wkb1, 4326))
        self.assertIsNone(result.iloc[1])
        self.assertEqual(result.iloc[2], Geography(wkb2, 4326))
        self.assertEqual(result.name, "g")

        # empty
        result = ArrowArrayToPandasConversion.convert_numpy(
            pa.array([], type=geography_type), GeographyType(4326)
        )
        self.assertEqual(len(result), 0)

    def test_geometry_convert_numpy(self):
        import pyarrow as pa

        geometry_type = pa.struct(
            [
                pa.field("srid", pa.int32(), nullable=False),
                pa.field(
                    "wkb",
                    pa.binary(),
                    nullable=False,
                    metadata={b"geometry": b"true", b"srid": b"0"},
                ),
            ]
        )

        # basic conversion with nulls
        # POINT(1.0, 2.0) and POINT(17.0, 7.0) in WKB format
        wkb1 = bytes.fromhex("0101000000000000000000F03F0000000000000040")
        wkb2 = bytes.fromhex("010100000000000000000031400000000000001c40")
        arr = pa.array(
            [
                {"srid": 0, "wkb": wkb1},
                None,
                {"srid": 0, "wkb": wkb2},
            ],
            type=geometry_type,
        )
        result = ArrowArrayToPandasConversion.convert_numpy(arr, GeometryType(0), ser_name="g")
        self.assertEqual(result.iloc[0], Geometry(wkb1, 0))
        self.assertIsNone(result.iloc[1])
        self.assertEqual(result.iloc[2], Geometry(wkb2, 0))
        self.assertEqual(result.name, "g")

        # empty
        result = ArrowArrayToPandasConversion.convert_numpy(
            pa.array([], type=geometry_type), GeometryType(0)
        )
        self.assertEqual(len(result), 0)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
