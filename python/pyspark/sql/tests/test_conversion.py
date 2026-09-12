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
import decimal
import itertools
import math
import unittest
import unittest.mock
from zoneinfo import ZoneInfo

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.sql.conversion import (
    ArrowArrayConversion,
    ArrowArrayToPandasConversion,
    ArrowBatchTransformer,
    ArrowTableToRowsConversion,
    LocalDataToArrowConversion,
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
    TimestampNTZType,
    TimestampType,
    TimeType,
    UserDefinedType,
    VariantType,
    VariantVal,
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
        from decimal import Decimal

        import pandas as pd

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

    def test_convert_chunked_array_backed(self):
        """Test a chunked arrow-backed series is converted to a single Array."""
        import pandas as pd
        import pyarrow as pa

        # pa.Array.from_pandas returns a ChunkedArray here, which
        # pa.RecordBatch.from_arrays rejects.
        chunked = pa.chunked_array([pa.array(["a", "b"]), pa.array(["c", "d", "e"])])
        series = pd.Series(chunked, dtype="string[pyarrow]")
        schema = StructType([StructField("s", StringType())])

        result = PandasToArrowConversion.convert([series], schema, arrow_cast=True)
        self.assertIsInstance(result.column(0), pa.Array)
        self.assertEqual(result.column(0).to_pylist(), ["a", "b", "c", "d", "e"])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ConversionTests(unittest.TestCase):
    def test_conversion(self):
        data = [
            # Schema, Test cases (Before, After_If_Different)
            (NullType(), (None,)),
            (IntegerType(), (1,), (None,)),
            ((IntegerType(), {"nullable": False}), (1,)),
            (StringType(), ("a",)),
            # bool coerced to string matches the JVM (EvaluatePython.makeFromJava).
            (StringType(), (True, "true"), (False, "false")),
            (BinaryType(), (b"a",)),
            (GeographyType("ANY"), (None,)),
            (GeometryType("ANY"), (None,)),
            (ArrayType(IntegerType()), ([1, None],)),
            (ArrayType(IntegerType(), containsNull=False), ([1, 2],)),
            (ArrayType(BinaryType()), ([b"a", b"b"],)),
            # array<string> with already-str, coerced (int/bool) and null elements.
            (
                ArrayType(StringType()),
                (["ok", 42, True, False, None], ["ok", "42", "true", "false", None]),
            ),
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

    def test_convert_array_preserves_nulls(self):
        """Rebuilding a nested array must carry its own validity bitmap over.

        The from_arrays constructors drop the parent's nulls unless they are passed back as
        `mask`, which turned a null list/map into an empty one and a null struct into a struct
        of zero values.
        """
        import pyarrow as pa

        ts = datetime.datetime(2026, 1, 5, 15, 0, 1)
        for arr in [
            pa.array([[ts], None, []], type=pa.list_(pa.timestamp("us"))),
            pa.array([[ts], None, []], type=pa.list_(pa.timestamp("us", tz="UTC"))),
            pa.array([[ts], None], type=pa.large_list(pa.timestamp("us"))),
            pa.array([[ts, ts], None], type=pa.list_(pa.timestamp("us"), 2)),
            pa.array([[("k", ts)], None], type=pa.map_(pa.string(), pa.timestamp("us"))),
            pa.array([{"t": ts}, None], type=pa.struct([("t", pa.timestamp("us"))])),
            pa.array([[[ts], None], None, []], type=pa.list_(pa.list_(pa.timestamp("us")))),
        ]:
            with self.subTest(type=str(arr.type)):
                output = ArrowArrayConversion.preprocess_time(arr)
                self.assertEqual(len(output), len(arr))
                self.assertEqual(output.null_count, arr.null_count)
                self.assertEqual(output.is_null().tolist(), arr.is_null().tolist())

    def test_convert_array_handles_sliced_arrays(self):
        """Every slice must keep its length and nulls.

        from_arrays rejects a mask on a slice that starts past row 0, and
        FixedSizeListArray.values returns the whole child regardless of the slice, including
        a prefix slice, which keeps offset 0 and so is not caught by an offset check. Cover
        every (start, length) pair: prefixes, suffixes, interior and empty slices.
        """
        import pyarrow as pa

        ts = datetime.datetime(2026, 1, 5, 15, 0, 1)
        for arr in [
            pa.array([[ts], None, [], [ts, None]], type=pa.list_(pa.timestamp("us"))),
            pa.array([[ts], None, [ts]], type=pa.large_list(pa.timestamp("us"))),
            pa.array([[ts, ts], None, [ts, ts], None], type=pa.list_(pa.timestamp("us"), 2)),
            pa.array(
                [[("k", ts)], None, [("j", ts)]], type=pa.map_(pa.string(), pa.timestamp("us"))
            ),
            pa.array([{"t": ts}, None, {"t": ts}], type=pa.struct([("t", pa.timestamp("us"))])),
            pa.array([ts, None, ts], type=pa.timestamp("us")).dictionary_encode(),
        ]:
            for start in range(len(arr)):
                for length in range(len(arr) - start + 1):
                    sliced = arr.slice(start, length)
                    with self.subTest(type=str(arr.type), start=start, length=length):
                        output = ArrowArrayConversion.preprocess_time(sliced)
                        self.assertEqual(len(output), len(sliced))
                        self.assertEqual(output.is_null().tolist(), sliced.is_null().tolist())

    def test_convert_array_alternative_list_layouts(self):
        """large_list and fixed_size_list of timestamps must convert.

        These branches were unreachable until array types were routed through convert_numpy:
        one named the type class (pa.LargeListType) instead of the array class, and the other
        built a FixedSizeListArray without a list size.
        """
        import pyarrow as pa

        # localize_tz drops the tzinfo and keeps the local wall clock, as above.
        ts = datetime.datetime(2026, 1, 5, 15, 0, 1, tzinfo=ZoneInfo("Asia/Tokyo"))
        expected = datetime.datetime(2026, 1, 5, 15, 0, 1)
        for arr, want in [
            (
                pa.array([[ts]], type=pa.large_list(pa.timestamp("us", tz="Asia/Tokyo"))),
                pa.array([[expected]], type=pa.large_list(pa.timestamp("us"))),
            ),
            (
                pa.array([[ts, ts]], type=pa.list_(pa.timestamp("us", tz="Asia/Tokyo"), 2)),
                pa.array([[expected, expected]], type=pa.list_(pa.timestamp("us"), 2)),
            ),
        ]:
            with self.subTest(type=str(arr.type)):
                output = ArrowArrayConversion.localize_tz(arr)
                self.assertEqual(output, want, f"{output.tolist()} != {want.tolist()}")


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowArrayToPandasConversionTests(unittest.TestCase):
    def test_convert_numpy_ser_name_survives_preprocess_time(self):
        # convert_numpy reads the Arrow field name before preprocess_time, because the
        # pa.compute kernels it runs for timestamps return a new array with no field name.
        import pyarrow as pa

        for pa_type in [pa.timestamp("us", tz="UTC"), pa.timestamp("s"), pa.timestamp("ns")]:
            ts = pa.array([datetime.datetime(2020, 6, 15, 12, 30)], type=pa.timestamp("us")).cast(
                pa_type
            )
            col = pa.RecordBatch.from_arrays([ts], ["tscol"]).column(0)
            spark_type = TimestampType() if pa_type.tz is not None else TimestampNTZType()
            result = ArrowArrayToPandasConversion.convert_numpy(col, spark_type, timezone="UTC")
            self.assertEqual(result.name, "tscol", f"name lost for {pa_type}")

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

    def test_array_convert_numpy(self):
        import numpy as np
        import pyarrow as pa

        arr = pa.array([[1, 2, 3], [4, 5]], type=pa.list_(pa.int64()))
        result = ArrowArrayToPandasConversion.convert_numpy(arr, ArrayType(IntegerType()))
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertEqual(list(result.iloc[0]), [1, 2, 3])
        self.assertEqual(list(result.iloc[1]), [4, 5])

        # empty inner arrays
        arr = pa.array([[], [1, 2], []], type=pa.list_(pa.int64()))
        result = ArrowArrayToPandasConversion.convert_numpy(arr, ArrayType(IntegerType()))
        self.assertEqual(len(result.iloc[0]), 0)
        self.assertEqual(list(result.iloc[1]), [1, 2])

        # nulls: inner nulls become NaN (float64) to preserve numeric ndarray dtype
        arr = pa.array([[1, None, 3], None, [4, 5]], type=pa.list_(pa.int64()))
        result = ArrowArrayToPandasConversion.convert_numpy(arr, ArrayType(IntegerType()))
        self.assertTrue(np.isnan(result.iloc[0][1]))
        self.assertIsNone(result.iloc[1])

        # nested arrays
        arr = pa.array([[[1, 2], [3]], [[4, 5]]], type=pa.list_(pa.list_(pa.int64())))
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(ArrayType(IntegerType()))
        )
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertEqual(list(result.iloc[0][0]), [1, 2])
        self.assertEqual(list(result.iloc[0][1]), [3])

    def test_array_with_timestamps(self):
        import numpy as np
        import pyarrow as pa

        # tz-aware timestamps: preprocess_time strips tz and coerces to ns
        ts1 = datetime.datetime(2024, 1, 1, 12, 0, tzinfo=ZoneInfo("UTC"))
        ts2 = datetime.datetime(2024, 6, 15, 8, 30, tzinfo=ZoneInfo("UTC"))
        arr = pa.array([[ts1, ts2]], type=pa.list_(pa.timestamp("us", tz="UTC")))
        result = ArrowArrayToPandasConversion.convert_numpy(arr, ArrayType(TimestampType()))
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertEqual(result.iloc[0][0], np.datetime64("2024-01-01T12:00:00", "ns"))
        self.assertEqual(result.iloc[0][1], np.datetime64("2024-06-15T08:30:00", "ns"))

        # tz-naive timestamps
        arr = pa.array(
            [[datetime.datetime(2024, 1, 1), datetime.datetime(2024, 6, 15)]],
            type=pa.list_(pa.timestamp("us")),
        )
        result = ArrowArrayToPandasConversion.convert_numpy(arr, ArrayType(TimestampNTZType()))
        self.assertEqual(result.iloc[0][0], np.datetime64("2024-01-01T00:00:00", "ns"))

    def test_array_ndarray_as_list(self):
        import pyarrow as pa

        arr = pa.array([[1, 2, 3], [4, 5]], type=pa.list_(pa.int64()))
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(IntegerType()), ndarray_as_list=True
        )
        self.assertIsInstance(result.iloc[0], list)
        self.assertEqual(result.iloc[0], [1, 2, 3])

        # nulls preserved as None (not NaN)
        arr = pa.array([[1, None, 3], None], type=pa.list_(pa.int64()))
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(IntegerType()), ndarray_as_list=True
        )
        self.assertIsInstance(result.iloc[0], list)
        self.assertIsNone(result.iloc[0][1])
        self.assertIsNone(result.iloc[1])

        # nested arrays recursively converted to lists
        arr = pa.array([[[1, 2], [3]]], type=pa.list_(pa.list_(pa.int64())))
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(ArrayType(IntegerType())), ndarray_as_list=True
        )
        self.assertIsInstance(result.iloc[0], list)
        self.assertIsInstance(result.iloc[0][0], list)
        self.assertEqual(result.iloc[0][0], [1, 2])

    def test_array_of_complex_ndarray_as_list(self):
        import numpy as np
        import pyarrow as pa

        variant_type = pa.struct(
            [
                pa.field("value", pa.binary(), nullable=False),
                pa.field("metadata", pa.binary(), nullable=False, metadata={b"variant": b"true"}),
            ]
        )

        # ndarray_as_list=True with element_conv: exercises to_pandas(integer_object_nulls=True)
        arr = pa.array(
            [[{"value": b"\x01", "metadata": b"\x02"}, None], None],
            type=pa.list_(variant_type),
        )
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(VariantType()), ndarray_as_list=True
        )
        self.assertIsInstance(result.iloc[0], list)
        self.assertIsInstance(result.iloc[0][0], VariantVal)
        self.assertIsNone(result.iloc[0][1])
        self.assertIsNone(result.iloc[1])

        # nested ArrayType with element_conv: exercises the recursive branch in
        # _create_element_converter where inner_conv is not None
        arr = pa.array(
            [[[{"value": b"\x01", "metadata": b"\x02"}], None]],
            type=pa.list_(pa.list_(variant_type)),
        )
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(ArrayType(VariantType()))
        )
        # ndarray_as_list=False (default): nested arrays are object ndarrays at every
        # level (consistent regardless of inner length; ragged/None-containing inners
        # stay distinct rather than collapsing into a 2-D array).
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertIsInstance(result.iloc[0][0], np.ndarray)
        self.assertIsInstance(result.iloc[0][0][0], VariantVal)
        self.assertIsNone(result.iloc[0][1])

    def test_array_of_variant(self):
        import numpy as np
        import pyarrow as pa

        variant_type = pa.struct(
            [
                pa.field("value", pa.binary(), nullable=False),
                pa.field("metadata", pa.binary(), nullable=False, metadata={b"variant": b"true"}),
            ]
        )

        arr = pa.array(
            [
                [{"value": b"\x01", "metadata": b"\x02"}, {"value": b"\x03", "metadata": b"\x04"}],
                [None],
                None,
            ],
            type=pa.list_(variant_type),
        )
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(VariantType()), ser_name="v"
        )
        # ndarray_as_list=False (default): preserves ndarray container
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertIsInstance(result.iloc[0][0], VariantVal)
        self.assertEqual(result.iloc[0][0].value, b"\x01")
        self.assertEqual(result.iloc[0][1].value, b"\x03")
        self.assertIsNone(result.iloc[1][0])
        self.assertIsNone(result.iloc[2])

    def test_array_of_udt(self):
        import numpy as np
        import pyarrow as pa

        arr = pa.array(
            [[[1.0, 2.0], [3.0, 4.0]], [None], None],
            type=pa.list_(pa.list_(pa.float64())),
        )
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(ExamplePointUDT()), ser_name="p"
        )
        # ndarray_as_list=False (default): preserves ndarray container
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertEqual(result.iloc[0][0], ExamplePoint(1.0, 2.0))
        self.assertEqual(result.iloc[0][1], ExamplePoint(3.0, 4.0))
        self.assertIsNone(result.iloc[1][0])
        self.assertIsNone(result.iloc[2])

    def test_array_of_geography(self):
        import numpy as np
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

        wkb1 = bytes.fromhex("0101000000000000000000F03F0000000000000040")
        wkb2 = bytes.fromhex("010100000000000000000031400000000000001c40")
        arr = pa.array(
            [
                [{"srid": 4326, "wkb": wkb1}, {"srid": 4326, "wkb": wkb2}],
                [None],
                None,
            ],
            type=pa.list_(geography_type),
        )
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(GeographyType(4326)), ser_name="g"
        )
        # ndarray_as_list=False (default): preserves ndarray container
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertEqual(result.iloc[0][0], Geography(wkb1, 4326))
        self.assertEqual(result.iloc[0][1], Geography(wkb2, 4326))
        self.assertIsNone(result.iloc[1][0])
        self.assertIsNone(result.iloc[2])

    def test_array_of_geometry(self):
        import numpy as np
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

        wkb1 = bytes.fromhex("0101000000000000000000F03F0000000000000040")
        wkb2 = bytes.fromhex("010100000000000000000031400000000000001c40")
        arr = pa.array(
            [
                [{"srid": 0, "wkb": wkb1}, {"srid": 0, "wkb": wkb2}],
                [None],
                None,
            ],
            type=pa.list_(geometry_type),
        )
        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(GeometryType(0)), ser_name="g"
        )
        # ndarray_as_list=False (default): preserves ndarray container
        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertEqual(result.iloc[0][0], Geometry(wkb1, 0))
        self.assertEqual(result.iloc[0][1], Geometry(wkb2, 0))
        self.assertIsNone(result.iloc[1][0])
        self.assertIsNone(result.iloc[2])

    def test_array_of_variant_matches_legacy(self):
        """Verify convert_numpy matches convert_legacy for ArrayType(VariantType()).

        TODO: Remove when convert_legacy is removed.
        """
        import pyarrow as pa

        variant_type = pa.struct(
            [
                pa.field("value", pa.binary(), nullable=False),
                pa.field("metadata", pa.binary(), nullable=False, metadata={b"variant": b"true"}),
            ]
        )
        arr = pa.array(
            [
                [{"value": b"\x01", "metadata": b"\x02"}, {"value": b"\x03", "metadata": b"\x04"}],
                [None],
                None,
            ],
            type=pa.list_(variant_type),
        )
        spark_type = ArrayType(VariantType())

        result_legacy = ArrowArrayToPandasConversion.convert_legacy(arr, spark_type)
        result_new = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type)
        # VariantVal lacks __eq__, so compare element attributes directly
        self.assertEqual(len(result_legacy), len(result_new))
        for i in range(len(result_legacy)):
            l, n = result_legacy.iloc[i], result_new.iloc[i]
            if l is None:
                self.assertIsNone(n)
            else:
                self.assertEqual(len(l), len(n))
                for j in range(len(l)):
                    if l[j] is None:
                        self.assertIsNone(n[j])
                    else:
                        self.assertEqual(l[j].value, n[j].value)
                        self.assertEqual(l[j].metadata, n[j].metadata)

    def test_array_of_udt_matches_legacy(self):
        """Verify convert_numpy matches convert_legacy for ArrayType(ExamplePointUDT()).

        TODO: Remove when convert_legacy is removed.
        """
        import pandas as pd
        import pyarrow as pa

        arr = pa.array(
            [[[1.0, 2.0], [3.0, 4.0]], [None], None],
            type=pa.list_(pa.list_(pa.float64())),
        )
        spark_type = ArrayType(ExamplePointUDT())

        result_legacy = ArrowArrayToPandasConversion.convert_legacy(arr, spark_type)
        result_new = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type)
        pd.testing.assert_series_equal(result_legacy, result_new)

    def test_nested_complex_array_shape_consistent(self):
        """Regression: ArrayType(ArrayType(complex)) inner shape must be a consistent
        object ndarray regardless of inner-array lengths.

        np.asarray(..., dtype=object) collapses equal-length inner arrays into a 2-D
        array but leaves ragged/None-containing inners as Python lists, so the inner
        container type used to depend on the data. It must now always be an ndarray.
        (Note: convert_legacy raises ValueError on the ragged/None cases, so there is no
        legacy behavior to match there.)
        """
        import numpy as np
        import pyarrow as pa

        variant_type = pa.struct(
            [
                pa.field("value", pa.binary(), nullable=False),
                pa.field("metadata", pa.binary(), nullable=False, metadata={b"variant": b"true"}),
            ]
        )

        def v(b):
            return {"value": bytes([b]), "metadata": bytes([b])}

        spark_type = ArrayType(ArrayType(VariantType()))
        for label, row in [
            ("rectangular", [[v(1)], [v(2)]]),
            ("ragged", [[v(1), v(2)], [v(3)]]),
            ("with-None", [[v(1)], None]),
        ]:
            with self.subTest(case=label):
                arr = pa.array([row], type=pa.list_(pa.list_(variant_type)))
                result = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type)
                outer = result.iloc[0]
                self.assertIsInstance(outer, np.ndarray)
                for inner in outer:
                    if inner is not None:
                        self.assertIsInstance(inner, np.ndarray)
                        for elem in inner:
                            self.assertIsInstance(elem, VariantVal)

        # ndarray_as_list=True remains lists all the way down
        arr = pa.array([[[v(1), v(2)], [v(3)]]], type=pa.list_(pa.list_(variant_type)))
        result = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type, ndarray_as_list=True)
        self.assertIsInstance(result.iloc[0], list)
        self.assertIsInstance(result.iloc[0][0], list)
        self.assertIsInstance(result.iloc[0][0][0], VariantVal)

    def test_array_value_parity_with_legacy(self):
        """For array element types that are routed to convert_numpy (i.e. in
        _prefer_convert_numpy's supported_types), convert_numpy matches convert_legacy
        exactly (same values and same representation).

        This pins parity for the element types that the dispatcher actually routes to
        convert_numpy, so a future change that breaks parity for one of them is caught here.

        TODO: Remove when convert_legacy is removed.
        """
        import datetime

        import pandas as pd
        import pyarrow as pa

        cases = [
            (ArrayType(DoubleType()), pa.list_(pa.float64()), [[1.5, None], None]),
            (ArrayType(BooleanType()), pa.list_(pa.bool_()), [[True, None, False]]),
            (ArrayType(BinaryType()), pa.list_(pa.binary()), [[b"a", None]]),
            (ArrayType(DateType()), pa.list_(pa.date32()), [[datetime.date(2020, 1, 1), None]]),
        ]
        for spark_type, pa_type, values in cases:
            with self.subTest(spark_type=spark_type):
                # element type is in supported_types, so the dispatcher uses convert_numpy
                self.assertTrue(
                    ArrowArrayToPandasConversion._prefer_convert_numpy(spark_type, False)
                )
                arr = pa.array(values, type=pa_type)
                legacy = ArrowArrayToPandasConversion.convert_legacy(arr, spark_type)
                numpy = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type)
                pd.testing.assert_series_equal(legacy, numpy)

    def test_array_int_with_null_differs_from_legacy(self):
        """Accepted difference: integer arrays with nulls, when prefer_int_ext_dtype is off.

        convert_numpy widens to float64 (null -> NaN), matching its own scalar-integer
        behavior, while convert_legacy keeps object dtype with Python ints and None. The array
        path mirrors the scalar path deliberately: making arrays alone preserve integers would
        leave array<long> and long disagreeing within a single DataFrame.

        The mirror holds under both settings of
        spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype. With it on, the scalar
        path keeps exact integers (pandas Int*Dtype), and so does the array path (Python ints
        and None, as convert_legacy). Both are asserted at the end.

        This is not only a null-representation change, and the cost is worth stating plainly:

        - Widening to float64 rounds int64 magnitudes above 2**53, so values are silently
          changed, not just nulls (see the LongType case below).
        - The whole flattened child is widened, so rows that contain no null of their own are
          affected too. A single null anywhere in the column is enough.

        The underlying defect is in the scalar path (convert_numpy on a plain IntegerType /
        LongType column loses precision the same way on master) and should be fixed there, for
        the scalar and array paths at once, rather than papered over here.
        """
        import numpy as np
        import pyarrow as pa

        for spark_type, pa_type in [
            (ArrayType(IntegerType()), pa.list_(pa.int32())),
            (ArrayType(LongType()), pa.list_(pa.int64())),
        ]:
            with self.subTest(spark_type=spark_type):
                arr = pa.array([[1, None, 3]], type=pa_type)
                legacy = ArrowArrayToPandasConversion.convert_legacy(arr, spark_type).iloc[0]
                numpy = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type).iloc[0]
                self.assertEqual(legacy.dtype, object)
                self.assertEqual(legacy.tolist(), [1, None, 3])
                self.assertEqual(numpy.dtype, np.float64)
                self.assertEqual(numpy[0], 1.0)
                self.assertTrue(np.isnan(numpy[1]))
                self.assertEqual(numpy[2], 3.0)

        # Pin the two consequences above, so lifting this divergence has a failing test.
        spark_type = ArrayType(LongType())
        beyond_float64 = 2**53 + 1
        arr = pa.array([[beyond_float64], [1, None]], type=pa.list_(pa.int64()))
        legacy = ArrowArrayToPandasConversion.convert_legacy(arr, spark_type)
        numpy = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type)
        # convert_legacy is exact; convert_numpy rounds ...93 down to ...92.
        self.assertEqual(legacy.iloc[0].tolist(), [beyond_float64])
        self.assertEqual(numpy.iloc[0].tolist(), [float(beyond_float64 - 1)])
        # Row 0 holds no null of its own, yet is widened because row 1 does.
        self.assertEqual(numpy.iloc[0].dtype, np.float64)

        # With prefer_int_ext_dtype on, neither path loses anything: the scalar path keeps an
        # Int64Dtype column, and the array path keeps exact Python ints and None like legacy.
        scalar = ArrowArrayToPandasConversion.convert_numpy(
            pa.array([beyond_float64, None], type=pa.int64()), LongType(), prefer_int_ext_dtype=True
        )
        self.assertEqual(str(scalar.dtype), "Int64")
        self.assertEqual(scalar.iloc[0], beyond_float64)
        exact = ArrowArrayToPandasConversion.convert_numpy(
            arr, spark_type, prefer_int_ext_dtype=True
        )
        self.assertEqual(exact.iloc[0].tolist(), [beyond_float64])
        self.assertEqual(exact.iloc[1].tolist(), [1, None])
        self.assertEqual(exact.iloc[1].tolist(), legacy.iloc[1].tolist())

    def test_array_timestamp_elements_match_legacy(self):
        """Timestamp array elements are pd.Timestamp / pd.NaT, exactly as convert_legacy
        produces, in both modes.

        to_pandas() hands back datetime64 elements, and keeping those would have made
        array<timestamp*> the only type whose representation differs from convert_legacy: every
        other type the new converter supports matches it, and a scalar timestamp column is a
        native datetime64[ns] Series on both paths. A bare np.datetime64 also has no datetime
        API, so `xs[0].year` and `xs[0] is pd.NaT` would have broken in existing UDFs.
        """
        import pandas as pd
        import pyarrow as pa

        ts = datetime.datetime(2024, 1, 1, 12, 0)
        utc = datetime.timezone.utc
        for spark_type, arr in [
            (
                ArrayType(TimestampNTZType()),
                pa.array([[ts, None], [], None], type=pa.list_(pa.timestamp("us"))),
            ),
            (
                ArrayType(TimestampType()),
                pa.array(
                    [[ts.replace(tzinfo=utc), None]], type=pa.list_(pa.timestamp("us", tz="UTC"))
                ),
            ),
            (
                # Equal-length inner arrays: convert_legacy cannot build a ragged nested
                # array in ndarray mode, so there is nothing to compare against there.
                ArrayType(ArrayType(TimestampNTZType())),
                pa.array([[[ts, None], [ts, None]]], type=pa.list_(pa.list_(pa.timestamp("us")))),
            ),
        ]:
            for ndarray_as_list in (False, True):
                with self.subTest(spark_type=spark_type, ndarray_as_list=ndarray_as_list):
                    options = dict(timezone="UTC", ndarray_as_list=ndarray_as_list)
                    legacy = ArrowArrayToPandasConversion.convert_legacy(arr, spark_type, **options)
                    numpy = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type, **options)

                    def element_types(v):
                        if v is None:
                            return None
                        return [element_types(x) if isinstance(x, list) else type(x) for x in v]

                    self.assertEqual(
                        [element_types(v) for v in numpy], [element_types(v) for v in legacy]
                    )

        arr = pa.array([[ts]], type=pa.list_(pa.timestamp("us")))
        row = ArrowArrayToPandasConversion.convert_numpy(
            arr, ArrayType(TimestampNTZType()), timezone="UTC"
        ).iloc[0]
        self.assertIsInstance(row[0], pd.Timestamp)
        self.assertEqual(row[0].year, 2024)

        # A scalar timestamp column keeps the native dtype: it needs no element boxing.
        scalar = ArrowArrayToPandasConversion.convert_numpy(
            pa.array([ts], type=pa.timestamp("us")), TimestampNTZType(), timezone="UTC"
        )
        self.assertEqual(str(scalar.dtype), "datetime64[ns]")

    def test_array_timestamp_session_timezone_matches_legacy(self):
        """Under a non-UTC session timezone, timestamps reach UDFs on the session-local wall
        clock on both paths, for scalars and arrays alike, and convert back to the same
        instant.

        The input is built as Spark sends it: TimestampType Arrow data is tagged with the
        session timezone (ArrowUtils.toArrowType(TimestampType, conf.sessionLocalTimeZone)).
        convert_legacy localizes with the session timezone argument and convert_numpy with the
        Arrow type's own timezone, so they agree precisely on that pairing. TimestampNTZType has
        no timezone to apply and stays on its own wall clock.
        """
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        # 2020-06-15 12:00Z is 05:00 in Los Angeles (PDT, UTC-7); a fixed summer date keeps
        # the offset unambiguous.
        session_tz = "America/Los_Angeles"
        instant = datetime.datetime(2020, 6, 15, 12, 0, tzinfo=datetime.timezone.utc)
        naive = datetime.datetime(2020, 6, 15, 12, 0)
        ltz = pa.timestamp("us", tz=session_tz)
        ntz = pa.timestamp("us")

        def wall_clock(series):
            """First timestamp of the series, unwrapping the array element if there is one."""
            value = series.iloc[0]
            return pd.Timestamp(value[0] if isinstance(value, np.ndarray) else value)

        for spark_type, arr, expected in [
            (TimestampType(), pa.array([instant, None], type=ltz), "2020-06-15 05:00:00"),
            (
                ArrayType(TimestampType()),
                pa.array([[instant, None], None], type=pa.list_(ltz)),
                "2020-06-15 05:00:00",
            ),
            (TimestampNTZType(), pa.array([naive, None], type=ntz), "2020-06-15 12:00:00"),
            (
                ArrayType(TimestampNTZType()),
                pa.array([[naive, None], None], type=pa.list_(ntz)),
                "2020-06-15 12:00:00",
            ),
        ]:
            with self.subTest(spark_type=spark_type):
                legacy = ArrowArrayToPandasConversion.convert_legacy(
                    arr, spark_type, timezone=session_tz
                )
                numpy = ArrowArrayToPandasConversion.convert_numpy(
                    arr, spark_type, timezone=session_tz
                )
                self.assertEqual(wall_clock(legacy), pd.Timestamp(expected))
                self.assertEqual(wall_clock(numpy), wall_clock(legacy))
                schema = StructType([StructField("c", spark_type)])
                back = PandasToArrowConversion.convert([numpy], schema, timezone=session_tz)
                self.assertEqual(back.column(0).to_pylist(), arr.to_pylist())

    def test_array_timestamp_out_of_ns_range_falls_back_to_legacy(self):
        """Timestamp arrays outside the datetime64[ns] range must still convert.

        convert_numpy runs preprocess_time, which coerces timestamps to nanoseconds, a range
        of only 1677-09-21 to 2262-04-11. convert_legacy has no such limit for array elements:
        it keeps them as pd.Timestamp objects in an object ndarray. Routing arrays to
        convert_numpy would therefore have failed a batch that converted before, so it falls
        back to convert_legacy when the coercion overflows.

        A scalar timestamp column out of range fails on both paths, as it already did on
        master; only arrays are rescued here.
        """
        import pyarrow as pa

        far = datetime.datetime(3000, 1, 1)
        old = datetime.datetime(1500, 6, 1)
        utc = datetime.timezone.utc
        for spark_type, arr in [
            (
                ArrayType(TimestampNTZType()),
                pa.array([[far, None], [old], [], None], type=pa.list_(pa.timestamp("us"))),
            ),
            (
                ArrayType(TimestampType()),
                pa.array(
                    [[far.replace(tzinfo=utc), None]],
                    type=pa.list_(pa.timestamp("us", tz="UTC")),
                ),
            ),
            (
                # Inner arrays of equal length and no null inner array: convert_legacy cannot
                # build a ragged nested array in ndarray mode ("setting an array element with
                # a sequence"), and the fallback inherits that limitation exactly.
                ArrayType(ArrayType(TimestampNTZType())),
                pa.array([[[far], [old]], None], type=pa.list_(pa.list_(pa.timestamp("us")))),
            ),
        ]:
            schema = StructType([StructField("c", spark_type)])
            for ndarray_as_list in (False, True):
                with self.subTest(spark_type=spark_type, ndarray_as_list=ndarray_as_list):
                    options = dict(timezone="UTC", ndarray_as_list=ndarray_as_list)
                    legacy = ArrowArrayToPandasConversion.convert_legacy(arr, spark_type, **options)
                    numpy = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type, **options)
                    self.assertEqual(
                        [None if v is None else list(v) for v in numpy],
                        [None if v is None else list(v) for v in legacy],
                    )
                    back = PandasToArrowConversion.convert([numpy], schema, timezone="UTC")
                    self.assertEqual(back.column(0).to_pylist(), arr.to_pylist())

        # In-range arrays take the normal path and still match legacy.
        in_range = pa.array(
            [[datetime.datetime(2024, 1, 1), None]], type=pa.list_(pa.timestamp("us"))
        )
        self.assertEqual(
            list(
                ArrowArrayToPandasConversion.convert_numpy(
                    in_range, ArrayType(TimestampNTZType()), timezone="UTC"
                ).iloc[0]
            ),
            list(
                ArrowArrayToPandasConversion.convert_legacy(
                    in_range, ArrayType(TimestampNTZType()), timezone="UTC"
                ).iloc[0]
            ),
        )

    def test_array_timestamp_ntz_round_trip(self):
        """A pandas UDF returning its array<timestamp_ntz> input unchanged must round-trip.

        Cover ordinary and nested arrays, null elements, and null and empty arrays, in both
        modes. See test_converter_from_pandas_array_of_timestamp_ntz for the output converter's
        own handling of datetime64 elements, which a UDF can still build itself.
        """
        import pyarrow as pa

        ts = datetime.datetime(2024, 1, 1, 12, 0, 0, 123456)
        ntz = pa.timestamp("us")
        for spark_type, arr in [
            (
                ArrayType(TimestampNTZType()),
                pa.array([[ts, None], [], None, [ts]], type=pa.list_(ntz)),
            ),
            (
                ArrayType(ArrayType(TimestampNTZType())),
                pa.array([[[ts, None], [], None], None, []], type=pa.list_(pa.list_(ntz))),
            ),
        ]:
            schema = StructType([StructField("c", spark_type)])
            for ndarray_as_list in (False, True):
                with self.subTest(spark_type=spark_type, ndarray_as_list=ndarray_as_list):
                    series = ArrowArrayToPandasConversion.convert_numpy(
                        arr, spark_type, timezone="UTC", ndarray_as_list=ndarray_as_list
                    )
                    back = PandasToArrowConversion.convert([series], schema, timezone="UTC")
                    self.assertEqual(back.column(0).to_pylist(), arr.to_pylist())

    def test_array_unsupported_element_falls_back_to_legacy(self):
        """Array[E] is routed to convert_numpy only when E is in supported_types. Element
        types not in supported_types (StringType, DecimalType, the interval types) and
        MapType/StructType stay on the legacy path, consistent with their scalar routing, so
        an unconfirmed element type cannot silently change its toPandas() representation.
        Pin both the routing (False) and that the dispatcher still produces correct output.
        """
        import decimal

        import pyarrow as pa

        prefer = ArrowArrayToPandasConversion._prefer_convert_numpy
        # element types not in supported_types -> legacy
        for spark_type in [
            ArrayType(StringType()),
            ArrayType(DecimalType(10, 2)),
            ArrayType(DayTimeIntervalType()),
            ArrayType(StructType([StructField("a", IntegerType())])),
            ArrayType(MapType(StringType(), IntegerType())),
            ArrayType(ArrayType(StringType())),
        ]:
            self.assertFalse(prefer(spark_type, False), msg=str(spark_type))

        # common Array[String] still converts correctly via the dispatcher (convert_legacy)
        st = ArrayType(StringType())
        arr = pa.array([["a", None, "c"], None], type=pa.list_(pa.string()))
        result = ArrowArrayToPandasConversion.convert(arr, st, timezone="UTC")
        self.assertEqual(list(result.iloc[0]), ["a", None, "c"])
        self.assertIsNone(result.iloc[1])

        # Array[Decimal]
        st = ArrayType(DecimalType(10, 2))
        arr = pa.array([[decimal.Decimal("1.50"), None]], type=pa.list_(pa.decimal128(10, 2)))
        result = ArrowArrayToPandasConversion.convert(arr, st, timezone="UTC")
        self.assertEqual(result.iloc[0][0], decimal.Decimal("1.50"))

        # Array[Struct]
        st = ArrayType(StructType([StructField("a", IntegerType())]))
        arr = pa.array([[{"a": 1}, None], None], type=pa.list_(pa.struct([("a", pa.int32())])))
        result = ArrowArrayToPandasConversion.convert(
            arr, st, timezone="UTC", struct_in_pandas="dict"
        )
        self.assertEqual(result.iloc[0][0], {"a": 1})
        self.assertIsNone(result.iloc[1])

        # Array[Map]
        st = ArrayType(MapType(StringType(), IntegerType()))
        arr = pa.array([[[("k", 1)], None]], type=pa.list_(pa.map_(pa.string(), pa.int32())))
        result = ArrowArrayToPandasConversion.convert(
            arr, st, timezone="UTC", struct_in_pandas="dict"
        )
        self.assertEqual(result.iloc[0][0], {"k": 1})
        self.assertIsNone(result.iloc[0][1])

    def test_array_of_udt_stays_on_legacy(self):
        """array<UDT> stays on the legacy path at every nesting depth.

        convert_numpy's UDT branch hands udt.deserialize the raw arr.to_pandas() value, which
        is a dict for a StructType sqlType (VectorUDT/MatrixUDT) that deserialize indexes
        positionally. Routing array<UDT> to convert_numpy would therefore raise KeyError where
        convert_legacy succeeds. Pin the routing and that the dispatcher still converts, so the
        exclusion can be lifted deliberately once SPARK-55462 is fixed.
        """
        import pyarrow as pa

        from pyspark.ml.linalg import Vectors, VectorUDT
        from pyspark.sql.pandas.types import to_arrow_type

        prefer = ArrowArrayToPandasConversion._prefer_convert_numpy
        for spark_type in [
            ArrayType(VectorUDT()),
            ArrayType(ExamplePointUDT()),
            ArrayType(ArrayType(VectorUDT())),
        ]:
            self.assertFalse(prefer(spark_type, False), msg=str(spark_type))

        udt = VectorUDT()
        ser = udt.serialize(Vectors.dense([1.0, 2.0]))
        arr = pa.array([[ser, ser], None], type=pa.list_(to_arrow_type(udt.sqlType())))
        result = ArrowArrayToPandasConversion.convert(
            arr, ArrayType(udt), timezone="UTC", struct_in_pandas="dict"
        )
        self.assertEqual([v.tolist() for v in result.iloc[0]], [[1.0, 2.0], [1.0, 2.0]])
        self.assertIsNone(result.iloc[1])

    def test_array_of_malformed_data(self):
        """Malformed data raises PySparkValueError for Variant/Geography/Geometry."""
        import pyarrow as pa

        bad_type = pa.struct([pa.field("bad_key", pa.binary(), nullable=False)])
        arr = pa.array([[{"bad_key": b"\x01"}]], type=pa.list_(bad_type))

        for spark_type, error_class in [
            (ArrayType(VariantType()), "MALFORMED_VARIANT"),
            (ArrayType(GeographyType(4326)), "MALFORMED_GEOGRAPHY"),
            (ArrayType(GeometryType(0)), "MALFORMED_GEOMETRY"),
        ]:
            with self.assertRaises(PySparkValueError) as ctx:
                ArrowArrayToPandasConversion.convert_numpy(arr, spark_type)
            self.assertIn(error_class, ctx.exception.getCondition())


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class ArrayConversionParityTests(unittest.TestCase):
    """Differential test: convert_numpy must agree with convert_legacy on every array column
    routed to it, across element types, Arrow list layouts, nesting, slices, chunking, list
    and ndarray modes, prefer_int_ext_dtype and session timezones.

    Hand-picked cases kept missing combinations: a null array through preprocess_time, a
    prefix slice of a fixed-size list, timestamps in list mode, prefer_int_ext_dtype on
    arrays, array<timestamp_ntz> failing to convert back. Each reached review before a test.
    Sweeping the space catches that class of regression up front.

    Each case checks both directions a UDF sees:

    - Input: what convert_numpy produces must equal what convert_legacy produces, compared in
      a canonical form. In list mode the element Python types must also match exactly, since
      that mode feeds row-wise Python UDFs.
    - Round trip: a UDF may return what it received, so the value must convert back through
      PandasToArrowConversion, the output converter the worker uses, to exactly what legacy's
      value converts back to.

    TimestampType input is built as Spark sends it, tagged with the session timezone
    (ArrowUtils.toArrowType(TimestampType, conf.sessionLocalTimeZone)). convert_legacy
    localizes with the session timezone argument and convert_numpy with the Arrow type's own
    timezone, so the two agree precisely on that pairing.

    One difference is accepted, as a precise rule rather than a skip, so any other difference,
    even for the same element type, still fails: in ndarray mode with prefer_int_ext_dtype
    off, integers in a column holding a null are widened to float64, as the scalar integer
    path does (see test_array_int_with_null_differs_from_legacy). The rule requires

    - the input to equal legacy with every int widened to float;
    - the round trip to equal legacy's with every int rounded through float, or, when widening
      puts a value outside the int64 range (Long.MaxValue rounds up to 2**63), to fail, and
      to fail only then.
    """

    _NULL = "<NULL>"
    _INT_FAMILY = (ByteType, ShortType, IntegerType, LongType)

    @classmethod
    def _canon(cls, x):
        """Hashable, container-agnostic form of a converted value.

        ndarray and list compare equal, and every null flavor (None, NaN, NaT, pd.NA) is one
        null, since they all mean SQL NULL. Scalars keep a kind tag, so an int that became a
        float, or a null list that became an empty one, still differs.
        """
        import numpy as np
        import pandas as pd

        if isinstance(x, (list, tuple, np.ndarray)):
            return ("list", tuple(cls._canon(e) for e in x))
        if x is None or x is pd.NaT or x is pd.NA:
            return cls._NULL
        if isinstance(x, (float, np.floating)):
            return cls._NULL if math.isnan(x) else ("float", float(x))
        if isinstance(x, np.datetime64):
            return cls._NULL if np.isnat(x) else ("ts", pd.Timestamp(x).value)
        if isinstance(x, (pd.Timestamp, datetime.datetime)):
            return ("ts", pd.Timestamp(x).value)
        if isinstance(x, (bool, np.bool_)):
            return ("bool", bool(x))
        if isinstance(x, (int, np.integer)):
            return ("int", int(x))
        if isinstance(x, VariantVal):
            return ("variant", bytes(x.value), bytes(x.metadata))
        if isinstance(x, (Geography, Geometry)):
            return (type(x).__name__, x.getSrid(), bytes(x.getBytes()))
        if isinstance(x, (bytes, bytearray)):
            return ("bytes", bytes(x))
        if isinstance(x, (datetime.date, datetime.time)):
            return (type(x).__name__, x)
        return ("other", type(x).__name__, repr(x))

    @classmethod
    def _types_of(cls, x):
        if isinstance(x, list):
            return ("list", tuple(cls._types_of(e) for e in x))
        return type(x).__name__

    @classmethod
    def _widen_ints(cls, c):
        """A canonical value with every int widened to float: the accepted integer rule."""
        if isinstance(c, tuple) and c[:1] == ("list",):
            return ("list", tuple(cls._widen_ints(e) for e in c[1]))
        if isinstance(c, tuple) and c[:1] == ("int",):
            return ("float", float(c[1]))
        return c

    @classmethod
    def _exact(cls, x):
        """Exact form of a to_pylist() value. Unlike _canon, NaN and None stay distinct: after
        a round trip a NaN is a value and None a SQL NULL."""
        if isinstance(x, list):
            return tuple(cls._exact(e) for e in x)
        if isinstance(x, float) and math.isnan(x):
            return "NaN"
        return x

    @classmethod
    def _round_ints_through_float(cls, x):
        """A to_pylist() value with every int passed through float64: the accepted integer
        rule, seen after a round trip."""
        if isinstance(x, list):
            return [cls._round_ints_through_float(e) for e in x]
        if isinstance(x, int) and not isinstance(x, bool):
            return int(float(x))
        return x

    @classmethod
    def _widened_int_out_of_range(cls, series):
        """True when passing an integer in `series` through float64 puts it outside the int64
        range, so the widened value cannot convert back (Long.MaxValue rounds up to 2**63)."""
        import numpy as np

        limit = 2**63

        def walk(v):
            if isinstance(v, (list, tuple, np.ndarray)):
                return any(walk(e) for e in v)
            if isinstance(v, (int, np.integer)) and not isinstance(v, bool):
                return not -limit <= float(v) < limit
            return False

        return any(walk(v) for v in series if v is not None)

    @staticmethod
    def _has_null_leaf(arr):
        def walk(v):
            return any(walk(e) for e in v) if isinstance(v, list) else v is None

        return any(walk(row) for row in arr.to_pylist() if row is not None)

    @staticmethod
    def _elements():
        wkb1 = bytes.fromhex("0101000000000000000000F03F0000000000000040")
        wkb2 = bytes.fromhex("010100000000000000000031400000000000001c40")
        utc = datetime.timezone.utc
        return [
            ("null", NullType(), (None, None)),
            ("bool", BooleanType(), (True, False)),
            ("byte", ByteType(), (1, -2)),
            ("short", ShortType(), (1, -300)),
            ("int", IntegerType(), (1, -70000)),
            # 2**53 + 1 is the first integer float64 cannot represent, so widening shows.
            # Long.MaxValue rounds *up* to 2**63, out of the int64 range, so a widened column
            # cannot convert back at all.
            ("long", LongType(), (2**53 + 1, 2**63 - 1)),
            ("float", FloatType(), (1.5, -2.25)),
            ("double", DoubleType(), (1.5, -2.25)),
            ("binary", BinaryType(), (b"a", b"")),
            # 9999-12-31 overflows datetime64[ns]; both paths must keep it a date.
            ("date", DateType(), (datetime.date(2024, 1, 1), datetime.date(9999, 12, 31))),
            ("time", TimeType(), (datetime.time(12, 30, 1, 5), datetime.time(0, 0))),
            (
                "timestamp_ntz",
                TimestampNTZType(),
                (datetime.datetime(2024, 1, 1, 12, 0, 0, 123456), datetime.datetime(1970, 1, 1)),
            ),
            (
                "timestamp",
                TimestampType(),
                (
                    datetime.datetime(2024, 1, 1, 12, tzinfo=utc),
                    datetime.datetime(1970, 1, 1, tzinfo=utc),
                ),
            ),
            (
                "variant",
                VariantType(),
                ({"value": b"\x01", "metadata": b"\x02"}, {"value": b"\x03", "metadata": b"\x04"}),
            ),
            (
                "geography",
                GeographyType(4326),
                ({"srid": 4326, "wkb": wkb1}, {"srid": 4326, "wkb": wkb2}),
            ),
            (
                "geometry",
                GeometryType(0),
                ({"srid": 0, "wkb": wkb1}, {"srid": 0, "wkb": wkb2}),
            ),
        ]

    @staticmethod
    def _layouts(element_type, arrow_element, v1, v2):
        """(name, spark type, arrow array) for each list layout.

        All but no_nulls hold a null row, a null element and an empty list where the layout
        allows one.
        """
        import pyarrow as pa

        varlen = [[v1, v2], [v1, None], [], None, [v2]]
        array_type = ArrayType(element_type)
        yield "list", array_type, pa.array(varlen, type=pa.list_(arrow_element))
        yield "large_list", array_type, pa.array(varlen, type=pa.large_list(arrow_element))
        yield (
            "fixed_size_list",
            array_type,
            pa.array([[v1, v2], [v1, None], None, [v2, v1]], type=pa.list_(arrow_element, 2)),
        )
        yield (
            "nested",
            ArrayType(array_type),
            pa.array(
                [[[v1], [], None], None, [[v2, None]], []], type=pa.list_(pa.list_(arrow_element))
            ),
        )
        yield "no_nulls", array_type, pa.array([[v1, v2], [], [v2]], type=pa.list_(arrow_element))

    @staticmethod
    def _shapes(arr):
        """The array whole, as prefix / suffix / interior slices, and chunked."""
        import pyarrow as pa

        yield "full", arr
        yield "prefix", arr.slice(0, 2)
        yield "suffix", arr.slice(1)
        yield "interior", arr.slice(1, max(1, len(arr) - 2))
        yield "chunked", pa.chunked_array([arr.slice(0, 2), arr.slice(2)], type=arr.type)

    def _divergence(self, arr, spark_type, ndarray_as_list, prefer_int_ext_dtype, timezone):
        """None when convert_numpy agrees with convert_legacy up to an accepted rule, else a
        description of the first unexplained difference."""
        leaf = spark_type
        while isinstance(leaf, ArrayType):
            leaf = leaf.elementType
        options = dict(struct_in_pandas="dict", ndarray_as_list=ndarray_as_list)
        try:
            numpy = ArrowArrayToPandasConversion.convert_numpy(
                arr,
                spark_type,
                timezone=timezone,
                prefer_int_ext_dtype=prefer_int_ext_dtype,
                **options,
            )
        except Exception as e:
            return f"convert_numpy raised {type(e).__name__}: {e}"

        try:
            legacy = ArrowArrayToPandasConversion.convert_legacy(
                arr, spark_type, timezone=timezone, **options
            )
        except ValueError:
            # Legacy cannot build some nested arrays in ndarray mode ("setting an array
            # element with a sequence"), which convert_numpy handles. Use legacy list mode as
            # the oracle there; `_canon` ignores the container kind.
            if ndarray_as_list:
                raise
            legacy = ArrowArrayToPandasConversion.convert_legacy(
                arr, spark_type, timezone=timezone, struct_in_pandas="dict", ndarray_as_list=True
            )

        int_widening_applies = (
            isinstance(leaf, self._INT_FAMILY)
            and not ndarray_as_list
            and not prefer_int_ext_dtype
            and self._has_null_leaf(arr)
        )

        # Input.
        if len(numpy) != len(legacy):
            return f"length {len(numpy)} != legacy {len(legacy)}"
        numpy_values = [self._canon(x) for x in numpy]
        legacy_values = [self._canon(x) for x in legacy]
        # Whether widening actually happened is observed here, not predicted: pyarrow widens
        # only when the innermost element child holds a null, so a column whose only nulls are
        # null or empty *arrays* keeps its integers exact.
        widened = False
        if numpy_values != legacy_values:
            if int_widening_applies and numpy_values == [
                self._widen_ints(c) for c in legacy_values
            ]:
                widened = True
            else:
                numpy_row, legacy_row = next(
                    (n, lg) for n, lg in zip(numpy_values, legacy_values) if n != lg
                )
                return f"value {numpy_row} != legacy {legacy_row}"
        if ndarray_as_list:
            numpy_types = [self._types_of(x) for x in numpy]
            legacy_types = [self._types_of(x) for x in legacy]
            if numpy_types != legacy_types:
                numpy_row, legacy_row = next(
                    (n, lg) for n, lg in zip(numpy_types, legacy_types) if n != lg
                )
                return f"element types {numpy_row} != legacy {legacy_row}"

        # Round trip.
        schema = StructType([StructField("c", spark_type)])
        # Second half of the accepted integer rule: a widened value can land outside the int64
        # range, and then it does not come back at all: an error, not a wrong number.
        overflows = widened and self._widened_int_out_of_range(legacy)
        try:
            numpy_back = PandasToArrowConversion.convert([numpy], schema, timezone=timezone)
        except Exception as e:
            if overflows:
                return None
            return f"round trip raised {type(e).__name__}: {e}"
        if overflows:
            return "round trip succeeded for an integer widened out of the int64 range"
        legacy_back = PandasToArrowConversion.convert([legacy], schema, timezone=timezone)
        expected = legacy_back.column(0).to_pylist()
        if widened:
            expected = self._round_ints_through_float(expected)
        numpy_rows = [self._exact(x) for x in numpy_back.column(0).to_pylist()]
        expected_rows = [self._exact(x) for x in expected]
        if numpy_rows != expected_rows:
            numpy_row, legacy_row = next(
                (n, lg) for n, lg in zip(numpy_rows, expected_rows) if n != lg
            )
            return f"round trip {numpy_row} != legacy {legacy_row}"
        return None

    def test_convert_numpy_matches_legacy_for_arrays(self):
        from pyspark.sql.pandas.types import to_arrow_type

        for name, element_type, (v1, v2) in self._elements():
            for timezone in ("UTC", "America/Los_Angeles"):
                # Tag TimestampType with the session timezone, as Spark does.
                arrow_element = to_arrow_type(element_type, timezone=timezone)
                for layout, spark_type, base in self._layouts(element_type, arrow_element, v1, v2):
                    with self.subTest(element=name, layout=layout, timezone=timezone):
                        # If a type stops being routed, this sweep no longer covers it; make
                        # that a deliberate change here rather than a silent loss of coverage.
                        self.assertTrue(
                            ArrowArrayToPandasConversion._prefer_convert_numpy(spark_type, False),
                            f"{spark_type} is no longer routed to convert_numpy",
                        )
                        failures = []
                        for shape, arr in self._shapes(base):
                            for ndarray_as_list, prefer_int_ext_dtype in itertools.product(
                                (False, True), (False, True)
                            ):
                                problem = self._divergence(
                                    arr, spark_type, ndarray_as_list, prefer_int_ext_dtype, timezone
                                )
                                if problem is not None:
                                    failures.append(
                                        f"{shape}, ndarray_as_list={ndarray_as_list}, "
                                        f"prefer_int_ext_dtype={prefer_int_ext_dtype}: {problem}"
                                    )
                        self.assertEqual(failures, [], "\n" + "\n".join(failures))


class ConvertNumpyVsLegacyTests(unittest.TestCase):
    """Tests documenting known behavioral differences between convert_numpy and convert_legacy.

    These differences are unreachable in practice (convert_numpy only receives raw Arrow data
    from arr.to_pandas(), never already-deserialized Python objects), but are documented here
    for completeness.

    TODO: Remove this test class when convert_legacy is removed.
    """

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_already_converted_variant_guard(self):
        """Legacy _create_converter_to_pandas has an isinstance(value, VariantVal) guard
        that returns already-converted values as-is. convert_numpy's _create_element_converter
        does not have this guard, since it only processes raw Arrow output (dicts), never
        already-deserialized VariantVal objects.

        This difference is unreachable in practice: convert_numpy always receives data from
        arr.to_pandas() which returns raw dicts from Arrow structs, not VariantVal objects.
        """
        import pandas as pd

        from pyspark.sql.pandas.types import _create_converter_to_pandas

        # Simulate a Series containing an already-converted VariantVal
        series = pd.Series([[VariantVal(b"\x01", b"\x02")]])

        # legacy: isinstance(value, VariantVal) guard returns as-is
        converter = _create_converter_to_pandas(
            ArrayType(VariantType()), nullable=True, ndarray_as_list=True
        )
        result_legacy = converter(series)
        self.assertIsInstance(result_legacy.iloc[0][0], VariantVal)

        # convert_numpy: no guard, treats input as raw dict and raises PySparkValueError
        element_conv = ArrowArrayToPandasConversion._create_element_converter(VariantType())
        with self.assertRaises(PySparkValueError):
            element_conv(VariantVal(b"\x01", b"\x02"))


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowColumnToPylistTests(unittest.TestCase):
    """
    ArrowTableToRowsConversion._to_pylist must return exactly what
    column.to_pylist() returns, including exact element types.
    """

    def setUp(self):
        # Force the manual bulk paths so they stay covered regardless of the
        # installed PyArrow version (with a fast native PyArrow the method
        # short-circuits to column.to_pylist()).
        self._gate_patcher = unittest.mock.patch.object(
            ArrowTableToRowsConversion, "_should_manual_bulk", lambda: True
        )
        self._gate_patcher.start()

    def tearDown(self):
        self._gate_patcher.stop()

    def test_native_to_pylist_gate(self):
        import pyarrow as pa

        column = pa.array([[1, None], None], type=pa.list_(pa.int32()))
        with unittest.mock.patch.object(
            ArrowTableToRowsConversion, "_should_manual_bulk", lambda: False
        ):
            self.assertEqual(ArrowTableToRowsConversion._to_pylist(column), [[1, None], None])

    def _assert_identical_types(self, actual, expected):
        self.assertIs(type(actual), type(expected))
        if isinstance(actual, (list, tuple)):
            self.assertEqual(len(actual), len(expected))
            for a, e in zip(actual, expected):
                self._assert_identical_types(a, e)

    def test_matches_to_pylist(self):
        import pyarrow as pa

        columns = [
            pa.array([[1, None, 3], None, [], [4]], type=pa.list_(pa.int32())),
            pa.array([["a", None], None, [], ["bcd", ""]], type=pa.list_(pa.string())),
            pa.array([["a", None], None, ["b"]], type=pa.large_list(pa.string())),
            pa.array([[[1], None, [2, None]], None], type=pa.list_(pa.list_(pa.int32()))),
            pa.array(
                [[{"a": 1, "b": "x"}, None], None],
                type=pa.list_(pa.struct([("a", pa.int32()), ("b", pa.string())])),
            ),
            pa.array([[("k1", 1), ("k2", None)], None, []], type=pa.map_(pa.string(), pa.int32())),
            pa.array([[1.5, None], [float("nan")]], type=pa.list_(pa.float64())),
            pa.array([1, None, 3], type=pa.int64()),
            pa.array(["x", None], type=pa.string()),
            pa.array([], type=pa.list_(pa.int32())),
            pa.array([None, None], type=pa.list_(pa.string())),
            pa.array([[1, 2], None], type=pa.list_(pa.int64(), 2)),
            # non-list leaves keep as_py semantics (native to_pylist)
            pa.array([b"", None, b"\x00\xff"], type=pa.binary()),
            pa.array([datetime.date(2020, 1, 2), None], type=pa.date32()),
            pa.array([decimal.Decimal("1.23"), None], type=pa.decimal128(10, 2)),
            pa.array([[b"x", None], None, [b""]], type=pa.list_(pa.binary())),
            pa.array([[True, None], [False]], type=pa.list_(pa.bool_())),
            # struct and map bulk paths
            pa.array(
                [{"a": 1, "b": "x"}, None, {"a": None, "b": None}],
                type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
            ),
            pa.array(
                [{"s": {"a": 1}, "l": [1, None]}, None],
                type=pa.struct(
                    [("s", pa.struct([("a", pa.int32())])), ("l", pa.list_(pa.int64()))]
                ),
            ),
            pa.array([{}, None, {}], type=pa.struct([])),
            pa.array([None] * 4, type=pa.struct([("a", pa.int32())])),
            pa.array(
                [[("k1", [1, None]), ("k2", None)], None, []],
                type=pa.map_(pa.string(), pa.list_(pa.int32())),
            ),
            pa.array(
                [{"m": [("k", 1)]}, None],
                type=pa.struct([("m", pa.map_(pa.string(), pa.int64()))]),
            ),
            pa.array(
                [[{"a": 1}, None], None],
                type=pa.list_(pa.struct([("a", pa.int64())])),
            ),
        ]
        for column in columns:
            views = [column, column.slice(1), column.slice(0, max(len(column) - 1, 0))]
            views.append(pa.chunked_array([column, column.slice(1)], type=column.type))
            for view in views:
                with self.subTest(type=str(column.type), length=len(view)):
                    actual = ArrowTableToRowsConversion._to_pylist(view)
                    expected = view.to_pylist()
                    # NaN != NaN; compare via repr for the float case
                    self.assertEqual(repr(actual), repr(expected))
                    self._assert_identical_types(actual, expected)

    def test_int_list_with_nulls_stays_int(self):
        # The exact case that makes a pandas round trip unusable: ints must not
        # become floats/NaN when the list contains nulls.
        import pyarrow as pa

        result = ArrowTableToRowsConversion._to_pylist(
            pa.array([[1, None, 3]], type=pa.list_(pa.int32()))
        )
        self.assertEqual(result, [[1, None, 3]])
        self.assertEqual([type(v) for v in result[0]], [int, type(None), int])

    def test_struct_duplicate_field_names_still_raises(self):
        import pyarrow as pa

        dup = pa.StructArray.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], names=["x", "x"])
        with self.assertRaises(ValueError):
            ArrowTableToRowsConversion._to_pylist(dup)

    def test_struct_rows_are_distinct_dicts(self):
        import pyarrow as pa

        result = ArrowTableToRowsConversion._to_pylist(pa.array([{}, {}], type=pa.struct([])))
        self.assertEqual(result, [{}, {}])
        self.assertIsNot(result[0], result[1])

    def test_convert_table_with_list_columns(self):
        import pyarrow as pa

        schema = (
            StructType()
            .add("arr", ArrayType(IntegerType()))
            .add("nested", ArrayType(ArrayType(StringType())))
        )
        tbl = pa.table(
            {
                "arr": pa.array([[1, None], None, []], type=pa.list_(pa.int32())),
                "nested": pa.array(
                    [[["a"], None], [[]], None], type=pa.list_(pa.list_(pa.string()))
                ),
            }
        )
        actual = ArrowTableToRowsConversion.convert(tbl, schema)
        self.assertEqual(actual[0], Row(arr=[1, None], nested=[["a"], None]))
        self.assertEqual(actual[1], Row(arr=None, nested=[[]]))
        self.assertEqual(actual[2], Row(arr=[], nested=None))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
