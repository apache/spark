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
    ArrowTimestampConversion,
    ArrowBatchTransformer,
    PandasBatchTransformer,
    PandasSeriesToArrowConversion,
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

    def test_concat_batches_basic(self):
        """Test concatenating multiple batches vertically."""
        import pyarrow as pa

        batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2])], ["x"])
        batch2 = pa.RecordBatch.from_arrays([pa.array([3, 4])], ["x"])

        result = ArrowBatchTransformer.concat_batches([batch1, batch2])

        self.assertEqual(result.num_rows, 4)
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3, 4])

    def test_concat_batches_single(self):
        """Test concatenating a single batch returns the same batch."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["x"])

        result = ArrowBatchTransformer.concat_batches([batch])

        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])

    def test_concat_batches_empty_batches(self):
        """Test concatenating empty batches."""
        import pyarrow as pa

        schema = pa.schema([("x", pa.int64())])
        batch1 = pa.RecordBatch.from_arrays([pa.array([], type=pa.int64())], schema=schema)
        batch2 = pa.RecordBatch.from_arrays([pa.array([1, 2])], ["x"])

        result = ArrowBatchTransformer.concat_batches([batch1, batch2])

        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.column(0).to_pylist(), [1, 2])

    def test_zip_batches_record_batches(self):
        """Test zipping multiple RecordBatches horizontally."""
        import pyarrow as pa

        batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2])], ["a"])
        batch2 = pa.RecordBatch.from_arrays([pa.array(["x", "y"])], ["b"])

        result = ArrowBatchTransformer.zip_batches([batch1, batch2])

        self.assertEqual(result.num_columns, 2)
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.column(0).to_pylist(), [1, 2])
        self.assertEqual(result.column(1).to_pylist(), ["x", "y"])

    def test_zip_batches_arrays(self):
        """Test zipping Arrow arrays directly."""
        import pyarrow as pa

        arr1 = pa.array([1, 2, 3])
        arr2 = pa.array(["a", "b", "c"])

        result = ArrowBatchTransformer.zip_batches([arr1, arr2])

        self.assertEqual(result.num_columns, 2)
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])
        self.assertEqual(result.column(1).to_pylist(), ["a", "b", "c"])

    def test_zip_batches_with_type_casting(self):
        """Test zipping with type casting."""
        import pyarrow as pa

        arr = pa.array([1, 2, 3], type=pa.int32())
        result = ArrowBatchTransformer.zip_batches([(arr, pa.int64())])

        self.assertEqual(result.column(0).type, pa.int64())
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])

    def test_zip_batches_empty_raises(self):
        """Test that zipping empty list raises error."""
        with self.assertRaises(PySparkValueError):
            ArrowBatchTransformer.zip_batches([])

    def test_zip_batches_single_batch(self):
        """Test zipping single batch returns it unchanged."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], ["x"])
        result = ArrowBatchTransformer.zip_batches([batch])

        self.assertIs(result, batch)

    def test_to_pandas_basic(self):
        """Test basic Arrow to pandas conversion."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["x", "y"],
        )

        result = ArrowBatchTransformer.to_pandas(batch, timezone="UTC")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].tolist(), [1, 2, 3])
        self.assertEqual(result[1].tolist(), ["a", "b", "c"])

    def test_to_pandas_empty_table(self):
        """Test converting empty table returns NoValue series."""
        import pyarrow as pa

        table = pa.Table.from_arrays([], names=[])

        result = ArrowBatchTransformer.to_pandas(table, timezone="UTC")

        # Empty table with rows should return a series with _NoValue
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 0)

    def test_to_pandas_with_nulls(self):
        """Test conversion with null values."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, None, 3]), pa.array([None, "b", None])],
            names=["x", "y"],
        )

        result = ArrowBatchTransformer.to_pandas(batch, timezone="UTC")

        self.assertEqual(len(result), 2)
        self.assertTrue(result[0].isna().tolist() == [False, True, False])
        self.assertTrue(result[1].isna().tolist() == [True, False, True])

    def test_to_pandas_with_schema_and_udt(self):
        """Test conversion with Spark schema containing UDT."""
        import pyarrow as pa

        # Create Arrow batch with raw UDT data (list representation)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([[1.0, 2.0], [3.0, 4.0]], type=pa.list_(pa.float64()))],
            names=["point"],
        )

        schema = StructType([StructField("point", ExamplePointUDT())])

        result = ArrowBatchTransformer.to_pandas(batch, timezone="UTC", schema=schema)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], ExamplePoint(1.0, 2.0))
        self.assertEqual(result[0][1], ExamplePoint(3.0, 4.0))

    def test_flatten_and_wrap_roundtrip(self):
        """Test that flatten -> wrap produces equivalent result."""
        import pyarrow as pa

        struct_array = pa.StructArray.from_arrays(
            [pa.array([1, 2]), pa.array(["a", "b"])],
            names=["x", "y"],
        )
        original = pa.RecordBatch.from_arrays([struct_array], ["_0"])

        flattened = ArrowBatchTransformer.flatten_struct(original)
        rewrapped = ArrowBatchTransformer.wrap_struct(flattened)

        self.assertEqual(rewrapped.num_columns, 1)
        self.assertEqual(rewrapped.column(0).to_pylist(), original.column(0).to_pylist())

    def test_enforce_schema_reorder(self):
        """Test reordering columns to match target schema."""
        import pyarrow as pa
        from pyspark.sql.types import LongType

        # Batch with columns in different order than target
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2]), pa.array(["a", "b"])],
            names=["b", "a"],
        )
        target = StructType(
            [
                StructField("a", StringType()),
                StructField("b", LongType()),
            ]
        )

        result = ArrowBatchTransformer.enforce_schema(batch, target)

        self.assertEqual(result.schema.names, ["a", "b"])
        self.assertEqual(result.column(0).to_pylist(), ["a", "b"])
        self.assertEqual(result.column(1).to_pylist(), [1, 2])

    def test_enforce_schema_coerce_types(self):
        """Test coercing column types to match target schema."""
        import pyarrow as pa
        from pyspark.sql.types import LongType

        # Batch with int32 that should be coerced to int64 (LongType)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2], type=pa.int32())],
            names=["x"],
        )
        target = StructType([StructField("x", LongType())])

        result = ArrowBatchTransformer.enforce_schema(batch, target)

        self.assertEqual(result.column(0).type, pa.int64())
        self.assertEqual(result.column(0).to_pylist(), [1, 2])

    def test_enforce_schema_reorder_and_coerce(self):
        """Test both reordering and type coercion together."""
        import pyarrow as pa
        from pyspark.sql.types import LongType

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2], type=pa.int32()), pa.array(["x", "y"])],
            names=["b", "a"],
        )
        target = StructType(
            [
                StructField("a", StringType()),
                StructField("b", LongType()),
            ]
        )

        result = ArrowBatchTransformer.enforce_schema(batch, target)

        self.assertEqual(result.schema.names, ["a", "b"])
        self.assertEqual(result.column(0).to_pylist(), ["x", "y"])
        self.assertEqual(result.column(1).type, pa.int64())
        self.assertEqual(result.column(1).to_pylist(), [1, 2])

    def test_enforce_schema_empty_batch(self):
        """Test that empty batch is returned as-is."""
        import pyarrow as pa
        from pyspark.sql.types import LongType

        batch = pa.RecordBatch.from_arrays([], names=[])
        target = StructType([StructField("x", LongType())])

        result = ArrowBatchTransformer.enforce_schema(batch, target)

        self.assertEqual(result.num_columns, 0)

    def test_enforce_schema_with_large_var_types(self):
        """Test using prefer_large_var_types option."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2]), pa.array(["a", "b"])],
            names=["b", "a"],
        )
        target = StructType(
            [
                StructField("a", StringType()),
                StructField("b", IntegerType()),
            ]
        )

        result = ArrowBatchTransformer.enforce_schema(batch, target, prefer_large_var_types=True)

        self.assertEqual(result.schema.names, ["a", "b"])
        # With prefer_large_var_types=True, string should be large_string
        self.assertEqual(result.column(0).type, pa.large_string())
        self.assertEqual(result.column(0).to_pylist(), ["a", "b"])
        self.assertEqual(result.column(1).to_pylist(), [1, 2])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PandasBatchTransformerTests(unittest.TestCase):
    def test_to_arrow_single_series(self):
        """Test converting a single pandas Series to Arrow."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([1, 2, 3])
        result = PandasBatchTransformer.to_arrow(
            (series, pa.int64(), IntegerType()), timezone="UTC"
        )

        self.assertEqual(result.num_columns, 1)
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])

    def test_to_arrow_empty_series(self):
        """Test converting empty pandas Series."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([], dtype="int64")
        result = PandasBatchTransformer.to_arrow(
            (series, pa.int64(), IntegerType()), timezone="UTC"
        )

        self.assertEqual(result.num_rows, 0)

    def test_to_arrow_with_nulls(self):
        """Test converting Series with null values."""
        import pandas as pd
        import pyarrow as pa
        import numpy as np
        from pyspark.sql.types import DoubleType

        series = pd.Series([1.0, np.nan, 3.0])
        result = PandasBatchTransformer.to_arrow(
            (series, pa.float64(), DoubleType()), timezone="UTC"
        )

        self.assertEqual(result.column(0).to_pylist(), [1.0, None, 3.0])

    def test_to_arrow_multiple_series(self):
        """Test converting multiple series at once."""
        import pandas as pd
        import pyarrow as pa

        series1 = pd.Series([1, 2])
        series2 = pd.Series(["a", "b"])

        result = PandasBatchTransformer.to_arrow(
            [(series1, pa.int64(), None), (series2, pa.string(), None)], timezone="UTC"
        )

        self.assertEqual(result.num_columns, 2)
        self.assertEqual(result.column(0).to_pylist(), [1, 2])
        self.assertEqual(result.column(1).to_pylist(), ["a", "b"])

    def test_to_arrow_struct_mode(self):
        """Test converting DataFrame to struct array."""
        import pandas as pd
        import pyarrow as pa

        df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        arrow_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        spark_type = StructType([StructField("x", IntegerType()), StructField("y", StringType())])

        result = PandasBatchTransformer.to_arrow(
            (df, arrow_type, spark_type), timezone="UTC", as_struct=True
        )

        self.assertEqual(result.num_columns, 1)
        struct_col = result.column(0)
        self.assertEqual(struct_col.field(0).to_pylist(), [1, 2])
        self.assertEqual(struct_col.field(1).to_pylist(), ["a", "b"])

    def test_to_arrow_struct_empty_dataframe(self):
        """Test converting empty DataFrame in struct mode."""
        import pandas as pd
        import pyarrow as pa

        df = pd.DataFrame({"x": pd.Series([], dtype="int64"), "y": pd.Series([], dtype="str")})
        arrow_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        spark_type = StructType([StructField("x", IntegerType()), StructField("y", StringType())])

        result = PandasBatchTransformer.to_arrow(
            (df, arrow_type, spark_type), timezone="UTC", as_struct=True
        )

        self.assertEqual(result.num_rows, 0)

    def test_to_arrow_categorical_series(self):
        """Test converting categorical pandas Series."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Categorical(["a", "b", "a", "c"])
        result = PandasBatchTransformer.to_arrow(
            (pd.Series(series), pa.string(), StringType()), timezone="UTC"
        )

        self.assertEqual(result.column(0).to_pylist(), ["a", "b", "a", "c"])

    def test_to_arrow_requires_dataframe_for_struct(self):
        """Test that struct mode requires DataFrame input."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([1, 2])
        arrow_type = pa.struct([("x", pa.int64())])

        with self.assertRaises(PySparkValueError):
            PandasBatchTransformer.to_arrow(
                (series, arrow_type, None), timezone="UTC", as_struct=True
            )


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PandasSeriesToArrowConversionTests(unittest.TestCase):
    def test_create_array_basic(self):
        """Test basic array creation from pandas Series."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([1, 2, 3])
        result = PandasSeriesToArrowConversion.create_array(
            series, pa.int64(), "UTC", spark_type=IntegerType()
        )

        self.assertEqual(result.to_pylist(), [1, 2, 3])

    def test_create_array_empty(self):
        """Test creating array from empty Series."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([], dtype="int64")
        result = PandasSeriesToArrowConversion.create_array(
            series, pa.int64(), "UTC", spark_type=IntegerType()
        )

        self.assertEqual(len(result), 0)

    def test_create_array_with_nulls(self):
        """Test creating array with null values."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([1, None, 3], dtype="Int64")  # nullable integer
        result = PandasSeriesToArrowConversion.create_array(
            series, pa.int64(), "UTC", spark_type=IntegerType()
        )

        self.assertEqual(result.to_pylist(), [1, None, 3])

    def test_create_array_categorical(self):
        """Test creating array from categorical Series."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series(pd.Categorical(["x", "y", "x"]))
        result = PandasSeriesToArrowConversion.create_array(
            series, pa.string(), "UTC", spark_type=StringType()
        )

        self.assertEqual(result.to_pylist(), ["x", "y", "x"])

    def test_create_array_with_udt(self):
        """Test creating array with UDT."""
        import pandas as pd
        import pyarrow as pa

        points = [ExamplePoint(1.0, 2.0), ExamplePoint(3.0, 4.0)]
        series = pd.Series(points)

        result = PandasSeriesToArrowConversion.create_array(
            series, pa.list_(pa.float64()), "UTC", spark_type=ExamplePointUDT()
        )

        self.assertEqual(result.to_pylist(), [[1.0, 2.0], [3.0, 4.0]])

    def test_create_array_binary(self):
        """Test creating array with binary data."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([b"hello", b"world"])
        result = PandasSeriesToArrowConversion.create_array(
            series, pa.binary(), "UTC", spark_type=BinaryType()
        )

        self.assertEqual(result.to_pylist(), [b"hello", b"world"])

    def test_create_array_all_nulls(self):
        """Test creating array with all null values."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([None, None, None])
        result = PandasSeriesToArrowConversion.create_array(
            series, pa.int64(), "UTC", spark_type=IntegerType()
        )

        self.assertEqual(result.to_pylist(), [None, None, None])

    def test_create_array_nested_list(self):
        """Test creating array with nested list type."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([[1, 2], [3, 4, 5], []])
        result = PandasSeriesToArrowConversion.create_array(
            series, pa.list_(pa.int64()), "UTC", spark_type=ArrayType(IntegerType())
        )

        self.assertEqual(result.to_pylist(), [[1, 2], [3, 4, 5], []])

    def test_create_array_map_type(self):
        """Test creating array with map type."""
        import pandas as pd
        import pyarrow as pa

        series = pd.Series([{"a": 1}, {"b": 2, "c": 3}])
        result = PandasSeriesToArrowConversion.create_array(
            series,
            pa.map_(pa.string(), pa.int64()),
            "UTC",
            spark_type=MapType(StringType(), IntegerType()),
        )

        # Maps are returned as list of tuples
        self.assertEqual(result.to_pylist(), [[("a", 1)], [("b", 2), ("c", 3)]])


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
            output = ArrowTimestampConversion.localize_tz(arr)
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
            output = ArrowTimestampConversion.localize_tz(arr)
            self.assertEqual(output, expected, f"{output.tolist()} != {expected.tolist()}")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
