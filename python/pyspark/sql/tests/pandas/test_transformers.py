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
from pyspark.testing.sqlutils import (
    have_pyarrow,
    pyarrow_requirement_message,
)

if have_pyarrow:
    import pyarrow as pa

    from pyspark.sql.pandas.transformers import FlattenStructTransformer, WrapStructTransformer


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class TransformerTests(unittest.TestCase):
    def test_flatten_struct_transformer_basic(self):
        """Test flattening a struct column into separate columns."""
        # Create a batch with a single struct column containing two fields
        struct_array = pa.StructArray.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["x", "y"],
        )
        batch = pa.RecordBatch.from_arrays([struct_array], ["_0"])

        transformer = FlattenStructTransformer()
        result = list(transformer(iter([batch])))

        self.assertEqual(len(result), 1)
        flattened = result[0]
        self.assertEqual(flattened.num_columns, 2)
        self.assertEqual(flattened.column(0).to_pylist(), [1, 2, 3])
        self.assertEqual(flattened.column(1).to_pylist(), ["a", "b", "c"])
        self.assertEqual(flattened.schema.names, ["x", "y"])

    def test_flatten_struct_transformer_multiple_batches(self):
        """Test flattening multiple batches."""
        batches = []
        for i in range(3):
            struct_array = pa.StructArray.from_arrays(
                [pa.array([i * 10 + j for j in range(2)])],
                names=["val"],
            )
            batches.append(pa.RecordBatch.from_arrays([struct_array], ["_0"]))

        transformer = FlattenStructTransformer()
        result = list(transformer(iter(batches)))

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].column(0).to_pylist(), [0, 1])
        self.assertEqual(result[1].column(0).to_pylist(), [10, 11])
        self.assertEqual(result[2].column(0).to_pylist(), [20, 21])

    def test_flatten_struct_transformer_empty_batch(self):
        """Test flattening an empty batch."""
        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        struct_array = pa.array([], type=struct_type)
        batch = pa.RecordBatch.from_arrays([struct_array], ["_0"])

        transformer = FlattenStructTransformer()
        result = list(transformer(iter([batch])))

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].num_rows, 0)
        self.assertEqual(result[0].num_columns, 2)

    def test_wrap_struct_transformer_basic(self):
        """Test wrapping columns into a struct."""
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["x", "y"],
        )
        arrow_type = pa.struct([("x", pa.int64()), ("y", pa.string())])

        transformer = WrapStructTransformer()
        result = list(transformer(iter([(batch, arrow_type)])))

        self.assertEqual(len(result), 1)
        wrapped = result[0]
        self.assertEqual(wrapped.num_columns, 1)
        self.assertEqual(wrapped.schema.names, ["_0"])

        # Verify the struct content
        struct_col = wrapped.column(0)
        self.assertEqual(len(struct_col), 3)
        # Access struct fields
        self.assertEqual(struct_col.field(0).to_pylist(), [1, 2, 3])
        self.assertEqual(struct_col.field(1).to_pylist(), ["a", "b", "c"])

    def test_wrap_struct_transformer_multiple_batches(self):
        """Test wrapping multiple batches."""
        batches_with_types = []
        arrow_type = pa.struct([("val", pa.int64())])
        for i in range(3):
            batch = pa.RecordBatch.from_arrays(
                [pa.array([i * 10 + j for j in range(2)])],
                names=["val"],
            )
            batches_with_types.append((batch, arrow_type))

        transformer = WrapStructTransformer()
        result = list(transformer(iter(batches_with_types)))

        self.assertEqual(len(result), 3)
        for i, wrapped in enumerate(result):
            self.assertEqual(wrapped.num_columns, 1)
            struct_col = wrapped.column(0)
            self.assertEqual(struct_col.field(0).to_pylist(), [i * 10, i * 10 + 1])

    def test_wrap_struct_transformer_empty_columns(self):
        """Test wrapping a batch with no columns."""
        # Create an empty schema batch with some rows
        batch = pa.RecordBatch.from_arrays([], names=[])
        # Manually set num_rows by creating from pydict
        batch = pa.RecordBatch.from_pydict({}, schema=pa.schema([]))
        # Create batch with rows using a workaround
        batch = pa.record_batch({"dummy": [1, 2, 3]}).select([]).slice(0, 3)
        # Actually, let's create it properly
        schema = pa.schema([])
        batch = pa.RecordBatch.from_arrays([], schema=schema)

        arrow_type = pa.struct([])

        transformer = WrapStructTransformer()
        result = list(transformer(iter([(batch, arrow_type)])))

        self.assertEqual(len(result), 1)
        wrapped = result[0]
        self.assertEqual(wrapped.num_columns, 1)
        # Empty struct batch has 0 rows
        self.assertEqual(wrapped.num_rows, 0)

    def test_wrap_struct_transformer_empty_batch(self):
        """Test wrapping an empty batch with schema."""
        schema = pa.schema([("x", pa.int64()), ("y", pa.string())])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
            schema=schema,
        )
        arrow_type = pa.struct([("x", pa.int64()), ("y", pa.string())])

        transformer = WrapStructTransformer()
        result = list(transformer(iter([(batch, arrow_type)])))

        self.assertEqual(len(result), 1)
        wrapped = result[0]
        self.assertEqual(wrapped.num_rows, 0)
        self.assertEqual(wrapped.num_columns, 1)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
