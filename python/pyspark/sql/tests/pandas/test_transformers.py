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

    from pyspark.sql.pandas.transformers import flatten_struct, wrap_struct


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class TransformerTests(unittest.TestCase):
    def test_flatten_struct_basic(self):
        """Test flattening a struct column into separate columns."""
        struct_array = pa.StructArray.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["x", "y"],
        )
        batch = pa.RecordBatch.from_arrays([struct_array], ["_0"])

        flattened = flatten_struct(batch)

        self.assertEqual(flattened.num_columns, 2)
        self.assertEqual(flattened.column(0).to_pylist(), [1, 2, 3])
        self.assertEqual(flattened.column(1).to_pylist(), ["a", "b", "c"])
        self.assertEqual(flattened.schema.names, ["x", "y"])

    def test_flatten_struct_empty_batch(self):
        """Test flattening an empty batch."""
        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        struct_array = pa.array([], type=struct_type)
        batch = pa.RecordBatch.from_arrays([struct_array], ["_0"])

        flattened = flatten_struct(batch)

        self.assertEqual(flattened.num_rows, 0)
        self.assertEqual(flattened.num_columns, 2)

    def test_wrap_struct_basic(self):
        """Test wrapping columns into a struct."""
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["x", "y"],
        )

        wrapped = wrap_struct(batch)

        self.assertEqual(wrapped.num_columns, 1)
        self.assertEqual(wrapped.schema.names, ["_0"])

        struct_col = wrapped.column(0)
        self.assertEqual(len(struct_col), 3)
        self.assertEqual(struct_col.field(0).to_pylist(), [1, 2, 3])
        self.assertEqual(struct_col.field(1).to_pylist(), ["a", "b", "c"])

    def test_wrap_struct_empty_columns(self):
        """Test wrapping a batch with no columns."""
        schema = pa.schema([])
        batch = pa.RecordBatch.from_arrays([], schema=schema)

        wrapped = wrap_struct(batch)

        self.assertEqual(wrapped.num_columns, 1)
        self.assertEqual(wrapped.num_rows, 0)

    def test_wrap_struct_empty_batch(self):
        """Test wrapping an empty batch with schema."""
        schema = pa.schema([("x", pa.int64()), ("y", pa.string())])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
            schema=schema,
        )

        wrapped = wrap_struct(batch)

        self.assertEqual(wrapped.num_rows, 0)
        self.assertEqual(wrapped.num_columns, 1)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
