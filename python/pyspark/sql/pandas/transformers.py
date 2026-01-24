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

"""
Arrow batch transformers for building data processing pipelines.

These are pure functions that transform RecordBatch -> RecordBatch.
They should have no side effects (no I/O, no writing to streams).
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


def flatten_struct(batch: "pa.RecordBatch") -> "pa.RecordBatch":
    """
    Flatten a single struct column into a RecordBatch.

    Used by: ArrowStreamUDFSerializer.load_stream
    """
    import pyarrow as pa

    struct = batch.column(0)
    return pa.RecordBatch.from_arrays(struct.flatten(), schema=pa.schema(struct.type))


def wrap_struct(batch: "pa.RecordBatch") -> "pa.RecordBatch":
    """
    Wrap a RecordBatch's columns into a single struct column.

    Used by: ArrowStreamUDFSerializer.dump_stream
    """
    import pyarrow as pa

    if batch.num_columns == 0:
        # When batch has no column, it should still create
        # an empty batch with the number of rows set.
        struct = pa.array([{}] * batch.num_rows)
    else:
        struct = pa.StructArray.from_arrays(batch.columns, fields=pa.struct(list(batch.schema)))
    return pa.RecordBatch.from_arrays([struct], ["_0"])
