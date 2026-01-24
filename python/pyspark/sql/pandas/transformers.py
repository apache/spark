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

These are pure callable classes that transform Iterator[RecordBatch] -> Iterator[...].
They should have no side effects (no I/O, no writing to streams).
"""

from typing import TYPE_CHECKING, Iterator, Tuple

if TYPE_CHECKING:
    import pyarrow as pa


class FlattenStructTransformer:
    """
    Flatten a single struct column into a RecordBatch.

    Input: Iterator of RecordBatch with a single struct column
    Output: Iterator of RecordBatch (flattened)
    """

    def __call__(self, batches: Iterator["pa.RecordBatch"]) -> Iterator["pa.RecordBatch"]:
        import pyarrow as pa

        for batch in batches:
            struct = batch.column(0)
            yield pa.RecordBatch.from_arrays(struct.flatten(), schema=pa.schema(struct.type))


class WrapStructTransformer:
    """
    Wrap a RecordBatch's columns into a single struct column.

    Input: Iterator of (RecordBatch, arrow_type)
    Output: Iterator of RecordBatch with a single struct column
    """

    def __call__(
        self, iterator: Iterator[Tuple["pa.RecordBatch", "pa.DataType"]]
    ) -> Iterator["pa.RecordBatch"]:
        import pyarrow as pa

        for batch, _ in iterator:
            if batch.num_columns == 0:
                # When batch has no column, it should still create
                # an empty batch with the number of rows set.
                struct = pa.array([{}] * batch.num_rows)
            else:
                struct = pa.StructArray.from_arrays(
                    batch.columns, fields=pa.struct(list(batch.schema))
                )
            yield pa.RecordBatch.from_arrays([struct], ["_0"])
