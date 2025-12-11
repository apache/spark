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
from typing import Iterator
import pyarrow as pa

import pyspark.sql
from pyspark.sql.types import StructType, StructField, BinaryType
from pyspark.sql.pandas.types import to_arrow_schema


def _get_arrow_array_partition_stream(df: pyspark.sql.DataFrame) -> Iterator[pyarrow.RecordBatch]:
    """Return all the partitions as Arrow arrays in an Iterator."""
    # We will be using mapInArrow to convert each partition to Arrow RecordBatches.
    # The return type of the function will be a single binary column containing
    # the serialized RecordBatch in Arrow IPC format.
    binary_schema = StructType([StructField("arrow_ipc_bytes", BinaryType(), nullable=False)])

    def batch_to_bytes_iter(batch_iter):
        """
        A generator function that converts RecordBatches to serialized Arrow IPC format.

        Spark sends each partition as an iterator of RecordBatches. In order to return
        the entire partition as a stream of Arrow RecordBatches, we need to serialize
        each RecordBatch to Arrow IPC format and yield it as a single binary blob.
        """
        # The size of the batch can be controlled by the Spark config
        # `spark.sql.execution.arrow.maxRecordsPerBatch`.
        for arrow_batch in batch_iter:
            # We create an in-memory byte stream to hold the serialized batch
            sink = pyarrow.BufferOutputStream()
            # Write the batch to the stream using Arrow IPC format
            with pyarrow.ipc.new_stream(sink, arrow_batch.schema) as writer:
                writer.write_batch(arrow_batch)
            buf = sink.getvalue()
            # The second buffer contains the offsets we are manually creating.
            offset_buf = pyarrow.array([0, len(buf)], type=pyarrow.int32()).buffers()[1]
            null_bitmap = None
            # Wrap the bytes in a new 1-row, 1-column RecordBatch to satisfy mapInArrow return
            # signature. This serializes the whole batch into a single pyarrow serialized cell.
            storage_arr = pyarrow.Array.from_buffers(
                type=pyarrow.binary(), length=1, buffers=[null_bitmap, offset_buf, buf]
            )
            yield pyarrow.RecordBatch.from_arrays([storage_arr], names=["arrow_ipc_bytes"])

    # Convert all partitions to Arrow RecordBatches and map to binary blobs.
    byte_df = df.mapInArrow(batch_to_bytes_iter, binary_schema)
    # A row is actually a batch of data in Arrow IPC format. Fetch the batches one by one.
    for row in byte_df.toLocalIterator():
        with pyarrow.ipc.open_stream(row.arrow_ipc_bytes) as reader:
            for batch in reader:
                # Each batch corresponds to a chunk of data in the partition.
                yield batch


class SparkArrowCStreamer:
    """
    A class that implements that __arrow_c_stream__ protocol for Spark partitions.

    This class is implemented in a way that allows consumers to consume each partition
    one at a time without materializing all partitions at once on the driver side.
    """

    def __init__(self, df: pyspark.sql.DataFrame):
        self._df = df
        self._schema = to_arrow_schema(df.schema)

    def __arrow_c_stream__(self, requested_schema=None):
        """
        Return the Arrow C stream for the dataframe partitions.
        """
        reader: pyarrow.RecordBatchReader = pyarrow.RecordBatchReader.from_batches(
            self._schema, _get_arrow_array_partition_stream(self._df)
        )
        return reader.__arrow_c_stream__(requested_schema=requested_schema)
