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
Serializers for PyArrow and pandas conversions. See `pyspark.serializers` for more details.
"""

from typing import IO, TYPE_CHECKING, Iterable, Iterator, List, Tuple

from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.serializers import (
    Serializer,
    UTF8Deserializer,
    read_int,
    write_int,
)

if TYPE_CHECKING:
    import pyarrow as pa


class SpecialLengths:
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6


class ArrowStreamSerializer(Serializer):
    """
    Serializes Arrow record batches as a plain stream.

    Parameters
    ----------
    write_start_stream : bool
        If True, writes the START_ARROW_STREAM marker before the first
        output batch. Default False.
    """

    def __init__(self, write_start_stream: bool = False, flush_per_batch: bool = False) -> None:
        super().__init__()
        self._write_start_stream: bool = write_start_stream
        self._flush_per_batch: bool = flush_per_batch

    def dump_stream(self, iterator: Iterable["pa.RecordBatch"], stream: IO[bytes]) -> None:
        """Optionally prepend START_ARROW_STREAM, then write batches."""
        iterator = iter(iterator)
        if self._write_start_stream:
            iterator = self._write_stream_start(iterator, stream)
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
                # In pipelined mode, flush after each batch so the JVM can read output
                # while still sending input, rather than buffering all output.
                if self._flush_per_batch:
                    stream.flush()
        finally:
            if writer is not None:
                writer.close()

    def load_stream(self, stream: IO[bytes]) -> Iterator["pa.RecordBatch"]:
        """Load batches from a plain Arrow stream."""
        import pyarrow as pa

        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield batch

    def _write_stream_start(
        self, batch_iterator: Iterator["pa.RecordBatch"], stream: IO[bytes]
    ) -> Iterator["pa.RecordBatch"]:
        """Write START_ARROW_STREAM before the first batch, then pass batches through."""
        import itertools

        first = next(batch_iterator, None)
        if first is None:
            return

        # Signal the JVM after the first batch succeeds, so errors during
        # batch creation can be reported before the Arrow stream starts.
        write_int(SpecialLengths.START_ARROW_STREAM, stream)
        yield from itertools.chain([first], batch_iterator)

    def __repr__(self) -> str:
        return "ArrowStreamSerializer(write_start_stream=%s)" % self._write_start_stream


class ArrowCollectSerializer(ArrowStreamSerializer):
    """
    Extends :class:`ArrowStreamSerializer` to load Arrow RecordBatches that the JVM
    sends out of order, followed by the indices giving their correct order, and yields
    the batches already reordered. Used in PandasConversionMixin._collect_as_arrow()
    after invoking Dataset.collectAsArrowToPython() in the JVM.
    """

    def load_stream(self, stream: IO[bytes]) -> Iterator["pa.RecordBatch"]:
        """Load the out-of-order batches, then yield them in the correct order."""
        batches = list(super().load_stream(stream))

        # Load the batch order indices, or propagate any error that occurred in the JVM.
        num = read_int(stream)
        if num == -1:
            error_msg = UTF8Deserializer().loads(stream)
            raise PySparkRuntimeError(
                errorClass="ERROR_OCCURRED_WHILE_CALLING",
                messageParameters={
                    "func_name": "ArrowCollectSerializer.load_stream",
                    "error_msg": error_msg,
                },
            )
        # Yield the batches in order, dropping our reference to each as it goes so
        # that, when selfDestruct is enabled, the caller's reallocated copy is the
        # only remaining reference and the original batch can be freed immediately
        # rather than staying pinned here until the stream is fully consumed. The
        # indices are a permutation, so each batch is yielded exactly once.
        for _ in range(num):
            i = read_int(stream)
            batch = batches[i]
            batches[i] = None
            yield batch

    def __repr__(self) -> str:
        return "ArrowCollectSerializer()"


class ArrowStreamGroupSerializer(ArrowStreamSerializer):
    """
    Extends :class:`ArrowStreamSerializer` with group-count protocol for loading
    grouped Arrow record batches (1 dataframe per group).
    """

    def load_stream(self, stream: IO[bytes]) -> Iterator[Iterator["pa.RecordBatch"]]:
        """Yield one iterator of record batches per group from the stream."""
        while dataframes_in_group := read_int(stream):
            if dataframes_in_group == 1:
                yield ArrowStreamSerializer.load_stream(self, stream)
            elif dataframes_in_group > 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )


class ArrowStreamCoGroupSerializer(ArrowStreamSerializer):
    """
    Extends :class:`ArrowStreamSerializer` with group-count protocol for loading
    cogrouped Arrow record batches (2 dataframes per group).
    """

    def load_stream(
        self, stream: IO[bytes]
    ) -> Iterator[Tuple[List["pa.RecordBatch"], List["pa.RecordBatch"]]]:
        """Yield pairs of (left_batches, right_batches) from the stream."""
        while dataframes_in_group := read_int(stream):
            if dataframes_in_group == 2:
                # Must eagerly load each dataframe to maintain correct stream position
                yield (
                    list(ArrowStreamSerializer.load_stream(self, stream)),
                    list(ArrowStreamSerializer.load_stream(self, stream)),
                )
            elif dataframes_in_group > 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )
