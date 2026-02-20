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
Base Arrow serializer classes and utilities.
"""

from typing import TYPE_CHECKING, Iterator

from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.serializers import (
    Serializer,
    SpecialLengths,
    read_int,
    write_int,
    UTF8Deserializer,
)
from pyspark.sql.types import DataType

if TYPE_CHECKING:
    import pyarrow as pa


def _normalize_packed(packed):
    """
    Normalize UDF output to a uniform tuple-of-tuples form.

    Iterator UDFs yield a single (series, spark_type) tuple directly,
    while batched UDFs return a tuple of tuples ((s1, t1), (s2, t2), ...).
    This function normalizes both forms to a tuple of tuples.
    """
    if len(packed) == 2 and isinstance(packed[1], DataType):
        return (packed,)
    return tuple(packed)


class ArrowCollectSerializer(Serializer):
    """
    Deserialize a stream of batches followed by batch order information. Used in
    PandasConversionMixin._collect_as_arrow() after invoking Dataset.collectAsArrowToPython()
    in the JVM.
    """

    def __init__(self):
        self.serializer = ArrowStreamSerializer()

    def dump_stream(self, iterator, stream):
        return self.serializer.dump_stream(iterator, stream)

    def load_stream(self, stream):
        """
        Load a stream of un-ordered Arrow RecordBatches, where the last iteration yields
        a list of indices that can be used to put the RecordBatches in the correct order.
        """
        # load the batches
        for batch in self.serializer.load_stream(stream):
            yield batch

        # load the batch order indices or propagate any error that occurred in the JVM
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
        batch_order = []
        for i in range(num):
            index = read_int(stream)
            batch_order.append(index)
        yield batch_order

    def __repr__(self):
        return "ArrowCollectSerializer(%s)" % self.serializer


class ArrowStreamSerializer(Serializer):
    """
    Serializes Arrow record batches as a stream.

    Parameters
    ----------
    write_start_stream : bool
        If True, writes the START_ARROW_STREAM marker before the first
        output batch. Default False.
    num_dfs : int
        Number of dataframes per group.
        For num_dfs=0, plain batch stream without group-count protocol.
        For num_dfs=1, grouped loading (1 dataframe per group).
        For num_dfs=2, cogrouped loading (2 dataframes per group).
        Default 0.
    """

    def __init__(self, write_start_stream: bool = False, num_dfs: int = 0) -> None:
        super().__init__()
        assert num_dfs in (0, 1, 2), "num_dfs must be 0, 1, or 2"
        self._write_start_stream: bool = write_start_stream
        self._num_dfs: int = num_dfs

    def dump_stream(self, iterator, stream):
        """Optionally prepend START_ARROW_STREAM, then write batches."""
        if self._write_start_stream:
            iterator = self._write_stream_start(iterator, stream)
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()

    def load_stream(self, stream):
        """Load batches: plain stream if num_dfs=0, grouped otherwise."""
        if self._num_dfs == 0:
            import pyarrow as pa

            reader = pa.ipc.open_stream(stream)
            for batch in reader:
                yield batch
        elif self._num_dfs == 1:
            # Grouped loading: yield single dataframe groups
            for (batches,) in self._load_group_dataframes(stream, num_dfs=1):
                yield batches
        elif self._num_dfs == 2:
            # Cogrouped loading: yield tuples of (left_batches, right_batches)
            for left_batches, right_batches in self._load_group_dataframes(stream, num_dfs=2):
                yield left_batches, right_batches
        else:
            assert False, f"Unexpected num_dfs: {self._num_dfs}"

    def _load_single_group_stream(self, stream):
        """
        Template method for grouped loading with one dataframe per group.

        Yields the result of ``_transform_group_batches`` for each group and
        exhausts the underlying batch iterator before advancing to the next
        group (required to keep the stream position correct).

        Subclasses override ``_transform_group_batches`` to customize the
        per-group transformation.
        """
        for (batches,) in self._load_group_dataframes(stream, num_dfs=1):
            result = self._transform_group_batches(batches)
            yield result
            # Exhaust the iterator to advance the stream past this group
            for _ in result:
                pass

    def _transform_group_batches(self, batches):
        """
        Hook for ``_load_single_group_stream``.  Default: pass through raw
        batches unchanged.
        """
        return batches

    def _load_cogroup_stream(self, stream):
        """
        Template method for cogrouped loading with two dataframes per group.

        Subclasses override ``_transform_cogroup_batches`` to customize the
        per-group transformation.  No exhaustion loop is needed here because
        the cogroup path in ``_load_group_dataframes`` eagerly materializes
        each side via ``list()``.
        """
        for left_batches, right_batches in self._load_group_dataframes(stream, num_dfs=2):
            yield self._transform_cogroup_batches(left_batches, right_batches)

    def _transform_cogroup_batches(self, left_batches, right_batches):
        """
        Hook for ``_load_cogroup_stream``.  Default: pass through as tuple.
        """
        return left_batches, right_batches

    def _load_group_dataframes(self, stream, num_dfs: int = 1) -> Iterator:
        """
        Yield groups of dataframes from the stream using the group-count protocol.
        """
        dataframes_in_group = None

        while dataframes_in_group is None or dataframes_in_group > 0:
            dataframes_in_group = read_int(stream)

            if dataframes_in_group == num_dfs:
                if num_dfs == 1:
                    # Single dataframe: can use lazy iterator
                    yield (ArrowStreamSerializer.load_stream(self, stream),)
                else:
                    # Multiple dataframes: must eagerly load sequentially
                    # to maintain correct stream position
                    yield tuple(
                        list(ArrowStreamSerializer.load_stream(self, stream))
                        for _ in range(num_dfs)
                    )
            elif dataframes_in_group > 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )

    def _write_stream_start(
        self, batch_iterator: Iterator["pa.RecordBatch"], stream
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

    def __repr__(self):
        return "ArrowStreamSerializer(write_start_stream=%s, num_dfs=%d)" % (
            self._write_start_stream,
            self._num_dfs,
        )
