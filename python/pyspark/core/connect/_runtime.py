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
Helpers that Spark Connect RDD pipelines run inside Python workers.

A Spark Connect RDD is a DataFrame whose rows encode the RDD elements, plus a pending
per-partition function. Actions and shuffles ship that function to Python workers through
``mapInArrow``. Workers import the module-level functions referenced by those pipelines, so this
module only depends on what every Python worker has: it must not import the Spark Connect client.
"""

import pickle
from typing import Any, Callable, Iterable, Iterator, List, Optional, Tuple

import pyarrow as pa

from pyspark import cloudpickle

CHUNK_COL = "__connect_rdd_chunk"
COUNT_COL = "__connect_rdd_n"
PID_COL = "__connect_rdd_pid"

# How the rows of an RDD's backing DataFrame map to RDD elements.
SOURCE_CHUNKS = "chunks"  # each row holds a pickled list of elements and its length
SOURCE_VALUES = "values"  # each row holds one element in its single column
SOURCE_PAIRS = "pairs"  # each row holds one (first column, second column) element
SOURCE_ROWS = "rows"  # each row is one pyspark.sql.Row element
SOURCE_TUPLES = "tuples"  # each row holds one tuple of its columns

# Size an encoded chunk aims for, the same as AutoBatchedSerializer's default.
CHUNK_TARGET_BYTES = 1 << 16
# Limits on one Arrow batch of encoded chunks emitted by a worker.
_MAX_CHUNKS_PER_BATCH = 256
_MAX_BATCH_BYTES = 16 << 20
# Rows per Arrow batch when converting elements to a typed DataFrame.
_ROWS_PER_BATCH = 10000

_PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL

CHUNK_ARROW_SCHEMA = pa.schema([(CHUNK_COL, pa.large_binary()), (COUNT_COL, pa.int32())])
PID_CHUNK_ARROW_SCHEMA = pa.schema(
    [(CHUNK_COL, pa.large_binary()), (COUNT_COL, pa.int32()), (PID_COL, pa.int32())]
)

PartitionFunc = Callable[[int, Iterator[Any]], Iterable[Any]]


def dumps(obj: Any) -> bytes:
    """
    Pickle ``obj`` with cloudpickle, so that classes defined in ``__main__`` or locally can be
    loaded on the other side. Elements are pickled a chunk at a time, which makes cloudpickle as
    fast as the C pickler here; its overhead is per call, not per object.
    """
    return cloudpickle.dumps(obj, _PICKLE_PROTOCOL)


loads = pickle.loads


def encode_chunks(
    iterator: Iterable[Any], dumps_fn: Callable[[Any], bytes] = dumps
) -> Iterator[Tuple[bytes, int]]:
    """
    Pickle elements into lists, adapting the list length like AutoBatchedSerializer: it starts at
    one element and doubles while a pickled list stays under the target size. Small first
    chunks keep ``take`` cheap; large later chunks amortize the per-row cost.
    """
    batch = 1
    buf: List[Any] = []
    for x in iterator:
        buf.append(x)
        if len(buf) >= batch:
            blob = dumps_fn(buf)
            yield blob, len(buf)
            buf = []
            size = len(blob)
            if size < CHUNK_TARGET_BYTES:
                batch *= 2
            elif size > CHUNK_TARGET_BYTES * 10 and batch > 1:
                batch //= 2
    if buf:
        yield dumps_fn(buf), len(buf)


def chunk_batches(chunks: Iterable[Tuple[bytes, int]]) -> Iterator[pa.RecordBatch]:
    """Group encoded chunks into Arrow batches with the chunk and count columns."""
    blobs: List[bytes] = []
    counts: List[int] = []
    size = 0
    for blob, n in chunks:
        blobs.append(blob)
        counts.append(n)
        size += len(blob)
        if len(blobs) >= _MAX_CHUNKS_PER_BATCH or size >= _MAX_BATCH_BYTES:
            yield _chunk_batch(blobs, counts)
            blobs, counts, size = [], [], 0
    if blobs:
        yield _chunk_batch(blobs, counts)


def _chunk_batch(blobs: List[bytes], counts: List[int]) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(blobs, pa.large_binary()), pa.array(counts, pa.int32())],
        schema=CHUNK_ARROW_SCHEMA,
    )


def pid_chunk_batches(rows: Iterable[Tuple[bytes, int, int]]) -> Iterator[pa.RecordBatch]:
    """Group ``(chunk, count, target partition)`` rows into Arrow batches for a shuffle."""
    blobs: List[bytes] = []
    counts: List[int] = []
    pids: List[int] = []
    size = 0
    for blob, n, pid in rows:
        blobs.append(blob)
        counts.append(n)
        pids.append(pid)
        size += len(blob)
        if len(blobs) >= _MAX_CHUNKS_PER_BATCH or size >= _MAX_BATCH_BYTES:
            yield _pid_chunk_batch(blobs, counts, pids)
            blobs, counts, pids, size = [], [], [], 0
    if blobs:
        yield _pid_chunk_batch(blobs, counts, pids)


def _pid_chunk_batch(blobs: List[bytes], counts: List[int], pids: List[int]) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [
            pa.array(blobs, pa.large_binary()),
            pa.array(counts, pa.int32()),
            pa.array(pids, pa.int32()),
        ],
        schema=PID_CHUNK_ARROW_SCHEMA,
    )


def decode(
    batches: Iterable[pa.RecordBatch],
    source: str,
    row_schema: Any = None,
    binary_as_bytes: bool = True,
) -> Iterator[Any]:
    """Turn the Arrow batches of an RDD's backing DataFrame into RDD elements."""
    if source == SOURCE_CHUNKS:
        for batch in batches:
            for blob in batch.column(0).to_pylist():
                yield from pickle.loads(blob)
    elif source == SOURCE_VALUES:
        for batch in batches:
            yield from batch.column(0).to_pylist()
    elif source == SOURCE_PAIRS:
        for batch in batches:
            yield from zip(batch.column(0).to_pylist(), batch.column(1).to_pylist())
    elif source == SOURCE_TUPLES:
        for batch in batches:
            yield from zip(*(column.to_pylist() for column in batch.columns))
    elif source == SOURCE_ROWS:
        from pyspark.sql.conversion import ArrowTableToRowsConversion

        for batch in batches:
            yield from ArrowTableToRowsConversion.convert(
                pa.Table.from_batches([batch]), row_schema, binary_as_bytes=binary_as_bytes
            )
    else:
        raise ValueError("Unknown Spark Connect RDD source: %s" % source)


def decode_table(table: pa.Table, source: str, row_schema: Any = None) -> List[Any]:
    """Decode a collected Arrow table on the driver."""
    return list(decode(table.to_batches(), source, row_schema))


def make_runner(
    source: str,
    func: Optional[PartitionFunc],
    emit: Callable[[int, Iterator[Any]], Iterator[pa.RecordBatch]],
    row_schema: Any = None,
    binary_as_bytes: bool = True,
    partitions: Optional[frozenset] = None,
) -> Callable[[Iterator[pa.RecordBatch]], Iterator[pa.RecordBatch]]:
    """
    Build the ``mapInArrow`` function of one RDD stage: decode the partition's rows, run the
    pending RDD function on them, and write the result with ``emit``. When ``partitions`` is
    given, other partitions return without running any user code.
    """

    def runner(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        from pyspark.taskcontext import TaskContext

        context = TaskContext.get()
        split = context.partitionId() if context is not None else 0
        if partitions is not None and split not in partitions:
            # Read the input, which the partition filter leaves empty: a worker returning
            # without reading all its input is not reused.
            for _ in batches:
                pass
            return
        elements = decode(batches, source, row_schema, binary_as_bytes)
        if func is not None:
            elements = iter(func(split, elements))
        yield from emit(split, elements)

    return runner


def emit_chunks(split: int, elements: Iterator[Any]) -> Iterator[pa.RecordBatch]:
    return chunk_batches(encode_chunks(elements))


def emit_pid_chunks(split: int, rows: Iterator[Tuple[bytes, int, int]]) -> Iterator[pa.RecordBatch]:
    return pid_chunk_batches(rows)


def emit_count(split: int, elements: Iterator[Any]) -> Iterator[pa.RecordBatch]:
    n = 0
    for _ in elements:
        n += 1
    yield pa.RecordBatch.from_arrays([pa.array([n], pa.int64())], names=["n"])


def mark_partition(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """
    ``mapInArrow`` function emitting one row per partition, to count partitions. It reads its
    input without converting it, since a worker returning without reading all its input is not
    reused.
    """
    for _ in batches:
        pass
    yield pa.RecordBatch.from_arrays([pa.array([1], pa.int32())], names=["p"])


def emit_lines(split: int, lines: Iterator[bytes]) -> Iterator[pa.RecordBatch]:
    buf: List[bytes] = []
    size = 0
    for line in lines:
        buf.append(line)
        size += len(line)
        if len(buf) >= _ROWS_PER_BATCH or size >= _MAX_BATCH_BYTES:
            yield pa.RecordBatch.from_arrays([pa.array(buf, pa.large_binary())], names=["value"])
            buf, size = [], 0
    if buf:
        yield pa.RecordBatch.from_arrays([pa.array(buf, pa.large_binary())], names=["value"])


def emit_hashes(split: int, hashes: Iterator[int]) -> Iterator[pa.RecordBatch]:
    buf: List[int] = []
    for h in hashes:
        buf.append(h)
        if len(buf) >= _ROWS_PER_BATCH:
            yield pa.RecordBatch.from_arrays([pa.array(buf, pa.int64())], names=["h"])
            buf = []
    if buf:
        yield pa.RecordBatch.from_arrays([pa.array(buf, pa.int64())], names=["h"])


def make_dataframe_emitter(
    struct: Any, use_large_var_types: bool
) -> Callable[[int, Iterator[Any]], Iterator[pa.RecordBatch]]:
    """Convert elements to rows of ``struct`` in bounded Arrow batches (``RDD.toDF``)."""

    def emit(split: int, elements: Iterator[Any]) -> Iterator[pa.RecordBatch]:
        from pyspark.errors import PySparkValueError
        from pyspark.sql.conversion import LocalDataToArrowConversion
        from pyspark.sql.types import VariantVal, _create_converter

        converter = _create_converter(struct)
        rows: List[Any] = []
        for elem in elements:
            if isinstance(elem, VariantVal):
                raise PySparkValueError("Rows cannot be of type VariantVal")
            rows.append(converter(elem))
            if len(rows) >= _ROWS_PER_BATCH:
                yield from LocalDataToArrowConversion.convert(
                    rows, struct, use_large_var_types
                ).to_batches()
                rows = []
        if rows:
            yield from LocalDataToArrowConversion.convert(
                rows, struct, use_large_var_types
            ).to_batches()

    return emit
