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
RDD API on Spark Connect.

An RDD here is a DataFrame whose rows encode its elements (see ``_runtime``) plus a pending
function ``func(split, iterator)`` from narrow transformations. Narrow transformations only
compose that function, so a chain of them runs as one ``mapInArrow`` pass per stage, the way
``PipelinedRDD`` does in Spark Classic. Shuffles bucket elements in Python and move them with
``DataFrame.repartitionById``; aggregations, sorting, and joins use the same Python algorithms as
Spark Classic on top of that.

``mapInArrow`` reports the Spark task's partition id, which is the RDD partition index unless a
union, coalesce, or cartesian product places the pipeline in a differently partitioned stage.
Pipelines that read the index (``mapPartitionsWithIndex``, sampling, ...) are tracked so those
operations can correct the index or materialize such a pipeline in a stage of its own first.
"""

import bisect
import copy
import heapq
import itertools
import math
import operator
import random
import shlex
import sys
import threading
import uuid
import warnings
from collections import defaultdict
from functools import reduce
from io import BytesIO
from itertools import chain
from subprocess import PIPE, Popen
from threading import Thread
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from pyspark.core.connect import _runtime as rt
from pyspark.core.rdd import RDD as RDDFacade
from pyspark.core.rdd import BoundedFloat, Partitioner
from pyspark.core.rdd import RDDBarrier as RDDBarrierFacade
from pyspark.errors import PySparkNotImplementedError, PySparkRuntimeError
from pyspark.join import (
    python_cogroup,
    python_full_outer_join,
    python_join,
    python_left_outer_join,
    python_right_outer_join,
)
from pyspark.rddsampler import RDDRangeSampler, RDDSampler, RDDStratifiedSampler
from pyspark.resource.profile import ResourceProfile
from pyspark.resultiterable import ResultIterable
from pyspark.serializers import (
    AutoBatchedSerializer,
    BatchedSerializer,
    CloudPickleSerializer,
    CPickleSerializer,
    Serializer,
)
from pyspark.shuffle import (
    Aggregator,
    ExternalGroupBy,
    ExternalMerger,
    ExternalSorter,
    get_used_memory,
)
from pyspark.statcounter import StatCounter
from pyspark.storagelevel import StorageLevel
from pyspark.util import _parse_memory, fail_on_stopiteration, portable_hash

if TYPE_CHECKING:
    from pyspark.core.connect.context import SparkContext
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import StructType

__all__ = ["RDD", "RDDBarrier"]

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
U = TypeVar("U")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

PartitionFunc = Callable[[int, Iterator[Any]], Iterable[Any]]

# Serializer for data that ExternalMerger, ExternalGroupBy, and ExternalSorter spill to disk.
_SPILL_SERIALIZER = AutoBatchedSerializer(CloudPickleSerializer())
# The chunk of shuffle markers, which holds no elements.
_EMPTY_CHUNK = rt.dumps([])


def _chunk_schema() -> "StructType":
    from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

    return StructType(
        [
            StructField(rt.CHUNK_COL, BinaryType(), True),
            StructField(rt.COUNT_COL, IntegerType(), True),
        ]
    )


def _pid_chunk_schema() -> "StructType":
    from pyspark.sql.types import IntegerType, StructField

    return _chunk_schema().add(StructField(rt.PID_COL, IntegerType(), True))


def _place(tagged: "DataFrame", num: int, *extra: Any, pid_col: str = rt.PID_COL) -> "DataFrame":
    """
    Shuffle ``tagged`` to the partitions in its partition id column. One empty row per partition
    is added (``extra`` gives the values of any columns after the chunk, count, and partition id
    columns): adaptive query execution replaces a shuffle with no rows at all by an empty
    relation without partitions, whereas an RDD keeps its empty partitions.
    """
    from pyspark.sql.connect import functions as F

    markers = tagged.sparkSession.range(0, num, 1, numPartitions=1).select(
        F.lit(_EMPTY_CHUNK).alias(rt.CHUNK_COL),
        F.lit(0).alias(rt.COUNT_COL),
        F.col("id").cast("int").alias(pid_col),
        *extra,
    )
    return tagged.union(markers).repartitionById(num, pid_col)


def _compose(first: Optional[PartitionFunc], second: PartitionFunc) -> PartitionFunc:
    if first is None:
        return second

    def composed(split: int, iterator: Iterator[Any]) -> Iterable[Any]:
        return second(split, iter(first(split, iterator)))

    return composed


def _shift(func: PartitionFunc, offset: int) -> PartitionFunc:
    """Run ``func`` in a stage whose task ids are this RDD's partition indices plus ``offset``."""

    def shifted(split: int, iterator: Iterator[Any]) -> Iterable[Any]:
        return func(split - offset, iterator)

    return shifted


def _identity_partition(split: int, iterator: Iterator[Any]) -> Iterator[Any]:
    return iterator


def _confidence_factor(confidence: float, count: int) -> float:
    """Two-sided normal or Student's t quantile, as used by Spark's approximate evaluators."""
    from statistics import NormalDist

    if confidence >= 1.0:
        return math.inf
    if confidence <= 0.0:
        return 0.0
    p = 1 - (1 - confidence) / 2
    z = NormalDist().inv_cdf(p)
    if count > 100:
        return z
    dof = count - 1
    if dof == 1:
        return math.tan(math.pi * (p - 0.5))
    if dof == 2:
        return (2 * p - 1) / math.sqrt(2 * p * (1 - p))
    # Abramowitz and Stegun 26.7.5, accurate to a few parts in 1e4 for three or more degrees of
    # freedom.
    g1 = (z**3 + z) / 4
    g2 = (5 * z**5 + 16 * z**3 + 3 * z) / 96
    g3 = (3 * z**7 + 19 * z**5 + 17 * z**3 - 15 * z) / 384
    g4 = (79 * z**9 + 776 * z**7 + 1482 * z**5 - 1920 * z**3 - 945 * z) / 92160
    return z + g1 / dof + g2 / dof**2 + g3 / dof**3 + g4 / dof**4


class _PythonJrddRef:
    """Lightweight handle so ``RDD._jrdd.first()`` keeps working without a JVM."""

    def __init__(self, rdd: "RDD[Any]") -> None:
        self._rdd = rdd

    def first(self) -> bytes:
        rows = self._rdd.take(1)
        if not rows:
            raise ValueError("RDD is empty")
        buf = BytesIO()
        BatchedSerializer(CPickleSerializer(), 1).dump_stream(iter(rows), buf)
        return buf.getvalue()


class RDD(RDDFacade, Generic[T_co]):
    """
    RDD backed by a Spark Connect :class:`DataFrame`.

    .. versionadded:: 5.0.0
    """

    _ids = itertools.count(1)

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        return object.__new__(cls)

    def __init__(
        self,
        df: "DataFrame",
        ctx: "SparkContext",
        num_partitions: Optional[int] = None,
        source: str = rt.SOURCE_CHUNKS,
        func: Optional[PartitionFunc] = None,
        row_schema: Optional["StructType"] = None,
        binary_as_bytes: bool = True,
    ) -> None:
        self._df = df
        # The facade declares ``ctx`` as Spark Classic's SparkContext; see ``_cctx``.
        self.ctx = cast(Any, ctx)
        # Known partition count, or None until computed.
        self._num_partitions = num_partitions
        self._source = source
        self._func = func
        self._row_schema = row_schema
        self._binary_as_bytes = binary_as_bytes
        # Whether the pending function reads the partition index it is given.
        self._func_uses_split = False
        # Whether ``_df`` holds materialized functions that read the Spark task id as the
        # partition index, so that it must not run under a differently partitioned stage.
        self._df_uses_split = False
        # Whether ``_df`` has a PID column holding each row's partition index.
        self._df_has_pid = False
        self._barrier = False
        self._stage_profile: Optional[ResourceProfile] = None
        self._resource_profile: Optional[ResourceProfile] = None
        self.has_resource_profile = False
        self._materialized: Optional["DataFrame"] = None
        self._rdd_id: Optional[int] = None
        self._name: Optional[str] = None
        self.partitioner: Optional[Partitioner] = None
        self.is_cached = False
        self.is_checkpointed = False
        self._locally_checkpointed = False
        self._jrdd_deserializer: Serializer = AutoBatchedSerializer(CPickleSerializer())

    @classmethod
    def _from_dataframe(cls, df: "ConnectDataFrame") -> "RDD[Any]":
        """``DataFrame.rdd``: an RDD of the DataFrame's rows."""
        rdd: RDD[Any] = RDD(
            df,
            df.sparkSession.sparkContext,
            None,
            rt.SOURCE_ROWS,
            row_schema=df.schema,
            binary_as_bytes=df._get_binary_as_bytes(),
        )
        return rdd

    # -- Internals -----------------------------------------------------------------------------

    @property
    def _cctx(self) -> "SparkContext":
        """``ctx`` as the Spark Connect SparkContext that it is."""
        return cast("SparkContext", self.ctx)

    def _derive(
        self,
        func: PartitionFunc,
        preservesPartitioning: bool = False,
        uses_split: bool = False,
        barrier: bool = False,
    ) -> "RDD[Any]":
        """A narrow transformation: compose ``func`` into the pending pipeline."""
        child: RDD[Any] = RDD(
            self._df,
            self._cctx,
            self._num_partitions,
            self._source,
            _compose(self._func, func),
            self._row_schema,
            self._binary_as_bytes,
        )
        child._func_uses_split = self._func_uses_split or uses_split
        child._df_uses_split = self._df_uses_split
        child._barrier = self._barrier or barrier
        child._stage_profile = self._stage_profile
        if preservesPartitioning:
            child.partitioner = self.partitioner
        return child

    def _input_df(self) -> "DataFrame":
        if self._source == rt.SOURCE_CHUNKS:
            return self._df.select(rt.CHUNK_COL)
        return self._df

    def _run(
        self,
        emit: Callable[..., Any],
        schema: Union["StructType", str],
        func: Optional[PartitionFunc] = None,
        partitions: Optional[Sequence[int]] = None,
        offset: int = 0,
    ) -> "DataFrame":
        """
        DataFrame running the pending pipeline, then ``func``, in one ``mapInArrow`` pass and
        writing the result with ``emit``. ``offset`` is how far the task ids of the stage this
        DataFrame ends up in are from this RDD's partition indices.
        """
        from pyspark.sql.connect import functions as F

        pipeline = self._func if func is None else _compose(self._func, func)
        if offset and pipeline is not None:
            pipeline = _shift(pipeline, offset)
        selected = None if partitions is None else frozenset(partitions)
        runner = rt.make_runner(
            self._source,
            pipeline,
            emit,
            self._row_schema,
            self._binary_as_bytes,
            selected,
        )
        df = self._input_df()
        if selected is not None:
            # Keep the rows of other partitions out of the Python workers; the runner also skips
            # those partitions before running any user function.
            df = df.filter(F.spark_partition_id().isin(sorted(selected)))
        return df.mapInArrow(runner, schema, barrier=self._barrier, profile=self._stage_profile)

    def _chunk_df(self) -> "DataFrame":
        """The elements as encoded chunks: a DataFrame with the chunk and count columns."""
        if self._func is None and self._source == rt.SOURCE_CHUNKS:
            return self._df.select(rt.CHUNK_COL, rt.COUNT_COL)
        if self._materialized is None:
            self._materialized = self._run(rt.emit_chunks, _chunk_schema())
        return self._materialized

    def _uses_split(self) -> bool:
        return self._func_uses_split or self._df_uses_split

    def _sealed(self) -> "RDD[T_co]":
        """
        This RDD without dependence on Spark task ids: a pipeline that reads the partition index
        is materialized in a stage of its own, and every partition is moved as a whole and in
        order to the same partition index.
        """
        if not self._uses_split():
            return self
        num = self.getNumPartitions()

        def tag(split: int, iterator: Iterator[Any]) -> Iterator[Tuple[bytes, int, int]]:
            for blob, n in rt.encode_chunks(iterator):
                yield blob, n, split

        tagged = self._run(rt.emit_pid_chunks, _pid_chunk_schema(), func=tag)
        sealed: RDD[T_co] = RDD(_place(tagged, num), self._cctx, num)
        sealed.partitioner = self.partitioner
        sealed._df_has_pid = True
        return sealed

    def _union_df(self, rdds: Sequence["RDD[Any]"]) -> "DataFrame":
        """
        Concatenate the chunks of ``rdds``. Every branch is re-aliased so that Spark keeps the
        union a concatenation even when branches report the same partitioning.
        """
        from pyspark.sql.connect import functions as F

        result = None
        for i, rdd in enumerate(rdds):
            if i > 0 and rdd._df_uses_split:
                rdd = rdd._sealed()
            if i > 0 and rdd._func_uses_split:
                offset = sum(r.getNumPartitions() for r in rdds[:i])
                part = rdd._run(rt.emit_chunks, _chunk_schema(), offset=offset)
            else:
                part = rdd._chunk_df()
            part = part.select(
                F.col(rt.CHUNK_COL).alias(rt.CHUNK_COL), F.col(rt.COUNT_COL).alias(rt.COUNT_COL)
            )
            result = part if result is None else result.union(part)
        assert result is not None
        return result

    def _pid_tagged_df(self, side: int, offset: int) -> "DataFrame":
        """
        The chunks with their partition index, ``side``, and a sequence number keeping their
        order within the partition, for a union of RDDs with the same partitioner. ``offset`` is
        as in :meth:`_run`.
        """
        from pyspark.sql.connect import functions as F

        rdd = self._sealed() if offset and self._df_uses_split else self
        if rdd._func is None and rdd._df_has_pid:
            tagged = rdd._df.select(rt.CHUNK_COL, rt.COUNT_COL, rt.PID_COL)
        else:

            def tag(split: int, iterator: Iterator[Any]) -> Iterator[Tuple[bytes, int, int]]:
                for blob, n in rt.encode_chunks(iterator):
                    yield blob, n, split

            tagged = rdd._run(rt.emit_pid_chunks, _pid_chunk_schema(), func=tag, offset=offset)
        return tagged.select(
            F.col(rt.CHUNK_COL).alias(rt.CHUNK_COL),
            F.col(rt.COUNT_COL).alias(rt.COUNT_COL),
            F.col(rt.PID_COL).alias(rt.PID_COL),
            F.lit(side).alias("__side"),
            F.monotonically_increasing_id().alias("__seq"),
        )

    def _collect_partition(self, split: int) -> List[Any]:
        """Run the pipeline for one partition only."""
        df = self._run(rt.emit_chunks, _chunk_schema(), partitions=[split])
        return rt.decode_table(df.select(rt.CHUNK_COL).toArrow(), rt.SOURCE_CHUNKS)

    def _collect_partitions(self, partitions: Sequence[int]) -> Dict[int, List[Any]]:
        """Run the pipeline for the given partitions only, keeping the results per partition."""

        def tag(split: int, iterator: Iterator[Any]) -> Iterator[Tuple[int, List[Any]]]:
            yield split, list(iterator)

        df = self._run(rt.emit_chunks, _chunk_schema(), func=tag, partitions=partitions)
        return dict(rt.decode_table(df.select(rt.CHUNK_COL).toArrow(), rt.SOURCE_CHUNKS))

    def _exchange(
        self,
        numPartitions: int,
        bucket: Callable[[int, Iterator[Any]], Iterator[Tuple[bytes, int, int]]],
    ) -> "RDD[Any]":
        """Shuffle the chunks that ``bucket`` assigns to target partitions."""
        tagged = self._run(rt.emit_pid_chunks, _pid_chunk_schema(), func=bucket)
        rdd: RDD[Any] = RDD(_place(tagged, numPartitions), self._cctx, numPartitions)
        rdd._df_has_pid = True
        return rdd

    def _bucketed(
        self, numPartitions: int, target: Callable[[int], Callable[[Any], int]]
    ) -> Callable[[int, Iterator[Any]], Iterator[Tuple[bytes, int, int]]]:
        """
        Map side of a shuffle: group each partition's elements by target partition into chunks,
        flushing a bucket once its chunk is big enough and every bucket when the worker runs out
        of memory. ``target(split)`` returns the target function for that partition.
        """
        limit = self._memory_limit() / 2

        def bucket(split: int, iterator: Iterator[Any]) -> Iterator[Tuple[bytes, int, int]]:
            pid_of = target(split)
            buckets: Dict[int, List[Any]] = defaultdict(list)
            sizes: Dict[int, int] = defaultdict(lambda: 1)
            c = 0
            for x in iterator:
                pid = pid_of(x)
                items = buckets[pid]
                items.append(x)
                c += 1
                if len(items) >= sizes[pid]:
                    blob = rt.dumps(items)
                    yield blob, len(items), pid
                    del buckets[pid]
                    if len(blob) < rt.CHUNK_TARGET_BYTES:
                        sizes[pid] *= 2
                    elif len(blob) > rt.CHUNK_TARGET_BYTES * 10 and sizes[pid] > 1:
                        sizes[pid] //= 2
                if c % 1000 == 0 and get_used_memory() > limit:
                    for p in list(buckets.keys()):
                        items = buckets.pop(p)
                        yield rt.dumps(items), len(items), p
            for p, items in buckets.items():
                yield rt.dumps(items), len(items), p

        return bucket

    def _approx(
        self, partial: Callable[[Iterable[Any]], Any], timeout: int
    ) -> Tuple[List[Any], int, bool]:
        """
        Compute ``partial`` per partition in the background and return the results available
        when ``timeout`` (milliseconds) expires: (results, number of partitions, complete).
        """
        total = self.getNumPartitions()
        if total == 0:
            return [], 0, True
        df = self.mapPartitions(lambda it: [partial(it)])._chunk_df().select(rt.CHUNK_COL)
        session = self._cctx._session
        tag = "spark-connect-rdd-approx-" + uuid.uuid4().hex
        results: List[Any] = []
        errors: List[BaseException] = []
        done = threading.Event()

        def run() -> None:
            try:
                session.addTag(tag)
                try:
                    for row in df.toLocalIterator():
                        results.extend(rt.loads(row[0]))
                finally:
                    session.removeTag(tag)
            except BaseException as e:
                errors.append(e)
            finally:
                done.set()

        Thread(target=run, daemon=True).start()
        complete = done.wait(max(0, timeout) / 1000.0)
        if not complete:
            # Python cannot ask for the final value later, so stop the job instead of leaving it
            # running.
            session.interruptTag(tag)
        elif errors:
            raise errors[0]
        return list(results), total, complete

    def _to_dataframe(self, struct: "StructType", use_large_var_types: bool) -> "DataFrame":
        """Convert the elements to rows of ``struct`` in the same pass as the pipeline."""
        from pyspark.sql.pandas.types import _deduplicate_field_names

        deduped = cast("StructType", _deduplicate_field_names(struct))
        return self._run(rt.make_dataframe_emitter(deduped, use_large_var_types), deduped)

    # -- Identity and persistence -------------------------------------------------------------

    def _pickled(self: "RDD[T]") -> "RDD[T]":
        return self

    def _reserialize(self: "RDD[T]", serializer: Optional[Serializer] = None) -> "RDD[T]":
        return self

    def _to_java_object_rdd(self) -> NoReturn:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD._to_java_object_rdd on Spark Connect"},
        )

    @property
    def _jrdd(self) -> _PythonJrddRef:
        return _PythonJrddRef(self)

    def id(self) -> int:
        if self._rdd_id is None:
            self._rdd_id = next(RDD._ids)
        return self._rdd_id

    def __repr__(self) -> str:
        if self._name is not None:
            return "%s (Spark Connect RDD)" % self._name
        return "Spark Connect RDD"

    def __getnewargs__(self) -> NoReturn:
        # This method is called when attempting to pickle an RDD, which is always an error:
        raise PySparkRuntimeError(
            errorClass="RDD_TRANSFORM_ONLY_VALID_ON_DRIVER",
            messageParameters={},
        )

    @property
    def context(self) -> "SparkContext":  # type: ignore[override]
        return self._cctx

    def cache(self: "RDD[T]") -> "RDD[T]":
        return self.persist(StorageLevel.MEMORY_ONLY)

    def persist(self: "RDD[T]", storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> "RDD[T]":
        if self._func is None:
            df = self._df
        else:
            df = self._chunk_df()
            self._df, self._source, self._func = df, rt.SOURCE_CHUNKS, None
            self._df_uses_split = self._uses_split()
            self._func_uses_split = False
            self._df_has_pid = False
            self._materialized = None
        df.persist(storageLevel)
        self.is_cached = True
        return self

    def unpersist(self: "RDD[T]", blocking: bool = False) -> "RDD[T]":
        self._df.unpersist(blocking)
        self.is_cached = False
        return self

    def getStorageLevel(self) -> StorageLevel:
        return self._df.storageLevel

    def _checkpoint(self, local: bool) -> None:
        sealed = self._sealed()
        if sealed._func is None:
            df, source, has_pid = sealed._df, sealed._source, sealed._df_has_pid
        else:
            df, source, has_pid = sealed._chunk_df(), rt.SOURCE_CHUNKS, False
        checkpointed = df.localCheckpoint(eager=True) if local else df.checkpoint(eager=True)
        self._df = checkpointed
        self._source = source
        self._row_schema = sealed._row_schema
        self._num_partitions = sealed._num_partitions
        self._func = None
        self._func_uses_split = False
        self._df_uses_split = False
        self._df_has_pid = has_pid
        self._materialized = None
        self.is_checkpointed = True
        self._locally_checkpointed = local

    def checkpoint(self) -> None:
        """
        Checkpoint this RDD through :meth:`DataFrame.checkpoint`. On Spark Connect the checkpoint
        is written immediately, to the checkpoint directory configured on the server.
        """
        self._checkpoint(local=False)

    def localCheckpoint(self) -> None:
        """
        Checkpoint this RDD through :meth:`DataFrame.localCheckpoint`, which truncates its
        lineage. On Spark Connect the checkpoint is written immediately.
        """
        self._checkpoint(local=True)

    def isCheckpointed(self) -> bool:
        return self.is_checkpointed

    def isLocallyCheckpointed(self) -> bool:
        return self._locally_checkpointed

    def getCheckpointFile(self) -> Optional[str]:
        return None

    def cleanShuffleDependencies(self, blocking: bool = False) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.cleanShuffleDependencies on Spark Connect"},
        )

    def name(self) -> Optional[str]:
        return self._name

    def setName(self: "RDD[T]", name: str) -> "RDD[T]":
        self._name = name
        return self

    def toDebugString(self) -> Optional[bytes]:
        """The extended plan of the DataFrame that computes this RDD."""
        df = self._df if self._func is None else self._chunk_df()
        return cast("ConnectDataFrame", df)._explain_string(extended=True).encode("utf-8")

    # -- Narrow transformations ---------------------------------------------------------------

    def map(self: "RDD[T]", f: Callable[[T], U], preservesPartitioning: bool = False) -> "RDD[U]":
        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return map(fail_on_stopiteration(f), iterator)

        return self._derive(func, preservesPartitioning)

    def flatMap(
        self: "RDD[T]", f: Callable[[T], Iterable[U]], preservesPartitioning: bool = False
    ) -> "RDD[U]":
        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return chain.from_iterable(map(fail_on_stopiteration(f), iterator))

        return self._derive(func, preservesPartitioning)

    def mapPartitions(
        self: "RDD[T]", f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning: bool = False
    ) -> "RDD[U]":
        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return f(iterator)

        return self._derive(func, preservesPartitioning)

    def mapPartitionsWithIndex(
        self: "RDD[T]",
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> "RDD[U]":
        return self._derive(f, preservesPartitioning, uses_split=True)

    def mapPartitionsWithSplit(
        self: "RDD[T]",
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> "RDD[U]":
        warnings.warn(
            "mapPartitionsWithSplit is deprecated; use mapPartitionsWithIndex instead",
            FutureWarning,
            stacklevel=2,
        )
        return self.mapPartitionsWithIndex(f, preservesPartitioning)

    def filter(self: "RDD[T]", f: Callable[[T], bool]) -> "RDD[T]":
        def func(iterator: Iterable[T]) -> Iterable[T]:
            return filter(fail_on_stopiteration(f), iterator)

        return self.mapPartitions(func, True)

    def glom(self: "RDD[T]") -> "RDD[List[T]]":
        def func(iterator: Iterable[T]) -> Iterable[List[T]]:
            yield list(iterator)

        return self.mapPartitions(func)

    def keyBy(self: "RDD[T]", f: Callable[[T], K]) -> "RDD[Tuple[K, T]]":
        return self.map(lambda x: (f(x), x))

    def keys(self: "RDD[Tuple[K, V]]") -> "RDD[K]":
        return self.map(lambda x: x[0])

    def values(self: "RDD[Tuple[K, V]]") -> "RDD[V]":
        return self.map(lambda x: x[1])

    def mapValues(self: "RDD[Tuple[K, V]]", f: Callable[[V], U]) -> "RDD[Tuple[K, U]]":
        def map_values_fn(kv: Tuple[K, V]) -> Tuple[K, U]:
            return kv[0], f(kv[1])

        return self.map(map_values_fn, preservesPartitioning=True)

    def flatMapValues(
        self: "RDD[Tuple[K, V]]", f: Callable[[V], Iterable[U]]
    ) -> "RDD[Tuple[K, U]]":
        def flat_map_fn(kv: Tuple[K, V]) -> Iterable[Tuple[K, U]]:
            return ((kv[0], x) for x in f(kv[1]))

        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def sample(
        self: "RDD[T]", withReplacement: bool, fraction: float, seed: Optional[int] = None
    ) -> "RDD[T]":
        if not fraction >= 0:
            raise ValueError("Fraction must be nonnegative.")
        sampler = RDDSampler(withReplacement, fraction, seed)
        return self._derive(sampler.func, True, uses_split=True)

    def sampleByKey(
        self: "RDD[Tuple[K, V]]",
        withReplacement: bool,
        fractions: Dict[K, Union[float, int]],
        seed: Optional[int] = None,
    ) -> "RDD[Tuple[K, V]]":
        for fraction in fractions.values():
            assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        sampler = RDDStratifiedSampler(withReplacement, fractions, seed)
        return self._derive(sampler.func, True, uses_split=True)

    def randomSplit(
        self: "RDD[T]", weights: Sequence[Union[int, float]], seed: Optional[int] = None
    ) -> "List[RDDFacade[T]]":
        if not all(w >= 0 for w in weights):
            raise ValueError("Weights must be nonnegative")
        s = float(sum(weights))
        if not s > 0:
            raise ValueError("Sum of weights must be positive")
        cweights = [0.0]
        for w in weights:
            cweights.append(cweights[-1] + w / s)
        if seed is None:
            seed = random.randint(0, 2**32 - 1)
        return [
            self._derive(RDDRangeSampler(lb, ub, seed).func, True, uses_split=True)
            for lb, ub in zip(cweights, cweights[1:])
        ]

    def zipWithIndex(self: "RDD[T]") -> "RDD[Tuple[T, int]]":
        starts = [0]
        num = self.getNumPartitions()
        if num > 1:
            counts = dict(
                self.mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))]).collect()
            )
            for i in range(num - 1):
                starts.append(starts[-1] + counts.get(i, 0))

        def func(k: int, it: Iterable[T]) -> Iterable[Tuple[T, int]]:
            for i, v in enumerate(it, starts[k]):
                yield v, i

        return self.mapPartitionsWithIndex(func)

    def zipWithUniqueId(self: "RDD[T]") -> "RDD[Tuple[T, int]]":
        n = self.getNumPartitions()

        def func(k: int, it: Iterable[T]) -> Iterable[Tuple[T, int]]:
            for i, v in enumerate(it):
                yield v, i * n + k

        return self.mapPartitionsWithIndex(func)

    def pipe(
        self, command: str, env: Optional[Dict[str, str]] = None, checkCode: bool = False
    ) -> "RDD[str]":
        if env is None:
            env = dict()

        def func(iterator: Iterable[T]) -> Iterable[str]:
            pipe = Popen(shlex.split(command), env=env, stdin=PIPE, stdout=PIPE)

            def pipe_objs(out: IO[bytes]) -> None:
                for obj in iterator:
                    s = str(obj).rstrip("\n") + "\n"
                    out.write(s.encode("utf-8"))
                out.close()

            # Feed stdin from another thread so that a full stdout pipe cannot block the writer.
            Thread(target=pipe_objs, args=[pipe.stdin]).start()

            def check_return_code() -> Iterable[int]:
                pipe.wait()
                if checkCode and pipe.returncode:
                    raise PySparkRuntimeError(
                        errorClass="PIPE_FUNCTION_EXITED",
                        messageParameters={
                            "func_name": command,
                            "error_code": str(pipe.returncode),
                        },
                    )
                else:
                    for i in range(0):
                        yield i

            return (
                cast(bytes, x).rstrip(b"\n").decode("utf-8")
                for x in chain(
                    iter(cast(IO[bytes], pipe.stdout).readline, b""), check_return_code()
                )
            )

        return self.mapPartitions(func)

    def barrier(self: "RDD[T]") -> "RDDBarrier[T]":
        return RDDBarrier(self)

    def _is_barrier(self) -> bool:
        return self._barrier

    def withResources(self: "RDD[T]", profile: ResourceProfile) -> "RDD[T]":
        """Run the stage computing this RDD with ``profile`` (stage-level scheduling)."""
        self._resource_profile = profile
        self._stage_profile = profile
        self.has_resource_profile = True
        self._materialized = None
        return self

    def getResourceProfile(self) -> Optional[ResourceProfile]:
        return self._resource_profile

    # -- Partitioning --------------------------------------------------------------------------

    def getNumPartitions(self) -> int:
        """
        The number of partitions. It is known without running a job for RDDs from
        ``parallelize``, ``range``, shuffles, ``coalesce``, and narrow transformations of those;
        otherwise one job counts them.
        """
        if self._num_partitions is None:
            from pyspark.sql.connect import functions as F

            markers = self._df.select(F.lit(0).alias("x")).mapInArrow(rt.mark_partition, "p int")
            self._num_partitions = int(markers.count())
        return self._num_partitions

    def _memory_limit(self) -> int:
        conf = self._cctx._session.conf.get("spark.python.worker.memory", "512m")
        return _parse_memory(cast(str, conf))

    def _defaultReducePartitions(self) -> int:
        if self._cctx._session.conf.get("spark.default.parallelism", None) is not None:
            return self._cctx.defaultParallelism
        return self.getNumPartitions()

    def partitionBy(
        self: "RDD[Tuple[K, V]]",
        numPartitions: Optional[int],
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, V]]":
        if numPartitions is None:
            num_partitions = self._defaultReducePartitions()
        else:
            num_partitions = numPartitions
        partitioner = Partitioner(num_partitions, partitionFunc)
        if self.partitioner == partitioner:
            return self

        def target(_: int) -> Callable[[Tuple[K, V]], int]:
            def pid(kv: Tuple[K, V]) -> int:
                k, _v = kv
                return partitionFunc(k) % num_partitions

            return pid

        rdd = self._exchange(num_partitions, self._bucketed(num_partitions, target))
        rdd.partitioner = partitioner
        return rdd

    def repartition(self: "RDD[T]", numPartitions: int) -> "RDD[T]":
        return self.coalesce(numPartitions, shuffle=True)

    def coalesce(self: "RDD[T]", numPartitions: int, shuffle: bool = False) -> "RDD[T]":
        if not numPartitions > 0:
            raise ValueError("Number of partitions must be positive.")
        if shuffle:
            # Deal the elements of each partition round-robin from a position that depends on the
            # partition, as Spark's coalesce with shuffle does, so output partitions are even.
            def target(split: int) -> Callable[[Any], int]:
                position = random.Random(split).randrange(numPartitions)

                def pid(_: Any) -> int:
                    nonlocal position
                    position = (position + 1) % numPartitions
                    return position

                return pid

            return self._exchange(numPartitions, self._bucketed(numPartitions, target))
        sealed = self._sealed()
        if sealed._func is None and sealed._source != rt.SOURCE_CHUNKS:
            df = sealed._df.coalesce(numPartitions)
            source = sealed._source
        else:
            df = sealed._chunk_df().coalesce(numPartitions)
            source = rt.SOURCE_CHUNKS
        known = sealed._num_partitions
        num = numPartitions if known is None else min(numPartitions, known)
        rdd: RDD[T] = RDD(
            df, self._cctx, num, source, None, sealed._row_schema, sealed._binary_as_bytes
        )
        return rdd

    def repartitionAndSortWithinPartitions(
        self: "RDD[Tuple[Any, Any]]",
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[Any], int] = portable_hash,
        ascending: bool = True,
        keyfunc: Callable[[Any], Any] = lambda x: x,
    ) -> "RDD[Tuple[Any, Any]]":
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()
        memory = self._memory_limit()

        def sortPartition(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, V]]:
            sort = ExternalSorter[tuple[K, V]](memory * 0.9, _SPILL_SERIALIZER).sorted
            return iter(sort(iterator, key=lambda k_v: keyfunc(k_v[0]), reverse=(not ascending)))

        return self.partitionBy(numPartitions, partitionFunc).mapPartitions(sortPartition, True)

    def sortByKey(
        self: "RDD[Tuple[K, V]]",
        ascending: Optional[bool] = True,
        numPartitions: Optional[int] = None,
        keyfunc: Callable[[Any], Any] = lambda x: x,
    ) -> "RDD[Tuple[K, V]]":
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()
        memory = self._memory_limit()

        def sortPartition(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, V]]:
            sort = ExternalSorter[tuple[K, V]](memory * 0.9, _SPILL_SERIALIZER).sorted
            return iter(sort(iterator, key=lambda kv: keyfunc(kv[0]), reverse=(not ascending)))

        if numPartitions == 1:
            rdd = self.coalesce(1) if self.getNumPartitions() > 1 else self
            return rdd.mapPartitions(sortPartition, True)

        # first compute the boundary of each part via sampling: we want to partition
        # the key-space into bins such that the bins have roughly the same
        # number of (key, value) pairs falling into them
        rddSize = self.count()
        if not rddSize:
            return self  # empty RDD
        maxSampleSize = numPartitions * 20.0  # constant from Spark's RangePartitioner
        fraction = min(maxSampleSize / max(rddSize, 1), 1.0)
        samples = self.sample(False, fraction, 1).map(lambda kv: kv[0]).collect()
        samples = sorted(samples, key=keyfunc)

        # we have numPartitions many parts but one of the them has
        # an implicit boundary
        bounds = [
            samples[int(len(samples) * (i + 1) / numPartitions)]
            for i in range(0, numPartitions - 1)
        ]

        def rangePartitioner(k: K) -> int:
            p = bisect.bisect_left(bounds, keyfunc(k))
            if ascending:
                return p
            else:
                return numPartitions - 1 - p

        return self.partitionBy(numPartitions, rangePartitioner).mapPartitions(sortPartition, True)

    def sortBy(
        self: "RDD[T]",
        keyfunc: Callable[[T], Any],
        ascending: bool = True,
        numPartitions: Optional[int] = None,
    ) -> "RDD[T]":
        return self.keyBy(keyfunc).sortByKey(ascending, numPartitions).values()

    # -- Combining RDDs ------------------------------------------------------------------------

    def union(self: "RDD[T]", other: "RDDFacade[U]") -> "RDD[Union[T, U]]":
        if not isinstance(other, RDD):
            raise TypeError("Cannot union a Spark Connect RDD with %s" % type(other).__name__)
        if self.partitioner is not None and self.partitioner == other.partitioner:
            # Like Spark Classic's PartitionerAwareUnionRDD: keep the partitioning, with the
            # elements of each partition of this RDD before those of the other.
            num = self.partitioner.numPartitions
            tagged = self._pid_tagged_df(0, 0).union(other._pid_tagged_df(1, num))
            from pyspark.sql.connect import functions as F

            markers = (F.lit(-1).alias("__side"), F.lit(0).cast("long").alias("__seq"))
            df = (
                _place(tagged, num, *markers)
                .sortWithinPartitions("__side", "__seq")
                .select(rt.CHUNK_COL, rt.COUNT_COL, rt.PID_COL)
            )
            aware: RDD[Union[T, U]] = RDD(df, self._cctx, num)
            aware.partitioner = self.partitioner
            aware._df_has_pid = True
            return aware
        df = self._union_df([self, other])
        known = (self._num_partitions, other._num_partitions)
        total = None if None in known else sum(cast(Tuple[int, int], known))
        rdd: RDD[Union[T, U]] = RDD(df, self._cctx, total)
        rdd._df_uses_split = self._uses_split() or other._uses_split()
        return rdd

    def __add__(self: "RDD[T]", other: "RDDFacade[U]") -> "RDD[Union[T, U]]":
        if not isinstance(other, RDD):
            raise TypeError
        return self.union(other)

    def intersection(self: "RDD[T]", other: "RDDFacade[T]") -> "RDD[T]":
        return (
            self.map(lambda v: (v, None))
            .cogroup(other.map(lambda v: (v, None)))
            .filter(lambda k_vs: all(k_vs[1]))
            .keys()
        )

    def subtract(
        self: "RDD[T]", other: "RDDFacade[T]", numPartitions: Optional[int] = None
    ) -> "RDD[T]":
        # note: here 'True' is just a placeholder
        rdd = other.map(lambda x: (x, True))
        return self.map(lambda x: (x, True)).subtractByKey(rdd, numPartitions).keys()

    def subtractByKey(
        self: "RDD[Tuple[K, V]]",
        other: "RDDFacade[Tuple[K, Any]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, V]]":
        def filter_func(pair: Tuple[K, Tuple[V, Any]]) -> bool:
            key, (val1, val2) = pair
            return val1 and not val2  # type: ignore[return-value]

        return (
            self.cogroup(other, numPartitions)
            .filter(filter_func)  # type: ignore[arg-type]
            .flatMapValues(lambda x: x[0])
        )

    def distinct(self: "RDD[T]", numPartitions: Optional[int] = None) -> "RDD[T]":
        return (
            self.map(lambda x: (x, None))
            .reduceByKey(lambda x, _: x, numPartitions)
            .map(lambda x: x[0])
        )

    def cartesian(self: "RDD[T]", other: "RDDFacade[U]") -> "RDD[Tuple[T, U]]":
        from pyspark.sql.connect import functions as F

        if not isinstance(other, RDD):
            raise TypeError("Cannot compute the cartesian product with %s" % type(other).__name__)
        left = self._sealed()._chunk_df().select(F.col(rt.CHUNK_COL).alias("__l"))
        right = other._sealed()._chunk_df().select(F.col(rt.CHUNK_COL).alias("__r"))
        crossed = left.crossJoin(right)

        def product(_: int, pairs: Iterator[Tuple[bytes, bytes]]) -> Iterator[Tuple[Any, Any]]:
            for lblob, rblob in pairs:
                rights = rt.loads(rblob)
                for x in rt.loads(lblob):
                    for y in rights:
                        yield x, y

        return RDD(crossed, self._cctx, None, rt.SOURCE_PAIRS, product)

    def zip(self: "RDD[T]", other: "RDDFacade[U]") -> "RDD[Tuple[T, U]]":
        if not isinstance(other, RDD):
            raise TypeError("Cannot zip with %s" % type(other).__name__)
        num = self.getNumPartitions()
        if num != other.getNumPartitions():
            raise ValueError("Can only zip with RDD which has the same number of partitions")

        def zip_partition(left: Iterable[Any], right: Iterable[Any]) -> Iterator[Tuple[Any, Any]]:
            lit, rit = iter(left), iter(right)
            for x in lit:
                try:
                    y = next(rit)
                except StopIteration:
                    raise ValueError(
                        "Can only zip RDDs with same number of elements in each partition"
                    )
                yield x, y
            for _ in rit:
                raise ValueError("Can only zip RDDs with same number of elements in each partition")

        if (
            self._df is other._df
            and self._source == other._source
            and self._barrier == other._barrier
        ):
            # Both sides derive from the same rows: compute both pipelines in one pass.
            lfunc = self._func or _identity_partition
            rfunc = other._func or _identity_partition

            def zip_pipelines(split: int, iterator: Iterator[Any]) -> Iterator[Tuple[Any, Any]]:
                lin, rin = itertools.tee(iterator)
                return zip_partition(lfunc(split, lin), rfunc(split, rin))

            rdd: RDD[Tuple[T, U]] = RDD(
                self._df,
                self._cctx,
                num,
                self._source,
                zip_pipelines,
                self._row_schema,
                self._binary_as_bytes,
            )
            rdd._func_uses_split = self._func_uses_split or other._func_uses_split
            rdd._df_uses_split = self._df_uses_split
            rdd._barrier = self._barrier
            return rdd

        # Number fixed-size groups of each side's elements per partition, bring the groups with
        # the same partition and number together, and pair them up in order.
        group = 1024

        def tagger(side: int) -> PartitionFunc:
            def tag(split: int, iterator: Iterator[Any]) -> Iterator[Any]:
                buf: List[Any] = []
                seq = 0
                for x in iterator:
                    buf.append(x)
                    if len(buf) == group:
                        yield split, seq, side, rt.dumps(buf)
                        buf = []
                        seq += 1
                if buf:
                    yield split, seq, side, rt.dumps(buf)

            return tag

        tagged_schema = "__p int, __s int, __side int, __b binary"

        def emit_tagged(split: int, rows: Iterator[Any]) -> Iterator[Any]:
            import pyarrow as pa

            cols: List[List[Any]] = [[], [], [], []]
            for row in rows:
                for col, v in zip(cols, row):
                    col.append(v)
                if len(cols[0]) >= 256:
                    yield pa.RecordBatch.from_arrays(
                        [
                            pa.array(cols[0], pa.int32()),
                            pa.array(cols[1], pa.int32()),
                            pa.array(cols[2], pa.int32()),
                            pa.array(cols[3], pa.large_binary()),
                        ],
                        names=["__p", "__s", "__side", "__b"],
                    )
                    cols = [[], [], [], []]
            if cols[0]:
                yield pa.RecordBatch.from_arrays(
                    [
                        pa.array(cols[0], pa.int32()),
                        pa.array(cols[1], pa.int32()),
                        pa.array(cols[2], pa.int32()),
                        pa.array(cols[3], pa.large_binary()),
                    ],
                    names=["__p", "__s", "__side", "__b"],
                )

        from pyspark.sql.connect import functions as F

        left = self._sealed() if self._df_uses_split else self
        right = other._sealed() if other._df_uses_split else other
        realias = [F.col(c).alias(c) for c in ("__p", "__s", "__side", "__b")]
        ldf = left._run(emit_tagged, tagged_schema, func=tagger(0)).select(*realias)
        # The right side runs after the left side's partitions in the union below.
        rdf = right._run(emit_tagged, tagged_schema, func=tagger(1), offset=num).select(*realias)
        both = ldf.union(rdf).select("__b", F.lit(0).alias("__n"), "__p", "__s", "__side")
        grouped = _place(
            both, num, F.lit(-1).alias("__s"), F.lit(-1).alias("__side"), pid_col="__p"
        ).sortWithinPartitions("__s", "__side")

        def pair_groups(_: int, rows: Iterator[Tuple[int, int, bytes]]) -> Iterator[Any]:
            pending: Optional[Tuple[int, int, bytes]] = None
            for row in rows:
                if row[0] < 0:
                    continue  # a marker of the shuffle
                if pending is None:
                    pending = row
                    continue
                seq, side, blob = pending
                rseq, rside, rblob = row
                if seq != rseq or side != 0 or rside != 1:
                    raise ValueError(
                        "Can only zip RDDs with same number of elements in each partition"
                    )
                yield from zip_partition(rt.loads(blob), rt.loads(rblob))
                pending = None
            if pending is not None:
                raise ValueError("Can only zip RDDs with same number of elements in each partition")

        rows_df = grouped.select("__s", "__side", "__b")
        return RDD(rows_df, self._cctx, num, rt.SOURCE_TUPLES, pair_groups)

    # -- Shuffle-based aggregation -------------------------------------------------------------

    def combineByKey(
        self: "RDD[Tuple[K, V]]",
        createCombiner: Callable[[V], U],
        mergeValue: Callable[[U, V], U],
        mergeCombiners: Callable[[U, U], U],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, U]]":
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()
        memory = self._memory_limit()
        agg = Aggregator(createCombiner, mergeValue, mergeCombiners)

        def combineLocally(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, U]]:
            merger = ExternalMerger[K, V, U](agg, memory * 0.9, _SPILL_SERIALIZER)
            merger.mergeValues(iterator)
            return merger.items()

        locally_combined = self.mapPartitions(combineLocally, preservesPartitioning=True)
        shuffled = locally_combined.partitionBy(numPartitions, partitionFunc)

        def _mergeCombiners(iterator: Iterable[Tuple[K, U]]) -> Iterable[Tuple[K, U]]:
            merger = ExternalMerger[K, V, U](agg, memory, _SPILL_SERIALIZER)
            merger.mergeCombiners(iterator)
            return merger.items()

        return shuffled.mapPartitions(_mergeCombiners, preservesPartitioning=True)

    def aggregateByKey(
        self: "RDD[Tuple[K, V]]",
        zeroValue: U,
        seqFunc: Callable[[U, V], U],
        combFunc: Callable[[U, U], U],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, U]]":
        def createZero() -> U:
            return copy.deepcopy(zeroValue)

        return self.combineByKey(
            lambda v: seqFunc(createZero(), v), seqFunc, combFunc, numPartitions, partitionFunc
        )

    def foldByKey(
        self: "RDD[Tuple[K, V]]",
        zeroValue: V,
        func: Callable[[V, V], V],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, V]]":
        def createZero() -> V:
            return copy.deepcopy(zeroValue)

        return self.combineByKey(
            lambda v: func(createZero(), v), func, func, numPartitions, partitionFunc
        )

    def reduceByKey(
        self: "RDD[Tuple[K, V]]",
        func: Callable[[V, V], V],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, V]]":
        return self.combineByKey(lambda x: x, func, func, numPartitions, partitionFunc)

    def groupByKey(
        self: "RDD[Tuple[K, V]]",
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, Iterable[V]]]":
        def createCombiner(x: V) -> List[V]:
            return [x]

        def mergeValue(xs: List[V], x: V) -> List[V]:
            xs.append(x)
            return xs

        def mergeCombiners(a: List[V], b: List[V]) -> List[V]:
            a.extend(b)
            return a

        memory = self._memory_limit()
        agg = Aggregator(createCombiner, mergeValue, mergeCombiners)

        def combine(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, List[V]]]:
            merger = ExternalMerger[K, V, list[V]](agg, memory * 0.9, _SPILL_SERIALIZER)
            merger.mergeValues(iterator)
            return merger.items()

        locally_combined = self.mapPartitions(combine, preservesPartitioning=True)
        shuffled = locally_combined.partitionBy(numPartitions, partitionFunc)

        def groupByKey(it: Iterable[Tuple[K, List[V]]]) -> Iterable[Tuple[K, Any]]:
            merger = ExternalGroupBy[K, V](agg, memory, _SPILL_SERIALIZER)  # type: ignore[arg-type]
            merger.mergeCombiners(it)
            return merger.items()

        return shuffled.mapPartitions(groupByKey, True).mapValues(ResultIterable)

    def groupBy(
        self: "RDD[T]",
        f: Callable[[T], K],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, Iterable[T]]]":
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions, partitionFunc)

    def cogroup(
        self: "RDD[Tuple[K, V]]",
        other: "RDDFacade[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[ResultIterable[V], ResultIterable[U]]]]":
        return python_cogroup((self, other), numPartitions)

    def groupWith(
        self: "RDD[Tuple[Any, Any]]",
        other: "RDDFacade[Tuple[Any, Any]]",
        /,
        *others: "RDDFacade[Tuple[Any, Any]]",
    ) -> "RDD[Tuple[Any, Tuple[ResultIterable[Any], ...]]]":
        return python_cogroup((self, other) + others, numPartitions=None)

    def join(
        self: "RDD[Tuple[K, V]]",
        other: "RDDFacade[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[V, U]]]":
        return python_join(self, other, numPartitions)

    def leftOuterJoin(
        self: "RDD[Tuple[K, V]]",
        other: "RDDFacade[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[V, Optional[U]]]]":
        return python_left_outer_join(self, other, numPartitions)

    def rightOuterJoin(
        self: "RDD[Tuple[K, V]]",
        other: "RDDFacade[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[Optional[V], U]]]":
        return python_right_outer_join(self, other, numPartitions)

    def fullOuterJoin(
        self: "RDD[Tuple[K, V]]",
        other: "RDDFacade[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[Optional[V], Optional[U]]]]":
        return python_full_outer_join(self, other, numPartitions)

    # -- Actions -------------------------------------------------------------------------------

    def collect(self: "RDD[T]") -> List[T]:
        if self._func is None and self._source == rt.SOURCE_ROWS:
            return cast(List[T], self._df.collect())
        if self._func is None and self._source != rt.SOURCE_CHUNKS:
            return rt.decode_table(self._df.toArrow(), self._source)
        return rt.decode_table(self._chunk_df().select(rt.CHUNK_COL).toArrow(), rt.SOURCE_CHUNKS)

    def collectWithJobGroup(
        self: "RDD[T]", groupId: str, description: str, interruptOnCancel: bool = False
    ) -> "List[T]":
        warnings.warn(
            "Deprecated in 3.1, Use pyspark.InheritableThread with the pinned thread mode enabled.",
            FutureWarning,
        )
        self._cctx._session.addTag(groupId)
        try:
            return self.collect()
        finally:
            self._cctx._discard_connect_job_tag_if_present(groupId)

    def collectAsMap(self: "RDD[Tuple[K, V]]") -> Dict[K, V]:
        return dict(self.collect())

    def toLocalIterator(self: "RDD[T]", prefetchPartitions: bool = False) -> Iterator[T]:
        """
        Iterate over the elements one partition at a time; each partition is computed by its
        own job when it is reached, or while the previous one is consumed with
        ``prefetchPartitions``.
        """
        num = self.getNumPartitions()
        if not prefetchPartitions:
            for split in range(num):
                yield from self._collect_partition(split)
            return
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._collect_partition, 0) if num > 0 else None
            for split in range(num):
                assert future is not None
                items = future.result()
                if split + 1 < num:
                    future = executor.submit(self._collect_partition, split + 1)
                yield from items

    def count(self) -> int:
        from pyspark.sql.connect import functions as F

        if self._func is None:
            if self._source == rt.SOURCE_CHUNKS:
                row = self._df.agg(F.sum(rt.COUNT_COL)).first()
                return int(row[0]) if row is not None and row[0] is not None else 0
            return int(self._df.count())
        row = self._run(rt.emit_count, "n long").agg(F.sum("n")).first()
        return int(row[0]) if row is not None and row[0] is not None else 0

    def take(self: "RDD[T]", num: int) -> List[T]:
        from pyspark.sql.connect import functions as F

        if num <= 0:
            return []
        if self._func is None and self._source == rt.SOURCE_ROWS:
            return cast(List[T], self._df.limit(num).collect())
        if self._func is None and self._source != rt.SOURCE_CHUNKS:
            return rt.decode_table(self._df.limit(num).toArrow(), self._source)
        chunks = self._chunk_df().filter(F.col(rt.COUNT_COL) > 0).select(rt.CHUNK_COL)
        # Without the shuffle markers, chunks are never empty, and the first chunks of a
        # partition hold 1, 2, 4, ... elements, so this many chunks usually cover ``num``
        # elements; ask for more if they do not.
        limit = max(1, num.bit_length())
        while True:
            table = chunks.limit(limit).toArrow()
            items = rt.decode_table(table, rt.SOURCE_CHUNKS)
            if len(items) >= num or table.num_rows < limit:
                return items[:num]
            per_chunk = max(len(items) / max(table.num_rows, 1), 1.0)
            limit = max(limit * 4, int(math.ceil(num / per_chunk)) + 1)

    def first(self: "RDD[T]") -> T:
        rs = self.take(1)
        if rs:
            return rs[0]
        raise ValueError("RDD is empty")

    def isEmpty(self) -> bool:
        return self._num_partitions == 0 or len(self.take(1)) == 0

    def foreach(self: "RDD[T]", f: Callable[[T], None]) -> None:
        f = fail_on_stopiteration(f)

        def processPartition(iterator: Iterable[T]) -> Iterable[Any]:
            for x in iterator:
                f(x)
            return iter([])

        self.mapPartitions(processPartition).count()  # Force evaluation

    def foreachPartition(self: "RDD[T]", f: Callable[[Iterable[T]], None]) -> None:
        def func(it: Iterable[T]) -> Iterable[Any]:
            r = f(it)  # type: ignore[func-returns-value]
            if r is None:
                return iter([])
            try:
                return iter(r)
            except TypeError:
                return iter([])

        self.mapPartitions(func).count()  # Force evaluation

    def reduce(self: "RDD[T]", f: Callable[[T, T], T]) -> T:
        f = fail_on_stopiteration(f)

        def func(iterator: Iterable[T]) -> Iterable[T]:
            iterator = iter(iterator)
            try:
                initial = next(iterator)
            except StopIteration:
                return
            yield reduce(f, iterator, initial)

        vals = self.mapPartitions(func).collect()
        if vals:
            return reduce(f, vals)
        raise ValueError("Can not reduce() empty RDD")

    def treeReduce(self: "RDD[T]", f: Callable[[T, T], T], depth: int = 2) -> T:
        if depth < 1:
            raise ValueError("Depth cannot be smaller than 1 but got %d." % depth)

        # Use the second entry to indicate whether this is a dummy value.
        zeroValue: Tuple[T, bool] = (  # type: ignore[assignment]
            None,
            True,
        )

        def op(x: Tuple[T, bool], y: Tuple[T, bool]) -> Tuple[T, bool]:
            if x[1]:
                return y
            elif y[1]:
                return x
            else:
                return f(x[0], y[0]), False

        reduced = self.map(lambda x: (x, False)).treeAggregate(zeroValue, op, op, depth)
        if reduced[1]:
            raise ValueError("Cannot reduce empty RDD.")
        return reduced[0]

    def fold(self: "RDD[T]", zeroValue: T, op: Callable[[T, T], T]) -> T:
        op = fail_on_stopiteration(op)

        def func(iterator: Iterable[T]) -> Iterable[T]:
            acc = zeroValue
            for obj in iterator:
                acc = op(acc, obj)
            yield acc

        vals = self.mapPartitions(func).collect()
        return reduce(op, vals, zeroValue)

    def aggregate(
        self: "RDD[T]", zeroValue: U, seqOp: Callable[[U, T], U], combOp: Callable[[U, U], U]
    ) -> U:
        seqOp = fail_on_stopiteration(seqOp)
        combOp = fail_on_stopiteration(combOp)

        def func(iterator: Iterable[T]) -> Iterable[U]:
            acc = zeroValue
            for obj in iterator:
                acc = seqOp(acc, obj)
            yield acc

        vals = self.mapPartitions(func).collect()
        return reduce(combOp, vals, zeroValue)

    def treeAggregate(
        self: "RDD[T]",
        zeroValue: U,
        seqOp: Callable[[U, T], U],
        combOp: Callable[[U, U], U],
        depth: int = 2,
    ) -> U:
        if depth < 1:
            raise ValueError("Depth cannot be smaller than 1 but got %d." % depth)

        if self.getNumPartitions() == 0:
            return zeroValue

        def aggregatePartition(iterator: Iterable[T]) -> Iterable[U]:
            acc = zeroValue
            for obj in iterator:
                acc = seqOp(acc, obj)
            yield acc

        partiallyAggregated = self.mapPartitions(aggregatePartition)
        numPartitions = partiallyAggregated.getNumPartitions()
        scale = max(int(math.ceil(pow(numPartitions, 1.0 / depth))), 2)
        # If creating an extra level doesn't help reduce the wall-clock time, we stop the tree
        # aggregation.
        while numPartitions > scale + numPartitions / scale:
            numPartitions /= scale  # type: ignore[assignment]
            curNumPartitions = int(numPartitions)

            def mapPartition(i: int, iterator: Iterable[U]) -> Iterable[Tuple[int, U]]:
                for obj in iterator:
                    yield (i % curNumPartitions, obj)

            partiallyAggregated = (
                partiallyAggregated.mapPartitionsWithIndex(mapPartition)
                .reduceByKey(combOp, curNumPartitions)
                .values()
            )

        return partiallyAggregated.reduce(combOp)

    def max(self: "RDD[T]", key: Optional[Callable[[T], Any]] = None) -> T:
        if key is None:
            return self.reduce(max)  # type: ignore[arg-type]
        return self.reduce(lambda a, b: max(a, b, key=key))

    def min(self: "RDD[T]", key: Optional[Callable[[T], Any]] = None) -> T:
        if key is None:
            return self.reduce(min)  # type: ignore[arg-type]
        return self.reduce(lambda a, b: min(a, b, key=key))

    def sum(self: "RDD[Any]") -> Any:
        return self.mapPartitions(lambda x: [sum(x)]).fold(0, operator.add)

    def stats(self: "RDD[Any]") -> StatCounter:
        def redFunc(left_counter: StatCounter, right_counter: StatCounter) -> StatCounter:
            return left_counter.mergeStats(right_counter)

        return self.mapPartitions(lambda i: [StatCounter(i)]).reduce(redFunc)

    def mean(self: "RDD[Any]") -> float:
        return self.stats().mean()

    def variance(self: "RDD[Any]") -> float:
        return self.stats().variance()

    def stdev(self: "RDD[Any]") -> float:
        return self.stats().stdev()

    def sampleStdev(self: "RDD[Any]") -> float:
        return self.stats().sampleStdev()

    def sampleVariance(self: "RDD[Any]") -> float:
        return self.stats().sampleVariance()

    def histogram(
        self: "RDD[Any]", buckets: Union[int, List[Any], Tuple[Any, ...]]
    ) -> Tuple[Sequence[Any], List[int]]:
        if isinstance(buckets, int):
            if buckets < 1:
                raise ValueError("number of buckets must be >= 1")

            # filter out non-comparable elements
            def comparable(x: Any) -> bool:
                if x is None:
                    return False
                if type(x) is float and math.isnan(x):
                    return False
                return True

            filtered = self.filter(comparable)

            # faster than stats()
            def minmax(a: Tuple[Any, Any], b: Tuple[Any, Any]) -> Tuple[Any, Any]:
                return min(a[0], b[0]), max(a[1], b[1])

            try:
                minv, maxv = filtered.map(lambda x: (x, x)).reduce(minmax)
            except TypeError as e:
                if " empty " in str(e):
                    raise ValueError("can not generate buckets from empty RDD")
                raise
            except ValueError as e:
                if "empty" in str(e):
                    raise ValueError("can not generate buckets from empty RDD")
                raise

            if minv == maxv or buckets == 1:
                return [minv, maxv], [filtered.count()]

            try:
                inc = (maxv - minv) / buckets
            except TypeError:
                raise TypeError("Can not generate buckets with non-number in RDD")

            if math.isinf(inc):
                raise ValueError("Can not generate buckets with infinite value")

            # keep them as integer if possible
            inc = int(inc)
            if inc * buckets != maxv - minv:
                inc = (maxv - minv) * 1.0 / buckets

            buckets = [i * inc + minv for i in range(buckets)]
            buckets.append(maxv)  # fix accumulated error
            even = True

        elif isinstance(buckets, (list, tuple)):
            if len(buckets) < 2:
                raise ValueError("buckets should have more than one value")

            if any(i is None or isinstance(i, float) and math.isnan(i) for i in buckets):
                raise ValueError("can not have None or NaN in buckets")

            if sorted(buckets) != list(buckets):
                raise ValueError("buckets should be sorted")

            if len(set(buckets)) != len(buckets):
                raise ValueError("buckets should not contain duplicated values")

            minv = buckets[0]
            maxv = buckets[-1]
            even = False
            inc = None
            try:
                steps = [buckets[i + 1] - buckets[i] for i in range(len(buckets) - 1)]
            except TypeError:
                pass  # objects in buckets do not support '-'
            else:
                if max(steps) - min(steps) < 1e-10:  # handle precision errors
                    even = True
                    inc = (maxv - minv) / (len(buckets) - 1)

        else:
            raise TypeError("buckets should be a list or tuple or number(int or long)")

        def histogram(iterator: Iterable[Any]) -> Iterable[List[int]]:
            counters = [0] * len(buckets)
            for i in iterator:
                if i is None or (isinstance(i, float) and math.isnan(i)) or i > maxv or i < minv:
                    continue
                t = int((i - minv) / inc) if even else bisect.bisect_right(buckets, i) - 1
                counters[t] += 1
            # add last two together
            last = counters.pop()
            counters[-1] += last
            return [counters]

        def mergeCounters(a: List[int], b: List[int]) -> List[int]:
            return [i + j for i, j in zip(a, b)]

        return buckets, self.mapPartitions(histogram).reduce(mergeCounters)

    def countByValue(self: "RDD[K]") -> Dict[K, int]:
        def countPartition(iterator: Iterable[K]) -> Iterable[Dict[K, int]]:
            counts: Dict[K, int] = defaultdict(int)
            for obj in iterator:
                counts[obj] += 1
            yield counts

        def mergeMaps(m1: Dict[K, int], m2: Dict[K, int]) -> Dict[K, int]:
            for k, v in m2.items():
                m1[k] += v
            return m1

        return self.mapPartitions(countPartition).reduce(mergeMaps)

    def countByKey(self: "RDD[Tuple[K, V]]") -> Dict[K, int]:
        return self.map(lambda x: x[0]).countByValue()

    def reduceByKeyLocally(self: "RDD[Tuple[K, V]]", func: Callable[[V, V], V]) -> Dict[K, V]:
        func = fail_on_stopiteration(func)

        def reducePartition(iterator: Iterable[Tuple[K, V]]) -> Iterable[Dict[K, V]]:
            m: Dict[K, V] = {}
            for k, v in iterator:
                m[k] = func(m[k], v) if k in m else v
            yield m

        def mergeMaps(m1: Dict[K, V], m2: Dict[K, V]) -> Dict[K, V]:
            for k, v in m2.items():
                m1[k] = func(m1[k], v) if k in m1 else v
            return m1

        return self.mapPartitions(reducePartition).reduce(mergeMaps)

    def top(self: "RDD[T]", num: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        def topIterator(iterator: Iterable[T]) -> Iterable[List[T]]:
            yield heapq.nlargest(num, iterator, key=key)

        def merge(a: List[T], b: List[T]) -> List[T]:
            return heapq.nlargest(num, a + b, key=key)

        return self.mapPartitions(topIterator).reduce(merge)

    def takeOrdered(self: "RDD[T]", num: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        if num < 0:
            raise ValueError("top N cannot be negative.")
        if num == 0:
            return []

        def merge(a: List[T], b: List[T]) -> List[T]:
            return heapq.nsmallest(num, a + b, key)

        partials = self.mapPartitions(lambda it: [heapq.nsmallest(num, it, key)]).collect()
        return reduce(merge, partials) if partials else []

    def takeSample(
        self: "RDD[T]", withReplacement: bool, num: int, seed: Optional[int] = None
    ) -> List[T]:
        numStDev = 10.0
        maxSampleSize = sys.maxsize - int(numStDev * math.sqrt(sys.maxsize))
        if num < 0:
            raise ValueError("Sample size cannot be negative.")
        elif num > maxSampleSize:
            raise ValueError("Sample size cannot be greater than %d." % maxSampleSize)

        if num == 0 or self.getNumPartitions() == 0:
            return []

        initialCount = self.count()
        if initialCount == 0:
            return []

        rand = random.Random(seed)

        if (not withReplacement) and num >= initialCount:
            # shuffle current RDD and return
            samples = self.collect()
            rand.shuffle(samples)
            return samples

        fraction = RDD._computeFractionForSampleSize(num, initialCount, withReplacement)
        samples = self.sample(withReplacement, fraction, seed).collect()

        # If the first sample didn't turn out large enough, keep trying to take samples;
        # this shouldn't happen often because we use a big multiplier for their initial size.
        while len(samples) < num:
            # TODO: add log warning for when more than one iteration was run
            seed = rand.randint(0, sys.maxsize)
            samples = self.sample(withReplacement, fraction, seed).collect()

        rand.shuffle(samples)

        return samples[0:num]

    @staticmethod
    def _computeFractionForSampleSize(
        sampleSizeLowerBound: int, total: int, withReplacement: bool
    ) -> float:
        fraction = float(sampleSizeLowerBound) / total
        if withReplacement:
            numStDev = 5
            if sampleSizeLowerBound < 12:
                numStDev = 9
            return fraction + numStDev * math.sqrt(fraction / total)
        else:
            delta = 0.00005
            gamma = -math.log(delta) / total
            return min(1, fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction))

    def lookup(self: "RDD[Tuple[K, V]]", key: K) -> List[V]:
        values = self.filter(lambda kv: kv[0] == key).values()
        if self.partitioner is not None:
            return self._cctx.runJob(values, lambda x: x, [self.partitioner(key)])
        return values.collect()

    def countApprox(self, timeout: int, confidence: float = 0.95) -> int:
        drdd = self.mapPartitions(lambda it: [float(sum(1 for i in it))])
        return int(drdd.sumApprox(timeout, confidence))

    def sumApprox(
        self: "RDD[Union[float, int]]", timeout: int, confidence: float = 0.95
    ) -> BoundedFloat:
        """
        Approximate sum within ``timeout`` milliseconds. On Spark Connect the estimate is made
        from the partitions finished by then, as Spark's SumEvaluator does, and the rest of the
        job is cancelled.
        """
        stats, total, complete = self._approx(StatCounter, timeout)
        counter = reduce(lambda a, b: a.mergeStats(b), stats, StatCounter())
        if complete:
            s = float(counter.sum())
            return BoundedFloat(s, 1.0, s, s)
        if not stats or counter.count() == 0:
            return BoundedFloat(0.0, 0.0, float("-inf"), float("inf"))
        p = len(stats) / total
        mean_estimate = counter.mean()
        count_estimate = (counter.count() + 1 - p) / p
        sum_estimate = mean_estimate * count_estimate
        mean_var = counter.sampleVariance() / counter.count()
        if counter.count() == 1 or math.isnan(mean_var):
            return BoundedFloat(sum_estimate, 0.0, float("-inf"), float("inf"))
        count_var = (counter.count() + 1) * (1 - p) / (p * p)
        sum_var = (
            mean_estimate * mean_estimate * count_var
            + count_estimate * count_estimate * mean_var
            + mean_var * count_var
        )
        bound = _confidence_factor(confidence, counter.count()) * math.sqrt(sum_var)
        return BoundedFloat(sum_estimate, confidence, sum_estimate - bound, sum_estimate + bound)

    def meanApprox(
        self: "RDD[Union[float, int]]", timeout: int, confidence: float = 0.95
    ) -> BoundedFloat:
        """
        Approximate mean within ``timeout`` milliseconds. On Spark Connect the estimate is made
        from the partitions finished by then, as Spark's MeanEvaluator does, and the rest of the
        job is cancelled.
        """
        stats, _, complete = self._approx(lambda it: StatCounter(map(float, it)), timeout)
        counter = reduce(lambda a, b: a.mergeStats(b), stats, StatCounter())
        if complete:
            m = counter.mean() if counter.count() > 0 else float("nan")
            return BoundedFloat(m, 1.0, m, m)
        if not stats or counter.count() == 0:
            return BoundedFloat(0.0, 0.0, float("-inf"), float("inf"))
        mean = counter.mean()
        if counter.count() == 1:
            return BoundedFloat(mean, confidence, float("-inf"), float("inf"))
        stdev = math.sqrt(counter.sampleVariance() / counter.count())
        bound = _confidence_factor(confidence, counter.count()) * stdev
        return BoundedFloat(mean, confidence, mean - bound, mean + bound)

    def countApproxDistinct(self: "RDD[T]", relativeSD: float = 0.05) -> int:
        from pyspark.sql.connect import functions as F

        if relativeSD < 0.000017:
            raise ValueError("relativeSD should be greater than 0.000017")
        # the hash space in Java is 2^32
        hashes = self.map(lambda x: portable_hash(x) & 0xFFFFFFFF)._run(rt.emit_hashes, "h long")
        row = hashes.agg(F.approx_count_distinct("h", relativeSD)).first()
        return int(row[0]) if row is not None and row[0] is not None else 0

    # -- Output --------------------------------------------------------------------------------

    def saveAsTextFile(self, path: str, compressionCodecClass: Optional[str] = None) -> None:
        """
        Write one line per element, like Spark Classic, through the DataFrame text writer. The
        path must not exist yet.
        """
        from pyspark.sql.connect import functions as F

        def func(_: int, iterator: Iterable[Any]) -> Iterable[bytes]:
            for x in iterator:
                if isinstance(x, (bytes, bytearray)):
                    # Spark Classic decodes bytes elements as UTF-8 on the JVM, replacing
                    # malformed sequences.
                    x = bytes(x).decode("utf-8", "replace")
                elif not isinstance(x, str):
                    x = str(x)
                yield x.encode("utf-8")

        lines = self._derive(func)._run(rt.emit_lines, "value binary")
        writer = lines.select(F.col("value").cast("string").alias("value")).write
        if compressionCodecClass:
            writer = writer.option("compression", compressionCodecClass)
        writer.text(path)

    def saveAsPickleFile(self, path: str, batchSize: int = 10) -> None:
        """
        Save the pickled elements under ``path`` as Parquet, for
        :meth:`SparkContext.pickleFile` on Spark Connect to read back. The files are written by
        the cluster but are not SequenceFiles, so Spark Classic cannot read them, and
        ``batchSize`` is not used: elements are pickled in chunks of adaptive size.
        """
        from pyspark.sql.connect import functions as F

        self._chunk_df().filter(F.col(rt.COUNT_COL) > 0).select(
            F.col(rt.CHUNK_COL).alias("value"), F.col(rt.COUNT_COL).alias("n")
        ).write.parquet(path)

    def saveAsHadoopDataset(
        self: "RDD[Tuple[K, V]]",
        conf: Dict[str, str],
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
    ) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsHadoopDataset on Spark Connect"},
        )

    def saveAsHadoopFile(
        self: "RDD[Tuple[K, V]]",
        path: str,
        outputFormatClass: str,
        keyClass: Optional[str] = None,
        valueClass: Optional[str] = None,
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
        compressionCodecClass: Optional[str] = None,
    ) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsHadoopFile on Spark Connect"},
        )

    def saveAsNewAPIHadoopDataset(
        self: "RDD[Tuple[K, V]]",
        conf: Dict[str, str],
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
    ) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsNewAPIHadoopDataset on Spark Connect"},
        )

    def saveAsNewAPIHadoopFile(
        self: "RDD[Tuple[K, V]]",
        path: str,
        outputFormatClass: str,
        keyClass: Optional[str] = None,
        valueClass: Optional[str] = None,
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
    ) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsNewAPIHadoopFile on Spark Connect"},
        )

    def saveAsSequenceFile(
        self: "RDD[Tuple[K, V]]", path: str, compressionCodecClass: Optional[str] = None
    ) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsSequenceFile on Spark Connect"},
        )

    def toDF(
        self: "RDD[Any]", schema: Optional[Any] = None, sampleRatio: Optional[float] = None
    ) -> "DataFrame":
        """
        Converts this ``RDD`` into a :class:`pyspark.sql.DataFrame` through the Spark Connect
        session of its :class:`~pyspark.core.connect.context.SparkContext`.
        """
        if (
            schema is None
            and sampleRatio is None
            and self._func is None
            and self._source == rt.SOURCE_ROWS
        ):
            # ``df.rdd.toDF()``: the rows already are this DataFrame.
            return self._df
        session = self._cctx._session
        return session.createDataFrame(self, schema, sampleRatio)


class RDDBarrier(RDDBarrierFacade[T]):
    """
    Barrier transformations on Spark Connect: the stage computing them runs through
    ``mapInArrow(..., barrier=True)``.

    .. versionadded:: 5.0.0
    """

    def __init__(self, rdd: RDD[T]) -> None:
        self.rdd = rdd

    def mapPartitions(
        self, f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning: bool = False
    ) -> RDD[Any]:
        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return f(iterator)

        return self.rdd._derive(func, preservesPartitioning, barrier=True)

    def mapPartitionsWithIndex(
        self,
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> RDD[Any]:
        return self.rdd._derive(f, preservesPartitioning, uses_split=True, barrier=True)
