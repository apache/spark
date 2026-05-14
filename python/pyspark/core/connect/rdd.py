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

from __future__ import annotations

import bisect
import copy
import math
import pickle
import random
import sys
import time
import warnings
from functools import cmp_to_key, reduce
from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
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
    no_type_check,
)

import pyarrow as pa

from pyspark import cloudpickle
from pyspark.errors import PySparkNotImplementedError, PySparkRuntimeError, PySparkTypeError
from pyspark.serializers import (
    AutoBatchedSerializer,
    BatchedSerializer,
    CPickleSerializer,
)
from pyspark.storagelevel import StorageLevel
from pyspark.util import portable_hash
from pyspark.sql.dataframe import DataFrame as SparkSqlDataFrame
from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType

from pyspark.sql.connect import functions as F

from pyspark.core.connect.context import SparkContext
from pyspark.core.classic.rdd import BoundedFloat, Partitioner
from pyspark.rddsampler import RDDStratifiedSampler

__all__ = ["RDD", "RDDBarrier"]

T = TypeVar("T")
U = TypeVar("U")

# Internal column storing pickled Python elements (RDD-style element type preserved by bytes).
_PAYLOAD_COL = "__connect_rdd_pickled"
_PICKLED_SCHEMA = StructType([StructField(_PAYLOAD_COL, BinaryType(), True)])
_STABLE_PARTITION_COL = "__stable_partition_id"
_PARTITION_BY_PID_COL = "__connect_partition_by_pid"
_KV_LOOKUP_PID_ALIAS = "__connect_kv_partition_id"
_SORT_PID_COL = "__connect_sort_by_key_pid"
_SORT_TAGGED_SCHEMA = StructType(
    [
        StructField(_PAYLOAD_COL, BinaryType(), True),
        StructField(_SORT_PID_COL, IntegerType(), False),
    ]
)
_PAIR_KV_SCHEMA = StructType(
    [
        StructField("__k", BinaryType(), True),
        StructField("__v", BinaryType(), True),
    ]
)
_PARTIAL_AGG_SCHEMA = StructType(
    [
        StructField("__k", BinaryType(), True),
        StructField("__acc", BinaryType(), True),
    ]
)
_ZIP_LOCAL_SCHEMA_SELF = StructType(
    [
        StructField(_PAYLOAD_COL, BinaryType(), True),
        StructField("__zip_rn", LongType(), False),
    ]
)
_ZIP_LOCAL_SCHEMA_OTHER = StructType(
    [
        StructField("__other_pb", BinaryType(), True),
        StructField("__zip_rn", LongType(), False),
    ]
)


def _loads(b: Optional[bytes]) -> Any:
    if b is None:
        return None
    if isinstance(b, memoryview):
        b = b.tobytes()
    return cloudpickle.loads(b)


def _dumps(obj: Any) -> bytes:
    return cloudpickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def _logical_range_num_elements(start: int, end: int, step: int) -> int:
    """Mirror ``org.apache.spark.sql.catalyst.plans.logical.Range.numElements``."""

    safe_start = int(start)
    safe_end = int(end)
    st = int(step)
    diff = safe_end - safe_start
    if diff % st == 0 or (safe_end > safe_start) != (st > 0):
        return diff // st
    return diff // st + 1


def _range_partition_value_bounds(
    start: int, end: int, step: int, num_slices: int, idx: int
) -> tuple[int, int]:
    """Partition bounds in RangeExec value space (upper bound exclusive)."""

    n = _logical_range_num_elements(start, end, step)
    k = max(1, int(num_slices))
    lo = (idx * n // k) * int(step) + int(start)
    hi_exclusive = ((idx + 1) * n // k) * int(step) + int(start)
    return lo, hi_exclusive


def _encode_pickled_range_df(rg_tagged: SparkSqlDataFrame) -> SparkSqlDataFrame:
    schema_rng = StructType(
        [
            StructField(_PAYLOAD_COL, BinaryType(), True),
            StructField(_STABLE_PARTITION_COL, IntegerType(), False),
        ]
    )

    def enc(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in it:
            ids = batch.column(0).to_pylist()
            pids = batch.column(1).to_pylist()
            blobs = [_dumps(int(x)) for x in ids]
            yield pa.record_batch(
                {
                    _PAYLOAD_COL: pa.array(blobs, type=pa.large_binary()),
                    _STABLE_PARTITION_COL: pa.array(
                        [int(pids[i]) for i in range(len(ids))], type=pa.int32()
                    ),
                }
            )

    return rg_tagged.mapInArrow(enc, schema=schema_rng)


def _apply_pickled_map_chain_df(
    df: SparkSqlDataFrame,
    chain: List[Callable[[Any], Any]],
    stable_col: str = _STABLE_PARTITION_COL,
) -> SparkSqlDataFrame:
    if not chain:
        return df
    schema_dual = StructType(
        [
            StructField(_PAYLOAD_COL, BinaryType(), True),
            StructField(stable_col, IntegerType(), False),
        ]
    )
    src = df.select(F.col(_PAYLOAD_COL), F.col(stable_col))

    def mapper(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in it:
            py_in = batch.column(0).to_pylist()
            pid_in = batch.column(1).to_pylist()
            pid = int(pid_in[0]) if pid_in else 0
            py_out: List[bytes] = []
            for pb in py_in:
                x = _loads(pb)
                for fn in chain:
                    x = fn(x)
                py_out.append(_dumps(x))
            yield pa.record_batch(
                {
                    _PAYLOAD_COL: pa.array(py_out, type=pa.large_binary()),
                    stable_col: pa.array([pid] * len(py_out), type=pa.int32()),
                }
            )

    return src.mapInArrow(mapper, schema=schema_dual)


def _histogram_empty_connect(buckets: Any) -> tuple[Any, list[int]]:
    from math import isnan

    if isinstance(buckets, int):
        raise ValueError("can not generate buckets from empty RDD")

    if isinstance(buckets, (list, tuple)):
        if len(buckets) < 2:
            raise ValueError("buckets should have more than one value")
        if any(i is None or isinstance(i, float) and isnan(i) for i in buckets):
            raise ValueError("can not have None or NaN in buckets")
        if sorted(buckets) != list(buckets):
            raise ValueError("buckets should be sorted")
        if len(set(buckets)) != len(buckets):
            raise ValueError("buckets should not have duplicated values")
        return buckets, [0] * (len(buckets) - 1)

    raise TypeError("buckets should be a list or tuple or number(int or long)")


def _tag_zip_local_rows(
    df: SparkSqlDataFrame, payload_col: str, schema: StructType
) -> SparkSqlDataFrame:
    """Assign deterministic 0..n-1 row indices within each partition (mapInArrow iteration order)."""

    def tag_batches(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        rn_base = 0
        for batch in it:
            n = batch.num_rows
            rns = list(range(rn_base, rn_base + n))
            rn_base += n
            yield pa.record_batch(
                {
                    payload_col: batch.column(0),
                    "__zip_rn": pa.array(rns, type=pa.int64()),
                }
            )

    return df.select(payload_col).mapInArrow(tag_batches, schema=schema)


def _sort_pickled_payload_partitions(
    df: SparkSqlDataFrame, ascending: bool, keyfunc: Callable[[Any], Any]
) -> SparkSqlDataFrame:
    col = _PAYLOAD_COL

    def sort_batches(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        rows: List[Any] = []
        for batch in it:
            for pb in batch.column(0).to_pylist():
                rows.append(_loads(pb))
        rows.sort(key=lambda kv: keyfunc(kv[0]), reverse=not ascending)
        yield pa.record_batch({col: pa.array([_dumps(r) for r in rows], type=pa.large_binary())})

    return df.mapInArrow(sort_batches, schema=_PICKLED_SCHEMA)


def _same_ctx(a: SparkContext, b: SparkContext) -> None:
    if a is not b:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "other.ctx",
                "expected_type": "same SparkContext (pyspark.core.connect.context) as self",
                "arg_type": "different SparkContext (pyspark.core.connect.context)",
            },
        )


def _compute_fraction_for_sample_size(
    sample_size_lower_bound: int, total: int, with_replacement: bool
) -> float:
    """Sampling rate heuristic ported from ``RDD._computeFractionForSampleSize``."""
    fraction = float(sample_size_lower_bound) / total
    if with_replacement:
        num_stdev = 5
        if sample_size_lower_bound < 12:
            num_stdev = 9
        return fraction + num_stdev * math.sqrt(fraction / total)
    delta = 0.00005
    gamma = -math.log(delta) / total
    return min(1.0, fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction))


def _connect_approx_bounds(
    estimate: float,
    *,
    complete: bool,
    num_partitions: int,
    processed_partitions: int,
    confidence: float,
) -> Tuple[float, float]:
    """Return (low, high) for :class:`~pyspark.core.classic.rdd.BoundedFloat` on Connect."""
    if complete:
        return (estimate, estimate)
    remaining = max(num_partitions - processed_partitions, 0)
    span = max(abs(estimate) * (1.0 - confidence) + 1e-9, 1e-9)
    if num_partitions > 0 and remaining > 0:
        span = max(span, abs(estimate) * remaining / num_partitions)
    return (estimate - span, estimate + span)


class _PythonJrddRef:
    """Lightweight handle so ``RDD._jrdd.first()`` parity checks run without Py4J."""

    def __init__(self, rdd: "RDD[Any]") -> None:
        self._rdd = rdd

    def first(self) -> bytes:
        rows = self._rdd.take(1)
        if not rows:
            raise ValueError("Cannot compute head on empty RDD.")
        buf = BytesIO()
        BatchedSerializer(CPickleSerializer(), 1).dump_stream(iter(rows), buf)
        return buf.getvalue()


class RDD(Generic[T]):
    """
    RDD-shaped API backed by Spark Connect :class:`DataFrame` execution.

    .. versionadded:: 5.0.0

    Checkpoint-style APIs are unsupported for now; use :class:`DataFrame` checkpointing APIs
    directly if needed.
    """

    _next_rdd_id = 1

    def __init__(
        self, df: SparkSqlDataFrame, ctx: SparkContext, num_partitions: Optional[int] = None
    ) -> None:
        self._df = df
        self.ctx = ctx
        self._num_partitions_hint = num_partitions
        self._rdd_id: Optional[int] = None
        self._resource_profile: Optional[Any] = None
        self._stable_partition_col: Optional[str] = None
        self._range_meta: Optional[Tuple[int, int, int, int]] = None
        self._range_map_chain: Optional[List[Callable[[Any], Any]]] = None
        self._pickled_map_chain: Optional[List[Callable[[Any], Any]]] = None
        self._barrier_rdd: bool = False
        # Populated after partitionBy(...): aligns with Partitioner for narrowed lookup(...)
        self.partitioner: Optional[Partitioner] = None
        self._kv_partition_id_col: Optional[str] = None

    def _kv_col_active(self) -> Optional[str]:
        col = getattr(self, "_kv_partition_id_col", None)
        if col is None or not getattr(self, "partitioner", None):
            return None
        try:
            names = list(self._df.columns)
        except Exception:
            return None
        return col if col in names else None

    def _sync_kv_partition_metadata_with_schema(self) -> None:
        """Clear partition metadata if the auxiliary column vanished from ``_df``."""
        col = getattr(self, "_kv_partition_id_col", None)
        pv = getattr(self, "partitioner", None)
        if pv is None and col is None:
            return
        if pv is None or col is None:
            self.partitioner = None
            self._kv_partition_id_col = None
            return
        try:
            names = list(self._df.columns)
        except Exception:
            self.partitioner = None
            self._kv_partition_id_col = None
            return
        if col not in names:
            self.partitioner = None
            self._kv_partition_id_col = None

    @property
    def _jrdd(self) -> _PythonJrddRef:
        return _PythonJrddRef(self)

    def _to_java_object_rdd(self) -> Any:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": "RDD._to_java_object_rdd on Spark Connect",
            },
        )

    def __repr__(self) -> str:
        return f"RDD(column={_PAYLOAD_COL!r})"

    def id(self) -> int:
        if self._rdd_id is None:
            self._rdd_id = RDD._next_rdd_id
            RDD._next_rdd_id += 1
        return self._rdd_id

    def __getnewargs__(self) -> NoReturn:
        raise PySparkRuntimeError(
            errorClass="RDD_TRANSFORM_ONLY_VALID_ON_DRIVER",
            messageParameters={},
        )

    @property
    def context(self) -> Any:
        """SparkContext-compatible handle (:class:`~pyspark.core.connect.context.SparkContext`)."""
        return self.ctx

    @no_type_check
    def toDF(self, schema=None, sampleRatio=None) -> SparkSqlDataFrame:
        """
        Converts this ``RDD`` into a :class:`pyspark.sql.DataFrame`.

        Dispatches via the :class:`~pyspark.core.connect.context.SparkContext` session that owns
        this ``RDD``. This avoids a global class-level shim whenever multiple Connect sessions
        exist in one Python process.
        """
        return self.ctx._session.createDataFrame(self, schema, sampleRatio)

    def __add__(self: RDD[T], other: RDD[Any]) -> RDD[Any]:
        if not isinstance(other, RDD):
            raise TypeError
        return self.union(other)

    @staticmethod
    def _computeFractionForSampleSize(
        sampleSizeLowerBound: int, total: int, withReplacement: bool
    ) -> float:
        """See :meth:`RDD._computeFractionForSampleSize`."""

        return _compute_fraction_for_sample_size(sampleSizeLowerBound, total, withReplacement)

    def _memory_limit(self) -> int:
        from pyspark.util import _parse_memory

        val = self.ctx._session.conf.get("spark.python.worker.memory", "512m")
        assert val is not None
        return _parse_memory(val)

    def _defaultReducePartitions(self) -> int:
        if "spark.default.parallelism" in self.ctx._session.conf.getAll:
            return self.ctx.defaultParallelism
        return self.getNumPartitions()

    def _is_barrier(self) -> bool:
        return bool(getattr(self, "_barrier_rdd", False))

    def _fork(
        self,
        df: SparkSqlDataFrame,
        hint: Optional[int],
        *,
        preserve_stable_partition_col: bool = False,
        is_barrier: bool = False,
    ) -> RDD[Any]:
        out: RDD[Any] = RDD(df, self.ctx, hint)
        out._resource_profile = getattr(self, "_resource_profile", None)
        out._barrier_rdd = is_barrier
        if preserve_stable_partition_col:
            sp = getattr(self, "_stable_partition_col", None)
            if sp is not None:
                out._stable_partition_col = sp
        out.partitioner = None
        out._kv_partition_id_col = None
        return out

    def _spawn(
        self,
        df: SparkSqlDataFrame,
        num_partitions: Optional[int] = None,
        *,
        clear_hint: bool = False,
        preserve_stable_partition_col: bool = False,
        is_barrier: bool = False,
        preserve_kv_partition_carry: bool = False,
    ) -> RDD[Any]:
        if clear_hint:
            hint = None
        elif num_partitions is not None:
            hint = num_partitions
        else:
            hint = self._num_partitions_hint
        out = self._fork(
            df,
            hint,
            preserve_stable_partition_col=preserve_stable_partition_col,
            is_barrier=is_barrier,
        )
        if preserve_kv_partition_carry:
            col = getattr(self, "_kv_partition_id_col", None)
            pv = getattr(self, "partitioner", None)
            try:
                dcols = list(df.columns)
            except Exception:
                dcols = []
            if pv is not None and col is not None and col in dcols:
                out.partitioner = pv
                out._kv_partition_id_col = col
        return out

    def _ensure_physical_pickled_elements_inplace(self) -> None:
        self._sync_kv_partition_metadata_with_schema()

        chain = getattr(self, "_range_map_chain", None) or []
        if chain:
            self._df = _apply_pickled_map_chain_df(self._df, chain)
            self._range_map_chain = []

        p_chain = getattr(self, "_pickled_map_chain", None) or []
        if p_chain:
            stable_tag = getattr(self, "_stable_partition_col", None)
            if stable_tag is None:
                raise RuntimeError("_pickled_map_chain requires _stable_partition_col")
            self._df = _apply_pickled_map_chain_df(self._df, p_chain, stable_tag)
            self._pickled_map_chain = []

    def _materialize_pending_range_maps(self: RDD[T]) -> RDD[T]:
        self._ensure_physical_pickled_elements_inplace()
        return self

    def collect(self) -> List[T]:
        self._ensure_physical_pickled_elements_inplace()
        return [_loads(row[0]) for row in self._df.select(_PAYLOAD_COL).collect()]

    def collectWithJobGroup(
        self, groupId: str, description: str, interruptOnCancel: bool = False
    ) -> List[T]:
        warnings.warn(
            "Deprecated in 3.1, Use pyspark.InheritableThread with the pinned thread mode enabled.",
            FutureWarning,
        )
        _ = description, interruptOnCancel
        self.ctx._session.addTag(groupId)
        try:
            return self.collect()
        finally:
            self.ctx._discard_connect_job_tag_if_present(groupId)

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Iterator[T]:
        if prefetchPartitions:
            raise PySparkNotImplementedError(
                errorClass="NOT_IMPLEMENTED",
                messageParameters={
                    "feature": ("RDD.toLocalIterator(prefetchPartitions=True) with Spark Connect"),
                },
            )

        np_hint = self._num_partitions_hint
        if np_hint is None:
            np = self.getNumPartitions()
        else:
            np = int(np_hint)

        stable = getattr(self, "_stable_partition_col", None)
        range_meta = getattr(self, "_range_meta", None)
        chain = getattr(self, "_range_map_chain", None) or []

        replay_maps_on_executor = range_meta is not None and np > 1 and bool(chain)
        pickled_chain_present = bool(getattr(self, "_pickled_map_chain", None) or [])
        needs_driver_materialize = pickled_chain_present or (
            bool(chain) and not replay_maps_on_executor
        )
        if needs_driver_materialize:
            self._ensure_physical_pickled_elements_inplace()

        chain_now = getattr(self, "_range_map_chain", None) or []

        if np <= 1:
            for row in self._df.select(_PAYLOAD_COL).toLocalIterator():
                yield _loads(row[0])
            return

        # Numeric range pipelines: mirror classic RDD lazy scheduling by running each logical
        # slice as its own single-partition Spark Connect job (Spark SQL stages otherwise launch
        # all Python mapInArrow tasks together).
        if range_meta is not None and np > 1:
            start_r, end_r, step_r, num_slices_r = range_meta
            sess = self.ctx._session
            for p in range(np):
                lo, hi_ex = _range_partition_value_bounds(start_r, end_r, step_r, num_slices_r, p)
                if step_r > 0 and lo >= hi_ex:
                    continue
                if step_r < 0 and lo <= hi_ex:
                    continue
                rg_part = sess.range(lo, hi_ex, step_r, numPartitions=1)
                rg_tagged = rg_part.withColumn(_STABLE_PARTITION_COL, F.lit(int(p)))
                encoded = _encode_pickled_range_df(rg_tagged)
                mapped = _apply_pickled_map_chain_df(encoded, chain_now)
                for row in mapped.select(_PAYLOAD_COL).toLocalIterator():
                    yield _loads(row[0])
            return

        # Prefer filtering on the logical partition id tagged at creation time so Catalyst can
        # prune partitions before executing subsequent Python mapInArrow stages.
        if stable is not None:
            for p in range(np):
                part = self._df.filter(F.col(stable) == F.lit(int(p))).select(_PAYLOAD_COL)
                for row in part.toLocalIterator():
                    yield _loads(row[0])
            return

        tagged = self._df.withColumn("__tl_pid", F.spark_partition_id())
        for p in range(np):
            part = tagged.filter(F.col("__tl_pid") == F.lit(int(p))).select(_PAYLOAD_COL)
            for row in part.toLocalIterator():
                yield _loads(row[0])

    def count(self) -> int:
        self._ensure_physical_pickled_elements_inplace()
        return int(self._df.count())

    def take(self, num: int) -> List[T]:
        if num <= 0:
            return []
        self._ensure_physical_pickled_elements_inplace()
        return [_loads(row[0]) for row in self._df.select(_PAYLOAD_COL).limit(num).collect()]

    def first(self) -> T:
        rows = self.take(1)
        if not rows:
            raise ValueError("RDD is empty")
        return rows[0]

    def isEmpty(self) -> bool:
        return self._df.limit(1).count() == 0

    def cache(self) -> RDD[T]:
        self._ensure_physical_pickled_elements_inplace()
        self._df.cache()
        return self

    def persist(self, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER) -> RDD[T]:
        self._ensure_physical_pickled_elements_inplace()
        self._df.persist(storageLevel)
        return self

    def unpersist(self, blocking: bool = False) -> RDD[T]:
        self._df.unpersist(blocking)
        return self

    def map(self: RDD[T], f: Callable[[T], Any], preservesPartitioning: bool = False) -> RDD[Any]:
        meta = getattr(self, "_range_meta", None)
        stable_fast = getattr(self, "_stable_partition_col", None)
        if meta is not None and stable_fast is not None:
            prev = getattr(self, "_range_map_chain", None)
            prev_list: List[Callable[[Any], Any]] = [] if prev is None else list(prev)
            child: RDD[Any] = RDD(self._df, self.ctx, self._num_partitions_hint)
            child._stable_partition_col = stable_fast
            child._range_meta = meta
            child._range_map_chain = prev_list + [f]
            child._resource_profile = getattr(self, "_resource_profile", None)
            return child

        col = _PAYLOAD_COL
        stable = getattr(self, "_stable_partition_col", None)

        if stable:
            prev_pickled = getattr(self, "_pickled_map_chain", None)
            prev_pickled_list: List[Callable[[Any], Any]] = (
                [] if prev_pickled is None else list(prev_pickled)
            )
            child_stable: RDD[Any] = RDD(self._df, self.ctx, self._num_partitions_hint)
            child_stable._stable_partition_col = stable
            child_stable._pickled_map_chain = prev_pickled_list + [f]
            child_stable._resource_profile = getattr(self, "_resource_profile", None)
            return child_stable

        kv_col = self._kv_col_active()
        if kv_col:
            schema_kv = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(kv_col, IntegerType(), False),
                ]
            )

            def mapper_kv(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                for batch in it:
                    py_in = batch.column(0).to_pylist()
                    pid_in = batch.column(1).to_pylist()
                    pid = int(pid_in[0]) if pid_in else 0
                    py_out = [_dumps(f(_loads(x))) for x in py_in]
                    yield pa.record_batch(
                        {
                            col: pa.array(py_out, type=pa.large_binary()),
                            kv_col: pa.array([pid] * len(py_out), type=pa.int32()),
                        }
                    )

            src_kv = self._df.select(F.col(_PAYLOAD_COL), F.col(kv_col))
            out_kv = src_kv.mapInArrow(mapper_kv, schema=schema_kv)
            return self._spawn(out_kv, preserve_kv_partition_carry=True)

        def mapper_plain(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                py_in = batch.column(0).to_pylist()
                py_out = [_dumps(f(_loads(x))) for x in py_in]
                arrow_out = pa.array(py_out, type=pa.large_binary())
                yield pa.record_batch({col: arrow_out})

        out = self._df.mapInArrow(mapper_plain, schema=_PICKLED_SCHEMA)
        return self._spawn(out)

    def filter(self: RDD[T], f: Callable[[T], bool]) -> RDD[T]:
        base = self._materialize_pending_range_maps()
        col = _PAYLOAD_COL
        stable = getattr(base, "_stable_partition_col", None)

        if stable:
            schema_dual = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(stable, IntegerType(), False),
                ]
            )

            def flt_dual(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                for batch in it:
                    py_in = batch.column(0).to_pylist()
                    pid_in = batch.column(1).to_pylist()
                    pid = int(pid_in[0]) if pid_in else 0
                    kept = [b for b in py_in if f(_loads(b))]
                    yield pa.record_batch(
                        {
                            col: pa.array(kept, type=pa.large_binary()),
                            stable: pa.array([pid] * len(kept), type=pa.int32()),
                        }
                    )

            src = base._df.select(F.col(_PAYLOAD_COL), F.col(stable))
            out = src.mapInArrow(flt_dual, schema=schema_dual)
            return base._spawn(out, preserve_stable_partition_col=True)

        kv_col_f = base._kv_col_active()
        if kv_col_f:
            schema_kv_f = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(kv_col_f, IntegerType(), False),
                ]
            )

            def flt_kv(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                for batch in it:
                    py_in = batch.column(0).to_pylist()
                    pid_in = batch.column(1).to_pylist()
                    pid = int(pid_in[0]) if pid_in else 0
                    kept = [b for b in py_in if f(_loads(b))]
                    yield pa.record_batch(
                        {
                            col: pa.array(kept, type=pa.large_binary()),
                            kv_col_f: pa.array([pid] * len(kept), type=pa.int32()),
                        }
                    )

            src_kv_f = base._df.select(F.col(_PAYLOAD_COL), F.col(kv_col_f))
            out_kv_f = src_kv_f.mapInArrow(flt_kv, schema=schema_kv_f)
            return base._spawn(out_kv_f, preserve_kv_partition_carry=True)

        def flt_plain(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                py_in = batch.column(0).to_pylist()
                kept = [b for b in py_in if f(_loads(b))]
                arrow_out = pa.array(kept, type=pa.large_binary())
                yield pa.record_batch({col: arrow_out})

        out = base._df.mapInArrow(flt_plain, schema=_PICKLED_SCHEMA)
        return base._spawn(out)

    def flatMap(
        self: RDD[T],
        f: Callable[[T], Iterable[Any]],
        preservesPartitioning: bool = False,
    ) -> RDD[Any]:
        base = self._materialize_pending_range_maps()
        col = _PAYLOAD_COL
        stable = getattr(base, "_stable_partition_col", None)

        if stable:
            schema_dual = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(stable, IntegerType(), False),
                ]
            )

            def fm_dual(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                for batch in it:
                    py_in = batch.column(0).to_pylist()
                    pid_in = batch.column(1).to_pylist()
                    pid = int(pid_in[0]) if pid_in else 0
                    out_bs: List[bytes] = []
                    for pb in py_in:
                        for y in f(_loads(pb)):
                            out_bs.append(_dumps(y))
                    yield pa.record_batch(
                        {
                            col: pa.array(out_bs, type=pa.large_binary()),
                            stable: pa.array([pid] * len(out_bs), type=pa.int32()),
                        }
                    )

            src = base._df.select(F.col(_PAYLOAD_COL), F.col(stable))
            ndf = src.mapInArrow(fm_dual, schema=schema_dual)
            return base._spawn(ndf, preserve_stable_partition_col=True)

        kv_col_fm = base._kv_col_active()
        if kv_col_fm:
            schema_kv_fm = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(kv_col_fm, IntegerType(), False),
                ]
            )

            def fm_kv(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                for batch in it:
                    py_in = batch.column(0).to_pylist()
                    pid_in = batch.column(1).to_pylist()
                    pid = int(pid_in[0]) if pid_in else 0
                    out_bs: List[bytes] = []
                    for pb in py_in:
                        for y in f(_loads(pb)):
                            out_bs.append(_dumps(y))
                    yield pa.record_batch(
                        {
                            col: pa.array(out_bs, type=pa.large_binary()),
                            kv_col_fm: pa.array([pid] * len(out_bs), type=pa.int32()),
                        }
                    )

            src_kv_fm = base._df.select(F.col(_PAYLOAD_COL), F.col(kv_col_fm))
            ndf_kv = src_kv_fm.mapInArrow(fm_kv, schema=schema_kv_fm)
            return base._spawn(ndf_kv, preserve_kv_partition_carry=True)

        def fm_plain(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                py_in = batch.column(0).to_pylist()
                out_bs: List[bytes] = []
                for pb in py_in:
                    for y in f(_loads(pb)):
                        out_bs.append(_dumps(y))
                arrow_out = pa.array(out_bs, type=pa.large_binary())
                yield pa.record_batch({col: arrow_out})

        ndf = base._df.mapInArrow(fm_plain, schema=_PICKLED_SCHEMA)
        return base._spawn(ndf)

    def mapValues(self: RDD[Any], f: Callable[[Any], Any]) -> RDD[Any]:
        def mv(kv: Any) -> Any:
            if not isinstance(kv, tuple) or len(kv) != 2:
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": "element",
                        "expected_type": "tuple key-value pair",
                        "arg_type": type(kv).__name__,
                    },
                )
            return (kv[0], f(kv[1]))

        return self.map(mv)

    def flatMapValues(self: RDD[Any], f: Callable[[Any], Iterable[Any]]) -> RDD[Any]:
        def fm(kv: Any) -> Iterable[Any]:
            if not isinstance(kv, tuple) or len(kv) != 2:
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": "element",
                        "expected_type": "tuple key-value pair",
                        "arg_type": type(kv).__name__,
                    },
                )
            return ((kv[0], x) for x in f(kv[1]))

        return self.flatMap(fm)

    def keys(self: RDD[Any]) -> RDD[Any]:
        def k(kv: Any) -> Any:
            if not isinstance(kv, tuple) or len(kv) != 2:
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": "element",
                        "expected_type": "tuple key-value pair",
                        "arg_type": type(kv).__name__,
                    },
                )
            return kv[0]

        return self.map(k)

    def values(self: RDD[Any]) -> RDD[Any]:
        def v(kv: Any) -> Any:
            if not isinstance(kv, tuple) or len(kv) != 2:
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": "element",
                        "expected_type": "tuple key-value pair",
                        "arg_type": type(kv).__name__,
                    },
                )
            return kv[1]

        return self.map(v)

    def collectAsMap(self: RDD[Any]) -> Dict[Any, Any]:
        """Return ``dict(self.collect())`` for small pair RDD results (classic parity)."""

        return dict(self.collect())

    def keyBy(self: RDD[T], f: Callable[[T], Any]) -> RDD[Any]:
        return self.map(lambda x: (f(x), x))

    def groupBy(
        self,
        f: Callable[[T], Any],
        numPartitions: Optional[int] = None,
        partitionFunc: Optional[Callable[[Any], int]] = None,
    ) -> RDD[Any]:
        pf = portable_hash if partitionFunc is None else partitionFunc
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions, pf)

    def sortBy(
        self,
        keyfunc: Callable[[T], Any],
        ascending: bool = True,
        numPartitions: Optional[int] = None,
    ) -> RDD[T]:
        return self.keyBy(keyfunc).sortByKey(ascending, numPartitions).values()

    def union(self: RDD[T], other: RDD[Any]) -> RDD[Any]:
        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        base_self = self._materialize_pending_range_maps()
        base_other = other._materialize_pending_range_maps()
        left = base_self._df.select(_PAYLOAD_COL)
        right = base_other._df.select(_PAYLOAD_COL)
        u = left.union(right)
        rd = base_self._spawn(u, clear_hint=True)
        if self is other:
            hint = base_self._num_partitions_hint
            if hint is not None:
                rd = rd.coalesce(hint)
        return rd

    def zip(self: RDD[T], other: RDD[Any]) -> RDD[tuple]:
        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        self._materialize_pending_range_maps()
        other._materialize_pending_range_maps()
        if self.getNumPartitions() != other.getNumPartitions():
            raise ValueError(
                "Can only zip RDDs with same number of partitions, but got {} and {}".format(
                    self.getNumPartitions(), other.getNumPartitions()
                )
            )

        tw = _tag_zip_local_rows(
            self._df.select(F.col(_PAYLOAD_COL)), _PAYLOAD_COL, _ZIP_LOCAL_SCHEMA_SELF
        )
        tw = tw.withColumn("__zip_pid", F.spark_partition_id())
        ow = _tag_zip_local_rows(
            other._df.select(F.col(_PAYLOAD_COL).alias("__other_pb")),
            "__other_pb",
            _ZIP_LOCAL_SCHEMA_OTHER,
        )
        ow = ow.withColumn("__zip_pid", F.spark_partition_id())
        joined = tw.join(ow, ["__zip_pid", "__zip_rn"], "inner")
        expected = int(self.count())
        if int(other.count()) != expected:
            raise RuntimeError("Can only zip RDDs with same number of elements in each partition")
        zcnt = int(joined.count())
        if zcnt != expected:
            raise RuntimeError("Can only zip RDDs with same number of elements in each partition")

        def pack_pb(a: Optional[bytes], b: Optional[bytes]) -> bytes:
            return _dumps((_loads(a), _loads(b)))

        pack_expr: Any = F.udf(pack_pb, BinaryType())
        nz = joined.select(pack_expr(F.col(_PAYLOAD_COL), F.col("__other_pb")).alias(_PAYLOAD_COL))
        return self._spawn(nz)

    def cartesian(self: RDD[T], other: RDD[Any]) -> RDD[tuple]:
        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        self._materialize_pending_range_maps()
        other._materialize_pending_range_maps()
        left = self._df.select(F.col(_PAYLOAD_COL).alias("__l"))
        right = other._df.select(F.col(_PAYLOAD_COL).alias("__r"))
        cross = left.crossJoin(right)

        def pack_pb(a: Optional[bytes], b: Optional[bytes]) -> bytes:
            return _dumps((_loads(a), _loads(b)))

        pack_expr: Any = F.udf(pack_pb, BinaryType())
        nz = cross.select(pack_expr(F.col("__l"), F.col("__r")).alias(_PAYLOAD_COL))
        return self._spawn(nz, clear_hint=True)

    def distinct(self: RDD[T], numPartitions: Optional[int] = None) -> RDD[T]:
        self._materialize_pending_range_maps()
        d = self._df.select(_PAYLOAD_COL).distinct()
        if numPartitions is not None:
            n = int(numPartitions)
            d = d.repartition(n)
            return self._fork(d, n)
        return self._spawn(d)

    def sortByKey(
        self: RDD[T],
        ascending: Optional[bool] = True,
        numPartitions: Optional[int] = None,
        keyfunc: Callable[[Any], Any] = lambda x: x,
    ) -> RDD[tuple]:
        """
        Sort (key, value) pairs using the same strategy as :meth:`RDD.sortByKey`:
        optional single-partition coalesce, else range-partition by sampled key bounds,
        shuffle with :meth:`DataFrame.repartitionById`, then sort within each partition.

        Only sample keys are collected on the driver (same bound as classic PySpark).
        """

        asc = ascending if ascending is not None else True
        self._ensure_physical_pickled_elements_inplace()
        np = numPartitions if numPartitions is not None else max(1, self.ctx.defaultParallelism)

        if np == 1:
            sdf = self._df.select(_PAYLOAD_COL)
            if self.getNumPartitions() > 1:
                sdf = sdf.coalesce(1)
            return self._fork(_sort_pickled_payload_partitions(sdf, asc, keyfunc), 1)

        rdd_size = self.count()
        if not rdd_size:
            return self._fork(self._df.select(_PAYLOAD_COL), self._num_partitions_hint)

        max_sample_size = np * 20.0
        fraction = min(max_sample_size / max(rdd_size, 1), 1.0)
        samples = (
            self.sample(False, fraction, 1).map(lambda kv: cast(Tuple[Any, Any], kv)[0]).collect()
        )
        samples = sorted(samples, key=keyfunc)
        bounds = [samples[int(len(samples) * (i + 1) / np)] for i in range(0, np - 1)]

        def tag_batches(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                col0 = batch.column(0).to_pylist()
                pids: List[int] = []
                for pb in col0:
                    kv = _loads(pb)
                    k = kv[0]
                    p = bisect.bisect_left(bounds, keyfunc(k))
                    if not asc:
                        p = np - 1 - p
                    pids.append(int(p))
                yield pa.record_batch(
                    {
                        _PAYLOAD_COL: batch.column(0),
                        _SORT_PID_COL: pa.array(pids, type=pa.int32()),
                    }
                )

        tagged = self._df.select(_PAYLOAD_COL).mapInArrow(tag_batches, schema=_SORT_TAGGED_SCHEMA)
        shuffled = tagged.repartitionById(np, _SORT_PID_COL)
        sorted_df = _sort_pickled_payload_partitions(shuffled.select(_PAYLOAD_COL), asc, keyfunc)
        return self._fork(sorted_df, np)

    def treeReduce(self, f: Callable[[T, T], T], depth: int = 2) -> T:
        if depth < 1:
            raise ValueError("Depth cannot be smaller than 1 but got %d." % depth)

        def combine_partition(iterator: Iterator[Any]) -> Iterator[Any]:
            it = iter(iterator)
            try:
                acc = next(it)
            except StopIteration:
                return
            for x in it:
                acc = f(acc, x)
            yield acc

        partial = self.mapPartitions(combine_partition).collect()
        if not partial:
            raise ValueError("Cannot reduce empty RDD.")
        vals: List[Any] = list(partial)
        scale = max(int(math.ceil(len(vals) ** (1.0 / depth))), 2)
        while len(vals) > 1:
            chunk_size = min(scale, len(vals))
            nxt: List[Any] = []
            for i in range(0, len(vals), chunk_size):
                chunk = vals[i : i + chunk_size]
                acc = chunk[0]
                for x in chunk[1:]:
                    acc = f(acc, x)
                nxt.append(acc)
            vals = nxt
        return vals[0]

    def repartition(self, numPartitions: int) -> RDD[T]:
        self._materialize_pending_range_maps()
        n = int(numPartitions)
        if n <= 0:
            raise ValueError("Number of partitions must be positive.")
        return self._fork(self._df.repartition(n), n)

    def coalesce(self, numPartitions: int, shuffle: bool = False) -> RDD[T]:
        self._materialize_pending_range_maps()
        n = int(numPartitions)
        if n <= 0:
            raise ValueError("Number of partitions must be positive.")
        if shuffle:
            return self.repartition(n)
        return self._fork(self._df.coalesce(n), n)

    def sample(
        self: RDD[T],
        withReplacement: bool,
        fraction: float,
        seed: Optional[int] = None,
    ) -> RDD[T]:
        if fraction < 0:
            raise ValueError("Fraction must be nonnegative.")
        self._materialize_pending_range_maps()
        sdf = self._df.select(_PAYLOAD_COL)
        samp = sdf.sample(withReplacement=withReplacement, fraction=fraction, seed=seed)
        return self._spawn(samp)

    def foreach(self, f: Callable[[T], None]) -> None:
        from pyspark.util import fail_on_stopiteration

        ff = fail_on_stopiteration(f)
        self._ensure_physical_pickled_elements_inplace()
        self._df.select(_PAYLOAD_COL).foreach(lambda row: ff(_loads(row[0])))

    def foreachPartition(self, f: Callable[[Iterator[T]], None]) -> None:
        from pyspark.util import fail_on_stopiteration

        ff = fail_on_stopiteration(f)

        def g(partition_rows: Iterator[Any]) -> None:
            objs = (_loads(row[0]) for row in partition_rows)
            ff(objs)

        self._materialize_pending_range_maps()
        self._df.select(_PAYLOAD_COL).foreachPartition(g)

    def reduce(self, f: Callable[[T, T], T]) -> T:
        """
        Left-fold over elements using ``f``.

        Unlike the classic RDD implementation, this streams rows via ``toLocalIterator`` and
        folds on the driver sequentially. The combine order follows that stream (not a
        distributed tree reduce), so non-commutative ``f`` results can differ from the JVM RDD.
        """
        from pyspark.util import fail_on_stopiteration

        self._ensure_physical_pickled_elements_inplace()
        func = fail_on_stopiteration(f)
        it = (_loads(row[0]) for row in self._df.select(_PAYLOAD_COL).toLocalIterator())
        try:
            acc = next(it)
        except StopIteration:
            raise ValueError("Cannot reduce empty RDD.")
        for x in it:
            acc = func(acc, x)
        return acc

    def zipWithIndex(self: RDD[T], indexColName: str = "index") -> RDD[tuple]:
        base = self._materialize_pending_range_maps()
        zo = base._df.zipWithIndex(indexColName=indexColName)

        def pack_pb(pb: Optional[bytes], ix: Optional[int]) -> bytes:
            return _dumps((_loads(pb), 0 if ix is None else int(ix)))

        pack_expr: Any = F.udf(pack_pb, BinaryType())
        nz = zo.select(pack_expr(F.col(_PAYLOAD_COL), F.col(indexColName)).alias(_PAYLOAD_COL))
        return base._spawn(nz)

    def zipWithUniqueId(self: RDD[T]) -> RDD[tuple]:
        base = self._materialize_pending_range_maps()
        tmp = base._df.withColumn("__connect_uid", F.monotonically_increasing_id())

        def pack(pb: Optional[bytes], uid: Optional[int]) -> bytes:
            return _dumps((_loads(pb), 0 if uid is None else int(uid)))

        pack_expr: Any = F.udf(pack, BinaryType())
        nz = tmp.select(pack_expr(F.col(_PAYLOAD_COL), F.col("__connect_uid")).alias(_PAYLOAD_COL))
        return base._spawn(nz)

    def getNumPartitions(self) -> int:
        """
        Partition count for this logical RDD.

        When the RDD was constructed with an explicit partition count (for example
        :meth:`SparkContext.parallelize <pyspark.core.connect.context.SparkContext.parallelize>` /
        :meth:`SparkContext.range <pyspark.core.connect.context.SparkContext.range>`), that hint is
        returned without executing a Spark job.

        Otherwise this falls back to detecting emptiness or counting distinct
        ``spark_partition_id()`` values.
        """
        if self._num_partitions_hint is not None:
            return int(self._num_partitions_hint)
        if self.isEmpty():
            return 0
        return int(self._df.select(F.spark_partition_id()).distinct().count())

    def barrier(self: RDD[T]) -> Any:
        """Mark following narrow transforms as a Spark Connect barrier map stage.

        Transforms executed through :class:`~pyspark.core.connect.rdd.RDDBarrier` use
        :meth:`pyspark.sql.connect.dataframe.DataFrame.mapInArrow` with ``barrier=True``, which sets
        ``MapPartitions.is_barrier`` on the server (same knob as Scala barrier RDD transforms).

        .. versionadded:: 5.0.0

        Returns
        -------
        :class:`~pyspark.core.connect.rdd.RDDBarrier`

        Notes
        -----
        This API is experimental.
        """
        return RDDBarrier(self)

    def _map_partitions_in_arrow(
        self: RDD[T],
        f: Callable[[Iterator[T]], Iterator[Any]],
        preservesPartitioning: bool,
        map_in_arrow_barrier: bool,
    ) -> RDD[Any]:
        base = self._materialize_pending_range_maps()
        col = _PAYLOAD_COL
        stable = getattr(base, "_stable_partition_col", None)

        if stable:
            schema_dual = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(stable, IntegerType(), False),
                ]
            )
            src = base._df.select(F.col(_PAYLOAD_COL), F.col(stable))

            def mp_dual(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                pid_use = 0
                seen_pid = False

                def elem_iter() -> Iterator[Any]:
                    nonlocal pid_use, seen_pid
                    for batch in it:
                        payloads = batch.column(0).to_pylist()
                        pids = batch.column(1).to_pylist()
                        if not seen_pid and pids:
                            pid_use = int(pids[0])
                            seen_pid = True
                        for x in payloads:
                            yield _loads(x)

                out_bs = [_dumps(y) for y in f(elem_iter())]
                yield pa.record_batch(
                    {
                        col: pa.array(out_bs, type=pa.large_binary()),
                        stable: pa.array([pid_use] * len(out_bs), type=pa.int32()),
                    }
                )

            out = src.mapInArrow(mp_dual, schema=schema_dual, barrier=map_in_arrow_barrier)
            return base._spawn(
                out,
                preserve_stable_partition_col=True,
                is_barrier=map_in_arrow_barrier,
            )

        def mp_plain(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            def elem_iter() -> Iterator[Any]:
                for batch in it:
                    for x in batch.column(0).to_pylist():
                        yield _loads(x)

            out_bs = [_dumps(y) for y in f(elem_iter())]
            yield pa.record_batch({col: pa.array(out_bs, type=pa.large_binary())})

        out = base._df.mapInArrow(mp_plain, schema=_PICKLED_SCHEMA, barrier=map_in_arrow_barrier)
        return base._spawn(out, is_barrier=map_in_arrow_barrier)

    def mapPartitions(
        self: RDD[T],
        f: Callable[[Iterator[T]], Iterator[Any]],
        preservesPartitioning: bool = False,
    ) -> RDD[Any]:
        return self._map_partitions_in_arrow(f, preservesPartitioning, map_in_arrow_barrier=False)

    def _map_partitions_with_index_in_arrow(
        self,
        f: Callable[[int, Iterator[T]], Iterator[Any]],
        preservesPartitioning: bool,
        map_in_arrow_barrier: bool,
    ) -> RDD[Any]:
        base = self._materialize_pending_range_maps()
        stable = getattr(base, "_stable_partition_col", None)
        col = _PAYLOAD_COL

        if stable:
            wp = base._df.select(F.col(_PAYLOAD_COL), F.col(stable))
            schema_out = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(stable, IntegerType(), False),
                ]
            )
        else:
            wp = base._df.select(
                F.col(_PAYLOAD_COL),
                F.spark_partition_id().alias("__connect_pid"),
            )
            schema_out = _PICKLED_SCHEMA

        def mpi(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            pid_val: Optional[int] = None
            blobs: List[Optional[bytes]] = []
            for batch in it:
                blobs_col = batch.column(0).to_pylist()
                pids_col = batch.column(1).to_pylist()
                for i in range(len(blobs_col)):
                    if pid_val is None:
                        pid_val = int(pids_col[i])
                    blobs.append(blobs_col[i])
            split_index = pid_val if pid_val is not None else 0

            def elem_iter() -> Iterator[Any]:
                for b in blobs:
                    yield _loads(cast(bytes, b))

            out_bs = [_dumps(y) for y in f(split_index, elem_iter())]
            if stable:
                yield pa.record_batch(
                    {
                        col: pa.array(out_bs, type=pa.large_binary()),
                        stable: pa.array([split_index] * len(out_bs), type=pa.int32()),
                    }
                )
            else:
                yield pa.record_batch({col: pa.array(out_bs, type=pa.large_binary())})

        out = wp.mapInArrow(mpi, schema=schema_out, barrier=map_in_arrow_barrier)
        return base._spawn(
            out,
            preserve_stable_partition_col=bool(stable),
            is_barrier=map_in_arrow_barrier,
        )

    def mapPartitionsWithIndex(
        self,
        f: Callable[[int, Iterator[T]], Iterator[Any]],
        preservesPartitioning: bool = False,
    ) -> RDD[Any]:
        return self._map_partitions_with_index_in_arrow(
            f, preservesPartitioning, map_in_arrow_barrier=False
        )

    def glom(self: RDD[T]) -> RDD[List[T]]:
        """
        Return an RDD created by coalescing all elements within each partition into a list.
        """

        base = self._materialize_pending_range_maps()
        col = _PAYLOAD_COL
        stable = getattr(base, "_stable_partition_col", None)

        if stable:
            schema_dual = StructType(
                [
                    StructField(_PAYLOAD_COL, BinaryType(), True),
                    StructField(stable, IntegerType(), False),
                ]
            )
            src = base._df.select(F.col(_PAYLOAD_COL), F.col(stable))

            def gl_dual(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                acc: List[Any] = []
                pid_use = 0
                seen_pid = False
                for batch in it:
                    payloads = batch.column(0).to_pylist()
                    pids = batch.column(1).to_pylist()
                    if not seen_pid and pids:
                        pid_use = int(pids[0])
                        seen_pid = True
                    for x in payloads:
                        acc.append(_loads(x))
                yield pa.record_batch(
                    {
                        col: pa.array([_dumps(acc)], type=pa.large_binary()),
                        stable: pa.array([pid_use], type=pa.int32()),
                    }
                )

            out = src.mapInArrow(gl_dual, schema=schema_dual)
            return base._spawn(out, preserve_stable_partition_col=True)

        def gl_plain(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            acc: List[Any] = []
            for batch in it:
                for x in batch.column(0).to_pylist():
                    acc.append(_loads(x))
            yield pa.record_batch({col: pa.array([_dumps(acc)], type=pa.large_binary())})

        out = base._df.mapInArrow(gl_plain, schema=_PICKLED_SCHEMA)
        return base._spawn(out)

    def fold(self: RDD[T], zeroValue: T, op: Callable[[T, T], T]) -> T:
        from pyspark.util import fail_on_stopiteration

        op = fail_on_stopiteration(op)

        def seq(iterator: Iterator[Any]) -> Iterator[T]:
            acc = copy.deepcopy(zeroValue)
            for obj in iterator:
                acc = op(acc, obj)
            yield acc

        vals = self.mapPartitions(seq).collect()
        return reduce(op, vals, zeroValue)

    def aggregate(
        self: RDD[T],
        zeroValue: U,
        seqOp: Callable[[U, T], U],
        combOp: Callable[[U, U], U],
    ) -> U:
        from pyspark.util import fail_on_stopiteration

        seqOp = fail_on_stopiteration(seqOp)
        combOp = fail_on_stopiteration(combOp)

        def seq(iterator: Iterator[Any]) -> Iterator[U]:
            acc = copy.deepcopy(zeroValue)
            for obj in iterator:
                acc = seqOp(acc, obj)
            yield acc

        vals = self.mapPartitions(seq).collect()
        return reduce(combOp, vals, zeroValue)

    def treeAggregate(
        self: RDD[T],
        zeroValue: U,
        seqOp: Callable[[U, T], U],
        combOp: Callable[[U, U], U],
        depth: int = 2,
    ) -> U:
        """Multi-level combine on the driver over per-partition aggregates (Spark SQL partitions)."""

        if depth < 1:
            raise ValueError("Depth cannot be smaller than 1 but got %d." % depth)
        if self.getNumPartitions() == 0:
            return zeroValue

        def aggregate_partition(iterator: Iterator[Any]) -> Iterator[U]:
            acc = copy.deepcopy(zeroValue)
            for obj in iterator:
                acc = seqOp(acc, obj)
            yield acc

        vals = list(self.mapPartitions(aggregate_partition).collect())
        if not vals:
            return zeroValue

        scale = max(int(math.ceil(len(vals) ** (1.0 / depth))), 2)
        while len(vals) > 1:
            chunk_size = min(scale, len(vals))
            nxt: List[U] = []
            for i in range(0, len(vals), chunk_size):
                chunk = vals[i : i + chunk_size]
                acc = chunk[0]
                for v in chunk[1:]:
                    acc = combOp(acc, v)
                nxt.append(acc)
            vals = nxt
        return vals[0]

    def sum(self: RDD[T]) -> Union[int, float]:
        """Sum elements using partition-wise sums combined with ``fold``."""

        def parts(it: Iterator[Any]) -> Iterator[Union[int, float]]:
            yield sum(it)

        return self.mapPartitions(parts).fold(0, lambda a, b: a + b)

    def mean(self: RDD[Any]) -> float:
        return self.stats().mean()

    def variance(self: RDD[Any]) -> float:
        return self.stats().variance()

    def stdev(self: RDD[Any]) -> float:
        return self.stats().stdev()

    def sampleVariance(self: RDD[Any]) -> float:
        return self.stats().sampleVariance()

    def sampleStdev(self: RDD[Any]) -> float:
        return self.stats().sampleStdev()

    def stats(self: RDD[Any]) -> Any:
        """Aggregate numeric summaries using partition-local :class:`~pyspark.statcounter.StatCounter`."""

        from pyspark.statcounter import StatCounter

        def red_func(left_counter: StatCounter, right_counter: StatCounter) -> StatCounter:
            return left_counter.mergeStats(right_counter)

        def ctr(it: Iterator[Any]) -> Iterator[StatCounter]:
            yield StatCounter(it)

        return self.mapPartitions(ctr).reduce(red_func)

    def min(self: RDD[T], key: Optional[Callable[[T], Any]] = None) -> T:
        if key is None:
            pair_min = cast(Callable[[T, T], T], min)
            return self.reduce(pair_min)
        return self.reduce(lambda a, b: min(a, b, key=key))

    def max(self: RDD[T], key: Optional[Callable[[T], Any]] = None) -> T:
        if key is None:
            pair_max = cast(Callable[[T, T], T], max)
            return self.reduce(pair_max)
        return self.reduce(lambda a, b: max(a, b, key=key))

    def takeOrdered(self: RDD[T], num: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        if num <= 0:
            return []
        rows = self.collect()
        rows.sort(key=key)
        return rows[:num]

    def top(self: RDD[T], num: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        if num <= 0:
            return []
        rows = self.collect()
        if key is None:
            rows.sort(reverse=True)
        else:
            rows.sort(key=key, reverse=True)
        return rows[:num]

    def takeSample(
        self: RDD[T], withReplacement: bool, num: int, seed: Optional[int] = None
    ) -> List[T]:
        """Driver collects sampled partitions (same spirit as classic ``RDD.takeSample``)."""
        num_stdev = 10.0
        max_sample_size = sys.maxsize - int(num_stdev * math.sqrt(sys.maxsize))
        if num < 0:
            raise ValueError("Sample size cannot be negative.")
        if num > max_sample_size:
            raise ValueError("Sample size cannot be greater than %d." % max_sample_size)

        if num == 0 or self.getNumPartitions() == 0:
            return []

        initial_count = self.count()
        if initial_count == 0:
            return []

        rand = random.Random(seed)

        if (not withReplacement) and num >= initial_count:
            samples = self.collect()
            rand.shuffle(samples)
            return samples

        fraction = _compute_fraction_for_sample_size(num, initial_count, withReplacement)
        samples = self.sample(withReplacement, fraction, seed).collect()

        while len(samples) < num:
            seed = rand.randint(0, sys.maxsize)
            samples = self.sample(withReplacement, fraction, seed).collect()

        rand.shuffle(samples)
        return samples[:num]

    def intersection(self: RDD[T], other: RDD[T]) -> RDD[T]:
        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        self._materialize_pending_range_maps()
        other._materialize_pending_range_maps()
        inter = self._df.select(_PAYLOAD_COL).intersect(other._df.select(_PAYLOAD_COL))
        return self._spawn(inter, clear_hint=True)

    def subtractByKey(
        self: RDD[Any],
        other: RDD[Any],
        numPartitions: Optional[int] = None,
    ) -> RDD[Any]:
        def filter_func(pair: Tuple[Any, Tuple[Any, Any]]) -> bool:
            key, (val1, val2) = pair
            return bool(val1) and not bool(val2)

        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)

        return self.cogroup(other, numPartitions).filter(filter_func).flatMapValues(lambda x: x[0])

    def subtract(self: RDD[T], other: RDD[Any]) -> RDD[T]:
        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        self._materialize_pending_range_maps()
        other._materialize_pending_range_maps()
        sub = self._df.select(_PAYLOAD_COL).subtract(other._df.select(_PAYLOAD_COL))
        return self._spawn(sub, clear_hint=True)

    def randomSplit(
        self: RDD[T],
        weights: List[float],
        seed: Optional[int] = None,
    ) -> List[RDD[T]]:
        self._materialize_pending_range_maps()
        dfs = self._df.select(_PAYLOAD_COL).randomSplit(weights, seed)
        return [self._fork(d, None) for d in dfs]

    def histogram(
        self,
        buckets: Union[int, List[Any], Tuple[Any, ...]],
    ) -> Tuple[Union[List[Any], Tuple[Any, ...]], List[int]]:
        """
        Histogram with the same semantics as Spark Classic :meth:`RDD.histogram`;
        implemented locally without importing JVM-backed modules.
        """
        if self.isEmpty():
            return _histogram_empty_connect(buckets)

        from math import isinf

        minv: Any
        maxv: Any
        even: bool
        inc: Any
        histogram_base: RDD[Any] = self

        if isinstance(buckets, int):
            if buckets < 1:
                raise ValueError("number of buckets must be >= 1")

            def comparable(x: Any) -> bool:
                if x is None:
                    return False
                if type(x) is float and math.isnan(x):
                    return False
                return True

            filtered = self.filter(comparable)
            histogram_base = filtered

            nb = buckets

            def pack(x: Any) -> Tuple[Tuple[Any, Any], int]:
                return ((x, x), 1)

            def merge_packed(
                a: Tuple[Tuple[Any, Any], int], b: Tuple[Tuple[Any, Any], int]
            ) -> Tuple[Tuple[Any, Any], int]:
                (min_a, max_a), c_a = a
                (min_b, max_b), c_b = b
                return (min(min_a, min_b), max(max_a, max_b)), c_a + c_b

            try:
                (minv, maxv), total_cnt = filtered.map(pack).reduce(merge_packed)
            except TypeError as e:
                err = str(e).lower()
                if " empty " in err:
                    raise ValueError("can not generate buckets from empty RDD") from None
                raise
            except ValueError as e:
                if "cannot reduce empty" in str(e).lower():
                    raise ValueError("can not generate buckets from empty RDD") from None
                raise

            if minv == maxv or nb == 1:
                return [minv, maxv], [total_cnt]

            try:
                inc = (maxv - minv) / nb
            except TypeError:
                raise TypeError("Can not generate buckets with non-number in RDD") from None

            if isinf(inc):
                raise ValueError("Can not generate buckets with infinite value")

            inc_try = int(inc)
            if inc_try * nb != maxv - minv:
                inc_try = (maxv - minv) * 1.0 / nb
            inc = inc_try

            bucket_edges = [i * inc + minv for i in range(nb)]
            bucket_edges.append(maxv)
            buckets = bucket_edges
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

            buckets_any: List[Any] = list(buckets)
            minv = buckets_any[0]
            maxv = buckets_any[-1]
            even = False
            inc = None
            try:
                steps = [buckets_any[i + 1] - buckets_any[i] for i in range(len(buckets_any) - 1)]
            except TypeError:
                pass  # objects in buckets do not support `-`
            else:
                if steps and max(steps) - min(steps) < 1e-10:
                    even = True
                    inc = (maxv - minv) / (len(buckets_any) - 1)

            buckets = buckets_any
        else:
            raise TypeError("buckets should be a list or tuple or number(int or long)")

        def histogram_partitions(iterator: Iterator[Any]) -> Iterator[List[int]]:
            bc = list(cast(Sequence[Any], buckets))
            counters = [0] * len(bc)
            for i in iterator:
                if i is None or (isinstance(i, float) and math.isnan(i)) or i > maxv or i < minv:
                    continue
                t = (
                    int((i - minv) / inc)
                    if even and inc is not None
                    else bisect.bisect_right(bc, i) - 1
                )
                counters[t] += 1
            last_cnt = counters.pop()
            counters[-1] += last_cnt
            yield counters

        def merge_counters(a: List[int], b: List[int]) -> List[int]:
            return [ii + jj for ii, jj in zip(a, b)]

        hist = histogram_base.mapPartitions(histogram_partitions).reduce(merge_counters)
        return buckets, hist

    def countByValue(self: RDD[Any]) -> Dict[Any, int]:
        from collections import defaultdict

        def countPartition(iterator: Iterator[Any]) -> Iterator[Dict[Any, int]]:
            counts: Dict[Any, int] = defaultdict(int)
            for obj in iterator:
                counts[obj] += 1
            yield counts

        def mergeMaps(m1: Dict[Any, int], m2: Dict[Any, int]) -> Dict[Any, int]:
            for k, v in m2.items():
                m1[k] = m1.get(k, 0) + v
            return m1

        return self.mapPartitions(countPartition).reduce(mergeMaps)

    def countByKey(self: RDD[Any]) -> Dict[Any, int]:
        return self.map(lambda x: x[0]).countByValue()

    def partitionBy(
        self: RDD[Any],
        numPartitions: Optional[int],
        partitionFunc: Optional[Callable[[Any], int]] = None,
    ) -> RDD[Any]:
        self._ensure_physical_pickled_elements_inplace()
        pf = portable_hash if partitionFunc is None else partitionFunc
        np = (
            int(numPartitions) if numPartitions is not None else max(1, self.ctx.defaultParallelism)
        )
        proposed = Partitioner(np, pf)
        kv_col_existing = getattr(self, "_kv_partition_id_col", None)
        pv_existing = getattr(self, "partitioner", None)
        if (
            pv_existing is not None
            and kv_col_existing == _KV_LOOKUP_PID_ALIAS
            and pv_existing == proposed
            and kv_col_existing in list(self._df.columns)
        ):
            return self

        pb_schema = StructType(
            [
                StructField(_PAYLOAD_COL, BinaryType(), True),
                StructField(_PARTITION_BY_PID_COL, IntegerType(), False),
            ]
        )

        def tag_batches(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                col0 = batch.column(0).to_pylist()
                pids: List[int] = []
                for pb in col0:
                    kv = _loads(pb)
                    if not isinstance(kv, tuple) or len(kv) != 2:
                        raise PySparkTypeError(
                            errorClass="NOT_EXPECTED_TYPE",
                            messageParameters={
                                "arg_name": "element",
                                "expected_type": "tuple key-value pair",
                                "arg_type": type(kv).__name__,
                            },
                        )
                    pids.append(int(pf(kv[0]) % np))
                yield pa.record_batch(
                    {
                        _PAYLOAD_COL: pa.array(col0, type=pa.large_binary()),
                        _PARTITION_BY_PID_COL: pa.array(pids, type=pa.int32()),
                    }
                )

        tagged = self._df.select(_PAYLOAD_COL).mapInArrow(tag_batches, schema=pb_schema)
        shuffled = tagged.repartitionById(np, _PARTITION_BY_PID_COL)
        branded_df = shuffled.select(
            F.col(_PAYLOAD_COL),
            F.col(_PARTITION_BY_PID_COL).alias(_KV_LOOKUP_PID_ALIAS),
        )
        out = self._fork(branded_df, np)
        out.partitioner = proposed
        out._kv_partition_id_col = _KV_LOOKUP_PID_ALIAS
        return out

    def countApproxDistinct(self, relativeSD: float = 0.05) -> int:
        if relativeSD < 0.000017:
            raise ValueError("relativeSD should be greater than 0.000017")
        self._ensure_physical_pickled_elements_inplace()
        row = self._df.select(F.approx_count_distinct(F.col(_PAYLOAD_COL), relativeSD)).first()
        if row is None or row[0] is None:
            return 0
        return int(row[0])

    def _to_pair_columns_df(self) -> SparkSqlDataFrame:
        self._ensure_physical_pickled_elements_inplace()
        col = _PAYLOAD_COL

        def extract(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                ks: List[bytes] = []
                vs: List[bytes] = []
                for pb in batch.column(0).to_pylist():
                    kv = _loads(pb)
                    if not isinstance(kv, tuple) or len(kv) != 2:
                        raise PySparkTypeError(
                            errorClass="NOT_EXPECTED_TYPE",
                            messageParameters={
                                "arg_name": "element",
                                "expected_type": "tuple key-value pair",
                                "arg_type": type(kv).__name__,
                            },
                        )
                    ks.append(_dumps(kv[0]))
                    vs.append(_dumps(kv[1]))
                yield pa.record_batch(
                    {
                        "__k": pa.array(ks, type=pa.large_binary()),
                        "__v": pa.array(vs, type=pa.large_binary()),
                    }
                )

        return self._df.select(col).mapInArrow(extract, schema=_PAIR_KV_SCHEMA)

    def combineByKey(
        self,
        createCombiner: Callable[[Any], Any],
        mergeValue: Callable[[Any, Any], Any],
        mergeCombiners: Callable[[Any, Any], Any],
        numPartitions: Optional[int] = None,
        partitionFunc: Optional[Callable[[Any], int]] = None,
    ) -> RDD[tuple]:
        # Classic partitionFunc is unused; partitioning follows numPartitions/defaultParallelism only.
        self._ensure_physical_pickled_elements_inplace()
        pairs = self._to_pair_columns_df()
        np = (
            int(numPartitions) if numPartitions is not None else max(1, self.ctx.defaultParallelism)
        )

        def phase1(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                local: Dict[bytes, Any] = {}
                order: List[bytes] = []
                kcol = batch.column(0).to_pylist()
                vcol = batch.column(1).to_pylist()
                for i in range(len(kcol)):
                    kb = kcol[i]
                    vb = vcol[i]
                    v = _loads(vb)
                    if kb not in local:
                        local[kb] = createCombiner(v)
                        order.append(kb)
                    else:
                        local[kb] = mergeValue(local[kb], v)
                ks_out: List[bytes] = []
                acc_out: List[bytes] = []
                for kb in order:
                    ks_out.append(kb)
                    acc_out.append(_dumps(local[kb]))
                yield pa.record_batch(
                    {
                        "__k": pa.array(ks_out, type=pa.large_binary()),
                        "__acc": pa.array(acc_out, type=pa.large_binary()),
                    }
                )

        partial = pairs.mapInArrow(phase1, schema=_PARTIAL_AGG_SCHEMA)
        rep = partial.repartition(np, "__k")
        grouped = rep.groupBy("__k").agg(F.collect_list("__acc").alias("__parts"))

        def phase2(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            blobs: List[bytes] = []
            for batch in it:
                kcol = batch.column(0).to_pylist()
                plist = batch.column(1).to_pylist()
                for i in range(batch.num_rows):
                    kb = kcol[i]
                    parts = plist[i]
                    merged = _loads(parts[0])
                    for j in range(1, len(parts)):
                        merged = mergeCombiners(merged, _loads(parts[j]))
                    blobs.append(_dumps((_loads(kb), merged)))
            yield pa.record_batch({_PAYLOAD_COL: pa.array(blobs, type=pa.large_binary())})

        final_df = grouped.mapInArrow(phase2, schema=_PICKLED_SCHEMA)
        return self._fork(final_df, np)

    def aggregateByKey(
        self,
        zeroValue: Any,
        seqFunc: Callable[[Any, Any], Any],
        combFunc: Callable[[Any, Any], Any],
        numPartitions: Optional[int] = None,
        partitionFunc: Optional[Callable[[Any], int]] = None,
    ) -> RDD[tuple]:
        def createCombiner(v: Any) -> Any:
            z = copy.deepcopy(zeroValue)
            return seqFunc(z, v)

        return self.combineByKey(createCombiner, seqFunc, combFunc, numPartitions, partitionFunc)

    def foldByKey(
        self,
        zeroValue: Any,
        func: Callable[[Any, Any], Any],
        numPartitions: Optional[int] = None,
        partitionFunc: Optional[Callable[[Any], int]] = None,
    ) -> RDD[tuple]:
        def createCombiner(v: Any) -> Any:
            z = copy.deepcopy(zeroValue)
            return func(z, v)

        return self.combineByKey(createCombiner, func, func, numPartitions, partitionFunc)

    def reduceByKey(
        self,
        func: Callable[[Any, Any], Any],
        numPartitions: Optional[int] = None,
        partitionFunc: Optional[Callable[[Any], int]] = None,
    ) -> RDD[tuple]:
        return self.combineByKey(lambda x: x, func, func, numPartitions, partitionFunc)

    def reduceByKeyLocally(self, func: Callable[[Any, Any], Any]) -> Dict[Any, Any]:
        from pyspark.util import fail_on_stopiteration

        self._ensure_physical_pickled_elements_inplace()
        func = fail_on_stopiteration(func)
        out: Dict[Any, Any] = {}
        for row in self._df.select(_PAYLOAD_COL).collect():
            kv = _loads(row[0])
            if not isinstance(kv, tuple) or len(kv) != 2:
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": "element",
                        "expected_type": "tuple key-value pair",
                        "arg_type": type(kv).__name__,
                    },
                )
            k, v = kv[0], kv[1]
            if k not in out:
                out[k] = v
            else:
                out[k] = func(out[k], v)
        return out

    def groupByKey(
        self,
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[Any], int] = portable_hash,
    ) -> RDD[tuple]:
        def createCombiner(v: Any) -> List[Any]:
            return [v]

        def mergeValue(acc: List[Any], v: Any) -> List[Any]:
            acc.append(v)
            return acc

        def mergeCombiners(a: List[Any], b: List[Any]) -> List[Any]:
            return a + b

        combined = self.combineByKey(
            createCombiner, mergeValue, mergeCombiners, numPartitions, partitionFunc
        )

        from pyspark.resultiterable import ResultIterable
        from pyspark.shuffle import ExternalListOfList

        return combined.map(lambda kv: (kv[0], ResultIterable(ExternalListOfList([kv[1]]))))

    def join(
        self: RDD[Any],
        other: RDD[Any],
        numPartitions: Optional[int] = None,
    ) -> RDD[Any]:
        from pyspark.join import python_join

        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        return python_join(self, other, numPartitions)

    def leftOuterJoin(
        self: RDD[Any],
        other: RDD[Any],
        numPartitions: Optional[int] = None,
    ) -> RDD[Any]:
        from pyspark.join import python_left_outer_join

        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        return python_left_outer_join(self, other, numPartitions)

    def rightOuterJoin(
        self: RDD[Any],
        other: RDD[Any],
        numPartitions: Optional[int] = None,
    ) -> RDD[Any]:
        from pyspark.join import python_right_outer_join

        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        return python_right_outer_join(self, other, numPartitions)

    def fullOuterJoin(
        self: RDD[Any],
        other: RDD[Any],
        numPartitions: Optional[int] = None,
    ) -> RDD[Any]:
        from pyspark.join import python_full_outer_join

        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        return python_full_outer_join(self, other, numPartitions)

    def cogroup(
        self: RDD[Any],
        other: RDD[Any],
        numPartitions: Optional[int] = None,
    ) -> RDD[Any]:
        from pyspark.join import python_cogroup

        if not isinstance(other, RDD):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "other",
                    "expected_type": "RDD",
                    "arg_type": type(other).__name__,
                },
            )
        _same_ctx(self.ctx, other.ctx)
        return python_cogroup((self, other), numPartitions)

    def groupWith(self, other: RDD[Any], /, *others: RDD[Any]) -> RDD[Any]:
        from pyspark.join import python_cogroup

        for o in (other,) + others:
            if not isinstance(o, RDD):
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": "other",
                        "expected_type": "RDD",
                        "arg_type": type(o).__name__,
                    },
                )
            _same_ctx(self.ctx, o.ctx)
        return python_cogroup((self, other) + others, numPartitions=None)

    def saveAsTextFile(self, path: str, compressionCodecClass: Optional[str] = None) -> None:
        def row_as_text(b: Optional[bytes]) -> str:
            x = _loads(b)
            if isinstance(x, (bytes, bytearray)):
                return bytes(x).decode("utf-8")
            return str(x)

        text_expr: Any = F.udf(row_as_text, StringType())
        str_df = self._df.select(text_expr(F.col(_PAYLOAD_COL)).alias("value"))
        writer = str_df.write.mode("overwrite")
        if compressionCodecClass:
            writer = writer.option("compression", compressionCodecClass)
        writer.text(path)

    def saveAsPickleFile(self, path: str, batchSize: int = 10) -> None:
        """
        Pickle elements using Python batch serializers and write bytes under ``path`` on the driver.

        Readable via :meth:`RDD.pickleFile`. Not compatible with JVM object files.
        """

        import os
        import shutil

        self._ensure_physical_pickled_elements_inplace()
        elems = [_loads(r[0]) for r in self._df.select(_PAYLOAD_COL).collect()]
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

        meta_path = os.path.join(path, ".connect_pickle_batchsize")
        buf = BytesIO()
        if batchSize == 0:
            with open(meta_path, "w", encoding="ascii") as mf:
                mf.write("auto")
            AutoBatchedSerializer(CPickleSerializer()).dump_stream(iter(elems), buf)
        else:
            bs = batchSize
            with open(meta_path, "w", encoding="ascii") as mf:
                mf.write(str(bs))
            BatchedSerializer(CPickleSerializer(), bs).dump_stream(iter(elems), buf)

        with open(os.path.join(path, "part-00000"), "wb") as sf:
            sf.write(buf.getvalue())

    def pipe(
        self, command: str, env: Optional[Dict[str, str]] = None, checkCode: bool = False
    ) -> RDD[str]:
        import shlex
        import subprocess

        argv = shlex.split(command)

        def pipe_partitions(iterator: Iterator[Any]) -> Iterator[str]:
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
            assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
            try:
                for x in iterator:
                    proc.stdin.write(str(x).encode("utf-8"))
                    proc.stdin.write(b"\n")
            finally:
                proc.stdin.close()
            data_out = proc.stdout.read()
            proc.stdout.close()
            err = proc.stderr.read()
            proc.stderr.close()
            code = proc.wait()
            if checkCode and code != 0:
                raise RuntimeError(err.decode("utf-8", errors="replace"))
            for line in data_out.splitlines():
                yield line.decode("utf-8")

        return self.mapPartitions(pipe_partitions)

    def repartitionAndSortWithinPartitions(
        self,
        numPartitions: int,
        partitionFunc: Callable[[Any], int],
        ascending: bool = True,
    ) -> RDD[tuple]:
        np = int(numPartitions)
        self._ensure_physical_pickled_elements_inplace()

        def tag_batches(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                col0 = batch.column(0).to_pylist()
                pids: List[int] = []
                for pb in col0:
                    kv = _loads(pb)
                    k = kv[0]
                    p = int(partitionFunc(k))
                    if p < 0:
                        p = (p % np + np) % np
                    elif p >= np:
                        p %= np
                    pids.append(int(p))
                yield pa.record_batch(
                    {
                        _PAYLOAD_COL: batch.column(0),
                        _SORT_PID_COL: pa.array(pids, type=pa.int32()),
                    }
                )

        tagged = self._df.select(_PAYLOAD_COL).mapInArrow(tag_batches, schema=_SORT_TAGGED_SCHEMA)
        shuffled = tagged.repartitionById(np, _SORT_PID_COL)

        def sort_pairs(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            rows: List[Any] = []
            for batch in it:
                for pb in batch.column(0).to_pylist():
                    rows.append(_loads(pb))

            def cmp_kv(a: Any, b: Any) -> int:
                ka, va = a[0], a[1]
                kb, vb = b[0], b[1]
                if ascending:
                    if ka < kb:
                        return -1
                    if ka > kb:
                        return 1
                else:
                    if ka < kb:
                        return 1
                    if ka > kb:
                        return -1
                if va < vb:
                    return -1
                if va > vb:
                    return 1
                return 0

            rows.sort(key=cmp_to_key(cmp_kv))
            yield pa.record_batch(
                {_PAYLOAD_COL: pa.array([_dumps(r) for r in rows], type=pa.large_binary())}
            )

        out = shuffled.select(_PAYLOAD_COL).mapInArrow(sort_pairs, schema=_PICKLED_SCHEMA)
        return self._fork(out, np)

    def _pickled(self: RDD[T]) -> RDD[T]:
        return self._reserialize(AutoBatchedSerializer(CPickleSerializer()))

    def cleanShuffleDependencies(self, blocking: bool = False) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.cleanShuffleDependencies on Spark Connect"},
        )

    def isCheckpointed(self) -> bool:
        return False

    def isLocallyCheckpointed(self) -> bool:
        return False

    def getCheckpointFile(self) -> Optional[str]:
        return None

    def getStorageLevel(self) -> StorageLevel:
        """
        Return the storage level of the backing Spark Connect :class:`DataFrame` plan.

        Connect RDD persistence is implemented via :meth:`persist` / :meth:`cache` on the
        underlying :class:`~pyspark.sql.connect.dataframe.DataFrame`.
        """
        return self._df.storageLevel

    def name(self) -> Optional[str]:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.name on Spark Connect"},
        )

    def setName(self, name: str) -> RDD[T]:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.setName on Spark Connect"},
        )

    def toDebugString(self) -> str:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.toDebugString on Spark Connect"},
        )

    def mapPartitionsWithSplit(
        self,
        f: Callable[[int, Iterable[T]], Iterable[Any]],
        preservesPartitioning: bool = False,
    ) -> RDD[Any]:
        warnings.warn(
            "mapPartitionsWithSplit is deprecated; use mapPartitionsWithIndex instead",
            FutureWarning,
            stacklevel=2,
        )

        def adapt(split: int, iterator: Iterator[T]) -> Iterator[Any]:
            return iter(f(split, iterator))

        return self.mapPartitionsWithIndex(adapt, preservesPartitioning)

    def lookup(self, key: Any) -> List[Any]:
        """
        Values for keys equal to ``key``.

        After :meth:`partitionBy`, restricts the scan using the persisted partition bucket
        (same ``partitionFunc`` / ``Partitioner`` as ``partitionBy``). Otherwise behaves like
        filter + ``values`` + ``collect``.
        """
        self._ensure_physical_pickled_elements_inplace()
        pid_col = self._kv_col_active()
        pv = getattr(self, "partitioner", None)
        df_work = self._df
        if pid_col is not None and pv is not None:
            target = int(pv(key))
            df_work = self._df.filter(F.col(pid_col) == F.lit(target))
        payload_df = df_work.select(_PAYLOAD_COL)
        sub: RDD[Any] = RDD(payload_df, self.ctx, self._num_partitions_hint)
        return (
            sub.filter(lambda kv_pair: kv_pair[0] == key).map(lambda kv_pair: kv_pair[1]).collect()
        )

    def countApprox(self, timeout: int, confidence: float = 0.95) -> int:
        """
        Approximate count within a wall-clock timeout (milliseconds).

        .. note:: On Spark Connect this runs one aggregated shuffle-free job instead
            of the JVM ``PartialRDD`` APIs; bounded metadata still reflects elapsed
            time versus ``timeout``.
        """
        drdd = self.mapPartitions(lambda it: iter([float(sum(1 for _ in it))]))
        return int(drdd.sumApprox(timeout, confidence))

    def meanApprox(
        self: RDD[Union[float, int]],
        timeout: int,
        confidence: float = 0.95,
    ) -> BoundedFloat:
        """
        Approximate mean within a wall-clock timeout (milliseconds).

        .. note:: On Spark Connect this runs one aggregated job (per-partition sums
            and counts, then merged on the driver) instead of the JVM partial-result
            path; bounded metadata compares elapsed wall time to ``timeout``.
        """
        r = self.map(float)
        np = r.getNumPartitions()
        if np == 0:
            return BoundedFloat(0.0, confidence, 0.0, 0.0)

        def sums_and_counts(it: Iterable[Any]) -> Iterator[Tuple[float, int]]:
            xs = list(it)
            return iter([(float(sum(xs)), len(xs))])

        started = time.monotonic()
        part_stats = r.mapPartitions(sums_and_counts).collect()
        elapsed = time.monotonic() - started

        deadline_s = max(0, int(timeout)) / 1000.0
        total = sum(s for s, _ in part_stats)
        count = sum(c for _, c in part_stats)
        mean = total / count if count else 0.0

        complete = elapsed <= deadline_s
        low, high = _connect_approx_bounds(
            mean,
            complete=complete,
            num_partitions=np,
            processed_partitions=np,
            confidence=confidence,
        )
        return BoundedFloat(mean, confidence, low, high)

    def sumApprox(
        self: RDD[Union[float, int]],
        timeout: int,
        confidence: float = 0.95,
    ) -> BoundedFloat:
        """
        Approximate sum within a wall-clock timeout (milliseconds).

        .. note:: On Spark Connect this runs one aggregated map-partitions collect
            instead of JVM ``PartialRDD`` APIs; bounded metadata compares elapsed wall
            time to ``timeout``.
        """
        np = self.getNumPartitions()
        if np == 0:
            return BoundedFloat(0.0, confidence, 0.0, 0.0)

        started = time.monotonic()
        part_sums = self.mapPartitions(lambda it: iter([float(sum(it))])).collect()
        elapsed = time.monotonic() - started

        deadline_s = max(0, int(timeout)) / 1000.0
        total = float(sum(part_sums))
        complete = elapsed <= deadline_s

        low, high = _connect_approx_bounds(
            total,
            complete=complete,
            num_partitions=np,
            processed_partitions=np,
            confidence=confidence,
        )
        return BoundedFloat(total, confidence, low, high)

    def sampleByKey(
        self,
        withReplacement: bool,
        fractions: Dict[Any, Union[float, int]],
        seed: Optional[int] = None,
    ) -> RDD[Any]:
        for fraction in fractions.values():
            assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        sampler = RDDStratifiedSampler(withReplacement, fractions, seed)

        def stratified(split: int, iterator: Iterator[Any]) -> Iterator[Any]:
            return sampler.func(split, iterator)

        return self.mapPartitionsWithIndex(stratified, True)

    def saveAsHadoopDataset(self, conf: Dict[str, str]) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsHadoopDataset on Spark Connect"},
        )

    def saveAsHadoopFile(
        self,
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

    def saveAsNewAPIHadoopDataset(self, conf: Dict[str, str]) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsNewAPIHadoopDataset on Spark Connect"},
        )

    def saveAsNewAPIHadoopFile(
        self,
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

    def saveAsSequenceFile(self, path: str, compressionCodecClass: Optional[str] = None) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.saveAsSequenceFile on Spark Connect"},
        )

    def _reserialize(self, serializer: Any = None) -> RDD[T]:
        return self

    def withResources(self, profile: Any) -> RDD[T]:
        out = self._fork(
            self._df,
            self._num_partitions_hint,
            is_barrier=bool(getattr(self, "_barrier_rdd", False)),
        )
        out._resource_profile = profile
        return out

    def getResourceProfile(self) -> Any:
        return self._resource_profile

    def checkpoint(self, eager: bool = True) -> RDD[Any]:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": (
                    "RDD.checkpoint on Spark Connect (use pyspark.sql.DataFrame.checkpoint instead)"
                ),
            },
        )

    def localCheckpoint(self, eager: bool = True, storageLevel: Optional[Any] = None) -> RDD[Any]:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": "RDD.localCheckpoint on Spark Connect (use pyspark.sql.DataFrame.localCheckpoint)",
            },
        )


class RDDBarrier(Generic[T]):
    """RDD barrier implementation for Spark Connect (``mapInArrow(..., barrier=True)``)."""

    def __init__(self, rdd: RDD[Any]) -> None:
        self.rdd = rdd

    def mapPartitions(
        self, f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning: bool = False
    ) -> RDD[Any]:
        """
        Applies ``f`` per partition with ``barrier=True`` so the planner marks a barrier map stage.

        .. versionadded:: 5.0.0

        See Also
        --------
        :meth:`RDD.mapPartitions`
        """

        def adapted(it: Iterator[Any]) -> Iterator[Any]:
            yield from f(it)

        return self.rdd._map_partitions_in_arrow(
            adapted, preservesPartitioning, map_in_arrow_barrier=True
        )

    def mapPartitionsWithIndex(
        self,
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> RDD[Any]:
        """
        Applies ``f`` per partition index with ``barrier=True``.

        .. versionadded:: 5.0.0

        See Also
        --------
        :meth:`RDD.mapPartitionsWithIndex`
        """

        def adapted(split_index: int, it: Iterator[Any]) -> Iterator[Any]:
            yield from f(split_index, it)

        return self.rdd._map_partitions_with_index_in_arrow(
            adapted, preservesPartitioning, map_in_arrow_barrier=True
        )
