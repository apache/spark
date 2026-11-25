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

The server builds the RDDs as Spark Classic builds the RDDs of PySpark, with the same RDDs of
Spark Core: a function of the partitions runs in Python as a ``PythonRDD``, a shuffle moves the
batches that Python buckets with a ``PairwiseRDD`` partitioned by a ``PythonPartitioner``, and
``union``, ``zip``, ``cartesian``, ``coalesce`` and the sources are the RDD methods that PySpark
calls. So the algorithms of this module are those of Spark Classic's RDD, and the elements move
between the JVM and Python in the same pickled batches.

The client describes an RDD with an ``RDDPlan`` (see relations.proto), which the server builds once
per session and keeps under the ID of the plan until the client releases it, when the plan is not
referenced anymore. The jobs and the commands of an RDD then share the RDD on the server, as they
share the JVM RDD in Spark Classic: its shuffle outputs, its cached partitions and its
checkpoint. Once the server keeps the RDD of a plan, the client sends the plan as a reference to it.
"""

import bisect
import copy
import heapq
import io
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
from itertools import chain
from math import ceil, isinf, isnan, log, pow, sqrt
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

import pyspark.sql.connect.proto as pb2
from pyspark.core.connect import _runtime
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
    CartesianDeserializer,
    CloudPickleSerializer,
    CPickleSerializer,
    NoOpSerializer,
    PairDeserializer,
    Serializer,
    pack_long,
)
from pyspark.shuffle import (
    Aggregator,
    ExternalGroupBy,
    ExternalMerger,
    ExternalSorter,
    get_used_memory,
)
from pyspark.sql.connect.logging import logger
from pyspark.statcounter import StatCounter
from pyspark.storagelevel import StorageLevel
from pyspark.util import _parse_memory, fail_on_stopiteration, portable_hash

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark._typing import NumberOrArray, S, SizedIterable
    from pyspark.core.connect.context import SparkContext
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.dataframe import DataFrame

__all__ = ["RDD", "RDDBarrier"]

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
U = TypeVar("U")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class _RDDRelationPlan:
    """
    The plan of a relation of the RDD API (``RDDRelation``): ``fill`` writes its relation type,
    with the relations of its inputs, the plans of the DataFrames of other RDDs.
    """

    @staticmethod
    def dataframe(
        ctx: "SparkContext",
        fill: Callable[[pb2.RDDRelation, Any], None],
        child: Optional["ConnectDataFrame"] = None,
    ) -> "ConnectDataFrame":
        """A DataFrame of the relation that ``fill`` writes."""
        from pyspark.sql.connect.dataframe import DataFrame
        from pyspark.sql.connect.plan import LogicalPlan

        class RDDRelation(LogicalPlan):
            def __init__(self) -> None:
                # The first input, for the observations of the plan.
                super().__init__(None if child is None else child._plan)

            def plan(self, session: Any) -> pb2.Relation:
                relation = self._create_proto_relation()
                fill(relation.rdd_relation, session)
                return relation

        return DataFrame(RDDRelation(), ctx._session)


def _relation(df: "ConnectDataFrame", session: Any) -> pb2.Relation:
    return df._plan.plan(session)


def _local_data_relation(ctx: "SparkContext", values: Sequence[bytes]) -> pb2.Relation:
    """
    A relation of local data with one binary column holding ``values`` in order. Like the local
    data of ``createDataFrame``, it is cached on the server as artifacts when it is large.
    """
    import pyarrow as pa

    from pyspark.sql.connect.plan import LocalRelation, LogicalPlan
    from pyspark.sql.types import BinaryType, StructField, StructType

    table = pa.Table.from_arrays([pa.array(values, pa.large_binary())], names=["value"])
    local = LocalRelation(table, schema=StructType([StructField("value", BinaryType())]).json())
    session = ctx._session
    plan: LogicalPlan = local
    # Data below the default threshold of the cache is sent in the plan without asking the
    # server for its configurations.
    if table.nbytes >= _INLINE_DATA_LIMIT:
        configs = session._client.get_config_dict(
            "spark.sql.session.localRelationCacheThreshold",
            "spark.sql.session.localRelationSizeLimit",
            "spark.sql.session.localRelationChunkSizeRows",
            "spark.sql.session.localRelationChunkSizeBytes",
            "spark.sql.session.localRelationBatchOfChunksSizeBytes",
        )

        def conf(name: str) -> int:
            # get_config_dict throws [SQL_CONF_NOT_FOUND] if the key is not found.
            return int(cast(str, configs["spark.sql.session." + name]))

        if conf("localRelationCacheThreshold") <= table.nbytes:
            plan = session._cache_local_relation(
                local,
                conf("localRelationSizeLimit"),
                conf("localRelationChunkSizeRows"),
                conf("localRelationChunkSizeBytes"),
                conf("localRelationBatchOfChunksSizeBytes"),
            )
    return plan.plan(session.client)


# The default of spark.sql.session.localRelationCacheThreshold.
_INLINE_DATA_LIMIT = 1 << 20


def _with_resources(
    ctx: "SparkContext", df: "ConnectDataFrame", profile: ResourceProfile
) -> "ConnectDataFrame":
    """The rows of ``df``, in a stage that runs with ``profile``."""
    profile_id = profile.id

    def fill(relation: pb2.RDDRelation, session: Any) -> None:
        relation.with_resources.input.CopyFrom(_relation(df, session))
        relation.with_resources.profile_id = profile_id

    return _RDDRelationPlan.dataframe(ctx, fill, df)


def _load(deserializer: Serializer, table: "pa.Table") -> Iterator[Any]:
    """The elements in the ``data`` column of the rows of ``RDDRelation.Elements``."""
    for data in table.column(1).to_pylist():
        yield from deserializer.load_stream(io.BytesIO(data))


class RDD(RDDFacade, Generic[T_co]):
    """
    A Resilient Distributed Dataset (RDD) on Spark Connect: a DataFrame whose rows hold the
    elements of the RDD, as its deserializer reads them, in the partitions of the RDD.

    .. versionadded:: 5.0.0
    """

    _ids = itertools.count(1)

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        return object.__new__(cls)

    def __init__(
        self,
        df: "ConnectDataFrame",
        ctx: "SparkContext",
        jrdd_deserializer: Serializer = AutoBatchedSerializer(CPickleSerializer()),
        num_partitions: Optional[int] = None,
        server_partitioner: Optional[Tuple[int, int]] = None,
    ) -> None:
        self._df_val = df
        self.is_cached = False
        self.is_checkpointed = False
        self.has_resource_profile = False
        # The facade declares ``ctx`` as Spark Classic's SparkContext; see ``_cctx``.
        self.ctx = cast(Any, ctx)
        self._jrdd_deserializer = jrdd_deserializer
        self._id: Optional[int] = None
        self.partitioner: Optional[Partitioner] = None
        self._name: Optional[str] = None
        self._locally_checkpointed = False
        self._resource_profile: Optional[ResourceProfile] = None
        # The cache of ``_df`` with a resource profile: the DataFrame of the elements and the
        # profile that it was made of, and it.
        self._df_with_resources: Optional[
            Tuple["ConnectDataFrame", ResourceProfile, "ConnectDataFrame"]
        ] = None
        # The number of partitions, when known without asking the server.
        self._num_partitions = num_partitions
        # The number of partitions and the ID of the partition function of the shuffle that
        # partitioned the elements, while the RDD keeps that partitioning on the server: as with
        # the partitioners of the RDDs of Spark Classic, unions of RDDs with the same one keep it.
        self._server_partitioner = server_partitioner

    @classmethod
    def _from_dataframe(cls, df: "ConnectDataFrame") -> "RDD[Any]":
        """``DataFrame.rdd``: the rows of the DataFrame, as Spark Classic pickles them."""
        ctx = df.sparkSession.sparkContext

        def fill(relation: pb2.RDDRelation, session: Any) -> None:
            relation.from_data_frame.input.CopyFrom(_relation(df, session))

        rdd_df = _RDDRelationPlan.dataframe(ctx, fill, df)
        return RDD(rdd_df, ctx, BatchedSerializer(CPickleSerializer()))

    # -- Internals -----------------------------------------------------------------------------

    @property
    def _df(self) -> "ConnectDataFrame":
        """
        The DataFrame of the RDD: the DataFrame of its elements, in a stage that runs with the
        resource profile of the RDD when it has one.
        """
        df = self._elements_df
        profile = self._resource_profile
        if profile is None:
            return df
        cached = self._df_with_resources
        if cached is None or cached[0] is not df or cached[1] is not profile:
            cached = (df, profile, _with_resources(self._cctx, df, profile))
            self._df_with_resources = cached
        return cached[2]

    @property
    def _elements_df(self) -> "ConnectDataFrame":
        """The DataFrame of the elements of the RDD, without its resource profile."""
        return self._df_val

    @property
    def _cctx(self) -> "SparkContext":
        """``ctx`` as the Spark Connect SparkContext that it is."""
        return cast("SparkContext", self.ctx)

    def _new(
        self,
        fill: Callable[[pb2.RDDRelation, Any], None],
        jrdd_deserializer: Serializer,
        num_partitions: Optional[int] = None,
        server_partitioner: Optional[Tuple[int, int]] = None,
    ) -> "RDD[Any]":
        """A new RDD of the relation that ``fill`` writes."""
        df = _RDDRelationPlan.dataframe(self._cctx, fill, self._df)
        return RDD(df, self._cctx, jrdd_deserializer, num_partitions, server_partitioner)

    def _pickled(self: "RDD[T]") -> "RDD[T]":
        return self._reserialize(AutoBatchedSerializer(CPickleSerializer()))

    def _reserialize(self: "RDD[T]", serializer: Optional[Serializer] = None) -> "RDD[T]":
        serializer = serializer or self.ctx.serializer
        if self._jrdd_deserializer != serializer:
            self = self.map(lambda x: x, preservesPartitioning=True)
            self._jrdd_deserializer = serializer
        return self

    def _collect(self, partitions: Optional[Sequence[int]] = None) -> List[Any]:
        """The elements of the partitions (all of them if not given), in the order given."""
        return self._cctx._collect_rdd(self._df, self._jrdd_deserializer, partitions)

    def _to_java_object_rdd(self) -> NoReturn:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD._to_java_object_rdd on Spark Connect"},
        )

    # -- Identity and persistence -------------------------------------------------------------

    def id(self) -> int:
        """A unique ID for this RDD within this Python process, not the server's RDD ID."""
        if self._id is None:
            self._id = next(RDD._ids)
        return self._id

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
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY)
        return self

    def persist(self: "RDD[T]", storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> "RDD[T]":
        """
        Persist the RDD through :meth:`DataFrame.persist` of its DataFrame, which the jobs of
        the RDDs computed from this one then read.
        """
        self.is_cached = True
        # The DataFrame of the elements, so that the resource profile of the RDD, which can be
        # set later, does not change the DataFrame to unpersist.
        self._elements_df.persist(storageLevel)
        return self

    def unpersist(self: "RDD[T]", blocking: bool = False) -> "RDD[T]":
        self.is_cached = False
        self._elements_df.unpersist(blocking)
        return self

    def getStorageLevel(self) -> StorageLevel:
        # The DataFrame of the RDD of a DataFrame may also be cached by the user.
        return self._elements_df.storageLevel if self.is_cached else StorageLevel.NONE

    def _checkpoint(self, local: bool) -> None:
        checkpointed = self._df.localCheckpoint(eager=True) if local else self._df.checkpoint(True)
        self._set_df(cast("ConnectDataFrame", checkpointed))
        self.is_checkpointed = True
        self._locally_checkpointed = local

    def _set_df(self, df: "ConnectDataFrame") -> None:
        self._df_val = df

    def checkpoint(self) -> None:
        """
        Checkpoint this RDD through :meth:`DataFrame.checkpoint`. On Spark Connect the checkpoint
        is written immediately, to the checkpoint directory configured on the server.
        """
        self._checkpoint(local=False)

    def isCheckpointed(self) -> bool:
        return self.is_checkpointed

    def localCheckpoint(self) -> None:
        """
        Checkpoint this RDD through :meth:`DataFrame.localCheckpoint`, which truncates its
        lineage. On Spark Connect the checkpoint is written immediately.
        """
        self._checkpoint(local=True)

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
        return self._df._explain_string(extended=True).encode("utf-8")

    # -- Narrow transformations ---------------------------------------------------------------

    def map(self: "RDD[T]", f: Callable[[T], U], preservesPartitioning: bool = False) -> "RDD[U]":
        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return map(fail_on_stopiteration(f), iterator)

        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def flatMap(
        self: "RDD[T]", f: Callable[[T], Iterable[U]], preservesPartitioning: bool = False
    ) -> "RDD[U]":
        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return chain.from_iterable(map(fail_on_stopiteration(f), iterator))

        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def mapPartitions(
        self: "RDD[T]", f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning: bool = False
    ) -> "RDD[U]":
        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return f(iterator)

        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def mapPartitionsWithIndex(
        self: "RDD[T]",
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> "RDD[U]":
        return PipelinedRDD(self, f, preservesPartitioning)

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

    def distinct(self: "RDD[T]", numPartitions: Optional[int] = None) -> "RDD[T]":
        return (
            self.map(lambda x: (x, None))
            .reduceByKey(lambda x, _: x, numPartitions)
            .map(lambda x: x[0])
        )

    def sample(
        self: "RDD[T]", withReplacement: bool, fraction: float, seed: Optional[int] = None
    ) -> "RDD[T]":
        if not fraction >= 0:
            raise ValueError("Fraction must be nonnegative.")
        return self.mapPartitionsWithIndex(RDDSampler(withReplacement, fraction, seed).func, True)

    def randomSplit(
        self: "RDD[T]", weights: Sequence[Union[int, float]], seed: Optional[int] = None
    ) -> "List[RDDFacade[Any]]":
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
        splits = [
            self.mapPartitionsWithIndex(RDDRangeSampler(lb, ub, seed).func, True)
            for lb, ub in zip(cweights, cweights[1:])
        ]
        return cast(List[RDDFacade[Any]], splits)

    def takeSample(
        self: "RDD[T]", withReplacement: bool, num: int, seed: Optional[int] = None
    ) -> List[T]:
        numStDev = 10.0
        maxSampleSize = sys.maxsize - int(numStDev * sqrt(sys.maxsize))
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
        numIters = 0
        while len(samples) < num:
            numIters += 1
            logger.warning(
                "Needed to re-sample due to insufficient sample size. Repeat #%d" % numIters
            )
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
            return fraction + numStDev * sqrt(fraction / total)
        else:
            delta = 0.00005
            gamma = -log(delta) / total
            return min(1, fraction + gamma + sqrt(gamma * gamma + 2 * gamma * fraction))

    def glom(self: "RDD[T]") -> "RDD[List[T]]":
        def func(iterator: Iterable[T]) -> Iterable[List[T]]:
            yield list(iterator)

        return self.mapPartitions(func)

    def groupBy(
        self: "RDD[T]",
        f: Callable[[T], K],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, Iterable[T]]]":
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions, partitionFunc)

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

    def keyBy(self: "RDD[T]", f: Callable[[T], K]) -> "RDD[Tuple[K, T]]":
        return self.map(lambda x: (f(x), x))

    def keys(self: "RDD[Tuple[K, V]]") -> "RDD[K]":
        return self.map(lambda x: x[0])

    def values(self: "RDD[Tuple[K, V]]") -> "RDD[V]":
        return self.map(lambda x: x[1])

    def flatMapValues(
        self: "RDD[Tuple[K, V]]", f: Callable[[V], Iterable[U]]
    ) -> "RDD[Tuple[K, U]]":
        def flat_map_fn(kv: Tuple[K, V]) -> Iterable[Tuple[K, U]]:
            return ((kv[0], x) for x in f(kv[1]))

        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def mapValues(self: "RDD[Tuple[K, V]]", f: Callable[[V], U]) -> "RDD[Tuple[K, U]]":
        def map_values_fn(kv: Tuple[K, V]) -> Tuple[K, U]:
            return kv[0], f(kv[1])

        return self.map(map_values_fn, preservesPartitioning=True)

    def sampleByKey(
        self: "RDD[Tuple[K, V]]",
        withReplacement: bool,
        fractions: Dict[K, Union[float, int]],
        seed: Optional[int] = None,
    ) -> "RDD[Tuple[K, V]]":
        for fraction in fractions.values():
            assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        return self.mapPartitionsWithIndex(
            RDDStratifiedSampler(withReplacement, fractions, seed).func, True
        )

    def zipWithIndex(self: "RDD[T]") -> "RDD[Tuple[T, int]]":
        starts = [0]
        if self.getNumPartitions() > 1:
            nums = self.mapPartitions(lambda it: [sum(1 for i in it)]).collect()
            for i in range(len(nums) - 1):
                starts.append(starts[-1] + nums[i])

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

    def barrier(self: "RDD[T]") -> "RDDBarrier[T]":
        return RDDBarrier(self)

    def _is_barrier(self) -> bool:
        return False

    def withResources(self: "RDD[T]", profile: ResourceProfile) -> "RDD[T]":
        """
        Run the stage that computes this RDD with ``profile``, as Spark Classic runs the stage of
        an RDD with a resource profile.
        """
        self.has_resource_profile = True
        self._resource_profile = profile
        return self

    def getResourceProfile(self) -> Optional[ResourceProfile]:
        return self._resource_profile

    # -- Partitioning and shuffles -------------------------------------------------------------

    def getNumPartitions(self) -> int:
        if self._num_partitions is None:
            self._num_partitions = self._cctx._num_partitions(self._df)
        return self._num_partitions

    def _memory_limit(self) -> int:
        return _parse_memory(self._cctx._spark_conf_value("spark.python.worker.memory") or "512m")

    def _defaultReducePartitions(self) -> int:
        parallelism = self._cctx._spark_conf_value("spark.default.parallelism")
        if parallelism is not None:
            return int(parallelism)
        else:
            return self.getNumPartitions()

    def _shuffle(
        self,
        keyed: "RDD[Any]",
        num_partitions: int,
        partitioner_id: Optional[int],
    ) -> "RDD[Any]":
        """
        Shuffle the batches that ``keyed`` writes after their partition index, as the PairwiseRDD
        of Spark Classic reads them, into ``num_partitions`` partitions.
        """
        keyed._bypass_serializer = True  # type: ignore[attr-defined]
        keyed._output_pairs = True  # type: ignore[attr-defined]
        keyed_df = keyed._df

        def shuffle(relation: pb2.RDDRelation, session: Any) -> None:
            relation.shuffle.input.CopyFrom(_relation(keyed_df, session))
            relation.shuffle.num_partitions = num_partitions

        shuffled_df = _RDDRelationPlan.dataframe(self._cctx, shuffle, keyed_df)

        def values(relation: pb2.RDDRelation, session: Any) -> None:
            relation.values.input.CopyFrom(_relation(shuffled_df, session))

        server_partitioner = None if partitioner_id is None else (num_partitions, partitioner_id)
        return RDD(
            _RDDRelationPlan.dataframe(self._cctx, values, shuffled_df),
            self._cctx,
            BatchedSerializer(self.ctx._unbatched_serializer),
            num_partitions,
            server_partitioner,
        )

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

        # Transferring O(n) objects to Java is too expensive.
        # Instead, we'll form the hash buckets in Python,
        # transferring O(numPartitions) objects to Java.
        # Each object is a (splitNumber, [objects]) pair.
        # In order to avoid too huge objects, the objects are
        # grouped into chunks.
        outputSerializer = self.ctx._unbatched_serializer

        limit = self._memory_limit() / 2

        def add_shuffle_key(split: int, iterator: Iterable[Tuple[K, V]]) -> Iterable[bytes]:
            buckets = defaultdict(list)
            c, batch = 0, min(10 * num_partitions, 1000)

            for k, v in iterator:
                buckets[partitionFunc(k) % num_partitions].append((k, v))
                c += 1

                # check used memory and avg size of chunk of objects
                if c % 1000 == 0 and get_used_memory() > limit or c > batch:
                    n, size = len(buckets), 0
                    for split in list(buckets.keys()):
                        yield pack_long(split)
                        d = outputSerializer.dumps(buckets[split])
                        del buckets[split]
                        yield d
                        size += len(d)

                    avg = int(size / n) >> 20
                    # let 1M < avg < 10M
                    if avg < 1:
                        batch = min(sys.maxsize, batch * 1.5)  # type: ignore[assignment]
                    elif avg > 10:
                        batch = max(int(batch / 1.5), 1)
                    c = 0

            for split, items in buckets.items():
                yield pack_long(split)
                yield outputSerializer.dumps(items)

        keyed = self.mapPartitionsWithIndex(add_shuffle_key, preservesPartitioning=True)
        rdd = self._shuffle(keyed, num_partitions, id(partitionFunc))
        rdd.partitioner = partitioner
        return rdd

    def repartition(self: "RDD[T]", numPartitions: int) -> "RDD[T]":
        return self.coalesce(numPartitions, shuffle=True)

    def coalesce(self: "RDD[T]", numPartitions: int, shuffle: bool = False) -> "RDD[T]":
        if not numPartitions > 0:
            raise ValueError("Number of partitions must be positive.")
        if shuffle:
            # As RDD.coalesce with a shuffle in Spark Core: each partition deals its elements to
            # the partitions in turn, from a position that depends on the partition, so that the
            # output partitions hold about as many elements each.
            outputSerializer = self.ctx._unbatched_serializer
            limit = self._memory_limit() / 2

            def distribute(split: int, iterator: Iterable[T]) -> Iterable[bytes]:
                position = random.Random(split).randrange(numPartitions)
                buckets: Dict[int, List[T]] = defaultdict(list)
                c = 0
                for x in iterator:
                    position = (position + 1) % numPartitions
                    buckets[position].append(x)
                    c += 1
                    if c % 1000 == 0 and (c >= 10 * numPartitions or get_used_memory() > limit):
                        for pid, items in buckets.items():
                            yield pack_long(pid)
                            yield outputSerializer.dumps(items)
                        buckets.clear()
                for pid, items in buckets.items():
                    yield pack_long(pid)
                    yield outputSerializer.dumps(items)

            return self._shuffle(self.mapPartitionsWithIndex(distribute), numPartitions, None)

        input_df = self._df

        def coalesce(relation: pb2.RDDRelation, session: Any) -> None:
            relation.coalesce.input.CopyFrom(_relation(input_df, session))
            relation.coalesce.num_partitions = numPartitions

        # Coalescing makes no more partitions than its input has; the server counts them.
        return self._new(coalesce, self._jrdd_deserializer)

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
        serializer = self._jrdd_deserializer

        def sortPartition(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, V]]:
            sort = ExternalSorter[tuple[K, V]](memory * 0.9, serializer).sorted
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
        serializer = self._jrdd_deserializer

        def sortPartition(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, V]]:
            sort = ExternalSorter[tuple[K, V]](memory * 0.9, serializer).sorted
            return iter(sort(iterator, key=lambda kv: keyfunc(kv[0]), reverse=(not ascending)))

        if numPartitions == 1:
            if self.getNumPartitions() > 1:
                self = self.coalesce(1)
            return self.mapPartitions(sortPartition, True)

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

    def _spread(self: "RDD[T]", minPartitions: int) -> "RDD[T]":
        """
        This RDD in at least ``minPartitions`` partitions, as the file sources of Spark Classic
        split their files into at least ``minPartitions`` splits: when it has fewer partitions,
        each partition is split into partitions of consecutive elements, about as many elements
        each. One job counts the elements of each partition.
        """
        num = self.getNumPartitions()
        if num >= minPartitions:
            return self
        counts = dict(
            self.mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))]).collect()
        )
        # The number of partitions each partition is split into: at least one, and the rest of
        # the partitions in proportion to the number of elements, the largest remainders first.
        pieces = [1] * num
        extra = minPartitions - num
        total = sum(counts.values())
        if total > 0:
            shares = [extra * counts.get(i, 0) / total for i in range(num)]
            for i in range(num):
                pieces[i] += int(shares[i])
            by_remainder = sorted(
                range(num), key=lambda i: shares[i] - int(shares[i]), reverse=True
            )
            for i in by_remainder[: minPartitions - sum(pieces)]:
                pieces[i] += 1
        else:
            pieces[0] += extra
        starts = [sum(pieces[:i]) for i in range(num)]
        outputSerializer = self.ctx._unbatched_serializer

        def split_partition(split: int, iterator: Iterable[T]) -> Iterable[bytes]:
            # The element at position j of a partition of c elements goes to its piece
            # j * k // c, so that the pieces hold consecutive elements, in order.
            k, c = pieces[split], counts.get(split, 0)
            batch: List[T] = []
            piece = 0
            for j, x in enumerate(iterator):
                target = j * k // c
                if target != piece or len(batch) >= 1024:
                    if batch:
                        yield pack_long(starts[split] + piece)
                        yield outputSerializer.dumps(batch)
                    batch = []
                    piece = target
                batch.append(x)
            if batch:
                yield pack_long(starts[split] + piece)
                yield outputSerializer.dumps(batch)

        # Each partition of the output gets the elements of one partition of this RDD, in order.
        return self._shuffle(self.mapPartitionsWithIndex(split_partition), minPartitions, None)

    # -- Combining RDDs ------------------------------------------------------------------------

    def union(self: "RDD[T]", other: "RDDFacade[U]") -> "RDD[Union[T, U]]":
        if not isinstance(other, RDD):
            raise TypeError("Cannot union a Spark Connect RDD with %s" % type(other).__name__)
        if self._jrdd_deserializer == other._jrdd_deserializer:
            rdd: "RDD[Union[T, U]]" = _union(self._cctx, [self, other], self._jrdd_deserializer)
        else:
            # These RDDs contain data in different serialized formats, so we
            # must normalize them to the default serializer.
            self_copy = self._reserialize()
            other_copy = other._reserialize()
            rdd = _union(self._cctx, [self_copy, other_copy], self.ctx.serializer)
        if (
            self.partitioner == other.partitioner
            and self.getNumPartitions() == rdd.getNumPartitions()
        ):
            rdd.partitioner = self.partitioner
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

    def cartesian(self: "RDD[T]", other: "RDDFacade[U]") -> "RDD[Tuple[T, U]]":
        if not isinstance(other, RDD):
            raise TypeError("Cannot compute the cartesian product with %s" % type(other).__name__)
        left_df, right_df = self._df, other._df

        def cartesian(relation: pb2.RDDRelation, session: Any) -> None:
            relation.cartesian.left.CopyFrom(_relation(left_df, session))
            relation.cartesian.right.CopyFrom(_relation(right_df, session))

        left_num, right_num = self._num_partitions, other._num_partitions
        num = None if left_num is None or right_num is None else left_num * right_num
        # Due to batching, we can't use the Java cartesian method.
        deserializer = CartesianDeserializer(self._jrdd_deserializer, other._jrdd_deserializer)
        return self._new(cartesian, deserializer, num)

    def zip(self: "RDD[T]", other: "RDDFacade[U]") -> "RDD[Tuple[T, U]]":
        if not isinstance(other, RDD):
            raise TypeError("Cannot zip with %s" % type(other).__name__)

        def get_batch_size(ser: Serializer) -> int:
            if isinstance(ser, BatchedSerializer):
                return ser.batchSize
            return 1  # not batched

        def batch_as(rdd: "RDD[Any]", batchSize: int) -> "RDD[Any]":
            return rdd._reserialize(BatchedSerializer(CPickleSerializer(), batchSize))

        my_batch = get_batch_size(self._jrdd_deserializer)
        other_batch = get_batch_size(other._jrdd_deserializer)
        if my_batch != other_batch or not my_batch:
            # use the smallest batchSize for both of them
            batchSize = min(my_batch, other_batch)
            if batchSize <= 0:
                # auto batched or unlimited
                batchSize = 100
            other = batch_as(other, batchSize)
            self = batch_as(self, batchSize)

        num = self.getNumPartitions()
        if num != other.getNumPartitions():
            raise ValueError("Can only zip with RDD which has the same number of partitions")

        # There will be an Exception in JVM if there are different number
        # of items in each partitions.
        left_df, right_df = self._df, other._df

        def zip_partitions(relation: pb2.RDDRelation, session: Any) -> None:
            relation.zip_partitions.inputs.append(_relation(left_df, session))
            relation.zip_partitions.inputs.append(_relation(right_df, session))

        deserializer = PairDeserializer(self._jrdd_deserializer, other._jrdd_deserializer)
        return self._new(zip_partitions, deserializer, num)

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

        serializer = self.ctx.serializer
        memory = self._memory_limit()
        agg = Aggregator(createCombiner, mergeValue, mergeCombiners)

        def combineLocally(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, U]]:
            merger = ExternalMerger[K, V, U](agg, memory * 0.9, serializer)
            merger.mergeValues(iterator)
            return merger.items()

        locally_combined = self.mapPartitions(combineLocally, preservesPartitioning=True)
        shuffled = locally_combined.partitionBy(numPartitions, partitionFunc)

        def _mergeCombiners(iterator: Iterable[Tuple[K, U]]) -> Iterable[Tuple[K, U]]:
            merger = ExternalMerger[K, V, U](agg, memory, serializer)
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
        serializer = self._jrdd_deserializer
        agg = Aggregator(createCombiner, mergeValue, mergeCombiners)

        def combine(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, List[V]]]:
            merger = ExternalMerger[K, V, list[V]](agg, memory * 0.9, serializer)
            merger.mergeValues(iterator)
            return merger.items()

        locally_combined = self.mapPartitions(combine, preservesPartitioning=True)
        shuffled = locally_combined.partitionBy(numPartitions, partitionFunc)

        def groupByKey(it: Iterable[Tuple[K, List[V]]]) -> Iterable[Tuple[K, "SizedIterable[V]"]]:
            merger = ExternalGroupBy[K, V](agg, memory, serializer)  # type: ignore[arg-type]
            merger.mergeCombiners(it)
            return merger.items()

        return shuffled.mapPartitions(groupByKey, True).mapValues(ResultIterable)

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
        return self._collect()

    def collectWithJobGroup(
        self: "RDD[T]", groupId: str, description: str, interruptOnCancel: bool = False
    ) -> "List[T]":
        """
        Collect with the job group ``groupId``: on Spark Connect, the jobs run with the operation
        tag ``groupId``, which :meth:`SparkContext.cancelJobGroup` cancels. ``description`` and
        ``interruptOnCancel`` are not used.
        """
        warnings.warn(
            "Deprecated in 3.1, Use pyspark.InheritableThread with the pinned thread mode enabled.",
            FutureWarning,
        )
        self._cctx._session.addTag(groupId)
        try:
            return self.collect()
        finally:
            self._cctx._discard_connect_job_tag_if_present(groupId)

    def toLocalIterator(self: "RDD[T]", prefetchPartitions: bool = False) -> Iterator[T]:
        """
        Iterate over the elements one partition at a time. Each partition is computed by its own
        job when it is reached, or while the previous one is consumed with ``prefetchPartitions``,
        as in Spark Classic.
        """
        num = self.getNumPartitions()
        if not prefetchPartitions:
            for split in range(num):
                yield from self._collect([split])
            return
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._collect, [0]) if num > 0 else None
            for split in range(num):
                assert future is not None
                items = future.result()
                if split + 1 < num:
                    future = executor.submit(self._collect, [split + 1])
                yield from items

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

        # collecting result of mapPartitions here ensures that the copy of
        # zeroValue provided to each partition is unique from the one provided
        # to the final reduce call
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

        # collecting result of mapPartitions here ensures that the copy of
        # zeroValue provided to each partition is unique from the one provided
        # to the final reduce call
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
        scale = max(int(ceil(pow(numPartitions, 1.0 / depth))), 2)
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

    def sum(self: "RDD[NumberOrArray]") -> "NumberOrArray":
        return self.mapPartitions(lambda x: [sum(x)]).fold(  # type: ignore[return-value]
            0, operator.add
        )

    def count(self) -> int:
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()

    def stats(self: "RDD[NumberOrArray]") -> StatCounter:
        def redFunc(left_counter: StatCounter, right_counter: StatCounter) -> StatCounter:
            return left_counter.mergeStats(right_counter)

        return self.mapPartitions(lambda i: [StatCounter(i)]).reduce(  # type: ignore[arg-type]
            redFunc
        )

    def histogram(
        self: "RDD[S]", buckets: Union[int, List["S"], Tuple["S", ...]]
    ) -> Tuple[Sequence["S"], List[int]]:
        if isinstance(buckets, int):
            if buckets < 1:
                raise ValueError("number of buckets must be >= 1")

            # filter out non-comparable elements
            def comparable(x: Any) -> bool:
                if x is None:
                    return False
                if type(x) is float and isnan(x):
                    return False
                return True

            filtered = self.filter(comparable)

            # faster than stats()
            def minmax(a: Tuple["S", "S"], b: Tuple["S", "S"]) -> Tuple["S", "S"]:
                return min(a[0], b[0]), max(a[1], b[1])

            try:
                minv, maxv = filtered.map(lambda x: (x, x)).reduce(minmax)
            except TypeError as e:
                if " empty " in str(e):
                    raise ValueError("can not generate buckets from empty RDD")
                raise

            if minv == maxv or buckets == 1:
                return [minv, maxv], [filtered.count()]

            try:
                inc = (maxv - minv) / buckets  # type: ignore[operator]
            except TypeError:
                raise TypeError("Can not generate buckets with non-number in RDD")

            if isinf(inc):
                raise ValueError("Can not generate buckets with infinite value")

            # keep them as integer if possible
            inc = int(inc)
            if inc * buckets != maxv - minv:  # type: ignore[operator]
                inc = (maxv - minv) * 1.0 / buckets  # type: ignore[operator]

            buckets = [i * inc + minv for i in range(buckets)]
            buckets.append(maxv)  # fix accumulated error
            even = True

        elif isinstance(buckets, (list, tuple)):
            if len(buckets) < 2:
                raise ValueError("buckets should have more than one value")

            if any(i is None or isinstance(i, float) and isnan(i) for i in buckets):
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
                steps = [
                    buckets[i + 1] - buckets[i]  # type: ignore[operator]
                    for i in range(len(buckets) - 1)
                ]
            except TypeError:
                pass  # objects in buckets do not support '-'
            else:
                if max(steps) - min(steps) < 1e-10:  # handle precision errors
                    even = True
                    inc = (maxv - minv) / (len(buckets) - 1)  # type: ignore[operator]

        else:
            raise TypeError("buckets should be a list or tuple or number(int or long)")

        def histogram(iterator: Iterable["S"]) -> Iterable[List[int]]:
            counters = [0] * len(buckets)
            for i in iterator:
                if i is None or (isinstance(i, float) and isnan(i)) or i > maxv or i < minv:
                    continue
                t = (
                    int((i - minv) / inc)  # type: ignore[operator]
                    if even
                    else bisect.bisect_right(buckets, i) - 1
                )
                counters[t] += 1
            # add last two together
            last = counters.pop()
            counters[-1] += last
            return [counters]

        def mergeCounters(a: List[int], b: List[int]) -> List[int]:
            return [i + j for i, j in zip(a, b)]

        return buckets, self.mapPartitions(histogram).reduce(mergeCounters)

    def mean(self: "RDD[NumberOrArray]") -> float:
        return self.stats().mean()

    def variance(self: "RDD[NumberOrArray]") -> float:
        return self.stats().variance()

    def stdev(self: "RDD[NumberOrArray]") -> float:
        return self.stats().stdev()

    def sampleStdev(self: "RDD[NumberOrArray]") -> float:
        return self.stats().sampleStdev()

    def sampleVariance(self: "RDD[NumberOrArray]") -> float:
        return self.stats().sampleVariance()

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

    def top(self: "RDD[T]", num: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        def topIterator(iterator: Iterable[T]) -> Iterable[List[T]]:
            yield heapq.nlargest(num, iterator, key=key)

        def merge(a: List[T], b: List[T]) -> List[T]:
            return heapq.nlargest(num, a + b, key=key)

        return self.mapPartitions(topIterator).reduce(merge)

    def takeOrdered(self: "RDD[T]", num: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        if num < 0:
            raise ValueError("top N cannot be negative.")

        if num == 0 or self.getNumPartitions() == 0:
            return []
        else:

            def merge(a: List[T], b: List[T]) -> List[T]:
                return heapq.nsmallest(num, a + b, key)

            return self.mapPartitions(lambda it: [heapq.nsmallest(num, it, key)]).reduce(merge)

    def take(self: "RDD[T]", num: int) -> List[T]:
        items: List[T] = []
        totalParts = self.getNumPartitions()
        partsScanned = 0

        while len(items) < num and partsScanned < totalParts:
            # The number of partitions to try in this iteration.
            # It is ok for this number to be greater than totalParts because
            # we actually cap it at totalParts in runJob.
            numPartsToTry = 1
            if partsScanned > 0:
                # If we didn't find any rows after the previous iteration,
                # quadruple and retry.  Otherwise, interpolate the number of
                # partitions we need to try, but overestimate it by 50%.
                # We also cap the estimation in the end.
                if len(items) == 0:
                    numPartsToTry = partsScanned * 4
                else:
                    # the first parameter of max is >=1 whenever partsScanned >= 2
                    numPartsToTry = int(1.5 * num * partsScanned / len(items)) - partsScanned
                    numPartsToTry = min(max(numPartsToTry, 1), partsScanned * 4)

            left = num - len(items)

            def takeUpToNumLeft(iterator: Iterable[T]) -> Iterable[T]:
                iterator = iter(iterator)
                taken = 0
                while taken < left:
                    try:
                        yield next(iterator)
                    except StopIteration:
                        return
                    taken += 1

            p = range(partsScanned, min(partsScanned + numPartsToTry, totalParts))
            res = self.context.runJob(self, takeUpToNumLeft, p)

            items += res
            partsScanned += numPartsToTry

        return items[:num]

    def first(self: "RDD[T]") -> T:
        rs = self.take(1)
        if rs:
            return rs[0]
        raise ValueError("RDD is empty")

    def isEmpty(self) -> bool:
        return self.getNumPartitions() == 0 or len(self.take(1)) == 0

    def collectAsMap(self: "RDD[Tuple[K, V]]") -> Dict[K, V]:
        return dict(self.collect())

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

    def countByKey(self: "RDD[Tuple[K, V]]") -> Dict[K, int]:
        return self.map(lambda x: x[0]).countByValue()

    def lookup(self: "RDD[Tuple[K, V]]", key: K) -> List[V]:
        values = self.filter(lambda kv: kv[0] == key).values()

        if self.partitioner is not None:
            return self.ctx.runJob(values, lambda x: x, [self.partitioner(key)])

        return values.collect()

    def countApprox(self, timeout: int, confidence: float = 0.95) -> int:
        drdd = self.mapPartitions(lambda it: [float(sum(1 for i in it))])
        return int(drdd.sumApprox(timeout, confidence))

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
        partials = self.mapPartitions(lambda it: [partial(it)])
        session = self._cctx._session
        tag = "spark-connect-rdd-approx-" + uuid.uuid4().hex
        results: List[Any] = []
        errors: List[BaseException] = []
        done = threading.Event()

        def run() -> None:
            try:
                session.addTag(tag)
                try:
                    for items in self._cctx._iterate_rdd(partials._df, partials._jrdd_deserializer):
                        results.extend(items)
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
        if counter.count() == 1 or isnan(mean_var):
            return BoundedFloat(sum_estimate, 0.0, float("-inf"), float("inf"))
        count_var = (counter.count() + 1) * (1 - p) / (p * p)
        sum_var = (
            mean_estimate * mean_estimate * count_var
            + count_estimate * count_estimate * mean_var
            + mean_var * count_var
        )
        bound = _confidence_factor(confidence, counter.count()) * sqrt(sum_var)
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
        stdev = sqrt(counter.sampleVariance() / counter.count())
        bound = _confidence_factor(confidence, counter.count()) * stdev
        return BoundedFloat(mean, confidence, mean - bound, mean + bound)

    def countApproxDistinct(self: "RDD[T]", relativeSD: float = 0.05) -> int:
        """
        Return the approximate number of distinct elements. On Spark Connect it is counted with
        Spark SQL's ``approx_count_distinct`` of the hashes of the elements.
        """
        from pyspark.sql.connect import functions as F
        from pyspark.sql.types import LongType, StructField, StructType

        if relativeSD < 0.000017:
            raise ValueError("relativeSD should be greater than 0.000017")
        # the hash space in Java is 2^32
        hashes = self.map(lambda x: (portable_hash(x) & 0xFFFFFFFF,))
        df = hashes._to_dataframe(StructType([StructField("h", LongType(), False)]))
        row = df.agg(F.approx_count_distinct("h", relativeSD)).first()
        return int(row[0]) if row is not None and row[0] is not None else 0

    # -- Output --------------------------------------------------------------------------------

    def _to_dataframe(self, struct: Any) -> "DataFrame":
        """
        A DataFrame of this RDD of the internal values of rows of ``struct``, as Spark Classic's
        SparkSession.createDataFrame creates it from an RDD.
        """
        from pyspark.sql.connect.types import pyspark_types_to_proto_types

        pickled_df = self._pickled()._df

        def to_data_frame(relation: pb2.RDDRelation, session: Any) -> None:
            relation.to_data_frame.input.CopyFrom(_relation(pickled_df, session))
            relation.to_data_frame.schema.CopyFrom(pyspark_types_to_proto_types(struct))

        df = _RDDRelationPlan.dataframe(self._cctx, to_data_frame, pickled_df)
        df._cached_schema = struct
        return df

    def _hadoop_not_supported(self, method: str) -> NoReturn:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "RDD.%s on Spark Connect" % method},
        )

    def saveAsNewAPIHadoopDataset(
        self: "RDD[Tuple[K, V]]",
        conf: Dict[str, str],
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
    ) -> None:
        self._hadoop_not_supported("saveAsNewAPIHadoopDataset")

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
        self._hadoop_not_supported("saveAsNewAPIHadoopFile")

    def saveAsHadoopDataset(
        self: "RDD[Tuple[K, V]]",
        conf: Dict[str, str],
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
    ) -> None:
        self._hadoop_not_supported("saveAsHadoopDataset")

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
        self._hadoop_not_supported("saveAsHadoopFile")

    def saveAsSequenceFile(
        self: "RDD[Tuple[K, V]]", path: str, compressionCodecClass: Optional[str] = None
    ) -> None:
        self._hadoop_not_supported("saveAsSequenceFile")

    def saveAsPickleFile(self, path: str, batchSize: int = 10) -> None:
        """
        Save the pickled batches of elements under ``path`` as Parquet, for
        :meth:`SparkContext.pickleFile` on Spark Connect to read back. The files are written by
        the cluster, but Spark Classic cannot read them.
        """
        ser: Serializer
        if batchSize == 0:
            ser = AutoBatchedSerializer(CPickleSerializer())
        else:
            ser = BatchedSerializer(CPickleSerializer(), batchSize)
        self._reserialize(ser)._df.write.parquet(path)

    def saveAsTextFile(self, path: str, compressionCodecClass: Optional[str] = None) -> None:
        """
        Write one line per element, as Spark Classic does, through the DataFrame text writer.
        The path must not exist yet.
        """
        from pyspark.sql.connect import functions as F

        def func(split: int, iterator: Iterable[Any]) -> Iterable[bytes]:
            for x in iterator:
                if isinstance(x, (bytes, bytearray)):
                    # Spark Classic decodes bytes elements as UTF-8 on the JVM, replacing
                    # malformed sequences.
                    yield bytes(x).decode("utf-8", "replace").encode("utf-8")
                elif isinstance(x, str):
                    yield x.encode("utf-8")
                else:
                    yield str(x).encode("utf-8")

        keyed = self.mapPartitionsWithIndex(func)
        keyed._bypass_serializer = True  # type: ignore[attr-defined]
        writer = keyed._df.select(F.col("value").cast("string").alias("value")).write
        if compressionCodecClass:
            writer = writer.option("compression", compressionCodecClass)
        writer.text(path)

    def toDF(
        self: "RDD[Any]", schema: Optional[Any] = None, sampleRatio: Optional[float] = None
    ) -> "DataFrame":
        """
        Converts this ``RDD`` into a :class:`pyspark.sql.DataFrame` through the Spark Connect
        session of its :class:`~pyspark.core.connect.context.SparkContext`.
        """
        return self._cctx._session.createDataFrame(self, schema, sampleRatio)


def _union(ctx: "SparkContext", rdds: Sequence[RDD[Any]], deserializer: Serializer) -> RDD[Any]:
    """
    The union of RDDs, as ``SparkContext.union`` of Spark Core makes it: the RDDs that have
    partitions keep their partitioning if they all have the same partitioner, and are concatenated
    otherwise.
    """
    partitioners = {r._server_partitioner for r in rdds}
    if partitioners == {None}:
        non_empty: Sequence[RDD[Any]] = rdds
        common = None
    else:
        non_empty = [r for r in rdds if r.getNumPartitions() > 0]
        keys = {r._server_partitioner for r in non_empty}
        common = keys.pop() if non_empty and len(keys) == 1 else None
    if common is not None:
        dfs = [r._df for r in non_empty]

        def concatenate(relation: pb2.RDDRelation, session: Any) -> None:
            for df in dfs:
                relation.zip_partitions.inputs.append(_relation(df, session))
            relation.zip_partitions.concatenate = True

        df = _RDDRelationPlan.dataframe(ctx, concatenate, dfs[0])
        return RDD(df, ctx, deserializer, common[0], common)

    dfs = [r._df for r in rdds]

    def union(relation: pb2.RDDRelation, session: Any) -> None:
        for df in dfs:
            relation.union.inputs.append(_relation(df, session))

    counts = [r._num_partitions for r in rdds]
    num = None if any(c is None for c in counts) else sum(cast(List[int], counts))
    return RDD(
        _RDDRelationPlan.dataframe(ctx, union, dfs[0] if dfs else None), ctx, deserializer, num
    )


def _confidence_factor(confidence: float, count: int) -> float:
    """Two-sided normal or Student's t quantile, as used by Spark's approximate evaluators."""
    from statistics import NormalDist

    if confidence >= 1.0:
        return float("inf")
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
        return (2 * p - 1) / sqrt(2 * p * (1 - p))
    # Abramowitz and Stegun 26.7.5, accurate to a few parts in 1e4 for three or more degrees of
    # freedom.
    g1 = (z**3 + z) / 4
    g2 = (5 * z**5 + 16 * z**3 + 3 * z) / 96
    g3 = (3 * z**7 + 19 * z**5 + 17 * z**3 - 15 * z) / 384
    g4 = (79 * z**9 + 776 * z**7 + 1482 * z**5 - 1920 * z**3 - 945 * z) / 92160
    return z + g1 / dof + g2 / dof**2 + g3 / dof**3 + g4 / dof**4


# The keys of the profiles of the functions of RDDs.
_profiler_keys = itertools.count(1)
_profiler_keys_lock = threading.Lock()


class PipelinedRDD(RDD[U], Generic[T, U]):
    """
    Narrow transformations composed into one Python function of the partitions of the RDD of the
    first one, which the server runs in one Python stage, as the PythonRDD of Spark Classic.
    """

    def __init__(
        self,
        prev: RDD[T],
        func: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
        isFromBarrier: bool = False,
    ):
        if not isinstance(prev, PipelinedRDD) or not prev._is_pipelinable():
            # This transformation is the first in its stage:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self._prev_rdd: RDD[Any] = prev
            self._prev_jrdd_deserializer = prev._jrdd_deserializer
        else:
            prev_func: Callable[[int, Iterable[V]], Iterable[T]] = prev.func

            def pipeline_func(split: int, iterator: Iterable[V]) -> Iterable[U]:
                return func(split, prev_func(split, iterator))

            self.func = pipeline_func
            self.preservesPartitioning = prev.preservesPartitioning and preservesPartitioning
            self._prev_rdd = prev._prev_rdd  # maintain the pipeline
            self._prev_jrdd_deserializer = prev._prev_jrdd_deserializer
        self.is_cached = False
        self.has_resource_profile = False
        self.is_checkpointed = False
        self.ctx = prev.ctx
        self.prev = prev
        self._df_cache: Optional["ConnectDataFrame"] = None
        self._id = None
        self._jrdd_deserializer = self.ctx.serializer
        self._bypass_serializer = False
        self._output_pairs = False
        self._name = None
        self._locally_checkpointed = False
        self._resource_profile = None
        self._df_with_resources = None
        self.partitioner = prev.partitioner if self.preservesPartitioning else None
        self._server_partitioner = (
            self._prev_rdd._server_partitioner if self.preservesPartitioning else None
        )
        self.is_barrier = isFromBarrier or prev._is_barrier()

    @property
    def _num_partitions(self) -> Optional[int]:
        return self._prev_rdd._num_partitions

    @_num_partitions.setter
    def _num_partitions(self, value: Optional[int]) -> None:
        self._prev_rdd._num_partitions = value

    def getNumPartitions(self) -> int:
        return self._prev_rdd.getNumPartitions()

    @property
    def _elements_df(self) -> "ConnectDataFrame":
        if self._df_cache is not None:
            return self._df_cache
        if self._bypass_serializer:
            self._jrdd_deserializer = NoOpSerializer()

        with _profiler_keys_lock:
            profiler = _runtime.RDDProfiler("RDD<id=%d>" % next(_profiler_keys))
        command = (self.func, profiler, self._prev_jrdd_deserializer, self._jrdd_deserializer)
        pickled_command = CloudPickleSerializer().dumps(command)
        python_ver = "%d.%d" % sys.version_info[:2]
        hash_seed = self._cctx._python_hash_seed
        input_df = self._prev_rdd._df
        is_barrier = self.is_barrier
        output_pairs = self._output_pairs

        def map_partitions(relation: pb2.RDDRelation, session: Any) -> None:
            mp = relation.map_partitions
            mp.input.CopyFrom(_relation(input_df, session))
            mp.command = pickled_command
            mp.python_ver = python_ver
            mp.is_barrier = is_barrier
            mp.python_hash_seed = hash_seed
            mp.output_pairs = output_pairs

        self._df_cache = _RDDRelationPlan.dataframe(self._cctx, map_partitions, input_df)
        return self._df_cache

    def _set_df(self, df: "ConnectDataFrame") -> None:
        self._df_cache = df

    def _is_pipelinable(self) -> bool:
        return not (self.is_cached or self.is_checkpointed or self.has_resource_profile)

    def _is_barrier(self) -> bool:
        return self.is_barrier


class RDDBarrier(RDDBarrierFacade[T]):
    """
    Wraps an RDD in a barrier stage on Spark Connect.

    .. versionadded:: 5.0.0
    """

    def __init__(self, rdd: RDD[T]) -> None:
        self.rdd = rdd

    def mapPartitions(
        self, f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning: bool = False
    ) -> RDD[Any]:
        def func(_split: int, iterator: Iterable[T]) -> Iterable[U]:
            return f(iterator)

        return PipelinedRDD(self.rdd, func, preservesPartitioning, isFromBarrier=True)

    def mapPartitionsWithIndex(
        self,
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> RDD[Any]:
        return PipelinedRDD(self.rdd, f, preservesPartitioning, isFromBarrier=True)
