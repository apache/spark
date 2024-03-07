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

import copy
import sys
import os
import operator
import shlex
import warnings
import heapq
import bisect
import random
from subprocess import Popen, PIPE
from threading import Thread
from collections import defaultdict
from itertools import chain
from functools import reduce
from math import sqrt, log, isinf, isnan, pow, ceil
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    IO,
    List,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Union,
    TypeVar,
    cast,
    overload,
    TYPE_CHECKING,
)

from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    AutoBatchedSerializer,
    BatchedSerializer,
    NoOpSerializer,
    CartesianDeserializer,
    CloudPickleSerializer,
    PairDeserializer,
    CPickleSerializer,
    Serializer,
    pack_long,
    read_int,
    write_int,
)
from pyspark.join import (
    python_join,
    python_left_outer_join,
    python_right_outer_join,
    python_full_outer_join,
    python_cogroup,
)
from pyspark.statcounter import StatCounter
from pyspark.rddsampler import RDDSampler, RDDRangeSampler, RDDStratifiedSampler
from pyspark.storagelevel import StorageLevel
from pyspark.resource.requests import ExecutorResourceRequests, TaskResourceRequests
from pyspark.resource.profile import ResourceProfile
from pyspark.resultiterable import ResultIterable
from pyspark.shuffle import (
    Aggregator,
    ExternalMerger,
    get_used_memory,
    ExternalSorter,
    ExternalGroupBy,
)
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.util import fail_on_stopiteration, _parse_memory


if TYPE_CHECKING:
    import socket
    import io

    from pyspark._typing import NonUDFType
    from pyspark._typing import S, NumberOrArray
    from pyspark.context import SparkContext
    from pyspark.sql.pandas._typing import (
        PandasScalarUDFType,
        PandasGroupedMapUDFType,
        PandasGroupedAggUDFType,
        PandasWindowAggUDFType,
        PandasScalarIterUDFType,
        PandasMapIterUDFType,
        PandasCogroupedMapUDFType,
        ArrowMapIterUDFType,
        PandasGroupedMapUDFWithStateType,
    )
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import AtomicType, StructType
    from pyspark.sql._typing import AtomicValue, RowLike, SQLBatchedUDFType

    from py4j.java_gateway import JavaObject
    from py4j.java_collections import JavaArray

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
U = TypeVar("U")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")
V1 = TypeVar("V1")
V2 = TypeVar("V2")
V3 = TypeVar("V3")


__all__ = ["RDD"]


class PythonEvalType:
    """
    Evaluation type of python rdd.

    These values are internal to PySpark.

    These values should match values in org.apache.spark.api.python.PythonEvalType.
    """

    NON_UDF: "NonUDFType" = 0

    SQL_BATCHED_UDF: "SQLBatchedUDFType" = 100

    SQL_SCALAR_PANDAS_UDF: "PandasScalarUDFType" = 200
    SQL_GROUPED_MAP_PANDAS_UDF: "PandasGroupedMapUDFType" = 201
    SQL_GROUPED_AGG_PANDAS_UDF: "PandasGroupedAggUDFType" = 202
    SQL_WINDOW_AGG_PANDAS_UDF: "PandasWindowAggUDFType" = 203
    SQL_SCALAR_PANDAS_ITER_UDF: "PandasScalarIterUDFType" = 204
    SQL_MAP_PANDAS_ITER_UDF: "PandasMapIterUDFType" = 205
    SQL_COGROUPED_MAP_PANDAS_UDF: "PandasCogroupedMapUDFType" = 206
    SQL_MAP_ARROW_ITER_UDF: "ArrowMapIterUDFType" = 207
    SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE: "PandasGroupedMapUDFWithStateType" = 208


def portable_hash(x: Hashable) -> int:
    """
    This function returns consistent hash code for builtin types, especially
    for None and tuple with None.

    The algorithm is similar to that one used by CPython 2.7

    Examples
    --------
    >>> portable_hash(None)
    0
    >>> portable_hash((None, 1)) & 0xffffffff
    219750521
    """

    if "PYTHONHASHSEED" not in os.environ:
        raise RuntimeError("Randomness of hash of string should be disabled via PYTHONHASHSEED")

    if x is None:
        return 0
    if isinstance(x, tuple):
        h = 0x345678
        for i in x:
            h ^= portable_hash(i)
            h *= 1000003
            h &= sys.maxsize
        h ^= len(x)
        if h == -1:
            h = -2
        return int(h)
    return hash(x)


class BoundedFloat(float):
    """
    Bounded value is generated by approximate job, with confidence and low
    bound and high bound.

    Examples
    --------
    >>> BoundedFloat(100.0, 0.95, 95.0, 105.0)
    100.0
    """

    confidence: float
    low: float
    high: float

    def __new__(cls, mean: float, confidence: float, low: float, high: float) -> "BoundedFloat":
        obj = float.__new__(cls, mean)
        obj.confidence = confidence
        obj.low = low
        obj.high = high
        return obj


def _create_local_socket(sock_info: "JavaArray") -> "io.BufferedRWPair":
    """
    Create a local socket that can be used to load deserialized data from the JVM

    Parameters
    ----------
    sock_info : tuple
        Tuple containing port number and authentication secret for a local socket.

    Returns
    -------
    sockfile file descriptor of the local socket
    """
    sockfile: "io.BufferedRWPair"
    sock: "socket.socket"
    port: int = sock_info[0]
    auth_secret: str = sock_info[1]
    sockfile, sock = local_connect_and_auth(port, auth_secret)
    # The RDD materialization time is unpredictable, if we set a timeout for socket reading
    # operation, it will very possibly fail. See SPARK-18281.
    sock.settimeout(None)
    return sockfile


def _load_from_socket(sock_info: "JavaArray", serializer: Serializer) -> Iterator[Any]:
    """
    Connect to a local socket described by sock_info and use the given serializer to yield data

    Parameters
    ----------
    sock_info : tuple
        Tuple containing port number and authentication secret for a local socket.
    serializer : class:`Serializer`
        The PySpark serializer to use

    Returns
    -------
    result of meth:`Serializer.load_stream`,
    usually a generator that yields deserialized data
    """
    sockfile = _create_local_socket(sock_info)
    # The socket will be automatically closed when garbage-collected.
    return serializer.load_stream(sockfile)


def _local_iterator_from_socket(sock_info: "JavaArray", serializer: Serializer) -> Iterator[Any]:
    class PyLocalIterable:
        """Create a synchronous local iterable over a socket"""

        def __init__(self, _sock_info: "JavaArray", _serializer: Serializer):
            port: int
            auth_secret: str
            jsocket_auth_server: "JavaObject"
            port, auth_secret, self.jsocket_auth_server = _sock_info
            self._sockfile = _create_local_socket((port, auth_secret))
            self._serializer = _serializer
            self._read_iter: Iterator[Any] = iter([])  # Initialize as empty iterator
            self._read_status = 1

        def __iter__(self) -> Iterator[Any]:
            while self._read_status == 1:
                # Request next partition data from Java
                write_int(1, self._sockfile)
                self._sockfile.flush()

                # If response is 1 then there is a partition to read, if 0 then fully consumed
                self._read_status = read_int(self._sockfile)
                if self._read_status == 1:

                    # Load the partition data as a stream and read each item
                    self._read_iter = self._serializer.load_stream(self._sockfile)
                    for item in self._read_iter:
                        yield item

                # An error occurred, join serving thread and raise any exceptions from the JVM
                elif self._read_status == -1:
                    self.jsocket_auth_server.getResult()

        def __del__(self) -> None:
            # If local iterator is not fully consumed,
            if self._read_status == 1:
                try:
                    # Finish consuming partition data stream
                    for _ in self._read_iter:
                        pass
                    # Tell Java to stop sending data and close connection
                    write_int(0, self._sockfile)
                    self._sockfile.flush()
                except Exception:
                    # Ignore any errors, socket is automatically closed when garbage-collected
                    pass

    return iter(PyLocalIterable(sock_info, serializer))


class Partitioner:
    def __init__(self, numPartitions: int, partitionFunc: Callable[[Any], int]):
        self.numPartitions = numPartitions
        self.partitionFunc = partitionFunc

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Partitioner)
            and self.numPartitions == other.numPartitions
            and self.partitionFunc == other.partitionFunc
        )

    def __call__(self, k: Any) -> int:
        return self.partitionFunc(k) % self.numPartitions


class RDD(Generic[T_co]):

    """
    A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
    Represents an immutable, partitioned collection of elements that can be
    operated on in parallel.
    """

    def __init__(
        self,
        jrdd: "JavaObject",
        ctx: "SparkContext",
        jrdd_deserializer: Serializer = AutoBatchedSerializer(CPickleSerializer()),
    ):
        self._jrdd = jrdd
        self.is_cached = False
        self.is_checkpointed = False
        self.has_resource_profile = False
        self.ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer
        self._id = jrdd.id()
        self.partitioner: Optional[Partitioner] = None

    def _pickled(self: "RDD[T]") -> "RDD[T]":
        return self._reserialize(AutoBatchedSerializer(CPickleSerializer()))

    def id(self) -> int:
        """
        A unique ID for this RDD (within its SparkContext).

        .. versionadded:: 0.7.0

        Returns
        -------
        int
            The unique ID for this :class:`RDD`

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd.id()  # doctest: +SKIP
        3
        """
        return self._id

    def __repr__(self) -> str:
        return self._jrdd.toString()

    def __getnewargs__(self) -> NoReturn:
        # This method is called when attempting to pickle an RDD, which is always an error:
        raise RuntimeError(
            "It appears that you are attempting to broadcast an RDD or reference an RDD from an "
            "action or transformation. RDD transformations and actions can only be invoked by the "
            "driver, not inside of other transformations; for example, "
            "rdd1.map(lambda x: rdd2.values.count() * x) is invalid because the values "
            "transformation and count action cannot be performed inside of the rdd1.map "
            "transformation. For more information, see SPARK-5063."
        )

    @property
    def context(self) -> "SparkContext":
        """
        The :class:`SparkContext` that this RDD was created on.

        .. versionadded:: 0.7.0

        Returns
        -------
        :class:`SparkContext`
            The :class:`SparkContext` that this RDD was created on

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd.context
        <SparkContext ...>
        >>> rdd.context is sc
        True
        """
        return self.ctx

    def cache(self: "RDD[T]") -> "RDD[T]":
        """
        Persist this RDD with the default storage level (`MEMORY_ONLY`).

        .. versionadded:: 0.7.0

        Returns
        -------
        :class:`RDD`
            The same :class:`RDD` with storage level set to `MEMORY_ONLY`

        See Also
        --------
        :meth:`RDD.persist`
        :meth:`RDD.unpersist`
        :meth:`RDD.getStorageLevel`

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd2 = rdd.cache()
        >>> rdd2 is rdd
        True
        >>> str(rdd.getStorageLevel())
        'Memory Serialized 1x Replicated'
        >>> _ = rdd.unpersist()
        """
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY)
        return self

    def persist(self: "RDD[T]", storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> "RDD[T]":
        """
        Set this RDD's storage level to persist its values across operations
        after the first time it is computed. This can only be used to assign
        a new storage level if the RDD does not have a storage level set yet.
        If no storage level is specified defaults to (`MEMORY_ONLY`).

        .. versionadded:: 0.9.1

        Parameters
        ----------
        storageLevel : :class:`StorageLevel`, default `MEMORY_ONLY`
            the target storage level

        Returns
        -------
        :class:`RDD`
            The same :class:`RDD` with storage level set to `storageLevel`.

        See Also
        --------
        :meth:`RDD.cache`
        :meth:`RDD.unpersist`
        :meth:`RDD.getStorageLevel`

        Examples
        --------
        >>> rdd = sc.parallelize(["b", "a", "c"])
        >>> rdd.persist().is_cached
        True
        >>> str(rdd.getStorageLevel())
        'Memory Serialized 1x Replicated'
        >>> _ = rdd.unpersist()
        >>> rdd.is_cached
        False

        >>> from pyspark import StorageLevel
        >>> rdd2 = sc.range(5)
        >>> _ = rdd2.persist(StorageLevel.MEMORY_AND_DISK)
        >>> rdd2.is_cached
        True
        >>> str(rdd2.getStorageLevel())
        'Disk Memory Serialized 1x Replicated'

        Can not override existing storage level

        >>> _ = rdd2.persist(StorageLevel.MEMORY_ONLY_2)
        Traceback (most recent call last):
            ...
        py4j.protocol.Py4JJavaError: ...

        Assign another storage level after `unpersist`

        >>> _ = rdd2.unpersist()
        >>> rdd2.is_cached
        False
        >>> _ = rdd2.persist(StorageLevel.MEMORY_ONLY_2)
        >>> str(rdd2.getStorageLevel())
        'Memory Serialized 2x Replicated'
        >>> rdd2.is_cached
        True
        >>> _ = rdd2.unpersist()
        """
        self.is_cached = True
        javaStorageLevel = self.ctx._getJavaStorageLevel(storageLevel)
        self._jrdd.persist(javaStorageLevel)
        return self

    def unpersist(self: "RDD[T]", blocking: bool = False) -> "RDD[T]":
        """
        Mark the RDD as non-persistent, and remove all blocks for it from
        memory and disk.

        .. versionadded:: 0.9.1

        Parameters
        ----------
        blocking : bool, optional, default False
            whether to block until all blocks are deleted

            .. versionadded:: 3.0.0

        Returns
        -------
        :class:`RDD`
            The same :class:`RDD`

        See Also
        --------
        :meth:`RDD.cache`
        :meth:`RDD.persist`
        :meth:`RDD.getStorageLevel`

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd.is_cached
        False
        >>> _ = rdd.unpersist()
        >>> rdd.is_cached
        False
        >>> _ = rdd.cache()
        >>> rdd.is_cached
        True
        >>> _ = rdd.unpersist()
        >>> rdd.is_cached
        False
        >>> _ = rdd.unpersist()
        """
        self.is_cached = False
        self._jrdd.unpersist(blocking)
        return self

    def checkpoint(self) -> None:
        """
        Mark this RDD for checkpointing. It will be saved to a file inside the
        checkpoint directory set with :meth:`SparkContext.setCheckpointDir` and
        all references to its parent RDDs will be removed. This function must
        be called before any job has been executed on this RDD. It is strongly
        recommended that this RDD is persisted in memory, otherwise saving it
        on a file will require recomputation.

        .. versionadded:: 0.7.0

        See Also
        --------
        :meth:`RDD.isCheckpointed`
        :meth:`RDD.getCheckpointFile`
        :meth:`RDD.localCheckpoint`
        :meth:`SparkContext.setCheckpointDir`
        :meth:`SparkContext.getCheckpointDir`

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd.is_checkpointed
        False
        >>> rdd.getCheckpointFile() == None
        True

        >>> rdd.checkpoint()
        >>> rdd.is_checkpointed
        True
        >>> rdd.getCheckpointFile() == None
        True

        >>> rdd.count()
        5
        >>> rdd.is_checkpointed
        True
        >>> rdd.getCheckpointFile() == None
        False
        """
        self.is_checkpointed = True
        self._jrdd.rdd().checkpoint()

    def isCheckpointed(self) -> bool:
        """
        Return whether this RDD is checkpointed and materialized, either reliably or locally.

        .. versionadded:: 0.7.0

        Returns
        -------
        bool
            whether this :class:`RDD` is checkpointed and materialized, either reliably or locally

        See Also
        --------
        :meth:`RDD.checkpoint`
        :meth:`RDD.getCheckpointFile`
        :meth:`SparkContext.setCheckpointDir`
        :meth:`SparkContext.getCheckpointDir`
        """
        return self._jrdd.rdd().isCheckpointed()

    def localCheckpoint(self) -> None:
        """
        Mark this RDD for local checkpointing using Spark's existing caching layer.

        This method is for users who wish to truncate RDD lineages while skipping the expensive
        step of replicating the materialized data in a reliable distributed file system. This is
        useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).

        Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
        data is written to ephemeral local storage in the executors instead of to a reliable,
        fault-tolerant storage. The effect is that if an executor fails during the computation,
        the checkpointed data may no longer be accessible, causing an irrecoverable job failure.

        This is NOT safe to use with dynamic allocation, which removes executors along
        with their cached blocks. If you must use both features, you are advised to set
        `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.

        The checkpoint directory set through :meth:`SparkContext.setCheckpointDir` is not used.

        .. versionadded:: 2.2.0

        See Also
        --------
        :meth:`RDD.checkpoint`
        :meth:`RDD.isLocallyCheckpointed`

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd.isLocallyCheckpointed()
        False

        >>> rdd.localCheckpoint()
        >>> rdd.isLocallyCheckpointed()
        True
        """
        self._jrdd.rdd().localCheckpoint()

    def isLocallyCheckpointed(self) -> bool:
        """
        Return whether this RDD is marked for local checkpointing.

        Exposed for testing.

        .. versionadded:: 2.2.0

        Returns
        -------
        bool
            whether this :class:`RDD` is marked for local checkpointing

        See Also
        --------
        :meth:`RDD.localCheckpoint`
        """
        return self._jrdd.rdd().isLocallyCheckpointed()

    def getCheckpointFile(self) -> Optional[str]:
        """
        Gets the name of the file to which this RDD was checkpointed

        Not defined if RDD is checkpointed locally.

        .. versionadded:: 0.7.0

        Returns
        -------
        str
            the name of the file to which this :class:`RDD` was checkpointed

        See Also
        --------
        :meth:`RDD.checkpoint`
        :meth:`SparkContext.setCheckpointDir`
        :meth:`SparkContext.getCheckpointDir`
        """
        checkpointFile = self._jrdd.rdd().getCheckpointFile()

        return checkpointFile.get() if checkpointFile.isDefined() else None

    def cleanShuffleDependencies(self, blocking: bool = False) -> None:
        """
        Removes an RDD's shuffles and it's non-persisted ancestors.

        When running without a shuffle service, cleaning up shuffle files enables downscaling.
        If you use the RDD after this call, you should checkpoint and materialize it first.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        blocking : bool, optional, default False
           whether to block on shuffle cleanup tasks

        Notes
        -----
        This API is a developer API.
        """
        self._jrdd.rdd().cleanShuffleDependencies(blocking)

    def map(self: "RDD[T]", f: Callable[[T], U], preservesPartitioning: bool = False) -> "RDD[U]":
        """
        Return a new RDD by applying a function to each element of this RDD.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            a function to run on each element of the RDD
        preservesPartitioning : bool, optional, default False
            indicates whether the input function preserves the partitioner,
            which should be False unless this is a pair RDD and the input

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to all elements

        See Also
        --------
        :meth:`RDD.flatMap`
        :meth:`RDD.mapPartitions`
        :meth:`RDD.mapPartitionsWithIndex`
        :meth:`RDD.mapPartitionsWithSplit`

        Examples
        --------
        >>> rdd = sc.parallelize(["b", "a", "c"])
        >>> sorted(rdd.map(lambda x: (x, 1)).collect())
        [('a', 1), ('b', 1), ('c', 1)]
        """

        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return map(fail_on_stopiteration(f), iterator)

        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def flatMap(
        self: "RDD[T]", f: Callable[[T], Iterable[U]], preservesPartitioning: bool = False
    ) -> "RDD[U]":
        """
        Return a new RDD by first applying a function to all elements of this
        RDD, and then flattening the results.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            a function to turn a T into a sequence of U
        preservesPartitioning : bool, optional, default False
            indicates whether the input function preserves the partitioner,
            which should be False unless this is a pair RDD and the input

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to all elements

        See Also
        --------
        :meth:`RDD.map`
        :meth:`RDD.mapPartitions`
        :meth:`RDD.mapPartitionsWithIndex`
        :meth:`RDD.mapPartitionsWithSplit`

        Examples
        --------
        >>> rdd = sc.parallelize([2, 3, 4])
        >>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
        [1, 1, 1, 2, 2, 3]
        >>> sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
        [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
        """

        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return chain.from_iterable(map(fail_on_stopiteration(f), iterator))

        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def mapPartitions(
        self: "RDD[T]", f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning: bool = False
    ) -> "RDD[U]":
        """
        Return a new RDD by applying a function to each partition of this RDD.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            a function to run on each partition of the RDD
        preservesPartitioning : bool, optional, default False
            indicates whether the input function preserves the partitioner,
            which should be False unless this is a pair RDD and the input

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to each partition

        See Also
        --------
        :meth:`RDD.map`
        :meth:`RDD.flatMap`
        :meth:`RDD.mapPartitionsWithIndex`
        :meth:`RDD.mapPartitionsWithSplit`
        :meth:`RDDBarrier.mapPartitions`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> def f(iterator): yield sum(iterator)
        >>> rdd.mapPartitions(f).collect()
        [3, 7]
        """

        def func(_: int, iterator: Iterable[T]) -> Iterable[U]:
            return f(iterator)

        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def mapPartitionsWithIndex(
        self: "RDD[T]",
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> "RDD[U]":
        """
        Return a new RDD by applying a function to each partition of this RDD,
        while tracking the index of the original partition.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            a function to run on each partition of the RDD
        preservesPartitioning : bool, optional, default False
            indicates whether the input function preserves the partitioner,
            which should be False unless this is a pair RDD and the input

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to each partition

        See Also
        --------
        :meth:`RDD.map`
        :meth:`RDD.flatMap`
        :meth:`RDD.mapPartitions`
        :meth:`RDD.mapPartitionsWithSplit`
        :meth:`RDDBarrier.mapPartitionsWithIndex`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4], 4)
        >>> def f(splitIndex, iterator): yield splitIndex
        >>> rdd.mapPartitionsWithIndex(f).sum()
        6
        """
        return PipelinedRDD(self, f, preservesPartitioning)

    def mapPartitionsWithSplit(
        self: "RDD[T]",
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> "RDD[U]":
        """
        Return a new RDD by applying a function to each partition of this RDD,
        while tracking the index of the original partition.

        .. versionadded:: 0.7.0

        .. deprecated:: 0.9.0
            use meth:`RDD.mapPartitionsWithIndex` instead.

        Parameters
        ----------
        f : function
            a function to run on each partition of the RDD
        preservesPartitioning : bool, optional, default False
            indicates whether the input function preserves the partitioner,
            which should be False unless this is a pair RDD and the input

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to each partition

        See Also
        --------
        :meth:`RDD.map`
        :meth:`RDD.flatMap`
        :meth:`RDD.mapPartitions`
        :meth:`RDD.mapPartitionsWithIndex`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4], 4)
        >>> def f(splitIndex, iterator): yield splitIndex
        >>> rdd.mapPartitionsWithSplit(f).sum()
        6
        """
        warnings.warn(
            "mapPartitionsWithSplit is deprecated; use mapPartitionsWithIndex instead",
            FutureWarning,
            stacklevel=2,
        )
        return self.mapPartitionsWithIndex(f, preservesPartitioning)

    def getNumPartitions(self) -> int:
        """
        Returns the number of partitions in RDD

        .. versionadded:: 1.1.0

        Returns
        -------
        int
            number of partitions

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> rdd.getNumPartitions()
        2
        """
        return self._jrdd.partitions().size()

    def filter(self: "RDD[T]", f: Callable[[T], bool]) -> "RDD[T]":
        """
        Return a new RDD containing only the elements that satisfy a predicate.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            a function to run on each element of the RDD

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to each element

        See Also
        --------
        :meth:`RDD.map`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4, 5])
        >>> rdd.filter(lambda x: x % 2 == 0).collect()
        [2, 4]
        """

        def func(iterator: Iterable[T]) -> Iterable[T]:
            return filter(fail_on_stopiteration(f), iterator)

        return self.mapPartitions(func, True)

    def distinct(self: "RDD[T]", numPartitions: Optional[int] = None) -> "RDD[T]":
        """
        Return a new RDD containing the distinct elements in this RDD.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` containing the distinct elements

        See Also
        --------
        :meth:`RDD.countApproxDistinct`

        Examples
        --------
        >>> sorted(sc.parallelize([1, 1, 2, 3]).distinct().collect())
        [1, 2, 3]
        """
        return (
            self.map(lambda x: (x, None))
            .reduceByKey(lambda x, _: x, numPartitions)
            .map(lambda x: x[0])
        )

    def sample(
        self: "RDD[T]", withReplacement: bool, fraction: float, seed: Optional[int] = None
    ) -> "RDD[T]":
        """
        Return a sampled subset of this RDD.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        withReplacement : bool
            can elements be sampled multiple times (replaced when sampled out)
        fraction : float
            expected size of the sample as a fraction of this RDD's size
            without replacement: probability that each element is chosen; fraction must be [0, 1]
            with replacement: expected number of times each element is chosen; fraction must be >= 0
        seed : int, optional
            seed for the random number generator

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` containing a sampled subset of elements

        See Also
        --------
        :meth:`RDD.takeSample`
        :meth:`RDD.sampleByKey`
        :meth:`pyspark.sql.DataFrame.sample`

        Notes
        -----
        This is not guaranteed to provide exactly the fraction specified of the total
        count of the given :class:`DataFrame`.

        Examples
        --------
        >>> rdd = sc.parallelize(range(100), 4)
        >>> 6 <= rdd.sample(False, 0.1, 81).count() <= 14
        True
        """
        if not fraction >= 0:
            raise ValueError("Fraction must be nonnegative.")
        return self.mapPartitionsWithIndex(RDDSampler(withReplacement, fraction, seed).func, True)

    def randomSplit(
        self: "RDD[T]", weights: Sequence[Union[int, float]], seed: Optional[int] = None
    ) -> "List[RDD[T]]":
        """
        Randomly splits this RDD with the provided weights.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        weights : list
            weights for splits, will be normalized if they don't sum to 1
        seed : int, optional
            random seed

        Returns
        -------
        list
            split :class:`RDD`\\s in a list

        See Also
        --------
        :meth:`pyspark.sql.DataFrame.randomSplit`

        Examples
        --------
        >>> rdd = sc.parallelize(range(500), 1)
        >>> rdd1, rdd2 = rdd.randomSplit([2, 3], 17)
        >>> len(rdd1.collect() + rdd2.collect())
        500
        >>> 150 < rdd1.count() < 250
        True
        >>> 250 < rdd2.count() < 350
        True
        """
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
            self.mapPartitionsWithIndex(RDDRangeSampler(lb, ub, seed).func, True)
            for lb, ub in zip(cweights, cweights[1:])
        ]

    # this is ported from scala/spark/RDD.scala
    def takeSample(
        self: "RDD[T]", withReplacement: bool, num: int, seed: Optional[int] = None
    ) -> List[T]:
        """
        Return a fixed-size sampled subset of this RDD.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        withReplacement : bool
            whether sampling is done with replacement
        num : int
            size of the returned sample
        seed : int, optional
            random seed

        Returns
        -------
        list
            a fixed-size sampled subset of this :class:`RDD` in an array

        See Also
        --------
        :meth:`RDD.sample`

        Notes
        -----
        This method should only be used if the resulting array is expected
        to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        >>> import sys
        >>> rdd = sc.parallelize(range(0, 10))
        >>> len(rdd.takeSample(True, 20, 1))
        20
        >>> len(rdd.takeSample(False, 5, 2))
        5
        >>> len(rdd.takeSample(False, 15, 3))
        10
        >>> sc.range(0, 10).takeSample(False, sys.maxsize)
        Traceback (most recent call last):
            ...
        ValueError: Sample size cannot be greater than ...
        """
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
        # See: scala/spark/RDD.scala
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
        """
        Returns a sampling rate that guarantees a sample of
        size >= sampleSizeLowerBound 99.99% of the time.

        How the sampling rate is determined:
        Let p = num / total, where num is the sample size and total is the
        total number of data points in the RDD. We're trying to compute
        q > p such that
          - when sampling with replacement, we're drawing each data point
            with prob_i ~ Pois(q), where we want to guarantee
            Pr[s < num] < 0.0001 for s = sum(prob_i for i from 0 to
            total), i.e. the failure rate of not having a sufficiently large
            sample < 0.0001. Setting q = p + 5 * sqrt(p/total) is sufficient
            to guarantee 0.9999 success rate for num > 12, but we need a
            slightly larger q (9 empirically determined).
          - when sampling without replacement, we're drawing each data point
            with prob_i ~ Binomial(total, fraction) and our choice of q
            guarantees 1-delta, or 0.9999 success rate, where success rate is
            defined the same as in sampling with replacement.
        """
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

    def union(self: "RDD[T]", other: "RDD[U]") -> "RDD[Union[T, U]]":
        """
        Return the union of this RDD and another one.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`

        Returns
        -------
        :class:`RDD`
            the union of this :class:`RDD` and another one

        See Also
        --------
        :meth:`SparkContext.union`
        :meth:`pyspark.sql.DataFrame.union`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> rdd.union(rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        if self._jrdd_deserializer == other._jrdd_deserializer:
            rdd: "RDD[Union[T, U]]" = RDD(
                self._jrdd.union(other._jrdd), self.ctx, self._jrdd_deserializer
            )
        else:
            # These RDDs contain data in different serialized formats, so we
            # must normalize them to the default serializer.
            self_copy = self._reserialize()
            other_copy = other._reserialize()
            rdd = RDD(self_copy._jrdd.union(other_copy._jrdd), self.ctx, self.ctx.serializer)
        if (
            self.partitioner == other.partitioner
            and self.getNumPartitions() == rdd.getNumPartitions()
        ):
            rdd.partitioner = self.partitioner
        return rdd

    def intersection(self: "RDD[T]", other: "RDD[T]") -> "RDD[T]":
        """
        Return the intersection of this RDD and another one. The output will
        not contain any duplicate elements, even if the input RDDs did.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`

        Returns
        -------
        :class:`RDD`
            the intersection of this :class:`RDD` and another one

        See Also
        --------
        :meth:`pyspark.sql.DataFrame.intersect`

        Notes
        -----
        This method performs a shuffle internally.

        Examples
        --------
        >>> rdd1 = sc.parallelize([1, 10, 2, 3, 4, 5])
        >>> rdd2 = sc.parallelize([1, 6, 2, 3, 7, 8])
        >>> rdd1.intersection(rdd2).collect()
        [1, 2, 3]
        """
        return (
            self.map(lambda v: (v, None))
            .cogroup(other.map(lambda v: (v, None)))
            .filter(lambda k_vs: all(k_vs[1]))
            .keys()
        )

    def _reserialize(self: "RDD[T]", serializer: Optional[Serializer] = None) -> "RDD[T]":
        serializer = serializer or self.ctx.serializer
        if self._jrdd_deserializer != serializer:
            self = self.map(lambda x: x, preservesPartitioning=True)
            self._jrdd_deserializer = serializer
        return self

    def __add__(self: "RDD[T]", other: "RDD[U]") -> "RDD[Union[T, U]]":
        """
        Return the union of this RDD and another one.

        Examples
        --------
        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> (rdd + rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        if not isinstance(other, RDD):
            raise TypeError
        return self.union(other)

    @overload
    def repartitionAndSortWithinPartitions(
        self: "RDD[Tuple[S, V]]",
        numPartitions: Optional[int] = ...,
        partitionFunc: Callable[["S"], int] = ...,
        ascending: bool = ...,
    ) -> "RDD[Tuple[S, V]]":
        ...

    @overload
    def repartitionAndSortWithinPartitions(
        self: "RDD[Tuple[K, V]]",
        numPartitions: Optional[int],
        partitionFunc: Callable[[K], int],
        ascending: bool,
        keyfunc: Callable[[K], "S"],
    ) -> "RDD[Tuple[K, V]]":
        ...

    @overload
    def repartitionAndSortWithinPartitions(
        self: "RDD[Tuple[K, V]]",
        numPartitions: Optional[int] = ...,
        partitionFunc: Callable[[K], int] = ...,
        ascending: bool = ...,
        *,
        keyfunc: Callable[[K], "S"],
    ) -> "RDD[Tuple[K, V]]":
        ...

    def repartitionAndSortWithinPartitions(
        self: "RDD[Tuple[Any, Any]]",
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[Any], int] = portable_hash,
        ascending: bool = True,
        keyfunc: Callable[[Any], Any] = lambda x: x,
    ) -> "RDD[Tuple[Any, Any]]":
        """
        Repartition the RDD according to the given partitioner and, within each resulting partition,
        sort records by their keys.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            a function to compute the partition index
        ascending : bool, optional, default True
            sort the keys in ascending or descending order
        keyfunc : function, optional, default identity mapping
            a function to compute the key

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD`

        See Also
        --------
        :meth:`RDD.repartition`
        :meth:`RDD.partitionBy`
        :meth:`RDD.sortBy`
        :meth:`RDD.sortByKey`

        Examples
        --------
        >>> rdd = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)])
        >>> rdd2 = rdd.repartitionAndSortWithinPartitions(2, lambda x: x % 2, True)
        >>> rdd2.glom().collect()
        [[(0, 5), (0, 8), (2, 6)], [(1, 3), (3, 8), (3, 8)]]
        """
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()

        memory = self._memory_limit()
        serializer = self._jrdd_deserializer

        def sortPartition(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, V]]:
            sort = ExternalSorter(memory * 0.9, serializer).sorted
            return iter(sort(iterator, key=lambda k_v: keyfunc(k_v[0]), reverse=(not ascending)))

        return self.partitionBy(numPartitions, partitionFunc).mapPartitions(sortPartition, True)

    @overload
    def sortByKey(
        self: "RDD[Tuple[S, V]]",
        ascending: bool = ...,
        numPartitions: Optional[int] = ...,
    ) -> "RDD[Tuple[K, V]]":
        ...

    @overload
    def sortByKey(
        self: "RDD[Tuple[K, V]]",
        ascending: bool,
        numPartitions: int,
        keyfunc: Callable[[K], "S"],
    ) -> "RDD[Tuple[K, V]]":
        ...

    @overload
    def sortByKey(
        self: "RDD[Tuple[K, V]]",
        ascending: bool = ...,
        numPartitions: Optional[int] = ...,
        *,
        keyfunc: Callable[[K], "S"],
    ) -> "RDD[Tuple[K, V]]":
        ...

    def sortByKey(
        self: "RDD[Tuple[K, V]]",
        ascending: Optional[bool] = True,
        numPartitions: Optional[int] = None,
        keyfunc: Callable[[Any], Any] = lambda x: x,
    ) -> "RDD[Tuple[K, V]]":
        """
        Sorts this RDD, which is assumed to consist of (key, value) pairs.

        .. versionadded:: 0.9.1

        Parameters
        ----------
        ascending : bool, optional, default True
            sort the keys in ascending or descending order
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        keyfunc : function, optional, default identity mapping
            a function to compute the key

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD`

        See Also
        --------
        :meth:`RDD.sortBy`
        :meth:`pyspark.sql.DataFrame.sort`

        Examples
        --------
        >>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
        >>> sc.parallelize(tmp).sortByKey().first()
        ('1', 3)
        >>> sc.parallelize(tmp).sortByKey(True, 1).collect()
        [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
        >>> sc.parallelize(tmp).sortByKey(True, 2).collect()
        [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
        >>> tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
        >>> tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
        >>> sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k: k.lower()).collect()
        [('a', 3), ('fleece', 7), ('had', 2), ('lamb', 5),...('white', 9), ('whose', 6)]
        """
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()

        memory = self._memory_limit()
        serializer = self._jrdd_deserializer

        def sortPartition(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, V]]:
            sort = ExternalSorter(memory * 0.9, serializer).sorted
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
                return numPartitions - 1 - p  # type: ignore[operator]

        return self.partitionBy(numPartitions, rangePartitioner).mapPartitions(sortPartition, True)

    def sortBy(
        self: "RDD[T]",
        keyfunc: Callable[[T], "S"],
        ascending: bool = True,
        numPartitions: Optional[int] = None,
    ) -> "RDD[T]":
        """
        Sorts this RDD by the given keyfunc

        .. versionadded:: 1.1.0

        Parameters
        ----------
        keyfunc : function
            a function to compute the key
        ascending : bool, optional, default True
            sort the keys in ascending or descending order
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD`

        See Also
        --------
        :meth:`RDD.sortByKey`
        :meth:`pyspark.sql.DataFrame.sort`

        Examples
        --------
        >>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
        >>> sc.parallelize(tmp).sortBy(lambda x: x[0]).collect()
        [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
        >>> sc.parallelize(tmp).sortBy(lambda x: x[1]).collect()
        [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
        """
        return (
            self.keyBy(keyfunc)  # type: ignore[type-var]
            .sortByKey(ascending, numPartitions)
            .values()
        )

    def glom(self: "RDD[T]") -> "RDD[List[T]]":
        """
        Return an RDD created by coalescing all elements within each partition
        into a list.

        .. versionadded:: 0.7.0

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` coalescing all elements within each partition into a list

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> sorted(rdd.glom().collect())
        [[1, 2], [3, 4]]
        """

        def func(iterator: Iterable[T]) -> Iterable[List[T]]:
            yield list(iterator)

        return self.mapPartitions(func)

    def cartesian(self: "RDD[T]", other: "RDD[U]") -> "RDD[Tuple[T, U]]":
        """
        Return the Cartesian product of this RDD and another one, that is, the
        RDD of all pairs of elements ``(a, b)`` where ``a`` is in `self` and
        ``b`` is in `other`.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`

        Returns
        -------
        :class:`RDD`
            the Cartesian product of this :class:`RDD` and another one

        See Also
        --------
        :meth:`pyspark.sql.DataFrame.crossJoin`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2])
        >>> sorted(rdd.cartesian(rdd).collect())
        [(1, 1), (1, 2), (2, 1), (2, 2)]
        """
        # Due to batching, we can't use the Java cartesian method.
        deserializer = CartesianDeserializer(self._jrdd_deserializer, other._jrdd_deserializer)
        return RDD(self._jrdd.cartesian(other._jrdd), self.ctx, deserializer)

    def groupBy(
        self: "RDD[T]",
        f: Callable[[T], K],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, Iterable[T]]]":
        """
        Return an RDD of grouped items.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            a function to compute the key
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            a function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` of grouped items

        See Also
        --------
        :meth:`RDD.groupByKey`
        :meth:`pyspark.sql.DataFrame.groupBy`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
        >>> result = rdd.groupBy(lambda x: x % 2).collect()
        >>> sorted([(x, sorted(y)) for (x, y) in result])
        [(0, [2, 8]), (1, [1, 1, 3, 5])]
        """
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions, partitionFunc)

    def pipe(
        self, command: str, env: Optional[Dict[str, str]] = None, checkCode: bool = False
    ) -> "RDD[str]":
        """
        Return an RDD created by piping elements to a forked external process.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        command : str
            command to run.
        env : dict, optional
            environment variables to set.
        checkCode : bool, optional
            whether to check the return value of the shell command.

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` of strings

        Examples
        --------
        >>> sc.parallelize(['1', '2', '', '3']).pipe('cat').collect()
        ['1', '2', '', '3']
        """
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
                    raise RuntimeError(
                        "Pipe function `%s' exited "
                        "with error code %d" % (command, pipe.returncode)
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

    def foreach(self: "RDD[T]", f: Callable[[T], None]) -> None:
        """
        Applies a function to all elements of this RDD.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            a function applyed to each element

        See Also
        --------
        :meth:`RDD.foreachPartition`
        :meth:`pyspark.sql.DataFrame.foreach`
        :meth:`pyspark.sql.DataFrame.foreachPartition`

        Examples
        --------
        >>> def f(x): print(x)
        >>> sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
        """
        f = fail_on_stopiteration(f)

        def processPartition(iterator: Iterable[T]) -> Iterable[Any]:
            for x in iterator:
                f(x)
            return iter([])

        self.mapPartitions(processPartition).count()  # Force evaluation

    def foreachPartition(self: "RDD[T]", f: Callable[[Iterable[T]], None]) -> None:
        """
        Applies a function to each partition of this RDD.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        f : function
            a function applied to each partition

        See Also
        --------
        :meth:`RDD.foreach`
        :meth:`pyspark.sql.DataFrame.foreach`
        :meth:`pyspark.sql.DataFrame.foreachPartition`

        Examples
        --------
        >>> def f(iterator):
        ...     for x in iterator:
        ...          print(x)
        >>> sc.parallelize([1, 2, 3, 4, 5]).foreachPartition(f)
        """

        def func(it: Iterable[T]) -> Iterable[Any]:
            r = f(it)
            try:
                return iter(r)  # type: ignore[call-overload]
            except TypeError:
                return iter([])

        self.mapPartitions(func).count()  # Force evaluation

    def collect(self: "RDD[T]") -> List[T]:
        """
        Return a list that contains all the elements in this RDD.

        .. versionadded:: 0.7.0

        Returns
        -------
        list
            a list containing all the elements

        Notes
        -----
        This method should only be used if the resulting array is expected
        to be small, as all the data is loaded into the driver's memory.

        See Also
        --------
        :meth:`RDD.toLocalIterator`
        :meth:`pyspark.sql.DataFrame.collect`

        Examples
        --------
        >>> sc.range(5).collect()
        [0, 1, 2, 3, 4]
        >>> sc.parallelize(["x", "y", "z"]).collect()
        ['x', 'y', 'z']
        """
        with SCCallSiteSync(self.context):
            assert self.ctx._jvm is not None
            sock_info = self.ctx._jvm.PythonRDD.collectAndServe(self._jrdd.rdd())
        return list(_load_from_socket(sock_info, self._jrdd_deserializer))

    def collectWithJobGroup(
        self: "RDD[T]", groupId: str, description: str, interruptOnCancel: bool = False
    ) -> "List[T]":
        """
        When collect rdd, use this method to specify job group.

        .. versionadded:: 3.0.0

        .. deprecated:: 3.1.0
            Use :class:`pyspark.InheritableThread` with the pinned thread mode enabled.

        Parameters
        ----------
        groupId : str
            The group ID to assign.
        description : str
            The description to set for the job group.
        interruptOnCancel : bool, optional, default False
            whether to interrupt jobs on job cancellation.

        Returns
        -------
        list
            a list containing all the elements

        See Also
        --------
        :meth:`RDD.collect`
        :meth:`SparkContext.setJobGroup`
        """
        warnings.warn(
            "Deprecated in 3.1, Use pyspark.InheritableThread with "
            "the pinned thread mode enabled.",
            FutureWarning,
        )

        with SCCallSiteSync(self.context):
            assert self.ctx._jvm is not None
            sock_info = self.ctx._jvm.PythonRDD.collectAndServeWithJobGroup(
                self._jrdd.rdd(), groupId, description, interruptOnCancel
            )
        return list(_load_from_socket(sock_info, self._jrdd_deserializer))

    def reduce(self: "RDD[T]", f: Callable[[T, T], T]) -> T:
        """
        Reduces the elements of this RDD using the specified commutative and
        associative binary operator. Currently reduces partitions locally.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
            the reduce function

        Returns
        -------
        T
            the aggregated result

        See Also
        --------
        :meth:`RDD.treeReduce`
        :meth:`RDD.aggregate`
        :meth:`RDD.treeAggregate`

        Examples
        --------
        >>> from operator import add
        >>> sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
        15
        >>> sc.parallelize((2 for _ in range(10))).map(lambda x: 1).cache().reduce(add)
        10
        >>> sc.parallelize([]).reduce(add)
        Traceback (most recent call last):
            ...
        ValueError: Can not reduce() empty RDD
        """
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
        """
        Reduces the elements of this RDD in a multi-level tree pattern.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        f : function
            the reduce function
        depth : int, optional, default 2
            suggested depth of the tree (default: 2)

        Returns
        -------
        T
            the aggregated result

        See Also
        --------
        :meth:`RDD.reduce`
        :meth:`RDD.aggregate`
        :meth:`RDD.treeAggregate`

        Examples
        --------
        >>> add = lambda x, y: x + y
        >>> rdd = sc.parallelize([-5, -4, -3, -2, -1, 1, 2, 3, 4], 10)
        >>> rdd.treeReduce(add)
        -5
        >>> rdd.treeReduce(add, 1)
        -5
        >>> rdd.treeReduce(add, 2)
        -5
        >>> rdd.treeReduce(add, 5)
        -5
        >>> rdd.treeReduce(add, 10)
        -5
        """
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
        """
        Aggregate the elements of each partition, and then the results for all
        the partitions, using a given associative function and a neutral "zero value."

        The function ``op(t1, t2)`` is allowed to modify ``t1`` and return it
        as its result value to avoid object allocation; however, it should not
        modify ``t2``.

        This behaves somewhat differently from fold operations implemented
        for non-distributed collections in functional languages like Scala.
        This fold operation may be applied to partitions individually, and then
        fold those results into the final result, rather than apply the fold
        to each element sequentially in some defined ordering. For functions
        that are not commutative, the result may differ from that of a fold
        applied to a non-distributed collection.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        zeroValue : T
            the initial value for the accumulated result of each partition
        op : function
            a function used to both accumulate results within a partition and combine
            results from different partitions

        Returns
        -------
        T
            the aggregated result

        See Also
        --------
        :meth:`RDD.reduce`
        :meth:`RDD.aggregate`

        Examples
        --------
        >>> from operator import add
        >>> sc.parallelize([1, 2, 3, 4, 5]).fold(0, add)
        15
        """
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
        """
        Aggregate the elements of each partition, and then the results for all
        the partitions, using a given combine functions and a neutral "zero
        value."

        The functions ``op(t1, t2)`` is allowed to modify ``t1`` and return it
        as its result value to avoid object allocation; however, it should not
        modify ``t2``.

        The first function (seqOp) can return a different result type, U, than
        the type of this RDD. Thus, we need one operation for merging a T into
        an U and one operation for merging two U

        .. versionadded:: 1.1.0

        Parameters
        ----------
        zeroValue : U
            the initial value for the accumulated result of each partition
        seqOp : function
            a function used to accumulate results within a partition
        combOp : function
            an associative function used to combine results from different partitions

        Returns
        -------
        U
            the aggregated result

        See Also
        --------
        :meth:`RDD.reduce`
        :meth:`RDD.fold`

        Examples
        --------
        >>> seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
        >>> combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
        >>> sc.parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)
        (10, 4)
        >>> sc.parallelize([]).aggregate((0, 0), seqOp, combOp)
        (0, 0)
        """
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
        """
        Aggregates the elements of this RDD in a multi-level tree
        pattern.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        zeroValue : U
            the initial value for the accumulated result of each partition
        seqOp : function
            a function used to accumulate results within a partition
        combOp : function
            an associative function used to combine results from different partitions
        depth : int, optional, default 2
            suggested depth of the tree

        Returns
        -------
        U
            the aggregated result

        See Also
        --------
        :meth:`RDD.aggregate`
        :meth:`RDD.treeReduce`

        Examples
        --------
        >>> add = lambda x, y: x + y
        >>> rdd = sc.parallelize([-5, -4, -3, -2, -1, 1, 2, 3, 4], 10)
        >>> rdd.treeAggregate(0, add, add)
        -5
        >>> rdd.treeAggregate(0, add, add, 1)
        -5
        >>> rdd.treeAggregate(0, add, add, 2)
        -5
        >>> rdd.treeAggregate(0, add, add, 5)
        -5
        >>> rdd.treeAggregate(0, add, add, 10)
        -5
        """
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

    @overload
    def max(self: "RDD[S]") -> "S":
        ...

    @overload
    def max(self: "RDD[T]", key: Callable[[T], "S"]) -> T:
        ...

    def max(self: "RDD[T]", key: Optional[Callable[[T], "S"]] = None) -> T:
        """
        Find the maximum item in this RDD.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        key : function, optional
            A function used to generate key for comparing

        Returns
        -------
        T
            the maximum item

        See Also
        --------
        :meth:`RDD.min`

        Examples
        --------
        >>> rdd = sc.parallelize([1.0, 5.0, 43.0, 10.0])
        >>> rdd.max()
        43.0
        >>> rdd.max(key=str)
        5.0
        """
        if key is None:
            return self.reduce(max)  # type: ignore[arg-type]
        return self.reduce(lambda a, b: max(a, b, key=key))  # type: ignore[arg-type]

    @overload
    def min(self: "RDD[S]") -> "S":
        ...

    @overload
    def min(self: "RDD[T]", key: Callable[[T], "S"]) -> T:
        ...

    def min(self: "RDD[T]", key: Optional[Callable[[T], "S"]] = None) -> T:
        """
        Find the minimum item in this RDD.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        key : function, optional
            A function used to generate key for comparing

        Returns
        -------
        T
            the minimum item

        See Also
        --------
        :meth:`RDD.max`

        Examples
        --------
        >>> rdd = sc.parallelize([2.0, 5.0, 43.0, 10.0])
        >>> rdd.min()
        2.0
        >>> rdd.min(key=str)
        10.0
        """
        if key is None:
            return self.reduce(min)  # type: ignore[arg-type]
        return self.reduce(lambda a, b: min(a, b, key=key))  # type: ignore[arg-type]

    def sum(self: "RDD[NumberOrArray]") -> "NumberOrArray":
        """
        Add up the elements in this RDD.

        .. versionadded:: 0.7.0

        Returns
        -------
        float, int, or complex
            the sum of all elements

        See Also
        --------
        :meth:`RDD.mean`
        :meth:`RDD.sumApprox`

        Examples
        --------
        >>> sc.parallelize([1.0, 2.0, 3.0]).sum()
        6.0
        """
        return self.mapPartitions(lambda x: [sum(x)]).fold(  # type: ignore[return-value]
            0, operator.add
        )

    def count(self) -> int:
        """
        Return the number of elements in this RDD.

        .. versionadded:: 0.7.0

        Returns
        -------
        int
            the number of elements

        See Also
        --------
        :meth:`RDD.countApprox`
        :meth:`pyspark.sql.DataFrame.count`

        Examples
        --------
        >>> sc.parallelize([2, 3, 4]).count()
        3
        """
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()

    def stats(self: "RDD[NumberOrArray]") -> StatCounter:
        """
        Return a :class:`StatCounter` object that captures the mean, variance
        and count of the RDD's elements in one operation.

        .. versionadded:: 0.9.1

        Returns
        -------
        :class:`StatCounter`
            a :class:`StatCounter` capturing the mean, variance and count of all elements

        See Also
        --------
        :meth:`RDD.stdev`
        :meth:`RDD.sampleStdev`
        :meth:`RDD.variance`
        :meth:`RDD.sampleVariance`
        :meth:`RDD.histogram`
        :meth:`pyspark.sql.DataFrame.stat`
        """

        def redFunc(left_counter: StatCounter, right_counter: StatCounter) -> StatCounter:
            return left_counter.mergeStats(right_counter)

        return self.mapPartitions(lambda i: [StatCounter(i)]).reduce(  # type: ignore[arg-type]
            redFunc
        )

    def histogram(
        self: "RDD[S]", buckets: Union[int, List["S"], Tuple["S", ...]]
    ) -> Tuple[Sequence["S"], List[int]]:
        """
        Compute a histogram using the provided buckets. The buckets
        are all open to the right except for the last which is closed.
        e.g. [1,10,20,50] means the buckets are [1,10) [10,20) [20,50],
        which means 1<=x<10, 10<=x<20, 20<=x<=50. And on the input of 1
        and 50 we would have a histogram of 1,0,1.

        If your histogram is evenly spaced (e.g. [0, 10, 20, 30]),
        this can be switched from an O(log n) insertion to O(1) per
        element (where n is the number of buckets).

        Buckets must be sorted, not contain any duplicates, and have
        at least two elements.

        If `buckets` is a number, it will generate buckets which are
        evenly spaced between the minimum and maximum of the RDD. For
        example, if the min value is 0 and the max is 100, given `buckets`
        as 2, the resulting buckets will be [0,50) [50,100]. `buckets` must
        be at least 1. An exception is raised if the RDD contains infinity.
        If the elements in the RDD do not vary (max == min), a single bucket
        will be used.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        buckets : int, or list, or tuple
            if `buckets` is a number, it computes a histogram of the data using
            `buckets` number of buckets evenly, otherwise, `buckets` is the provided
            buckets to bin the data.

        Returns
        -------
        tuple
            a tuple of buckets and histogram

        See Also
        --------
        :meth:`RDD.stats`

        Examples
        --------
        >>> rdd = sc.parallelize(range(51))
        >>> rdd.histogram(2)
        ([0, 25, 50], [25, 26])
        >>> rdd.histogram([0, 5, 25, 50])
        ([0, 5, 25, 50], [5, 20, 26])
        >>> rdd.histogram([0, 15, 30, 45, 60])  # evenly spaced buckets
        ([0, 15, 30, 45, 60], [15, 15, 15, 6])
        >>> rdd = sc.parallelize(["ab", "ac", "b", "bd", "ef"])
        >>> rdd.histogram(("a", "b", "c"))
        (('a', 'b', 'c'), [2, 2])
        """
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
            counters = [0] * len(buckets)  # type: ignore[arg-type]
            for i in iterator:
                if i is None or (isinstance(i, float) and isnan(i)) or i > maxv or i < minv:
                    continue
                t = (
                    int((i - minv) / inc)  # type: ignore[operator]
                    if even
                    else bisect.bisect_right(buckets, i) - 1  # type: ignore[arg-type]
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
        """
        Compute the mean of this RDD's elements.

        .. versionadded:: 0.9.1

        Returns
        -------
        float
            the mean of all elements

        See Also
        --------
        :meth:`RDD.stats`
        :meth:`RDD.sum`
        :meth:`RDD.meanApprox`

        Examples
        --------
        >>> sc.parallelize([1, 2, 3]).mean()
        2.0
        """
        return self.stats().mean()

    def variance(self: "RDD[NumberOrArray]") -> float:
        """
        Compute the variance of this RDD's elements.

        .. versionadded:: 0.9.1

        Returns
        -------
        float
            the variance of all elements

        See Also
        --------
        :meth:`RDD.stats`
        :meth:`RDD.sampleVariance`
        :meth:`RDD.stdev`
        :meth:`RDD.sampleStdev`

        Examples
        --------
        >>> sc.parallelize([1, 2, 3]).variance()
        0.666...
        """
        return self.stats().variance()

    def stdev(self: "RDD[NumberOrArray]") -> float:
        """
        Compute the standard deviation of this RDD's elements.

        .. versionadded:: 0.9.1

        Returns
        -------
        float
            the standard deviation of all elements

        See Also
        --------
        :meth:`RDD.stats`
        :meth:`RDD.sampleStdev`
        :meth:`RDD.variance`
        :meth:`RDD.sampleVariance`

        Examples
        --------
        >>> sc.parallelize([1, 2, 3]).stdev()
        0.816...
        """
        return self.stats().stdev()

    def sampleStdev(self: "RDD[NumberOrArray]") -> float:
        """
        Compute the sample standard deviation of this RDD's elements (which
        corrects for bias in estimating the standard deviation by dividing by
        N-1 instead of N).

        .. versionadded:: 0.9.1

        Returns
        -------
        float
            the sample standard deviation of all elements

        See Also
        --------
        :meth:`RDD.stats`
        :meth:`RDD.stdev`
        :meth:`RDD.variance`
        :meth:`RDD.sampleVariance`

        Examples
        --------
        >>> sc.parallelize([1, 2, 3]).sampleStdev()
        1.0
        """
        return self.stats().sampleStdev()

    def sampleVariance(self: "RDD[NumberOrArray]") -> float:
        """
        Compute the sample variance of this RDD's elements (which corrects
        for bias in estimating the variance by dividing by N-1 instead of N).

        .. versionadded:: 0.9.1

        Returns
        -------
        float
            the sample variance of all elements

        See Also
        --------
        :meth:`RDD.stats`
        :meth:`RDD.variance`
        :meth:`RDD.stdev`
        :meth:`RDD.sampleStdev`

        Examples
        --------
        >>> sc.parallelize([1, 2, 3]).sampleVariance()
        1.0
        """
        return self.stats().sampleVariance()

    def countByValue(self: "RDD[K]") -> Dict[K, int]:
        """
        Return the count of each unique value in this RDD as a dictionary of
        (value, count) pairs.

        .. versionadded:: 0.7.0

        Returns
        -------
        dict
            a dictionary of (value, count) pairs

        See Also
        --------
        :meth:`RDD.collectAsMap`
        :meth:`RDD.countByKey`

        Examples
        --------
        >>> sorted(sc.parallelize([1, 2, 1, 2, 2], 2).countByValue().items())
        [(1, 2), (2, 3)]
        """

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

    @overload
    def top(self: "RDD[S]", num: int) -> List["S"]:
        ...

    @overload
    def top(self: "RDD[T]", num: int, key: Callable[[T], "S"]) -> List[T]:
        ...

    def top(self: "RDD[T]", num: int, key: Optional[Callable[[T], "S"]] = None) -> List[T]:
        """
        Get the top N elements from an RDD.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        num : int
            top N
        key : function, optional
            a function used to generate key for comparing

        Returns
        -------
        list
            the top N elements

        See Also
        --------
        :meth:`RDD.takeOrdered`
        :meth:`RDD.max`
        :meth:`RDD.min`

        Notes
        -----
        This method should only be used if the resulting array is expected
        to be small, as all the data is loaded into the driver's memory.

        It returns the list sorted in descending order.

        Examples
        --------
        >>> sc.parallelize([10, 4, 2, 12, 3]).top(1)
        [12]
        >>> sc.parallelize([2, 3, 4, 5, 6], 2).top(2)
        [6, 5]
        >>> sc.parallelize([10, 4, 2, 12, 3]).top(3, key=str)
        [4, 3, 2]
        """

        def topIterator(iterator: Iterable[T]) -> Iterable[List[T]]:
            yield heapq.nlargest(num, iterator, key=key)

        def merge(a: List[T], b: List[T]) -> List[T]:
            return heapq.nlargest(num, a + b, key=key)

        return self.mapPartitions(topIterator).reduce(merge)

    @overload
    def takeOrdered(self: "RDD[S]", num: int) -> List["S"]:
        ...

    @overload
    def takeOrdered(self: "RDD[T]", num: int, key: Callable[[T], "S"]) -> List[T]:
        ...

    def takeOrdered(self: "RDD[T]", num: int, key: Optional[Callable[[T], "S"]] = None) -> List[T]:
        """
        Get the N elements from an RDD ordered in ascending order or as
        specified by the optional key function.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        num : int
            top N
        key : function, optional
            a function used to generate key for comparing

        Returns
        -------
        list
            the top N elements

        See Also
        --------
        :meth:`RDD.top`
        :meth:`RDD.max`
        :meth:`RDD.min`

        Notes
        -----
        This method should only be used if the resulting array is expected
        to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        >>> sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7]).takeOrdered(6)
        [1, 2, 3, 4, 5, 6]
        >>> sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7], 2).takeOrdered(6, key=lambda x: -x)
        [10, 9, 7, 6, 5, 4]
        >>> sc.emptyRDD().takeOrdered(3)
        []
        """
        if num < 0:
            raise ValueError("top N cannot be negative.")

        if num == 0 or self.getNumPartitions() == 0:
            return []
        else:

            def merge(a: List[T], b: List[T]) -> List[T]:
                return heapq.nsmallest(num, a + b, key)

            return self.mapPartitions(lambda it: [heapq.nsmallest(num, it, key)]).reduce(merge)

    def take(self: "RDD[T]", num: int) -> List[T]:
        """
        Take the first num elements of the RDD.

        It works by first scanning one partition, and use the results from
        that partition to estimate the number of additional partitions needed
        to satisfy the limit.

        Translated from the Scala implementation in RDD#take().

        .. versionadded:: 0.7.0

        Parameters
        ----------
        num : int
            first number of elements

        Returns
        -------
        list
            the first `num` elements

        See Also
        --------
        :meth:`RDD.first`
        :meth:`pyspark.sql.DataFrame.take`

        Notes
        -----
        This method should only be used if the resulting array is expected
        to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        >>> sc.parallelize([2, 3, 4, 5, 6]).cache().take(2)
        [2, 3]
        >>> sc.parallelize([2, 3, 4, 5, 6]).take(10)
        [2, 3, 4, 5, 6]
        >>> sc.parallelize(range(100), 100).filter(lambda x: x > 90).take(3)
        [91, 92, 93]
        """
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
        """
        Return the first element in this RDD.

        .. versionadded:: 0.7.0

        Returns
        -------
        T
            the first element

        See Also
        --------
        :meth:`RDD.take`
        :meth:`pyspark.sql.DataFrame.first`
        :meth:`pyspark.sql.DataFrame.head`

        Examples
        --------
        >>> sc.parallelize([2, 3, 4]).first()
        2
        >>> sc.parallelize([]).first()
        Traceback (most recent call last):
            ...
        ValueError: RDD is empty
        """
        rs = self.take(1)
        if rs:
            return rs[0]
        raise ValueError("RDD is empty")

    def isEmpty(self) -> bool:
        """
        Returns true if and only if the RDD contains no elements at all.

        .. versionadded:: 1.3.0

        Returns
        -------
        bool
            whether the :class:`RDD` is empty

        See Also
        --------
        :meth:`RDD.first`
        :meth:`pyspark.sql.DataFrame.isEmpty`

        Notes
        -----
        An RDD may be empty even when it has at least 1 partition.

        Examples
        --------
        >>> sc.parallelize([]).isEmpty()
        True
        >>> sc.parallelize([1]).isEmpty()
        False
        """
        return self.getNumPartitions() == 0 or len(self.take(1)) == 0

    def saveAsNewAPIHadoopDataset(
        self: "RDD[Tuple[K, V]]",
        conf: Dict[str, str],
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
    ) -> None:
        """
        Output a Python RDD of key-value pairs (of form ``RDD[(K, V)]``) to any Hadoop file
        system, using the new Hadoop OutputFormat API (mapreduce package). Keys/values are
        converted for output using either user specified converters or, by default,
        "org.apache.spark.api.python.JavaToWritableConverter".

        .. versionadded:: 1.1.0

        Parameters
        ----------
        conf : dict
            Hadoop job configuration
        keyConverter : str, optional
            fully qualified classname of key converter (None by default)
        valueConverter : str, optional
            fully qualified classname of value converter (None by default)

        See Also
        --------
        :meth:`SparkContext.newAPIHadoopRDD`
        :meth:`RDD.saveAsHadoopDataset`
        :meth:`RDD.saveAsHadoopFile`
        :meth:`RDD.saveAsNewAPIHadoopFile`
        :meth:`RDD.saveAsSequenceFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> output_format_class = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
        >>> input_format_class = "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
        >>> key_class = "org.apache.hadoop.io.IntWritable"
        >>> value_class = "org.apache.hadoop.io.Text"

        >>> with tempfile.TemporaryDirectory() as d:
        ...     path = os.path.join(d, "new_hadoop_file")
        ...
        ...     # Create the conf for writing
        ...     write_conf = {
        ...         "mapreduce.job.outputformat.class": (output_format_class),
        ...         "mapreduce.job.output.key.class": key_class,
        ...         "mapreduce.job.output.value.class": value_class,
        ...         "mapreduce.output.fileoutputformat.outputdir": path,
        ...     }
        ...
        ...     # Write a temporary Hadoop file
        ...     rdd = sc.parallelize([(1, ""), (1, "a"), (3, "x")])
        ...     rdd.saveAsNewAPIHadoopDataset(conf=write_conf)
        ...
        ...     # Create the conf for reading
        ...     read_conf = {"mapreduce.input.fileinputformat.inputdir": path}
        ...
        ...     # Load this Hadoop file as an RDD
        ...     loaded = sc.newAPIHadoopRDD(input_format_class,
        ...         key_class, value_class, conf=read_conf)
        ...     sorted(loaded.collect())
        [(1, ''), (1, 'a'), (3, 'x')]
        """
        jconf = self.ctx._dictToJavaMap(conf)
        pickledRDD = self._pickled()
        assert self.ctx._jvm is not None

        self.ctx._jvm.PythonRDD.saveAsHadoopDataset(
            pickledRDD._jrdd, True, jconf, keyConverter, valueConverter, True
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
        """
        Output a Python RDD of key-value pairs (of form ``RDD[(K, V)]``) to any Hadoop file
        system, using the new Hadoop OutputFormat API (mapreduce package). Key and value types
        will be inferred if not specified. Keys and values are converted for output using either
        user specified converters or "org.apache.spark.api.python.JavaToWritableConverter". The
        `conf` is applied on top of the base Hadoop conf associated with the SparkContext
        of this RDD to create a merged Hadoop MapReduce job configuration for saving the data.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        path : str
            path to Hadoop file
        outputFormatClass : str
            fully qualified classname of Hadoop OutputFormat
            (e.g. "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
        keyClass : str, optional
            fully qualified classname of key Writable class
             (e.g. "org.apache.hadoop.io.IntWritable", None by default)
        valueClass : str, optional
            fully qualified classname of value Writable class
            (e.g. "org.apache.hadoop.io.Text", None by default)
        keyConverter : str, optional
            fully qualified classname of key converter (None by default)
        valueConverter : str, optional
            fully qualified classname of value converter (None by default)
        conf : dict, optional
            Hadoop job configuration (None by default)

        See Also
        --------
        :meth:`SparkContext.newAPIHadoopFile`
        :meth:`RDD.saveAsHadoopDataset`
        :meth:`RDD.saveAsNewAPIHadoopDataset`
        :meth:`RDD.saveAsHadoopFile`
        :meth:`RDD.saveAsSequenceFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the class of output format

        >>> output_format_class = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"

        >>> with tempfile.TemporaryDirectory() as d:
        ...     path = os.path.join(d, "hadoop_file")
        ...
        ...     # Write a temporary Hadoop file
        ...     rdd = sc.parallelize([(1, {3.0: "bb"}), (2, {1.0: "aa"}), (3, {2.0: "dd"})])
        ...     rdd.saveAsNewAPIHadoopFile(path, output_format_class)
        ...
        ...     # Load this Hadoop file as an RDD
        ...     sorted(sc.sequenceFile(path).collect())
        [(1, {3.0: 'bb'}), (2, {1.0: 'aa'}), (3, {2.0: 'dd'})]
        """
        jconf = self.ctx._dictToJavaMap(conf)
        pickledRDD = self._pickled()
        assert self.ctx._jvm is not None

        self.ctx._jvm.PythonRDD.saveAsNewAPIHadoopFile(
            pickledRDD._jrdd,
            True,
            path,
            outputFormatClass,
            keyClass,
            valueClass,
            keyConverter,
            valueConverter,
            jconf,
        )

    def saveAsHadoopDataset(
        self: "RDD[Tuple[K, V]]",
        conf: Dict[str, str],
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
    ) -> None:
        """
        Output a Python RDD of key-value pairs (of form ``RDD[(K, V)]``) to any Hadoop file
        system, using the old Hadoop OutputFormat API (mapred package). Keys/values are
        converted for output using either user specified converters or, by default,
        "org.apache.spark.api.python.JavaToWritableConverter".

        .. versionadded:: 1.1.0

        Parameters
        ----------
        conf : dict
            Hadoop job configuration
        keyConverter : str, optional
            fully qualified classname of key converter (None by default)
        valueConverter : str, optional
            fully qualified classname of value converter (None by default)

        See Also
        --------
        :meth:`SparkContext.hadoopRDD`
        :meth:`RDD.saveAsNewAPIHadoopDataset`
        :meth:`RDD.saveAsHadoopFile`
        :meth:`RDD.saveAsNewAPIHadoopFile`
        :meth:`RDD.saveAsSequenceFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> output_format_class = "org.apache.hadoop.mapred.TextOutputFormat"
        >>> input_format_class = "org.apache.hadoop.mapred.TextInputFormat"
        >>> key_class = "org.apache.hadoop.io.IntWritable"
        >>> value_class = "org.apache.hadoop.io.Text"

        >>> with tempfile.TemporaryDirectory() as d:
        ...     path = os.path.join(d, "old_hadoop_file")
        ...
        ...     # Create the conf for writing
        ...     write_conf = {
        ...         "mapred.output.format.class": output_format_class,
        ...         "mapreduce.job.output.key.class": key_class,
        ...         "mapreduce.job.output.value.class": value_class,
        ...         "mapreduce.output.fileoutputformat.outputdir": path,
        ...     }
        ...
        ...     # Write a temporary Hadoop file
        ...     rdd = sc.parallelize([(1, ""), (1, "a"), (3, "x")])
        ...     rdd.saveAsHadoopDataset(conf=write_conf)
        ...
        ...     # Create the conf for reading
        ...     read_conf = {"mapreduce.input.fileinputformat.inputdir": path}
        ...
        ...     # Load this Hadoop file as an RDD
        ...     loaded = sc.hadoopRDD(input_format_class, key_class, value_class, conf=read_conf)
        ...     sorted(loaded.collect())
        [(0, '1\\t'), (0, '1\\ta'), (0, '3\\tx')]
        """
        jconf = self.ctx._dictToJavaMap(conf)
        pickledRDD = self._pickled()
        assert self.ctx._jvm is not None

        self.ctx._jvm.PythonRDD.saveAsHadoopDataset(
            pickledRDD._jrdd, True, jconf, keyConverter, valueConverter, False
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
        """
        Output a Python RDD of key-value pairs (of form ``RDD[(K, V)]``) to any Hadoop file
        system, using the old Hadoop OutputFormat API (mapred package). Key and value types
        will be inferred if not specified. Keys and values are converted for output using either
        user specified converters or "org.apache.spark.api.python.JavaToWritableConverter". The
        `conf` is applied on top of the base Hadoop conf associated with the SparkContext
        of this RDD to create a merged Hadoop MapReduce job configuration for saving the data.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        path : str
            path to Hadoop file
        outputFormatClass : str
            fully qualified classname of Hadoop OutputFormat
            (e.g. "org.apache.hadoop.mapred.SequenceFileOutputFormat")
        keyClass : str, optional
            fully qualified classname of key Writable class
            (e.g. "org.apache.hadoop.io.IntWritable", None by default)
        valueClass : str, optional
            fully qualified classname of value Writable class
            (e.g. "org.apache.hadoop.io.Text", None by default)
        keyConverter : str, optional
            fully qualified classname of key converter (None by default)
        valueConverter : str, optional
            fully qualified classname of value converter (None by default)
        conf : dict, optional
            (None by default)
        compressionCodecClass : str
            fully qualified classname of the compression codec class
            i.e. "org.apache.hadoop.io.compress.GzipCodec" (None by default)

        See Also
        --------
        :meth:`SparkContext.hadoopFile`
        :meth:`RDD.saveAsNewAPIHadoopFile`
        :meth:`RDD.saveAsHadoopDataset`
        :meth:`RDD.saveAsNewAPIHadoopDataset`
        :meth:`RDD.saveAsSequenceFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> output_format_class = "org.apache.hadoop.mapred.TextOutputFormat"
        >>> input_format_class = "org.apache.hadoop.mapred.TextInputFormat"
        >>> key_class = "org.apache.hadoop.io.IntWritable"
        >>> value_class = "org.apache.hadoop.io.Text"

        >>> with tempfile.TemporaryDirectory() as d:
        ...     path = os.path.join(d, "old_hadoop_file")
        ...
        ...     # Write a temporary Hadoop file
        ...     rdd = sc.parallelize([(1, ""), (1, "a"), (3, "x")])
        ...     rdd.saveAsHadoopFile(path, output_format_class, key_class, value_class)
        ...
        ...     # Load this Hadoop file as an RDD
        ...     loaded = sc.hadoopFile(path, input_format_class, key_class, value_class)
        ...     sorted(loaded.collect())
        [(0, '1\\t'), (0, '1\\ta'), (0, '3\\tx')]
        """
        jconf = self.ctx._dictToJavaMap(conf)
        pickledRDD = self._pickled()
        assert self.ctx._jvm is not None

        self.ctx._jvm.PythonRDD.saveAsHadoopFile(
            pickledRDD._jrdd,
            True,
            path,
            outputFormatClass,
            keyClass,
            valueClass,
            keyConverter,
            valueConverter,
            jconf,
            compressionCodecClass,
        )

    def saveAsSequenceFile(
        self: "RDD[Tuple[K, V]]", path: str, compressionCodecClass: Optional[str] = None
    ) -> None:
        """
        Output a Python RDD of key-value pairs (of form ``RDD[(K, V)]``) to any Hadoop file
        system, using the "org.apache.hadoop.io.Writable" types that we convert from the
        RDD's key and value types. The mechanism is as follows:

            1. Pickle is used to convert pickled Python RDD into RDD of Java objects.
            2. Keys and values of this Java RDD are converted to Writables and written out.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        path : str
            path to sequence file
        compressionCodecClass : str, optional
            fully qualified classname of the compression codec class
            i.e. "org.apache.hadoop.io.compress.GzipCodec" (None by default)

        See Also
        --------
        :meth:`SparkContext.sequenceFile`
        :meth:`RDD.saveAsHadoopFile`
        :meth:`RDD.saveAsNewAPIHadoopFile`
        :meth:`RDD.saveAsHadoopDataset`
        :meth:`RDD.saveAsNewAPIHadoopDataset`
        :meth:`RDD.saveAsSequenceFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> with tempfile.TemporaryDirectory() as d:
        ...     path = os.path.join(d, "sequence_file")
        ...
        ...     # Write a temporary sequence file
        ...     rdd = sc.parallelize([(1, ""), (1, "a"), (3, "x")])
        ...     rdd.saveAsSequenceFile(path)
        ...
        ...     # Load this sequence file as an RDD
        ...     loaded = sc.sequenceFile(path)
        ...     sorted(loaded.collect())
        [(1, ''), (1, 'a'), (3, 'x')]
        """
        pickledRDD = self._pickled()
        assert self.ctx._jvm is not None

        self.ctx._jvm.PythonRDD.saveAsSequenceFile(
            pickledRDD._jrdd, True, path, compressionCodecClass
        )

    def saveAsPickleFile(self, path: str, batchSize: int = 10) -> None:
        """
        Save this RDD as a SequenceFile of serialized objects. The serializer
        used is :class:`pyspark.serializers.CPickleSerializer`, default batch size
        is 10.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        path : str
            path to pickled file
        batchSize : int, optional, default 10
            the number of Python objects represented as a single Java object.

        See Also
        --------
        :meth:`SparkContext.pickleFile`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     path = os.path.join(d, "pickle_file")
        ...
        ...     # Write a temporary pickled file
        ...     sc.parallelize(range(10)).saveAsPickleFile(path, 3)
        ...
        ...     # Load picked file as an RDD
        ...     sorted(sc.pickleFile(path, 3).collect())
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        ser: Serializer
        if batchSize == 0:
            ser = AutoBatchedSerializer(CPickleSerializer())
        else:
            ser = BatchedSerializer(CPickleSerializer(), batchSize)
        self._reserialize(ser)._jrdd.saveAsObjectFile(path)

    def saveAsTextFile(self, path: str, compressionCodecClass: Optional[str] = None) -> None:
        """
        Save this RDD as a text file, using string representations of elements.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        path : str
            path to text file
        compressionCodecClass : str, optional
            fully qualified classname of the compression codec class
            i.e. "org.apache.hadoop.io.compress.GzipCodec" (None by default)

        See Also
        --------
        :meth:`SparkContext.textFile`
        :meth:`SparkContext.wholeTextFiles`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> from fileinput import input
        >>> from glob import glob
        >>> with tempfile.TemporaryDirectory() as d1:
        ...     path1 = os.path.join(d1, "text_file1")
        ...
        ...     # Write a temporary text file
        ...     sc.parallelize(range(10)).saveAsTextFile(path1)
        ...
        ...     # Load text file as an RDD
        ...     ''.join(sorted(input(glob(path1 + "/part-0000*"))))
        '0\\n1\\n2\\n3\\n4\\n5\\n6\\n7\\n8\\n9\\n'

        Empty lines are tolerated when saving to text files.

        >>> with tempfile.TemporaryDirectory() as d2:
        ...     path2 = os.path.join(d2, "text2_file2")
        ...
        ...     # Write another temporary text file
        ...     sc.parallelize(['', 'foo', '', 'bar', '']).saveAsTextFile(path2)
        ...
        ...     # Load text file as an RDD
        ...     ''.join(sorted(input(glob(path2 + "/part-0000*"))))
        '\\n\\n\\nbar\\nfoo\\n'

        Using compressionCodecClass

        >>> from fileinput import input, hook_compressed
        >>> with tempfile.TemporaryDirectory() as d3:
        ...     path3 = os.path.join(d3, "text3")
        ...     codec = "org.apache.hadoop.io.compress.GzipCodec"
        ...
        ...     # Write another temporary text file with specified codec
        ...     sc.parallelize(['foo', 'bar']).saveAsTextFile(path3, codec)
        ...
        ...     # Load text file as an RDD
        ...     result = sorted(input(glob(path3 + "/part*.gz"), openhook=hook_compressed))
        ...     ''.join([r.decode('utf-8') if isinstance(r, bytes) else r for r in result])
        'bar\\nfoo\\n'
        """

        def func(split: int, iterator: Iterable[Any]) -> Iterable[bytes]:
            for x in iterator:
                if isinstance(x, bytes):
                    yield x
                elif isinstance(x, str):
                    yield x.encode("utf-8")
                else:
                    yield str(x).encode("utf-8")

        keyed = self.mapPartitionsWithIndex(func)
        keyed._bypass_serializer = True  # type: ignore[attr-defined]

        assert self.ctx._jvm is not None

        if compressionCodecClass:
            compressionCodec = self.ctx._jvm.java.lang.Class.forName(compressionCodecClass)
            keyed._jrdd.map(self.ctx._jvm.BytesToString()).saveAsTextFile(path, compressionCodec)
        else:
            keyed._jrdd.map(self.ctx._jvm.BytesToString()).saveAsTextFile(path)

    # Pair functions

    def collectAsMap(self: "RDD[Tuple[K, V]]") -> Dict[K, V]:
        """
        Return the key-value pairs in this RDD to the master as a dictionary.

        .. versionadded:: 0.7.0

        Returns
        -------
        :class:`dict`
            a dictionary of (key, value) pairs

        See Also
        --------
        :meth:`RDD.countByValue`

        Notes
        -----
        This method should only be used if the resulting data is expected
        to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        >>> m = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
        >>> m[1]
        2
        >>> m[3]
        4
        """
        return dict(self.collect())

    def keys(self: "RDD[Tuple[K, V]]") -> "RDD[K]":
        """
        Return an RDD with the keys of each tuple.

        .. versionadded:: 0.7.0

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` only containing the keys

        See Also
        --------
        :meth:`RDD.values`

        Examples
        --------
        >>> rdd = sc.parallelize([(1, 2), (3, 4)]).keys()
        >>> rdd.collect()
        [1, 3]
        """
        return self.map(lambda x: x[0])

    def values(self: "RDD[Tuple[K, V]]") -> "RDD[V]":
        """
        Return an RDD with the values of each tuple.

        .. versionadded:: 0.7.0

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` only containing the values

        See Also
        --------
        :meth:`RDD.keys`

        Examples
        --------
        >>> rdd = sc.parallelize([(1, 2), (3, 4)]).values()
        >>> rdd.collect()
        [2, 4]
        """
        return self.map(lambda x: x[1])

    def reduceByKey(
        self: "RDD[Tuple[K, V]]",
        func: Callable[[V, V], V],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, V]]":
        """
        Merge the values for each key using an associative and commutative reduce function.

        This will also perform the merging locally on each mapper before
        sending results to a reducer, similarly to a "combiner" in MapReduce.

        Output will be partitioned with `numPartitions` partitions, or
        the default parallelism level if `numPartitions` is not specified.
        Default partitioner is hash-partition.

        .. versionadded:: 1.6.0

        Parameters
        ----------
        func : function
            the reduce function
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and the aggregated result for each key

        See Also
        --------
        :meth:`RDD.reduceByKeyLocally`
        :meth:`RDD.combineByKey`
        :meth:`RDD.aggregateByKey`
        :meth:`RDD.foldByKey`
        :meth:`RDD.groupByKey`

        Examples
        --------
        >>> from operator import add
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.reduceByKey(add).collect())
        [('a', 2), ('b', 1)]
        """
        return self.combineByKey(lambda x: x, func, func, numPartitions, partitionFunc)

    def reduceByKeyLocally(self: "RDD[Tuple[K, V]]", func: Callable[[V, V], V]) -> Dict[K, V]:
        """
        Merge the values for each key using an associative and commutative reduce function, but
        return the results immediately to the master as a dictionary.

        This will also perform the merging locally on each mapper before
        sending results to a reducer, similarly to a "combiner" in MapReduce.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        func : function
            the reduce function

        Returns
        -------
        dict
            a dict containing the keys and the aggregated result for each key

        See Also
        --------
        :meth:`RDD.reduceByKey`
        :meth:`RDD.aggregateByKey`

        Examples
        --------
        >>> from operator import add
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.reduceByKeyLocally(add).items())
        [('a', 2), ('b', 1)]
        """
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
        """
        Count the number of elements for each key, and return the result to the
        master as a dictionary.

        .. versionadded:: 0.7.0

        Returns
        -------
        dict
            a dictionary of (key, count) pairs

        See Also
        --------
        :meth:`RDD.collectAsMap`
        :meth:`RDD.countByValue`

        Examples
        --------
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.countByKey().items())
        [('a', 2), ('b', 1)]
        """
        return self.map(lambda x: x[0]).countByValue()

    def join(
        self: "RDD[Tuple[K, V]]",
        other: "RDD[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[V, U]]]":
        """
        Return an RDD containing all pairs of elements with matching keys in
        `self` and `other`.

        Each pair of elements will be returned as a (k, (v1, v2)) tuple, where
        (k, v1) is in `self` and (k, v2) is in `other`.

        Performs a hash join across the cluster.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing all pairs of elements with matching keys

        See Also
        --------
        :meth:`RDD.leftOuterJoin`
        :meth:`RDD.rightOuterJoin`
        :meth:`RDD.fullOuterJoin`
        :meth:`RDD.cogroup`
        :meth:`RDD.groupWith`
        :meth:`pyspark.sql.DataFrame.join`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 1), ("b", 4)])
        >>> rdd2 = sc.parallelize([("a", 2), ("a", 3)])
        >>> sorted(rdd1.join(rdd2).collect())
        [('a', (1, 2)), ('a', (1, 3))]
        """
        return python_join(self, other, numPartitions)

    def leftOuterJoin(
        self: "RDD[Tuple[K, V]]",
        other: "RDD[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[V, Optional[U]]]]":
        """
        Perform a left outer join of `self` and `other`.

        For each element (k, v) in `self`, the resulting RDD will either
        contain all pairs (k, (v, w)) for w in `other`, or the pair
        (k, (v, None)) if no elements in `other` have key k.

        Hash-partitions the resulting RDD into the given number of partitions.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing all pairs of elements with matching keys

        See Also
        --------
        :meth:`RDD.join`
        :meth:`RDD.rightOuterJoin`
        :meth:`RDD.fullOuterJoin`
        :meth:`pyspark.sql.DataFrame.join`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 1), ("b", 4)])
        >>> rdd2 = sc.parallelize([("a", 2)])
        >>> sorted(rdd1.leftOuterJoin(rdd2).collect())
        [('a', (1, 2)), ('b', (4, None))]
        """
        return python_left_outer_join(self, other, numPartitions)

    def rightOuterJoin(
        self: "RDD[Tuple[K, V]]",
        other: "RDD[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[Optional[V], U]]]":
        """
        Perform a right outer join of `self` and `other`.

        For each element (k, w) in `other`, the resulting RDD will either
        contain all pairs (k, (v, w)) for v in this, or the pair (k, (None, w))
        if no elements in `self` have key k.

        Hash-partitions the resulting RDD into the given number of partitions.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing all pairs of elements with matching keys

        See Also
        --------
        :meth:`RDD.join`
        :meth:`RDD.leftOuterJoin`
        :meth:`RDD.fullOuterJoin`
        :meth:`pyspark.sql.DataFrame.join`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 1), ("b", 4)])
        >>> rdd2 = sc.parallelize([("a", 2)])
        >>> sorted(rdd2.rightOuterJoin(rdd1).collect())
        [('a', (2, 1)), ('b', (None, 4))]
        """
        return python_right_outer_join(self, other, numPartitions)

    def fullOuterJoin(
        self: "RDD[Tuple[K, V]]",
        other: "RDD[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[Optional[V], Optional[U]]]]":
        """
        Perform a right outer join of `self` and `other`.

        For each element (k, v) in `self`, the resulting RDD will either
        contain all pairs (k, (v, w)) for w in `other`, or the pair
        (k, (v, None)) if no elements in `other` have key k.

        Similarly, for each element (k, w) in `other`, the resulting RDD will
        either contain all pairs (k, (v, w)) for v in `self`, or the pair
        (k, (None, w)) if no elements in `self` have key k.

        Hash-partitions the resulting RDD into the given number of partitions.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing all pairs of elements with matching keys

        See Also
        --------
        :meth:`RDD.join`
        :meth:`RDD.leftOuterJoin`
        :meth:`RDD.fullOuterJoin`
        :meth:`pyspark.sql.DataFrame.join`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 1), ("b", 4)])
        >>> rdd2 = sc.parallelize([("a", 2), ("c", 8)])
        >>> sorted(rdd1.fullOuterJoin(rdd2).collect())
        [('a', (1, 2)), ('b', (4, None)), ('c', (None, 8))]
        """
        return python_full_outer_join(self, other, numPartitions)

    # TODO: add option to control map-side combining
    # portable_hash is used as default, because builtin hash of None is different
    # cross machines.
    def partitionBy(
        self: "RDD[Tuple[K, V]]",
        numPartitions: Optional[int],
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, V]]":
        """
        Return a copy of the RDD partitioned using the specified partitioner.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` partitioned using the specified partitioner

        See Also
        --------
        :meth:`RDD.repartition`
        :meth:`RDD.repartitionAndSortWithinPartitions`

        Examples
        --------
        >>> pairs = sc.parallelize([1, 2, 3, 4, 2, 4, 1]).map(lambda x: (x, x))
        >>> sets = pairs.partitionBy(2).glom().collect()
        >>> len(set(sets[0]).intersection(set(sets[1])))
        0
        """
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()
        partitioner = Partitioner(numPartitions, partitionFunc)
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
            c, batch = 0, min(10 * numPartitions, 1000)  # type: ignore[operator]

            for k, v in iterator:
                buckets[partitionFunc(k) % numPartitions].append((k, v))  # type: ignore[operator]
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
        keyed._bypass_serializer = True  # type: ignore[attr-defined]
        assert self.ctx._jvm is not None

        with SCCallSiteSync(self.context):
            pairRDD = self.ctx._jvm.PairwiseRDD(keyed._jrdd.rdd()).asJavaPairRDD()
            jpartitioner = self.ctx._jvm.PythonPartitioner(numPartitions, id(partitionFunc))
        jrdd = self.ctx._jvm.PythonRDD.valueOfPair(pairRDD.partitionBy(jpartitioner))
        rdd: "RDD[Tuple[K, V]]" = RDD(jrdd, self.ctx, BatchedSerializer(outputSerializer))
        rdd.partitioner = partitioner
        return rdd

    # TODO: add control over map-side aggregation
    def combineByKey(
        self: "RDD[Tuple[K, V]]",
        createCombiner: Callable[[V], U],
        mergeValue: Callable[[U, V], U],
        mergeCombiners: Callable[[U, U], U],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, U]]":
        """
        Generic function to combine the elements for each key using a custom
        set of aggregation functions.

        Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined
        type" C.

        To avoid memory allocation, both mergeValue and mergeCombiners are allowed to
        modify and return their first argument instead of creating a new C.

        In addition, users can control the partitioning of the output RDD.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        createCombiner : function
            a function to turns a V into a C
        mergeValue : function
            a function to merge a V into a C
        mergeCombiners : function
            a function to combine two C's into a single one
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and the aggregated result for each key

        See Also
        --------
        :meth:`RDD.reduceByKey`
        :meth:`RDD.aggregateByKey`
        :meth:`RDD.foldByKey`
        :meth:`RDD.groupByKey`

        Notes
        -----
        V and C can be different -- for example, one might group an RDD of type
            (Int, Int) into an RDD of type (Int, List[Int]).

        Examples
        --------
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])
        >>> def to_list(a):
        ...     return [a]
        ...
        >>> def append(a, b):
        ...     a.append(b)
        ...     return a
        ...
        >>> def extend(a, b):
        ...     a.extend(b)
        ...     return a
        ...
        >>> sorted(rdd.combineByKey(to_list, append, extend).collect())
        [('a', [1, 2]), ('b', [1])]
        """
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()

        serializer = self.ctx.serializer
        memory = self._memory_limit()
        agg = Aggregator(createCombiner, mergeValue, mergeCombiners)

        def combineLocally(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, U]]:
            merger = ExternalMerger(agg, memory * 0.9, serializer)
            merger.mergeValues(iterator)
            return merger.items()

        locally_combined = self.mapPartitions(combineLocally, preservesPartitioning=True)
        shuffled = locally_combined.partitionBy(numPartitions, partitionFunc)

        def _mergeCombiners(iterator: Iterable[Tuple[K, U]]) -> Iterable[Tuple[K, U]]:
            merger = ExternalMerger(agg, memory, serializer)
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
        """
        Aggregate the values of each key, using given combine functions and a neutral
        "zero value". This function can return a different result type, U, than the type
        of the values in this RDD, V. Thus, we need one operation for merging a V into
        a U and one operation for merging two U's, The former operation is used for merging
        values within a partition, and the latter is used for merging values between
        partitions. To avoid memory allocation, both of these functions are
        allowed to modify and return their first argument instead of creating a new U.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        zeroValue : U
            the initial value for the accumulated result of each partition
        seqFunc : function
            a function to merge a V into a U
        combFunc : function
            a function to combine two U's into a single one
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and the aggregated result for each key

        See Also
        --------
        :meth:`RDD.reduceByKey`
        :meth:`RDD.combineByKey`
        :meth:`RDD.foldByKey`
        :meth:`RDD.groupByKey`

        Examples
        --------
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])
        >>> seqFunc = (lambda x, y: (x[0] + y, x[1] + 1))
        >>> combFunc = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
        >>> sorted(rdd.aggregateByKey((0, 0), seqFunc, combFunc).collect())
        [('a', (3, 2)), ('b', (1, 1))]
        """

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
        """
        Merge the values for each key using an associative function "func"
        and a neutral "zeroValue" which may be added to the result an
        arbitrary number of times, and must not change the result
        (e.g., 0 for addition, or 1 for multiplication.).

        .. versionadded:: 1.1.0

        Parameters
        ----------
        zeroValue : V
            the initial value for the accumulated result of each partition
        func : function
            a function to combine two V's into a single one
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and the aggregated result for each key

        See Also
        --------
        :meth:`RDD.reduceByKey`
        :meth:`RDD.combineByKey`
        :meth:`RDD.aggregateByKey`
        :meth:`RDD.groupByKey`

        Examples
        --------
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> from operator import add
        >>> sorted(rdd.foldByKey(0, add).collect())
        [('a', 2), ('b', 1)]
        """

        def createZero() -> V:
            return copy.deepcopy(zeroValue)

        return self.combineByKey(
            lambda v: func(createZero(), v), func, func, numPartitions, partitionFunc
        )

    def _memory_limit(self) -> int:
        return _parse_memory(self.ctx._conf.get("spark.python.worker.memory", "512m"))

    # TODO: support variant with custom partitioner
    def groupByKey(
        self: "RDD[Tuple[K, V]]",
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> "RDD[Tuple[K, Iterable[V]]]":
        """
        Group the values for each key in the RDD into a single sequence.
        Hash-partitions the resulting RDD with numPartitions partitions.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and the grouped result for each key

        See Also
        --------
        :meth:`RDD.reduceByKey`
        :meth:`RDD.combineByKey`
        :meth:`RDD.aggregateByKey`
        :meth:`RDD.foldByKey`

        Notes
        -----
        If you are grouping in order to perform an aggregation (such as a
        sum or average) over each key, using reduceByKey or aggregateByKey will
        provide much better performance.

        Examples
        --------
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.groupByKey().mapValues(len).collect())
        [('a', 2), ('b', 1)]
        >>> sorted(rdd.groupByKey().mapValues(list).collect())
        [('a', [1, 1]), ('b', [1])]
        """

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
            merger = ExternalMerger(agg, memory * 0.9, serializer)
            merger.mergeValues(iterator)
            return merger.items()

        locally_combined = self.mapPartitions(combine, preservesPartitioning=True)
        shuffled = locally_combined.partitionBy(numPartitions, partitionFunc)

        def groupByKey(it: Iterable[Tuple[K, List[V]]]) -> Iterable[Tuple[K, List[V]]]:
            merger = ExternalGroupBy(agg, memory, serializer)
            merger.mergeCombiners(it)
            return merger.items()

        return shuffled.mapPartitions(groupByKey, True).mapValues(ResultIterable)

    def flatMapValues(
        self: "RDD[Tuple[K, V]]", f: Callable[[V], Iterable[U]]
    ) -> "RDD[Tuple[K, U]]":
        """
        Pass each value in the key-value pair RDD through a flatMap function
        without changing the keys; this also retains the original RDD's
        partitioning.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
           a function to turn a V into a sequence of U

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and the flat-mapped value

        See Also
        --------
        :meth:`RDD.flatMap`
        :meth:`RDD.mapValues`

        Examples
        --------
        >>> rdd = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
        >>> def f(x): return x
        >>> rdd.flatMapValues(f).collect()
        [('a', 'x'), ('a', 'y'), ('a', 'z'), ('b', 'p'), ('b', 'r')]
        """

        def flat_map_fn(kv: Tuple[K, V]) -> Iterable[Tuple[K, U]]:
            return ((kv[0], x) for x in f(kv[1]))

        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def mapValues(self: "RDD[Tuple[K, V]]", f: Callable[[V], U]) -> "RDD[Tuple[K, U]]":
        """
        Pass each value in the key-value pair RDD through a map function
        without changing the keys; this also retains the original RDD's
        partitioning.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        f : function
           a function to turn a V into a U

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and the mapped value

        See Also
        --------
        :meth:`RDD.map`
        :meth:`RDD.flatMapValues`

        Examples
        --------
        >>> rdd = sc.parallelize([("a", ["apple", "banana", "lemon"]), ("b", ["grapes"])])
        >>> def f(x): return len(x)
        >>> rdd.mapValues(f).collect()
        [('a', 3), ('b', 1)]
        """

        def map_values_fn(kv: Tuple[K, V]) -> Tuple[K, U]:
            return kv[0], f(kv[1])

        return self.map(map_values_fn, preservesPartitioning=True)

    @overload
    def groupWith(
        self: "RDD[Tuple[K, V]]", other: "RDD[Tuple[K, V1]]"
    ) -> "RDD[Tuple[K, Tuple[ResultIterable[V], ResultIterable[V1]]]]":
        ...

    @overload
    def groupWith(
        self: "RDD[Tuple[K, V]]", other: "RDD[Tuple[K, V1]]", __o1: "RDD[Tuple[K, V2]]"
    ) -> "RDD[Tuple[K, Tuple[ResultIterable[V], ResultIterable[V1], ResultIterable[V2]]]]":
        ...

    @overload
    def groupWith(
        self: "RDD[Tuple[K, V]]",
        other: "RDD[Tuple[K, V1]]",
        _o1: "RDD[Tuple[K, V2]]",
        _o2: "RDD[Tuple[K, V3]]",
    ) -> """RDD[
        Tuple[
            K,
            Tuple[
                ResultIterable[V],
                ResultIterable[V1],
                ResultIterable[V2],
                ResultIterable[V3],
            ],
        ]
    ]""":
        ...

    def groupWith(  # type: ignore[misc]
        self: "RDD[Tuple[Any, Any]]", other: "RDD[Tuple[Any, Any]]", *others: "RDD[Tuple[Any, Any]]"
    ) -> "RDD[Tuple[Any, Tuple[ResultIterable[Any], ...]]]":
        """
        Alias for cogroup but with support for multiple RDDs.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`
        others : :class:`RDD`
            other :class:`RDD`\\s

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and cogrouped values

        See Also
        --------
        :meth:`RDD.cogroup`
        :meth:`RDD.join`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 5), ("b", 6)])
        >>> rdd2 = sc.parallelize([("a", 1), ("b", 4)])
        >>> rdd3 = sc.parallelize([("a", 2)])
        >>> rdd4 = sc.parallelize([("b", 42)])
        >>> [(x, tuple(map(list, y))) for x, y in
        ...     sorted(list(rdd1.groupWith(rdd2, rdd3, rdd4).collect()))]
        [('a', ([5], [1], [2], [])), ('b', ([6], [4], [], [42]))]

        """
        return python_cogroup((self, other) + others, numPartitions=None)

    # TODO: add variant with custom partitioner
    def cogroup(
        self: "RDD[Tuple[K, V]]",
        other: "RDD[Tuple[K, U]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, Tuple[ResultIterable[V], ResultIterable[U]]]]":
        """
        For each key k in `self` or `other`, return a resulting RDD that
        contains a tuple with the list of values for that key in `self` as
        well as `other`.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the keys and cogrouped values

        See Also
        --------
        :meth:`RDD.groupWith`
        :meth:`RDD.join`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 1), ("b", 4)])
        >>> rdd2 = sc.parallelize([("a", 2)])
        >>> [(x, tuple(map(list, y))) for x, y in sorted(list(rdd1.cogroup(rdd2).collect()))]
        [('a', ([1], [2])), ('b', ([4], []))]
        """
        return python_cogroup((self, other), numPartitions)

    def sampleByKey(
        self: "RDD[Tuple[K, V]]",
        withReplacement: bool,
        fractions: Dict[K, Union[float, int]],
        seed: Optional[int] = None,
    ) -> "RDD[Tuple[K, V]]":
        """
        Return a subset of this RDD sampled by key (via stratified sampling).
        Create a sample of this RDD using variable sampling rates for
        different keys as specified by fractions, a key to sampling rate map.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        withReplacement : bool
            whether to sample with or without replacement
        fractions : dict
            map of specific keys to sampling rates
        seed : int, optional
            seed for the random number generator

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the stratified sampling result

        See Also
        --------
        :meth:`RDD.sample`

        Examples
        --------
        >>> fractions = {"a": 0.2, "b": 0.1}
        >>> rdd = sc.parallelize(fractions.keys()).cartesian(sc.parallelize(range(0, 1000)))
        >>> sample = dict(rdd.sampleByKey(False, fractions, 2).groupByKey().collect())
        >>> 100 < len(sample["a"]) < 300 and 50 < len(sample["b"]) < 150
        True
        >>> max(sample["a"]) <= 999 and min(sample["a"]) >= 0
        True
        >>> max(sample["b"]) <= 999 and min(sample["b"]) >= 0
        True
        """
        for fraction in fractions.values():
            assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        return self.mapPartitionsWithIndex(
            RDDStratifiedSampler(withReplacement, fractions, seed).func, True
        )

    def subtractByKey(
        self: "RDD[Tuple[K, V]]",
        other: "RDD[Tuple[K, Any]]",
        numPartitions: Optional[int] = None,
    ) -> "RDD[Tuple[K, V]]":
        """
        Return each (key, value) pair in `self` that has no pair with matching
        key in `other`.

        .. versionadded:: 0.9.1

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` with the pairs from this whose keys are not in `other`

        See Also
        --------
        :meth:`RDD.subtract`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 2)])
        >>> rdd2 = sc.parallelize([("a", 3), ("c", None)])
        >>> sorted(rdd1.subtractByKey(rdd2).collect())
        [('b', 4), ('b', 5)]
        """

        def filter_func(pair: Tuple[K, Tuple[V, Any]]) -> bool:
            key, (val1, val2) = pair
            return val1 and not val2  # type: ignore[return-value]

        return (
            self.cogroup(other, numPartitions)
            .filter(filter_func)  # type: ignore[arg-type]
            .flatMapValues(lambda x: x[0])
        )

    def subtract(self: "RDD[T]", other: "RDD[T]", numPartitions: Optional[int] = None) -> "RDD[T]":
        """
        Return each value in `self` that is not contained in `other`.

        .. versionadded:: 0.9.1

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` with the elements from this that are not in `other`

        See Also
        --------
        :meth:`RDD.subtractByKey`

        Examples
        --------
        >>> rdd1 = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 3)])
        >>> rdd2 = sc.parallelize([("a", 3), ("c", None)])
        >>> sorted(rdd1.subtract(rdd2).collect())
        [('a', 1), ('b', 4), ('b', 5)]
        """
        # note: here 'True' is just a placeholder
        rdd = other.map(lambda x: (x, True))
        return self.map(lambda x: (x, True)).subtractByKey(rdd, numPartitions).keys()

    def keyBy(self: "RDD[T]", f: Callable[[T], K]) -> "RDD[Tuple[K, T]]":
        """
        Creates tuples of the elements in this RDD by applying `f`.

        .. versionadded:: 0.9.1

        Parameters
        ----------
        f : function
            a function to compute the key

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` with the elements from this that are not in `other`

        See Also
        --------
        :meth:`RDD.map`
        :meth:`RDD.keys`
        :meth:`RDD.values`

        Examples
        --------
        >>> rdd1 = sc.parallelize(range(0,3)).keyBy(lambda x: x*x)
        >>> rdd2 = sc.parallelize(zip(range(0,5), range(0,5)))
        >>> [(x, list(map(list, y))) for x, y in sorted(rdd1.cogroup(rdd2).collect())]
        [(0, [[0], [0]]), (1, [[1], [1]]), (2, [[], [2]]), (3, [[], [3]]), (4, [[2], [4]])]
        """
        return self.map(lambda x: (f(x), x))

    def repartition(self: "RDD[T]", numPartitions: int) -> "RDD[T]":
        """
         Return a new RDD that has exactly numPartitions partitions.

         Can increase or decrease the level of parallelism in this RDD.
         Internally, this uses a shuffle to redistribute data.
         If you are decreasing the number of partitions in this RDD, consider
         using `coalesce`, which can avoid performing a shuffle.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` with exactly numPartitions partitions

        See Also
        --------
        :meth:`RDD.coalesce`
        :meth:`RDD.partitionBy`
        :meth:`RDD.repartitionAndSortWithinPartitions`

        Examples
        --------
         >>> rdd = sc.parallelize([1,2,3,4,5,6,7], 4)
         >>> sorted(rdd.glom().collect())
         [[1], [2, 3], [4, 5], [6, 7]]
         >>> len(rdd.repartition(2).glom().collect())
         2
         >>> len(rdd.repartition(10).glom().collect())
         10
        """
        return self.coalesce(numPartitions, shuffle=True)

    def coalesce(self: "RDD[T]", numPartitions: int, shuffle: bool = False) -> "RDD[T]":
        """
        Return a new RDD that is reduced into `numPartitions` partitions.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        shuffle : bool, optional, default False
            whether to add a shuffle step

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` that is reduced into `numPartitions` partitions

        See Also
        --------
        :meth:`RDD.repartition`

        Examples
        --------
        >>> sc.parallelize([1, 2, 3, 4, 5], 3).glom().collect()
        [[1], [2, 3], [4, 5]]
        >>> sc.parallelize([1, 2, 3, 4, 5], 3).coalesce(1).glom().collect()
        [[1, 2, 3, 4, 5]]
        """
        if not numPartitions > 0:
            raise ValueError("Number of partitions must be positive.")
        if shuffle:
            # Decrease the batch size in order to distribute evenly the elements across output
            # partitions. Otherwise, repartition will possibly produce highly skewed partitions.
            batchSize = min(10, self.ctx._batchSize or 1024)
            ser = BatchedSerializer(CPickleSerializer(), batchSize)
            selfCopy = self._reserialize(ser)
            jrdd_deserializer = selfCopy._jrdd_deserializer
            jrdd = selfCopy._jrdd.coalesce(numPartitions, shuffle)
        else:
            jrdd_deserializer = self._jrdd_deserializer
            jrdd = self._jrdd.coalesce(numPartitions, shuffle)
        return RDD(jrdd, self.ctx, jrdd_deserializer)

    def zip(self: "RDD[T]", other: "RDD[U]") -> "RDD[Tuple[T, U]]":
        """
        Zips this RDD with another one, returning key-value pairs with the
        first element in each RDD second element in each RDD, etc. Assumes
        that the two RDDs have the same number of partitions and the same
        number of elements in each partition (e.g. one was made through
        a map on the other).

        .. versionadded:: 1.0.0

        Parameters
        ----------
        other : :class:`RDD`
            another :class:`RDD`

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the zipped key-value pairs

        See Also
        --------
        :meth:`RDD.zipWithIndex`
        :meth:`RDD.zipWithUniqueId`

        Examples
        --------
        >>> rdd1 = sc.parallelize(range(0,5))
        >>> rdd2 = sc.parallelize(range(1000, 1005))
        >>> rdd1.zip(rdd2).collect()
        [(0, 1000), (1, 1001), (2, 1002), (3, 1003), (4, 1004)]
        """

        def get_batch_size(ser: Serializer) -> int:
            if isinstance(ser, BatchedSerializer):
                return ser.batchSize
            return 1  # not batched

        def batch_as(rdd: "RDD[V]", batchSize: int) -> "RDD[V]":
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

        if self.getNumPartitions() != other.getNumPartitions():
            raise ValueError("Can only zip with RDD which has the same number of partitions")

        # There will be an Exception in JVM if there are different number
        # of items in each partitions.
        pairRDD = self._jrdd.zip(other._jrdd)
        deserializer = PairDeserializer(self._jrdd_deserializer, other._jrdd_deserializer)
        return RDD(pairRDD, self.ctx, deserializer)

    def zipWithIndex(self: "RDD[T]") -> "RDD[Tuple[T, int]]":
        """
        Zips this RDD with its element indices.

        The ordering is first based on the partition index and then the
        ordering of items within each partition. So the first item in
        the first partition gets index 0, and the last item in the last
        partition receives the largest index.

        This method needs to trigger a spark job when this RDD contains
        more than one partitions.

        .. versionadded:: 1.2.0

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the zipped key-index pairs

        See Also
        --------
        :meth:`RDD.zip`
        :meth:`RDD.zipWithUniqueId`

        Examples
        --------
        >>> sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
        [('a', 0), ('b', 1), ('c', 2), ('d', 3)]
        """
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
        """
        Zips this RDD with generated unique Long ids.

        Items in the kth partition will get ids k, n+k, 2*n+k, ..., where
        n is the number of partitions. So there may exist gaps, but this
        method won't trigger a spark job, which is different from
        :meth:`zipWithIndex`.

        .. versionadded:: 1.2.0

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` containing the zipped key-UniqueId pairs

        See Also
        --------
        :meth:`RDD.zip`
        :meth:`RDD.zipWithIndex`

        Examples
        --------
        >>> sc.parallelize(["a", "b", "c", "d", "e"], 3).zipWithUniqueId().collect()
        [('a', 0), ('b', 1), ('c', 4), ('d', 2), ('e', 5)]
        """
        n = self.getNumPartitions()

        def func(k: int, it: Iterable[T]) -> Iterable[Tuple[T, int]]:
            for i, v in enumerate(it):
                yield v, i * n + k

        return self.mapPartitionsWithIndex(func)

    def name(self) -> Optional[str]:
        """
        Return the name of this RDD.

        .. versionadded:: 1.0.0

        Returns
        -------
        str
            :class:`RDD` name

        See Also
        --------
        :meth:`RDD.setName`

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd.name() == None
        True
        """
        n = self._jrdd.name()
        return n if n else None

    def setName(self: "RDD[T]", name: str) -> "RDD[T]":
        """
        Assign a name to this RDD.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        name : str
            new name

        Returns
        -------
        :class:`RDD`
            the same :class:`RDD` with name updated

        See Also
        --------
        :meth:`RDD.name`

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2])
        >>> rdd.setName('I am an RDD').name()
        'I am an RDD'
        """
        self._jrdd.setName(name)
        return self

    def toDebugString(self) -> Optional[bytes]:
        """
        A description of this RDD and its recursive dependencies for debugging.

        .. versionadded:: 1.0.0

        Returns
        -------
        bytes
            debugging information of this :class:`RDD`

        Examples
        --------
        >>> rdd = sc.range(5)
        >>> rdd.toDebugString()
        b'...PythonRDD...ParallelCollectionRDD...'
        """
        debug_string = self._jrdd.toDebugString()

        return debug_string.encode("utf-8") if debug_string else None

    def getStorageLevel(self) -> StorageLevel:
        """
        Get the RDD's current storage level.

        .. versionadded:: 1.0.0

        Returns
        -------
        :class:`StorageLevel`
            current :class:`StorageLevel`

        See Also
        --------
        :meth:`RDD.name`

        Examples
        --------
        >>> rdd = sc.parallelize([1,2])
        >>> rdd.getStorageLevel()
        StorageLevel(False, False, False, False, 1)
        >>> print(rdd.getStorageLevel())
        Serialized 1x Replicated
        """
        java_storage_level = self._jrdd.getStorageLevel()
        storage_level = StorageLevel(
            java_storage_level.useDisk(),
            java_storage_level.useMemory(),
            java_storage_level.useOffHeap(),
            java_storage_level.deserialized(),
            java_storage_level.replication(),
        )
        return storage_level

    def _defaultReducePartitions(self) -> int:
        """
        Returns the default number of partitions to use during reduce tasks (e.g., groupBy).
        If spark.default.parallelism is set, then we'll use the value from SparkContext
        defaultParallelism, otherwise we'll use the number of partitions in this RDD.

        This mirrors the behavior of the Scala Partitioner#defaultPartitioner, intended to reduce
        the likelihood of OOMs. Once PySpark adopts Partitioner-based APIs, this behavior will
        be inherent.
        """
        if self.ctx._conf.contains("spark.default.parallelism"):
            return self.ctx.defaultParallelism
        else:
            return self.getNumPartitions()

    def lookup(self: "RDD[Tuple[K, V]]", key: K) -> List[V]:
        """
        Return the list of values in the RDD for key `key`. This operation
        is done efficiently if the RDD has a known partitioner by only
        searching the partition that the key maps to.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        key : K
            the key to look up

        Returns
        -------
        list
            the list of values in the :class:`RDD` for key `key`

        Examples
        --------
        >>> l = range(1000)
        >>> rdd = sc.parallelize(zip(l, l), 10)
        >>> rdd.lookup(42)  # slow
        [42]
        >>> sorted = rdd.sortByKey()
        >>> sorted.lookup(42)  # fast
        [42]
        >>> sorted.lookup(1024)
        []
        >>> rdd2 = sc.parallelize([(('a', 'b'), 'c')]).groupByKey()
        >>> list(rdd2.lookup(('a', 'b'))[0])
        ['c']
        """
        values = self.filter(lambda kv: kv[0] == key).values()

        if self.partitioner is not None:
            return self.ctx.runJob(values, lambda x: x, [self.partitioner(key)])

        return values.collect()

    def _to_java_object_rdd(self) -> "JavaObject":
        """Return a JavaRDD of Object by unpickling

        It will convert each Python object into Java object by Pickle, whenever the
        RDD is serialized in batch or not.
        """
        rdd = self._pickled()
        assert self.ctx._jvm is not None

        return self.ctx._jvm.SerDeUtil.pythonToJava(rdd._jrdd, True)

    def countApprox(self, timeout: int, confidence: float = 0.95) -> int:
        """
        Approximate version of count() that returns a potentially incomplete
        result within a timeout, even if not all tasks have finished.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        timeout : int
            maximum time to wait for the job, in milliseconds
        confidence : float
            the desired statistical confidence in the result

        Returns
        -------
        int
            a potentially incomplete result, with error bounds

        See Also
        --------
        :meth:`RDD.count`

        Examples
        --------
        >>> rdd = sc.parallelize(range(1000), 10)
        >>> rdd.countApprox(1000, 1.0)
        1000
        """
        drdd = self.mapPartitions(lambda it: [float(sum(1 for i in it))])
        return int(drdd.sumApprox(timeout, confidence))

    def sumApprox(
        self: "RDD[Union[float, int]]", timeout: int, confidence: float = 0.95
    ) -> BoundedFloat:
        """
        Approximate operation to return the sum within a timeout
        or meet the confidence.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        timeout : int
            maximum time to wait for the job, in milliseconds
        confidence : float
            the desired statistical confidence in the result

        Returns
        -------
        :class:`BoundedFloat`
            a potentially incomplete result, with error bounds

        See Also
        --------
        :meth:`RDD.sum`

        Examples
        --------
        >>> rdd = sc.parallelize(range(1000), 10)
        >>> r = sum(range(1000))
        >>> abs(rdd.sumApprox(1000) - r) / r < 0.05
        True
        """
        jrdd = self.mapPartitions(lambda it: [float(sum(it))])._to_java_object_rdd()
        assert self.ctx._jvm is not None
        jdrdd = self.ctx._jvm.JavaDoubleRDD.fromRDD(jrdd.rdd())
        r = jdrdd.sumApprox(timeout, confidence).getFinalValue()
        return BoundedFloat(r.mean(), r.confidence(), r.low(), r.high())

    def meanApprox(
        self: "RDD[Union[float, int]]", timeout: int, confidence: float = 0.95
    ) -> BoundedFloat:
        """
        Approximate operation to return the mean within a timeout
        or meet the confidence.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        timeout : int
            maximum time to wait for the job, in milliseconds
        confidence : float
            the desired statistical confidence in the result

        Returns
        -------
        :class:`BoundedFloat`
            a potentially incomplete result, with error bounds

        See Also
        --------
        :meth:`RDD.mean`

        Examples
        --------
        >>> rdd = sc.parallelize(range(1000), 10)
        >>> r = sum(range(1000)) / 1000.0
        >>> abs(rdd.meanApprox(1000) - r) / r < 0.05
        True
        """
        jrdd = self.map(float)._to_java_object_rdd()
        assert self.ctx._jvm is not None
        jdrdd = self.ctx._jvm.JavaDoubleRDD.fromRDD(jrdd.rdd())
        r = jdrdd.meanApprox(timeout, confidence).getFinalValue()
        return BoundedFloat(r.mean(), r.confidence(), r.low(), r.high())

    def countApproxDistinct(self: "RDD[T]", relativeSD: float = 0.05) -> int:
        """
        Return approximate number of distinct elements in the RDD.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        relativeSD : float, optional
            Relative accuracy. Smaller values create
            counters that require more space.
            It must be greater than 0.000017.

        Returns
        -------
        int
            approximate number of distinct elements

        See Also
        --------
        :meth:`RDD.distinct`

        Notes
        -----
        The algorithm used is based on streamlib's implementation of
        `"HyperLogLog in Practice: Algorithmic Engineering of a State
        of The Art Cardinality Estimation Algorithm", available here
        <https://doi.org/10.1145/2452376.2452456>`_.

        Examples
        --------
        >>> n = sc.parallelize(range(1000)).map(str).countApproxDistinct()
        >>> 900 < n < 1100
        True
        >>> n = sc.parallelize([i % 20 for i in range(1000)]).countApproxDistinct()
        >>> 16 < n < 24
        True
        """
        if relativeSD < 0.000017:
            raise ValueError("relativeSD should be greater than 0.000017")
        # the hash space in Java is 2^32
        hashRDD = self.map(lambda x: portable_hash(x) & 0xFFFFFFFF)
        return hashRDD._to_java_object_rdd().countApproxDistinct(relativeSD)

    def toLocalIterator(self: "RDD[T]", prefetchPartitions: bool = False) -> Iterator[T]:
        """
        Return an iterator that contains all of the elements in this RDD.
        The iterator will consume as much memory as the largest partition in this RDD.
        With prefetch it may consume up to the memory of the 2 largest partitions.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        prefetchPartitions : bool, optional
            If Spark should pre-fetch the next partition
            before it is needed.

        Returns
        -------
        :class:`collections.abc.Iterator`
            an iterator that contains all of the elements in this :class:`RDD`

        See Also
        --------
        :meth:`RDD.collect`
        :meth:`pyspark.sql.DataFrame.toLocalIterator`

        Examples
        --------
        >>> rdd = sc.parallelize(range(10))
        >>> [x for x in rdd.toLocalIterator()]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        assert self.ctx._jvm is not None

        with SCCallSiteSync(self.context):
            sock_info = self.ctx._jvm.PythonRDD.toLocalIteratorAndServe(
                self._jrdd.rdd(), prefetchPartitions
            )
        return _local_iterator_from_socket(sock_info, self._jrdd_deserializer)

    def barrier(self: "RDD[T]") -> "RDDBarrier[T]":
        """
        Marks the current stage as a barrier stage, where Spark must launch all tasks together.
        In case of a task failure, instead of only restarting the failed task, Spark will abort the
        entire stage and relaunch all tasks for this stage.
        The barrier execution mode feature is experimental and it only handles limited scenarios.
        Please read the linked SPIP and design docs to understand the limitations and future plans.

        .. versionadded:: 2.4.0

        Returns
        -------
        :class:`RDDBarrier`
            instance that provides actions within a barrier stage.

        See Also
        --------
        :class:`pyspark.BarrierTaskContext`

        Notes
        -----
        For additional information see

        - `SPIP: Barrier Execution Mode <http://jira.apache.org/jira/browse/SPARK-24374>`_
        - `Design Doc <https://jira.apache.org/jira/browse/SPARK-24582>`_

        This API is experimental
        """
        return RDDBarrier(self)

    def _is_barrier(self) -> bool:
        """
        Whether this RDD is in a barrier stage.
        """
        return self._jrdd.rdd().isBarrier()

    def withResources(self: "RDD[T]", profile: ResourceProfile) -> "RDD[T]":
        """
        Specify a :class:`pyspark.resource.ResourceProfile` to use when calculating this RDD.
        This is only supported on certain cluster managers and currently requires dynamic
        allocation to be enabled. It will result in new executors with the resources specified
        being acquired to calculate the RDD.

        .. versionadded:: 3.1.0

        Parameters
        ----------
        profile : :class:`pyspark.resource.ResourceProfile`
            a resource profile

        Returns
        -------
        :class:`RDD`
            the same :class:`RDD` with user specified profile

        See Also
        --------
        :meth:`RDD.getResourceProfile`

        Notes
        -----
        This API is experimental
        """
        self.has_resource_profile = True
        if profile._java_resource_profile is not None:
            jrp = profile._java_resource_profile
        else:
            assert self.ctx._jvm is not None

            builder = self.ctx._jvm.org.apache.spark.resource.ResourceProfileBuilder()
            ereqs = ExecutorResourceRequests(self.ctx._jvm, profile._executor_resource_requests)
            treqs = TaskResourceRequests(self.ctx._jvm, profile._task_resource_requests)
            builder.require(ereqs._java_executor_resource_requests)
            builder.require(treqs._java_task_resource_requests)
            jrp = builder.build()

        self._jrdd.withResources(jrp)
        return self

    def getResourceProfile(self) -> Optional[ResourceProfile]:
        """
        Get the :class:`pyspark.resource.ResourceProfile` specified with this RDD or None
        if it wasn't specified.

        .. versionadded:: 3.1.0

        Returns
        -------
        class:`pyspark.resource.ResourceProfile`
            The user specified profile or None if none were specified

        See Also
        --------
        :meth:`RDD.withResources`

        Notes
        -----
        This API is experimental
        """
        rp = self._jrdd.getResourceProfile()
        if rp is not None:
            return ResourceProfile(_java_resource_profile=rp)
        else:
            return None

    @overload
    def toDF(
        self: "RDD[RowLike]",
        schema: Optional[Union[List[str], Tuple[str, ...]]] = None,
        sampleRatio: Optional[float] = None,
    ) -> "DataFrame":
        ...

    @overload
    def toDF(
        self: "RDD[RowLike]", schema: Optional[Union["StructType", str]] = None
    ) -> "DataFrame":
        ...

    @overload
    def toDF(
        self: "RDD[AtomicValue]",
        schema: Union["AtomicType", str],
    ) -> "DataFrame":
        ...

    def toDF(
        self: "RDD[Any]", schema: Optional[Any] = None, sampleRatio: Optional[float] = None
    ) -> "DataFrame":
        raise RuntimeError("""RDD.toDF was called before SparkSession was initialized.""")


def _prepare_for_python_RDD(sc: "SparkContext", command: Any) -> Tuple[bytes, Any, Any, Any]:
    # the serialized command will be compressed by broadcast
    ser = CloudPickleSerializer()
    pickled_command = ser.dumps(command)
    assert sc._jvm is not None
    if len(pickled_command) > sc._jvm.PythonUtils.getBroadcastThreshold(sc._jsc):  # Default 1M
        # The broadcast will have same life cycle as created PythonRDD
        broadcast = sc.broadcast(pickled_command)
        pickled_command = ser.dumps(broadcast)
    broadcast_vars = [x._jbroadcast for x in sc._pickled_broadcast_vars]
    sc._pickled_broadcast_vars.clear()
    return pickled_command, broadcast_vars, sc.environment, sc._python_includes


def _wrap_function(
    sc: "SparkContext", func: Callable, deserializer: Any, serializer: Any, profiler: Any = None
) -> "JavaObject":
    assert deserializer, "deserializer should not be empty"
    assert serializer, "serializer should not be empty"
    command = (func, profiler, deserializer, serializer)
    pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command)
    assert sc._jvm is not None
    return sc._jvm.SimplePythonFunction(
        bytearray(pickled_command),
        env,
        includes,
        sc.pythonExec,
        sc.pythonVer,
        broadcast_vars,
        sc._javaAccumulator,
    )


class RDDBarrier(Generic[T]):

    """
    Wraps an RDD in a barrier stage, which forces Spark to launch tasks of this stage together.
    :class:`RDDBarrier` instances are created by :meth:`RDD.barrier`.

    .. versionadded:: 2.4.0

    Notes
    -----
    This API is experimental
    """

    def __init__(self, rdd: RDD[T]):
        self.rdd = rdd

    def mapPartitions(
        self, f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning: bool = False
    ) -> RDD[U]:
        """
        Returns a new RDD by applying a function to each partition of the wrapped RDD,
        where tasks are launched together in a barrier stage.
        The interface is the same as :meth:`RDD.mapPartitions`.
        Please see the API doc there.

        .. versionadded:: 2.4.0

        Parameters
        ----------
        f : function
           a function to run on each partition of the RDD
        preservesPartitioning : bool, optional, default False
            indicates whether the input function preserves the partitioner,
            which should be False unless this is a pair RDD and the input

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to each partition

        See Also
        --------
        :meth:`RDD.mapPartitions`

        Notes
        -----
        This API is experimental

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> def f(iterator): yield sum(iterator)
        >>> barrier = rdd.barrier()
        >>> barrier
        <pyspark.rdd.RDDBarrier ...>
        >>> barrier.mapPartitions(f).collect()
        [3, 7]
        """

        def func(s: int, iterator: Iterable[T]) -> Iterable[U]:
            return f(iterator)

        return PipelinedRDD(self.rdd, func, preservesPartitioning, isFromBarrier=True)

    def mapPartitionsWithIndex(
        self,
        f: Callable[[int, Iterable[T]], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> RDD[U]:
        """
        Returns a new RDD by applying a function to each partition of the wrapped RDD, while
        tracking the index of the original partition. And all tasks are launched together
        in a barrier stage.
        The interface is the same as :meth:`RDD.mapPartitionsWithIndex`.
        Please see the API doc there.

        .. versionadded:: 3.0.0

        Parameters
        ----------
        f : function
           a function to run on each partition of the RDD
        preservesPartitioning : bool, optional, default False
            indicates whether the input function preserves the partitioner,
            which should be False unless this is a pair RDD and the input

        Returns
        -------
        :class:`RDD`
            a new :class:`RDD` by applying a function to each partition

        See Also
        --------
        :meth:`RDD.mapPartitionsWithIndex`

        Notes
        -----
        This API is experimental

        Examples
        --------
        >>> rdd = sc.parallelize([1, 2, 3, 4], 4)
        >>> def f(splitIndex, iterator): yield splitIndex
        >>> barrier = rdd.barrier()
        >>> barrier
        <pyspark.rdd.RDDBarrier ...>
        >>> barrier.mapPartitionsWithIndex(f).sum()
        6
        """
        return PipelinedRDD(self.rdd, f, preservesPartitioning, isFromBarrier=True)


class PipelinedRDD(RDD[U], Generic[T, U]):

    """
    Examples
    --------
    Pipelined maps:

    >>> rdd = sc.parallelize([1, 2, 3, 4])
    >>> rdd.map(lambda x: 2 * x).cache().map(lambda x: 2 * x).collect()
    [4, 8, 12, 16]
    >>> rdd.map(lambda x: 2 * x).map(lambda x: 2 * x).collect()
    [4, 8, 12, 16]

    Pipelined reduces:

    >>> from operator import add
    >>> rdd.map(lambda x: 2 * x).reduce(add)
    20
    >>> rdd.flatMap(lambda x: [x, x]).reduce(add)
    20
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
            self._prev_jrdd = prev._jrdd
            self._prev_jrdd_deserializer = prev._jrdd_deserializer
        else:
            prev_func: Callable[[int, Iterable[V]], Iterable[T]] = prev.func

            def pipeline_func(split: int, iterator: Iterable[V]) -> Iterable[U]:
                return func(split, prev_func(split, iterator))

            self.func = pipeline_func
            self.preservesPartitioning = prev.preservesPartitioning and preservesPartitioning
            self._prev_jrdd = prev._prev_jrdd  # maintain the pipeline
            self._prev_jrdd_deserializer = prev._prev_jrdd_deserializer
        self.is_cached = False
        self.has_resource_profile = False
        self.is_checkpointed = False
        self.ctx = prev.ctx
        self.prev = prev
        self._jrdd_val: Optional["JavaObject"] = None
        self._id = None
        self._jrdd_deserializer = self.ctx.serializer
        self._bypass_serializer = False
        self.partitioner = prev.partitioner if self.preservesPartitioning else None
        self.is_barrier = isFromBarrier or prev._is_barrier()

    def getNumPartitions(self) -> int:
        return self._prev_jrdd.partitions().size()

    @property
    def _jrdd(self) -> "JavaObject":
        if self._jrdd_val:
            return self._jrdd_val
        if self._bypass_serializer:
            self._jrdd_deserializer = NoOpSerializer()

        if (
            self.ctx.profiler_collector
            and self.ctx._conf.get("spark.python.profile", "false") == "true"
        ):
            profiler = self.ctx.profiler_collector.new_profiler(self.ctx)
        else:
            profiler = None

        wrapped_func = _wrap_function(
            self.ctx, self.func, self._prev_jrdd_deserializer, self._jrdd_deserializer, profiler
        )

        assert self.ctx._jvm is not None
        python_rdd = self.ctx._jvm.PythonRDD(
            self._prev_jrdd.rdd(), wrapped_func, self.preservesPartitioning, self.is_barrier
        )
        self._jrdd_val = python_rdd.asJavaRDD()

        if profiler:
            assert self._jrdd_val is not None
            self._id = self._jrdd_val.id()
            self.ctx.profiler_collector.add_profiler(self._id, profiler)
        return self._jrdd_val

    def id(self) -> int:
        if self._id is None:
            self._id = self._jrdd.id()
        return self._id

    def _is_pipelinable(self) -> bool:
        return not (self.is_cached or self.is_checkpointed or self.has_resource_profile)

    def _is_barrier(self) -> bool:
        return self.is_barrier


def _test() -> None:
    import doctest
    import tempfile
    from pyspark.context import SparkContext

    tmp_dir = tempfile.TemporaryDirectory()
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    globs["sc"] = SparkContext("local[4]", "PythonTest")
    globs["sc"].setCheckpointDir(tmp_dir.name)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["sc"].stop()
    tmp_dir.cleanup()
    if failure_count:
        tmp_dir.cleanup()
        sys.exit(-1)


if __name__ == "__main__":
    _test()
