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

import io
import os
import sys
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    cast,
)

from pyspark.errors import PySparkNotImplementedError, PySparkTypeError
from pyspark.serializers import (
    AutoBatchedSerializer,
    BatchedSerializer,
    CPickleSerializer,
    NoOpSerializer,
    PairDeserializer,
    UTF8Deserializer,
)

if TYPE_CHECKING:
    import pyspark.sql.connect.proto as pb2
    from pyspark.conf import SparkConf
    from pyspark.core.connect.rdd import RDD
    from pyspark.sql.dataframe import DataFrame


T = TypeVar("T")
U = TypeVar("U")

# Sentinel so default ``serializer`` is not recreated per call site (would break equality checks).
_CONNECT_SC_SERIALIZER_ARG_UNSPECIFIED = object()


class _JvmQuietStub:
    """Minimal JVM stub so ``QuietTest`` can toggle log levels without a Java gateway."""

    class org:
        class apache:
            class log4j:
                class Level:
                    FATAL = object()

                class LogManager:
                    @staticmethod
                    def getRootLogger() -> Any:
                        class _RootLogger:
                            _lvl: Any = None

                            def getLevel(self) -> Any:
                                return self._lvl

                            def setLevel(self, lvl: Any) -> None:
                                self._lvl = lvl

                        return _RootLogger()


_JVM_QUIET_STUB = _JvmQuietStub()


def _connect_remote_endpoint(master: Optional[str], conf_pairs: List[Tuple[str, str]]) -> str:
    for k, v in conf_pairs:
        if k == "spark.remote":
            return str(v)

    if master is not None and (
        isinstance(master, str) and (master.startswith("sc://") or master.startswith("grpc:"))
    ):
        return master

    env_remote = os.environ.get("SPARK_REMOTE")
    if env_remote:
        return env_remote

    from pyspark.sql.connect.client.core import DefaultChannelBuilder

    return f"sc://localhost:{DefaultChannelBuilder.default_port()}"


def _input_paths(path: str) -> List[str]:
    """The paths of a comma-separated list of inputs, as Spark Classic's file sources take."""
    return [p.strip() for p in path.split(",") if p.strip()]


def _resource_uri(path: str) -> str:
    """The URI Spark Classic lists for a resource added from ``path``."""
    from urllib.parse import urlparse

    if urlparse(path).scheme:
        return path
    return "file:" + os.path.abspath(path)


class SparkContext:
    """
    Session-scoped SparkContext facade for Spark Connect (PySpark only).

    RDD elements are exchanged as pickled chunks in a binary column backed by Spark SQL /
    Arrow execution (``mapInArrow``).

    Besides ``SparkContext(connectSparkSession)`` (wrapper used by
    :meth:`SparkSession.sparkContext <pyspark.sql.connect.session.SparkSession.sparkContext>`),
    this constructor deliberately mirrors most of :class:`~pyspark.core.context.SparkContext`:
    unrecognized classic ``master`` strings are ignored for gRPC routing and Spark Connect resolves
    the endpoint via ``spark.remote``, ``SPARK_REMOTE``, or ``localhost`` (see helper
    :func:`_connect_remote_endpoint`). JVM-only ``gateway`` / ``jsc`` arguments are rejected.
    """

    def __new__(
        cls,
        master: Any = None,
        appName: Optional[str] = None,
        sparkHome: Optional[str] = None,
        pyFiles: Optional[List[str]] = None,
        environment: Optional[Dict[str, Any]] = None,
        batchSize: int = 0,
        serializer: Any = _CONNECT_SC_SERIALIZER_ARG_UNSPECIFIED,
        conf: Any = None,
        gateway: Any = None,
        jsc: Any = None,
        profiler_cls: Any = None,
        udf_profiler_cls: Any = None,
        memory_profiler_cls: Any = None,
    ) -> "SparkContext":
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        if (
            gateway is None
            and jsc is None
            and isinstance(master, ConnectSparkSession)
            and appName is None
            and sparkHome is None
            and not pyFiles
            and not environment
            and batchSize == 0
            and profiler_cls is None
            and udf_profiler_cls is None
            and memory_profiler_cls is None
            and conf is None
            and serializer is _CONNECT_SC_SERIALIZER_ARG_UNSPECIFIED
        ):
            existing = getattr(master, "_connect_spark_context", None)
            if existing is not None:
                return existing
        instance = super().__new__(cls)
        return instance

    def __init__(
        self,
        master: Any = None,
        appName: Optional[str] = None,
        sparkHome: Optional[str] = None,
        pyFiles: Optional[List[str]] = None,
        environment: Optional[Dict[str, Any]] = None,
        batchSize: int = 0,
        serializer: Any = _CONNECT_SC_SERIALIZER_ARG_UNSPECIFIED,
        conf: Any = None,
        gateway: Any = None,
        jsc: Any = None,
        profiler_cls: Any = None,
        udf_profiler_cls: Any = None,
        memory_profiler_cls: Any = None,
    ) -> None:
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        if gateway is not None or jsc is not None:
            raise PySparkNotImplementedError(
                errorClass="NOT_IMPLEMENTED",
                messageParameters={
                    "feature": (
                        "`gateway` and `jsc` arguments are not supported on Spark Connect "
                        "`SparkContext`"
                    ),
                },
            )

        # Primary path used by SparkSession.sparkContext: wrap an existing Connect session.
        if isinstance(master, ConnectSparkSession):
            if (
                appName is not None
                or sparkHome is not None
                or pyFiles
                or environment
                or batchSize != 0
                or profiler_cls is not None
                or udf_profiler_cls is not None
                or memory_profiler_cls is not None
                or conf is not None
                or serializer is not _CONNECT_SC_SERIALIZER_ARG_UNSPECIFIED
            ):
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": (
                            "first argument SparkSession wrapper may not be combined "
                            "with other SparkContext constructor arguments"
                        ),
                        "expected_type": "`SparkContext(connectSparkSession)` only",
                        "arg_type": "Spark Connect SparkSession with extra ctor parameters",
                    },
                )

            cached = getattr(master, "_connect_spark_context", None)
            if cached is not None and cached is self:
                return

            self._session = master
            master._connect_spark_context = self
            self._finalize_connect_sc_init()
            return

        if batchSize != 0:
            warnings.warn(
                "`batchSize` is ignored by Spark Connect `SparkContext`",
                UserWarning,
                stacklevel=2,
            )

        if serializer is not _CONNECT_SC_SERIALIZER_ARG_UNSPECIFIED and (
            serializer is None or type(serializer) is not CPickleSerializer
        ):
            warnings.warn(
                "`serializer` is ignored by Spark Connect `SparkContext`; "
                "`CloudPickleSerializer` is fixed for pickled RDD columns.",
                UserWarning,
                stacklevel=2,
            )

        for label, profiler in (
            ("profiler_cls", profiler_cls),
            ("udf_profiler_cls", udf_profiler_cls),
            ("memory_profiler_cls", memory_profiler_cls),
        ):
            if profiler is not None:
                warnings.warn(
                    f"`{label}` is ignored by Spark Connect `SparkContext`.",
                    UserWarning,
                    stacklevel=2,
                )

        # The Spark Connect server was started with its own Spark home and executor environment.
        for label, value in (("sparkHome", sparkHome), ("environment", environment)):
            if value:
                warnings.warn(
                    f"`{label}` is ignored by Spark Connect `SparkContext`; it is decided when "
                    "the Spark Connect server starts.",
                    UserWarning,
                    stacklevel=2,
                )

        if isinstance(master, str) and not (
            master.startswith("sc://") or master.startswith("grpc:")
        ):
            warnings.warn(
                f"`master` {master!r} is ignored by Spark Connect `SparkContext`; it connects to "
                "the Spark Connect server in `spark.remote`, SPARK_REMOTE, or localhost.",
                UserWarning,
                stacklevel=2,
            )

        pairs: List[Tuple[str, str]] = []
        if conf is not None:
            pairs.extend(cast(Any, conf).getAll())

        master_remote = master if isinstance(master, str) else None
        endpoint = _connect_remote_endpoint(master_remote, pairs)

        b = ConnectSparkSession.builder.remote(endpoint)
        if appName is not None:
            b = b.appName(appName)
        if not any(k == "spark.app.name" for k, _ in pairs) and appName is None:
            b = b.appName("PySparkSparkConnect")
        for ck, cv in pairs:
            if ck in ("spark.remote", "spark.master"):
                continue
            b = b.config(ck, cv)

        # Always allocate a new Connect SparkSession for this facade. ``getOrCreate()`` would
        # return the global default/active session and make ``stop()`` affect unrelated callers.
        session = b.create()
        self._session = session
        if pyFiles:
            for pf in pyFiles:
                self.addPyFile(pf)
        self._finalize_connect_sc_init()

    def _finalize_connect_sc_init(self) -> None:
        """Initialize the state of the facade kept on its session."""
        # Session-scoped attribute duck-typed for the Connect facade only.
        s = cast(Any, self._session)
        if getattr(s, "_pyspark_connect_sc_files", None) is None:
            s._pyspark_connect_sc_files = []
            s._pyspark_connect_sc_archives = []

    @classmethod
    def getOrCreate(cls, conf: Optional["SparkConf"] = None) -> "SparkContext":
        """
        The :class:`SparkContext` of the active Spark Connect session, or of a new session
        built from ``conf`` when there is none.
        """
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        session = ConnectSparkSession.getActiveSession()
        if session is None:
            session = ConnectSparkSession._get_default_session()
        if session is not None and not session.is_stopped:
            return session.sparkContext
        return cls(conf=conf)

    # The serializers of the elements, as Spark Classic's SparkContext has them by default.
    _unbatched_serializer = CPickleSerializer()
    serializer = AutoBatchedSerializer(_unbatched_serializer)
    _batchSize = 0

    @property
    def version(self) -> str:
        """
        The version of Spark for the connected cluster (from the Spark Connect session).

        .. versionadded:: 5.0.0
        """
        return self._session.version

    @property
    def appName(self) -> str:
        """The ``spark.app.name`` of this Spark Connect session."""
        return cast(str, self._session.conf.get("spark.app.name"))

    @property
    def master(self) -> str:
        """The ``spark.master`` of the application serving this Spark Connect session."""
        return cast(str, self._session.conf.get("spark.master"))

    @property
    def pythonVer(self) -> str:
        """The major and minor version of this Python, as in Spark Classic."""
        return "%d.%d" % sys.version_info[:2]

    @property
    def applicationId(self) -> str:
        """The ``spark.app.id`` of the application serving this Spark Connect session."""
        return cast(str, self._session.conf.get("spark.app.id"))

    @property
    def _jvm(self) -> Any:
        return _JVM_QUIET_STUB

    @property
    def defaultParallelism(self) -> int:
        """
        The default parallelism of the SparkContext of the Spark Connect server, as
        ``SparkContext.defaultParallelism`` returns it: ``spark.default.parallelism`` when set,
        and otherwise what its scheduler decides, for instance the total number of cores.
        """
        return int(self._fetch_context_info().default_parallelism)

    @property
    def defaultMinPartitions(self) -> int:
        """
        Default suggested minimum partitions for Hadoop-style file-backed RDD helpers when not
        given by the caller.

        Matches Scala ``SparkContext.defaultMinPartitions`` (``math.min(defaultParallelism, 2)``
        ), as exposed by classic :attr:`pyspark.core.context.SparkContext.defaultMinPartitions`.

        .. versionadded:: 5.0.0
        """
        return min(self.defaultParallelism, 2)

    @property
    def startTime(self) -> int:
        """The time when the SparkContext of the Spark Connect server started, in milliseconds."""
        return int(self._context_info().start_time)

    @property
    def _conf(self) -> "_SparkContextConf":
        return _SparkContextConf(self._session)

    def _spark_conf_value(self, key: str) -> Optional[str]:
        """
        A value of the SparkConf of the application, as the RDD API of Spark Classic reads it from
        its SparkContext: set through ``_conf``, or else of the server for the keys it returns.
        """
        value = _SparkContextConf(self._session).set_value(key)
        if value is not None:
            return value
        info = self._context_info()
        if key == "spark.python.worker.memory":
            return info.python_worker_memory
        if key == "spark.default.parallelism":
            return str(info.default_parallelism) if info.default_parallelism_set else None
        return None

    def cancelJobGroup(self, groupId: str) -> None:
        self._session.interruptTag(groupId)

    def cancelAllJobs(self, reason: Optional[str] = None) -> None:
        """Interrupt every operation of this Spark Connect session. ``reason`` is not sent."""
        self._session.interruptAll()

    def cancelJobsWithTag(self, tag: str) -> None:
        """Cancel active executions carrying this tag (:meth:`~pyspark.sql.SparkSession.addTag`)."""
        self._session.interruptTag(tag)

    def setJobGroup(self, groupId: str, description: str, interruptOnCancel: bool = False) -> None:
        """
        Mirrors classic job grouping via Spark Connect tagging for use with
        :meth:`interruptTag <pyspark.sql.SparkSession.interruptTag>` or
        :meth:`SparkContext.cancelJobGroup`. As in Spark Classic, a thread has one job group, so
        this replaces the job group that the thread had.
        """
        # Kept with the thread's tags, which threads created with inheritable_thread_target copy.
        thread_local = self._session.client.thread_local
        previous = getattr(thread_local, "connect_rdd_job_group", None)
        if previous is not None and previous != groupId:
            self._discard_connect_job_tag_if_present(previous)
        self._session.addTag(groupId)
        thread_local.connect_rdd_job_group = groupId
        _ = description
        if interruptOnCancel:
            warnings.warn(
                "`interruptOnCancel=True` has no JVM thread-interrupt semantics on Spark Connect; "
                "cancellation uses Spark Connect interruption APIs instead.",
                UserWarning,
                stacklevel=2,
            )

    def _discard_connect_job_tag_if_present(self, tag: str) -> None:
        """Remove a thread-local Connect tag if present (job-group / cancel teardown)."""
        self._session.client._remove_tag_allow_missing(tag)

    def setInterruptOnCancel(self, interruptOnCancel: bool) -> None:
        if interruptOnCancel:
            warnings.warn(
                "`setInterruptOnCancel(True)` cannot enable JVM executor thread interruption "
                "on Spark Connect SparkContext.",
                UserWarning,
                stacklevel=2,
            )

    def setCheckpointDir(self, dirName: str) -> None:
        """
        Not supported: ``spark.checkpoint.dir`` is a server configuration that a Spark Connect
        session cannot change. Set it when starting the Spark Connect server.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": (
                    "SparkContext.setCheckpointDir on Spark Connect (set spark.checkpoint.dir "
                    "when starting the Spark Connect server)"
                ),
            },
        )

    def getCheckpointDir(self) -> Optional[str]:
        return self._session.conf.get("spark.checkpoint.dir", None)

    def getConf(self) -> Any:
        """
        Builds a JVM-free :class:`~pyspark.conf.SparkConf` snapshot from Spark Connect RuntimeConf.
        Updates on the resulting object do **not** automatically mutate this session.
        """
        from pyspark.conf import SparkConf

        snapshot = SparkConf(loadDefaults=False)
        for rk, rv in sorted(self._session.conf.getAll.items()):
            snapshot.set(rk, rv)
        return snapshot

    def sparkUser(self) -> str:
        uid = getattr(self._session.client, "_user_id", None)
        if uid:
            return str(uid)
        return os.environ.get("SPARK_USER") or os.environ.get("USER") or ""

    def stop(self) -> None:
        """Stops the backing Spark Connect :class:`~pyspark.sql.SparkSession`."""
        self._session.stop()

    def addFile(self, path: str, recursive: bool = False) -> None:
        if recursive:
            raise PySparkNotImplementedError(
                errorClass="NOT_IMPLEMENTED",
                messageParameters={
                    "feature": "SparkContext.addFile with recursive=True on Spark Connect",
                },
            )
        self._session.addArtifacts(path, file=True)
        self._record_resource("_pyspark_connect_sc_files", path)

    def addPyFile(self, path: str) -> None:
        self._session.addArtifacts(path, pyfile=True)
        self._record_resource("_pyspark_connect_sc_files", path)

    def addArchive(self, path: str) -> None:
        self._session.addArtifacts(path, archive=True)
        self._record_resource("_pyspark_connect_sc_archives", path)

    def _record_resource(self, attr: str, path: str) -> None:
        self._finalize_connect_sc_init()
        resources = getattr(self._session, attr)
        uri = _resource_uri(path)
        if uri not in resources:
            resources.append(uri)

    @property
    def listFiles(self) -> List[str]:
        """The files added through this context with :meth:`addFile` or :meth:`addPyFile`."""
        self._finalize_connect_sc_init()
        return list(cast(Any, self._session)._pyspark_connect_sc_files)

    @property
    def listArchives(self) -> List[str]:
        """The archives added through this context with :meth:`addArchive`."""
        self._finalize_connect_sc_init()
        return list(cast(Any, self._session)._pyspark_connect_sc_archives)

    def addJobTag(self, tag: str) -> None:
        self._session.addTag(tag)

    def removeJobTag(self, tag: str) -> None:
        self._discard_connect_job_tag_if_present(tag)

    def getJobTags(self) -> Set[str]:
        return self._session.getTags()

    def clearJobTags(self) -> None:
        self._session.clearTags()

    def show_profiles(self) -> None:
        """
        Print the Python profiles collected for this session, which RDD functions produce when
        ``spark.sql.pyspark.udf.profiler`` is set (see :meth:`SparkSession.profile`).
        """
        self._session.profile.show()

    def dump_profiles(self, path: str) -> None:
        """Dump the Python profiles collected for this session into ``path``."""
        self._session.profile.dump(path)

    # -- Requests of the RDD API ---------------------------------------------------------------

    @property
    def _python_hash_seed(self) -> str:
        """The PYTHONHASHSEED of the Python workers, the client's as in Spark Classic."""
        return os.environ.get("PYTHONHASHSEED", "0")

    def _execute_rdd_command(self, fill: Callable[[Any], None]) -> "pb2.RDDCommandResult":
        """Run an ``RDDCommand`` that ``fill`` writes."""
        import pyspark.sql.connect.proto as pb2

        command = pb2.Command()
        fill(command.rdd_command)
        _, properties, _ = self._session.client.execute_command(command)
        return properties["rdd_command_result"]

    def _num_partitions(self, df: "DataFrame") -> int:
        """The number of partitions of the RDD of ``df``, which the server finds."""
        client = self._session.client

        def fill(command: Any) -> None:
            command.describe.rdd.CopyFrom(cast(Any, df)._plan.plan(client))

        return int(self._execute_rdd_command(fill).info.num_partitions)

    def _context_info(self) -> "pb2.RDDCommandResult.ContextInfo":
        """The properties of the server, fetched once per session."""
        s = cast(Any, self._session)
        info = getattr(s, "_pyspark_connect_sc_context_info", None)
        if info is None:
            info = self._fetch_context_info()
            s._pyspark_connect_sc_context_info = info
        return info

    def _fetch_context_info(self) -> "pb2.RDDCommandResult.ContextInfo":
        def fill(command: Any) -> None:
            command.get_context_info.SetInParent()

        return self._execute_rdd_command(fill).context_info

    def _elements_plan(self, df: "DataFrame", partitions: Optional[Sequence[int]]) -> Any:
        import pyspark.sql.connect.proto as pb2

        root = pb2.Plan()
        elements = root.root.rdd_relation.elements
        elements.input.CopyFrom(cast(Any, df)._plan.plan(self._session.client))
        if partitions is not None:
            elements.partitions.values.extend(partitions)
        return root

    def _collect_rdd(
        self,
        df: "DataFrame",
        deserializer: Any,
        partitions: Optional[Sequence[int]] = None,
    ) -> List[Any]:
        """
        The elements of the given partitions of the RDD of ``df`` (all of them when not given),
        in the order of the partitions given, as ``PythonRDD.runJob`` of Spark Classic returns
        them.
        """
        from pyspark.core.connect.rdd import _load

        table, _, _ = self._session.client.to_table(self._elements_plan(df, partitions), {})
        if partitions is None or len(partitions) <= 1:
            return list(_load(deserializer, table))
        by_partition: Dict[int, List[Any]] = {}
        for index, data in zip(table.column(0).to_pylist(), table.column(1).to_pylist()):
            by_partition.setdefault(index, []).extend(deserializer.load_stream(io.BytesIO(data)))
        return [x for p in partitions for x in by_partition.get(p, [])]

    def _iterate_rdd(self, df: "DataFrame", deserializer: Any) -> Iterator[List[Any]]:
        """The elements of the RDD of ``df``, in lists as the Arrow batches of the result come."""
        from pyspark.core.connect.rdd import _load
        from pyspark.sql.types import StructType

        for table in self._session.client.to_table_as_iterator(self._elements_plan(df, None), {}):
            if not isinstance(table, StructType):
                yield list(_load(deserializer, table))

    # -- RDD sources ---------------------------------------------------------------------------

    def _rdd(
        self,
        df: "DataFrame",
        deserializer: Any = None,
        num_partitions: Optional[int] = None,
    ) -> "RDD[Any]":
        from pyspark.core.connect.rdd import RDD

        if deserializer is None:
            deserializer = AutoBatchedSerializer(CPickleSerializer())
        return RDD(cast(Any, df), self, deserializer, num_partitions)

    def _with_min_partitions(self, rdd: "RDD[T]", minPartitions: Optional[int]) -> "RDD[T]":
        """
        Honor an explicit ``minPartitions`` of a file source, whose splits Spark SQL decides from
        the session's configurations: when there are fewer splits, their elements are split into
        ``minPartitions`` partitions of consecutive elements, in order, as Spark Classic splits
        files. One job counts the elements of each split.
        """
        if minPartitions is None or minPartitions <= 1:
            return rdd
        return rdd._spread(minPartitions)

    def emptyRDD(self) -> "RDD[Any]":
        from pyspark.core.connect.rdd import _RDDRelationPlan

        def fill(relation: Any, session: Any) -> None:
            relation.union.SetInParent()

        return self._rdd(_RDDRelationPlan.dataframe(self, fill), NoOpSerializer(), 0)

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numSlices: Optional[int] = None,
    ) -> "RDD[int]":
        if end is None:
            end = start
            start = 0

        return self.parallelize(range(start, end, step), numSlices)

    def parallelize(self, c: Iterable[T], numSlices: Optional[int] = None) -> "RDD[T]":
        """
        Distribute a local Python collection to form an RDD, as Spark Classic does: the elements
        are pickled in batches, which the server slices over the partitions.
        """
        from pyspark.core.connect.rdd import _local_data_relation, _RDDRelationPlan

        if not isinstance(c, Iterable):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "c",
                    "expected_type": "collections.abc.Iterable",
                    "arg_type": type(c).__name__,
                },
            )

        numSlices = int(numSlices) if numSlices is not None else self.defaultParallelism
        if isinstance(c, range):
            size = len(c)
            if size == 0:
                return self.parallelize([], numSlices)
            step = c[1] - c[0] if size > 1 else 1
            start0 = c[0]

            def getStart(split: int) -> int:
                assert numSlices is not None
                return start0 + int((split * size / numSlices)) * step

            def f(split: int, iterator: Iterable[T]) -> Iterable:
                # it's an empty iterator here but we need this line for triggering the
                # logic of signal handling in FramedSerializer.load_stream, for instance,
                # SpecialLengths.END_OF_DATA_SECTION in _read_with_length. Since
                # FramedSerializer.load_stream produces a generator, the control should
                # at least be in that function once. Here we do it by explicitly converting
                # the empty iterator to a list, thus make sure worker reuse takes effect.
                # See more details in SPARK-26549.
                assert len(list(iterator)) == 0
                return range(getStart(split), getStart(split + 1), step)

            return self.parallelize([], numSlices).mapPartitionsWithIndex(f)

        # Make sure we distribute data evenly if it's smaller than self.batchSize
        if "__len__" not in dir(c):
            c = list(c)  # Make it a list so we can compute its length
        batchSize = max(
            1,
            min(len(c) // numSlices, self._batchSize or 1024),  # type: ignore[arg-type]
        )
        serializer = BatchedSerializer(self._unbatched_serializer, batchSize)
        batches = [serializer.serializer.dumps(b) for b in serializer._batched(c)]
        data = _local_data_relation(self, batches)
        num_slices = numSlices

        def fill(relation: Any, session: Any) -> None:
            relation.parallelize.data.CopyFrom(data)
            relation.parallelize.num_slices = num_slices

        return self._rdd(_RDDRelationPlan.dataframe(self, fill), serializer, numSlices)

    def pickleFile(self, name: str, minPartitions: Optional[int] = None) -> "RDD[Any]":
        """
        Read the elements saved by :meth:`~pyspark.core.connect.rdd.RDD.saveAsPickleFile` on
        Spark Connect, from comma-separated paths. The files of Spark Classic's
        ``saveAsPickleFile`` cannot be read.
        """
        df = self._session.read.parquet(*_input_paths(name))
        return self._with_min_partitions(self._rdd(df), minPartitions)

    def textFile(
        self,
        name: str,
        minPartitions: Optional[int] = None,
        use_unicode: bool = True,
    ) -> "RDD[str]":
        """
        Read a text file, or comma-separated paths, through the Spark SQL text source, one
        element per line. File splits follow the session's file source configurations;
        ``minPartitions`` is honored as a minimum, which takes a job to count the lines of each
        split, and keeps the lines in order.
        """
        df = self._session.read.text(_input_paths(name))
        rdd = self._rdd(df, UTF8Deserializer(use_unicode))
        return self._with_min_partitions(rdd, minPartitions)

    def wholeTextFiles(
        self,
        path: str,
        minPartitions: Optional[int] = None,
        use_unicode: bool = True,
    ) -> "RDD[Tuple[str, str]]":
        """
        Each file becomes one ``(uri, text)`` element, read with ``read.text(wholetext=True)``.
        """
        from pyspark.sql.connect import functions as F

        df = self._session.read.text(_input_paths(path), wholetext=True).select(
            F.input_file_name(), F.col("value")
        )
        rdd = self._rdd(
            df, PairDeserializer(UTF8Deserializer(use_unicode), UTF8Deserializer(use_unicode))
        )
        return self._with_min_partitions(rdd, minPartitions)

    def _binary_files_df(self, path: str) -> "DataFrame":
        from pyspark.sql.connect import functions as F

        paths = _input_paths(path)
        reader = self._session.read.format("binaryFile")
        df = reader.load(paths[0]) if len(paths) == 1 else reader.load(paths)
        return df.select(F.col("path"), F.col("content"))

    def binaryFiles(
        self, path: str, minPartitions: Optional[int] = None
    ) -> "RDD[Tuple[str, bytes]]":
        """
        Read paths through the Spark SQL ``binaryFile`` source; each file becomes a
        ``(file_uri, content_bytes)`` element. Very large files remain subject to
        ``spark.sql.sources.binaryFile.maxLength`` on the server.
        """
        rdd = self._rdd(
            self._binary_files_df(path), PairDeserializer(UTF8Deserializer(), NoOpSerializer())
        )
        return self._with_min_partitions(rdd, minPartitions)

    def binaryRecords(self, path: str, recordLength: int) -> "RDD[bytes]":
        """
        Read flat binary files of fixed-length records through the Spark SQL ``binaryFile``
        source. Each file is read as a whole, so files remain subject to
        ``spark.sql.sources.binaryFile.maxLength`` on the server.
        """
        if recordLength <= 0:
            raise ValueError("recordLength must be positive, but got %d" % recordLength)

        def split_records(_: int, files: Iterable[Tuple[str, bytes]]) -> Iterable[bytes]:
            for file_path, content in files:
                if len(content) % recordLength != 0:
                    raise ValueError(
                        "Partial record (length = %d, fixed record length = %d) found at the "
                        "end of %s." % (len(content) % recordLength, recordLength, file_path)
                    )
                for start in range(0, len(content), recordLength):
                    yield content[start : start + recordLength]

        files = self._rdd(
            self._binary_files_df(path), PairDeserializer(UTF8Deserializer(), NoOpSerializer())
        )
        return files.mapPartitionsWithIndex(split_records)

    def union(self, rdds: List["RDD[T]"]) -> "RDD[T]":
        from pyspark.core.connect.rdd import _union

        first_jrdd_deserializer = rdds[0]._jrdd_deserializer
        if any(x._jrdd_deserializer != first_jrdd_deserializer for x in rdds):
            rdds = [x._reserialize() for x in rdds]
        return _union(self, rdds, rdds[0]._jrdd_deserializer)

    def runJob(
        self,
        rdd: "RDD[T]",
        partitionFunc: Callable[[Iterable[T]], Iterable[U]],
        partitions: Optional[Sequence[int]] = None,
        allowLocal: bool = False,
    ) -> List[U]:
        """
        Run ``partitionFunc`` on the given partitions only, and return the results in the order
        of ``partitions``, as Spark Classic does.
        """
        if partitions is None:
            partitions = list(range(rdd.getNumPartitions()))
        # Implementation note: This is implemented as a mapPartitions followed
        # by runJob() in order to avoid having to pass a Python lambda into
        # SparkContext#runJob.
        mappedRDD = rdd.mapPartitions(partitionFunc)
        if len(partitions) == 0:
            return []
        return mappedRDD._collect(list(partitions))


class _SparkContextConf:
    """
    ``SparkContext._conf`` on Spark Connect: the runtime configuration of the session, which also
    keeps the values set through it, as Spark Classic's RDD API reads the SparkConf of its
    SparkContext for ``spark.python.worker.memory`` and ``spark.default.parallelism``.
    """

    def __init__(self, session: Any) -> None:
        self._session = session

    def _values(self) -> Dict[str, str]:
        s = cast(Any, self._session)
        values = getattr(s, "_pyspark_connect_sc_conf_values", None)
        if values is None:
            values = {}
            s._pyspark_connect_sc_conf_values = values
        return values

    def set_value(self, key: str) -> Optional[str]:
        """The value set for ``key`` through this configuration, if any."""
        return self._values().get(key)

    def set(self, key: str, value: Any) -> "_SparkContextConf":
        self._session.conf.set(key, value)
        self._values()[key] = str(value)
        return self

    def unset(self, key: str) -> None:
        self._session.conf.unset(key)
        self._values().pop(key, None)

    def get(self, key: str, defaultValue: Optional[str] = None) -> Optional[str]:
        values = self._values()
        if key in values:
            return values[key]
        return self._session.conf.get(key, defaultValue)

    def contains(self, key: str) -> bool:
        return key in self._values() or self._session.conf.get(key, None) is not None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session.conf, name)
