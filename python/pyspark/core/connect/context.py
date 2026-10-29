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

import os
import sys
import time
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    cast,
)

from pyspark.core.connect import _runtime as rt
from pyspark.errors import PySparkNotImplementedError, PySparkTypeError
from pyspark.serializers import CloudPickleSerializer, CPickleSerializer
from pyspark.sql.connect import functions as F

if TYPE_CHECKING:
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

_CLOUD_PICKLE_SERIALIZER = CloudPickleSerializer()


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
        """One-time facade clock for :attr:`startTime` (milliseconds since Unix epoch)."""
        # Session-scoped attribute duck-typed for the Connect facade only.
        s = cast(Any, self._session)
        if getattr(s, "_pyspark_connect_sc_epoch_ms", None) is None:
            s._pyspark_connect_sc_epoch_ms = int(time.time() * 1000)
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

    @property
    def serializer(self) -> CloudPickleSerializer:
        return _CLOUD_PICKLE_SERIALIZER

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
        ``spark.default.parallelism`` when set; otherwise the server's default parallelism,
        found once per session from the partitions of a ``range`` without a partition count.
        """
        v = self._session.conf.get("spark.default.parallelism", None)
        if v is not None:
            return max(1, int(v))
        s = cast(Any, self._session)
        cached = getattr(s, "_pyspark_connect_sc_default_parallelism", None)
        if cached is None:
            # A range without a partition count is split into the server's default parallelism,
            # and every split is non-empty while there are at least as many rows as splits.
            pids = self._session.range(1 << 20).select(F.spark_partition_id().alias("p"))
            row = pids.agg(F.max("p")).first()
            cached = 1 if row is None or row[0] is None else int(row[0]) + 1
            s._pyspark_connect_sc_default_parallelism = cached
        return cached

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
        """
        Milliseconds since the Unix epoch when this Connect :class:`SparkContext` facade was
        first initialized for the backing session (best-effort analogue of classic ``startTime``).
        """
        s = cast(Any, self._session)
        if getattr(s, "_pyspark_connect_sc_epoch_ms", None) is None:
            self._finalize_connect_sc_init()
        return int(s._pyspark_connect_sc_epoch_ms)

    @property
    def _conf(self) -> Any:
        return self._session.conf

    def union(self, rdds: List["RDD[T]"]) -> "RDD[T]":
        if len(rdds) == 0:
            return self.emptyRDD()
        acc = rdds[0]
        for x in rdds[1:]:
            acc = acc.union(x)
        return acc

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
        :meth:`SparkContext.cancelJobGroup`.
        """
        self._session.addTag(groupId)
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

    # -- RDD sources ---------------------------------------------------------------------------

    def _with_min_partitions(self, rdd: "RDD[T]", minPartitions: Optional[int]) -> "RDD[T]":
        """
        Honor an explicit ``minPartitions`` for file sources. Spark SQL decides file splits from
        session configurations, so count them, and spread the elements over ``minPartitions``
        partitions only when there are fewer.
        """
        if minPartitions is None or minPartitions <= 1:
            return rdd
        if rdd.getNumPartitions() >= minPartitions:
            return rdd
        return rdd.repartition(minPartitions)

    def textFile(
        self,
        name: str,
        minPartitions: Optional[int] = None,
        use_unicode: bool = True,
    ) -> "RDD[str]":
        """
        Read a text file through the Spark SQL text source, one element per line. File splits
        follow the session's file source configurations; ``minPartitions`` is honored as a
        minimum, which takes a job to count the splits.
        """
        from pyspark.core.connect.rdd import RDD

        if not use_unicode:
            warnings.warn(
                "`use_unicode=False` still yields UTF-8 decoded strings from Spark Connect "
                "`read.text` similarly to classic `textFile` parity.",
                UserWarning,
                stacklevel=2,
            )
        rdd: RDD[str] = RDD(self._session.read.text(name), self, None, rt.SOURCE_VALUES)
        return self._with_min_partitions(rdd, minPartitions)

    def wholeTextFiles(
        self,
        path: str,
        minPartitions: Optional[int] = None,
        use_unicode: bool = True,
    ) -> "RDD[Tuple[str, str]]":
        """
        Each file becomes one ``(uri, utf-8 text)`` element, read with
        ``read.text(wholetext=True)``.
        """
        from pyspark.core.connect.rdd import RDD

        if not use_unicode:
            warnings.warn(
                "`use_unicode=False` still decodes Spark text rows as Unicode strings "
                "on Spark Connect SparkContext.",
                UserWarning,
                stacklevel=2,
            )

        paths = [p.strip() for p in path.split(",") if p.strip()]
        df = self._session.read.text(paths, wholetext=True).select(
            F.input_file_name(), F.col("value")
        )
        rdd: RDD[Tuple[str, str]] = RDD(df, self, None, rt.SOURCE_PAIRS)
        return self._with_min_partitions(rdd, minPartitions)

    def _binary_files_df(self, path: str) -> "DataFrame":
        paths = [p.strip() for p in path.split(",") if p.strip()]
        reader = self._session.read.format("binaryFile")
        df = reader.load(paths[0]) if len(paths) == 1 else reader.load(paths)
        return df.select(F.col("path"), F.col("content"))

    def binaryFiles(
        self, path: str, minPartitions: Optional[int] = None
    ) -> "RDD[Tuple[str, bytes]]":
        """
        Read paths through the Spark SQL ``binaryFile`` datasource; each file becomes a
        ``(file_uri, content_bytes)`` element. Very large files remain subject to
        ``spark.sql.sources.binaryFile.maxLength`` on the server.
        """
        from pyspark.core.connect.rdd import RDD

        rdd: RDD[Tuple[str, bytes]] = RDD(self._binary_files_df(path), self, None, rt.SOURCE_PAIRS)
        return self._with_min_partitions(rdd, minPartitions)

    def binaryRecords(self, path: str, recordLength: int) -> "RDD[bytes]":
        """
        Read flat binary files of fixed-length records through the Spark SQL ``binaryFile``
        datasource. Each file is read as a whole, so files remain subject to
        ``spark.sql.sources.binaryFile.maxLength`` on the server.
        """
        from pyspark.core.connect.rdd import RDD

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

        return RDD(self._binary_files_df(path), self, None, rt.SOURCE_PAIRS, split_records)

    def pickleFile(self, name: str, minPartitions: Optional[int] = None) -> "RDD[Any]":
        """
        Read the elements saved by :meth:`~pyspark.core.connect.rdd.RDD.saveAsPickleFile` on
        Spark Connect. The files of Spark Classic's ``saveAsPickleFile`` cannot be read.
        """
        from pyspark.core.connect.rdd import RDD

        df = self._session.read.parquet(name).select(
            F.col("value").alias(rt.CHUNK_COL), F.col("n").alias(rt.COUNT_COL)
        )
        rdd: RDD[Any] = RDD(df, self, None)
        return self._with_min_partitions(rdd, minPartitions)

    def emptyRDD(self) -> "RDD[Any]":
        from pyspark.core.connect.rdd import RDD, _chunk_schema

        df = self._session.createDataFrame([], schema=_chunk_schema())
        return RDD(df, self, 0)

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numSlices: Optional[int] = None,
    ) -> "RDD[int]":
        """
        Create an RDD of int over ``range(start, end, step)`` through
        :meth:`SparkSession.range`; the partitions hold the same elements as Spark Classic's.
        """
        from pyspark.core.connect.rdd import RDD

        if end is None:
            end = start
            start = 0
        num_partitions = int(numSlices) if numSlices is not None else self.defaultParallelism
        df = self._session.range(int(start), int(end), int(step), numPartitions=num_partitions)
        return RDD(df, self, num_partitions, rt.SOURCE_VALUES)

    def parallelize(self, c: Iterable[T], numSlices: Optional[int] = None) -> "RDD[T]":
        """
        Distribute a local Python collection: it is sliced like Spark Classic's
        ``ParallelCollectionRDD``, pickled in chunks on the driver, and placed with
        :meth:`DataFrame.repartitionById`.
        """
        import pyarrow as pa

        from pyspark.core.connect.rdd import RDD, _pid_chunk_schema, _place

        if not isinstance(c, Iterable):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "c",
                    "expected_type": "collections.abc.Iterable",
                    "arg_type": type(c).__name__,
                },
            )

        num_partitions = int(numSlices) if numSlices is not None else self.defaultParallelism

        if isinstance(c, range):
            return cast("RDD[T]", self.range(c.start, c.stop, c.step, numSlices=num_partitions))

        elems: Sequence[Any] = c if isinstance(c, (list, tuple)) else list(c)
        n = len(elems)
        k = max(1, num_partitions)
        blobs: List[bytes] = []
        counts: List[int] = []
        pids: List[int] = []
        for p in range(k):
            lo, hi = (p * n) // k, ((p + 1) * n) // k
            for blob, cnt in rt.encode_chunks(elems[lo:hi]):
                blobs.append(blob)
                counts.append(cnt)
                pids.append(p)
        if n == 0:
            local = self._session.createDataFrame([], schema=_pid_chunk_schema())
        else:
            table = pa.Table.from_arrays(
                [
                    pa.array(blobs, pa.large_binary()),
                    pa.array(counts, pa.int32()),
                    pa.array(pids, pa.int32()),
                ],
                schema=rt.PID_CHUNK_ARROW_SCHEMA,
            )
            local = self._session.createDataFrame(table)
        df = _place(local, k)
        rdd: RDD[T] = RDD(df, self, k)
        rdd._df_has_pid = True
        return rdd

    def runJob(
        self,
        rdd: "RDD[T]",
        partitionFunc: Callable[[Iterable[T]], Iterable[U]],
        partitions: Optional[Sequence[int]] = None,
        allowLocal: bool = False,
    ) -> List[U]:
        """
        Run ``partitionFunc`` on the given partitions only, and return the results in the order
        of ``partitions``.
        """
        if partitions is None:
            partitions = list(range(rdd.getNumPartitions()))
        if len(partitions) == 0:
            return []
        results = rdd.mapPartitions(partitionFunc)._collect_partitions(partitions)
        out: List[U] = []
        for p in partitions:
            out.extend(results.get(p, []))
        return out
