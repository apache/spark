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

import os
import time
import warnings
from collections.abc import Sequence
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    cast,
)

import pyarrow as pa

from pyspark.errors import PySparkNotImplementedError, PySparkTypeError
from pyspark.serializers import BatchedSerializer, CloudPickleSerializer, CPickleSerializer
from pyspark.sql.connect import functions as F
from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

if TYPE_CHECKING:
    from pyspark.core.connect.rdd import RDD


T = TypeVar("T")

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


class SparkContext:
    """
    Session-scoped SparkContext façade for Spark Connect (PySpark only).

    Elements are exchanged as ``pickle`` bytes in a single binary column backed by Spark SQL /
    Arrow execution (``mapInArrow``, Python UDFs, etc.).

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
    ) -> SparkContext:
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
        if sparkHome is not None:
            b = b.config("spark.home", str(sparkHome))
        if environment:
            for ev_k, ev_v in environment.items():
                b = b.config(f"spark.executorEnv.{ev_k}", str(ev_v))
        for ck, cv in pairs:
            if ck in ("spark.remote", "spark.master"):
                continue
            b = b.config(ck, cv)

        # Always allocate a new Connect SparkSession for this façade. ``getOrCreate()`` would
        # return the global default/active session and make ``stop()`` affect unrelated callers.
        session = b.create()
        self._session = session
        if pyFiles:
            for pf in pyFiles:
                self.addPyFile(pf)
        self._finalize_connect_sc_init()

    def _finalize_connect_sc_init(self) -> None:
        """One-time façade clock for :attr:`startTime` (milliseconds since Unix epoch)."""
        # Session-scoped attribute duck-typed for the Connect façade only.
        sess = self._session
        s = cast(Any, sess)
        if getattr(s, "_pyspark_connect_sc_epoch_ms", None) is None:
            s._pyspark_connect_sc_epoch_ms = int(time.time() * 1000)

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
    def _jvm(self) -> Any:
        return _JVM_QUIET_STUB

    @property
    def defaultParallelism(self) -> int:
        v = self._session.conf.get("spark.default.parallelism", "1")
        assert v is not None
        return max(1, int(v))

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
        Milliseconds since the Unix epoch when this Connect :class:`SparkContext` façade was
        first initialized for the backing session (best-effort analogue of classic ``startTime``).
        """
        s = cast(Any, self._session)
        if getattr(s, "_pyspark_connect_sc_epoch_ms", None) is None:
            self._finalize_connect_sc_init()
        return int(s._pyspark_connect_sc_epoch_ms)

    @property
    def _conf(self) -> Any:
        return self._session.conf

    def union(self, rdds: List[RDD[Any]]) -> RDD[Any]:
        if len(rdds) == 0:
            return self.emptyRDD()
        acc = rdds[0]
        for x in rdds[1:]:
            acc = acc.union(x)
        return acc

    def cancelJobGroup(self, groupId: str) -> None:
        self._session.interruptTag(groupId)

    def cancelAllJobs(self) -> None:
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
        """Best-effort: stores ``spark.checkpoint.dir`` in session configuration."""
        self._session.conf.set("spark.checkpoint.dir", dirName)

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

    def addPyFile(self, path: str) -> None:
        self._session.addArtifacts(path, pyfile=True)

    def addArchive(self, path: str) -> None:
        self._session.addArtifacts(path, archive=True)

    def addJobTag(self, tag: str) -> None:
        self._session.addTag(tag)

    def removeJobTag(self, tag: str) -> None:
        self._discard_connect_job_tag_if_present(tag)

    def getJobTags(self) -> Set[str]:
        return self._session.getTags()

    def clearJobTags(self) -> None:
        self._session.clearTags()

    def textFile(
        self,
        name: str,
        minPartitions: Optional[int] = None,
        use_unicode: bool = True,
    ) -> RDD[str]:
        """Match classic :meth:`SparkContext.textFile`; ``use_unicode=False`` emits a parity warning."""

        import pyspark.core.connect.rdd as _rdd
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        if not use_unicode:
            warnings.warn(
                "`use_unicode=False` still yields UTF-8 decoded strings from Spark Connect "
                "`read.text` similarly to classic `textFile` parity.",
                UserWarning,
                stacklevel=2,
            )

        df_txt = self._session.read.text(name)
        minPartitions = minPartitions or min(self.defaultParallelism, 2)
        np = int(minPartitions)
        if np > 1:
            df_txt = cast(ConnectDataFrame, df_txt.repartition(np))

        def enc(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                vals = batch.column(0).to_pylist()
                yield pa.record_batch(
                    {
                        _rdd._PAYLOAD_COL: pa.array(
                            [_rdd._dumps(v) for v in vals], type=pa.large_binary()
                        )
                    }
                )

        out = df_txt.mapInArrow(enc, schema=_rdd._PICKLED_SCHEMA)
        return _rdd.RDD(out, self, np)

    def wholeTextFiles(
        self,
        path: str,
        minPartitions: Optional[int] = None,
        use_unicode: bool = True,
    ) -> RDD[Tuple[str, str]]:
        """Each file becomes one pickled ``(uri, utf-8 text)`` row (via ``read.text(wholetext=True)``)."""
        import pyspark.core.connect.rdd as _rdd
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        if not use_unicode:
            warnings.warn(
                "`use_unicode=False` still decodes Spark text rows as Unicode strings "
                "on Spark Connect SparkContext.",
                UserWarning,
                stacklevel=2,
            )

        paths = [p.strip() for p in path.split(",") if p.strip()]
        tagged = cast(
            ConnectDataFrame,
            self._session.read.text(paths, wholetext=True).select(
                F.input_file_name().alias("__wtp"),
                F.col("value").alias("__wtv"),
            ),
        )

        minPartitions = minPartitions or self.defaultMinPartitions
        np = int(minPartitions)
        if np > 1:
            tagged = cast(ConnectDataFrame, tagged.repartition(np))

        def enc_pairs(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                pl = batch.column(0).to_pylist()
                vl = batch.column(1).to_pylist()
                yield pa.record_batch(
                    {
                        _rdd._PAYLOAD_COL: pa.array(
                            [_rdd._dumps((str(pu), str(vu))) for pu, vu in zip(pl, vl)],
                            type=pa.large_binary(),
                        )
                    }
                )

        out = cast(ConnectDataFrame, tagged.select("__wtp", "__wtv")).mapInArrow(
            enc_pairs, schema=_rdd._PICKLED_SCHEMA
        )
        return _rdd.RDD(out, self, np)

    def binaryFiles(self, path: str, minPartitions: Optional[int] = None) -> RDD[Tuple[str, bytes]]:
        """
        Read paths through the Spark SQL ``binaryFile`` datasource; each row becomes a
        pickled ``(file_uri, content_bytes)``.

        Uses the same ``minPartitions`` default as classic
        :meth:`pyspark.core.context.SparkContext.binaryFiles`. Very large payloads remain subject to
        ``spark.sql.sources.binaryFile.maxLength`` on the server.
        """
        import pyspark.core.connect.rdd as _rdd

        paths = [p.strip() for p in path.split(",") if p.strip()]
        reader = self._session.read.format("binaryFile")
        if len(paths) == 1:
            df_bin = reader.load(paths[0])
        else:
            df_bin = reader.load(paths)
        staged = df_bin.select(F.col("path"), F.col("content"))

        minPartitions = minPartitions or self.defaultMinPartitions
        np = int(minPartitions)
        if np > 1:
            staged = staged.repartition(np)

        def enc_pairs(it: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in it:
                pl = batch.column(0).to_pylist()
                cl = batch.column(1).to_pylist()
                pairs: List[Tuple[str, bytes]] = []
                for pu, raw in zip(pl, cl):
                    if isinstance(raw, memoryview):
                        bu = raw.tobytes()
                    elif raw is None:
                        bu = b""
                    else:
                        bu = bytes(raw)
                    pairs.append((str(pu), bu))
                yield pa.record_batch(
                    {
                        _rdd._PAYLOAD_COL: pa.array(
                            [_rdd._dumps(pair) for pair in pairs], type=pa.large_binary()
                        )
                    }
                )

        out = staged.mapInArrow(enc_pairs, schema=_rdd._PICKLED_SCHEMA)
        return _rdd.RDD(out, self, np)

    def pickleFile(self, name: str, minPartitions: Optional[int] = None) -> RDD[Any]:
        """
        Read a directory produced by :meth:`~pyspark.core.connect.rdd.RDD.saveAsPickleFile`.

        Not compatible with classic JVM pickle object files.
        """

        import os

        from pyspark.serializers import read_int

        parts = sorted(
            p
            for p in (
                os.path.join(name, f)
                for f in os.listdir(name)
                if f.startswith("part-") and not f.endswith(".crc")
            )
        )
        raw = b""
        for p in parts:
            with open(p, "rb") as bf:
                raw += bf.read()

        if not raw:
            elems: List[Any] = []
        else:
            meta_path = os.path.join(name, ".connect_pickle_batchsize")
            mode = "10"
            if os.path.isfile(meta_path):
                with open(meta_path, encoding="ascii") as meta_f:
                    mode = meta_f.read().strip()
            stream = BytesIO(raw)
            if mode == "auto":
                ser = CPickleSerializer()
                elems = []
                while True:
                    try:
                        ln = read_int(stream)
                    except Exception:
                        break
                    if ln <= 0:
                        break
                    chunk = stream.read(ln)
                    if len(chunk) != ln:
                        break
                    batch = ser.loads(chunk)
                    if isinstance(batch, list):
                        elems.extend(batch)
                    else:
                        elems.append(batch)
            else:
                bs = int(mode)
                elems = list(BatchedSerializer(CPickleSerializer(), bs).load_stream(stream))

        np = max(1, int(minPartitions or self.defaultMinPartitions))
        return self.parallelize(elems, numSlices=np)

    def emptyRDD(self) -> RDD[Any]:
        import pyspark.core.connect.rdd as _rdd

        df = self._session.createDataFrame([], schema=_rdd._PICKLED_SCHEMA)
        return _rdd.RDD(df, self, None)

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numSlices: Optional[int] = None,
    ) -> RDD[int]:
        import pyspark.core.connect.rdd as _rdd

        if end is None:
            end = start
            start = 0
        num_partitions = int(numSlices) if numSlices is not None else self.defaultParallelism

        rg = self._session.range(int(start), int(end), int(step), numPartitions=num_partitions)
        rg_tagged = rg.withColumn(_rdd._STABLE_PARTITION_COL, F.spark_partition_id())
        out = _rdd._encode_pickled_range_df(rg_tagged)
        rd_int: _rdd.RDD[int] = _rdd.RDD(out, self, num_partitions)
        rd_int._stable_partition_col = _rdd._STABLE_PARTITION_COL
        rd_int._range_meta = (int(start), int(end), int(step), num_partitions)
        rd_int._range_map_chain = []
        return rd_int

    def parallelize(self, c: Iterable[T], numSlices: Optional[int] = None) -> RDD[T]:
        import pyspark.core.connect.rdd as _rdd

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
            return cast(_rdd.RDD[T], self.range(c.start, c.stop, c.step, numSlices=num_partitions))

        elems_source: Sequence[Any]
        # Partition slicing requires sequence-style indexing (``dict_keys``, ``set``, etc. must be copied).
        if "__len__" not in dir(c) or getattr(type(c), "__getitem__", None) is None:
            elems_source = list(c)
        else:
            elems_source = cast(Sequence[Any], c)

        if len(elems_source) == 0:
            return cast(_rdd.RDD[T], self.emptyRDD())

        n = len(elems_source)
        k = max(1, num_partitions)
        # Mirror JVM ParallelCollectionRDD slicing, then pin rows with repartitionById. Keep a stable
        # partition id column so narrow transforms can filter before Python mapInArrow executes.
        part_schema = StructType(
            [
                StructField(_rdd._PAYLOAD_COL, BinaryType(), True),
                StructField(_rdd._STABLE_PARTITION_COL, IntegerType(), False),
            ]
        )
        parts_rows: List[Tuple[bytes, int]] = []
        for p in range(k):
            lo = (p * n) // k
            hi = ((p + 1) * n) // k
            for x in elems_source[lo:hi]:
                parts_rows.append((_rdd._dumps(x), p))
        df_tagged = self._session.createDataFrame(parts_rows, schema=part_schema)
        df_out = df_tagged.repartitionById(k, _rdd._STABLE_PARTITION_COL)
        rd_out: _rdd.RDD[T] = _rdd.RDD(df_out, self, k)
        rd_out._stable_partition_col = _rdd._STABLE_PARTITION_COL
        return rd_out
