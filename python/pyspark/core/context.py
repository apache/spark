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
import shutil
import signal
import sys
import threading
import warnings
import importlib
from threading import RLock
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import (
    Any,
    Callable,
    cast,
    ClassVar,
    Dict,
    Iterable,
    List,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
    Set,
)

from py4j.java_collections import JavaMap
from py4j.protocol import Py4JError

from pyspark import accumulators
from pyspark.conf import SparkConf
from pyspark.accumulators import Accumulator
from pyspark.core.broadcast import Broadcast, BroadcastPickleRegistry
from pyspark.core.files import SparkFiles
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import (
    CPickleSerializer,
    BatchedSerializer,
    Serializer,
    UTF8Deserializer,
    PairDeserializer,
    AutoBatchedSerializer,
    NoOpSerializer,
    ChunkedStream,
)
from pyspark.storagelevel import StorageLevel
from pyspark.resource.information import ResourceInformation
from pyspark.core.rdd import RDD
from pyspark.util import _load_from_socket, local_connect_and_auth
from pyspark.taskcontext import TaskContext
from pyspark.traceback_utils import CallSite, first_spark_call
from pyspark.core.status import StatusTracker
from pyspark.profiler import ProfilerCollector, BasicProfiler, UDFBasicProfiler, MemoryProfiler
from pyspark.errors import PySparkRuntimeError
from py4j.java_gateway import is_instance_of, JavaGateway, JavaObject, JVMView

if TYPE_CHECKING:
    from pyspark.accumulators import AccumulatorParam

__all__ = ["SparkContext"]


# These are special default configs for PySpark, they will overwrite
# the default ones for Spark if they are not configured by user.
DEFAULT_CONFIGS: Dict[str, Any] = {
    "spark.serializer.objectStreamReset": 100,
    "spark.rdd.compress": True,
    # Disable artifact isolation in PySpark, or user-added .py file won't work
    "spark.sql.artifact.isolation.enabled": "false",
}

T = TypeVar("T")
U = TypeVar("U")


class SparkContext:

    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create :class:`RDD` and
    broadcast variables on that cluster.

    When you create a new SparkContext, at least the master and app name should
    be set, either through the named parameters here or through `conf`.

    Parameters
    ----------
    master : str, optional
        Cluster URL to connect to (e.g. spark://host:port, local[4]).
    appName : str, optional
        A name for your job, to display on the cluster web UI.
    sparkHome : str, optional
        Location where Spark is installed on cluster nodes.
    pyFiles : list, optional
        Collection of .zip or .py files to send to the cluster
        and add to PYTHONPATH.  These can be paths on the local file
        system or HDFS, HTTP, HTTPS, or FTP URLs.
    environment : dict, optional
        A dictionary of environment variables to set on
        worker nodes.
    batchSize : int, optional, default 0
        The number of Python objects represented as a single
        Java object. Set 1 to disable batching, 0 to automatically choose
        the batch size based on object sizes, or -1 to use an unlimited
        batch size
    serializer : :class:`Serializer`, optional, default :class:`CPickleSerializer`
        The serializer for RDDs.
    conf : :class:`SparkConf`, optional
        An object setting Spark properties.
    gateway : class:`py4j.java_gateway.JavaGateway`,  optional
        Use an existing gateway and JVM, otherwise a new JVM
        will be instantiated. This is only used internally.
    jsc : class:`py4j.java_gateway.JavaObject`, optional
        The JavaSparkContext instance. This is only used internally.
    profiler_cls : type, optional, default :class:`BasicProfiler`
        A class of custom Profiler used to do profiling
    udf_profiler_cls : type, optional, default :class:`UDFBasicProfiler`
        A class of custom Profiler used to do udf profiling

    Notes
    -----
    Only one :class:`SparkContext` should be active per JVM. You must `stop()`
    the active :class:`SparkContext` before creating a new one.

    :class:`SparkContext` instance is not supported to share across multiple
    processes out of the box, and PySpark does not guarantee multi-processing execution.
    Use threads instead for concurrent processing purpose.

    Examples
    --------
    >>> from pyspark.core.context import SparkContext
    >>> sc = SparkContext('local', 'test')
    >>> sc2 = SparkContext('local', 'test2') # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: ...
    """

    _gateway: ClassVar[Optional[JavaGateway]] = None
    _jvm: ClassVar[Optional[JVMView]] = None
    _next_accum_id = 0
    _active_spark_context: ClassVar[Optional["SparkContext"]] = None
    _lock = RLock()
    _python_includes: Optional[
        List[str]
    ] = None  # zip and egg files that need to be added to PYTHONPATH
    serializer: Serializer
    profiler_collector: ProfilerCollector

    PACKAGE_EXTENSIONS: Iterable[str] = (".zip", ".egg", ".jar")

    def __init__(
        self,
        master: Optional[str] = None,
        appName: Optional[str] = None,
        sparkHome: Optional[str] = None,
        pyFiles: Optional[List[str]] = None,
        environment: Optional[Dict[str, Any]] = None,
        batchSize: int = 0,
        serializer: "Serializer" = CPickleSerializer(),
        conf: Optional[SparkConf] = None,
        gateway: Optional[JavaGateway] = None,
        jsc: Optional[JavaObject] = None,
        profiler_cls: Type[BasicProfiler] = BasicProfiler,
        udf_profiler_cls: Type[UDFBasicProfiler] = UDFBasicProfiler,
        memory_profiler_cls: Type[MemoryProfiler] = MemoryProfiler,
    ):
        if "SPARK_CONNECT_MODE_ENABLED" in os.environ and "SPARK_LOCAL_REMOTE" not in os.environ:
            raise PySparkRuntimeError(
                errorClass="CONTEXT_UNAVAILABLE_FOR_REMOTE_CLIENT",
                messageParameters={},
            )

        if conf is None or conf.get("spark.executor.allowSparkContext", "false").lower() != "true":
            # In order to prevent SparkContext from being created in executors.
            SparkContext._assert_on_driver()

        self._callsite = first_spark_call() or CallSite(None, None, None)
        if gateway is not None and gateway.gateway_parameters.auth_token is None:
            raise ValueError(
                "You are trying to pass an insecure Py4j gateway to Spark. This"
                " is not allowed as it is a security risk."
            )

        SparkContext._ensure_initialized(self, gateway=gateway, conf=conf)
        try:
            self._do_init(
                master,
                appName,
                sparkHome,
                pyFiles,
                environment,
                batchSize,
                serializer,
                conf,
                jsc,
                profiler_cls,
                udf_profiler_cls,
                memory_profiler_cls,
            )
        except BaseException:
            # If an error occurs, clean up in order to allow future SparkContext creation:
            self.stop()
            raise

    def _do_init(
        self,
        master: Optional[str],
        appName: Optional[str],
        sparkHome: Optional[str],
        pyFiles: Optional[List[str]],
        environment: Optional[Dict[str, Any]],
        batchSize: int,
        serializer: Serializer,
        conf: Optional[SparkConf],
        jsc: JavaObject,
        profiler_cls: Type[BasicProfiler] = BasicProfiler,
        udf_profiler_cls: Type[UDFBasicProfiler] = UDFBasicProfiler,
        memory_profiler_cls: Type[MemoryProfiler] = MemoryProfiler,
    ) -> None:
        self.environment = environment or {}
        # java gateway must have been launched at this point.
        if conf is not None and conf._jconf is not None:
            # conf has been initialized in JVM properly, so use conf directly. This represents the
            # scenario that JVM has been launched before SparkConf is created (e.g. SparkContext is
            # created and then stopped, and we create a new SparkConf and new SparkContext again)
            self._conf = conf
        else:
            self._conf = SparkConf(_jvm=SparkContext._jvm)
            if conf is not None:
                for k, v in conf.getAll():
                    self._conf.set(k, v)

        self._batchSize = batchSize  # -1 represents an unlimited batch size
        self._unbatched_serializer = serializer
        if batchSize == 0:
            self.serializer = AutoBatchedSerializer(self._unbatched_serializer)
        else:
            self.serializer = BatchedSerializer(self._unbatched_serializer, batchSize)

        # Set any parameters passed directly to us on the conf
        if master:
            self._conf.setMaster(master)
        if appName:
            self._conf.setAppName(appName)
        if sparkHome:
            self._conf.setSparkHome(sparkHome)
        if environment:
            for key, value in environment.items():
                self._conf.setExecutorEnv(key, value)
        for key, value in DEFAULT_CONFIGS.items():
            self._conf.setIfMissing(key, value)

        # Check that we have at least the required parameters
        if not self._conf.contains("spark.master"):
            raise PySparkRuntimeError(
                errorClass="MASTER_URL_NOT_SET",
                messageParameters={},
            )
        if not self._conf.contains("spark.app.name"):
            raise PySparkRuntimeError(
                errorClass="APPLICATION_NAME_NOT_SET",
                messageParameters={},
            )

        # Read back our properties from the conf in case we loaded some of them from
        # the classpath or an external config file
        self.master = self._conf.get("spark.master")
        self.appName = self._conf.get("spark.app.name")
        self.sparkHome = self._conf.get("spark.home", None)

        for k, v in self._conf.getAll():
            if k.startswith("spark.executorEnv."):
                varName = k[len("spark.executorEnv.") :]
                self.environment[varName] = v

        self.environment["PYTHONHASHSEED"] = os.environ.get("PYTHONHASHSEED", "0")

        # Create the Java SparkContext through Py4J
        self._jsc = jsc or self._initialize_context(self._conf._jconf)
        # Reset the SparkConf to the one actually used by the SparkContext in JVM.
        self._conf = SparkConf(_jconf=self._jsc.sc().conf())

        # Create a single Accumulator in Java that we'll send all our updates through;
        # they will be passed back to us through a TCP server
        assert self._gateway is not None
        auth_token = self._gateway.gateway_parameters.auth_token
        start_update_server = accumulators._start_update_server
        self._accumulatorServer = start_update_server(auth_token)
        (host, port) = self._accumulatorServer.server_address
        assert self._jvm is not None
        self._javaAccumulator = self._jvm.PythonAccumulatorV2(host, port, auth_token)
        self._jsc.sc().register(self._javaAccumulator)

        # If encryption is enabled, we need to setup a server in the jvm to read broadcast
        # data via a socket.
        # scala's mangled names w/ $ in them require special treatment.
        self._encryption_enabled = self._jvm.PythonUtils.isEncryptionEnabled(self._jsc)
        os.environ["SPARK_AUTH_SOCKET_TIMEOUT"] = str(
            self._jvm.PythonUtils.getPythonAuthSocketTimeout(self._jsc)
        )
        os.environ["SPARK_BUFFER_SIZE"] = str(self._jvm.PythonUtils.getSparkBufferSize(self._jsc))

        self.pythonExec = os.environ.get("PYSPARK_PYTHON", "python3")
        self.pythonVer = "%d.%d" % sys.version_info[:2]

        # Broadcast's __reduce__ method stores Broadcast instances here.
        # This allows other code to determine which Broadcast instances have
        # been pickled, so it can determine which Java broadcast objects to
        # send.
        self._pickled_broadcast_vars = BroadcastPickleRegistry()

        SparkFiles._sc = self
        root_dir = SparkFiles.getRootDirectory()
        sys.path.insert(1, root_dir)

        # Deploy any code dependencies specified in the constructor
        self._python_includes = list()
        for path in pyFiles or []:
            self.addPyFile(path)

        # Deploy code dependencies set by spark-submit; these will already have been added
        # with SparkContext.addFile, so we just need to add them to the PYTHONPATH
        for path in self._conf.get("spark.submit.pyFiles", "").split(","):
            if path != "":
                (dirname, filename) = os.path.split(path)
                try:
                    filepath = os.path.join(SparkFiles.getRootDirectory(), filename)
                    if not os.path.exists(filepath):
                        # In case of YARN with shell mode, 'spark.submit.pyFiles' files are
                        # not added via SparkContext.addFile. Here we check if the file exists,
                        # try to copy and then add it to the path. See SPARK-21945.
                        shutil.copyfile(path, filepath)
                    if filename[-4:].lower() in self.PACKAGE_EXTENSIONS:
                        self._python_includes.append(filename)
                        sys.path.insert(1, filepath)
                except Exception:
                    warnings.warn(
                        "Failed to add file [%s] specified in 'spark.submit.pyFiles' to "
                        "Python path:\n  %s" % (path, "\n  ".join(sys.path)),
                        RuntimeWarning,
                    )

        # Create a temporary directory inside spark.local.dir:
        assert self._jvm is not None
        local_dir = self._jvm.org.apache.spark.util.Utils.getLocalDir(self._jsc.sc().conf())
        self._temp_dir = self._jvm.org.apache.spark.util.Utils.createTempDir(
            local_dir, "pyspark"
        ).getAbsolutePath()

        # profiling stats collected for each PythonRDD
        if (
            self._conf.get("spark.python.profile", "false") == "true"
            or self._conf.get("spark.python.profile.memory", "false") == "true"
        ):
            dump_path = self._conf.get("spark.python.profile.dump", None)
            self.profiler_collector = ProfilerCollector(
                profiler_cls, udf_profiler_cls, memory_profiler_cls, dump_path
            )
        else:
            self.profiler_collector = None  # type: ignore[assignment]

        # create a signal handler which would be invoked on receiving SIGINT
        def signal_handler(signal: Any, frame: Any) -> NoReturn:
            self.cancelAllJobs()
            raise KeyboardInterrupt()

        # see http://stackoverflow.com/questions/23206787/
        if isinstance(
            threading.current_thread(), threading._MainThread  # type: ignore[attr-defined]
        ):
            signal.signal(signal.SIGINT, signal_handler)

    def __repr__(self) -> str:
        return "<SparkContext master={master} appName={appName}>".format(
            master=self.master,
            appName=self.appName,
        )

    def _repr_html_(self) -> str:
        return """
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="{sc.uiWebUrl}">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v{sc.version}</code></dd>
              <dt>Master</dt>
                <dd><code>{sc.master}</code></dd>
              <dt>AppName</dt>
                <dd><code>{sc.appName}</code></dd>
            </dl>
        </div>
        """.format(
            sc=self
        )

    def _initialize_context(self, jconf: JavaObject) -> JavaObject:
        """
        Initialize SparkContext in function to allow subclass specific initialization
        """
        assert self._jvm is not None
        return self._jvm.JavaSparkContext(jconf)

    @classmethod
    def _ensure_initialized(
        cls,
        instance: Optional["SparkContext"] = None,
        gateway: Optional[JavaGateway] = None,
        conf: Optional[SparkConf] = None,
    ) -> None:
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
        """
        with SparkContext._lock:
            if not SparkContext._gateway:
                SparkContext._gateway = gateway or launch_gateway(conf)
                SparkContext._jvm = SparkContext._gateway.jvm

            if instance:
                if (
                    SparkContext._active_spark_context
                    and SparkContext._active_spark_context != instance
                ):
                    currentMaster = SparkContext._active_spark_context.master
                    currentAppName = SparkContext._active_spark_context.appName
                    callsite = SparkContext._active_spark_context._callsite

                    # Raise error if there is already a running Spark context
                    raise ValueError(
                        "Cannot run multiple SparkContexts at once; "
                        "existing SparkContext(app=%s, master=%s)"
                        " created by %s at %s:%s "
                        % (
                            currentAppName,
                            currentMaster,
                            callsite.function,
                            callsite.file,
                            callsite.linenum,
                        )
                    )
                else:
                    SparkContext._active_spark_context = instance

    def __getnewargs__(self) -> NoReturn:
        # This method is called when attempting to pickle SparkContext, which is always an error:
        raise PySparkRuntimeError(
            errorClass="CONTEXT_ONLY_VALID_ON_DRIVER",
            messageParameters={},
        )

    def __enter__(self) -> "SparkContext":
        """
        Enable 'with SparkContext(...) as sc: app(sc)' syntax.
        """
        return self

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        trace: Optional[TracebackType],
    ) -> None:
        """
        Enable 'with SparkContext(...) as sc: app' syntax.

        Specifically stop the context on exit of the with block.
        """
        self.stop()

    @classmethod
    def getOrCreate(cls, conf: Optional[SparkConf] = None) -> "SparkContext":
        """
        Get or instantiate a :class:`SparkContext` and register it as a singleton object.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        conf : :class:`SparkConf`, optional
            :class:`SparkConf` that will be used for initialization of the :class:`SparkContext`.

        Returns
        -------
        :class:`SparkContext`
            current :class:`SparkContext`, or a new one if it wasn't created before the function
            call.

        Examples
        --------
        >>> SparkContext.getOrCreate()
        <SparkContext ...>
        """
        with SparkContext._lock:
            if SparkContext._active_spark_context is None:
                SparkContext(conf=conf or SparkConf())
            assert SparkContext._active_spark_context is not None
            return SparkContext._active_spark_context

    def setLogLevel(self, logLevel: str) -> None:
        """
        Control our logLevel. This overrides any user-defined log settings.
        Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

        .. versionadded:: 1.4.0

        Parameters
        ----------
        logLevel : str
            The desired log level as a string.

        Examples
        --------
        >>> sc.setLogLevel("WARN")  # doctest :+SKIP
        """
        self._jsc.setLogLevel(logLevel)

    @classmethod
    def setSystemProperty(cls, key: str, value: str) -> None:
        """
        Set a Java system property, such as `spark.executor.memory`. This must
        be invoked before instantiating :class:`SparkContext`.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        key : str
            The key of a new Java system property.
        value : str
            The value of a new Java system property.
        """
        SparkContext._ensure_initialized()
        assert SparkContext._jvm is not None
        SparkContext._jvm.java.lang.System.setProperty(key, value)

    @classmethod
    def getSystemProperty(cls, key: str) -> str:
        """
        Get a Java system property, such as `java.home`.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        key : str
            The key of a new Java system property.

        Examples
        --------
        >>> sc.getSystemProperty("SPARK_SUBMIT")
        'true'
        >>> _ = sc.getSystemProperty("java.home")
        """
        SparkContext._ensure_initialized()
        assert SparkContext._jvm is not None
        return SparkContext._jvm.java.lang.System.getProperty(key)

    @property
    def version(self) -> str:
        """
        The version of Spark on which this application is running.

        .. versionadded:: 1.1.0

        Examples
        --------
        >>> _ = sc.version
        """
        return self._jsc.version()

    @property
    def applicationId(self) -> str:
        """
        A unique identifier for the Spark application.
        Its format depends on the scheduler implementation.

        * in case of local spark app something like 'local-1433865536131'
        * in case of YARN something like 'application_1433865536131_34483'

        .. versionadded:: 1.5.0

        Examples
        --------
        >>> sc.applicationId  # doctest: +ELLIPSIS
        'local-...'
        """
        return self._jsc.sc().applicationId()

    @property
    def uiWebUrl(self) -> Optional[str]:
        """Return the URL of the SparkUI instance started by this :class:`SparkContext`

        .. versionadded:: 2.1.0

        Notes
        -----
        When the web ui is disabled, e.g., by ``spark.ui.enabled`` set to ``False``,
        it returns ``None``.

        Examples
        --------
        >>> sc.uiWebUrl
        'http://...'
        """
        jurl = self._jsc.sc().uiWebUrl()
        return jurl.get() if jurl.nonEmpty() else None

    @property
    def startTime(self) -> int:
        """Return the epoch time when the :class:`SparkContext` was started.

        .. versionadded:: 1.5.0

        Examples
        --------
        >>> _ = sc.startTime
        """
        return self._jsc.startTime()

    @property
    def defaultParallelism(self) -> int:
        """
        Default level of parallelism to use when not given by user (e.g. for reduce tasks)

        .. versionadded:: 0.7.0

        Examples
        --------
        >>> sc.defaultParallelism > 0
        True
        """
        return self._jsc.sc().defaultParallelism()

    @property
    def defaultMinPartitions(self) -> int:
        """
        Default min number of partitions for Hadoop RDDs when not given by user

        .. versionadded:: 1.1.0

        Examples
        --------
        >>> sc.defaultMinPartitions > 0
        True
        """
        return self._jsc.sc().defaultMinPartitions()

    def stop(self) -> None:
        """
        Shut down the :class:`SparkContext`.

        .. versionadded:: 0.7.0
        """
        if getattr(self, "_jsc", None):
            try:
                self._jsc.stop()
            except Py4JError:
                # Case: SPARK-18523
                warnings.warn(
                    "Unable to cleanly shutdown Spark JVM process."
                    " It is possible that the process has crashed,"
                    " been killed or may also be in a zombie state.",
                    RuntimeWarning,
                )
            finally:
                self._jsc = None
        if getattr(self, "_accumulatorServer", None):
            self._accumulatorServer.shutdown()
            self._accumulatorServer = None  # type: ignore[assignment]
        with SparkContext._lock:
            SparkContext._active_spark_context = None

    def emptyRDD(self) -> RDD[Any]:
        """
        Create an :class:`RDD` that has no partitions or elements.

        .. versionadded:: 1.5.0

        Returns
        -------
        :class:`RDD`
            An empty RDD

        Examples
        --------
        >>> sc.emptyRDD()
        EmptyRDD...
        >>> sc.emptyRDD().count()
        0
        """
        return RDD(self._jsc.emptyRDD(), self, NoOpSerializer())

    def range(
        self, start: int, end: Optional[int] = None, step: int = 1, numSlices: Optional[int] = None
    ) -> RDD[int]:
        """
        Create a new RDD of int containing elements from `start` to `end`
        (exclusive), increased by `step` every element. Can be called the same
        way as python's built-in range() function. If called with a single argument,
        the argument is interpreted as `end`, and `start` is set to 0.

        .. versionadded:: 1.5.0

        Parameters
        ----------
        start : int
            the start value
        end : int, optional
            the end value (exclusive)
        step : int, optional, default 1
            the incremental step
        numSlices : int, optional
            the number of partitions of the new RDD

        Returns
        -------
        :class:`RDD`
            An RDD of int

        See Also
        --------
        :meth:`pyspark.sql.SparkSession.range`

        Examples
        --------
        >>> sc.range(5).collect()
        [0, 1, 2, 3, 4]
        >>> sc.range(2, 4).collect()
        [2, 3]
        >>> sc.range(1, 7, 2).collect()
        [1, 3, 5]

        Generate RDD with a negative step

        >>> sc.range(5, 0, -1).collect()
        [5, 4, 3, 2, 1]
        >>> sc.range(0, 5, -1).collect()
        []

        Control the number of partitions

        >>> sc.range(5, numSlices=1).getNumPartitions()
        1
        >>> sc.range(5, numSlices=10).getNumPartitions()
        10
        """
        if end is None:
            end = start
            start = 0

        return self.parallelize(range(start, end, step), numSlices)

    def parallelize(self, c: Iterable[T], numSlices: Optional[int] = None) -> RDD[T]:
        """
        Distribute a local Python collection to form an RDD. Using range
        is recommended if the input represents a range for performance.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        c : :class:`collections.abc.Iterable`
            iterable collection to distribute
        numSlices : int, optional
            the number of partitions of the new RDD

        Returns
        -------
        :class:`RDD`
            RDD representing distributed collection.

        Examples
        --------
        >>> sc.parallelize([0, 2, 3, 4, 6], 5).glom().collect()
        [[0], [2], [3], [4], [6]]
        >>> sc.parallelize(range(0, 6, 2), 5).glom().collect()
        [[], [0], [], [2], [4]]

        Deal with a list of strings.

        >>> strings = ["a", "b", "c"]
        >>> sc.parallelize(strings, 2).glom().collect()
        [['a'], ['b', 'c']]
        """
        numSlices = int(numSlices) if numSlices is not None else self.defaultParallelism
        if isinstance(c, range):
            size = len(c)
            if size == 0:
                return self.parallelize([], numSlices)
            step = c[1] - c[0] if size > 1 else 1  # type: ignore[index]
            start0 = c[0]  # type: ignore[index]

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
            1, min(len(c) // numSlices, self._batchSize or 1024)  # type: ignore[arg-type]
        )
        serializer = BatchedSerializer(self._unbatched_serializer, batchSize)

        def reader_func(temp_filename: str) -> JavaObject:
            assert self._jvm is not None
            return self._jvm.PythonRDD.readRDDFromFile(self._jsc, temp_filename, numSlices)

        def createRDDServer() -> JavaObject:
            assert self._jvm is not None
            return self._jvm.PythonParallelizeServer(self._jsc.sc(), numSlices)

        jrdd = self._serialize_to_jvm(c, serializer, reader_func, createRDDServer)
        return RDD(jrdd, self, serializer)

    def _serialize_to_jvm(
        self,
        data: Iterable[T],
        serializer: Serializer,
        reader_func: Callable,
        server_func: Callable,
    ) -> JavaObject:
        """
        Using Py4J to send a large dataset to the jvm is slow, so we use either a file
        or a socket if we have encryption enabled.

        Examples
        --------
        data
            object to be serialized
        serializer : class:`pyspark.serializers.Serializer`
        reader_func : function
            A function which takes a filename and reads in the data in the jvm and
            returns a JavaRDD. Only used when encryption is disabled.
        server_func : function
            A function which creates a SocketAuthServer in the JVM to
            accept the serialized data, for use when encryption is enabled.
        """
        if self._encryption_enabled:
            # with encryption, we open a server in java and send the data directly
            server = server_func()
            (sock_file, _) = local_connect_and_auth(server.port(), server.secret())
            chunked_out = ChunkedStream(sock_file, 8192)
            serializer.dump_stream(data, chunked_out)
            chunked_out.close()
            # this call will block until the server has read all the data and processed it (or
            # throws an exception)
            r = server.getResult()
            return r
        else:
            # without encryption, we serialize to a file, and we read the file in java and
            # parallelize from there.
            tempFile = NamedTemporaryFile(delete=False, dir=self._temp_dir)
            try:
                try:
                    serializer.dump_stream(data, tempFile)
                finally:
                    tempFile.close()
                return reader_func(tempFile.name)
            finally:
                # we eagerly reads the file so we can delete right after.
                os.unlink(tempFile.name)

    def pickleFile(self, name: str, minPartitions: Optional[int] = None) -> RDD[Any]:
        """
        Load an RDD previously saved using :meth:`RDD.saveAsPickleFile` method.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        name : str
            directory to the input data files, the path can be comma separated
            paths as a list of inputs
        minPartitions : int, optional
            suggested minimum number of partitions for the resulting RDD

        Returns
        -------
        :class:`RDD`
            RDD representing unpickled data from the file(s).

        See Also
        --------
        :meth:`RDD.saveAsPickleFile`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="pickleFile") as d:
        ...     # Write a temporary pickled file
        ...     path1 = os.path.join(d, "pickled1")
        ...     sc.parallelize(range(10)).saveAsPickleFile(path1, 3)
        ...
        ...     # Write another temporary pickled file
        ...     path2 = os.path.join(d, "pickled2")
        ...     sc.parallelize(range(-10, -5)).saveAsPickleFile(path2, 3)
        ...
        ...     # Load picked file
        ...     collected1 = sorted(sc.pickleFile(path1, 3).collect())
        ...     collected2 = sorted(sc.pickleFile(path2, 4).collect())
        ...
        ...     # Load two picked files together
        ...     collected3 = sorted(sc.pickleFile('{},{}'.format(path1, path2), 5).collect())

        >>> collected1
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        >>> collected2
        [-10, -9, -8, -7, -6]
        >>> collected3
        [-10, -9, -8, -7, -6, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(self._jsc.objectFile(name, minPartitions), self)

    def textFile(
        self, name: str, minPartitions: Optional[int] = None, use_unicode: bool = True
    ) -> RDD[str]:
        """
        Read a text file from HDFS, a local file system (available on all
        nodes), or any Hadoop-supported file system URI, and return it as an
        RDD of Strings. The text files must be encoded as UTF-8.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        name : str
            directory to the input data files, the path can be comma separated
            paths as a list of inputs
        minPartitions : int, optional
            suggested minimum number of partitions for the resulting RDD
        use_unicode : bool, default True
            If `use_unicode` is False, the strings will be kept as `str` (encoding
            as `utf-8`), which is faster and smaller than unicode.

            .. versionadded:: 1.2.0

        Returns
        -------
        :class:`RDD`
            RDD representing text data from the file(s).

        See Also
        --------
        :meth:`RDD.saveAsTextFile`
        :meth:`SparkContext.wholeTextFiles`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="textFile") as d:
        ...     path1 = os.path.join(d, "text1")
        ...     path2 = os.path.join(d, "text2")
        ...
        ...     # Write a temporary text file
        ...     sc.parallelize(["x", "y", "z"]).saveAsTextFile(path1)
        ...
        ...     # Write another temporary text file
        ...     sc.parallelize(["aa", "bb", "cc"]).saveAsTextFile(path2)
        ...
        ...     # Load text file
        ...     collected1 = sorted(sc.textFile(path1, 3).collect())
        ...     collected2 = sorted(sc.textFile(path2, 4).collect())
        ...
        ...     # Load two text files together
        ...     collected3 = sorted(sc.textFile('{},{}'.format(path1, path2), 5).collect())

        >>> collected1
        ['x', 'y', 'z']
        >>> collected2
        ['aa', 'bb', 'cc']
        >>> collected3
        ['aa', 'bb', 'cc', 'x', 'y', 'z']
        """
        minPartitions = minPartitions or min(self.defaultParallelism, 2)
        return RDD(self._jsc.textFile(name, minPartitions), self, UTF8Deserializer(use_unicode))

    def wholeTextFiles(
        self, path: str, minPartitions: Optional[int] = None, use_unicode: bool = True
    ) -> RDD[Tuple[str, str]]:
        """
        Read a directory of text files from HDFS, a local file system
        (available on all nodes), or any  Hadoop-supported file system
        URI. Each file is read as a single record and returned in a
        key-value pair, where the key is the path of each file, the
        value is the content of each file.
        The text files must be encoded as UTF-8.

        .. versionadded:: 1.0.0

        For example, if you have the following files:

        .. code-block:: text

            hdfs://a-hdfs-path/part-00000
            hdfs://a-hdfs-path/part-00001
            ...
            hdfs://a-hdfs-path/part-nnnnn

        Do ``rdd = sparkContext.wholeTextFiles("hdfs://a-hdfs-path")``,
        then ``rdd`` contains:

        .. code-block:: text

            (a-hdfs-path/part-00000, its content)
            (a-hdfs-path/part-00001, its content)
            ...
            (a-hdfs-path/part-nnnnn, its content)

        Parameters
        ----------
        path : str
            directory to the input data files, the path can be comma separated
            paths as a list of inputs
        minPartitions : int, optional
            suggested minimum number of partitions for the resulting RDD
        use_unicode : bool, default True
            If `use_unicode` is False, the strings will be kept as `str` (encoding
            as `utf-8`), which is faster and smaller than unicode.

            .. versionadded:: 1.2.0

        Returns
        -------
        :class:`RDD`
            RDD representing path-content pairs from the file(s).

        Notes
        -----
        Small files are preferred, as each file will be loaded fully in memory.

        See Also
        --------
        :meth:`RDD.saveAsTextFile`
        :meth:`SparkContext.textFile`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="wholeTextFiles") as d:
        ...     # Write a temporary text file
        ...     with open(os.path.join(d, "1.txt"), "w") as f:
        ...         _ = f.write("123")
        ...
        ...     # Write another temporary text file
        ...     with open(os.path.join(d, "2.txt"), "w") as f:
        ...         _ = f.write("xyz")
        ...
        ...     collected = sorted(sc.wholeTextFiles(d).collect())
        >>> collected
        [('.../1.txt', '123'), ('.../2.txt', 'xyz')]
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(
            self._jsc.wholeTextFiles(path, minPartitions),
            self,
            PairDeserializer(UTF8Deserializer(use_unicode), UTF8Deserializer(use_unicode)),
        )

    def binaryFiles(self, path: str, minPartitions: Optional[int] = None) -> RDD[Tuple[str, bytes]]:
        """
        Read a directory of binary files from HDFS, a local file system
        (available on all nodes), or any Hadoop-supported file system URI
        as a byte array. Each file is read as a single record and returned
        in a key-value pair, where the key is the path of each file, the
        value is the content of each file.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        path : str
            directory to the input data files, the path can be comma separated
            paths as a list of inputs
        minPartitions : int, optional
            suggested minimum number of partitions for the resulting RDD

        Returns
        -------
        :class:`RDD`
            RDD representing path-content pairs from the file(s).

        Notes
        -----
        Small files are preferred, large file is also allowable, but may cause bad performance.

        See Also
        --------
        :meth:`SparkContext.binaryRecords`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="binaryFiles") as d:
        ...     # Write a temporary binary file
        ...     with open(os.path.join(d, "1.bin"), "wb") as f1:
        ...         _ = f1.write(b"binary data I")
        ...
        ...     # Write another temporary binary file
        ...     with open(os.path.join(d, "2.bin"), "wb") as f2:
        ...         _ = f2.write(b"binary data II")
        ...
        ...     collected = sorted(sc.binaryFiles(d).collect())

        >>> collected
        [('.../1.bin', b'binary data I'), ('.../2.bin', b'binary data II')]
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(
            self._jsc.binaryFiles(path, minPartitions),
            self,
            PairDeserializer(UTF8Deserializer(), NoOpSerializer()),
        )

    def binaryRecords(self, path: str, recordLength: int) -> RDD[bytes]:
        """
        Load data from a flat binary file, assuming each record is a set of numbers
        with the specified numerical format (see ByteBuffer), and the number of
        bytes per record is constant.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        path : str
            Directory to the input data files
        recordLength : int
            The length at which to split the records

        Returns
        -------
        :class:`RDD`
            RDD of data with values, represented as byte arrays

        See Also
        --------
        :meth:`SparkContext.binaryFiles`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="binaryRecords") as d:
        ...     # Write a temporary file
        ...     with open(os.path.join(d, "1.bin"), "w") as f:
        ...         for i in range(3):
        ...             _ = f.write("%04d" % i)
        ...
        ...     # Write another file
        ...     with open(os.path.join(d, "2.bin"), "w") as f:
        ...         for i in [-1, -2, -10]:
        ...             _ = f.write("%04d" % i)
        ...
        ...     collected = sorted(sc.binaryRecords(d, 4).collect())

        >>> collected
        [b'-001', b'-002', b'-010', b'0000', b'0001', b'0002']
        """
        return RDD(self._jsc.binaryRecords(path, recordLength), self, NoOpSerializer())

    def _dictToJavaMap(self, d: Optional[Dict[str, str]]) -> JavaMap:
        assert self._jvm is not None
        jm = self._jvm.java.util.HashMap()
        if not d:
            d = {}
        for k, v in d.items():
            jm[k] = v
        return jm

    def sequenceFile(
        self,
        path: str,
        keyClass: Optional[str] = None,
        valueClass: Optional[str] = None,
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
        minSplits: Optional[int] = None,
        batchSize: int = 0,
    ) -> RDD[Tuple[T, U]]:
        """
        Read a Hadoop SequenceFile with arbitrary key and value Writable class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is as follows:

            1. A Java RDD is created from the SequenceFile or other InputFormat, and the key
               and value Writable classes
            2. Serialization is attempted via Pickle pickling
            3. If this fails, the fallback is to call 'toString' on each key and value
            4. :class:`CPickleSerializer` is used to deserialize pickled objects on the Python side

        .. versionadded:: 1.3.0

        Parameters
        ----------
        path : str
            path to sequencefile
        keyClass: str, optional
            fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")
        valueClass : str, optional
            fully qualified classname of value Writable class
            (e.g. "org.apache.hadoop.io.LongWritable")
        keyConverter : str, optional
            fully qualified name of a function returning key WritableConverter
        valueConverter : str, optional
            fully qualifiedname of a function returning value WritableConverter
        minSplits : int, optional
            minimum splits in dataset (default min(2, sc.defaultParallelism))
        batchSize : int, optional, default 0
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)

        Returns
        -------
        :class:`RDD`
            RDD of tuples of key and corresponding value

        See Also
        --------
        :meth:`RDD.saveAsSequenceFile`
        :meth:`RDD.saveAsNewAPIHadoopFile`
        :meth:`RDD.saveAsHadoopFile`
        :meth:`SparkContext.newAPIHadoopFile`
        :meth:`SparkContext.hadoopFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the class of output format

        >>> output_format_class = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"

        >>> with tempfile.TemporaryDirectory(prefix="sequenceFile") as d:
        ...     path = os.path.join(d, "hadoop_file")
        ...
        ...     # Write a temporary Hadoop file
        ...     rdd = sc.parallelize([(1, {3.0: "bb"}), (2, {1.0: "aa"}), (3, {2.0: "dd"})])
        ...     rdd.saveAsNewAPIHadoopFile(path, output_format_class)
        ...
        ...     collected = sorted(sc.sequenceFile(path).collect())

        >>> collected
        [(1, {3.0: 'bb'}), (2, {1.0: 'aa'}), (3, {2.0: 'dd'})]
        """
        minSplits = minSplits or min(self.defaultParallelism, 2)
        assert self._jvm is not None
        jrdd = self._jvm.PythonRDD.sequenceFile(
            self._jsc,
            path,
            keyClass,
            valueClass,
            keyConverter,
            valueConverter,
            minSplits,
            batchSize,
        )
        return RDD(jrdd, self)

    def newAPIHadoopFile(
        self,
        path: str,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
        batchSize: int = 0,
    ) -> RDD[Tuple[T, U]]:
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for meth:`SparkContext.sequenceFile`.

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java

        .. versionadded:: 1.1.0

        Parameters
        ----------
        path : str
            path to Hadoop file
        inputFormatClass : str
            fully qualified classname of Hadoop InputFormat
            (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        keyClass : str
            fully qualified classname of key Writable class
            (e.g. "org.apache.hadoop.io.Text")
        valueClass : str
            fully qualified classname of value Writable class
            (e.g. "org.apache.hadoop.io.LongWritable")
        keyConverter : str, optional
            fully qualified name of a function returning key WritableConverter
            None by default
        valueConverter : str, optional
            fully qualified name of a function returning value WritableConverter
            None by default
        conf : dict, optional
            Hadoop configuration, passed in as a dict
            None by default
        batchSize : int, optional, default 0
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)

        Returns
        -------
        :class:`RDD`
            RDD of tuples of key and corresponding value

        See Also
        --------
        :meth:`RDD.saveAsSequenceFile`
        :meth:`RDD.saveAsNewAPIHadoopFile`
        :meth:`RDD.saveAsHadoopFile`
        :meth:`SparkContext.sequenceFile`
        :meth:`SparkContext.hadoopFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> output_format_class = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
        >>> input_format_class = "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
        >>> key_class = "org.apache.hadoop.io.IntWritable"
        >>> value_class = "org.apache.hadoop.io.Text"

        >>> with tempfile.TemporaryDirectory(prefix="newAPIHadoopFile") as d:
        ...     path = os.path.join(d, "new_hadoop_file")
        ...
        ...     # Write a temporary Hadoop file
        ...     rdd = sc.parallelize([(1, ""), (1, "a"), (3, "x")])
        ...     rdd.saveAsNewAPIHadoopFile(path, output_format_class, key_class, value_class)
        ...
        ...     loaded = sc.newAPIHadoopFile(path, input_format_class, key_class, value_class)
        ...     collected = sorted(loaded.collect())

        >>> collected
        [(1, ''), (1, 'a'), (3, 'x')]
        """
        jconf = self._dictToJavaMap(conf)
        assert self._jvm is not None
        jrdd = self._jvm.PythonRDD.newAPIHadoopFile(
            self._jsc,
            path,
            inputFormatClass,
            keyClass,
            valueClass,
            keyConverter,
            valueConverter,
            jconf,
            batchSize,
        )
        return RDD(jrdd, self)

    def newAPIHadoopRDD(
        self,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
        batchSize: int = 0,
    ) -> RDD[Tuple[T, U]]:
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for meth:`SparkContext.sequenceFile`.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        inputFormatClass : str
            fully qualified classname of Hadoop InputFormat
            (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        keyClass : str
            fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")
        valueClass : str
            fully qualified classname of value Writable class
            (e.g. "org.apache.hadoop.io.LongWritable")
        keyConverter : str, optional
            fully qualified name of a function returning key WritableConverter
            (None by default)
        valueConverter : str, optional
            fully qualified name of a function returning value WritableConverter
            (None by default)
        conf : dict, optional
            Hadoop configuration, passed in as a dict (None by default)
        batchSize : int, optional, default 0
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)

        Returns
        -------
        :class:`RDD`
            RDD of tuples of key and corresponding value

        See Also
        --------
        :meth:`RDD.saveAsNewAPIHadoopDataset`
        :meth:`RDD.saveAsHadoopDataset`
        :meth:`SparkContext.hadoopRDD`
        :meth:`SparkContext.hadoopFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> output_format_class = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
        >>> input_format_class = "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
        >>> key_class = "org.apache.hadoop.io.IntWritable"
        >>> value_class = "org.apache.hadoop.io.Text"

        >>> with tempfile.TemporaryDirectory(prefix="newAPIHadoopRDD") as d:
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
        ...     loaded = sc.newAPIHadoopRDD(input_format_class,
        ...         key_class, value_class, conf=read_conf)
        ...     collected = sorted(loaded.collect())

        >>> collected
        [(1, ''), (1, 'a'), (3, 'x')]
        """
        jconf = self._dictToJavaMap(conf)
        assert self._jvm is not None
        jrdd = self._jvm.PythonRDD.newAPIHadoopRDD(
            self._jsc,
            inputFormatClass,
            keyClass,
            valueClass,
            keyConverter,
            valueConverter,
            jconf,
            batchSize,
        )
        return RDD(jrdd, self)

    def hadoopFile(
        self,
        path: str,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
        batchSize: int = 0,
    ) -> RDD[Tuple[T, U]]:
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for meth:`SparkContext.sequenceFile`.

        .. versionadded:: 1.1.0

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java.

        Parameters
        ----------
        path : str
            path to Hadoop file
        inputFormatClass : str
            fully qualified classname of Hadoop InputFormat
            (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        keyClass : str
            fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")
        valueClass : str
            fully qualified classname of value Writable class
            (e.g. "org.apache.hadoop.io.LongWritable")
        keyConverter : str, optional
            fully qualified name of a function returning key WritableConverter
        valueConverter : str, optional
            fully qualified name of a function returning value WritableConverter
        conf : dict, optional
            Hadoop configuration, passed in as a dict
        batchSize : int, optional, default 0
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)

        Returns
        -------
        :class:`RDD`
            RDD of tuples of key and corresponding value

        See Also
        --------
        :meth:`RDD.saveAsSequenceFile`
        :meth:`RDD.saveAsNewAPIHadoopFile`
        :meth:`RDD.saveAsHadoopFile`
        :meth:`SparkContext.newAPIHadoopFile`
        :meth:`SparkContext.hadoopRDD`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> output_format_class = "org.apache.hadoop.mapred.TextOutputFormat"
        >>> input_format_class = "org.apache.hadoop.mapred.TextInputFormat"
        >>> key_class = "org.apache.hadoop.io.IntWritable"
        >>> value_class = "org.apache.hadoop.io.Text"

        >>> with tempfile.TemporaryDirectory(prefix="hadoopFile") as d:
        ...     path = os.path.join(d, "old_hadoop_file")
        ...
        ...     # Write a temporary Hadoop file
        ...     rdd = sc.parallelize([(1, ""), (1, "a"), (3, "x")])
        ...     rdd.saveAsHadoopFile(path, output_format_class, key_class, value_class)
        ...
        ...     loaded = sc.hadoopFile(path, input_format_class, key_class, value_class)
        ...     collected = sorted(loaded.collect())

        >>> collected
        [(0, '1\\t'), (0, '1\\ta'), (0, '3\\tx')]
        """
        jconf = self._dictToJavaMap(conf)
        assert self._jvm is not None
        jrdd = self._jvm.PythonRDD.hadoopFile(
            self._jsc,
            path,
            inputFormatClass,
            keyClass,
            valueClass,
            keyConverter,
            valueConverter,
            jconf,
            batchSize,
        )
        return RDD(jrdd, self)

    def hadoopRDD(
        self,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = None,
        valueConverter: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
        batchSize: int = 0,
    ) -> RDD[Tuple[T, U]]:
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for meth:`SparkContext.sequenceFile`.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        inputFormatClass : str
            fully qualified classname of Hadoop InputFormat
            (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        keyClass : str
            fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")
        valueClass : str
            fully qualified classname of value Writable class
            (e.g. "org.apache.hadoop.io.LongWritable")
        keyConverter : str, optional
            fully qualified name of a function returning key WritableConverter
        valueConverter : str, optional
            fully qualified name of a function returning value WritableConverter
        conf : dict, optional
            Hadoop configuration, passed in as a dict
        batchSize : int, optional, default 0
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)

        Returns
        -------
        :class:`RDD`
            RDD of tuples of key and corresponding value

        See Also
        --------
        :meth:`RDD.saveAsNewAPIHadoopDataset`
        :meth:`RDD.saveAsHadoopDataset`
        :meth:`SparkContext.newAPIHadoopRDD`
        :meth:`SparkContext.hadoopFile`

        Examples
        --------
        >>> import os
        >>> import tempfile

        Set the related classes

        >>> output_format_class = "org.apache.hadoop.mapred.TextOutputFormat"
        >>> input_format_class = "org.apache.hadoop.mapred.TextInputFormat"
        >>> key_class = "org.apache.hadoop.io.IntWritable"
        >>> value_class = "org.apache.hadoop.io.Text"

        >>> with tempfile.TemporaryDirectory(prefix="hadoopRDD") as d:
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
        ...     loaded = sc.hadoopRDD(input_format_class, key_class, value_class, conf=read_conf)
        ...     collected = sorted(loaded.collect())

        >>> collected
        [(0, '1\\t'), (0, '1\\ta'), (0, '3\\tx')]
        """
        jconf = self._dictToJavaMap(conf)
        assert self._jvm is not None
        jrdd = self._jvm.PythonRDD.hadoopRDD(
            self._jsc,
            inputFormatClass,
            keyClass,
            valueClass,
            keyConverter,
            valueConverter,
            jconf,
            batchSize,
        )
        return RDD(jrdd, self)

    def _checkpointFile(self, name: str, input_deserializer: PairDeserializer) -> RDD:
        jrdd = self._jsc.checkpointFile(name)
        return RDD(jrdd, self, input_deserializer)

    def union(self, rdds: List[RDD[T]]) -> RDD[T]:
        """
        Build the union of a list of RDDs.

        This supports unions() of RDDs with different serialized formats,
        although this forces them to be reserialized using the default
        serializer:

        .. versionadded:: 0.7.0

        See Also
        --------
        :meth:`RDD.union`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="union") as d:
        ...     # generate a text RDD
        ...     with open(os.path.join(d, "union-text.txt"), "w") as f:
        ...         _ = f.write("Hello")
        ...     text_rdd = sc.textFile(d)
        ...
        ...     # generate another RDD
        ...     parallelized = sc.parallelize(["World!"])
        ...
        ...     unioned = sorted(sc.union([text_rdd, parallelized]).collect())

        >>> unioned
        ['Hello', 'World!']
        """
        first_jrdd_deserializer = rdds[0]._jrdd_deserializer
        if any(x._jrdd_deserializer != first_jrdd_deserializer for x in rdds):
            rdds = [x._reserialize() for x in rdds]
        gw = SparkContext._gateway
        assert gw is not None
        jvm = SparkContext._jvm
        assert jvm is not None
        jrdd_cls = jvm.org.apache.spark.api.java.JavaRDD
        jpair_rdd_cls = jvm.org.apache.spark.api.java.JavaPairRDD
        jdouble_rdd_cls = jvm.org.apache.spark.api.java.JavaDoubleRDD
        if is_instance_of(gw, rdds[0]._jrdd, jrdd_cls):
            cls = jrdd_cls
        elif is_instance_of(gw, rdds[0]._jrdd, jpair_rdd_cls):
            cls = jpair_rdd_cls
        elif is_instance_of(gw, rdds[0]._jrdd, jdouble_rdd_cls):
            cls = jdouble_rdd_cls
        else:
            cls_name = rdds[0]._jrdd.getClass().getCanonicalName()
            raise TypeError("Unsupported Java RDD class %s" % cls_name)
        jrdds = gw.new_array(cls, len(rdds))
        for i in range(0, len(rdds)):
            jrdds[i] = rdds[i]._jrdd
        return RDD(self._jsc.union(jrdds), self, rdds[0]._jrdd_deserializer)

    def broadcast(self, value: T) -> "Broadcast[T]":
        """
        Broadcast a read-only variable to the cluster, returning a :class:`Broadcast`
        object for reading it in distributed functions. The variable will
        be sent to each cluster only once.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        value : T
            value to broadcast to the Spark nodes

        Returns
        -------
        :class:`Broadcast`
            :class:`Broadcast` object, a read-only variable cached on each machine

        Examples
        --------
        >>> mapping = {1: 10001, 2: 10002}
        >>> bc = sc.broadcast(mapping)

        >>> rdd = sc.range(5)
        >>> rdd2 = rdd.map(lambda i: bc.value[i] if i in bc.value else -1)
        >>> rdd2.collect()
        [-1, 10001, 10002, -1, -1]

        >>> bc.destroy()
        """
        return Broadcast(self, value, self._pickled_broadcast_vars)

    def accumulator(
        self, value: T, accum_param: Optional["AccumulatorParam[T]"] = None
    ) -> "Accumulator[T]":
        """
        Create an :class:`Accumulator` with the given initial value, using a given
        :class:`AccumulatorParam` helper object to define how to add values of the
        data type if provided. Default AccumulatorParams are used for integers
        and floating-point numbers if you do not provide one. For other types,
        a custom AccumulatorParam can be used.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        value : T
            initialized value
        accum_param : :class:`pyspark.AccumulatorParam`, optional
            helper object to define how to add values

        Returns
        -------
        :class:`Accumulator`
            `Accumulator` object, a shared variable that can be accumulated

        Examples
        --------
        >>> acc = sc.accumulator(9)
        >>> acc.value
        9
        >>> acc += 1
        >>> acc.value
        10

        Accumulator object can be accumulated in RDD operations:

        >>> rdd = sc.range(5)
        >>> def f(x):
        ...     global acc
        ...     acc += 1
        ...
        >>> rdd.foreach(f)
        >>> acc.value
        15
        """
        if accum_param is None:
            if isinstance(value, int):
                accum_param = cast("AccumulatorParam[T]", accumulators.INT_ACCUMULATOR_PARAM)
            elif isinstance(value, float):
                accum_param = cast("AccumulatorParam[T]", accumulators.FLOAT_ACCUMULATOR_PARAM)
            elif isinstance(value, complex):
                accum_param = cast("AccumulatorParam[T]", accumulators.COMPLEX_ACCUMULATOR_PARAM)
            else:
                raise TypeError("No default accumulator param for type %s" % type(value))
        SparkContext._next_accum_id += 1
        return Accumulator(SparkContext._next_accum_id - 1, value, accum_param)

    def addFile(self, path: str, recursive: bool = False) -> None:
        """
        Add a file to be downloaded with this Spark job on every node.
        The `path` passed can be either a local file, a file in HDFS
        (or other Hadoop-supported filesystems), or an HTTP, HTTPS or
        FTP URI.

        To access the file in Spark jobs, use :meth:`SparkFiles.get` with the
        filename to find its download location.

        A directory can be given if the recursive option is set to True.
        Currently directories are only supported for Hadoop-supported filesystems.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        path : str
            can be either a local file, a file in HDFS (or other Hadoop-supported
            filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
            use :meth:`SparkFiles.get` to find its download location.
        recursive : bool, default False
            whether to recursively add files in the input directory

        See Also
        --------
        :meth:`SparkContext.listFiles`
        :meth:`SparkContext.addPyFile`
        :meth:`SparkFiles.get`

        Notes
        -----
        A path can be added only once. Subsequent additions of the same path are ignored.

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> from pyspark import SparkFiles

        >>> with tempfile.TemporaryDirectory(prefix="addFile") as d:
        ...     path1 = os.path.join(d, "test1.txt")
        ...     with open(path1, "w") as f:
        ...         _ = f.write("100")
        ...
        ...     path2 = os.path.join(d, "test2.txt")
        ...     with open(path2, "w") as f:
        ...         _ = f.write("200")
        ...
        ...     sc.addFile(path1)
        ...     file_list1 = sorted(sc.listFiles)
        ...
        ...     sc.addFile(path2)
        ...     file_list2 = sorted(sc.listFiles)
        ...
        ...     # add path2 twice, this addition will be ignored
        ...     sc.addFile(path2)
        ...     file_list3 = sorted(sc.listFiles)
        ...
        ...     def func(iterator):
        ...         with open(SparkFiles.get("test1.txt")) as f:
        ...             mul = int(f.readline())
        ...             return [x * mul for x in iterator]
        ...
        ...     collected = sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()

        >>> file_list1
        ['file:/.../test1.txt']
        >>> file_list2
        ['file:/.../test1.txt', 'file:/.../test2.txt']
        >>> file_list3
        ['file:/.../test1.txt', 'file:/.../test2.txt']
        >>> collected
        [100, 200, 300, 400]
        """
        self._jsc.sc().addFile(path, recursive)

    @property
    def listFiles(self) -> List[str]:
        """Returns a list of file paths that are added to resources.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`SparkContext.addFile`
        """
        return list(
            self._jvm.scala.jdk.javaapi.CollectionConverters.asJava(  # type: ignore[union-attr]
                self._jsc.sc().listFiles()
            )
        )

    def addPyFile(self, path: str) -> None:
        """
        Add a .py or .zip dependency for all tasks to be executed on this
        SparkContext in the future.  The `path` passed can be either a local
        file, a file in HDFS (or other Hadoop-supported filesystems), or an
        HTTP, HTTPS or FTP URI.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        path : str
            can be either a .py file or .zip dependency.

        See Also
        --------
        :meth:`SparkContext.addFile`

        Notes
        -----
        A path can be added only once. Subsequent additions of the same path are ignored.
        """
        self.addFile(path)
        (dirname, filename) = os.path.split(path)  # dirname may be directory or HDFS/S3 prefix
        if filename[-4:].lower() in self.PACKAGE_EXTENSIONS:
            assert self._python_includes is not None
            self._python_includes.append(filename)
            # for tests in local mode
            sys.path.insert(1, os.path.join(SparkFiles.getRootDirectory(), filename))

        importlib.invalidate_caches()

    def addArchive(self, path: str) -> None:
        """
        Add an archive to be downloaded with this Spark job on every node.
        The `path` passed can be either a local file, a file in HDFS
        (or other Hadoop-supported filesystems), or an HTTP, HTTPS or
        FTP URI.

        To access the file in Spark jobs, use :meth:`SparkFiles.get` with the
        filename to find its download/unpacked location. The given path should
        be one of .zip, .tar, .tar.gz, .tgz and .jar.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        path : str
            can be either a local file, a file in HDFS (or other Hadoop-supported
            filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
            use :meth:`SparkFiles.get` to find its download location.

        See Also
        --------
        :meth:`SparkContext.listArchives`
        :meth:`SparkFiles.get`

        Notes
        -----
        A path can be added only once. Subsequent additions of the same path are ignored.
        This API is experimental.

        Examples
        --------
        Creates a zipped file that contains a text file written '100'.

        >>> import os
        >>> import tempfile
        >>> import zipfile
        >>> from pyspark import SparkFiles

        >>> with tempfile.TemporaryDirectory(prefix="addArchive") as d:
        ...     path = os.path.join(d, "test.txt")
        ...     with open(path, "w") as f:
        ...         _ = f.write("100")
        ...
        ...     zip_path1 = os.path.join(d, "test1.zip")
        ...     with zipfile.ZipFile(zip_path1, "w", zipfile.ZIP_DEFLATED) as z:
        ...         z.write(path, os.path.basename(path))
        ...
        ...     zip_path2 = os.path.join(d, "test2.zip")
        ...     with zipfile.ZipFile(zip_path2, "w", zipfile.ZIP_DEFLATED) as z:
        ...         z.write(path, os.path.basename(path))
        ...
        ...     sc.addArchive(zip_path1)
        ...     arch_list1 = sorted(sc.listArchives)
        ...
        ...     sc.addArchive(zip_path2)
        ...     arch_list2 = sorted(sc.listArchives)
        ...
        ...     # add zip_path2 twice, this addition will be ignored
        ...     sc.addArchive(zip_path2)
        ...     arch_list3 = sorted(sc.listArchives)
        ...
        ...     def func(iterator):
        ...         with open("%s/test.txt" % SparkFiles.get("test1.zip")) as f:
        ...             mul = int(f.readline())
        ...             return [x * mul for x in iterator]
        ...
        ...     collected = sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()

        >>> arch_list1
        ['file:/.../test1.zip']
        >>> arch_list2
        ['file:/.../test1.zip', 'file:/.../test2.zip']
        >>> arch_list3
        ['file:/.../test1.zip', 'file:/.../test2.zip']
        >>> collected
        [100, 200, 300, 400]
        """
        self._jsc.sc().addArchive(path)

    @property
    def listArchives(self) -> List[str]:
        """Returns a list of archive paths that are added to resources.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`SparkContext.addArchive`
        """
        return list(
            self._jvm.scala.jdk.javaapi.CollectionConverters.asJava(  # type: ignore[union-attr]
                self._jsc.sc().listArchives()
            )
        )

    def setCheckpointDir(self, dirName: str) -> None:
        """
        Set the directory under which RDDs are going to be checkpointed. The
        directory must be an HDFS path if running on a cluster.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        dirName : str
            path to the directory where checkpoint files will be stored
            (must be HDFS path if running in cluster)

        See Also
        --------
        :meth:`SparkContext.getCheckpointDir`
        :meth:`RDD.checkpoint`
        :meth:`RDD.getCheckpointFile`
        """
        self._jsc.sc().setCheckpointDir(dirName)

    def getCheckpointDir(self) -> Optional[str]:
        """
        Return the directory where RDDs are checkpointed. Returns None if no
        checkpoint directory has been set.

        .. versionadded:: 3.1.0

        See Also
        --------
        :meth:`SparkContext.setCheckpointDir`
        :meth:`RDD.checkpoint`
        :meth:`RDD.getCheckpointFile`
        """
        if not self._jsc.sc().getCheckpointDir().isEmpty():
            return self._jsc.sc().getCheckpointDir().get()
        return None

    def _getJavaStorageLevel(self, storageLevel: StorageLevel) -> JavaObject:
        """
        Returns a Java StorageLevel based on a pyspark.StorageLevel.
        """
        if not isinstance(storageLevel, StorageLevel):
            raise TypeError("storageLevel must be of type pyspark.StorageLevel")
        assert self._jvm is not None
        newStorageLevel = self._jvm.org.apache.spark.storage.StorageLevel
        return newStorageLevel(
            storageLevel.useDisk,
            storageLevel.useMemory,
            storageLevel.useOffHeap,
            storageLevel.deserialized,
            storageLevel.replication,
        )

    def setJobGroup(self, groupId: str, description: str, interruptOnCancel: bool = False) -> None:
        """
        Assigns a group ID to all the jobs started by this thread until the group ID is set to a
        different value or cleared.

        Often, a unit of execution in an application consists of multiple Spark actions or jobs.
        Application programmers can use this method to group all those jobs together and give a
        group description. Once set, the Spark web UI will associate such jobs with this group.

        The application can use :meth:`SparkContext.cancelJobGroup` to cancel all
        running jobs in this group.

        .. versionadded:: 1.0.0

        Parameters
        ----------
        groupId : str
            The group ID to assign.
        description : str
            The description to set for the job group.
        interruptOnCancel : bool, optional, default False
            whether to interrupt jobs on job cancellation.

        Notes
        -----
        If interruptOnCancel is set to true for the job group, then job cancellation will result
        in Thread.interrupt() being called on the job's executor threads. This is useful to help
        ensure that the tasks are actually stopped in a timely manner, but is off by default due
        to HDFS-1208, where HDFS may respond to Thread.interrupt() by marking nodes as dead.

        If you run jobs in parallel, use :class:`pyspark.InheritableThread` for thread
        local inheritance.

        See Also
        --------
        :meth:`SparkContext.cancelJobGroup`

        Examples
        --------
        >>> import threading
        >>> from time import sleep
        >>> from pyspark import InheritableThread
        >>> result = "Not Set"
        >>> lock = threading.Lock()
        >>> def map_func(x):
        ...     sleep(100)
        ...     raise RuntimeError("Task should have been cancelled")
        ...
        >>> def start_job(x):
        ...     global result
        ...     try:
        ...         sc.setJobGroup("job_to_cancel", "some description")
        ...         result = sc.parallelize(range(x)).map(map_func).collect()
        ...     except Exception as e:
        ...         result = "Cancelled"
        ...     lock.release()
        ...
        >>> def stop_job():
        ...     sleep(5)
        ...     sc.cancelJobGroup("job_to_cancel")
        ...
        >>> suppress = lock.acquire()
        >>> suppress = InheritableThread(target=start_job, args=(10,)).start()
        >>> suppress = InheritableThread(target=stop_job).start()
        >>> suppress = lock.acquire()
        >>> print(result)
        Cancelled
        """
        self._jsc.setJobGroup(groupId, description, interruptOnCancel)

    def setInterruptOnCancel(self, interruptOnCancel: bool) -> None:
        """
        Set the behavior of job cancellation from jobs started in this thread.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        interruptOnCancel : bool
            If true, then job cancellation will result in ``Thread.interrupt()``
            being called on the job's executor threads. This is useful to help ensure that
            the tasks are actually stopped in a timely manner, but is off by default due to
            HDFS-1208, where HDFS may respond to ``Thread.interrupt()`` by marking nodes as dead.

        See Also
        --------
        :meth:`SparkContext.addJobTag`
        :meth:`SparkContext.removeJobTag`
        :meth:`SparkContext.cancelAllJobs`
        :meth:`SparkContext.cancelJobGroup`
        :meth:`SparkContext.cancelJobsWithTag`
        """
        self._jsc.setInterruptOnCancel(interruptOnCancel)

    def addJobTag(self, tag: str) -> None:
        """
        Add a tag to be assigned to all the jobs started by this thread.

        Often, a unit of execution in an application consists of multiple Spark actions or jobs.
        Application programmers can use this method to group all those jobs together and give a
        group tag. The application can use :meth:`SparkContext.cancelJobsWithTag` to cancel all
        running executions with this tag.

        There may be multiple tags present at the same time, so different parts of application may
        use different tags to perform cancellation at different levels of granularity.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        tag : str
            The tag to be added. Cannot contain ',' (comma) character.

        See Also
        --------
        :meth:`SparkContext.removeJobTag`
        :meth:`SparkContext.getJobTags`
        :meth:`SparkContext.clearJobTags`
        :meth:`SparkContext.cancelJobsWithTag`
        :meth:`SparkContext.setInterruptOnCancel`

        Examples
        --------
        >>> import threading
        >>> from time import sleep
        >>> from pyspark import InheritableThread
        >>> sc.setInterruptOnCancel(interruptOnCancel=True)
        >>> result = "Not Set"
        >>> lock = threading.Lock()
        >>> def map_func(x):
        ...     sleep(100)
        ...     raise RuntimeError("Task should have been cancelled")
        ...
        >>> def start_job(x):
        ...     global result
        ...     try:
        ...         sc.addJobTag("job_to_cancel")
        ...         result = sc.parallelize(range(x)).map(map_func).collect()
        ...     except Exception as e:
        ...         result = "Cancelled"
        ...     lock.release()
        ...
        >>> def stop_job():
        ...     sleep(5)
        ...     sc.cancelJobsWithTag("job_to_cancel")
        ...
        >>> suppress = lock.acquire()
        >>> suppress = InheritableThread(target=start_job, args=(10,)).start()
        >>> suppress = InheritableThread(target=stop_job).start()
        >>> suppress = lock.acquire()
        >>> print(result)
        Cancelled
        >>> sc.clearJobTags()
        """
        self._jsc.addJobTag(tag)

    def removeJobTag(self, tag: str) -> None:
        """
        Remove a tag previously added to be assigned to all the jobs started by this thread.
        Noop if such a tag was not added earlier.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        tag : str
            The tag to be removed. Cannot contain ',' (comma) character.

        See Also
        --------
        :meth:`SparkContext.addJobTag`
        :meth:`SparkContext.getJobTags`
        :meth:`SparkContext.clearJobTags`
        :meth:`SparkContext.cancelJobsWithTag`
        :meth:`SparkContext.setInterruptOnCancel`

        Examples
        --------
        >>> sc.addJobTag("job_to_cancel1")
        >>> sc.addJobTag("job_to_cancel2")
        >>> sc.getJobTags()
        {'job_to_cancel1', 'job_to_cancel2'}
        >>> sc.removeJobTag("job_to_cancel1")
        >>> sc.getJobTags()
        {'job_to_cancel2'}
        >>> sc.clearJobTags()
        """
        self._jsc.removeJobTag(tag)

    def getJobTags(self) -> Set[str]:
        """
        Get the tags that are currently set to be assigned to all the jobs started by this thread.

        .. versionadded:: 3.5.0

        Returns
        -------
        set of str
            the tags that are currently set to be assigned to all the jobs started by this thread.

        See Also
        --------
        :meth:`SparkContext.addJobTag`
        :meth:`SparkContext.removeJobTag`
        :meth:`SparkContext.clearJobTags`
        :meth:`SparkContext.cancelJobsWithTag`
        :meth:`SparkContext.setInterruptOnCancel`

        Examples
        --------
        >>> sc.addJobTag("job_to_cancel")
        >>> sc.getJobTags()
        {'job_to_cancel'}
        >>> sc.clearJobTags()
        """
        return self._jsc.getJobTags()

    def clearJobTags(self) -> None:
        """
        Clear the current thread's job tags.

        .. versionadded:: 3.5.0

        See Also
        --------
        :meth:`SparkContext.addJobTag`
        :meth:`SparkContext.removeJobTag`
        :meth:`SparkContext.getJobTags`
        :meth:`SparkContext.cancelJobsWithTag`
        :meth:`SparkContext.setInterruptOnCancel`

        Examples
        --------
        >>> sc.addJobTag("job_to_cancel")
        >>> sc.clearJobTags()
        >>> sc.getJobTags()
        set()
        """
        self._jsc.clearJobTags()

    def setLocalProperty(self, key: str, value: str) -> None:
        """
        Set a local property that affects jobs submitted from this thread, such as the
        Spark fair scheduler pool.

        To remove/unset property simply set `value` to None e.g. sc.setLocalProperty("key", None)

        .. versionadded:: 1.0.0

        Parameters
        ----------
        key : str
            The key of the local property to set.
        value : str
            The value of the local property to set. If set to `None` then the
            property will be removed

        See Also
        --------
        :meth:`SparkContext.getLocalProperty`

        Notes
        -----
        If you run jobs in parallel, use :class:`pyspark.InheritableThread` for thread
        local inheritance.
        """
        self._jsc.setLocalProperty(key, value)

    def getLocalProperty(self, key: str) -> Optional[str]:
        """
        Get a local property set in this thread, or null if it is missing. See
        :meth:`setLocalProperty`.

        .. versionadded:: 1.0.0

        See Also
        --------
        :meth:`SparkContext.setLocalProperty`
        """
        return self._jsc.getLocalProperty(key)

    def setJobDescription(self, value: str) -> None:
        """
        Set a human readable description of the current job.

        .. versionadded:: 2.3.0

        Parameters
        ----------
        value : str
            The job description to set.

        Notes
        -----
        If you run jobs in parallel, use :class:`pyspark.InheritableThread` for thread
        local inheritance.
        """
        self._jsc.setJobDescription(value)

    def sparkUser(self) -> str:
        """
        Get SPARK_USER for user who is running SparkContext.

        .. versionadded:: 1.0.0
        """
        return self._jsc.sc().sparkUser()

    def cancelJobGroup(self, groupId: str) -> None:
        """
        Cancel active jobs for the specified group. See :meth:`SparkContext.setJobGroup`.
        for more information.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        groupId : str
            The group ID to cancel the job.

        See Also
        --------
        :meth:`SparkContext.setJobGroup`
        """
        self._jsc.sc().cancelJobGroup(groupId)

    def cancelJobsWithTag(self, tag: str) -> None:
        """
        Cancel active jobs that have the specified tag. See
        :meth:`SparkContext.addJobTag`.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        tag : str
            The tag to be cancelled. Cannot contain ',' (comma) character.

        See Also
        --------
        :meth:`SparkContext.addJobTag`
        :meth:`SparkContext.removeJobTag`
        :meth:`SparkContext.getJobTags`
        :meth:`SparkContext.clearJobTags`
        :meth:`SparkContext.setInterruptOnCancel`
        """
        return self._jsc.cancelJobsWithTag(tag)

    def cancelAllJobs(self) -> None:
        """
        Cancel all jobs that have been scheduled or are running.

        .. versionadded:: 1.1.0

        See Also
        --------
        :meth:`SparkContext.cancelJobGroup`
        :meth:`SparkContext.cancelJobsWithTag`
        :meth:`SparkContext.runJob`
        """
        self._jsc.sc().cancelAllJobs()

    def statusTracker(self) -> StatusTracker:
        """
        Return :class:`StatusTracker` object

        .. versionadded:: 1.4.0
        """
        return StatusTracker(self._jsc.statusTracker())

    def runJob(
        self,
        rdd: RDD[T],
        partitionFunc: Callable[[Iterable[T]], Iterable[U]],
        partitions: Optional[Sequence[int]] = None,
        allowLocal: bool = False,
    ) -> List[U]:
        """
        Executes the given partitionFunc on the specified set of partitions,
        returning the result as an array of elements.

        If 'partitions' is not specified, this will run over all partitions.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        rdd : :class:`RDD`
            target RDD to run tasks on
        partitionFunc : function
            a function to run on each partition of the RDD
        partitions : list, optional
            set of partitions to run on; some jobs may not want to compute on all
            partitions of the target RDD, e.g. for operations like `first`
        allowLocal : bool, default False
            this parameter takes no effect

        Returns
        -------
        list
            results of specified partitions

        See Also
        --------
        :meth:`SparkContext.cancelAllJobs`

        Examples
        --------
        >>> myRDD = sc.parallelize(range(6), 3)
        >>> sc.runJob(myRDD, lambda part: [x * x for x in part])
        [0, 1, 4, 9, 16, 25]

        >>> myRDD = sc.parallelize(range(6), 3)
        >>> sc.runJob(myRDD, lambda part: [x * x for x in part], [0, 2], True)
        [0, 1, 16, 25]
        """
        if partitions is None:
            partitions = list(range(rdd._jrdd.partitions().size()))

        # Implementation note: This is implemented as a mapPartitions followed
        # by runJob() in order to avoid having to pass a Python lambda into
        # SparkContext#runJob.
        mappedRDD = rdd.mapPartitions(partitionFunc)
        assert self._jvm is not None
        sock_info = self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, partitions)
        return list(_load_from_socket(sock_info, mappedRDD._jrdd_deserializer))

    def show_profiles(self) -> None:
        """Print the profile stats to stdout

        .. versionadded:: 1.2.0

        See Also
        --------
        :meth:`SparkContext.dump_profiles`
        """
        if self.profiler_collector is not None:
            self.profiler_collector.show_profiles()
        else:
            raise PySparkRuntimeError(
                errorClass="INCORRECT_CONF_FOR_PROFILE",
                messageParameters={},
            )

    def dump_profiles(self, path: str) -> None:
        """Dump the profile stats into directory `path`

        .. versionadded:: 1.2.0

        See Also
        --------
        :meth:`SparkContext.show_profiles`
        """
        if self.profiler_collector is not None:
            self.profiler_collector.dump_profiles(path)
        else:
            raise PySparkRuntimeError(
                errorClass="INCORRECT_CONF_FOR_PROFILE",
                messageParameters={},
            )

    def getConf(self) -> SparkConf:
        """Return a copy of this SparkContext's configuration :class:`SparkConf`.

        .. versionadded:: 2.1.0
        """
        conf = SparkConf()
        conf.setAll(self._conf.getAll())
        return conf

    @property
    def resources(self) -> Dict[str, ResourceInformation]:
        """
        Return the resource information of this :class:`SparkContext`.
        A resource could be a GPU, FPGA, etc.

        .. versionadded:: 3.0.0
        """
        resources = {}
        jresources = self._jsc.resources()
        for x in jresources:
            name = jresources[x].name()
            jaddresses = jresources[x].addresses()
            addrs = [addr for addr in jaddresses]
            resources[name] = ResourceInformation(name, addrs)
        return resources

    @staticmethod
    def _assert_on_driver() -> None:
        """
        Called to ensure that SparkContext is created only on the Driver.

        Throws an exception if a SparkContext is about to be created in executors.
        """
        if TaskContext.get() is not None:
            raise PySparkRuntimeError(
                errorClass="CONTEXT_ONLY_VALID_ON_DRIVER",
                messageParameters={},
            )


def _test() -> None:
    import doctest

    globs = globals().copy()
    conf = SparkConf().set("spark.ui.enabled", "True")
    globs["sc"] = SparkContext("local[4]", "context tests", conf=conf)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
