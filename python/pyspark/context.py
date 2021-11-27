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

from py4j.protocol import Py4JError
from py4j.java_gateway import is_instance_of

from pyspark import accumulators, since
from pyspark.accumulators import Accumulator
from pyspark.broadcast import Broadcast, BroadcastPickleRegistry
from pyspark.conf import SparkConf
from pyspark.files import SparkFiles
from pyspark.java_gateway import launch_gateway, local_connect_and_auth
from pyspark.serializers import (
    CPickleSerializer,
    BatchedSerializer,
    UTF8Deserializer,
    PairDeserializer,
    AutoBatchedSerializer,
    NoOpSerializer,
    ChunkedStream,
)
from pyspark.storagelevel import StorageLevel
from pyspark.resource.information import ResourceInformation
from pyspark.rdd import RDD, _load_from_socket
from pyspark.taskcontext import TaskContext
from pyspark.traceback_utils import CallSite, first_spark_call
from pyspark.status import StatusTracker
from pyspark.profiler import ProfilerCollector, BasicProfiler


__all__ = ["SparkContext"]


# These are special default configs for PySpark, they will overwrite
# the default ones for Spark if they are not configured by user.
DEFAULT_CONFIGS = {
    "spark.serializer.objectStreamReset": 100,
    "spark.rdd.compress": True,
}


class SparkContext(object):

    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create :class:`RDD` and
    broadcast variables on that cluster.

    When you create a new SparkContext, at least the master and app name should
    be set, either through the named parameters here or through `conf`.

    Parameters
    ----------
    master : str, optional
        Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
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
    batchSize : int, optional
        The number of Python objects represented as a single
        Java object. Set 1 to disable batching, 0 to automatically choose
        the batch size based on object sizes, or -1 to use an unlimited
        batch size
    serializer : :class:`pyspark.serializers.Serializer`, optional
        The serializer for RDDs.
    conf : :py:class:`pyspark.SparkConf`, optional
        An object setting Spark properties.
    gateway : :py:class:`py4j.java_gateway.JavaGateway`,  optional
        Use an existing gateway and JVM, otherwise a new JVM
        will be instantiated. This is only used internally.
    jsc : :py:class:`py4j.java_gateway.JavaObject`, optional
        The JavaSparkContext instance. This is only used internally.
    profiler_cls : type, optional
        A class of custom Profiler used to do profiling
        (default is :class:`pyspark.profiler.BasicProfiler`).

    Notes
    -----
    Only one :class:`SparkContext` should be active per JVM. You must `stop()`
    the active :class:`SparkContext` before creating a new one.

    :class:`SparkContext` instance is not supported to share across multiple
    processes out of the box, and PySpark does not guarantee multi-processing execution.
    Use threads instead for concurrent processing purpose.

    Examples
    --------
    >>> from pyspark.context import SparkContext
    >>> sc = SparkContext('local', 'test')
    >>> sc2 = SparkContext('local', 'test2') # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: ...
    """

    _gateway = None
    _jvm = None
    _next_accum_id = 0
    _active_spark_context = None
    _lock = RLock()
    _python_includes = None  # zip and egg files that need to be added to PYTHONPATH

    PACKAGE_EXTENSIONS = (".zip", ".egg", ".jar")

    def __init__(
        self,
        master=None,
        appName=None,
        sparkHome=None,
        pyFiles=None,
        environment=None,
        batchSize=0,
        serializer=CPickleSerializer(),
        conf=None,
        gateway=None,
        jsc=None,
        profiler_cls=BasicProfiler,
    ):
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
            )
        except:
            # If an error occurs, clean up in order to allow future SparkContext creation:
            self.stop()
            raise

    def _do_init(
        self,
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
    ):
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
            raise RuntimeError("A master URL must be set in your configuration")
        if not self._conf.contains("spark.app.name"):
            raise RuntimeError("An application name must be set in your configuration")

        # Read back our properties from the conf in case we loaded some of them from
        # the classpath or an external config file
        self.master = self._conf.get("spark.master")
        self.appName = self._conf.get("spark.app.name")
        self.sparkHome = self._conf.get("spark.home", None)

        for (k, v) in self._conf.getAll():
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
        auth_token = self._gateway.gateway_parameters.auth_token
        self._accumulatorServer = accumulators._start_update_server(auth_token)
        (host, port) = self._accumulatorServer.server_address
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

        if sys.version_info[:2] < (3, 7):
            with warnings.catch_warnings():
                warnings.simplefilter("once")
                warnings.warn("Python 3.6 support is deprecated in Spark 3.2.", FutureWarning)

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
        local_dir = self._jvm.org.apache.spark.util.Utils.getLocalDir(self._jsc.sc().conf())
        self._temp_dir = self._jvm.org.apache.spark.util.Utils.createTempDir(
            local_dir, "pyspark"
        ).getAbsolutePath()

        # profiling stats collected for each PythonRDD
        if self._conf.get("spark.python.profile", "false") == "true":
            dump_path = self._conf.get("spark.python.profile.dump", None)
            self.profiler_collector = ProfilerCollector(profiler_cls, dump_path)
        else:
            self.profiler_collector = None

        # create a signal handler which would be invoked on receiving SIGINT
        def signal_handler(signal, frame):
            self.cancelAllJobs()
            raise KeyboardInterrupt()

        # see http://stackoverflow.com/questions/23206787/
        if isinstance(threading.current_thread(), threading._MainThread):
            signal.signal(signal.SIGINT, signal_handler)

    def __repr__(self):
        return "<SparkContext master={master} appName={appName}>".format(
            master=self.master,
            appName=self.appName,
        )

    def _repr_html_(self):
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

    def _initialize_context(self, jconf):
        """
        Initialize SparkContext in function to allow subclass specific initialization
        """
        return self._jvm.JavaSparkContext(jconf)

    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None, conf=None):
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

    def __getnewargs__(self):
        # This method is called when attempting to pickle SparkContext, which is always an error:
        raise RuntimeError(
            "It appears that you are attempting to reference SparkContext from a broadcast "
            "variable, action, or transformation. SparkContext can only be used on the driver, "
            "not in code that it run on workers. For more information, see SPARK-5063."
        )

    def __enter__(self):
        """
        Enable 'with SparkContext(...) as sc: app(sc)' syntax.
        """
        return self

    def __exit__(self, type, value, trace):
        """
        Enable 'with SparkContext(...) as sc: app' syntax.

        Specifically stop the context on exit of the with block.
        """
        self.stop()

    @classmethod
    def getOrCreate(cls, conf=None):
        """
        Get or instantiate a SparkContext and register it as a singleton object.

        Parameters
        ----------
        conf : :py:class:`pyspark.SparkConf`, optional
        """
        with SparkContext._lock:
            if SparkContext._active_spark_context is None:
                SparkContext(conf=conf or SparkConf())
            return SparkContext._active_spark_context

    def setLogLevel(self, logLevel):
        """
        Control our logLevel. This overrides any user-defined log settings.
        Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
        """
        self._jsc.setLogLevel(logLevel)

    @classmethod
    def setSystemProperty(cls, key, value):
        """
        Set a Java system property, such as spark.executor.memory. This must
        must be invoked before instantiating SparkContext.
        """
        SparkContext._ensure_initialized()
        SparkContext._jvm.java.lang.System.setProperty(key, value)

    @property
    def version(self):
        """
        The version of Spark on which this application is running.
        """
        return self._jsc.version()

    @property
    def applicationId(self):
        """
        A unique identifier for the Spark application.
        Its format depends on the scheduler implementation.

        * in case of local spark app something like 'local-1433865536131'
        * in case of YARN something like 'application_1433865536131_34483'

        Examples
        --------
        >>> sc.applicationId  # doctest: +ELLIPSIS
        'local-...'
        """
        return self._jsc.sc().applicationId()

    @property
    def uiWebUrl(self):
        """Return the URL of the SparkUI instance started by this SparkContext"""
        return self._jsc.sc().uiWebUrl().get()

    @property
    def startTime(self):
        """Return the epoch time when the Spark Context was started."""
        return self._jsc.startTime()

    @property
    def defaultParallelism(self):
        """
        Default level of parallelism to use when not given by user (e.g. for
        reduce tasks)
        """
        return self._jsc.sc().defaultParallelism()

    @property
    def defaultMinPartitions(self):
        """
        Default min number of partitions for Hadoop RDDs when not given by user
        """
        return self._jsc.sc().defaultMinPartitions()

    def stop(self):
        """
        Shut down the SparkContext.
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
            self._accumulatorServer = None
        with SparkContext._lock:
            SparkContext._active_spark_context = None

    def emptyRDD(self):
        """
        Create an RDD that has no partitions or elements.
        """
        return RDD(self._jsc.emptyRDD(), self, NoOpSerializer())

    def range(self, start, end=None, step=1, numSlices=None):
        """
        Create a new RDD of int containing elements from `start` to `end`
        (exclusive), increased by `step` every element. Can be called the same
        way as python's built-in range() function. If called with a single argument,
        the argument is interpreted as `end`, and `start` is set to 0.

        Parameters
        ----------
        start : int
            the start value
        end : int, optional
            the end value (exclusive)
        step : int, optional
            the incremental step (default: 1)
        numSlices : int, optional
            the number of partitions of the new RDD

        Returns
        -------
        :py:class:`pyspark.RDD`
            An RDD of int

        Examples
        --------
        >>> sc.range(5).collect()
        [0, 1, 2, 3, 4]
        >>> sc.range(2, 4).collect()
        [2, 3]
        >>> sc.range(1, 7, 2).collect()
        [1, 3, 5]
        """
        if end is None:
            end = start
            start = 0

        return self.parallelize(range(start, end, step), numSlices)

    def parallelize(self, c, numSlices=None):
        """
        Distribute a local Python collection to form an RDD. Using range
        is recommended if the input represents a range for performance.

        Examples
        --------
        >>> sc.parallelize([0, 2, 3, 4, 6], 5).glom().collect()
        [[0], [2], [3], [4], [6]]
        >>> sc.parallelize(range(0, 6, 2), 5).glom().collect()
        [[], [0], [], [2], [4]]
        """
        numSlices = int(numSlices) if numSlices is not None else self.defaultParallelism
        if isinstance(c, range):
            size = len(c)
            if size == 0:
                return self.parallelize([], numSlices)
            step = c[1] - c[0] if size > 1 else 1
            start0 = c[0]

            def getStart(split):
                return start0 + int((split * size / numSlices)) * step

            def f(split, iterator):
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
        batchSize = max(1, min(len(c) // numSlices, self._batchSize or 1024))
        serializer = BatchedSerializer(self._unbatched_serializer, batchSize)

        def reader_func(temp_filename):
            return self._jvm.PythonRDD.readRDDFromFile(self._jsc, temp_filename, numSlices)

        def createRDDServer():
            return self._jvm.PythonParallelizeServer(self._jsc.sc(), numSlices)

        jrdd = self._serialize_to_jvm(c, serializer, reader_func, createRDDServer)
        return RDD(jrdd, self, serializer)

    def _serialize_to_jvm(self, data, serializer, reader_func, createRDDServer):
        """
        Using py4j to send a large dataset to the jvm is really slow, so we use either a file
        or a socket if we have encryption enabled.

        Examples
        --------
        data
            object to be serialized
        serializer : :py:class:`pyspark.serializers.Serializer`
        reader_func : function
            A function which takes a filename and reads in the data in the jvm and
            returns a JavaRDD. Only used when encryption is disabled.
        createRDDServer : function
            A function which creates a PythonRDDServer in the jvm to
            accept the serialized data, for use when encryption is enabled.
        """
        if self._encryption_enabled:
            # with encryption, we open a server in java and send the data directly
            server = createRDDServer()
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

    def pickleFile(self, name, minPartitions=None):
        """
        Load an RDD previously saved using :meth:`RDD.saveAsPickleFile` method.

        Examples
        --------
        >>> tmpFile = NamedTemporaryFile(delete=True)
        >>> tmpFile.close()
        >>> sc.parallelize(range(10)).saveAsPickleFile(tmpFile.name, 5)
        >>> sorted(sc.pickleFile(tmpFile.name, 3).collect())
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(self._jsc.objectFile(name, minPartitions), self)

    def textFile(self, name, minPartitions=None, use_unicode=True):
        """
        Read a text file from HDFS, a local file system (available on all
        nodes), or any Hadoop-supported file system URI, and return it as an
        RDD of Strings.
        The text files must be encoded as UTF-8.

        If use_unicode is False, the strings will be kept as `str` (encoding
        as `utf-8`), which is faster and smaller than unicode. (Added in
        Spark 1.2)

        Examples
        --------
        >>> path = os.path.join(tempdir, "sample-text.txt")
        >>> with open(path, "w") as testFile:
        ...    _ = testFile.write("Hello world!")
        >>> textFile = sc.textFile(path)
        >>> textFile.collect()
        ['Hello world!']
        """
        minPartitions = minPartitions or min(self.defaultParallelism, 2)
        return RDD(self._jsc.textFile(name, minPartitions), self, UTF8Deserializer(use_unicode))

    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        """
        Read a directory of text files from HDFS, a local file system
        (available on all nodes), or any  Hadoop-supported file system
        URI. Each file is read as a single record and returned in a
        key-value pair, where the key is the path of each file, the
        value is the content of each file.
        The text files must be encoded as UTF-8.

        If `use_unicode` is False, the strings will be kept as `str` (encoding
        as `utf-8`), which is faster and smaller than unicode. (Added in
        Spark 1.2)

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

        Notes
        -----
        Small files are preferred, as each file will be loaded fully in memory.

        Examples
        --------
        >>> dirPath = os.path.join(tempdir, "files")
        >>> os.mkdir(dirPath)
        >>> with open(os.path.join(dirPath, "1.txt"), "w") as file1:
        ...    _ = file1.write("1")
        >>> with open(os.path.join(dirPath, "2.txt"), "w") as file2:
        ...    _ = file2.write("2")
        >>> textFiles = sc.wholeTextFiles(dirPath)
        >>> sorted(textFiles.collect())
        [('.../1.txt', '1'), ('.../2.txt', '2')]
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(
            self._jsc.wholeTextFiles(path, minPartitions),
            self,
            PairDeserializer(UTF8Deserializer(use_unicode), UTF8Deserializer(use_unicode)),
        )

    def binaryFiles(self, path, minPartitions=None):
        """
        Read a directory of binary files from HDFS, a local file system
        (available on all nodes), or any Hadoop-supported file system URI
        as a byte array. Each file is read as a single record and returned
        in a key-value pair, where the key is the path of each file, the
        value is the content of each file.

        Notes
        -----
        Small files are preferred, large file is also allowable, but may cause bad performance.
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(
            self._jsc.binaryFiles(path, minPartitions),
            self,
            PairDeserializer(UTF8Deserializer(), NoOpSerializer()),
        )

    def binaryRecords(self, path, recordLength):
        """
        Load data from a flat binary file, assuming each record is a set of numbers
        with the specified numerical format (see ByteBuffer), and the number of
        bytes per record is constant.

        Parameters
        ----------
        path : str
            Directory to the input data files
        recordLength : int
            The length at which to split the records
        """
        return RDD(self._jsc.binaryRecords(path, recordLength), self, NoOpSerializer())

    def _dictToJavaMap(self, d):
        jm = self._jvm.java.util.HashMap()
        if not d:
            d = {}
        for k, v in d.items():
            jm[k] = v
        return jm

    def sequenceFile(
        self,
        path,
        keyClass=None,
        valueClass=None,
        keyConverter=None,
        valueConverter=None,
        minSplits=None,
        batchSize=0,
    ):
        """
        Read a Hadoop SequenceFile with arbitrary key and value Writable class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is as follows:

            1. A Java RDD is created from the SequenceFile or other InputFormat, and the key
               and value Writable classes
            2. Serialization is attempted via Pickle pickling
            3. If this fails, the fallback is to call 'toString' on each key and value
            4. :class:`CPickleSerializer` is used to deserialize pickled objects on the Python side

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
        batchSize : int, optional
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)
        """
        minSplits = minSplits or min(self.defaultParallelism, 2)
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
        path,
        inputFormatClass,
        keyClass,
        valueClass,
        keyConverter=None,
        valueConverter=None,
        conf=None,
        batchSize=0,
    ):
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for :py:meth:`SparkContext.sequenceFile`.

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java

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
        batchSize : int, optional
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
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
        inputFormatClass,
        keyClass,
        valueClass,
        keyConverter=None,
        valueConverter=None,
        conf=None,
        batchSize=0,
    ):
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for :py:meth:`SparkContext.sequenceFile`.

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
        batchSize : int, optional
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
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
        path,
        inputFormatClass,
        keyClass,
        valueClass,
        keyConverter=None,
        valueConverter=None,
        conf=None,
        batchSize=0,
    ):
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for :py:meth:`SparkContext.sequenceFile`.

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java.

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
            (None by default)
        valueConverter : str, optional
            fully qualified name of a function returning value WritableConverter
            (None by default)
        conf : dict, optional
            Hadoop configuration, passed in as a dict (None by default)
        batchSize : int, optional
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
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
        inputFormatClass,
        keyClass,
        valueClass,
        keyConverter=None,
        valueConverter=None,
        conf=None,
        batchSize=0,
    ):
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for :py:meth:`SparkContext.sequenceFile`.

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
        batchSize : int, optional
            The number of Python objects represented as a single
            Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
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

    def _checkpointFile(self, name, input_deserializer):
        jrdd = self._jsc.checkpointFile(name)
        return RDD(jrdd, self, input_deserializer)

    def union(self, rdds):
        """
        Build the union of a list of RDDs.

        This supports unions() of RDDs with different serialized formats,
        although this forces them to be reserialized using the default
        serializer:

        Examples
        --------
        >>> path = os.path.join(tempdir, "union-text.txt")
        >>> with open(path, "w") as testFile:
        ...    _ = testFile.write("Hello")
        >>> textFile = sc.textFile(path)
        >>> textFile.collect()
        ['Hello']
        >>> parallelized = sc.parallelize(["World!"])
        >>> sorted(sc.union([textFile, parallelized]).collect())
        ['Hello', 'World!']
        """
        first_jrdd_deserializer = rdds[0]._jrdd_deserializer
        if any(x._jrdd_deserializer != first_jrdd_deserializer for x in rdds):
            rdds = [x._reserialize() for x in rdds]
        gw = SparkContext._gateway
        jvm = SparkContext._jvm
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

    def broadcast(self, value):
        """
        Broadcast a read-only variable to the cluster, returning a :class:`Broadcast`
        object for reading it in distributed functions. The variable will
        be sent to each cluster only once.
        """
        return Broadcast(self, value, self._pickled_broadcast_vars)

    def accumulator(self, value, accum_param=None):
        """
        Create an :class:`Accumulator` with the given initial value, using a given
        :class:`AccumulatorParam` helper object to define how to add values of the
        data type if provided. Default AccumulatorParams are used for integers
        and floating-point numbers if you do not provide one. For other types,
        a custom AccumulatorParam can be used.
        """
        if accum_param is None:
            if isinstance(value, int):
                accum_param = accumulators.INT_ACCUMULATOR_PARAM
            elif isinstance(value, float):
                accum_param = accumulators.FLOAT_ACCUMULATOR_PARAM
            elif isinstance(value, complex):
                accum_param = accumulators.COMPLEX_ACCUMULATOR_PARAM
            else:
                raise TypeError("No default accumulator param for type %s" % type(value))
        SparkContext._next_accum_id += 1
        return Accumulator(SparkContext._next_accum_id - 1, value, accum_param)

    def addFile(self, path, recursive=False):
        """
        Add a file to be downloaded with this Spark job on every node.
        The `path` passed can be either a local file, a file in HDFS
        (or other Hadoop-supported filesystems), or an HTTP, HTTPS or
        FTP URI.

        To access the file in Spark jobs, use :meth:`SparkFiles.get` with the
        filename to find its download location.

        A directory can be given if the recursive option is set to True.
        Currently directories are only supported for Hadoop-supported filesystems.

        Notes
        -----
        A path can be added only once. Subsequent additions of the same path are ignored.

        Examples
        --------
        >>> from pyspark import SparkFiles
        >>> path = os.path.join(tempdir, "test.txt")
        >>> with open(path, "w") as testFile:
        ...    _ = testFile.write("100")
        >>> sc.addFile(path)
        >>> def func(iterator):
        ...    with open(SparkFiles.get("test.txt")) as testFile:
        ...        fileVal = int(testFile.readline())
        ...        return [x * fileVal for x in iterator]
        >>> sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()
        [100, 200, 300, 400]
        """
        self._jsc.sc().addFile(path, recursive)

    def addPyFile(self, path):
        """
        Add a .py or .zip dependency for all tasks to be executed on this
        SparkContext in the future.  The `path` passed can be either a local
        file, a file in HDFS (or other Hadoop-supported filesystems), or an
        HTTP, HTTPS or FTP URI.

        Notes
        -----
        A path can be added only once. Subsequent additions of the same path are ignored.
        """
        self.addFile(path)
        (dirname, filename) = os.path.split(path)  # dirname may be directory or HDFS/S3 prefix
        if filename[-4:].lower() in self.PACKAGE_EXTENSIONS:
            self._python_includes.append(filename)
            # for tests in local mode
            sys.path.insert(1, os.path.join(SparkFiles.getRootDirectory(), filename))

        importlib.invalidate_caches()

    def setCheckpointDir(self, dirName):
        """
        Set the directory under which RDDs are going to be checkpointed. The
        directory must be an HDFS path if running on a cluster.
        """
        self._jsc.sc().setCheckpointDir(dirName)

    @since(3.1)
    def getCheckpointDir(self):
        """
        Return the directory where RDDs are checkpointed. Returns None if no
        checkpoint directory has been set.
        """
        if not self._jsc.sc().getCheckpointDir().isEmpty():
            return self._jsc.sc().getCheckpointDir().get()
        return None

    def _getJavaStorageLevel(self, storageLevel):
        """
        Returns a Java StorageLevel based on a pyspark.StorageLevel.
        """
        if not isinstance(storageLevel, StorageLevel):
            raise TypeError("storageLevel must be of type pyspark.StorageLevel")

        newStorageLevel = self._jvm.org.apache.spark.storage.StorageLevel
        return newStorageLevel(
            storageLevel.useDisk,
            storageLevel.useMemory,
            storageLevel.useOffHeap,
            storageLevel.deserialized,
            storageLevel.replication,
        )

    def setJobGroup(self, groupId, description, interruptOnCancel=False):
        """
        Assigns a group ID to all the jobs started by this thread until the group ID is set to a
        different value or cleared.

        Often, a unit of execution in an application consists of multiple Spark actions or jobs.
        Application programmers can use this method to group all those jobs together and give a
        group description. Once set, the Spark web UI will associate such jobs with this group.

        The application can use :meth:`SparkContext.cancelJobGroup` to cancel all
        running jobs in this group.

        Notes
        -----
        If interruptOnCancel is set to true for the job group, then job cancellation will result
        in Thread.interrupt() being called on the job's executor threads. This is useful to help
        ensure that the tasks are actually stopped in a timely manner, but is off by default due
        to HDFS-1208, where HDFS may respond to Thread.interrupt() by marking nodes as dead.

        If you run jobs in parallel, use :class:`pyspark.InheritableThread` for thread
        local inheritance, and preventing resource leak.

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
        >>> def start_job(x):
        ...     global result
        ...     try:
        ...         sc.setJobGroup("job_to_cancel", "some description")
        ...         result = sc.parallelize(range(x)).map(map_func).collect()
        ...     except Exception as e:
        ...         result = "Cancelled"
        ...     lock.release()
        >>> def stop_job():
        ...     sleep(5)
        ...     sc.cancelJobGroup("job_to_cancel")
        >>> suppress = lock.acquire()
        >>> suppress = InheritableThread(target=start_job, args=(10,)).start()
        >>> suppress = InheritableThread(target=stop_job).start()
        >>> suppress = lock.acquire()
        >>> print(result)
        Cancelled
        """
        self._jsc.setJobGroup(groupId, description, interruptOnCancel)

    def setLocalProperty(self, key, value):
        """
        Set a local property that affects jobs submitted from this thread, such as the
        Spark fair scheduler pool.

        Notes
        -----
        If you run jobs in parallel, use :class:`pyspark.InheritableThread` for thread
        local inheritance, and preventing resource leak.
        """
        self._jsc.setLocalProperty(key, value)

    def getLocalProperty(self, key):
        """
        Get a local property set in this thread, or null if it is missing. See
        :meth:`setLocalProperty`.
        """
        return self._jsc.getLocalProperty(key)

    def setJobDescription(self, value):
        """
        Set a human readable description of the current job.

        Notes
        -----
        If you run jobs in parallel, use :class:`pyspark.InheritableThread` for thread
        local inheritance, and preventing resource leak.
        """
        self._jsc.setJobDescription(value)

    def sparkUser(self):
        """
        Get SPARK_USER for user who is running SparkContext.
        """
        return self._jsc.sc().sparkUser()

    def cancelJobGroup(self, groupId):
        """
        Cancel active jobs for the specified group. See :meth:`SparkContext.setJobGroup`.
        for more information.
        """
        self._jsc.sc().cancelJobGroup(groupId)

    def cancelAllJobs(self):
        """
        Cancel all jobs that have been scheduled or are running.
        """
        self._jsc.sc().cancelAllJobs()

    def statusTracker(self):
        """
        Return :class:`StatusTracker` object
        """
        return StatusTracker(self._jsc.statusTracker())

    def runJob(self, rdd, partitionFunc, partitions=None, allowLocal=False):
        """
        Executes the given partitionFunc on the specified set of partitions,
        returning the result as an array of elements.

        If 'partitions' is not specified, this will run over all partitions.

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
            partitions = range(rdd._jrdd.partitions().size())

        # Implementation note: This is implemented as a mapPartitions followed
        # by runJob() in order to avoid having to pass a Python lambda into
        # SparkContext#runJob.
        mappedRDD = rdd.mapPartitions(partitionFunc)
        sock_info = self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, partitions)
        return list(_load_from_socket(sock_info, mappedRDD._jrdd_deserializer))

    def show_profiles(self):
        """Print the profile stats to stdout"""
        if self.profiler_collector is not None:
            self.profiler_collector.show_profiles()
        else:
            raise RuntimeError(
                "'spark.python.profile' configuration must be set "
                "to 'true' to enable Python profile."
            )

    def dump_profiles(self, path):
        """Dump the profile stats into directory `path`"""
        if self.profiler_collector is not None:
            self.profiler_collector.dump_profiles(path)
        else:
            raise RuntimeError(
                "'spark.python.profile' configuration must be set "
                "to 'true' to enable Python profile."
            )

    def getConf(self):
        conf = SparkConf()
        conf.setAll(self._conf.getAll())
        return conf

    @property
    def resources(self):
        resources = {}
        jresources = self._jsc.resources()
        for x in jresources:
            name = jresources[x].name()
            jaddresses = jresources[x].addresses()
            addrs = [addr for addr in jaddresses]
            resources[name] = ResourceInformation(name, addrs)
        return resources

    @staticmethod
    def _assert_on_driver():
        """
        Called to ensure that SparkContext is created only on the Driver.

        Throws an exception if a SparkContext is about to be created in executors.
        """
        if TaskContext.get() is not None:
            raise RuntimeError("SparkContext should only be created and accessed on the driver.")


def _test():
    import atexit
    import doctest
    import tempfile

    globs = globals().copy()
    globs["sc"] = SparkContext("local[4]", "PythonTest")
    globs["tempdir"] = tempfile.mkdtemp()
    atexit.register(lambda: shutil.rmtree(globs["tempdir"]))
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
