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
import sys
from threading import Lock
from tempfile import NamedTemporaryFile

from pyspark import accumulators
from pyspark.accumulators import Accumulator
from pyspark.broadcast import Broadcast
from pyspark.conf import SparkConf
from pyspark.files import SparkFiles
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer, \
    PairDeserializer, AutoBatchedSerializer, NoOpSerializer
from pyspark.storagelevel import StorageLevel
from pyspark.rdd import RDD
from pyspark.traceback_utils import CallSite, first_spark_call
from pyspark.profiler import ProfilerCollector, BasicProfiler

from py4j.java_collections import ListConverter


__all__ = ['SparkContext']


# These are special default configs for PySpark, they will overwrite
# the default ones for Spark if they are not configured by user.
DEFAULT_CONFIGS = {
    "spark.serializer.objectStreamReset": 100,
    "spark.rdd.compress": True,
}


class SparkContext(object):

    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create L{RDD} and
    broadcast variables on that cluster.
    """

    _gateway = None
    _jvm = None
    _writeToFile = None
    _next_accum_id = 0
    _active_spark_context = None
    _lock = Lock()
    _python_includes = None  # zip and egg files that need to be added to PYTHONPATH

    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None,
                 environment=None, batchSize=0, serializer=PickleSerializer(), conf=None,
                 gateway=None, jsc=None, profiler_cls=BasicProfiler):
        """
        Create a new SparkContext. At least the master and app name should be set,
        either through the named parameters here or through C{conf}.

        :param master: Cluster URL to connect to
               (e.g. mesos://host:port, spark://host:port, local[4]).
        :param appName: A name for your job, to display on the cluster web UI.
        :param sparkHome: Location where Spark is installed on cluster nodes.
        :param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
        :param environment: A dictionary of environment variables to set on
               worker nodes.
        :param batchSize: The number of Python objects represented as a single
               Java object. Set 1 to disable batching, 0 to automatically choose
               the batch size based on object sizes, or -1 to use an unlimited
               batch size
        :param serializer: The serializer for RDDs.
        :param conf: A L{SparkConf} object setting Spark properties.
        :param gateway: Use an existing gateway and JVM, otherwise a new JVM
               will be instantiated.
        :param jsc: The JavaSparkContext instance (optional).
        :param profiler_cls: A class of custom Profiler used to do profiling
               (default is pyspark.profiler.BasicProfiler).


        >>> from pyspark.context import SparkContext
        >>> sc = SparkContext('local', 'test')

        >>> sc2 = SparkContext('local', 'test2') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError:...
        """
        self._callsite = first_spark_call() or CallSite(None, None, None)
        SparkContext._ensure_initialized(self, gateway=gateway)
        try:
            self._do_init(master, appName, sparkHome, pyFiles, environment, batchSize, serializer,
                          conf, jsc, profiler_cls)
        except:
            # If an error occurs, clean up in order to allow future SparkContext creation:
            self.stop()
            raise

    def _do_init(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer,
                 conf, jsc, profiler_cls):
        self.environment = environment or {}
        self._conf = conf or SparkConf(_jvm=self._jvm)
        self._batchSize = batchSize  # -1 represents an unlimited batch size
        self._unbatched_serializer = serializer
        if batchSize == 0:
            self.serializer = AutoBatchedSerializer(self._unbatched_serializer)
        else:
            self.serializer = BatchedSerializer(self._unbatched_serializer,
                                                batchSize)

        # Set any parameters passed directly to us on the conf
        if master:
            self._conf.setMaster(master)
        if appName:
            self._conf.setAppName(appName)
        if sparkHome:
            self._conf.setSparkHome(sparkHome)
        if environment:
            for key, value in environment.iteritems():
                self._conf.setExecutorEnv(key, value)
        for key, value in DEFAULT_CONFIGS.items():
            self._conf.setIfMissing(key, value)

        # Check that we have at least the required parameters
        if not self._conf.contains("spark.master"):
            raise Exception("A master URL must be set in your configuration")
        if not self._conf.contains("spark.app.name"):
            raise Exception("An application name must be set in your configuration")

        # Read back our properties from the conf in case we loaded some of them from
        # the classpath or an external config file
        self.master = self._conf.get("spark.master")
        self.appName = self._conf.get("spark.app.name")
        self.sparkHome = self._conf.get("spark.home", None)
        for (k, v) in self._conf.getAll():
            if k.startswith("spark.executorEnv."):
                varName = k[len("spark.executorEnv."):]
                self.environment[varName] = v

        # Create the Java SparkContext through Py4J
        self._jsc = jsc or self._initialize_context(self._conf._jconf)

        # Create a single Accumulator in Java that we'll send all our updates through;
        # they will be passed back to us through a TCP server
        self._accumulatorServer = accumulators._start_update_server()
        (host, port) = self._accumulatorServer.server_address
        self._javaAccumulator = self._jsc.accumulator(
            self._jvm.java.util.ArrayList(),
            self._jvm.PythonAccumulatorParam(host, port))

        self.pythonExec = os.environ.get("PYSPARK_PYTHON", 'python')

        # Broadcast's __reduce__ method stores Broadcast instances here.
        # This allows other code to determine which Broadcast instances have
        # been pickled, so it can determine which Java broadcast objects to
        # send.
        self._pickled_broadcast_vars = set()

        SparkFiles._sc = self
        root_dir = SparkFiles.getRootDirectory()
        sys.path.insert(1, root_dir)

        # Deploy any code dependencies specified in the constructor
        self._python_includes = list()
        for path in (pyFiles or []):
            self.addPyFile(path)

        # Deploy code dependencies set by spark-submit; these will already have been added
        # with SparkContext.addFile, so we just need to add them to the PYTHONPATH
        for path in self._conf.get("spark.submit.pyFiles", "").split(","):
            if path != "":
                (dirname, filename) = os.path.split(path)
                if filename.lower().endswith("zip") or filename.lower().endswith("egg"):
                    self._python_includes.append(filename)
                    sys.path.insert(1, os.path.join(SparkFiles.getRootDirectory(), filename))

        # Create a temporary directory inside spark.local.dir:
        local_dir = self._jvm.org.apache.spark.util.Utils.getLocalDir(self._jsc.sc().conf())
        self._temp_dir = \
            self._jvm.org.apache.spark.util.Utils.createTempDir(local_dir, "pyspark") \
                .getAbsolutePath()

        # profiling stats collected for each PythonRDD
        if self._conf.get("spark.python.profile", "false") == "true":
            dump_path = self._conf.get("spark.python.profile.dump", None)
            self.profiler_collector = ProfilerCollector(profiler_cls, dump_path)
        else:
            self.profiler_collector = None

    def _initialize_context(self, jconf):
        """
        Initialize SparkContext in function to allow subclass specific initialization
        """
        return self._jvm.JavaSparkContext(jconf)

    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None):
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
        """
        with SparkContext._lock:
            if not SparkContext._gateway:
                SparkContext._gateway = gateway or launch_gateway()
                SparkContext._jvm = SparkContext._gateway.jvm
                SparkContext._writeToFile = SparkContext._jvm.PythonRDD.writeToFile

            if instance:
                if (SparkContext._active_spark_context and
                        SparkContext._active_spark_context != instance):
                    currentMaster = SparkContext._active_spark_context.master
                    currentAppName = SparkContext._active_spark_context.appName
                    callsite = SparkContext._active_spark_context._callsite

                    # Raise error if there is already a running Spark context
                    raise ValueError(
                        "Cannot run multiple SparkContexts at once; "
                        "existing SparkContext(app=%s, master=%s)"
                        " created by %s at %s:%s "
                        % (currentAppName, currentMaster,
                            callsite.function, callsite.file, callsite.linenum))
                else:
                    SparkContext._active_spark_context = instance

    def __getnewargs__(self):
        # This method is called when attempting to pickle SparkContext, which is always an error:
        raise Exception(
            "It appears that you are attempting to reference SparkContext from a broadcast "
            "variable, action, or transforamtion. SparkContext can only be used on the driver, "
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
            self._jsc.stop()
            self._jsc = None
        if getattr(self, "_accumulatorServer", None):
            self._accumulatorServer.shutdown()
            self._accumulatorServer = None
        with SparkContext._lock:
            SparkContext._active_spark_context = None

    def parallelize(self, c, numSlices=None):
        """
        Distribute a local Python collection to form an RDD. Using xrange
        is recommended if the input represents a range for performance.

        >>> sc.parallelize([0, 2, 3, 4, 6], 5).glom().collect()
        [[0], [2], [3], [4], [6]]
        >>> sc.parallelize(xrange(0, 6, 2), 5).glom().collect()
        [[], [0], [], [2], [4]]
        """
        numSlices = int(numSlices) if numSlices is not None else self.defaultParallelism
        if isinstance(c, xrange):
            size = len(c)
            if size == 0:
                return self.parallelize([], numSlices)
            step = c[1] - c[0] if size > 1 else 1
            start0 = c[0]

            def getStart(split):
                return start0 + (split * size / numSlices) * step

            def f(split, iterator):
                return xrange(getStart(split), getStart(split + 1), step)

            return self.parallelize([], numSlices).mapPartitionsWithIndex(f)
        # Calling the Java parallelize() method with an ArrayList is too slow,
        # because it sends O(n) Py4J commands.  As an alternative, serialized
        # objects are written to a file and loaded through textFile().
        tempFile = NamedTemporaryFile(delete=False, dir=self._temp_dir)
        # Make sure we distribute data evenly if it's smaller than self.batchSize
        if "__len__" not in dir(c):
            c = list(c)    # Make it a list so we can compute its length
        batchSize = max(1, min(len(c) // numSlices, self._batchSize or 1024))
        serializer = BatchedSerializer(self._unbatched_serializer, batchSize)
        serializer.dump_stream(c, tempFile)
        tempFile.close()
        readRDDFromFile = self._jvm.PythonRDD.readRDDFromFile
        jrdd = readRDDFromFile(self._jsc, tempFile.name, numSlices)
        return RDD(jrdd, self, serializer)

    def pickleFile(self, name, minPartitions=None):
        """
        Load an RDD previously saved using L{RDD.saveAsPickleFile} method.

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

        If use_unicode is False, the strings will be kept as `str` (encoding
        as `utf-8`), which is faster and smaller than unicode. (Added in
        Spark 1.2)

        >>> path = os.path.join(tempdir, "sample-text.txt")
        >>> with open(path, "w") as testFile:
        ...    testFile.write("Hello world!")
        >>> textFile = sc.textFile(path)
        >>> textFile.collect()
        [u'Hello world!']
        """
        minPartitions = minPartitions or min(self.defaultParallelism, 2)
        return RDD(self._jsc.textFile(name, minPartitions), self,
                   UTF8Deserializer(use_unicode))

    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        """
        Read a directory of text files from HDFS, a local file system
        (available on all nodes), or any  Hadoop-supported file system
        URI. Each file is read as a single record and returned in a
        key-value pair, where the key is the path of each file, the
        value is the content of each file.

        If use_unicode is False, the strings will be kept as `str` (encoding
        as `utf-8`), which is faster and smaller than unicode. (Added in
        Spark 1.2)

        For example, if you have the following files::

          hdfs://a-hdfs-path/part-00000
          hdfs://a-hdfs-path/part-00001
          ...
          hdfs://a-hdfs-path/part-nnnnn

        Do C{rdd = sparkContext.wholeTextFiles("hdfs://a-hdfs-path")},
        then C{rdd} contains::

          (a-hdfs-path/part-00000, its content)
          (a-hdfs-path/part-00001, its content)
          ...
          (a-hdfs-path/part-nnnnn, its content)

        NOTE: Small files are preferred, as each file will be loaded
        fully in memory.

        >>> dirPath = os.path.join(tempdir, "files")
        >>> os.mkdir(dirPath)
        >>> with open(os.path.join(dirPath, "1.txt"), "w") as file1:
        ...    file1.write("1")
        >>> with open(os.path.join(dirPath, "2.txt"), "w") as file2:
        ...    file2.write("2")
        >>> textFiles = sc.wholeTextFiles(dirPath)
        >>> sorted(textFiles.collect())
        [(u'.../1.txt', u'1'), (u'.../2.txt', u'2')]
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(self._jsc.wholeTextFiles(path, minPartitions), self,
                   PairDeserializer(UTF8Deserializer(use_unicode), UTF8Deserializer(use_unicode)))

    def binaryFiles(self, path, minPartitions=None):
        """
        .. note:: Experimental

        Read a directory of binary files from HDFS, a local file system
        (available on all nodes), or any Hadoop-supported file system URI
        as a byte array. Each file is read as a single record and returned
        in a key-value pair, where the key is the path of each file, the
        value is the content of each file.

        Note: Small files are preferred, large file is also allowable, but
        may cause bad performance.
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(self._jsc.binaryFiles(path, minPartitions), self,
                   PairDeserializer(UTF8Deserializer(), NoOpSerializer()))

    def binaryRecords(self, path, recordLength):
        """
        .. note:: Experimental

        Load data from a flat binary file, assuming each record is a set of numbers
        with the specified numerical format (see ByteBuffer), and the number of
        bytes per record is constant.

        :param path: Directory to the input data files
        :param recordLength: The length at which to split the records
        """
        return RDD(self._jsc.binaryRecords(path, recordLength), self, NoOpSerializer())

    def _dictToJavaMap(self, d):
        jm = self._jvm.java.util.HashMap()
        if not d:
            d = {}
        for k, v in d.iteritems():
            jm[k] = v
        return jm

    def sequenceFile(self, path, keyClass=None, valueClass=None, keyConverter=None,
                     valueConverter=None, minSplits=None, batchSize=0):
        """
        Read a Hadoop SequenceFile with arbitrary key and value Writable class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is as follows:

            1. A Java RDD is created from the SequenceFile or other InputFormat, and the key
               and value Writable classes
            2. Serialization is attempted via Pyrolite pickling
            3. If this fails, the fallback is to call 'toString' on each key and value
            4. C{PickleSerializer} is used to deserialize pickled objects on the Python side

        :param path: path to sequncefile
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter:
        :param valueConverter:
        :param minSplits: minimum splits in dataset
               (default min(2, sc.defaultParallelism))
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        minSplits = minSplits or min(self.defaultParallelism, 2)
        jrdd = self._jvm.PythonRDD.sequenceFile(self._jsc, path, keyClass, valueClass,
                                                keyConverter, valueConverter, minSplits, batchSize)
        return RDD(jrdd, self)

    def newAPIHadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None,
                         valueConverter=None, conf=None, batchSize=0):
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for sc.sequenceFile.

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java

        :param path: path to Hadoop file
        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.newAPIHadoopFile(self._jsc, path, inputFormatClass, keyClass,
                                                    valueClass, keyConverter, valueConverter,
                                                    jconf, batchSize)
        return RDD(jrdd, self)

    def newAPIHadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None,
                        valueConverter=None, conf=None, batchSize=0):
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for sc.sequenceFile.

        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.newAPIHadoopRDD(self._jsc, inputFormatClass, keyClass,
                                                   valueClass, keyConverter, valueConverter,
                                                   jconf, batchSize)
        return RDD(jrdd, self)

    def hadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None,
                   valueConverter=None, conf=None, batchSize=0):
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for sc.sequenceFile.

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java.

        :param path: path to Hadoop file
        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapred.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.hadoopFile(self._jsc, path, inputFormatClass, keyClass,
                                              valueClass, keyConverter, valueConverter,
                                              jconf, batchSize)
        return RDD(jrdd, self)

    def hadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None,
                  valueConverter=None, conf=None, batchSize=0):
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for sc.sequenceFile.

        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapred.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.hadoopRDD(self._jsc, inputFormatClass, keyClass,
                                             valueClass, keyConverter, valueConverter,
                                             jconf, batchSize)
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

        >>> path = os.path.join(tempdir, "union-text.txt")
        >>> with open(path, "w") as testFile:
        ...    testFile.write("Hello")
        >>> textFile = sc.textFile(path)
        >>> textFile.collect()
        [u'Hello']
        >>> parallelized = sc.parallelize(["World!"])
        >>> sorted(sc.union([textFile, parallelized]).collect())
        [u'Hello', 'World!']
        """
        first_jrdd_deserializer = rdds[0]._jrdd_deserializer
        if any(x._jrdd_deserializer != first_jrdd_deserializer for x in rdds):
            rdds = [x._reserialize() for x in rdds]
        first = rdds[0]._jrdd
        rest = [x._jrdd for x in rdds[1:]]
        rest = ListConverter().convert(rest, self._gateway._gateway_client)
        return RDD(self._jsc.union(first, rest), self, rdds[0]._jrdd_deserializer)

    def broadcast(self, value):
        """
        Broadcast a read-only variable to the cluster, returning a
        L{Broadcast<pyspark.broadcast.Broadcast>}
        object for reading it in distributed functions. The variable will
        be sent to each cluster only once.
        """
        return Broadcast(self, value, self._pickled_broadcast_vars)

    def accumulator(self, value, accum_param=None):
        """
        Create an L{Accumulator} with the given initial value, using a given
        L{AccumulatorParam} helper object to define how to add values of the
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
                raise Exception("No default accumulator param for type %s" % type(value))
        SparkContext._next_accum_id += 1
        return Accumulator(SparkContext._next_accum_id - 1, value, accum_param)

    def addFile(self, path):
        """
        Add a file to be downloaded with this Spark job on every node.
        The C{path} passed can be either a local file, a file in HDFS
        (or other Hadoop-supported filesystems), or an HTTP, HTTPS or
        FTP URI.

        To access the file in Spark jobs, use
        L{SparkFiles.get(fileName)<pyspark.files.SparkFiles.get>} with the
        filename to find its download location.

        >>> from pyspark import SparkFiles
        >>> path = os.path.join(tempdir, "test.txt")
        >>> with open(path, "w") as testFile:
        ...    testFile.write("100")
        >>> sc.addFile(path)
        >>> def func(iterator):
        ...    with open(SparkFiles.get("test.txt")) as testFile:
        ...        fileVal = int(testFile.readline())
        ...        return [x * fileVal for x in iterator]
        >>> sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()
        [100, 200, 300, 400]
        """
        self._jsc.sc().addFile(path)

    def clearFiles(self):
        """
        Clear the job's list of files added by L{addFile} or L{addPyFile} so
        that they do not get downloaded to any new nodes.
        """
        # TODO: remove added .py or .zip files from the PYTHONPATH?
        self._jsc.sc().clearFiles()

    def addPyFile(self, path):
        """
        Add a .py or .zip dependency for all tasks to be executed on this
        SparkContext in the future.  The C{path} passed can be either a local
        file, a file in HDFS (or other Hadoop-supported filesystems), or an
        HTTP, HTTPS or FTP URI.
        """
        self.addFile(path)
        (dirname, filename) = os.path.split(path)  # dirname may be directory or HDFS/S3 prefix

        if filename.endswith('.zip') or filename.endswith('.ZIP') or filename.endswith('.egg'):
            self._python_includes.append(filename)
            # for tests in local mode
            sys.path.insert(1, os.path.join(SparkFiles.getRootDirectory(), filename))

    def setCheckpointDir(self, dirName):
        """
        Set the directory under which RDDs are going to be checkpointed. The
        directory must be a HDFS path if running on a cluster.
        """
        self._jsc.sc().setCheckpointDir(dirName)

    def _getJavaStorageLevel(self, storageLevel):
        """
        Returns a Java StorageLevel based on a pyspark.StorageLevel.
        """
        if not isinstance(storageLevel, StorageLevel):
            raise Exception("storageLevel must be of type pyspark.StorageLevel")

        newStorageLevel = self._jvm.org.apache.spark.storage.StorageLevel
        return newStorageLevel(storageLevel.useDisk,
                               storageLevel.useMemory,
                               storageLevel.useOffHeap,
                               storageLevel.deserialized,
                               storageLevel.replication)

    def setJobGroup(self, groupId, description, interruptOnCancel=False):
        """
        Assigns a group ID to all the jobs started by this thread until the group ID is set to a
        different value or cleared.

        Often, a unit of execution in an application consists of multiple Spark actions or jobs.
        Application programmers can use this method to group all those jobs together and give a
        group description. Once set, the Spark web UI will associate such jobs with this group.

        The application can use L{SparkContext.cancelJobGroup} to cancel all
        running jobs in this group.

        >>> import thread, threading
        >>> from time import sleep
        >>> result = "Not Set"
        >>> lock = threading.Lock()
        >>> def map_func(x):
        ...     sleep(100)
        ...     raise Exception("Task should have been cancelled")
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
        >>> supress = lock.acquire()
        >>> supress = thread.start_new_thread(start_job, (10,))
        >>> supress = thread.start_new_thread(stop_job, tuple())
        >>> supress = lock.acquire()
        >>> print result
        Cancelled

        If interruptOnCancel is set to true for the job group, then job cancellation will result
        in Thread.interrupt() being called on the job's executor threads. This is useful to help
        ensure that the tasks are actually stopped in a timely manner, but is off by default due
        to HDFS-1208, where HDFS may respond to Thread.interrupt() by marking nodes as dead.
        """
        self._jsc.setJobGroup(groupId, description, interruptOnCancel)

    def setLocalProperty(self, key, value):
        """
        Set a local property that affects jobs submitted from this thread, such as the
        Spark fair scheduler pool.
        """
        self._jsc.setLocalProperty(key, value)

    def getLocalProperty(self, key):
        """
        Get a local property set in this thread, or null if it is missing. See
        L{setLocalProperty}
        """
        return self._jsc.getLocalProperty(key)

    def sparkUser(self):
        """
        Get SPARK_USER for user who is running SparkContext.
        """
        return self._jsc.sc().sparkUser()

    def cancelJobGroup(self, groupId):
        """
        Cancel active jobs for the specified group. See L{SparkContext.setJobGroup}
        for more information.
        """
        self._jsc.sc().cancelJobGroup(groupId)

    def cancelAllJobs(self):
        """
        Cancel all jobs that have been scheduled or are running.
        """
        self._jsc.sc().cancelAllJobs()

    def runJob(self, rdd, partitionFunc, partitions=None, allowLocal=False):
        """
        Executes the given partitionFunc on the specified set of partitions,
        returning the result as an array of elements.

        If 'partitions' is not specified, this will run over all partitions.

        >>> myRDD = sc.parallelize(range(6), 3)
        >>> sc.runJob(myRDD, lambda part: [x * x for x in part])
        [0, 1, 4, 9, 16, 25]

        >>> myRDD = sc.parallelize(range(6), 3)
        >>> sc.runJob(myRDD, lambda part: [x * x for x in part], [0, 2], True)
        [0, 1, 16, 25]
        """
        if partitions is None:
            partitions = range(rdd._jrdd.partitions().size())
        javaPartitions = ListConverter().convert(partitions, self._gateway._gateway_client)

        # Implementation note: This is implemented as a mapPartitions followed
        # by runJob() in order to avoid having to pass a Python lambda into
        # SparkContext#runJob.
        mappedRDD = rdd.mapPartitions(partitionFunc)
        it = self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, javaPartitions, allowLocal)
        return list(mappedRDD._collect_iterator_through_file(it))

    def show_profiles(self):
        """ Print the profile stats to stdout """
        self.profiler_collector.show_profiles()

    def dump_profiles(self, path):
        """ Dump the profile stats into directory `path`
        """
        self.profiler_collector.dump_profiles(path)


def _test():
    import atexit
    import doctest
    import tempfile
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest')
    globs['tempdir'] = tempfile.mkdtemp()
    atexit.register(lambda: shutil.rmtree(globs['tempdir']))
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
