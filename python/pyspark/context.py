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
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.rdd import RDD

from py4j.java_collections import ListConverter


class SparkContext(object):
    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create L{RDD}s and
    broadcast variables on that cluster.
    """

    _gateway = None
    _jvm = None
    _writeToFile = None
    _next_accum_id = 0
    _active_spark_context = None
    _lock = Lock()
    _python_includes = None # zip and egg files that need to be added to PYTHONPATH


    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None,
        environment=None, batchSize=1024, serializer=PickleSerializer(), conf=None):
        """
        Create a new SparkContext. At least the master and app name should be set,
        either through the named parameters here or through C{conf}.

        @param master: Cluster URL to connect to
               (e.g. mesos://host:port, spark://host:port, local[4]).
        @param appName: A name for your job, to display on the cluster web UI.
        @param sparkHome: Location where Spark is installed on cluster nodes.
        @param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
        @param environment: A dictionary of environment variables to set on
               worker nodes.
        @param batchSize: The number of Python objects represented as a single
               Java object.  Set 1 to disable batching or -1 to use an
               unlimited batch size.
        @param serializer: The serializer for RDDs.
        @param conf: A L{SparkConf} object setting Spark properties.


        >>> from pyspark.context import SparkContext
        >>> sc = SparkContext('local', 'test')

        >>> sc2 = SparkContext('local', 'test2') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError:...
        """
        SparkContext._ensure_initialized(self)

        self.environment = environment or {}
        self._conf = conf or SparkConf(_jvm=self._jvm)
        self._batchSize = batchSize  # -1 represents an unlimited batch size
        self._unbatched_serializer = serializer
        if batchSize == 1:
            self.serializer = self._unbatched_serializer
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
        self._jsc = self._jvm.JavaSparkContext(self._conf._jconf)

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
        sys.path.append(root_dir)

        # Deploy any code dependencies specified in the constructor
        self._python_includes = list()
        for path in (pyFiles or []):
            self.addPyFile(path)

        # Create a temporary directory inside spark.local.dir:
        local_dir = self._jvm.org.apache.spark.util.Utils.getLocalDir(self._jsc.sc().conf())
        self._temp_dir = \
            self._jvm.org.apache.spark.util.Utils.createTempDir(local_dir).getAbsolutePath()

    @classmethod
    def _ensure_initialized(cls, instance=None):
        with SparkContext._lock:
            if not SparkContext._gateway:
                SparkContext._gateway = launch_gateway()
                SparkContext._jvm = SparkContext._gateway.jvm
                SparkContext._writeToFile = SparkContext._jvm.PythonRDD.writeToFile

            if instance:
                if SparkContext._active_spark_context and SparkContext._active_spark_context != instance:
                    raise ValueError("Cannot run multiple SparkContexts at once")
                else:
                    SparkContext._active_spark_context = instance

    @classmethod
    def setSystemProperty(cls, key, value):
        """
        Set a Java system property, such as spark.executor.memory. This must
        must be invoked before instantiating SparkContext.
        """
        SparkContext._ensure_initialized()
        SparkContext._jvm.java.lang.System.setProperty(key, value)

    @property
    def defaultParallelism(self):
        """
        Default level of parallelism to use when not given by user (e.g. for
        reduce tasks)
        """
        return self._jsc.sc().defaultParallelism()

    def __del__(self):
        self.stop()

    def stop(self):
        """
        Shut down the SparkContext.
        """
        if self._jsc:
            self._jsc.stop()
            self._jsc = None
        if self._accumulatorServer:
            self._accumulatorServer.shutdown()
            self._accumulatorServer = None
        with SparkContext._lock:
            SparkContext._active_spark_context = None

    def parallelize(self, c, numSlices=None):
        """
        Distribute a local Python collection to form an RDD.

        >>> sc.parallelize(range(5), 5).glom().collect()
        [[0], [1], [2], [3], [4]]
        """
        numSlices = numSlices or self.defaultParallelism
        # Calling the Java parallelize() method with an ArrayList is too slow,
        # because it sends O(n) Py4J commands.  As an alternative, serialized
        # objects are written to a file and loaded through textFile().
        tempFile = NamedTemporaryFile(delete=False, dir=self._temp_dir)
        # Make sure we distribute data evenly if it's smaller than self.batchSize
        if "__len__" not in dir(c):
            c = list(c)    # Make it a list so we can compute its length
        batchSize = min(len(c) // numSlices, self._batchSize)
        if batchSize > 1:
            serializer = BatchedSerializer(self._unbatched_serializer,
                                           batchSize)
        else:
            serializer = self._unbatched_serializer
        serializer.dump_stream(c, tempFile)
        tempFile.close()
        readRDDFromFile = self._jvm.PythonRDD.readRDDFromFile
        jrdd = readRDDFromFile(self._jsc, tempFile.name, numSlices)
        return RDD(jrdd, self, serializer)

    def textFile(self, name, minSplits=None):
        """
        Read a text file from HDFS, a local file system (available on all
        nodes), or any Hadoop-supported file system URI, and return it as an
        RDD of Strings.
        """
        minSplits = minSplits or min(self.defaultParallelism, 2)
        return RDD(self._jsc.textFile(name, minSplits), self,
                   UTF8Deserializer())

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
        return RDD(self._jsc.union(first, rest), self,
                   rdds[0]._jrdd_deserializer)

    def broadcast(self, value):
        """
        Broadcast a read-only variable to the cluster, returning a
        L{Broadcast<pyspark.broadcast.Broadcast>}
        object for reading it in distributed functions. The variable will be
        sent to each cluster only once.
        """
        pickleSer = PickleSerializer()
        pickled = pickleSer.dumps(value)
        jbroadcast = self._jsc.broadcast(bytearray(pickled))
        return Broadcast(jbroadcast.id(), value, jbroadcast,
                         self._pickled_broadcast_vars)

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
        L{SparkFiles.get(path)<pyspark.files.SparkFiles.get>} to find its
        download location.

        >>> from pyspark import SparkFiles
        >>> path = os.path.join(tempdir, "test.txt")
        >>> with open(path, "w") as testFile:
        ...    testFile.write("100")
        >>> sc.addFile(path)
        >>> def func(iterator):
        ...    with open(SparkFiles.get("test.txt")) as testFile:
        ...        fileVal = int(testFile.readline())
        ...        return [x * 100 for x in iterator]
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
        (dirname, filename) = os.path.split(path) # dirname may be directory or HDFS/S3 prefix

        if filename.endswith('.zip') or filename.endswith('.ZIP') or filename.endswith('.egg'):
            self._python_includes.append(filename)
            sys.path.append(os.path.join(SparkFiles.getRootDirectory(), filename)) # for tests in local mode

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
        return newStorageLevel(storageLevel.useDisk, storageLevel.useMemory,
            storageLevel.deserialized, storageLevel.replication)

def _test():
    import atexit
    import doctest
    import tempfile
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    globs['tempdir'] = tempfile.mkdtemp()
    atexit.register(lambda: shutil.rmtree(globs['tempdir']))
    (failure_count, test_count) = doctest.testmod(globs=globs)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
