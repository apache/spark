import os
import atexit
from tempfile import NamedTemporaryFile

from pyspark import accumulators
from pyspark.accumulators import Accumulator
from pyspark.broadcast import Broadcast
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import dump_pickle, write_with_length, batched
from pyspark.rdd import RDD

from py4j.java_collections import ListConverter


class SparkContext(object):
    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create L{RDD}s and
    broadcast variables on that cluster.
    """

    gateway = launch_gateway()
    jvm = gateway.jvm
    _readRDDFromPickleFile = jvm.PythonRDD.readRDDFromPickleFile
    _writeIteratorToPickleFile = jvm.PythonRDD.writeIteratorToPickleFile
    _takePartition = jvm.PythonRDD.takePartition
    _next_accum_id = 0

    def __init__(self, master, jobName, sparkHome=None, pyFiles=None,
        environment=None, batchSize=1024):
        """
        Create a new SparkContext.

        @param master: Cluster URL to connect to
               (e.g. mesos://host:port, spark://host:port, local[4]).
        @param jobName: A name for your job, to display on the cluster web UI
        @param sparkHome: Location where Spark is installed on cluster nodes.
        @param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
        @param environment: A dictionary of environment variables to set on
               worker nodes.
        @param batchSize: The number of Python objects represented as a single
               Java object.  Set 1 to disable batching or -1 to use an
               unlimited batch size.
        """
        self.master = master
        self.jobName = jobName
        self.sparkHome = sparkHome or None # None becomes null in Py4J
        self.environment = environment or {}
        self.batchSize = batchSize  # -1 represents a unlimited batch size

        # Create the Java SparkContext through Py4J
        empty_string_array = self.gateway.new_array(self.jvm.String, 0)
        self._jsc = self.jvm.JavaSparkContext(master, jobName, sparkHome,
                                              empty_string_array)

        # Create a single Accumulator in Java that we'll send all our updates through;
        # they will be passed back to us through a TCP server
        self._accumulatorServer = accumulators._start_update_server()
        (host, port) = self._accumulatorServer.server_address
        self._javaAccumulator = self._jsc.accumulator(
                self.jvm.java.util.ArrayList(),
                self.jvm.PythonAccumulatorParam(host, port))

        self.pythonExec = os.environ.get("PYSPARK_PYTHON", 'python')
        # Broadcast's __reduce__ method stores Broadcast instances here.
        # This allows other code to determine which Broadcast instances have
        # been pickled, so it can determine which Java broadcast objects to
        # send.
        self._pickled_broadcast_vars = set()

        # Deploy any code dependencies specified in the constructor
        for path in (pyFiles or []):
            self.addPyFile(path)

    @property
    def defaultParallelism(self):
        """
        Default level of parallelism to use when not given by user (e.g. for
        reduce tasks)
        """
        return self._jsc.sc().defaultParallelism()

    def __del__(self):
        if self._jsc:
            self._jsc.stop()
        if self._accumulatorServer:
            self._accumulatorServer.shutdown()

    def stop(self):
        """
        Shut down the SparkContext.
        """
        self._jsc.stop()
        self._jsc = None

    def parallelize(self, c, numSlices=None):
        """
        Distribute a local Python collection to form an RDD.
        """
        numSlices = numSlices or self.defaultParallelism
        # Calling the Java parallelize() method with an ArrayList is too slow,
        # because it sends O(n) Py4J commands.  As an alternative, serialized
        # objects are written to a file and loaded through textFile().
        tempFile = NamedTemporaryFile(delete=False)
        atexit.register(lambda: os.unlink(tempFile.name))
        if self.batchSize != 1:
            c = batched(c, self.batchSize)
        for x in c:
            write_with_length(dump_pickle(x), tempFile)
        tempFile.close()
        jrdd = self._readRDDFromPickleFile(self._jsc, tempFile.name, numSlices)
        return RDD(jrdd, self)

    def textFile(self, name, minSplits=None):
        """
        Read a text file from HDFS, a local file system (available on all
        nodes), or any Hadoop-supported file system URI, and return it as an
        RDD of Strings.
        """
        minSplits = minSplits or min(self.defaultParallelism, 2)
        jrdd = self._jsc.textFile(name, minSplits)
        return RDD(jrdd, self)

    def union(self, rdds):
        """
        Build the union of a list of RDDs.
        """
        first = rdds[0]._jrdd
        rest = [x._jrdd for x in rdds[1:]]
        rest = ListConverter().convert(rest, self.gateway._gateway_client)
        return RDD(self._jsc.union(first, rest), self)

    def broadcast(self, value):
        """
        Broadcast a read-only variable to the cluster, returning a C{Broadcast}
        object for reading it in distributed functions. The variable will be
        sent to each cluster only once.
        """
        jbroadcast = self._jsc.broadcast(bytearray(dump_pickle(value)))
        return Broadcast(jbroadcast.id(), value, jbroadcast,
                         self._pickled_broadcast_vars)

    def accumulator(self, value, accum_param=None):
        """
        Create an C{Accumulator} with the given initial value, using a given
        AccumulatorParam helper object to define how to add values of the data 
        type if provided. Default AccumulatorParams are used for integers and
        floating-point numbers if you do not provide one. For other types, the
        AccumulatorParam must implement two methods:
        - C{zero(value)}: provide a "zero value" for the type, compatible in
          dimensions with the provided C{value} (e.g., a zero vector).
        - C{addInPlace(val1, val2)}: add two values of the accumulator's data
          type, returning a new value; for efficiency, can also update C{val1}
          in place and return it.
        """
        if accum_param == None:
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
        Add a file to be downloaded into the working directory of this Spark
        job on every node. The C{path} passed can be either a local file,
        a file in HDFS (or other Hadoop-supported filesystems), or an HTTP,
        HTTPS or FTP URI.
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
        filename = path.split("/")[-1]
        os.environ["PYTHONPATH"] = \
            "%s:%s" % (filename, os.environ["PYTHONPATH"])
