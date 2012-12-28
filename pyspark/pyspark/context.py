import os
import atexit
from tempfile import NamedTemporaryFile

from pyspark.broadcast import Broadcast
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import dump_pickle, write_with_length
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
    readRDDFromPickleFile = jvm.PythonRDD.readRDDFromPickleFile
    writeArrayToPickleFile = jvm.PythonRDD.writeArrayToPickleFile

    def __init__(self, master, name, defaultParallelism=None, batchSize=-1):
        self.master = master
        self.name = name
        self._jsc = self.jvm.JavaSparkContext(master, name)
        self.defaultParallelism = \
            defaultParallelism or self._jsc.sc().defaultParallelism()
        self.pythonExec = os.environ.get("PYSPARK_PYTHON_EXEC", 'python')
        self.batchSize = batchSize  # -1 represents a unlimited batch size
        # Broadcast's __reduce__ method stores Broadcast instances here.
        # This allows other code to determine which Broadcast instances have
        # been pickled, so it can determine which Java broadcast objects to
        # send.
        self._pickled_broadcast_vars = set()

    def __del__(self):
        if self._jsc:
            self._jsc.stop()

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
        for x in c:
            write_with_length(dump_pickle(x), tempFile)
        tempFile.close()
        jrdd = self.readRDDFromPickleFile(self._jsc, tempFile.name, numSlices)
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
        Build the union of a list of RDDs
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
