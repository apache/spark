import os
import atexit
from tempfile import NamedTemporaryFile

from pyspark.broadcast import Broadcast
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import dump_pickle, write_with_length
from pyspark.rdd import RDD

from py4j.java_collections import ListConverter


class SparkContext(object):

    gateway = launch_gateway()
    jvm = gateway.jvm
    pickleFile = jvm.spark.api.python.PythonRDD.pickleFile
    asPickle = jvm.spark.api.python.PythonRDD.asPickle
    arrayAsPickle = jvm.spark.api.python.PythonRDD.arrayAsPickle

    def __init__(self, master, name, defaultParallelism=None,
                 pythonExec='python'):
        self.master = master
        self.name = name
        self._jsc = self.jvm.JavaSparkContext(master, name)
        self.defaultParallelism = \
            defaultParallelism or self._jsc.sc().defaultParallelism()
        self.pythonExec = pythonExec
        # Broadcast's __reduce__ method stores Broadcast instances here.
        # This allows other code to determine which Broadcast instances have
        # been pickled, so it can determine which Java broadcast objects to
        # send.
        self._pickled_broadcast_vars = set()

    def __del__(self):
        if self._jsc:
            self._jsc.stop()

    def stop(self):
        self._jsc.stop()
        self._jsc = None

    def parallelize(self, c, numSlices=None):
        numSlices = numSlices or self.defaultParallelism
        # Calling the Java parallelize() method with an ArrayList is too slow,
        # because it sends O(n) Py4J commands.  As an alternative, serialized
        # objects are written to a file and loaded through textFile().
        tempFile = NamedTemporaryFile(delete=False)
        for x in c:
            write_with_length(dump_pickle(x), tempFile)
        tempFile.close()
        atexit.register(lambda: os.unlink(tempFile.name))
        jrdd = self.pickleFile(self._jsc, tempFile.name, numSlices)
        return RDD(jrdd, self)

    def textFile(self, name, minSplits=None):
        minSplits = minSplits or min(self.defaultParallelism, 2)
        jrdd = self._jsc.textFile(name, minSplits)
        return RDD(jrdd, self)

    def union(self, rdds):
        first = rdds[0]._jrdd
        rest = [x._jrdd for x in rdds[1:]]
        rest = ListConverter().convert(rest, self.gateway._gateway_client)
        return RDD(self._jsc.union(first, rest), self)

    def broadcast(self, value):
        jbroadcast = self._jsc.broadcast(bytearray(dump_pickle(value)))
        return Broadcast(jbroadcast.id(), value, jbroadcast,
                         self._pickled_broadcast_vars)
