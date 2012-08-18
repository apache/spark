import os
import atexit
from tempfile import NamedTemporaryFile

from pyspark.java_gateway import launch_gateway
from pyspark.serializers import PickleSerializer, dumps
from pyspark.rdd import RDD


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

    def __del__(self):
        if self._jsc:
            self._jsc.stop()

    def stop(self):
        self._jsc.stop()
        self._jsc = None

    def parallelize(self, c, numSlices=None):
        """
        >>> sc = SparkContext("local", "test")
        >>> rdd = sc.parallelize([(1, 2), (3, 4)])
        >>> rdd.collect()
        [(1, 2), (3, 4)]
        """
        numSlices = numSlices or self.defaultParallelism
        # Calling the Java parallelize() method with an ArrayList is too slow,
        # because it sends O(n) Py4J commands.  As an alternative, serialized
        # objects are written to a file and loaded through textFile().
        tempFile = NamedTemporaryFile(delete=False)
        for x in c:
            dumps(PickleSerializer.dumps(x), tempFile)
        tempFile.close()
        atexit.register(lambda: os.unlink(tempFile.name))
        jrdd = self.pickleFile(self._jsc, tempFile.name, numSlices)
        return RDD(jrdd, self)

    def textFile(self, name, numSlices=None):
        numSlices = numSlices or self.defaultParallelism
        jrdd = self._jsc.textFile(name, numSlices)
        return RDD(jrdd, self)
