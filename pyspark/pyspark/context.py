import os
import atexit
from tempfile import NamedTemporaryFile

from pyspark.java_gateway import launch_gateway
from pyspark.serializers import JSONSerializer, NopSerializer
from pyspark.rdd import RDD, PairRDD


class SparkContext(object):

    gateway = launch_gateway()
    jvm = gateway.jvm
    python_dump = jvm.spark.api.python.PythonRDD.pythonDump

    def __init__(self, master, name, defaultSerializer=JSONSerializer,
            defaultParallelism=None, pythonExec='python'):
        self.master = master
        self.name = name
        self._jsc = self.jvm.JavaSparkContext(master, name)
        self.defaultSerializer = defaultSerializer
        self.defaultParallelism = \
            defaultParallelism or self._jsc.sc().defaultParallelism()
        self.pythonExec = pythonExec

    def __del__(self):
        if self._jsc:
            self._jsc.stop()

    def stop(self):
        self._jsc.stop()
        self._jsc = None

    def parallelize(self, c, numSlices=None, serializer=None):
        serializer = serializer or self.defaultSerializer
        numSlices = numSlices or self.defaultParallelism
        # Calling the Java parallelize() method with an ArrayList is too slow,
        # because it sends O(n) Py4J commands.  As an alternative, serialized
        # objects are written to a file and loaded through textFile().
        tempFile = NamedTemporaryFile(delete=False)
        tempFile.writelines(serializer.dumps(x) + '\n' for x in c)
        tempFile.close()
        atexit.register(lambda: os.unlink(tempFile.name))
        return self.textFile(tempFile.name, numSlices, serializer)

    def parallelizePairs(self, c, numSlices=None, keySerializer=None,
                         valSerializer=None):
        """
        >>> sc = SparkContext("local", "test")
        >>> rdd = sc.parallelizePairs([(1, 2), (3, 4)])
        >>> rdd.collect()
        [(1, 2), (3, 4)]
        """
        keySerializer = keySerializer or self.defaultSerializer
        valSerializer = valSerializer or self.defaultSerializer
        numSlices = numSlices or self.defaultParallelism
        tempFile = NamedTemporaryFile(delete=False)
        for (k, v) in c:
            tempFile.write(keySerializer.dumps(k).rstrip('\r\n') + '\n')
            tempFile.write(valSerializer.dumps(v).rstrip('\r\n') + '\n')
        tempFile.close()
        atexit.register(lambda: os.unlink(tempFile.name))
        jrdd = self.textFile(tempFile.name, numSlices)._pipePairs([], "echo")
        return PairRDD(jrdd, self, keySerializer, valSerializer)

    def textFile(self, name, numSlices=None, serializer=NopSerializer):
        numSlices = numSlices or self.defaultParallelism
        jrdd = self._jsc.textFile(name, numSlices)
        return RDD(jrdd, self, serializer)
