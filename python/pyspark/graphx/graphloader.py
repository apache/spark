

from pyspark import SparkContext


class GraphLoader(object):

    @staticmethod
    def edgeListFile(sc, filename, partitions, edgeStorageLevel, vertexStorageLevel):

        edgeStorageLevel = sc._jvm.JavaStorageLevel.MEMORY_ONLY
        vertexStorageLevel = sc._jvm.JavaStorageLevel.MEMORY_ONLY
        graphLoader = sc._jvm.org.apache.spark.PythonGraphLoader
        graph = graphLoader.edgeListFile(sc, filename, partitions, edgeStorageLevel, vertexStorageLevel)

        return graph
