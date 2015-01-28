

from pyspark import SparkContext
from pyspark.graphx import Graph, EdgeRDD, VertexRDD


class GraphLoader(object):

    @staticmethod
    def edgeListFile(sc, filename, partitions, edgeStorageLevel, vertexStorageLevel):

        jrdd = sc.textFile(filename)

        graphLoader = sc._jvm.org.apache.spark.PythonGraphLoader
        graph = graphLoader.edgeListFile(sc, filename, partitions, edgeStorageLevel, vertexStorageLevel)
        return graph
