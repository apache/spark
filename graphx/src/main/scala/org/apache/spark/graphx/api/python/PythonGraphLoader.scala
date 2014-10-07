package org.apache.spark.graphx.api.python

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.storage.StorageLevel

/**
 * Created by kdatta1 on 10/7/14.
 */
class PythonGraphLoader {

  def edgeListFile(
    sc: SparkContext,
    path: String,
    partitions: Int,
    edgeStorageLevel: StorageLevel,
    vertexStorageLevel: StorageLevel) : Graph[Array[Byte], Array[Byte]] = {

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, path, false, partitions, edgeStorageLevel, vertexStorageLevel)
    graph.vertices.foreach(vertex => vertex.toByteArray())
  }
}
