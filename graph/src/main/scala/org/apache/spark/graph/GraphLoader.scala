package org.apache.spark.graph

import java.util.{Arrays => JArrays}
import scala.reflect.ClassTag

import org.apache.spark.graph.impl.EdgePartitionBuilder
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graph.impl.{EdgePartition, GraphImpl}
import org.apache.spark.util.collection.PrimitiveVector


object GraphLoader extends Logging {

  /**
   * Load an edge list from file initializing the Graph
   *
   * @tparam ED the type of the edge data of the resulting Graph
   *
   * @param sc the SparkContext used to construct RDDs
   * @param path the path to the text file containing the edge list
   * @param edgeParser a function that takes an array of strings and
   * returns an ED object
   * @param minEdgePartitions the number of partitions for the
   * the Edge RDD
   *
   */
  def textFile[ED: ClassTag](
      sc: SparkContext,
      path: String,
      edgeParser: Array[String] => ED,
      minEdgePartitions: Int = 1):
    Graph[Int, ED] = {
    // Parse the edge data table
    val edges = sc.textFile(path, minEdgePartitions).mapPartitions( iter =>
      iter.filter(line => !line.isEmpty && line(0) != '#').map { line =>
        val lineArray = line.split("\\s+")
        if(lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0).trim.toLong
        val target = lineArray(1).trim.toLong
        val tail = lineArray.drop(2)
        val edata = edgeParser(tail)
        Edge(source, target, edata)
      })
    val defaultVertexAttr = 1
    Graph.fromEdges(edges, defaultVertexAttr)
  }

  /**
   * Load a graph from an edge list formatted file with each line containing
   * two integers: a source Id and a target Id.
   *
   * @example A file in the following format:
   * {{{
   * # Comment Line
   * # Source Id <\t> Target Id
   * 1   -5
   * 1    2
   * 2    7
   * 1    8
   * }}}
   *
   * If desired the edges can be automatically oriented in the positive
   * direction (source Id < target Id) by setting `canonicalOrientation` to
   * true
   *
   * @param sc
   * @param path the path to the file (e.g., /Home/data/file or hdfs://file)
   * @param canonicalOrientation whether to orient edges in the positive
   *        direction.
   * @param minEdgePartitions the number of partitions for the
   *        the Edge RDD
   * @tparam ED
   * @return
   */
  def edgeListFile(
      sc: SparkContext,
      path: String,
      canonicalOrientation: Boolean = false,
      minEdgePartitions: Int = 1):
    Graph[Int, Int] = {
    val startTime = System.currentTimeMillis

    // Parse the edge data table directly into edge partitions
    val edges = sc.textFile(path, minEdgePartitions).mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            logWarning("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          if (canonicalOrientation && dstId > srcId) {
            builder.add(dstId, srcId, 1)
          } else {
            builder.add(srcId, dstId, 1)
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }.cache()
    edges.count()

    logInfo("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))

    GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1)
  } // end of edgeListFile

}
