package org.apache.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph.impl.GraphImpl


object GraphLoader {

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
  def textFile[ED: ClassManifest](
      sc: SparkContext,
      path: String,
      edgeParser: Array[String] => ED,
      minEdgePartitions: Int = 1,
      partitionStrategy: PartitionStrategy = RandomVertexCut):
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
    Graph.fromEdges(edges, defaultVertexAttr, partitionStrategy)
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
  def edgeListFile[ED: ClassManifest](
      sc: SparkContext,
      path: String,
      canonicalOrientation: Boolean = false,
      minEdgePartitions: Int = 1,
      partitionStrategy: PartitionStrategy = RandomVertexCut):
    Graph[Int, Int] = {
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
        if (canonicalOrientation && target > source) {
          Edge(target, source, 1)
        } else {
          Edge(source, target, 1)
        }
      })
    val defaultVertexAttr = 1
    Graph.fromEdges(edges, defaultVertexAttr, partitionStrategy)
  } // end of edgeListFile

}
