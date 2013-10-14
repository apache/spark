package org.apache.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph.impl.GraphImpl


object GraphLoader {

  /**
   * Load an edge list from file initializing the Graph RDD
   */
  def textFile[ED: ClassManifest](
      sc: SparkContext,
      path: String,
      edgeParser: Array[String] => ED,
      minEdgePartitions: Int = 1,
      minVertexPartitions: Int = 1)
    : GraphImpl[Int, ED] = {

    // Parse the edge data table
    val edges = sc.textFile(path).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        if(lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0)
        val target = lineArray(1)
        val tail = lineArray.drop(2)
        val edata = edgeParser(tail)
        Array(Edge(source.trim.toInt, target.trim.toInt, edata))
      } else {
        Array.empty[Edge[ED]]
      }
    }.cache()

    val graph = fromEdges(edges)
    // println("Loaded graph:" +
    //   "\n\t#edges:    " + graph.numEdges +
    //   "\n\t#vertices: " + graph.numVertices)

    graph
  }

  def fromEdges[ED: ClassManifest](edges: RDD[Edge[ED]]): GraphImpl[Int, ED] = {
    val vertices = edges.flatMap { edge => List((edge.src, 1), (edge.dst, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => (vid, degree) }
    GraphImpl(vertices, edges)
  }
}
