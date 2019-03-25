package org.apache.spark.graph.cypher.algos

import org.apache.spark.graph.api.PropertyGraph
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.types.BinaryType

object GraphAlgorithms {

  implicit class PageRankGraph(graph: PropertyGraph) {

    def withPageRank(tolerance: Double = 0.01, resetProbability: Double = 0.15): PropertyGraph = {
      // Create GraphX compatible RDDs from nodes and relationships
      val graphXNodeRDD = graph
        .nodes
        .select("$ID")
        .rdd.map(row => row.getAs[Array[Byte]](0).toLong -> null)

      val graphXRelRDD = graph
        .relationships
        .select("$SOURCE_ID", "$TARGET_ID")
        .rdd.map(row => Edge(row.getAs[Array[Byte]](0).toLong, row.getAs[Array[Byte]](1).toLong, ()))

      // Compute Page Rank via GraphX
      val graphX = Graph(graphXNodeRDD, graphXRelRDD)
      val ranks = graphX.pageRank(tolerance, resetProbability).vertices

      // Convert RDD to DataFrame
      val rankDf = graph.cypherSession.sparkSession.createDataFrame(ranks)
      val rankTable = rankDf.select(
        rankDf.col("_1").cast(BinaryType).as("$ID"),
        rankDf.col("_2").as("pageRank"))

      graph.cypherSession.createGraph(graph.nodes.join(rankTable, "$ID"), graph.relationships)
    }
  }

  implicit class ByteArrayConversion(val a: Array[Byte]) extends AnyVal {
    def toLong: Long = {
      var result = a(0).toLong << 56
      result |= a(1).toLong << 48
      result |= a(2).toLong << 40
      result |= a(3).toLong << 32
      result |= a(4).toLong << 24
      result |= a(5).toLong << 16
      result |= a(6).toLong << 8
      result |= a(7).toLong
      result
    }
  }
}
