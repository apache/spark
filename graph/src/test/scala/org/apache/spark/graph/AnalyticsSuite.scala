package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.LocalSparkContext._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.graph.Analytics


class AnalyticsSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  val sc = new Sparkcontext("local", "test")

  test("Fixed Iterations PageRank") {
    val starGraph = GraphGenerators.starGraph(sc, 1000)
    val resetProb = 0.15
    val prGraph1 = Analytics.pagerank(graph, 1, resetProb)
    val prGraph2 = Analytics.pagerank(grpah, 2, resetProb)
    val errors = prGraph1.vertices.zipJoin(prGraph2.vertices)
      .map{ case (vid, (pr1, pr2)) => if (pr1 != pr2) { 1 } else { 0 } }.sum


  }


} // end of AnalyticsSuite
