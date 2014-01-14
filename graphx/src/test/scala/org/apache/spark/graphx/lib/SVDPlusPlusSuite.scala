package org.apache.spark.graphx.lib

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._


class SVDPlusPlusSuite extends FunSuite with LocalSparkContext {

  test("Test SVD++ with mean square error on training set") {
    withSpark { sc =>
      val svdppErr = 8.0
      val edges = sc.textFile("mllib/data/als/test.data").map { line =>
        val fields = line.split(",")
        Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
      }
      val conf = new SVDPlusPlus.Conf(10, 2, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015) // 2 iterations
      var (graph, u) = SVDPlusPlus.run(edges, conf)
      graph.cache()
      val err = graph.vertices.collect.map{ case (vid, vd) =>
        if (vid % 2 == 1) vd._4 else 0.0
      }.reduce(_ + _) / graph.triplets.collect.size
      assert(err <= svdppErr)
    }
  }

}
