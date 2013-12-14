package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.LocalSparkContext._
import org.apache.spark.rdd._

class PregelSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  test("1 iteration") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 5
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid)), 3), "v")
      val result = Pregel(star, 0)(
        (vid, attr, msg) => attr,
        et => Iterator.empty,
        (a: Int, b: Int) => throw new Exception("mergeMsg run unexpectedly"))
      assert(result.vertices.collect.toSet === star.vertices.collect.toSet)
    }
  }

  test("chain propagation") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 5
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until n).map(x => (x: Vid, x + 1: Vid)), 3),
        0).cache()
      assert(chain.vertices.collect.toSet === (1 to n).map(x => (x: Vid, 0)).toSet)
      val chainWithSeed = chain.mapVertices { (vid, attr) => if (vid == 1) 1 else 0 }
      assert(chainWithSeed.vertices.collect.toSet === Set((1: Vid, 1)) ++ (2 to n).map(x => (x: Vid, 0)).toSet)
      val result = Pregel(chainWithSeed, 0)(
        (vid, attr, msg) => { println("vprog on " + (vid, attr, msg)); math.max(msg, attr) },
        et => { println("sendMsg on " + ((et.srcId, et.srcAttr), (et.dstId, et.dstAttr))); Iterator((et.dstId, et.srcAttr)) },
        (a: Int, b: Int) => { println("mergeMsg on " + (a, b)); math.max(a, b) })
      assert(result.vertices.collect.toSet ===
        chain.vertices.mapValues { (vid, attr) => attr + 1 }.collect.toSet)
    }
  }
}
