package org.apache.spark.graph

import org.apache.spark.SparkContext
import org.apache.spark.graph.Graph._
import org.apache.spark.graph.impl.EdgePartition
import org.apache.spark.rdd._
import org.scalatest.FunSuite

class VertexRDDSuite extends FunSuite with LocalSparkContext {

  def vertices(sc: SparkContext, n: Int) = {
    VertexRDD(sc.parallelize((0 to n).map(x => (x.toLong, x)), 5))
  }

  test("filter") {
    withSpark { sc =>
      val n = 100
      val verts = vertices(sc, n)
      val evens = verts.filter(q => ((q._2 % 2) == 0))
      assert(evens.count === (0 to n).filter(_ % 2 == 0).size)
    }
  }

  test("mapValues") {
    withSpark { sc =>
      val n = 100
      val verts = vertices(sc, n)
      val negatives = verts.mapValues(x => -x).cache() // Allow joining b with a derived RDD of b
      assert(negatives.count === n + 1)
    }
  }

  test("diff") {
    withSpark { sc =>
      val n = 100
      val verts = vertices(sc, n)
      val flipEvens = verts.mapValues(x => if (x % 2 == 0) -x else x)
      // diff should keep only the changed vertices
      assert(verts.diff(flipEvens).map(_._2).collect().toSet === (2 to n by 2).map(-_).toSet)
      // diff should keep the vertex values from `other`
      assert(flipEvens.diff(verts).map(_._2).collect().toSet === (2 to n by 2).toSet)
    }
  }

  test("leftJoin") {
    withSpark { sc =>
      val n = 100
      val verts = vertices(sc, n)
      val evens = verts.filter(q => ((q._2 % 2) == 0))
      // leftJoin with another VertexRDD
      assert(verts.leftJoin(evens) { (id, a, bOpt) => a - bOpt.getOrElse(0) }.collect.toSet ===
        (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
      // leftJoin with an RDD
      val evensRDD = evens.map(identity)
      assert(verts.leftJoin(evensRDD) { (id, a, bOpt) => a - bOpt.getOrElse(0) }.collect.toSet ===
        (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
    }
  }

  test("innerJoin") {
    withSpark { sc =>
      val n = 100
      val verts = vertices(sc, n)
      val evens = verts.filter(q => ((q._2 % 2) == 0))
      // innerJoin with another VertexRDD
      assert(verts.innerJoin(evens) { (id, a, b) => a - b }.collect.toSet ===
        (0 to n by 2).map(x => (x.toLong, 0)).toSet)
      // innerJoin with an RDD
      val evensRDD = evens.map(identity)
      assert(verts.innerJoin(evensRDD) { (id, a, b) => a - b }.collect.toSet ===
        (0 to n by 2).map(x => (x.toLong, 0)).toSet)    }
  }

  test("aggregateUsingIndex") {
    withSpark { sc =>
      val n = 100
      val verts = vertices(sc, n)
      val messageTargets = (0 to n) ++ (0 to n by 2)
      val messages = sc.parallelize(messageTargets.map(x => (x.toLong, 1)))
      assert(verts.aggregateUsingIndex[Int](messages, _ + _).collect.toSet ===
        (0 to n).map(x => (x.toLong, if (x % 2 == 0) 2 else 1)).toSet)
    }
  }

}
