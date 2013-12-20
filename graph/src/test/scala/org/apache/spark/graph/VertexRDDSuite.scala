package org.apache.spark.graph

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.Graph._
import org.apache.spark.graph.impl.EdgePartition
import org.apache.spark.graph.impl.EdgePartitionBuilder
import org.apache.spark.rdd._

class VertexRDDSuite extends FunSuite with LocalSparkContext {

  test("VertexRDD") {
    withSpark { sc =>
      val n = 100
      val a = sc.parallelize((0 to n).map(x => (x.toLong, x.toLong)), 5)
      val b = VertexRDD(a).mapValues(x => -x).cache() // Allow joining b with a derived RDD of b
      assert(b.count === n + 1)
      assert(b.leftJoin(a){ (id, a, bOpt) => a + bOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val c = b.aggregateUsingIndex[Long](a, (x, y) => x)
      assert(b.leftJoin(c){ (id, b, cOpt) => b + cOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val d = c.filter(q => ((q._2 % 2) == 0))
      val e = a.filter(q => ((q._2 % 2) == 0))
      assert(d.count === e.count)
      assert(b.zipJoin(c)((id, b, c) => b + c).map(x => x._2).reduce(_+_) === 0)
      val f = b.mapValues(x => if (x % 2 == 0) -x else x)
      assert(b.diff(f).collect().toSet === (2 to n by 2).map(x => (x.toLong, x.toLong)).toSet)
    }
  }
}
