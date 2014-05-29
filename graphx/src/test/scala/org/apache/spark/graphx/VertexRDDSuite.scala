/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph._
import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.rdd._
import org.scalatest.FunSuite

class VertexRDDSuite extends FunSuite with LocalSparkContext {

  def vertices(sc: SparkContext, n: Int) = {
    VertexRDD(sc.parallelize((0 to n).map(x => (x.toLong, x)), 5))
  }

  test("shipVertexAttributes") {
    withSpark { sc =>
      val verts = VertexRDD(
        sc.parallelize(List((0L, "a"), (1L, "b"), (2L, "c")), 2),
        EdgeRDD.fromEdges(sc.parallelize(
          List(Edge(0L, 1L, 1), Edge(1L, 2L, 1), Edge(2L, 0L, 1)), 3)))
      val shipped = verts.shipVertexAttributes(true, true).collect
      assert(shipped.length === 3)
      assert(shipped(0)._1 === 0)
      assert(shipped(0)._2.vids === Array(0L, 1L))
      assert(shipped(0)._2.attrs === Array("a", "b"))
      assert(shipped(1)._1 === 1)
      assert(shipped(1)._2.vids === Array(1L, 2L))
      assert(shipped(1)._2.attrs === Array("b", "c"))
      assert(shipped(2)._1 === 2)
      assert(shipped(2)._2.vids === Array(0L, 2L))
      assert(shipped(2)._2.attrs === Array("a", "c"))
    }
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
      val verts = vertices(sc, n).cache()
      val flipEvens = verts.mapValues(x => if (x % 2 == 0) -x else x).cache()
      // diff should keep only the changed vertices
      assert(verts.diff(flipEvens).map(_._2).collect().toSet === (2 to n by 2).map(-_).toSet)
      // diff should keep the vertex values from `other`
      assert(flipEvens.diff(verts).map(_._2).collect().toSet === (2 to n by 2).toSet)
    }
  }

  test("leftJoin") {
    withSpark { sc =>
      val n = 100
      val verts = vertices(sc, n).cache()
      val evens = verts.filter(q => ((q._2 % 2) == 0)).cache()
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
      val verts = vertices(sc, n).cache()
      val evens = verts.filter(q => ((q._2 % 2) == 0)).cache()
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
