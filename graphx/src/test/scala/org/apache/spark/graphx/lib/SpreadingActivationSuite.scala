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

package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._

class SpreadingActivationSuite extends SparkFunSuite with LocalSparkContext {

  def compareTwoMaps(expected: Map[VertexId, Map[VertexId, Double]],
                     actual: Map[VertexId, Map[VertexId, Double]]): Boolean = {
    expected.forall((x) => x._2.forall((y) => {
      val diff = y._2 - actual(x._1)(y._1)
      diff.abs < 0.01
    }))
  }

  test("Spreading Activation trivial test") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 2L).map(x => (x, -1)))
      val edges = sc.parallelize(List(1L -> 2L).map((x) => new Edge[Int](x._1, x._2, 0)))
      val graph = Graph(vertices, edges)
      val res = SpreadingActivation.run(graph, List(1L)).collect().toMap
      val expected = Map(1L -> Map((1L, 1.0)), 2L -> Map((1L, 1.0)))
      assert(compareTwoMaps(expected, res))
    }
  }

  test("Spreading Activation trivial test, edge direction in") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 2L).map(x => (x, -1)))
      val edges = sc.parallelize(List(2L -> 1L).map((x) => new Edge[Int](x._1, x._2, 0)))
      val graph = Graph(vertices, edges)
      val res = SpreadingActivation.run(graph, List(1L), edgeDirection = EdgeDirection.In)
        .collect().toMap
      val expected = Map(1L -> Map((1L, 1.0)), 2L -> Map((1L, 1.0)))
      assert(compareTwoMaps(expected, res))
    }
  }

  test("Spreading Activation trivial test, edge direction either") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 3L).map(x => (x, -1)))
      val edges = sc.parallelize(List(1L -> 2L, 2L -> 3L).map((x) => new Edge[Int](x._1, x._2, 0)))
      val graph = Graph(vertices, edges)
      val res = SpreadingActivation.run(graph, List(2L), edgeDirection = EdgeDirection.Either)
        .collect().toMap
      val expected = Map(1L -> Map((2L, 0.5)), 2L -> Map((2L, 1.0)), 3L -> Map((2L, 0.5)))
      assert(compareTwoMaps(expected, res))
    }
  }

  test("Spreading Activation weight on edges") {
    // see http://en.wikipedia.org/wiki/Spreading_activation#Examples
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 7L).map(x => (x, -1)))
      val edges = sc.parallelize(
        List(
          1L -> 2L,
          2L -> 3L,
          3L -> 4L,
          3L -> 6L,
          4L -> 5L,
          6L -> 7L
        ).map((x) => new Edge[Double](x._1, x._2, 0.9)))

      val graph = Graph(vertices, edges)
      val res = SpreadingActivation
        .run(
          graph,
          List(1L),
          activationFunc = (old: Double, degree: Int, weight: Double) => old * weight * 0.85)
        .collect().toMap
      val expected =
        Map(
          1L -> Map((1L, 1.0)),
          2L -> Map((1L, 0.76)),
          3L -> Map((1L, 0.59)),
          4L -> Map((1L, 0.45)),
          5L -> Map((1L, 0.34)),
          6L -> Map((1L, 0.45)),
          7L -> Map((1L, 0.34)))
      assert(compareTwoMaps(expected, res))
    }
  }

  test("Spreading Activation simple circle") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 3L).map(x => (x, -1)))
      val edges = sc.parallelize(
        List(
          1L -> 2L,
          2L -> 3L,
          3L -> 1L
        ).map((x) => new Edge[Int](x._1, x._2, 0)))

      val graph = Graph(vertices, edges)
      val res = SpreadingActivation.run(graph, List(1L)).collect().toMap
      val expected =
        Map(
          1L -> Map((1L, 1.0)),
          2L -> Map((1L, 1.0)),
          3L -> Map((1L, 1.0)))
      assert(compareTwoMaps(expected, res))
    }
  }

  test("Spreading Activation simple circle, two starting nodes") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 3L).map(x => (x, -1)))
      val edges = sc.parallelize(
        List(
          1L -> 2L,
          2L -> 3L,
          3L -> 1L
        ).map((x) => new Edge[Int](x._1, x._2, 0)))

      val graph = Graph(vertices, edges)
      val res = SpreadingActivation.run(graph, List(1L, 2L)).collect().toMap
      val expected =
        Map(
          1L -> Map((1L, 1.0), (2L, 1.0)),
          2L -> Map((1L, 1.0), (2L, 1.0)),
          3L -> Map((1L, 1.0), (2L, 1.0)))
      assert(compareTwoMaps(expected, res))
    }
  }

  test("Spreading Activation split and merge") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 8L).map(x => (x, -1)))
      val edges = sc.parallelize(
        List(
          1L -> 2L,
          1L -> 3L,
          2L -> 4L,
          3L -> 5L,
          4L -> 6L,
          5L -> 6L,
          6L -> 7L,
          6L -> 8L
        ).map((x) => new Edge[Int](x._1, x._2, 0)))

      val graph = Graph(vertices, edges)
      val res = SpreadingActivation.run(graph, List(1L)).collect().toMap
      val expected =
        Map(
          1L -> Map((1L, 1.0)),
          2L -> Map((1L, 0.5)),
          3L -> Map((1L, 0.5)),
          4L -> Map((1L, 0.5)),
          5L -> Map((1L, 0.5)),
          6L -> Map((1L, 1.0)),
          7L -> Map((1L, 0.5)),
          8L -> Map((1L, 0.5)))
      assert(compareTwoMaps(expected, res))
    }
  }
}
