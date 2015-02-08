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

package org.apache.spark.mllib.clustering

import scala.collection.mutable

import org.scalatest.FunSuite

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class PowerIterationClusteringSuite extends FunSuite with MLlibTestSparkContext {

  import org.apache.spark.mllib.clustering.PowerIterationClustering._

  test("power iteration clustering") {
    /*
     We use the following graph to test PIC. All edges are assigned similarity 1.0 except 0.1 for
     edge (3, 4).

     15-14 -13 -12
     |           |
     4 . 3 - 2  11
     |   | x |   |
     5   0 - 1  10
     |           |
     6 - 7 - 8 - 9
     */

    val similarities = Seq[(Long, Long, Double)]((0, 1, 1.0), (0, 2, 1.0), (0, 3, 1.0), (1, 2, 1.0),
      (1, 3, 1.0), (2, 3, 1.0), (3, 4, 0.1), // (3, 4) is a weak edge
      (4, 5, 1.0), (4, 15, 1.0), (5, 6, 1.0), (6, 7, 1.0), (7, 8, 1.0), (8, 9, 1.0), (9, 10, 1.0),
      (10, 11, 1.0), (11, 12, 1.0), (12, 13, 1.0), (13, 14, 1.0), (14, 15, 1.0))
    val model = new PowerIterationClustering()
      .setK(2)
      .run(sc.parallelize(similarities, 2))
    val predictions = Array.fill(2)(mutable.Set.empty[Long])
    model.assignments.collect().foreach { case (i, c) =>
        predictions(c) += i
    }
    assert(predictions.toSet == Set((0 to 3).toSet, (4 to 15).toSet))
 
    val model2 = new PowerIterationClustering()
      .setK(2)
      .setInitializationMode("degree")
      .run(sc.parallelize(similarities, 2))
    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    model2.assignments.collect().foreach { case (i, c) =>
        predictions2(c) += i
    }
    assert(predictions2.toSet == Set((0 to 3).toSet, (4 to 15).toSet))
  }

  test("normalize and powerIter") {
    /*
     Test normalize() with the following graph:

     0 - 3
     | \ |
     1 - 2

     The affinity matrix (A) is

     0 1 1 1
     1 0 1 0
     1 1 0 1
     1 0 1 0

     D is diag(3, 2, 3, 2) and hence W is

       0 1/3 1/3 1/3
     1/2   0 1/2   0
     1/3 1/3   0 1/3
     1/2   0 1/2   0
     */
    val similarities = Seq[(Long, Long, Double)](
      (0, 1, 1.0), (0, 2, 1.0), (0, 3, 1.0), (1, 2, 1.0), (2, 3, 1.0))
    val expected = Array(
      Array(0.0,     1.0/3.0, 1.0/3.0, 1.0/3.0),
      Array(1.0/2.0,     0.0, 1.0/2.0,     0.0),
      Array(1.0/3.0, 1.0/3.0,     0.0, 1.0/3.0),
      Array(1.0/2.0,     0.0, 1.0/2.0,     0.0))
    val w = normalize(sc.parallelize(similarities, 2))
    w.edges.collect().foreach { case Edge(i, j, x) =>
      assert(x ~== expected(i.toInt)(j.toInt) absTol 1e-14)
    }
    val v0 = sc.parallelize(Seq[(Long, Double)]((0, 0.1), (1, 0.2), (2, 0.3), (3, 0.4)), 2)
    val w0 = Graph(v0, w.edges)
    val v1 = powerIter(w0, maxIterations = 1).collect()
    val u = Array(0.3, 0.2, 0.7/3.0, 0.2)
    val norm = u.sum
    val u1 = u.map(x => x / norm)
    v1.foreach { case (i, x) =>
      assert(x ~== u1(i.toInt) absTol 1e-14)
    }
  }
}
