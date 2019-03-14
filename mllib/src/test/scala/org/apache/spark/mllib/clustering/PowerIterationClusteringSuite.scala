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
import scala.util.Random

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils

class PowerIterationClusteringSuite extends SparkFunSuite with MLlibTestSparkContext {

  import org.apache.spark.mllib.clustering.PowerIterationClustering._

  /** Generates a circle of points. */
  private def genCircle(r: Double, n: Int): Array[(Double, Double)] = {
    Array.tabulate(n) { i =>
      val theta = 2.0 * math.Pi * i / n
      (r * math.cos(theta), r * math.sin(theta))
    }
  }

  /** Computes Gaussian similarity. */
  private def sim(x: (Double, Double), y: (Double, Double)): Double = {
    val dist2 = (x._1 - y._1) * (x._1 - y._1) + (x._2 - y._2) * (x._2 - y._2)
    math.exp(-dist2 / 2.0)
  }

  test("power iteration clustering") {
    // Generate two circles following the example in the PIC paper.
    val r1 = 1.0
    val n1 = 10
    val r2 = 4.0
    val n2 = 10
    val n = n1 + n2
    val points = genCircle(r1, n1) ++ genCircle(r2, n2)
    val similarities = for (i <- 1 until n; j <- 0 until i) yield {
      (i.toLong, j.toLong, sim(points(i), points(j)))
    }

    val model = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(40)
      .run(sc.parallelize(similarities, 2))
    val predictions = Array.fill(2)(mutable.Set.empty[Long])
    model.assignments.collect().foreach { a =>
      predictions(a.cluster) += a.id
    }
    assert(predictions.toSet == Set((0 until n1).toSet, (n1 until n).toSet))

    val model2 = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(10)
      .setInitializationMode("degree")
      .run(sc.parallelize(similarities, 2))
    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    model2.assignments.collect().foreach { a =>
      predictions2(a.cluster) += a.id
    }
    assert(predictions2.toSet == Set((0 until n1).toSet, (n1 until n).toSet))
  }

  test("power iteration clustering on graph") {
    // Generate two circles following the example in the PIC paper.
    val r1 = 1.0
    val n1 = 10
    val r2 = 4.0
    val n2 = 10
    val n = n1 + n2
    val points = genCircle(r1, n1) ++ genCircle(r2, n2)
    val similarities = for (i <- 1 until n; j <- 0 until i) yield {
      (i.toLong, j.toLong, sim(points(i), points(j)))
    }

    val edges = similarities.flatMap { case (i, j, s) =>
      Seq(Edge(i, j, s), Edge(j, i, s))
    }
    val graph = Graph.fromEdges(sc.parallelize(edges, 2), 0.0)

    val model = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(40)
      .run(graph)
    val predictions = Array.fill(2)(mutable.Set.empty[Long])
    model.assignments.collect().foreach { a =>
      predictions(a.cluster) += a.id
    }
    assert(predictions.toSet == Set((0 until n1).toSet, (n1 until n).toSet))

    val model2 = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(10)
      .setInitializationMode("degree")
      .run(sc.parallelize(similarities, 2))
    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    model2.assignments.collect().foreach { a =>
      predictions2(a.cluster) += a.id
    }
    assert(predictions2.toSet == Set((0 until n1).toSet, (n1 until n).toSet))
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
    // scalastyle:off
    val expected = Array(
      Array(0.0,     1.0/3.0, 1.0/3.0, 1.0/3.0),
      Array(1.0/2.0,     0.0, 1.0/2.0,     0.0),
      Array(1.0/3.0, 1.0/3.0,     0.0, 1.0/3.0),
      Array(1.0/2.0,     0.0, 1.0/2.0,     0.0))
    // scalastyle:on
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

  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    val model = PowerIterationClusteringSuite.createModel(sc, 3, 10)
    try {
      model.save(sc, path)
      val sameModel = PowerIterationClusteringModel.load(sc, path)
      PowerIterationClusteringSuite.checkEqual(model, sameModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

object PowerIterationClusteringSuite extends SparkFunSuite {
  def createModel(sc: SparkContext, k: Int, nPoints: Int): PowerIterationClusteringModel = {
    val assignments = sc.parallelize(
      (0 until nPoints).map(p => PowerIterationClustering.Assignment(p, Random.nextInt(k))))
    new PowerIterationClusteringModel(k, assignments)
  }

  def checkEqual(a: PowerIterationClusteringModel, b: PowerIterationClusteringModel): Unit = {
    assert(a.k === b.k)

    val aAssignments = a.assignments.map(x => (x.id, x.cluster))
    val bAssignments = b.assignments.map(x => (x.id, x.cluster))
    val unequalElements = aAssignments.join(bAssignments).filter {
      case (id, (c1, c2)) => c1 != c2 }.count()
    assert(unequalElements === 0L)
  }
}
