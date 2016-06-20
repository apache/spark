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

package org.apache.spark.ml.clustering

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext

class PowerIterationClusteringSuite extends SparkFunSuite
  with MLlibTestSparkContext with DefaultReadWriteTest {

  import org.apache.spark.ml.clustering.PowerIterationClustering._

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

  test("default parameters") {
    val pic = new PowerIterationClustering()

    assert(pic.getK === 2)
    assert(pic.getMaxIter === 20)
    assert(pic.getInitMode === "random")
    assert(pic.getFeaturesCol === "features")
    assert(pic.getPredictionCol === "prediction")
  }

  test("set parameters") {
    val pic = new PowerIterationClustering()
      .setK(9)
      .setMaxIter(33)
      .setInitMode("degree")
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")

    assert(pic.getK === 9)
    assert(pic.getMaxIter === 33)
    assert(pic.getInitMode === "degree")
    assert(pic.getFeaturesCol === "test_feature")
    assert(pic.getPredictionCol === "test_prediction")
  }

  test("parameters validation") {
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setK(1)
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setInitMode("no_such_a_mode")
    }
  }

  test("power iteration clustering") {
    // Generate two circles following the example in the PIC paper.
    val r1 = 1.0
    val n1 = 10
    val r2 = 4.0
    val n2 = 40
    val n = n1 + n2
    val points = genCircle(r1, n1) ++ genCircle(r2, n2)
    val similarities = for (i <- 1 until n; j <- 0 until i) yield {
      (i.toLong, j.toLong, sim(points(i), points(j)))
    }

    val sc = spark.sparkContext
    val rdd = sc.parallelize(similarities)
      .map{case (i: Long, j: Long, sim: Double) => Vectors.dense(Array(i, j, sim))}
      .map(v => TestRow(v))
    val data = spark.createDataFrame(rdd)

    val model = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(40)
      .fit(data)
    val predictions = Array.fill(2)(mutable.Set.empty[Long])
    model.assignments.collect().foreach { a =>
      predictions(a.cluster) += a.id
    }
    assert(predictions.toSet == Set((0 until n1).toSet, (n1 until n).toSet))

    val model2 = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(10)
      .setInitMode("degree")
      .fit(data)
    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    model2.assignments.collect().foreach { a =>
      predictions2(a.cluster) += a.id
    }
    assert(predictions2.toSet == Set((0 until n1).toSet, (n1 until n).toSet))
  }
}
