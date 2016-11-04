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
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class PowerIterationClusteringSuite extends SparkFunSuite
  with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var data: Dataset[_] = _
  final val r1 = 1.0
  final val n1 = 10
  final val r2 = 4.0
  final val n2 = 40

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = PowerIterationClusteringSuite.generatePICData(spark, r1, r2, n1, n2)
  }

  test("default parameters") {
    val pic = new PowerIterationClustering()

    assert(pic.getK === 2)
    assert(pic.getMaxIter === 20)
    assert(pic.getInitMode === "random")
    assert(pic.getFeaturesCol === "features")
    assert(pic.getPredictionCol === "prediction")
    assert(pic.getLabelCol === "id")
  }

  test("set parameters") {
    val pic = new PowerIterationClustering()
      .setK(9)
      .setMaxIter(33)
      .setInitMode("degree")
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setLabelCol("test_id")

    assert(pic.getK === 9)
    assert(pic.getMaxIter === 33)
    assert(pic.getInitMode === "degree")
    assert(pic.getFeaturesCol === "test_feature")
    assert(pic.getPredictionCol === "test_prediction")
    assert(pic.getLabelCol === "test_id")
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
    val n = n1 + n2

    val result = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(40)
      .transform(data)

    val predictions = Array.fill(2)(mutable.Set.empty[Long])
    result.select("id", "prediction").collect().foreach {
      case Row(id: Long, cluster: Integer) => predictions(cluster) += id
    }
    assert(predictions.toSet == Set((0 until n1).toSet, (n1 until n).toSet))

    val result2 = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(10)
      .setInitMode("degree")
      .transform(data)
    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    result2.select("id", "prediction").collect().foreach {
      case Row(id: Long, cluster: Integer) => predictions2(cluster) += id
    }
    assert(predictions2.toSet == Set((0 until n1).toSet, (n1 until n).toSet))

    val expectedColumns = Array("id", "prediction")
    expectedColumns.foreach { column =>
      assert(result2.columns.contains(column))
    }
  }

  test("read/write") {
    val t = new PowerIterationClustering()
      .setK(4)
      .setMaxIter(100)
      .setInitMode("degree")
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setLabelCol("test_id")
    testDefaultReadWrite(t)
  }
}

object PowerIterationClusteringSuite {

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

  def generatePICData(spark: SparkSession, r1: Double, r2: Double,
    n1: Int, n2: Int): DataFrame = {
    // Generate two circles following the example in the PIC paper.
    val n = n1 + n2
    val points = genCircle(r1, n1) ++ genCircle(r2, n2)
    val similarities = for (i <- 1 until n; j <- 0 until i) yield {
      (i.toLong, j.toLong, sim(points(i), points(j)))
    }
    val sc = spark.sparkContext
    val rdd = sc.parallelize(similarities)
      .map{case (i: Long, j: Long, sim: Double) => Vectors.dense(Array(i, j, sim))}
      .map(v => TestRow(v))
    spark.createDataFrame(rdd)
  }
}
