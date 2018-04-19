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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._


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
    assert(pic.getPredictionCol === "prediction")
    assert(pic.getIdCol === "id")
    assert(pic.getNeighborsCol === "neighbors")
    assert(pic.getSimilaritiesCol === "similarities")
  }

  test("parameter validation") {
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setK(1)
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setInitMode("no_such_a_mode")
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setIdCol("")
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setNeighborsCol("")
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setSimilaritiesCol("")
    }
  }

  test("power iteration clustering") {
    val n = n1 + n2

    val model = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(40)
    val result = model.transform(data)

    val predictions = Array.fill(2)(mutable.Set.empty[Long])
    result.select("id", "prediction").collect().foreach {
      case Row(id: Long, cluster: Integer) => predictions(cluster) += id
    }
    assert(predictions.toSet == Set((1 until n1).toSet, (n1 until n).toSet))

    val result2 = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(10)
      .setInitMode("degree")
      .transform(data)
    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    result2.select("id", "prediction").collect().foreach {
      case Row(id: Long, cluster: Integer) => predictions2(cluster) += id
    }
    assert(predictions2.toSet == Set((1 until n1).toSet, (n1 until n).toSet))
  }

  test("supported input types") {
    val model = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(1)

    def runTest(idType: DataType, neighborType: DataType, similarityType: DataType): Unit = {
      val typedData = data.select(
        col("id").cast(idType).alias("id"),
        col("neighbors").cast(ArrayType(neighborType, containsNull = false)).alias("neighbors"),
        col("similarities").cast(ArrayType(similarityType, containsNull = false))
          .alias("similarities")
      )
      model.transform(typedData).collect()
    }

    for (idType <- Seq(IntegerType, LongType)) {
      runTest(idType, LongType, DoubleType)
    }
    for (neighborType <- Seq(IntegerType, LongType)) {
      runTest(LongType, neighborType, DoubleType)
    }
    for (similarityType <- Seq(FloatType, DoubleType)) {
      runTest(LongType, LongType, similarityType)
    }
  }

  test("invalid input: wrong types") {
    val model = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(1)
    intercept[IllegalArgumentException] {
      val typedData = data.select(
        col("id").cast(DoubleType).alias("id"),
        col("neighbors"),
        col("similarities")
      )
      model.transform(typedData)
    }
    intercept[IllegalArgumentException] {
      val typedData = data.select(
        col("id"),
        col("neighbors").cast(ArrayType(DoubleType, containsNull = false)).alias("neighbors"),
        col("similarities")
      )
      model.transform(typedData)
    }
    intercept[IllegalArgumentException] {
      val typedData = data.select(
        col("id"),
        col("neighbors"),
        col("neighbors").alias("similarities")
      )
      model.transform(typedData)
    }
  }

  test("invalid input: negative similarity") {
    val model = new PowerIterationClustering()
      .setMaxIter(1)
    val badData = spark.createDataFrame(Seq(
      (0, Array(1), Array(-1.0)),
      (1, Array(0), Array(-1.0))
    )).toDF("id", "neighbors", "similarities")
    val msg = intercept[SparkException] {
      model.transform(badData)
    }.getCause.getMessage
    assert(msg.contains("Similarity must be nonnegative"))
  }

  test("invalid input: mismatched lengths for neighbor and similarity arrays") {
    val model = new PowerIterationClustering()
      .setMaxIter(1)
    val badData = spark.createDataFrame(Seq(
      (0, Array(1), Array(0.5)),
      (1, Array(0, 2), Array(0.5)),
      (2, Array(1), Array(0.5))
    )).toDF("id", "neighbors", "similarities")
    val msg = intercept[SparkException] {
      model.transform(badData)
    }.getCause.getMessage
    assert(msg.contains("The length of the neighbor ID list must be equal to the the length of " +
      "the neighbor similarity list."))
    assert(msg.contains(s"Row for ID ${model.getIdCol}=1"))
  }

  test("read/write") {
    val t = new PowerIterationClustering()
      .setK(4)
      .setMaxIter(100)
      .setInitMode("degree")
      .setIdCol("test_id")
      .setNeighborsCol("myNeighborsCol")
      .setSimilaritiesCol("mySimilaritiesCol")
      .setPredictionCol("test_prediction")
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

  def generatePICData(
      spark: SparkSession,
      r1: Double,
      r2: Double,
      n1: Int,
      n2: Int): DataFrame = {
    // Generate two circles following the example in the PIC paper.
    val n = n1 + n2
    val points = genCircle(r1, n1) ++ genCircle(r2, n2)

    val rows = for (i <- 1 until n) yield {
      val neighbors = for (j <- 0 until i) yield {
        j.toLong
      }
      val similarities = for (j <- 0 until i) yield {
        sim(points(i), points(j))
      }
      (i.toLong, neighbors.toArray, similarities.toArray)
    }

    spark.createDataFrame(rows).toDF("id", "neighbors", "similarities")
  }

}
