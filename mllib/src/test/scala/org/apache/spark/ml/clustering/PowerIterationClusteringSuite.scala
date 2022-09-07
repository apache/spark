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
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._


class PowerIterationClusteringSuite extends SparkFunSuite
  with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Dataset[_] = _
  final val r1 = 1.0
  final val n1 = 80
  final val r2 = 4.0
  final val n2 = 80

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = PowerIterationClusteringSuite.generatePICData(spark, r1, r2, n1, n2)
  }

  test("default parameters") {
    val pic = new PowerIterationClustering()

    assert(pic.getK === 2)
    assert(pic.getMaxIter === 20)
    assert(pic.getInitMode === "random")
    assert(pic.getSrcCol === "src")
    assert(pic.getDstCol === "dst")
    assert(!pic.isDefined(pic.weightCol))
  }

  test("parameter validation") {
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setK(1)
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setInitMode("no_such_a_mode")
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setSrcCol("")
    }
    intercept[IllegalArgumentException] {
      new PowerIterationClustering().setDstCol("")
    }
  }

  test("power iteration clustering") {
    val n = n1 + n2

    val assignments = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(40)
      .setWeightCol("weight")
      .assignClusters(data)
      .select("id", "cluster")
      .as[(Long, Int)]
      .collect()

    val predictions = Array.fill(2)(mutable.Set.empty[Long])
    assignments.foreach {
      case (id, cluster) => predictions(cluster) += id
    }
    assert(predictions.toSet === Set((0 until n1).toSet, (n1 until n).toSet))

    val assignments2 = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(10)
      .setInitMode("degree")
      .setWeightCol("weight")
      .assignClusters(data)
      .select("id", "cluster")
      .as[(Long, Int)]
      .collect()

    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    assignments2.foreach {
      case (id, cluster) => predictions2(cluster) += id
    }
    assert(predictions2.toSet === Set((0 until n1).toSet, (n1 until n).toSet))
  }

  test("supported input types") {
    val pic = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(1)
      .setWeightCol("weight")

    def runTest(srcType: DataType, dstType: DataType, weightType: DataType): Unit = {
      val typedData = data.select(
        col("src").cast(srcType).alias("src"),
        col("dst").cast(dstType).alias("dst"),
        col("weight").cast(weightType).alias("weight")
      )
      pic.assignClusters(typedData).collect()
    }

    for (srcType <- Seq(IntegerType, LongType)) {
      runTest(srcType, LongType, DoubleType)
    }
    for (dstType <- Seq(IntegerType, LongType)) {
      runTest(LongType, dstType, DoubleType)
    }
    for (weightType <- Seq(FloatType, DoubleType)) {
      runTest(LongType, LongType, weightType)
    }
  }

  test("invalid input: negative similarity") {
    val pic = new PowerIterationClustering()
      .setMaxIter(1)
      .setWeightCol("weight")
    val badData = spark.createDataFrame(Seq(
      (0, 1, -1.0),
      (1, 0, -1.0)
    )).toDF("src", "dst", "weight")
    val msg = intercept[Exception] {
      pic.assignClusters(badData)
    }.getMessage
    assert(msg.contains("Weights MUST NOT be Negative or Infinity"))
  }

  test("check for invalid input types of weight") {
    val invalidWeightData = spark.createDataFrame(Seq(
      (0L, 1L, "a"),
      (2L, 3L, "b")
    )).toDF("src", "dst", "weight")

    val msg = intercept[IllegalArgumentException] {
      new PowerIterationClustering()
        .setWeightCol("weight")
        .assignClusters(invalidWeightData)
    }.getMessage
    assert(msg.contains("requirement failed: Column weight must be of type numeric" +
      " but was actually of type string."))
  }

  test("test default weight") {
    val dataWithoutWeight = data.sample(0.5, 1L).select("src", "dst")

    val assignments = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(40)
      .assignClusters(dataWithoutWeight)
    val localAssignments = assignments
      .select("id", "cluster")
      .as[(Long, Int)].collect().toSet

    val dataWithWeightOne = dataWithoutWeight.withColumn("weight", lit(1.0))

    val assignments2 = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(40)
      .assignClusters(dataWithWeightOne)
    val localAssignments2 = assignments2
      .select("id", "cluster")
      .as[(Long, Int)].collect().toSet

    assert(localAssignments === localAssignments2)
  }

  test("power iteration clustering gives incorrect results due to failed to converge") {
    /*
         Graph:
            1
           /
          /
         0      2 -- 3
     */
    val data1 = spark.createDataFrame(Seq(
      (0, 1),
      (2, 3)
    )).toDF("src", "dst")

    val assignments1 = new PowerIterationClustering()
      .setInitMode("random")
      .setK(2)
      .assignClusters(data1)
      .select("id", "cluster")
      .as[(Long, Int)]
      .collect()
    val predictions1 = Array.fill(2)(mutable.Set.empty[Long])
    assignments1.foreach {
      case (id, cluster) => predictions1(cluster) += id
    }
    assert(Set(predictions1(0).size, predictions1(1).size) !== Set(2, 2))


    /*
         Graph:
            1
           /
          /
         0 - - 2     3 -- 4
     */
    val data2 = spark.createDataFrame(Seq(
      (0, 1),
      (0, 2),
      (3, 4)
    )).toDF("src", "dst").repartition(1)

    val assignments2 = new PowerIterationClustering()
      .setInitMode("random")
      .setK(2)
      .assignClusters(data2)
      .select("id", "cluster")
      .as[(Long, Int)]
      .collect()
    val predictions2 = Array.fill(2)(mutable.Set.empty[Long])
    assignments2.foreach {
      case (id, cluster) => predictions2(cluster) += id
    }
    assert(Set(predictions2(0).size, predictions2(1).size) !== Set(2, 3))


    val assignments3 = new PowerIterationClustering()
      .setInitMode("degree")
      .setK(2)
      .assignClusters(data2)
      .select("id", "cluster")
      .as[(Long, Int)]
      .collect()
    val predictions3 = Array.fill(2)(mutable.Set.empty[Long])
    assignments3.foreach {
      case (id, cluster) => predictions3(cluster) += id
    }
    assert(Set(predictions3(0).size, predictions3(1).size) !== Set(2, 3))
  }

  test("read/write") {
    val t = new PowerIterationClustering()
      .setK(4)
      .setMaxIter(100)
      .setInitMode("degree")
      .setSrcCol("src1")
      .setDstCol("dst1")
      .setWeightCol("weight")
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

    val rows = (for (i <- 1 until n) yield {
      for (j <- 0 until i) yield {
        (i.toLong, j.toLong, sim(points(i), points(j)))
      }
    }).flatMap(_.iterator)

    spark.createDataFrame(rows).toDF("src", "dst", "weight")
  }

}
