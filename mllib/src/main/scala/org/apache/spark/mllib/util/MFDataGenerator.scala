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

package org.apache.spark.mllib.util

import java.{util => ju}

import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{BLAS, DenseMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ArrayImplicits._

/**
 * Generate RDD(s) containing data for Matrix Factorization.
 *
 * This method samples training entries according to the oversampling factor
 * 'trainSampFact', which is a multiplicative factor of the number of
 * degrees of freedom of the matrix: rank*(m+n-rank).
 *
 * It optionally samples entries for a testing matrix using
 * 'testSampFact', the percentage of the number of training entries
 * to use for testing.
 *
 * This method takes the following inputs:
 *   sparkMaster    (String) The master URL.
 *   outputPath     (String) Directory to save output.
 *   m              (Int) Number of rows in data matrix.
 *   n              (Int) Number of columns in data matrix.
 *   rank           (Int) Underlying rank of data matrix.
 *   trainSampFact  (Double) Oversampling factor.
 *   noise          (Boolean) Whether to add gaussian noise to training data.
 *   sigma          (Double) Standard deviation of added gaussian noise.
 *   test           (Boolean) Whether to create testing RDD.
 *   testSampFact   (Double) Percentage of training data to use as test data.
 */
@Since("0.8.0")
object MFDataGenerator {
  @Since("0.8.0")
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      // scalastyle:off println
      println("Usage: MFDataGenerator " +
        "<master> <outputDir> [m] [n] [rank] [trainSampFact] [noise] [sigma] [test] [testSampFact]")
      // scalastyle:on println
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val m: Int = if (args.length > 2) args(2).toInt else 100
    val n: Int = if (args.length > 3) args(3).toInt else 100
    val rank: Int = if (args.length > 4) args(4).toInt else 10
    val trainSampFact: Double = if (args.length > 5) args(5).toDouble else 1.0
    val noise: Boolean = if (args.length > 6) args(6).toBoolean else false
    val sigma: Double = if (args.length > 7) args(7).toDouble else 0.1
    val test: Boolean = if (args.length > 8) args(8).toBoolean else false
    val testSampFact: Double = if (args.length > 9) args(9).toDouble else 0.1

    val sc = new SparkContext(sparkMaster, "MFDataGenerator")

    val random = new ju.Random(42L)

    val A = DenseMatrix.randn(m, rank, random)
    val B = DenseMatrix.randn(rank, n, random)
    val z = 1 / math.sqrt(rank)
    val fullData = DenseMatrix.zeros(m, n)
    BLAS.gemm(z, A, B, 1.0, fullData)

    val df = rank * (m + n - rank)
    val sampSize = math.min(math.round(trainSampFact * df), math.round(.99 * m * n)).toInt
    val rand = new Random()
    val mn = m * n
    val shuffled = rand.shuffle((0 until mn).toList)

    val omega = shuffled.slice(0, sampSize)
    val ordered = omega.sortWith(_ < _).toArray
    val trainData: RDD[(Int, Int, Double)] = sc.parallelize(ordered.toImmutableArraySeq)
      .map(x => (x % m, x / m, fullData.values(x)))

    // optionally add gaussian noise
    if (noise) {
      trainData.map(x => (x._1, x._2, x._3 + rand.nextGaussian() * sigma))
    }

    trainData.map(x => s"${x._1},${x._2},${x._3}").saveAsTextFile(outputPath)

    // optionally generate testing data
    if (test) {
      val testSampSize = math.min(math.round(sampSize * testSampFact).toInt, mn - sampSize)
      val testOmega = shuffled.slice(sampSize, sampSize + testSampSize)
      val testOrdered = testOmega.sortWith(_ < _).toArray
      val testData: RDD[(Int, Int, Double)] = sc.parallelize(testOrdered.toImmutableArraySeq)
        .map(x => (x % m, x / m, fullData.values(x)))
      testData.map(x => s"${x._1},${x._2},${x._3}").saveAsTextFile(outputPath)
    }

    sc.stop()

  }
}
