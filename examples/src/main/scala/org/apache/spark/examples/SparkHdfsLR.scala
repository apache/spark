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

// scalastyle:off println
package org.apache.spark.examples

import java.util.Random

import scala.math.exp

import breeze.linalg.{DenseVector, Vector}

import org.apache.spark.sql.SparkSession

/**
 * Logistic regression based classification.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.classification.LogisticRegression.
 */
object SparkHdfsLR {
  val D = 10   // Number of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String): DataPoint = {
    val tok = new java.util.StringTokenizer(line, " ")
    val y = tok.nextToken.toDouble
    val x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def showWarning(): Unit = {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use org.apache.spark.ml.classification.LogisticRegression
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: SparkHdfsLR <file> <iters>")
      System.exit(1)
    }

    showWarning()

    val spark = SparkSession
      .builder
      .appName("SparkHdfsLR")
      .getOrCreate()

    val inputPath = args(0)
    val lines = spark.read.textFile(inputPath).rdd

    val points = lines.map(parsePoint).cache()
    val ITERATIONS = args(1).toInt

    // Initialize w to a random value
    val w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println(s"Initial w: $w")

    for (i <- 1 to ITERATIONS) {
      println(s"On iteration $i")
      val gradient = points.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println(s"Final w: $w")
    spark.stop()
  }
}
// scalastyle:on println
