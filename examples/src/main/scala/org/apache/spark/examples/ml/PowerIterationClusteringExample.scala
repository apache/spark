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
package org.apache.spark.examples.ml

import org.apache.log4j.{Level, Logger}

// $example on$
import org.apache.spark.ml.clustering.PowerIterationClustering
// $example off$
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


 /**
  * An example demonstrating power iteration clustering.
  * Run with
  * {{{
  * bin/run-example ml.PowerIterationClusteringExample
  * }}}
  */

object PowerIterationClusteringExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    // $example on$

    // Generates data.
    val radius1 = 1.0
    val numPoints1 = 5
    val radius2 = 4.0
    val numPoints2 = 20

    val dataset = generatePICData(spark, radius1, radius2, numPoints1, numPoints2)

    // Trains a PIC model.
    val model = new PowerIterationClustering().
      setK(2).
      setInitMode("degree").
      setMaxIter(20)

    val prediction = model.transform(dataset).select("id", "prediction")

    //  Shows the result.
    //  println("Cluster Assignment: ")
    val result = prediction.collect().map {
      row => (row(1), row(0))
    }.groupBy(_._1).mapValues(_.map(_._2))

    result.foreach {
      case (cluster, points) => println(s"$cluster -> [${points.mkString(",")}]")
    }
    // $example off$

    spark.stop()
  }

  def generatePICData(spark: SparkSession,
                      r1: Double,
                      r2: Double,
                      n1: Int,
                      n2: Int): DataFrame = {
    val n = n1 + n2
    val points = genCircle(r1, n1) ++ genCircle(r2, n2)

    val rows = for (i <- 0 until n) yield {
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
}

// scalastyle:on println
