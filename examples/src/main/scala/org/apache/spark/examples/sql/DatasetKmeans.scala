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
package org.apache.spark.examples.sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Dataset. For more conventional use,
 * please refer to org.apache.spark.mllib.clustering.KMeans
 */
object DatasetKmeans {

  // TODO: now we only support 2-dimension vector
  type Vector = (Double, Double)

  private def parseVector(line: String): Vector = {
    val Array(d1, d2) = line.split(' ').map(_.toDouble)
    d1 -> d2
  }

  private def closestPoint(p: Vector, centers: Array[Vector]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  private def squaredDistance(p1: Vector, p2: Vector): Double = {
    (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2)
  }

  private def add(p1: Vector, p2: Vector): Vector = (p1._1 + p2._1, p1._2 + p2._2)

  private def scale(p: Vector, scale: Double): Vector = (p._1 * scale, p._2 * scale)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: DatasetKmeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("DatasetKmeans")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val lines = sc.textFile(args(0))
    val data = lines.map(parseVector _)
    val K = args(1).toInt
    val convergeDist = args(2).toDouble

    val kPoints = data.takeSample(withReplacement = false, K, 42)
    var tempDist = 1.0

    val ds = data.toDS()

    while (tempDist > convergeDist) {
      val closest = ds.map(p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.groupBy(_._1).mapGroups {
        case (key, data) =>
          val agged = data.map(_._2).reduce {
            case ((p1, c1), (p2, c2)) => add(p1, p2) -> (c1 + c2)
          }
          Iterator(key -> agged)
      }

      val newPoints = pointStats.map {
        case (key, (point, count)) => (key, scale(point, 1.0 / count))
      }.collect().toMap

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration (delta = " + tempDist + ")")
    }

    println("Final centers:")
    kPoints.foreach(println)
    sc.stop()
  }
}

// scalastyle:on println

