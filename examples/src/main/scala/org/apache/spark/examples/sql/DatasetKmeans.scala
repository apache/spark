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

  case class Point(x: Double, y: Double) {
    def + (other: Point): Point = Point(x + other.x, y + other.y)

    def / (d: Double): Point = Point(x / d, y / d)

    def distance(other: Point): Double =
      (x - other.x) * (x - other.x) + (y - other.y) * (y - other.y)
  }

  private def closestPoint(p: Point, centers: Array[Point]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = p.distance(centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

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

    val points = sc.textFile(args(0)).map { line =>
      val Array(x, y) = line.split(' ').map(_.toDouble)
      Point(x, y)
    }.toDS()
    points.rdd.cache()

    val K = args(1).toInt
    val convergeDist = args(2).toDouble

    val kPoints = points.rdd.takeSample(withReplacement = false, K, 42)

    var tempDist = 1.0

    while (tempDist > convergeDist) {
      val closest = points.map(p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.groupBy(_._1).mapGroups {
        case (key, data) =>
          val agged = data.map(_._2).reduce {
            case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2)
          }
          Iterator(key -> agged)
      }

      val newPoints = pointStats.map {
        case (centroid, (ptSum, numPts)) => (centroid, ptSum / numPts)
      }.collect().toMap

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += kPoints(i).distance(newPoints(i))
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
