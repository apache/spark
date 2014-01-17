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

package org.apache.spark.examples

import java.util.Random
import org.apache.spark.util.Vector
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

/**
 * K-means clustering.
 */
object LocalKMeans {
  val N = 1000
  val R = 1000    // Scaling factor
  val D = 10
  val K = 10
  val convergeDist = 0.001
  val rand = new Random(42)

  def generateData = {
    def generatePoint(i: Int) = {
      Vector(D, _ => rand.nextDouble * R)
    }
    Array.tabulate(N)(generatePoint)
  }

  def closestPoint(p: Vector, centers: HashMap[Int, Vector]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 1 to centers.size) {
      val vCurr = centers.get(i).get
      val tempDist = p.squaredDist(vCurr)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def main(args: Array[String]) {
    val data = generateData
    var points = new HashSet[Vector]
    var kPoints = new HashMap[Int, Vector]
    var tempDist = 1.0

    while (points.size < K) {
      points.add(data(rand.nextInt(N)))
    }

    val iter = points.iterator
    for (i <- 1 to points.size) {
      kPoints.put(i, iter.next())
    }

    println("Initial centers: " + kPoints)

    while(tempDist > convergeDist) {
      var closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))

      var mappings = closest.groupBy[Int] (x => x._1)

      var pointStats = mappings.map(pair => pair._2.reduceLeft [(Int, (Vector, Int))] {case ((id1, (x1, y1)), (id2, (x2, y2))) => (id1, (x1 + x2, y1+y2))})

      var newPoints = pointStats.map {mapping => (mapping._1, mapping._2._1/mapping._2._2)}

      tempDist = 0.0
      for (mapping <- newPoints) {
        tempDist += kPoints.get(mapping._1).get.squaredDist(mapping._2)
      }

      for (newP <- newPoints) {
        kPoints.put(newP._1, newP._2)
      }
    }

    println("Final centers: " + kPoints)
  }
}
