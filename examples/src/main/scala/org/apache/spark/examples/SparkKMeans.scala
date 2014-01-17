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
import org.apache.spark.SparkContext
import org.apache.spark.util.Vector
import org.apache.spark.SparkContext._

/**
 * K-means clustering.
 */
object SparkKMeans {
  val R = 1000     // Scaling factor
  val rand = new Random(42)
    
  def parseVector(line: String): Vector = {
    new Vector(line.split(' ').map(_.toDouble))
  }
  
  def closestPoint(p: Vector, centers: Array[Vector]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity
  
    for (i <- 0 until centers.length) {
      val tempDist = p.squaredDist(centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
  
    bestIndex
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
        System.err.println("Usage: SparkLocalKMeans <master> <file> <k> <convergeDist>")
        System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkLocalKMeans",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val lines = sc.textFile(args(1))
    val data = lines.map(parseVector _).cache()
    val K = args(2).toInt
    val convergeDist = args(3).toDouble
  
    val kPoints = data.takeSample(withReplacement = false, K, 42).toArray
    var tempDist = 1.0

    while(tempDist > convergeDist) {
      val closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))
      
      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}
      
      val newPoints = pointStats.map {pair => (pair._1, pair._2._1 / pair._2._2)}.collectAsMap()
      
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += kPoints(i).squaredDist(newPoints(i))
      }
      
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration (delta = " + tempDist + ")")
    }

    println("Final centers:")
    kPoints.foreach(println)
    System.exit(0)
  }
}
