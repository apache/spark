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
import scala.math.exp
import org.apache.spark.util.Vector
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.InputFormatInfo

/**
 * Logistic regression based classification.
 */
object SparkHdfsLR {
  val D = 10   // Numer of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def parsePoint(line: String): DataPoint = {
    //val nums = line.split(' ').map(_.toDouble)
    //return DataPoint(new Vector(nums.slice(1, D+1)), nums(0))
    val tok = new java.util.StringTokenizer(line, " ")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new Vector(x), y)
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SparkHdfsLR <master> <file> <iters>")
      System.exit(1)
    }
    val inputPath = args(1)
    val conf = SparkHadoopUtil.get.newConfiguration()
    val sc = new SparkContext(args(0), "SparkHdfsLR",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass), Map(), 
      InputFormatInfo.computePreferredLocations(
        Seq(new InputFormatInfo(conf, classOf[org.apache.hadoop.mapred.TextInputFormat], inputPath))
      ))
    val lines = sc.textFile(inputPath)
    val points = lines.map(parsePoint _).cache()
    val ITERATIONS = args(2).toInt

    // Initialize w to a random value
    var w = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y * p.x
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    System.exit(0)
  }
}
