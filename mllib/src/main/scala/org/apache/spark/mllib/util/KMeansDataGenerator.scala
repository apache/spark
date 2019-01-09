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

import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Generate test data for KMeans. This class first chooses k cluster centers
 * from a d-dimensional Gaussian distribution scaled by factor r and then creates a Gaussian
 * cluster with scale 1 around each center.
 */
@DeveloperApi
@Since("0.8.0")
object KMeansDataGenerator {

  /**
   * Generate an RDD containing test data for KMeans.
   *
   * @param sc SparkContext to use for creating the RDD
   * @param numPoints Number of points that will be contained in the RDD
   * @param k Number of clusters
   * @param d Number of dimensions
   * @param r Scaling factor for the distribution of the initial centers
   * @param numPartitions Number of partitions of the generated RDD; default 2
   */
  @Since("0.8.0")
  def generateKMeansRDD(
      sc: SparkContext,
      numPoints: Int,
      k: Int,
      d: Int,
      r: Double,
      numPartitions: Int = 2)
    : RDD[Array[Double]] =
  {
    // First, generate some centers
    val rand = new Random(42)
    val centers = Array.fill(k)(Array.fill(d)(rand.nextGaussian() * r))
    // Then generate points around each center
    sc.parallelize(0 until numPoints, numPartitions).map { idx =>
      val center = centers(idx % k)
      val rand2 = new Random(42 + idx)
      Array.tabulate(d)(i => center(i) + rand2.nextGaussian())
    }
  }

  @Since("0.8.0")
  def main(args: Array[String]) {
    if (args.length < 6) {
      // scalastyle:off println
      println("Usage: KMeansGenerator " +
        "<master> <output_dir> <num_points> <k> <d> <r> [<num_partitions>]")
      // scalastyle:on println
      System.exit(1)
    }

    val sparkMaster = args(0)
    val outputPath = args(1)
    val numPoints = args(2).toInt
    val k = args(3).toInt
    val d = args(4).toInt
    val r = args(5).toDouble
    val parts = if (args.length >= 7) args(6).toInt else 2

    val sc = new SparkContext(sparkMaster, "KMeansDataGenerator")
    val data = generateKMeansRDD(sc, numPoints, k, d, r, parts)
    data.map(_.mkString(" ")).saveAsTextFile(outputPath)

    sc.stop()
    System.exit(0)
  }
}

