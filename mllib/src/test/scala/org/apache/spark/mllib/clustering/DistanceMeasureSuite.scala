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

package org.apache.spark.mllib.clustering

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class DistanceMeasureSuite extends SparkFunSuite with MLlibTestSparkContext {

  private val seed = 42
  private val k = 10
  private val dim = 8

  private var centers: Array[VectorWithNorm] = _

  private var data: Array[VectorWithNorm] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val rng = new Random(seed)

    centers = Array.tabulate(k) { i =>
      val values = Array.fill(dim)(rng.nextGaussian)
      new VectorWithNorm(Vectors.dense(values))
    }

    data = Array.tabulate(1000) { i =>
      val values = Array.fill(dim)(rng.nextGaussian)
      new VectorWithNorm(Vectors.dense(values))
    }
  }

  test("predict with statistics") {
    Seq(DistanceMeasure.COSINE, DistanceMeasure.EUCLIDEAN).foreach { distanceMeasure =>
      val distance = DistanceMeasure.decodeFromString(distanceMeasure)
      val statistics = distance.computeStatistics(centers)
      data.foreach { point =>
        val (index1, cost1) = distance.findClosest(centers, point)
        val (index2, cost2) = distance.findClosest(centers, statistics, point)
        assert(index1 == index2)
        assert(cost1 ~== cost2 relTol 1E-10)
      }
    }
  }

  test("compute statistics distributedly") {
    Seq(DistanceMeasure.COSINE, DistanceMeasure.EUCLIDEAN).foreach { distanceMeasure =>
      val distance = DistanceMeasure.decodeFromString(distanceMeasure)
      val statistics1 = distance.computeStatistics(centers)
      val sc = spark.sparkContext
      val bcCenters = sc.broadcast(centers)
      val statistics2 = distance.computeStatisticsDistributedly(sc, bcCenters)
      bcCenters.destroy()
      assert(Vectors.dense(statistics1) ~== Vectors.dense(statistics2) relTol 1E-10)
    }
  }
}
