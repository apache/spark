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

import org.apache.spark.annotation.Experimental

import scala.reflect.ClassTag

import org.apache.spark.mllib.base.{ FP, PointOps }
import org.apache.spark.mllib.clustering.metrics.FastEuclideanOps
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vector

class KMeans private (
  private var k: Int,
  private var maxIterations: Int,
  private var runs: Int,
  private var initializationMode: String,
  private var initializationSteps: Int,
  private var epsilon: Double) extends Serializable with Logging {

  def this() = this(2, 20, 1, KMeans.K_MEANS_PARALLEL, 5, 1e-4)

  /** Set the number of clusters to create (k). Default: 2. */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /** Set maximum number of iterations to run. Default: 20. */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   */
  def setInitializationMode(initializationMode: String): this.type = {
    if (initializationMode != KMeans.RANDOM && initializationMode != KMeans.K_MEANS_PARALLEL) {
      throw new IllegalArgumentException("Invalid initialization mode: " + initializationMode)
    }
    this.initializationMode = initializationMode
    this
  }

  /**
   * :: Experimental ::
   * Set the number of runs of the algorithm to execute in parallel. We initialize the algorithm
   * this many times with random starting conditions (configured by the initialization mode), then
   * return the best clustering found over any run. Default: 1.
   */
  @Experimental
  def setRuns(runs: Int): this.type = {
    if (runs <= 0) {
      throw new IllegalArgumentException("Number of runs must be positive")
    }
    this.runs = runs
    this
  }

  /**
   * Set the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Default: 5.
   */
  def setInitializationSteps(initializationSteps: Int): this.type = {
    if (initializationSteps <= 0) {
      throw new IllegalArgumentException("Number of initialization steps must be positive")
    }
    this.initializationSteps = initializationSteps
    this
  }

  /**
   * Set the distance threshold within which we've consider centers to have converged.
   * If all centers move less than this Euclidean distance, we stop iterating one run.
   */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  def run(data: RDD[Vector]): KMeansModel = {
    new KMeansModel(KMeans.doTrain(new FastEuclideanOps)(data, k, maxIterations, runs,
      initializationMode, initializationSteps)._2)
  }
}

object KMeans extends Logging {
  // Initialization mode names
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(data: RDD[Vector], k: Int, maxIterations: Int, runs: Int, mode: String): KMeansModel =
    new KMeansModel(doTrain(new FastEuclideanOps)(data, k, maxIterations, runs, mode)._2)

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(data: RDD[Vector], k: Int, maxIterations: Int): KMeansModel =
    new KMeansModel(doTrain(new FastEuclideanOps)(data, k, maxIterations)._2)

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(data: RDD[Vector], k: Int, maxIterations: Int, runs: Int): KMeansModel =
    new KMeansModel(doTrain(new FastEuclideanOps)(data, k, maxIterations, runs)._2)

  private def doTrain[P <: FP, C <: FP](
    pointOps: PointOps[P, C])(
    raw: RDD[Vector],
    k: Int = 2,
    maxIterations: Int = 20,
    runs: Int = 1,
    initializationMode: String = K_MEANS_PARALLEL,
    initializationSteps: Int = 5,
    epsilon: Double = 1e-4)(implicit ctag: ClassTag[C], ptag: ClassTag[P]): (Double, GeneralizedKMeansModel[P, C]) = {

    val initializer = if (initializationMode == RANDOM) {
      new KMeansRandom(pointOps, k, runs)
    } else {
      new KMeansParallel(pointOps, k, runs, initializationSteps, 1)
    }
    val data = (raw.map(vals => pointOps.vectorToPoint(vals))).cache()
    val centers = initializer.init(data, 0)
    new MultiKMeans(pointOps, maxIterations).cluster(data, centers)
  }
}
