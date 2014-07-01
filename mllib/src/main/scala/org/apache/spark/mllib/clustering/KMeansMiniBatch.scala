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

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}

import org.apache.spark.annotation.Experimental
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

/**
 * K-means clustering with support for multiple parallel runs, a k-means++ like initialization
 * mode (the k-means|| algorithm by Bahmani et al), and randomly-sampled mini-batches of points
 * in each iteration instead of all points for speed (Web-Scale K-Means Clustering by Sculley).
 * 
 * This is an iterative algorithm that will make multiple passes over the data, so any RDDs given
 * to it should be cached by the user.
 */
class KMeansMiniBatch private (
    private var k: Int,
    private var maxIterations: Int,
    private var batchSize: Int,
    private var runs: Int,
    private var initializationMode: String,
    private var initializationSteps: Int,
    private var epsilon: Double) extends Serializable with KMeansCommons with Logging {

  /**
   * Constructs a KMeans instance with default parameters: {k: 2, maxIterations: 20, runs: 1,
   * batchSize: 1000, initializationMode: "k-means||", initializationSteps: 5, epsilon: 1e-4}.
   */
  def this() = this(2, 20, 1, 1000, KMeansMiniBatch.K_MEANS_PARALLEL, 5, 1e-4)

  def setBatchSize(batchSize: Int): KMeansMiniBatch = {
    this.batchSize = batchSize
    this
  }
  
  /** Set the number of clusters to create (k). Default: 2. */
  def setK(k: Int): KMeansMiniBatch = {
    this.k = k
    this
  }

  /** Set maximum number of iterations to run. Default: 20. */
  def setMaxIterations(maxIterations: Int): KMeansMiniBatch = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   */
  def setInitializationMode(initializationMode: String): KMeansMiniBatch = {
    if (initializationMode != KMeansMiniBatch.RANDOM && initializationMode != KMeansMiniBatch.K_MEANS_PARALLEL) {
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
  def setRuns(runs: Int): KMeansMiniBatch = {
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
  def setInitializationSteps(initializationSteps: Int): KMeansMiniBatch = {
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
  def setEpsilon(epsilon: Double): KMeansMiniBatch = {
    this.epsilon = epsilon
    this
  }

  /**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  def run(data: RDD[Vector]): KMeansModel = {
    // Compute squared norms and cache them.
    val norms = data.map(v => breezeNorm(v.toBreeze, 2.0))
    norms.persist()
    val breezeData = data.map(_.toBreeze).zip(norms).map { case (v, norm) =>
      new BreezeVectorWithNorm(v, norm)
    }
    
    val runModels = (0 until runs).map { _ =>
      runBreeze(breezeData)
    }
    
    val bestModel = runModels.minBy(t => t._2)._1
    
    norms.unpersist()
    bestModel
  }

  /**
   * Implementation of K-Means using breeze.
   */
  private def runBreeze(data: RDD[BreezeVectorWithNorm]): (KMeansModel, Double) = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val centers = if (initializationMode == KMeansMiniBatch.RANDOM) {
      initRandom(data, k)
    } else {
      initParallel(data, k, initializationSteps)
    }

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(s"Initialization with $initializationMode took " + "%.3f".format(initTimeInSeconds) +
      " seconds.")

    var costs = 0.0
    var iteration = 0
    val iterationStartTime = System.nanoTime()

    // Execute iterations of Lloyd's algorithm until all runs have converged
    while (iteration < maxIterations) {
      type WeightedPoint = (BV[Double], Long)
      def mergeContribs(p1: WeightedPoint, p2: WeightedPoint): WeightedPoint = {
        (p1._1 += p2._1, p1._2 + p2._2)
      }

      val costAccums = sc.accumulator(0.0)

      // Find the sum and count of points mapping to each center
      val totalContribs = data.mapPartitions { points =>
        val k = centers.length
        val dims = centers(0).vector.length

        val sums = Array.fill(k)(BDV.zeros[Double](dims).asInstanceOf[BV[Double]])
        val counts = Array.fill(k)(0L)

        points.foreach { point =>
          val (bestCenter, cost) = KMeansMiniBatch.findClosest(centers, point)
          costAccums += cost
          sums(bestCenter) += point.vector
          counts(bestCenter) += 1
        }

        val contribs = for (j <- 0 until k) yield {
          (j, (sums(j), counts(j)))
        }
        contribs.iterator
      }.reduceByKey(mergeContribs).collectAsMap()

      // Update the cluster centers and costs
	  var j = 0
	  while (j < k) {
	    val (sum, count) = totalContribs(j)
	    if (count != 0) {
	      sum /= count.toDouble
	      val newCenter = new BreezeVectorWithNorm(sum)
	      centers(j) = newCenter
	    }
	    j += 1
  	  }
        
      costs = costAccums.value
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(s"Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")
 
    logInfo(s"The cost for the run is $costs.")

    new Tuple2(new KMeansModel(centers.map(c => Vectors.fromBreeze(c.vector))), costs)
  }
}


/**
 * Top-level methods for calling K-means clustering.
 */
object KMeansMiniBatch extends KMeansObjectCommons {

  // Initialization mode names
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"

  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Array[Double]]`
   * @param k number of clusters
   * @param batchSize number of points in each batch
   * @param maxIterations max number of iterations
   * @param runs number of parallel runs, defaults to 1. The best model is returned.
   * @param initializationMode initialization model, either "random" or "k-means||" (default).
   */
  def train(
      data: RDD[Vector],
      k: Int,
      batchSize: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): KMeansModel = {
    new KMeansMiniBatch().setK(k)
      .setBatchSize(batchSize)
      .setMaxIterations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .run(data)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int): KMeansModel = {
    train(data, k, 1000, maxIterations, 1, K_MEANS_PARALLEL)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int): KMeansModel = {
    train(data, k, 1000, maxIterations, runs, K_MEANS_PARALLEL)
  }
  
    /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): KMeansModel = {
    train(data, k, 1000, maxIterations, runs, initializationMode)
  }
}
