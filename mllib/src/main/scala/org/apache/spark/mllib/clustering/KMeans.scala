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


import org.apache.spark.mllib.base.{FP, PointOps}
import org.apache.spark.mllib.clustering.metrics.FastEuclideanOps
import org.apache.spark.rdd.RDD
<<<<<<< HEAD
import org.apache.spark.Logging
=======
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.XORShiftRandom

/**
 * K-means clustering with support for multiple parallel runs and a k-means++ like initialization
 * mode (the k-means|| algorithm by Bahmani et al). When multiple concurrent runs are requested,
 * they are executed together with joint passes over the data for efficiency.
 *
 * This is an iterative algorithm that will make multiple passes over the data, so any RDDs given
 * to it should be cached by the user.
 */
class KMeans private (
    private var k: Int,
    private var maxIterations: Int,
    private var runs: Int,
    private var initializationMode: String,
    private var initializationSteps: Int,
    private var epsilon: Double) extends Serializable with Logging {

  /**
   * Constructs a KMeans instance with default parameters: {k: 2, maxIterations: 20, runs: 1,
   * initializationMode: "k-means||", initializationSteps: 5, epsilon: 1e-4}.
   */
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

  /** Whether a warning should be logged if the input RDD is uncached. */
  private var warnOnUncachedInput = true

  /** Disable warnings about uncached input. */
  private[spark] def disableUncachedWarning(): this.type = {
    warnOnUncachedInput = false
    this
  }  

  /**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  def run(data: RDD[Vector]): KMeansModel = {

    if (warnOnUncachedInput && data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Compute squared norms and cache them.
    val norms = data.map(v => breezeNorm(v.toBreeze, 2.0))
    norms.persist()
    val breezeData = data.map(_.toBreeze).zip(norms).map { case (v, norm) =>
      new BreezeVectorWithNorm(v, norm)
    }
    val model = runBreeze(breezeData)
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (warnOnUncachedInput && data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

  /**
   * Implementation of K-Means using breeze.
   */
  private def runBreeze(data: RDD[BreezeVectorWithNorm]): KMeansModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val centers = if (initializationMode == KMeans.RANDOM) {
      initRandom(data)
    } else {
      initKMeansParallel(data)
    }

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(s"Initialization with $initializationMode took " + "%.3f".format(initTimeInSeconds) +
      " seconds.")

    val active = Array.fill(runs)(true)
    val costs = Array.fill(runs)(0.0)

    var activeRuns = new ArrayBuffer[Int] ++ (0 until runs)
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    // Execute iterations of Lloyd's algorithm until all runs have converged
    while (iteration < maxIterations && !activeRuns.isEmpty) {
      type WeightedPoint = (BV[Double], Long)
      def mergeContribs(p1: WeightedPoint, p2: WeightedPoint): WeightedPoint = {
        (p1._1 += p2._1, p1._2 + p2._2)
      }

      val activeCenters = activeRuns.map(r => centers(r)).toArray
      val costAccums = activeRuns.map(_ => sc.accumulator(0.0))

      val bcActiveCenters = sc.broadcast(activeCenters)

      // Find the sum and count of points mapping to each center
      val totalContribs = data.mapPartitions { points =>
        val thisActiveCenters = bcActiveCenters.value
        val runs = thisActiveCenters.length
        val k = thisActiveCenters(0).length
        val dims = thisActiveCenters(0)(0).vector.length

        val sums = Array.fill(runs, k)(BDV.zeros[Double](dims).asInstanceOf[BV[Double]])
        val counts = Array.fill(runs, k)(0L)

        points.foreach { point =>
          (0 until runs).foreach { i =>
            val (bestCenter, cost) = KMeans.findClosest(thisActiveCenters(i), point)
            costAccums(i) += cost
            sums(i)(bestCenter) += point.vector
            counts(i)(bestCenter) += 1
          }
        }

        val contribs = for (i <- 0 until runs; j <- 0 until k) yield {
          ((i, j), (sums(i)(j), counts(i)(j)))
        }
        contribs.iterator
      }.reduceByKey(mergeContribs).collectAsMap()

      // Update the cluster centers and costs for each active run
      for ((run, i) <- activeRuns.zipWithIndex) {
        var changed = false
        var j = 0
        while (j < k) {
          val (sum, count) = totalContribs((i, j))
          if (count != 0) {
            sum /= count.toDouble
            val newCenter = new BreezeVectorWithNorm(sum)
            if (KMeans.fastSquaredDistance(newCenter, centers(run)(j)) > epsilon * epsilon) {
              changed = true
            }
            centers(run)(j) = newCenter
          }
          j += 1
        }
        if (!changed) {
          active(run) = false
          logInfo("Run " + run + " finished in " + (iteration + 1) + " iterations")
        }
        costs(run) = costAccums(i).value
      }

      activeRuns = activeRuns.filter(active(_))
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(s"Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")

    if (iteration == maxIterations) {
      logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    val (minCost, bestRun) = costs.zipWithIndex.min

    logInfo(s"The cost for the best run is $minCost.")

    new KMeansModel(centers(bestRun).map(c => Vectors.fromBreeze(c.vector)))
  }

  /**
   * Initialize `runs` sets of cluster centers at random.
   */
  private def initRandom(data: RDD[BreezeVectorWithNorm])
  : Array[Array[BreezeVectorWithNorm]] = {
    // Sample all the cluster centers in one pass to avoid repeated scans
    val sample = data.takeSample(true, runs * k, new XORShiftRandom().nextInt()).toSeq
    Array.tabulate(runs)(r => sample.slice(r * k, (r + 1) * k).map { v =>
      new BreezeVectorWithNorm(v.vector.toDenseVector, v.norm)
    }.toArray)
  }

  /**
   * Initialize `runs` sets of cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find with dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private def initKMeansParallel(data: RDD[BreezeVectorWithNorm])
  : Array[Array[BreezeVectorWithNorm]] = {
    // Initialize each run's center to a random point
    val seed = new XORShiftRandom().nextInt()
    val sample = data.takeSample(true, runs, seed).toSeq
    val centers = Array.tabulate(runs)(r => ArrayBuffer(sample(r).toDense))

    // On each step, sample 2 * k points on average for each run with probability proportional
    // to their squared distance from that run's current centers
    var step = 0
    while (step < initializationSteps) {
      val bcCenters = data.context.broadcast(centers)
      val sumCosts = data.flatMap { point =>
        (0 until runs).map { r =>
          (r, KMeans.pointCost(bcCenters.value(r), point))
        }
      }.reduceByKey(_ + _).collectAsMap()
      val chosen = data.mapPartitionsWithIndex { (index, points) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        points.flatMap { p =>
          (0 until runs).filter { r =>
            rand.nextDouble() < 2.0 * KMeans.pointCost(bcCenters.value(r), p) * k / sumCosts(r)
          }.map((_, p))
        }
      }.collect()
      chosen.foreach { case (r, p) =>
        centers(r) += p.toDense
      }
      step += 1
    }
>>>>>>> 7364fa5a176da69e425bca0e3e137ee73275c78c

import scala.reflect.ClassTag

import org.apache.spark.mllib.linalg.Vector


object KMeans extends Logging  {
  // Initialization mode names
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"

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
  def train( data: RDD[Vector], k: Int, maxIterations: Int, runs: Int): KMeansModel =
    new KMeansModel(doTrain(new FastEuclideanOps)(data, k, maxIterations, runs)._2)


  def doTrain[P <: FP, C <: FP](pointOps: PointOps[P, C])(
    raw: RDD[Vector],
    k: Int = 2,
    maxIterations: Int = 20,
    runs: Int = 1,
    initializationMode: String = K_MEANS_PARALLEL,
    initializationSteps: Int = 5,
    epsilon: Double = 1e-4)(implicit ctag: ClassTag[C], ptag: ClassTag[P])
  : (Double, GeneralizedKMeansModel[P, C]) = {

    val initializer = if (initializationMode == RANDOM) {
      new KMeansRandom(pointOps, k, runs)
    } else {
      new KMeansParallel(pointOps, k, runs, initializationSteps, 1)
    }
    val data = (raw map { vals => pointOps.vectorToPoint(vals) }).cache()
    val centers = initializer.init(data, 0)
    new MultiKMeans(pointOps, maxIterations).cluster(data, centers)
  }
}
