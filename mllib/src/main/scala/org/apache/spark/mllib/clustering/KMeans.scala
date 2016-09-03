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

import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{KMeans => NewKMeans}
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * K-means clustering with a k-means++ like initialization mode
 * (the k-means|| algorithm by Bahmani et al).
 *
 * This is an iterative algorithm that will make multiple passes over the data, so any RDDs given
 * to it should be cached by the user.
 */
@Since("0.8.0")
class KMeans private (
    private var k: Int,
    private var maxIterations: Int,
    private var initializationMode: String,
    private var initializationSteps: Int,
    private var epsilon: Double,
    private var seed: Long) extends Serializable with Logging {

  /**
   * Constructs a KMeans instance with default parameters: {k: 2, maxIterations: 20,
   * initializationMode: "k-means||", initializationSteps: 2, epsilon: 1e-4, seed: random}.
   */
  @Since("0.8.0")
  def this() = this(2, 20, KMeans.K_MEANS_PARALLEL, 2, 1e-4, Utils.random.nextLong())

  /**
   * Number of clusters to create (k).
   */
  @Since("1.4.0")
  def getK: Int = k

  /**
   * Set the number of clusters to create (k). Default: 2.
   */
  @Since("0.8.0")
  def setK(k: Int): this.type = {
    require(k > 0,
      s"Number of clusters must be positive but got ${k}")
    this.k = k
    this
  }

  /**
   * Maximum number of iterations allowed.
   */
  @Since("1.4.0")
  def getMaxIterations: Int = maxIterations

  /**
   * Set maximum number of iterations allowed. Default: 20.
   */
  @Since("0.8.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  /**
   * The initialization algorithm. This can be either "random" or "k-means||".
   */
  @Since("1.4.0")
  def getInitializationMode: String = initializationMode

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   */
  @Since("0.8.0")
  def setInitializationMode(initializationMode: String): this.type = {
    KMeans.validateInitMode(initializationMode)
    this.initializationMode = initializationMode
    this
  }

  /**
   * This function has no effect since Spark 2.0.0.
   */
  @Since("1.4.0")
  @deprecated("This has no effect and always returns 1", "2.1.0")
  def getRuns: Int = {
    logWarning("Getting number of runs has no effect since Spark 2.0.0.")
    1
  }

  /**
   * This function has no effect since Spark 2.0.0.
   */
  @Since("0.8.0")
  @deprecated("This has no effect", "2.1.0")
  def setRuns(runs: Int): this.type = {
    logWarning("Setting number of runs has no effect since Spark 2.0.0.")
    this
  }

  /**
   * Number of steps for the k-means|| initialization mode
   */
  @Since("1.4.0")
  def getInitializationSteps: Int = initializationSteps

  /**
   * Set the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Default: 5.
   */
  @Since("0.8.0")
  def setInitializationSteps(initializationSteps: Int): this.type = {
    require(initializationSteps > 0,
      s"Number of initialization steps must be positive but got ${initializationSteps}")
    this.initializationSteps = initializationSteps
    this
  }

  /**
   * The distance threshold within which we've consider centers to have converged.
   */
  @Since("1.4.0")
  def getEpsilon: Double = epsilon

  /**
   * Set the distance threshold within which we've consider centers to have converged.
   * If all centers move less than this Euclidean distance, we stop iterating one run.
   */
  @Since("0.8.0")
  def setEpsilon(epsilon: Double): this.type = {
    require(epsilon >= 0,
      s"Distance threshold must be nonnegative but got ${epsilon}")
    this.epsilon = epsilon
    this
  }

  /**
   * The random seed for cluster initialization.
   */
  @Since("1.4.0")
  def getSeed: Long = seed

  /**
   * Set the random seed for cluster initialization.
   */
  @Since("1.4.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  // Initial cluster centers can be provided as a KMeansModel object rather than using the
  // random or k-means|| initializationMode
  private var initialModel: Option[KMeansModel] = None

  /**
   * Set the initial starting point, bypassing the random initialization or k-means||
   * The condition model.k == this.k must be met, failure results
   * in an IllegalArgumentException.
   */
  @Since("1.4.0")
  def setInitialModel(model: KMeansModel): this.type = {
    require(model.k == k, "mismatched cluster count")
    initialModel = Some(model)
    this
  }

  /**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  @Since("0.8.0")
  def run(data: RDD[Vector]): KMeansModel = {
    run(data, None)
  }

  private[spark] def run(
      data: RDD[Vector],
      instr: Option[Instrumentation[NewKMeans]]): KMeansModel = {

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Compute squared norms and cache them.
    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist()
    val zippedData = data.zip(norms).map { case (v, norm) =>
      new VectorWithNorm(v, norm)
    }
    val model = runAlgorithm(zippedData, instr)
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

  /**
   * Implementation of K-Means algorithm.
   */
  private def runAlgorithm(
      data: RDD[VectorWithNorm],
      instr: Option[Instrumentation[NewKMeans]]): KMeansModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val centers = initialModel match {
      case Some(kMeansCenters) =>
        kMeansCenters.clusterCenters.map(new VectorWithNorm(_))
      case None =>
        if (initializationMode == KMeans.RANDOM) {
          initRandom(data)
        } else {
          initKMeansParallel(data)
        }
    }
    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(f"Initialization with $initializationMode took $initTimeInSeconds%.3f seconds.")

    var active = true
    var cost = 0.0
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    instr.foreach(_.logNumFeatures(centers.head.vector.size))

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < maxIterations && active) {
      val costAccum = sc.doubleAccumulator
      val bcCenters = sc.broadcast(centers)

      // Find the sum and count of points mapping to each center
      val totalContribs = data.mapPartitions { points =>
        val thisCenters = bcCenters.value
        val k = thisCenters.length
        val dims = thisCenters.head.vector.size

        val sums = Array.fill(k)(Vectors.zeros(dims))
        val counts = Array.fill(k)(0L)

        points.foreach { point =>
          val (bestCenter, cost) = KMeans.findClosest(thisCenters, point)
          costAccum.add(cost)
          val sum = sums(bestCenter)
          axpy(1.0, point.vector, sum)
          counts(bestCenter) += 1
        }

        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        axpy(1.0, sum2, sum1)
        (sum1, count1 + count2)
      }.collectAsMap()

      bcCenters.destroy(blocking = false)

      // Update the cluster centers and costs
      active = false
      totalContribs.foreach { case (j, (sum, count)) =>
        scal(1.0 / count, sum)
        val newCenter = new VectorWithNorm(sum)
        if (!active && KMeans.fastSquaredDistance(newCenter, centers(j)) > epsilon * epsilon) {
          active = true
        }
        centers(j) = newCenter
      }

      cost = costAccum.value
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

    if (iteration == maxIterations) {
      logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    logInfo(s"The cost is $cost.")

    new KMeansModel(centers.map(_.vector))
  }

  /**
   * Initialize set of cluster centers at random.
   */
  private def initRandom(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    val sample = data.takeSample(true, k, new XORShiftRandom(this.seed).nextInt())
    sample.map(v => new VectorWithNorm(Vectors.dense(v.vector.toArray), v.norm))
  }

  /**
   * Initialize set of cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find with dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private def initKMeansParallel(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    // Initialize empty centers and point costs.
    var costs = data.map(_ => Double.PositiveInfinity)

    // Initialize each run's first center to a random point.
    val seed = new XORShiftRandom(this.seed).nextInt()
    val sample = data.takeSample(false, 1, seed)
    // Could be empty if data is empty; fail with a better message early:
    require(sample.nonEmpty, s"No samples available from $data")

    val centers = ArrayBuffer[VectorWithNorm]()
    var newCenters = Seq(sample.head.toDense)
    centers ++= newCenters

    // On each step, sample 2 * k points on average with probability proportional
    // to their squared distance from the centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    var bcNewCentersList = ArrayBuffer[Broadcast[_]]()
    while (step < initializationSteps) {
      val bcNewCenters = data.context.broadcast(newCenters)
      bcNewCentersList += bcNewCenters
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
          math.min(KMeans.pointCost(bcNewCenters.value, point), cost)
        }.persist(StorageLevel.MEMORY_AND_DISK)
      val sumCosts = costs.sum()

      bcNewCenters.unpersist(blocking = false)
      preCosts.unpersist(blocking = false)

      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointCosts) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        pointCosts.filter { case (_, c) => rand.nextDouble() < 2.0 * c * k / sumCosts }.map(_._1)
      }.collect()
      newCenters = chosen.map(_.toDense)
      centers ++= newCenters
      step += 1
    }

    costs.unpersist(blocking = false)
    bcNewCentersList.foreach(_.destroy(false))

    if (centers.size <= k) {
      return centers.toArray
    }

    // Finally, we might have a set of more than k candidate centers; weight each
    // candidate by the number of points in the dataset mapping to it and run a local k-means++
    // on the weighted centers to pick just k of them
    val bcCenters = data.context.broadcast(centers)
    val countMap = data.map(p => KMeans.findClosest(bcCenters.value, p)._1).countByValue()

    bcCenters.destroy(blocking = false)

    val myWeights = centers.indices.map(countMap.getOrElse(_, 0L).toDouble).toArray
    LocalKMeans.kMeansPlusPlus(0, centers.toArray, myWeights, k, 30)
  }
}


/**
 * Top-level methods for calling K-means clustering.
 */
@Since("0.8.0")
object KMeans {

  // Initialization mode names
  @Since("0.8.0")
  val RANDOM = "random"
  @Since("0.8.0")
  val K_MEANS_PARALLEL = "k-means||"

  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data Training points as an `RDD` of `Vector` types.
   * @param k Number of clusters to create.
   * @param maxIterations Maximum number of iterations allowed.
   * @param runs This param has no effect since Spark 2.0.0.
   * @param initializationMode The initialization algorithm. This can either be "random" or
   *                           "k-means||". (default: "k-means||")
   * @param seed Random seed for cluster initialization. Default is to generate seed based
   *             on system time.
   */
  @Since("1.3.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String,
      seed: Long): KMeansModel = {
    new KMeans().setK(k)
      .setMaxIterations(maxIterations)
      .setInitializationMode(initializationMode)
      .setSeed(seed)
      .run(data)
  }

  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data Training points as an `RDD` of `Vector` types.
   * @param k Number of clusters to create.
   * @param maxIterations Maximum number of iterations allowed.
   * @param runs This param has no effect since Spark 2.0.0.
   * @param initializationMode The initialization algorithm. This can either be "random" or
   *                           "k-means||". (default: "k-means||")
   */
  @Since("0.8.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): KMeansModel = {
    new KMeans().setK(k)
      .setMaxIterations(maxIterations)
      .setInitializationMode(initializationMode)
      .run(data)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  @Since("0.8.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int): KMeansModel = {
    train(data, k, maxIterations, 1, K_MEANS_PARALLEL)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  @Since("0.8.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int): KMeansModel = {
    train(data, k, maxIterations, runs, K_MEANS_PARALLEL)
  }

  /**
   * Returns the index of the closest center to the given point, as well as the squared distance.
   */
  private[mllib] def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * Returns the K-means cost of a given point against the given cluster centers.
   */
  private[mllib] def pointCost(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }

  private[spark] def validateInitMode(initMode: String): Boolean = {
    initMode match {
      case KMeans.RANDOM => true
      case KMeans.K_MEANS_PARALLEL => true
      case _ => false
    }
  }
}

/**
 * A vector with its norm for fast distance computation.
 *
 * @see [[org.apache.spark.mllib.clustering.KMeans#fastSquaredDistance]]
 */
private[clustering]
class VectorWithNorm(val vector: Vector, val norm: Double) extends Serializable {

  def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** Converts the vector to a dense vector. */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)
}
