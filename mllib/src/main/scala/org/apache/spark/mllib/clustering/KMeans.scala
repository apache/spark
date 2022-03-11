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
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.axpy
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
    private var seed: Long,
    private var distanceMeasure: String) extends Serializable with Logging {

  @Since("0.8.0")
  private def this(k: Int, maxIterations: Int, initializationMode: String, initializationSteps: Int,
      epsilon: Double, seed: Long) =
    this(k, maxIterations, initializationMode, initializationSteps,
      epsilon, seed, DistanceMeasure.EUCLIDEAN)

  /**
   * Constructs a KMeans instance with default parameters: {k: 2, maxIterations: 20,
   * initializationMode: "k-means||", initializationSteps: 2, epsilon: 1e-4, seed: random,
   * distanceMeasure: "euclidean"}.
   */
  @Since("0.8.0")
  def this() = this(2, 20, KMeans.K_MEANS_PARALLEL, 2, 1e-4, Utils.random.nextLong(),
    DistanceMeasure.EUCLIDEAN)

  /**
   * Number of clusters to create (k).
   *
   * @note It is possible for fewer than k clusters to
   * be returned, for example, if there are fewer than k distinct points to cluster.
   */
  @Since("1.4.0")
  def getK: Int = k

  /**
   * Set the number of clusters to create (k).
   *
   * @note It is possible for fewer than k clusters to
   * be returned, for example, if there are fewer than k distinct points to cluster. Default: 2.
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
   * Number of steps for the k-means|| initialization mode
   */
  @Since("1.4.0")
  def getInitializationSteps: Int = initializationSteps

  /**
   * Set the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 2 is almost always enough. Default: 2.
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

  /**
   * The distance suite used by the algorithm.
   */
  @Since("2.4.0")
  def getDistanceMeasure: String = distanceMeasure

  /**
   * Set the distance suite used by the algorithm.
   */
  @Since("2.4.0")
  def setDistanceMeasure(distanceMeasure: String): this.type = {
    DistanceMeasure.validateDistanceMeasure(distanceMeasure)
    this.distanceMeasure = distanceMeasure
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
    val instances = data.map(point => (point, 1.0))
    val handlePersistence = data.getStorageLevel == StorageLevel.NONE
    runWithWeight(instances, handlePersistence, None)
  }

  private[spark] def runWithWeight(
      instances: RDD[(Vector, Double)],
      handlePersistence: Boolean,
      instr: Option[Instrumentation]): KMeansModel = {
    val norms = instances.map { case (v, _) => Vectors.norm(v, 2.0) }
    val vectors = instances.zip(norms)
      .map { case ((v, w), norm) => new VectorWithNorm(v, norm, w) }

    if (handlePersistence) {
      vectors.persist(StorageLevel.MEMORY_AND_DISK)
    } else {
      // Compute squared norms and cache them.
      norms.persist(StorageLevel.MEMORY_AND_DISK)
    }
    val model = runAlgorithmWithWeight(vectors, instr)
    if (handlePersistence) { vectors.unpersist() } else { norms.unpersist() }

    model
  }

  /**
   * Implementation of K-Means algorithm.
   */
  private def runAlgorithmWithWeight(
      data: RDD[VectorWithNorm],
      instr: Option[Instrumentation]): KMeansModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val distanceMeasureInstance = DistanceMeasure.decodeFromString(this.distanceMeasure)

    val centers = initialModel match {
      case Some(kMeansCenters) =>
        kMeansCenters.clusterCenters.map(new VectorWithNorm(_))
      case None =>
        if (initializationMode == KMeans.RANDOM) {
          initRandom(data)
        } else {
          initKMeansParallel(data, distanceMeasureInstance)
        }
    }
    val numFeatures = centers.head.vector.size
    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(f"Initialization with $initializationMode took $initTimeInSeconds%.3f seconds.")

    var converged = false
    var cost = 0.0
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    instr.foreach(_.logNumFeatures(numFeatures))

    val shouldComputeStats =
      DistanceMeasure.shouldComputeStatistics(centers.length)
    val shouldComputeStatsLocally =
      DistanceMeasure.shouldComputeStatisticsLocally(centers.length, numFeatures)

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < maxIterations && !converged) {
      val bcCenters = sc.broadcast(centers)
      val stats = if (shouldComputeStats) {
        if (shouldComputeStatsLocally) {
          Some(distanceMeasureInstance.computeStatistics(centers))
        } else {
          Some(distanceMeasureInstance.computeStatisticsDistributedly(sc, bcCenters))
        }
      } else {
        None
      }
      val bcStats = sc.broadcast(stats)

      val costAccum = sc.doubleAccumulator

      // Find the new centers
      val collected = data.mapPartitions { points =>
        val centers = bcCenters.value
        val stats = bcStats.value
        val dims = centers.head.vector.size

        val sums = Array.fill(centers.length)(Vectors.zeros(dims))

        // clusterWeightSum is needed to calculate cluster center
        // cluster center =
        //     sample1 * weight1/clusterWeightSum + sample2 * weight2/clusterWeightSum + ...
        val clusterWeightSum = Array.ofDim[Double](centers.length)

        points.foreach { point =>
          val (bestCenter, cost) = distanceMeasureInstance.findClosest(centers, stats, point)
          costAccum.add(cost * point.weight)
          distanceMeasureInstance.updateClusterSum(point, sums(bestCenter))
          clusterWeightSum(bestCenter) += point.weight
        }

        Iterator.tabulate(centers.length)(j => (j, (sums(j), clusterWeightSum(j))))
          .filter(_._2._2 > 0)
      }.reduceByKey { (sumweight1, sumweight2) =>
        axpy(1.0, sumweight2._1, sumweight1._1)
        (sumweight1._1, sumweight1._2 + sumweight2._2)
      }.collectAsMap()

      if (iteration == 0) {
        instr.foreach(_.logNumExamples(costAccum.count))
        instr.foreach(_.logSumOfWeights(collected.values.map(_._2).sum))
      }

      bcCenters.destroy()
      bcStats.destroy()

      // Update the cluster centers and costs
      converged = true
      collected.foreach { case (j, (sum, weightSum)) =>
        val newCenter = distanceMeasureInstance.centroid(sum, weightSum)
        if (converged &&
          !distanceMeasureInstance.isCenterConverged(centers(j), newCenter, epsilon)) {
          converged = false
        }
        centers(j) = newCenter
      }

      cost = costAccum.value
      instr.foreach(_.logNamedValue(s"Cost@iter=$iteration", s"$cost"))
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

    new KMeansModel(centers.map(_.vector), distanceMeasure, cost, iteration)
  }

  /**
   * Initialize a set of cluster centers at random.
   */
  private def initRandom(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    // Select without replacement; may still produce duplicates if the data has < k distinct
    // points, so deduplicate the centroids to match the behavior of k-means|| in the same situation
    data.takeSample(false, k, new XORShiftRandom(this.seed).nextInt())
      .map(_.vector).distinct.map(new VectorWithNorm(_))
  }

  /**
   * Initialize a set of cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private[clustering] def initKMeansParallel(data: RDD[VectorWithNorm],
      distanceMeasureInstance: DistanceMeasure): Array[VectorWithNorm] = {
    // Initialize empty centers and point costs.
    var costs = data.map(_ => Double.PositiveInfinity)

    // Initialize the first center to a random point.
    val seed = new XORShiftRandom(this.seed).nextInt()
    val sample = data.takeSample(false, 1, seed)
    // Could be empty if data is empty; fail with a better message early:
    require(sample.nonEmpty, s"No samples available from $data")

    val centers = ArrayBuffer[VectorWithNorm]()
    var newCenters = Array(sample.head.toDense)
    centers ++= newCenters

    // On each step, sample 2 * k points on average with probability proportional
    // to their squared distance from the centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    val bcNewCentersList = ArrayBuffer[Broadcast[_]]()
    while (step < initializationSteps) {
      val bcNewCenters = data.context.broadcast(newCenters)
      bcNewCentersList += bcNewCenters
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
        math.min(distanceMeasureInstance.pointCost(bcNewCenters.value, point), cost)
      }.persist(StorageLevel.MEMORY_AND_DISK)
      val sumCosts = costs.sum()

      bcNewCenters.unpersist()
      preCosts.unpersist()

      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointCosts) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        pointCosts.filter { case (_, c) => rand.nextDouble() < 2.0 * c * k / sumCosts }.map(_._1)
      }.collect()
      newCenters = chosen.map(_.toDense)
      centers ++= newCenters
      step += 1
    }

    costs.unpersist()
    bcNewCentersList.foreach(_.destroy())

    val distinctCenters = centers.map(_.vector).distinct.map(new VectorWithNorm(_)).toArray

    if (distinctCenters.length <= k) {
      distinctCenters
    } else {
      // Finally, we might have a set of more than k distinct candidate centers; weight each
      // candidate by the number of points in the dataset mapping to it and run a local k-means++
      // on the weighted centers to pick k of them
      val bcCenters = data.context.broadcast(distinctCenters)
      val countMap = data
        .map(distanceMeasureInstance.findClosest(bcCenters.value, _)._1)
        .countByValue()

      bcCenters.destroy()

      val myWeights = distinctCenters.indices.map(countMap.getOrElse(_, 0L).toDouble).toArray
      LocalKMeans.kMeansPlusPlus(0, distinctCenters, myWeights, k, 30)
    }
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
   * @param initializationMode The initialization algorithm. This can either be "random" or
   *                           "k-means||". (default: "k-means||")
   * @param seed Random seed for cluster initialization. Default is to generate seed based
   *             on system time.
   */
  @Since("2.1.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
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
   * @param initializationMode The initialization algorithm. This can either be "random" or
   *                           "k-means||". (default: "k-means||")
   */
  @Since("2.1.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
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
    new KMeans().setK(k)
      .setMaxIterations(maxIterations)
      .run(data)
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
 */
private[clustering] class VectorWithNorm(
    val vector: Vector,
    val norm: Double,
    val weight: Double = 1.0) extends Serializable {

  def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** Converts the vector to a dense vector. */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm, weight)
}
