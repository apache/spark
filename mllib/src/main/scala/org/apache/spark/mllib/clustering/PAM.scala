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

import scala.collection.TraversableOnce
import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm, SparseVector => BSV,
squaredDistance => breezeSquaredDistance}

import org.apache.spark.annotation.Experimental
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import java.nio.ByteBuffer
import java.util.{Random => JavaRandom}
import scala.util.hashing.MurmurHash3

/**
 * K-medoids clustering (PAM) with support for multiple parallel runs.
 * This is an iterative algorithm that will make multiple passes over the data, so any RDDs given
 * to it should be cached by the user.
 */
class PAM (
            var k: Int,
            var maxIterations: Int,
            var minIterations: Int,
            var runs: Int,
            var initializationMode: String,
            var initializationSteps: Int) extends Serializable with Logging {

  /**
   * Constructs a PAM instance with default parameters: {k: 2, maxIterations: 20, minIterations: 5,
   * runs: 1, initializationMode: "PAM_RANDOM", initializationSteps: 5}.
   */
  def this() = this(2, 20, 5, 1, PAM.PAM_RANDOM, 5)

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

  /** Set minimum number of iterations to run. Default: 5. */
  def setMinIterations(minIterations: Int): this.type = {
    this.minIterations = minIterations
    this
  }

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "parallel" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: PAM_RANDOM.
   */
  def setInitializationMode(initializationMode: String): this.type = {
    if (initializationMode != PAM.PAM_RANDOM && initializationMode != PAM.PAM_PARALLEL) {
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
   * Set the number of steps for the PAM|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Default: 5.
   */
  def setInitializationSteps(initializationSteps: Int): this.type = {
    if (initializationSteps <= 0) {
      throw new IllegalArgumentException("Number of initialization steps must be positive")
    }
    this.initializationSteps = initializationSteps
    this
  }

  /** Whether a warning should be logged if the input RDD is uncached. */
  private var warnOnUncachedInput = true

  /** Disable warnings about uncached input. */
  private def disableUncachedWarning(): this.type = {
    warnOnUncachedInput = false
    this
  }

  /**
   * Train a PAM model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  def run(data: RDD[Vector]): PAMModel = {

    if (warnOnUncachedInput && data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Compute squared norms and cache them.
    val norms = data.map{v =>
      val vectorValues: Array[Double] = v.toArray
      val vectorBV: BV[Double] = new BDV[Double](vectorValues)
      breezeNorm(vectorBV, 2.0)
    }
    norms.persist()
    val breezeData = data.map{v =>
      val vectorValues: Array[Double] = v.toArray
      val vectorBV: BV[Double] = new BDV[Double](vectorValues)
      vectorBV
    }.zip(norms).map { case (v, norm) =>
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
   * Implementation of PAM using breeze.
   */
  private def runBreeze(data: RDD[BreezeVectorWithNorm]): PAMModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val centers = if (initializationMode == PAM.PAM_RANDOM) {
      initRandom(data)
    } else {
      initPAMParallel(data)
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
      type WeightedPoint = (ArrayBuffer[BreezeVectorWithNorm], Double)
      def mergeContribs(p1: WeightedPoint, p2: WeightedPoint): WeightedPoint = {
        val pp1: ArrayBuffer[BreezeVectorWithNorm] = p1._1
        val pp2: ArrayBuffer[BreezeVectorWithNorm] = p2._1
        (pp1 ++= pp2, p1._2 + p2._2)
      }

      val activeCenters = activeRuns.map(r => centers(r)).toArray
      val costAccums = activeRuns.map(_ => sc.accumulator(0.0))

      val bcActiveCenters = sc.broadcast(activeCenters)

      // Find the cost and points mapping to each center
      val totalContribs = data.mapPartitions { points =>
        val thisActiveCenters = bcActiveCenters.value
        val runs = thisActiveCenters.length
        val k = thisActiveCenters(0).length

        val membersCenters = Array.fill(runs, k)(new ArrayBuffer[BreezeVectorWithNorm])
        val costsCenters = Array.fill(runs, k)(0.0)

        points.foreach { point =>
          (0 until runs).foreach { i =>
            val (bestCenter, cost) = PAM.findClosest(thisActiveCenters(i), point)
            costAccums(i) += cost
            membersCenters(i)(bestCenter).append(point)
            costsCenters(i)(bestCenter) += cost
          }
        }

        val contribs = for (i <- 0 until runs; j <- 0 until k) yield {
          ((i, j), (membersCenters(i)(j), costsCenters(i)(j)))
        }
        contribs.iterator
      }.reduceByKey(mergeContribs).collectAsMap()

      // Update the cluster centers and costs for each active run
      for ((run, i) <- activeRuns.zipWithIndex) {
        var changed = false
        var j = 0
        while (j < k) {
          var (membersCenter, costsCenter) = totalContribs((i, j))
          if (costsCenter != 0.0) {
            for(p <- 0 until membersCenter.length){
              var newCost = calInClusterCost(membersCenter, membersCenter(p))
              if(newCost < costsCenter){
                costsCenter = newCost
                changed = true
                centers(run)(j) = membersCenter(p)
              }
            }
          }
          j += 1
        }
        if (!changed && iteration > (minIterations - 2)) {
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
      logInfo(s"PAM reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"PAM converged in $iteration iterations.")
    }

    val (minCost, bestRun) = costs.zipWithIndex.min

    logInfo(s"The cost for the best run is $minCost.")

    new PAMModel(centers(bestRun).map(c => new DenseVector(c.vector.toArray)))
  }

  /**
   * Calculate the in cluster cost with a particular medoid.
   */
  private def calInClusterCost(
                                members: ArrayBuffer[BreezeVectorWithNorm],
                                member: BreezeVectorWithNorm): Double = {
    var inClusterCost = 0.0
    for(q <- 0 until members.length){
      if(members(q) != member){
        inClusterCost += PAM.fastSquaredDistance(members(q), member)
      }
    }
    inClusterCost
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
   * Initialize `runs` sets of cluster centers using the PAM|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find with dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private def initPAMParallel(data: RDD[BreezeVectorWithNorm])
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
          (r, PAM.pointCost(bcCenters.value(r), point))
        }
      }.reduceByKey(_ + _).collectAsMap()
      val chosen = data.mapPartitionsWithIndex { (index, points) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        points.flatMap { p =>
          (0 until runs).filter { r =>
            rand.nextDouble() < 2.0 * PAM.pointCost(bcCenters.value(r), p) * k / sumCosts(r)
          }.map((_, p))
        }
      }.collect()
      chosen.foreach { case (r, p) =>
        centers(r) += p.toDense
      }
      step += 1
    }

    // Finally, we might have a set of more than k candidate centers for each run; weigh each
    // candidate by the number of points in the dataset mapping to it and run a local k-means++
    // on the weighted centers to pick just k of them
    val bcCenters = data.context.broadcast(centers)
    val weightMap = data.flatMap { p =>
      (0 until runs).map { r =>
        ((r, PAM.findClosest(bcCenters.value(r), p)._1), 1.0)
      }
    }.reduceByKey(_ + _).collectAsMap()
    val finalCenters = (0 until runs).map { r =>
      val myCenters = centers(r).toArray
      val myWeights = (0 until myCenters.length).map(i => weightMap.getOrElse((r, i), 0.0)).toArray
      LocalPAM.kMeansPlusPlus(r, myCenters, myWeights, k, 30)
    }

    finalCenters.toArray
  }
}


/**
 * Top-level methods for calling PAM clustering.
 */
object PAM {

  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  // Initialization mode names
  val PAM_RANDOM = "random"
  val PAM_PARALLEL = "parallel"

  /**
   * Returns the index of the closest center to the given point, as well as the squared distance.
   */
  def findClosest(
                   centers: TraversableOnce[BreezeVectorWithNorm],
                   point: BreezeVectorWithNorm): (Int, Double) = {
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
   * Returns the PAM cost of a given point against the given cluster centers.
   */
  def pointCost(
                 centers: TraversableOnce[BreezeVectorWithNorm],
                 point: BreezeVectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
   * Returns the squared Euclidean distance between two vectors computed
   */
  def fastSquaredDistance(
                           v1: BreezeVectorWithNorm,
                           v2: BreezeVectorWithNorm): Double = {
    val precision: Double = 1e-6
    val vector1: BV[Double] = v1.vector
    val vector2: BV[Double] = v2.vector
    val norm1 = v1.norm
    val norm2 = v2.norm
    val n = vector1.size
    require(vector2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * vector1.dot(vector2)
    } else if (vector1.isInstanceOf[BSV[Double]] || vector2.isInstanceOf[BSV[Double]]) {
      val dot = vector1.dot(vector2)
      sqDist = math.max(sumSquaredNorm - 2.0 * dot, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dot)) / (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = breezeSquaredDistance(vector1, vector2)
      }
    } else {
      sqDist = breezeSquaredDistance(vector1, vector2)
    }
    sqDist
  }
}

  /**
   * This class implements a XORShift random number generator algorithm
   * Source:
   * Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
   * @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
   * This implementation is approximately 3.5 times faster than
   * {java.util.Random java.util.Random}, partly because of the algorithm, but also due
   * to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class
   * uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
   * for each thread.
   */
  class XORShiftRandom(init: Long) extends JavaRandom(init) {

    def this() = this(System.nanoTime)

    var seed = XORShiftRandom.hashSeed(init)

    // we need to just override next - this will be called by nextInt, nextDouble,
    // nextGaussian, nextLong, etc.
    override protected def next(bits: Int): Int = {
      var nextSeed = seed ^ (seed << 21)
      nextSeed ^= (nextSeed >>> 35)
      nextSeed ^= (nextSeed << 4)
      seed = nextSeed
      (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
    }

    override def setSeed(s: Long) {
      seed = XORShiftRandom.hashSeed(s)
    }
  }

  /** Contains benchmark method and main method to run benchmark of the RNG */
  object XORShiftRandom {
    /** Hash seeds to have 0/1 bits throughout. */
    def hashSeed(seed: Long): Long = {
      val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
      MurmurHash3.bytesHash(bytes)
    }
  }
