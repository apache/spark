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

import org.jblas.DoubleMatrix

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.util.MLUtils
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
    var k: Int,
    var maxIterations: Int,
    var runs: Int,
    var initializationMode: String,
    var initializationSteps: Int,
    var epsilon: Double)
  extends Serializable with Logging
{
  private type ClusterCenters = Array[Array[Double]]

  def this() = this(2, 20, 1, KMeans.K_MEANS_PARALLEL, 5, 1e-4)

  /** Set the number of clusters to create (k). Default: 2. */
  def setK(k: Int): KMeans = {
    this.k = k
    this
  }

  /** Set maximum number of iterations to run. Default: 20. */
  def setMaxIterations(maxIterations: Int): KMeans = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   */
  def setInitializationMode(initializationMode: String): KMeans = {
    if (initializationMode != KMeans.RANDOM && initializationMode != KMeans.K_MEANS_PARALLEL) {
      throw new IllegalArgumentException("Invalid initialization mode: " + initializationMode)
    }
    this.initializationMode = initializationMode
    this
  }

  /**
   * Set the number of runs of the algorithm to execute in parallel. We initialize the algorithm
   * this many times with random starting conditions (configured by the initialization mode), then
   * return the best clustering found over any run. Default: 1.
   */
  def setRuns(runs: Int): KMeans = {
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
  def setInitializationSteps(initializationSteps: Int): KMeans = {
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
  def setEpsilon(epsilon: Double): KMeans = {
    this.epsilon = epsilon
    this
  }

  /**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  def run(data: RDD[Array[Double]]): KMeansModel = {
    // TODO: check whether data is persistent; this needs RDD.storageLevel to be publicly readable

    val sc = data.sparkContext

    val centers = if (initializationMode == KMeans.RANDOM) {
      initRandom(data)
    } else {
      initKMeansParallel(data)
    }

    val active = Array.fill(runs)(true)
    val costs = Array.fill(runs)(0.0)

    var activeRuns = new ArrayBuffer[Int] ++ (0 until runs)
    var iteration = 0

    // Execute iterations of Lloyd's algorithm until all runs have converged
    while (iteration < maxIterations && !activeRuns.isEmpty) {
      type WeightedPoint = (DoubleMatrix, Long)
      def mergeContribs(p1: WeightedPoint, p2: WeightedPoint): WeightedPoint = {
        (p1._1.addi(p2._1), p1._2 + p2._2)
      }

      val activeCenters = activeRuns.map(r => centers(r)).toArray
      val costAccums = activeRuns.map(_ => sc.accumulator(0.0))

      // Find the sum and count of points mapping to each center
      val totalContribs = data.mapPartitions { points =>
        val runs = activeCenters.length
        val k = activeCenters(0).length
        val dims = activeCenters(0)(0).length

        val sums = Array.fill(runs, k)(new DoubleMatrix(dims))
        val counts = Array.fill(runs, k)(0L)

        for (point <- points; (centers, runIndex) <- activeCenters.zipWithIndex) {
          val (bestCenter, cost) = KMeans.findClosest(centers, point)
          costAccums(runIndex) += cost
          sums(runIndex)(bestCenter).addi(new DoubleMatrix(point))
          counts(runIndex)(bestCenter) += 1
        }

        val contribs = for (i <- 0 until runs; j <- 0 until k) yield {
          ((i, j), (sums(i)(j), counts(i)(j)))
        }
        contribs.iterator
      }.reduceByKey(mergeContribs).collectAsMap()

      // Update the cluster centers and costs for each active run
      for ((run, i) <- activeRuns.zipWithIndex) {
        var changed = false
        for (j <- 0 until k) {
          val (sum, count) = totalContribs((i, j))
          if (count != 0) {
            val newCenter = sum.divi(count).data
            if (MLUtils.squaredDistance(newCenter, centers(run)(j)) > epsilon * epsilon) {
              changed = true
            }
            centers(run)(j) = newCenter
          }
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

    val bestRun = costs.zipWithIndex.min._2
    new KMeansModel(centers(bestRun))
  }

  /**
   * Initialize `runs` sets of cluster centers at random.
   */
  private def initRandom(data: RDD[Array[Double]]): Array[ClusterCenters] = {
    // Sample all the cluster centers in one pass to avoid repeated scans
    val sample = data.takeSample(true, runs * k, new XORShiftRandom().nextInt()).toSeq
    Array.tabulate(runs)(r => sample.slice(r * k, (r + 1) * k).toArray)
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
  private def initKMeansParallel(data: RDD[Array[Double]]): Array[ClusterCenters] = {
    // Initialize each run's center to a random point
    val seed = new XORShiftRandom().nextInt()
    val sample = data.takeSample(true, runs, seed).toSeq
    val centers = Array.tabulate(runs)(r => ArrayBuffer(sample(r)))

    // On each step, sample 2 * k points on average for each run with probability proportional
    // to their squared distance from that run's current centers
    for (step <- 0 until initializationSteps) {
      val centerArrays = centers.map(_.toArray)
      val sumCosts = data.flatMap { point =>
        for (r <- 0 until runs) yield (r, KMeans.pointCost(centerArrays(r), point))
      }.reduceByKey(_ + _).collectAsMap()
      val chosen = data.mapPartitionsWithIndex { (index, points) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        for {
          p <- points
          r <- 0 until runs
          if rand.nextDouble() < KMeans.pointCost(centerArrays(r), p) * 2 * k / sumCosts(r)
        } yield (r, p)
      }.collect()
      for ((r, p) <- chosen) {
        centers(r) += p
      }
    }

    // Finally, we might have a set of more than k candidate centers for each run; weigh each
    // candidate by the number of points in the dataset mapping to it and run a local k-means++
    // on the weighted centers to pick just k of them
    val centerArrays = centers.map(_.toArray)
    val weightMap = data.flatMap { p =>
      for (r <- 0 until runs) yield ((r, KMeans.findClosest(centerArrays(r), p)._1), 1.0)
    }.reduceByKey(_ + _).collectAsMap()
    val finalCenters = (0 until runs).map { r =>
      val myCenters = centers(r).toArray
      val myWeights = (0 until myCenters.length).map(i => weightMap.getOrElse((r, i), 0.0)).toArray
      LocalKMeans.kMeansPlusPlus(r, myCenters, myWeights, k, 30)
    }

    finalCenters.toArray
  }
}


/**
 * Top-level methods for calling K-means clustering.
 */
object KMeans {
  // Initialization mode names
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"

  def train(
      data: RDD[Array[Double]],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String)
    : KMeansModel =
  {
    new KMeans().setK(k)
                .setMaxIterations(maxIterations)
                .setRuns(runs)
                .setInitializationMode(initializationMode)
                .run(data)
  }

  def train(data: RDD[Array[Double]], k: Int, maxIterations: Int, runs: Int): KMeansModel = {
    train(data, k, maxIterations, runs, K_MEANS_PARALLEL)
  }

  def train(data: RDD[Array[Double]], k: Int, maxIterations: Int): KMeansModel = {
    train(data, k, maxIterations, 1, K_MEANS_PARALLEL)
  }

  /**
   * Return the index of the closest point in `centers` to `point`, as well as its distance.
   */
  private[mllib] def findClosest(centers: Array[Array[Double]], point: Array[Double])
    : (Int, Double) =
  {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    for (i <- 0 until centers.length) {
      val distance = MLUtils.squaredDistance(point, centers(i))
      if (distance < bestDistance) {
        bestDistance = distance
        bestIndex = i
      }
    }
    (bestIndex, bestDistance)
  }

  /**
   * Return the K-means cost of a given point against the given cluster centers.
   */
  private[mllib] def pointCost(centers: Array[Array[Double]], point: Array[Double]): Double = {
    var bestDistance = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val distance = MLUtils.squaredDistance(point, centers(i))
      if (distance < bestDistance) {
        bestDistance = distance
      }
    }
    bestDistance
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: KMeans <master> <input_file> <k> <max_iterations> [<runs>]")
      System.exit(1)
    }
    val (master, inputFile, k, iters) = (args(0), args(1), args(2).toInt, args(3).toInt)
    val runs = if (args.length >= 5) args(4).toInt else 1
    val sc = new SparkContext(master, "KMeans")
    val data = sc.textFile(inputFile).map(line => line.split(' ').map(_.toDouble)).cache()
    val model = KMeans.train(data, k, iters, runs)
    val cost = model.computeCost(data)
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.mkString(" "))
    }
    println("Cost: " + cost)
    System.exit(0)
  }
}
