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
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.base.{ PointOps, FP, Zero }
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

private[mllib] class KMeansParallel[P <: FP: ClassTag, C <: FP: ClassTag](
  pointOps: PointOps[P, C],
  k: Int,
  runs: Int,
  initializationSteps: Int,
  numPartitions: Int)
  extends KMeansInitializer[P, C] with Logging {

  /**
   * Initialize `runs` sets of cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find  dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   *
   * @param data the RDD of points
   * @param seed the random number generator seed
   * @return
   */
  def init(data: RDD[P], seed: Int): Array[Array[C]] = {
    log.debug("k-means parallel on {} points" + data.count())

    // randomly select one center per run, putting each into a separate array buffer
    val sample = data.takeSample(true, runs, seed).toSeq.map(pointOps.pointToCenter)
    val centers: Array[ArrayBuffer[C]] = Array.tabulate(runs)(r => ArrayBuffer(sample(r)))

    // add at most 2k points per step
    for (step <- 0 until initializationSteps) {
      if (log.isInfoEnabled) showCenters(centers, step)
      val centerArrays = centers.map { x: ArrayBuffer[C] => x.toArray }
      val bcCenters = data.sparkContext.broadcast(centerArrays)
      for ((r, p) <- choose(data, seed, step, bcCenters)) {
        centers(r) += pointOps.pointToCenter(p)
      }
      bcCenters.unpersist()
    }

    val bcCenters = data.sparkContext.broadcast(centers.map(_.toArray))
    val result = finalCenters(data, bcCenters, seed)
    bcCenters.unpersist()
    result
  }

  def showCenters(centers: Array[ArrayBuffer[C]], step: Int) {
    log.info("step {}", step)
    for (run <- 0 until runs) {
      log.info("final: run {} has {} centers", run, centers.length)
    }
  }

  /**
   * Randomly choose at most 2 * k  additional cluster centers by weighting them by their distance
   * to the current closest cluster
   *
   * @param data  the RDD of points
   * @param seed  random generator seed
   * @param step  which step of the selection process
   * @return  array of (run, point)
   */
  def choose(
    data: RDD[P],
    seed: Int,
    step: Int,
    bcCenters: Broadcast[Array[Array[C]]]): Array[(Int, P)] = {
    // compute the weighted distortion for each run
    val sumCosts = data.flatMap {
      point =>
        val centers = bcCenters.value
        for (r <- 0 until runs) yield {
          (r, point.weight * pointOps.pointCost(centers(r), point))
        }
    }.reduceByKey(_ + _).collectAsMap()

    // choose points in proportion to ratio of weighted cost to weighted distortion
    data.mapPartitionsWithIndex {
      (index, points: Iterator[P]) =>
        val centers = bcCenters.value
        val range = 0 until runs
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        points.flatMap { p =>
          range.filter { r =>
            rand.nextDouble() < 2.0 * p.weight * pointOps.pointCost(centers(r), p) * k / sumCosts(r)
          }.map((_, p))
        }
    }.collect()
  }

  /**
   * Reduce sets of candidate cluster centers to at most k points per set using KMeansPlusPlus.
   * Weight the points by the distance to the closest cluster center.
   *
   * @param data  original points
   * @param bcCenters  array of sets of candidate centers
   * @param seed  random number seed
   * @return  array of sets of cluster centers
   */
  def finalCenters(
    data: RDD[P],
    bcCenters: Broadcast[Array[Array[C]]], seed: Int): Array[Array[C]] = {
    // for each (run, cluster) compute the sum of the weights of the points in the cluster
    val weightMap = data.flatMap {
      point =>
        val centers = bcCenters.value
        for (r <- 0 until runs) yield {
          ((r, pointOps.findClosest(centers(r), point)._1), point.weight)
        }
    }.reduceByKey(_ + _).collectAsMap()

    val centers = bcCenters.value
    val kmeansPlusPlus = new KMeansPlusPlus(pointOps)
    val trackingKmeans = new MultiKMeans(pointOps, 30)
    val finalCenters = (0 until runs).map {
      r =>
        val myCenters = centers(r).toArray
        log.info("run {} has {} centers", r, myCenters.length)
        val weights = (0 until myCenters.length).map(i => weightMap.getOrElse((r, i), Zero)).toArray
        // Check for the degenerate case that the number of centers available is less than k.
        val kx = if (k > myCenters.length) myCenters.length else k
        val sc = data.sparkContext
        val initial = kmeansPlusPlus.getCenters(sc, seed, myCenters, weights, kx, numPartitions, 1)
        trackingKmeans.cluster(data.sparkContext.parallelize(myCenters.map(pointOps.centerToPoint)),
          Array(initial))._2.centers
    }
    finalCenters.toArray
  }
}
