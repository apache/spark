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

import org.apache.spark.mllib.base.{ PointOps, FP, Infinity, One, Zero }
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{ Logging, SparkContext }

/**
 *
 * The KMeans++ initialization algorithm
 *
 * @param pointOps distance function
 * @tparam P point type
 * @tparam C center type
 */
private[mllib] class KMeansPlusPlus[P <: FP: ClassTag, C <: FP: ClassTag](
  pointOps: PointOps[P, C]) extends Serializable with Logging {

  /**
   * We will maintain for each point the distance to its closest cluster center.
   * Since only one center is added on each iteration, recomputing the closest cluster center
   * only requires computing the distance to the new cluster center if
   * that distance is less than the closest cluster center.
   */
  case class FatPoint(location: P, index: Int, weight: Double, distance: Double)

  /**
   * K-means++ on the weighted point set `points`. This first does the K-means++
   * initialization procedure and then rounds of Lloyd's algorithm.
   */

  def cluster(
    sc: SparkContext,
    seed: Int,
    points: Array[C],
    weights: Array[Double],
    k: Int,
    maxIterations: Int,
    numPartitions: Int): Array[C] = {
    val centers: Array[C] = getCenters(sc, seed, points, weights, k, numPartitions, 1)
    val pts = sc.parallelize(points.map(pointOps.centerToPoint))
    new MultiKMeans(pointOps, maxIterations).cluster(pts, Array(centers))._2.centers
  }

  /**
   * Select centers in rounds.  On each round, select 'perRound' centers, with probability of
   * selection equal to the product of the given weights and distance to the closest cluster center
   * of the previous round.
   *
   * @param sc the Spark context
   * @param seed a random number seed
   * @param points  the candidate centers
   * @param weights  the weights on the candidate centers
   * @param k  the total number of centers to select
   * @param numPartitions the number of data partitions to use
   * @param perRound the number of centers to add per round
   * @return   an array of at most k cluster centers
   */
  def getCenters(
    sc: SparkContext,
    seed: Int, points: Array[C],
    weights: Array[Double],
    k: Int,
    numPartitions: Int,
    perRound: Int): Array[C] = {
    assert(points.length > 0)
    assert(k > 0)
    assert(numPartitions > 0)
    assert(perRound > 0)

    if (points.length < k) log.warn("number of clusters requested {} exceeds number of points {}",
      k, points.length)
    val centers = new ArrayBuffer[C](k)
    val rand = new XORShiftRandom(seed)
    centers += points(pickWeighted(rand, weights))
    log.info("starting kMeansPlusPlus initialization on {} points", points.length)

    var more = true
    var fatPoints = initialFatPoints(points, weights)
    fatPoints = updateDistances(fatPoints, centers.view.take(1))

    while (centers.length < k && more) {
      val chosen = choose(fatPoints, seed ^ (centers.length << 24), rand, perRound)
      val newCenters = chosen.map(points(_))
      fatPoints = updateDistances(fatPoints, newCenters)
      log.info("chose {} points", chosen.length)
      for (index <- chosen) {
        log.info("  center({}) = points({})", centers.length, index)
        centers += points(index)
      }
      more = chosen.nonEmpty
    }
    val result = centers.take(k)
    log.info("completed kMeansPlusPlus initialization with {} centers of {} requested",
      result.length, k)
    result.toArray
  }

  /**
   * Choose points
   *
   * @param fatPoints points to choose from
   * @param seed  random number seed
   * @param rand  random number generator
   * @param count number of points to choose
   * @return indices of chosen points
   */
  def choose(fatPoints: Array[FatPoint], seed: Int, rand: XORShiftRandom, count: Int) =
    (0 until count).flatMap { x => pickCenter(rand, fatPoints.iterator) }.map { _.index }

  /**
   * Create initial fat points with weights given and infinite distance to closest cluster center.
   * @param points points
   * @param weights weights of points
   * @return fat points with given weighs and infinite distance to closest cluster center
   */
  def initialFatPoints(points: Array[C], weights: Array[Double]): Array[FatPoint] =
    (0 until points.length).map { i =>
      FatPoint(pointOps.centerToPoint(points(i)), i, weights(i),
        Infinity)
    }.toArray

  /**
   * Update the distance of each point to its closest cluster center, given only the given cluster
   * centers that were modified.
   *
   * @param points set of candidate initial cluster centers
   * @param center new cluster center
   * @return  points with their distance to closest to cluster center updated
   */

  def updateDistances(points: Array[FatPoint], center: Seq[C]): Array[FatPoint] =
    points.map { p =>
      var i = 0
      val to = center.length
      var dist = p.distance
      val point = p.location
      while (i < to) {
        dist = pointOps.distance(point, center(i), dist)
        i = i + 1
      }
      p.copy(distance = dist)
    }

  /**
   * Pick a point at random, weighing the choices by the given weight vector.
   * Return -1 if all weights are 0.0
   *
   * Checks for illegal weight vector and throws exception instead of returning -1
   *
   * @param rand  random number generator
   * @param weights  the weights of the points
   * @return the index of the point chosen
   */
  def pickWeighted(rand: XORShiftRandom, weights: Array[Double]): Int = {
    val r = rand.nextDouble() * weights.sum
    var i = 0
    var curWeight = 0.0
    while (i < weights.length && curWeight < r) {
      assert(weights(i) >= 0.0)
      curWeight += weights(i)
      i += 1
    }
    if (i == 0) throw new IllegalArgumentException("all weights are zero")
    i - 1
  }

  /**
   *
   * Select point randomly with probability weighted by the product of the weight and the distance
   *
   * @param rand random number generator
   * @return
   */
  def pickCenter(rand: XORShiftRandom, fatPoints: Iterator[FatPoint]): Array[FatPoint] = {
    var cumulative = Zero
    val rangeAndIndexedPoints = fatPoints map { z =>
      val weightedDistance = z.weight * z.distance
      val from = cumulative
      cumulative = cumulative + weightedDistance
      (from, cumulative, z.location, z.index)
    }
    val pts = rangeAndIndexedPoints.toArray
    val total = pts.last._2
    val r = rand.nextDouble() * total
    for (w <- pts if w._1 <= r && r < w._2) yield FatPoint(w._3, w._4, One, total)
  }
}
