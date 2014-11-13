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

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.base.{ Centroid, FP, PointOps, Zero }
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * A K-Means clustering implementation that performs multiple K-means clusterings simultaneously,
 * returning the one with the lowest cost.
 *
 * We only compute if a center has moved if we need to.
 *
 * We use null to represent empty clusters instead of an Option type to save space.
 *
 * The resulting clustering may contain fewer than K clusters.
 */

private[mllib] class MultiKMeans[P <: FP: ClassTag, C <: FP: ClassTag](
  pointOps: PointOps[P, C],
  maxIterations: Int) extends MultiKMeansClusterer[P, C] {

  def cluster(data: RDD[P], centers: Array[Array[C]]): (Double, GeneralizedKMeansModel[P, C]) = {
    val runs = centers.length
    val active = Array.fill(runs)(true)
    val costs = Array.fill(runs)(Zero)
    var activeRuns = new ArrayBuffer[Int] ++ (0 until runs)
    var iteration = 0

    /*
     * Execute iterations of Lloyd's algorithm until all runs have converged.
     */

    while (iteration < maxIterations && activeRuns.nonEmpty) {
      logInfo(s"iteration $iteration")

      val activeCenters = activeRuns.map(r => centers(r)).toArray

      if (log.isInfoEnabled) {
        for (r <- 0 until activeCenters.length)
          logInfo(s"run ${activeRuns(r)} has ${activeCenters(r).length} centers")
      }

      // Find the sum and count of points mapping to each center
      val (centroids, runDistortion) = getCentroids(data, activeCenters)

      if (log.isInfoEnabled) {
        for (run <- activeRuns) logInfo(s"run $run distortion ${runDistortion(run)}")
      }

      for (run <- activeRuns) active(run) = false

      for (((runIndex: Int, clusterIndex: Int), cn: Centroid) <- centroids) {
        val run = activeRuns(runIndex)
        if (cn.isEmpty) {
          active(run) = true
          centers(run)(clusterIndex) = null.asInstanceOf[C]
        } else {
          val centroid = pointOps.centroidToPoint(cn)
          active(run) = active(run) || pointOps.centerMoved(centroid, centers(run)(clusterIndex))
          centers(run)(clusterIndex) = pointOps.pointToCenter(centroid)
        }
      }

      // filter out null centers
      for (r <- activeRuns) centers(r) = centers(r).filter(_ != null)

      // update distortions and print log message if run completed during this iteration
      for ((run, runIndex) <- activeRuns.zipWithIndex) {
        costs(run) = runDistortion(runIndex)
        if (!active(run)) logInfo(s"run $run finished in ${iteration + 1} iterations")
      }
      activeRuns = activeRuns.filter(active(_))
      iteration += 1
    }

    val best = costs.zipWithIndex.min._2
    (costs(best), new GeneralizedKMeansModel(pointOps, centers(best)))
  }

  def getCentroids(data: RDD[P], activeCenters: Array[Array[C]]) = {
    val runDistortion = activeCenters.map(_ => data.sparkContext.accumulator(Zero))
    val bcActiveCenters = data.sparkContext.broadcast(activeCenters)
    val ops = pointOps
    val result = data.mapPartitions { points =>
      val bcCenters = bcActiveCenters.value
      val centers = bcCenters.map(x => Array.fill(x.length)(new Centroid()))
      for (
        point <- points;
        (clusters: Array[C], run) <- bcCenters.zipWithIndex
      ) {
        val (cluster, cost) = ops.findClosest(clusters, point)
        runDistortion(run) += cost
        centers(run)(cluster).add(point)
      }

      val contribution =
        for (
          (clusters, run) <- bcCenters.zipWithIndex;
          (contrib, cluster) <- clusters.zipWithIndex
        ) yield {
          ((run, cluster), centers(run)(cluster))
        }

      contribution.iterator
    }.reduceByKey { (x, y) => x.add(y) }.collect()
    bcActiveCenters.unpersist()
    (result, runDistortion.map(x => x.localValue))
  }
}
