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

import java.util.Random

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CLUSTER_LEVEL, COST, DIVISIBLE_CLUSTER_INDICES_SIZE, FEATURE_DIMENSION, MIN_POINT_PER_CLUSTER, NUM_POINT}
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.axpy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * A bisecting k-means algorithm based on the paper "A comparison of document clustering techniques"
 * by Steinbach, Karypis, and Kumar, with modification to fit Spark.
 * The algorithm starts from a single cluster that contains all points.
 * Iteratively it finds divisible clusters on the bottom level and bisects each of them using
 * k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.
 * The bisecting steps of clusters on the same level are grouped together to increase parallelism.
 * If bisecting all divisible clusters on the bottom level would result more than `k` leaf clusters,
 * larger clusters get higher priority.
 *
 * @param k the desired number of leaf clusters (default: 4). The actual number could be smaller if
 *          there are no divisible leaf clusters.
 * @param maxIterations the max number of k-means iterations to split clusters (default: 20)
 * @param minDivisibleClusterSize the minimum number of points (if greater than or equal 1.0) or
 *                                the minimum proportion of points (if less than 1.0) of a divisible
 *                                cluster (default: 1)
 * @param seed a random seed (default: hash value of the class name)
 *
 * @see <a href="http://glaros.dtc.umn.edu/gkhome/fetch/papers/docclusterKDDTMW00.pdf">
 * Steinbach, Karypis, and Kumar, A comparison of document clustering techniques,
 * KDD Workshop on Text Mining, 2000.</a>
 */
@Since("1.6.0")
class BisectingKMeans private (
    private var k: Int,
    private var maxIterations: Int,
    private var minDivisibleClusterSize: Double,
    private var seed: Long,
    private var distanceMeasure: String) extends Logging {

  import BisectingKMeans._

  /**
   * Constructs with the default configuration
   */
  @Since("1.6.0")
  def this() = this(4, 20, 1.0, classOf[BisectingKMeans].getName.##, DistanceMeasure.EUCLIDEAN)

  /**
   * Sets the desired number of leaf clusters (default: 4).
   * The actual number could be smaller if there are no divisible leaf clusters.
   */
  @Since("1.6.0")
  def setK(k: Int): this.type = {
    require(k > 0, s"k must be positive but got $k.")
    this.k = k
    this
  }

  /**
   * Gets the desired number of leaf clusters.
   */
  @Since("1.6.0")
  def getK: Int = this.k

  /**
   * Sets the max number of k-means iterations to split clusters (default: 20).
   */
  @Since("1.6.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations > 0, s"maxIterations must be positive but got $maxIterations.")
    this.maxIterations = maxIterations
    this
  }

  /**
   * Gets the max number of k-means iterations to split clusters.
   */
  @Since("1.6.0")
  def getMaxIterations: Int = this.maxIterations

  /**
   * Sets the minimum number of points (if greater than or equal to `1.0`) or the minimum proportion
   * of points (if less than `1.0`) of a divisible cluster (default: 1).
   */
  @Since("1.6.0")
  def setMinDivisibleClusterSize(minDivisibleClusterSize: Double): this.type = {
    require(minDivisibleClusterSize > 0.0,
      s"minDivisibleClusterSize must be positive but got $minDivisibleClusterSize.")
    this.minDivisibleClusterSize = minDivisibleClusterSize
    this
  }

  /**
   * Gets the minimum number of points (if greater than or equal to `1.0`) or the minimum proportion
   * of points (if less than `1.0`) of a divisible cluster.
   */
  @Since("1.6.0")
  def getMinDivisibleClusterSize: Double = minDivisibleClusterSize

  /**
   * Sets the random seed (default: hash value of the class name).
   */
  @Since("1.6.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Gets the random seed.
   */
  @Since("1.6.0")
  def getSeed: Long = this.seed

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

  private[spark] def runWithWeight(
      instances: RDD[(Vector, Double)],
      handlePersistence: Boolean,
      instr: Option[Instrumentation]): BisectingKMeansModel = {
    val d = instances.map(_._1.size).first()
    logInfo(log"Feature dimension: ${MDC(FEATURE_DIMENSION, d)}.")

    val dMeasure = DistanceMeasure.decodeFromString(this.distanceMeasure)
    val norms = instances.map(d => Vectors.norm(d._1, 2.0))
    val vectors = instances.zip(norms)
      .map { case ((x, weight), norm) => new VectorWithNorm(x, norm, weight) }

    if (handlePersistence) {
      vectors.persist(StorageLevel.MEMORY_AND_DISK)
    } else {
      // Compute and cache vector norms for fast distance computation.
      norms.persist(StorageLevel.MEMORY_AND_DISK)
    }

    var assignments = vectors.map(v => (ROOT_INDEX, v))
    var activeClusters = summarize(d, assignments, dMeasure)
    instr.foreach(_.logNumExamples(activeClusters.values.map(_.size).sum))
    instr.foreach(_.logSumOfWeights(activeClusters.values.map(_.weightSum).sum))
    val rootSummary = activeClusters(ROOT_INDEX)
    val n = rootSummary.size
    logInfo(log"Number of points: ${MDC(NUM_POINT, n)}.")
    logInfo(log"Initial cost: ${MDC(COST, rootSummary.cost)}.")
    val minSize = if (minDivisibleClusterSize >= 1.0) {
      math.ceil(minDivisibleClusterSize).toLong
    } else {
      math.ceil(minDivisibleClusterSize * n).toLong
    }
    logInfo(log"The minimum number of points of a divisible cluster is " +
      log"${MDC(MIN_POINT_PER_CLUSTER, minSize)}.")
    var inactiveClusters = mutable.Seq.empty[(Long, ClusterSummary)]
    val random = new Random(seed)
    var numLeafClustersNeeded = k - 1
    var level = 1
    var preIndices: RDD[Long] = null
    var indices: RDD[Long] = null
    while (activeClusters.nonEmpty && numLeafClustersNeeded > 0 && level < LEVEL_LIMIT) {
      // Divisible clusters are sufficiently large and have non-trivial cost.
      var divisibleClusters = activeClusters.filter { case (_, summary) =>
        (summary.size >= minSize) && (summary.cost > MLUtils.EPSILON * summary.size)
      }
      // If we don't need all divisible clusters, take the larger ones.
      if (divisibleClusters.size > numLeafClustersNeeded) {
        divisibleClusters = divisibleClusters.toSeq.sortBy { case (_, summary) =>
            -summary.size
          }.take(numLeafClustersNeeded)
          .toMap
      }
      if (divisibleClusters.nonEmpty) {
        val divisibleIndices = divisibleClusters.keys.toSet
        logInfo(log"Dividing ${MDC(DIVISIBLE_CLUSTER_INDICES_SIZE, divisibleIndices.size)}" +
          log" clusters on level ${MDC(CLUSTER_LEVEL, level)}.")
        var newClusterCenters = divisibleClusters.flatMap { case (index, summary) =>
          val (left, right) = splitCenter(summary.center, random, dMeasure)
          Iterator((leftChildIndex(index), left), (rightChildIndex(index), right))
        }.map(identity) // workaround for a Scala bug (SI-7005) that produces a not serializable map
        var newClusters: Map[Long, ClusterSummary] = null
        var newAssignments: RDD[(Long, VectorWithNorm)] = null
        for (iter <- 0 until maxIterations) {
          newAssignments = updateAssignments(assignments, divisibleIndices, newClusterCenters,
              dMeasure)
            .filter { case (index, _) =>
            divisibleIndices.contains(parentIndex(index))
          }
          newClusters = summarize(d, newAssignments, dMeasure)
          newClusterCenters = newClusters.transform((_, v) => v.center).map(identity)
        }
        if (preIndices != null) {
          preIndices.unpersist()
        }
        preIndices = indices
        indices = updateAssignments(assignments, divisibleIndices, newClusterCenters, dMeasure).keys
          .persist(StorageLevel.MEMORY_AND_DISK)
        assignments = indices.zip(vectors)
        inactiveClusters ++= activeClusters
        activeClusters = newClusters
        numLeafClustersNeeded -= divisibleClusters.size
      } else {
        logInfo(log"None active and divisible clusters left " +
          log"on level ${MDC(CLUSTER_LEVEL, level)}. Stop iterations.")
        inactiveClusters ++= activeClusters
        activeClusters = Map.empty
      }
      level += 1
    }

    if (preIndices != null) { preIndices.unpersist() }
    if (indices != null) { indices.unpersist() }
    if (handlePersistence) { vectors.unpersist() } else { norms.unpersist() }

    val clusters = activeClusters ++ inactiveClusters
    val root = buildTree(clusters, dMeasure)
    val totalCost = root.leafNodes.map(_.cost).sum
    new BisectingKMeansModel(root, this.distanceMeasure, totalCost)
  }

  /**
   * Runs the bisecting k-means algorithm.
   * @param input RDD of vectors
   * @return model for the bisecting kmeans
   */
  @Since("1.6.0")
  def run(input: RDD[Vector]): BisectingKMeansModel = {
    val instances = input.map(point => (point, 1.0))
    val handlePersistence = input.getStorageLevel == StorageLevel.NONE
    runWithWeight(instances, handlePersistence, None)
  }

  /**
   * Java-friendly version of `run()`.
   */
  def run(data: JavaRDD[Vector]): BisectingKMeansModel = run(data.rdd)
}

private object BisectingKMeans extends Serializable {

  /** The index of the root node of a tree. */
  private val ROOT_INDEX: Long = 1

  private val MAX_DIVISIBLE_CLUSTER_INDEX: Long = Long.MaxValue / 2

  private val LEVEL_LIMIT = math.log10(Long.MaxValue) / math.log10(2)

  /** Returns the left child index of the given node index. */
  private def leftChildIndex(index: Long): Long = {
    require(index <= MAX_DIVISIBLE_CLUSTER_INDEX, s"Child index out of bound: 2 * $index.")
    2 * index
  }

  /** Returns the right child index of the given node index. */
  private def rightChildIndex(index: Long): Long = {
    require(index <= MAX_DIVISIBLE_CLUSTER_INDEX, s"Child index out of bound: 2 * $index + 1.")
    2 * index + 1
  }

  /** Returns the parent index of the given node index, or 0 if the input is 1 (root). */
  private def parentIndex(index: Long): Long = {
    index / 2
  }

  /**
   * Summarizes data by each cluster as Map.
   * @param d feature dimension
   * @param assignments pairs of point and its cluster index
   * @return a map from cluster indices to corresponding cluster summaries
   */
  private def summarize(
      d: Int,
      assignments: RDD[(Long, VectorWithNorm)],
      distanceMeasure: DistanceMeasure): Map[Long, ClusterSummary] = {
    assignments.aggregateByKey(new ClusterSummaryAggregator(d, distanceMeasure))(
        seqOp = (agg, v) => agg.add(v),
        combOp = (agg1, agg2) => agg1.merge(agg2)
      ).mapValues(_.summary)
      .collect().toMap
  }

  /**
   * Cluster summary aggregator.
   * @param d feature dimension
   */
  private class ClusterSummaryAggregator(val d: Int, val distanceMeasure: DistanceMeasure)
      extends Serializable {
    private var n: Long = 0L
    private var weightSum: Double = 0.0
    private val sum: Vector = Vectors.zeros(d)
    private var sumSq: Double = 0.0

    /** Adds a point. */
    def add(v: VectorWithNorm): this.type = {
      n += 1L
      weightSum += v.weight
      // TODO: use a numerically stable approach to estimate cost
      sumSq += v.norm * v.norm  * v.weight
      distanceMeasure.updateClusterSum(v, sum)
      this
    }

    /** Merges another aggregator. */
    def merge(other: ClusterSummaryAggregator): this.type = {
      n += other.n
      weightSum += other.weightSum
      sumSq += other.sumSq
      axpy(1.0, other.sum, sum)
      this
    }

    /** Returns the summary. */
    def summary: ClusterSummary = {
      val center = distanceMeasure.centroid(sum.copy, weightSum)
      val cost = distanceMeasure.clusterCost(center, new VectorWithNorm(sum), weightSum,
        sumSq)
      ClusterSummary(n, weightSum, center, cost)
    }
  }

  /**
   * Bisects a cluster center.
   *
   * @param center current cluster center
   * @param random a random number generator
   * @return initial centers
   */
  private def splitCenter(
      center: VectorWithNorm,
      random: Random,
      distanceMeasure: DistanceMeasure): (VectorWithNorm, VectorWithNorm) = {
    val d = center.vector.size
    val norm = center.norm
    val level = 1e-4 * norm
    val noise = Vectors.dense(Array.fill(d)(random.nextDouble()))
    distanceMeasure.symmetricCentroids(level, noise, center.vector)
  }

  /**
   * Updates assignments.
   * @param assignments current assignments
   * @param divisibleIndices divisible cluster indices
   * @param newClusterCenters new cluster centers
   * @return new assignments
   */
  private def updateAssignments(
      assignments: RDD[(Long, VectorWithNorm)],
      divisibleIndices: Set[Long],
      newClusterCenters: Map[Long, VectorWithNorm],
      distanceMeasure: DistanceMeasure): RDD[(Long, VectorWithNorm)] = {
    assignments.map { case (index, v) =>
      if (divisibleIndices.contains(index)) {
        val children = Seq(leftChildIndex(index), rightChildIndex(index))
        val newClusterChildren = children.filter(newClusterCenters.contains)
        val newClusterChildrenCenterToId =
          newClusterChildren.map(id => newClusterCenters(id) -> id).toMap
        val newClusterChildrenCenters = newClusterChildrenCenterToId.keys.toArray
        if (newClusterChildren.nonEmpty) {
          val selected = distanceMeasure.findClosest(newClusterChildrenCenters, v)._1
          val center = newClusterChildrenCenters(selected)
          val id = newClusterChildrenCenterToId(center)
          (id, v)
        } else {
          (index, v)
        }
      } else {
        (index, v)
      }
    }
  }

  /**
   * Builds a clustering tree by re-indexing internal and leaf clusters.
   * @param clusters a map from cluster indices to corresponding cluster summaries
   * @return the root node of the clustering tree
   */
  private def buildTree(
      clusters: Map[Long, ClusterSummary],
      distanceMeasure: DistanceMeasure): ClusteringTreeNode = {
    var leafIndex = 0
    var internalIndex = -1

    /**
     * Builds a subtree from this given node index.
     */
    def buildSubTree(rawIndex: Long): ClusteringTreeNode = {
      val cluster = clusters(rawIndex)
      val size = cluster.size
      val center = cluster.center
      val cost = cluster.cost
      val isInternal = clusters.contains(leftChildIndex(rawIndex))
      if (isInternal) {
        val index = internalIndex
        internalIndex -= 1
        val leftIndex = leftChildIndex(rawIndex)
        val rightIndex = rightChildIndex(rawIndex)
        val indexes = Seq(leftIndex, rightIndex).filter(clusters.contains)
        val height = indexes.map { childIndex =>
          distanceMeasure.distance(center, clusters(childIndex).center)
        }.max
        val children = indexes.map(buildSubTree).toArray
        new ClusteringTreeNode(index, size, center, cost, height, children)
      } else {
        val index = leafIndex
        leafIndex += 1
        val height = 0.0
        new ClusteringTreeNode(index, size, center, cost, height, Array.empty)
      }
    }

    buildSubTree(ROOT_INDEX)
  }

  /**
   * Summary of a cluster.
   *
   * @param size the number of points within this cluster
   * @param weightSum the weightSum within this cluster
   * @param center the center of the points within this cluster
   * @param cost the sum of squared distances to the center
   */
  private case class ClusterSummary(
      size: Long,
      weightSum: Double,
      center: VectorWithNorm,
      cost: Double)
}

/**
 * Represents a node in a clustering tree.
 *
 * @param index node index, negative for internal nodes and non-negative for leaf nodes
 * @param size size of the cluster
 * @param centerWithNorm cluster center with norm
 * @param cost cost of the cluster, i.e., the sum of squared distances to the center
 * @param height height of the node in the dendrogram. Currently this is defined as the max distance
 *               from the center to the centers of the children's, but subject to change.
 * @param children children nodes
 */
@Since("1.6.0")
private[clustering] class ClusteringTreeNode private[clustering] (
    val index: Int,
    val size: Long,
    private[clustering] val centerWithNorm: VectorWithNorm,
    val cost: Double,
    val height: Double,
    val children: Array[ClusteringTreeNode]) extends Serializable {

  /** Whether this is a leaf node. */
  val isLeaf: Boolean = children.isEmpty

  require((isLeaf && index >= 0) || (!isLeaf && index < 0))

  /** Cluster center. */
  def center: Vector = centerWithNorm.vector

  /** Predicts the leaf cluster node index that the input point belongs to. */
  def predict(point: Vector, distanceMeasure: DistanceMeasure): Int = {
    val (index, _) = predict(new VectorWithNorm(point), distanceMeasure)
    index
  }

  /** Returns the full prediction path from root to leaf. */
  def predictPath(point: Vector, distanceMeasure: DistanceMeasure): Array[ClusteringTreeNode] = {
    predictPath(new VectorWithNorm(point), distanceMeasure).toArray
  }

  /** Returns the full prediction path from root to leaf. */
  private def predictPath(
      pointWithNorm: VectorWithNorm,
      distanceMeasure: DistanceMeasure): List[ClusteringTreeNode] = {
    if (isLeaf) {
      this :: Nil
    } else {
      val selected = children.minBy { child =>
        distanceMeasure.distance(child.centerWithNorm, pointWithNorm)
      }
      selected :: selected.predictPath(pointWithNorm, distanceMeasure)
    }
  }

  /**
   * Computes the cost of the input point.
   */
  def computeCost(point: Vector, distanceMeasure: DistanceMeasure): Double = {
    val (_, cost) = predict(new VectorWithNorm(point), distanceMeasure)
    cost
  }

  /**
   * Predicts the cluster index and the cost of the input point.
   */
  private def predict(
      pointWithNorm: VectorWithNorm,
      distanceMeasure: DistanceMeasure): (Int, Double) = {
    predict(pointWithNorm, distanceMeasure.cost(centerWithNorm, pointWithNorm), distanceMeasure)
  }

  /**
   * Predicts the cluster index and the cost of the input point.
   * @param pointWithNorm input point
   * @param cost the cost to the current center
   * @return (predicted leaf cluster index, cost)
   */
  @tailrec
  private def predict(
      pointWithNorm: VectorWithNorm,
      cost: Double,
      distanceMeasure: DistanceMeasure): (Int, Double) = {
    if (isLeaf) {
      (index, cost)
    } else {
      val (selectedChild, minCost) = children.map { child =>
        (child, distanceMeasure.cost(child.centerWithNorm, pointWithNorm))
      }.minBy(_._2)
      selectedChild.predict(pointWithNorm, minCost, distanceMeasure)
    }
  }

  /**
   * Returns all leaf nodes from this node.
   */
  def leafNodes: Array[ClusteringTreeNode] = {
    if (isLeaf) {
      Array(this)
    } else {
      children.flatMap(_.leafNodes)
    }
  }
}
