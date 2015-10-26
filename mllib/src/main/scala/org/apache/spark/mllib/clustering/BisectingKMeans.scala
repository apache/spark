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

import scala.collection.{Map, mutable}

import breeze.linalg
  .{SparseVector => BSV, Vector => BV, any => breezeAny, norm => breezeNorm, sum => breezeSum}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


/**
 * This is a divisive hierarchical clustering algorithm based on bisecting k-means algorithm.
 *
 * The main idea of this algorithm is based on "A comparison of document clustering techniques",
 * M. Steinbach, G. Karypis and V. Kumar. Workshop on Text Mining, KDD, 2000.
 * http://cs.fit.edu/~pkc/classes/ml-internet/papers/steinbach00tr.pdf
 *
 * However, we modified it to fit for Spark. This algorithm consists of the two main parts.
 *
 * 1. Split clusters until the number of clusters will be enough to build a cluster tree
 * 2. Build a cluster tree as a binary tree by the splitted clusters
 *
 * First, it splits clusters to their children clusters step by step, not considering a cluster
 * will be included in the final cluster tree or not. That's because it makes the algorithm more
 * efficient on Spark and splitting a cluster one by one is very slow. It will keep splitting until
 * the number of clusters will be enough to build a cluster tree. Otherwise, it will stop splitting
 * when there are no dividable clusters before the number of clusters will be sufficient. And
 * it calculates the costs, such as average cost, entropy and so on, for building a cluster
 * tree in the first part. The costs means how large the cluster is. That is, the cluster
 * whose cost is maximum of all the clusters is the largest cluster.
 *
 * Second, it builds a cluster tree as a binary tree by the result of the first part.
 * First of all, the cluster tree starts with only the root cluster which includes all points.
 * So, there are two candidates which can be merged to the cluster tree. Those are the children of
 * the root. Then, it picks up the larger child of the two and merge it to the cluster tree.
 * After that, there are tree candidates to merge. Those are the smaller child of the root and
 * the two children of the larger cluster of the root. It picks up the largest cluster of the tree
 * and merge it to the * cluster tree. Like this, it continues to pick up the largest one of the
 * candidates and merge it to the cluster tree until the desired number of clusters is reached.
 *
 * @param k tne desired number of clusters
 * @param maxIterations the number of maximal iterations to split clusters
 * @param seed a random seed
 */
@Since("1.6.0")
class BisectingKMeans private (
    private var k: Int,
    private var maxIterations: Int,
    private var seed: Long) extends Logging {

  import BisectingKMeans._

  /**
   * Constructs with the default configuration
   */
  @Since("1.6.0")
  def this() = this(20, 20, 1)

  /**
   * Sets the number of clusters you want
   */
  @Since("1.6.0")
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  @Since("1.6.0")
  def getK: Int = this.k

  /**
   * Sets the number of maximal iterations in each clustering step
   */
  @Since("1.6.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  @Since("1.6.0")
  def getMaxIterations: Int = this.maxIterations

  /**
   * Sets the random seed
   */
  @Since("1.6.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  @Since("1.6.0")
  def getSeed: Long = this.seed

  /**
   * Runs the bisecting k-means algorithm
   * @param input RDD of vectors
   * @return model for the bisecting kmeans
   */
  @Since("1.6.0")
  def run(input: RDD[Vector]): BisectingKMeansModel = {
    val sc = input.sparkContext

    // `clusterStats` is described as binary tree structure
    // `clusterStats(1)` means the root of a binary tree
    var clusterStats = mutable.Map.empty[Long, BisectingClusterStat]
    var step = 1
    var noMoreDividable = false
    var updatedDataHistory = Array.empty[RDD[(Long, BV[Double])]]
    // the minimum number of nodes of a binary tree by given parameter
    val numNodeLimit = getMinimumNumNodesInTree(this.k)

    // divide clusters until the number of clusters reachs the condition
    // or there is no dividable cluster
    val startTime = System.currentTimeMillis()
    var data = initData(input).cache()
    while (clusterStats.size < numNodeLimit && noMoreDividable == false) {
      logInfo(s"${sc.appName} starts step ${step}")
      // TODO Remove non-leaf cluster stats from `leafClusterStats`
      val leafClusterStats = summarizeClusters(data)
      val dividableLeafClusters = leafClusterStats.filter(_._2.isDividable)
      clusterStats = clusterStats ++ leafClusterStats

      // can be clustered if the number of divided clusterStats is equal to 0
      val divided = divideClusters(data, dividableLeafClusters, maxIterations)
      // update each index
      val newData = updateClusterIndex(data, divided).cache()
      updatedDataHistory = updatedDataHistory ++ Array(data)
      data = newData
      // keep recent 2 cached RDDs in order to run more quickly
      if (updatedDataHistory.length > 1) {
        val head = updatedDataHistory.head
        updatedDataHistory = updatedDataHistory.tail
        head.unpersist()
      }
      clusterStats = clusterStats ++ divided
      step += 1
      logInfo(s"${sc.appName} adding ${divided.size} new clusterStats at step:${step}")

      if (dividableLeafClusters.isEmpty) {
        noMoreDividable = true
      }
    }
    // create a map of cluster node with their costs
    val nodes = createClusterNodes(data, clusterStats)
    // unpersist RDDs
    data.unpersist()
    updatedDataHistory.foreach(_.unpersist())

    // build a cluster tree by Map class which is expressed
    logInfo(s"Building the cluster tree is started in ${sc.appName}")
    val root = buildTree(nodes, ROOT_INDEX_KEY, this.k)
    if (root.isEmpty) {
      new SparkException("Failed to build a cluster tree from a Map type of clusterStats")
    }

    // set the elapsed time for training
    val finishTime = (System.currentTimeMillis() - startTime) / 1000.0
    logInfo(s"Elapsed Time for ${this.getClass.getSimpleName} Training: ${finishTime} [sec]")

    // make a bisecting kmeans model
    val model = new BisectingKMeansModel(root.get)
    val leavesNodes = model.getClusters
    if (leavesNodes.length < this.k) {
      logWarning(s"# clusters is less than you want: ${leavesNodes.length} / ${k}")
    }
    model
  }
}


private[clustering] object BisectingKMeans {

  import BisectingClusterStat._

  val ROOT_INDEX_KEY: Long = 1

  /**
   * Finds the closes cluster's center
   *
   * @param metric a distance metric
   * @param centers centers of the clusters
   * @param point a target point
   * @return an index of the array of clusters
   */
  def findClosestCenter(metric: (BV[Double], BV[Double]) => Double)
      (centers: Seq[BV[Double]])(point: BV[Double]): Int = {
    // get the closest index
    centers.zipWithIndex.map { case (center, idx) => (metric(center, point), idx)}.minBy(_._1)._2
  }

  /**
   * Gets the minimum number of nodes in a tree by the number of leaves
   *
   * @param k: the number of leaf nodes
   */
  def getMinimumNumNodesInTree(k: Int): Int = {
    val multiplier = math.ceil(math.log(k) / math.log(2.0))
    // the calculation is same as `math.pow(2, multiplier)`
    var numNodes = 2
    (1 to multiplier.toInt).foreach (i => numNodes = numNodes << 1)
    numNodes
  }

  /**
   * Summarizes data by each cluster as Map
   *
   * @param data pairs of point and its cluster index
   */
  def summarizeClusters(data: RDD[(Long, BV[Double])]): Map[Long, BisectingClusterStat] = {

    data.mapPartitions { iter =>
      // calculate the accumulation of the all point in a partition and count the rows
      val map = mutable.Map.empty[Long, (BV[Double], Double, BV[Double])]
      iter.foreach { case (idx: Long, point: BV[Double]) =>
        // get a map value or else get a sparse vector
        val (sumBV, n, sumOfSquares) = map
          .getOrElse(idx, (BSV.zeros[Double](point.size), 0.0, BSV.zeros[Double](point.size)))
        map(idx) = (sumBV + point, n + 1.0, sumOfSquares + (point :* point))
      }
      map.toIterator
    }.reduceByKey { case ((sum1, n1, sumOfSquares1), (sum2, n2, sumOfSquares2)) =>
      // sum the accumulation and the count in the all partition
      (sum1 + sum2, n1 + n2, sumOfSquares1 + sumOfSquares2)
    }.map { case (i, (sum, n, sumOfSquares)) =>
      val mean = calcMean(n.toLong, sum)
      val variance = getVariance(n.toLong, sum, sumOfSquares)
      (i, new BisectingClusterStat(n.toLong, mean, variance))
    }.collectAsMap()
  }

  /**
   * Assigns the initial cluster index id to all data
   */
  def initData(data: RDD[Vector]): RDD[(Long, BV[Double])] = {
    data.map { v: Vector => (ROOT_INDEX_KEY, v.toBreeze)}
  }

  /**
   * Gets the initial centers for bisecting k-means
   *
   * @param data pairs of point and its cluster index
   * @param stats pairs of cluster index and cluster statistics
   */
  def initNextCenters(
      data: RDD[(Long, BV[Double])],
      stats: Map[Long, BisectingClusterStat]): Map[Long, BV[Double]] = {

    // Since the combination sampleByKey and groupByKey is more expensive,
    // this as follows would be better.
    val bcIndeces = data.sparkContext.broadcast(stats.keySet)
    val samples = data.mapPartitions { iter =>
      val map = mutable.Map.empty[Long, mutable.ArrayBuffer[BV[Double]]]

      bcIndeces.value.foreach {i => map(i) = mutable.ArrayBuffer.empty[BV[Double]]}
      val LOCAL_SAMPLE_SIZE = 100
      iter.foreach { case (i, point) =>
        map(i).append(point)
        // to avoid to increase the memory usage on each map thread,
        // the number of elements is cut off at the right time.
        if (map(i).size > LOCAL_SAMPLE_SIZE) {
          val elements = map(i).sortWith((a, b) => breezeNorm(a, 2.0) < breezeNorm(b, 2.0))
          map(i) = mutable.ArrayBuffer(elements.head, elements.last)
        }
      }

      // in order to reduce the shuffle size, take only two elements
      map.filterNot(_._2.isEmpty).map { case (i, points) =>
        val elements = map(i).toSeq.sortWith((a, b) => breezeNorm(a, 2.0) < breezeNorm(b, 2.0))
        i -> mutable.ArrayBuffer(elements.head, elements.last)
      }.toIterator
    }.reduceByKey { case (points1, points2) =>
      points1.union(points2)
    }.collect()

    val nextCenters = samples.flatMap { case (i, points) =>
      val elements = points.toSeq.sortWith((a, b) => breezeNorm(a, 2.0) < breezeNorm(b, 2.0))
      Array((2 * i, elements.head), (2 * i + 1, elements.last))
    }.toMap
    if (!stats.keySet.flatMap(idx => Array(2 * idx, 2 * idx + 1)).forall(nextCenters.contains(_))) {
      throw new SparkException("Failed to initialize centers for next step")
    }
    nextCenters
  }

  /**
   * Updates the indexes of clusters which is divided to its children indexes
   *
   * @param data pairs of point and its cluster index
   * @param dividedClusters pairs of cluster index and cluster statistics
   */
  def updateClusterIndex(
      data: RDD[(Long, BV[Double])],
      dividedClusters: Map[Long, BisectingClusterStat]): RDD[(Long, BV[Double])] = {

    // If there is no divided clusters, return the original
    if (dividedClusters.size == 0) {
      return data
    }

    // extract the centers of the clusters
    val sc = data.sparkContext
    var centers = dividedClusters.map { case (idx, cluster) => (idx, cluster.mean)}
    val bcCenters = sc.broadcast(centers)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val bcMetric = sc.broadcast(metric)

    // update the indexes to their children indexes
    data.map { case (idx, point) =>
      val childrenIndexes = Array(2 * idx, 2 * idx + 1).filter(c => bcCenters.value.contains(c))
      childrenIndexes.length match {
        // update the indexes
        case s if s == 2 => {
          val nextCenters = childrenIndexes.map(bcCenters.value(_))
          val closestIndex = BisectingKMeans
            .findClosestCenter(bcMetric.value)(nextCenters)(point)
          val nextIndex = 2 * idx + closestIndex
          (nextIndex, point)
        }
        // stay the index if the number of children is not enough
        case _ => (idx, point)
      }
    }
  }

  /**
   * Divides clusters according to their statistics
   *
   * @param data pairs of point and its cluster index
   * @param clusterStats target clusters to divide
   * @param maxIterations the maximum iterations to calculate clusters statistics
   */
  def divideClusters(
      data: RDD[(Long, BV[Double])],
      clusterStats: Map[Long, BisectingClusterStat],
      maxIterations: Int): Map[Long, BisectingClusterStat] = {
    val sc = data.sparkContext
    val appName = sc.appName

    // get keys of dividable clusters
    val dividableClusterStats = clusterStats.filter { case (idx, cluster) => cluster.isDividable }
    if (dividableClusterStats.isEmpty) {
      return Map.empty[Long, BisectingClusterStat]
    }
    // extract dividable input data
    val dividableData = data.filter { case (idx, point) => dividableClusterStats.contains(idx)}

    var newCenters = initNextCenters(dividableData, dividableClusterStats)
    var bcNewCenters = sc.broadcast(newCenters)
    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val bcMetric = sc.broadcast(metric)
    // pairs of cluster index and (sums, #points, sumOfSquares)
    var stats = Map.empty[Long, (BV[Double], Double, BV[Double])]

    var subIter = 0
    var totalStd = Double.MaxValue
    var oldTotalStd = Double.MaxValue
    var relativeError = Double.MaxValue
    while (subIter < maxIterations && relativeError > 10E-4) {
      // calculate summary of each cluster
      val eachStats = dividableData.mapPartitions { iter =>
        val map = mutable.Map.empty[Long, (BV[Double], Double, BV[Double])]
        iter.foreach { case (idx, point) =>
          // calculate next index number
          val childrenCenters = Array(2 * idx, 2 * idx + 1)
            .filter(x => bcNewCenters.value.contains(x)).map(bcNewCenters.value(_))
          if (childrenCenters.length == 2) {
            val closestIndex = findClosestCenter(bcMetric.value)(childrenCenters)(point)
            val nextIndex = 2 * idx + closestIndex

            // get a map value or else get a sparse vector
            val (sumBV, n, sumOfSquares) = map
              .getOrElse(
                nextIndex,
                (BSV.zeros[Double](point.size), 0.0, BSV.zeros[Double](point.size))
              )
            map(nextIndex) = (sumBV + point, n + 1.0, sumOfSquares + (point :* point))
          }
        }
        map.toIterator
      }.reduceByKey { case ((sv1, n1, sumOfSquares1), (sv2, n2, sumOfSquares2)) =>
        // sum the accumulation and the count in the all partition
        (sv1 + sv2, n1 + n2, sumOfSquares1 + sumOfSquares2)
      }.collect().toMap

      // calculate the center of each cluster
      newCenters = eachStats.map { case (idx, (sum, n, sumOfSquares)) => (idx, sum :/ n)}
      bcNewCenters = sc.broadcast(newCenters)

      // update summary of each cluster
      stats = eachStats.toMap

      totalStd = stats.map { case (idx, (sum, n, sumOfSquares)) =>
        breezeSum((sumOfSquares :/ n) :- breezeNorm(sum :/ n, 2.0))
      }.sum
      relativeError = math.abs(oldTotalStd - totalStd) / totalStd
      oldTotalStd = totalStd
      subIter += 1
    }
    stats.map { case (i, (sums, rows, sumOfSquares)) =>
      val mean = calcMean(rows.toLong, sums)
      val variance = getVariance(rows.toLong, sums, sumOfSquares)
      i -> new BisectingClusterStat(rows.toLong, mean, variance)
    }
  }

  /**
   * Creates the map of cluster stats to the map of cluster nodes with their costs
   *
   * @param data input data
   * @param stats map of cluster stats which is described as a binary tree
   */
  def createClusterNodes(
      data: RDD[(Long, BV[Double])],
      stats: Map[Long, BisectingClusterStat]): Map[Long, BisectingClusterNode] = {

    // TODO: support other cost, such as entropy
    createClusterNodesWithAverageCost(data, stats)
  }

  /**
   * Creates the map of cluster stats to the map of cluster nodes with their average costs
   */
  private def createClusterNodesWithAverageCost(
      data: RDD[(Long, BV[Double])],
      stats: Map[Long, BisectingClusterStat]): Map[Long, BisectingClusterNode] = {

    // calculate average costs of all clusters
    val bcCenters = data.sparkContext.broadcast(stats.map { case (i, stat) => i -> stat.mean })
    val costs = data.mapPartitions { iter =>
      val counters = mutable.Map.empty[Long, (Long, Double)]
      bcCenters.value.foreach {case (i, center) => counters(i) = (0L, 0.0)}
      iter.foreach { case (i, point) =>
        val cost = breezeNorm(bcCenters.value.apply(i) - point, 2.0)
        counters(i) = (counters(i)._1 + 1, counters(i)._2 + cost)
      }
      counters.toIterator
    }.reduceByKey { case((n1, cost1), (n2, cost2)) =>
      (n1 + n2, cost1 + cost2)
    }.collectAsMap()

    stats.map { case (i, stat) =>
      val avgCost = costs(i)._1 match {
        case x if x == 0.0 => 0.0
        case _ => costs(i)._2 / costs(i)._1
      }
      i -> new BisectingClusterNode(Vectors.fromBreeze(stat.mean), stat.rows, avgCost)
    }
  }

  /**
   * Builds a cluster tree from a Map of clusters
   *
   * @param treeMap divided clusters as a Map class
   * @param rootIndex index you want to start
   * @param numClusters the number of clusters you want
   * @return a built cluster tree
   */
  private def buildTree(
      treeMap: Map[Long, BisectingClusterNode],
      rootIndex: Long,
      numClusters: Int): Option[BisectingClusterNode] = {

    // if there is no index in the Map
    if (!treeMap.contains(rootIndex)) return None

    // build a cluster tree if the queue is empty or until the number of leaf clusters is enough
    var numLeavesClusters = 1
    val root = treeMap(rootIndex)
    var leavesQueue = Map(rootIndex -> root)
    while (leavesQueue.nonEmpty && numLeavesClusters < numClusters) {
      // pick up the largest cluster by the maximum cost of all the clusters
      val mostScattered = leavesQueue.maxBy(_._2.cost)
      val mostScatteredKey = mostScattered._1
      val mostScatteredCluster = mostScattered._2

      // relate the most scattered cluster to its children clusters
      val childrenIndexes = Array(2 * mostScatteredKey, 2 * mostScatteredKey + 1)
      if (childrenIndexes.forall(i => treeMap.contains(i))) {
        // insert children to the most scattered cluster
        val children = childrenIndexes.map(i => treeMap(i))
        mostScatteredCluster.insert(children)

        // calculate the local dendrogram height
        // TODO Supports distance metrics other Euclidean distance metric
        val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
        val localHeight = children
          .map(child => metric(child.center.toBreeze, mostScatteredCluster.center.toBreeze)).max
        mostScatteredCluster.setLocalHeight(localHeight)

        // update the queue
        leavesQueue = leavesQueue ++ childrenIndexes.map(i => i -> treeMap(i)).toMap
        numLeavesClusters += 1
      }

      // remove the cluster which is involved to the cluster tree
      leavesQueue = leavesQueue.filterNot(_ == mostScattered)
    }
    Some(root)
  }
}

/**
 * A cluster as a tree node which can have its sub nodes
 *
 * @param center the center of the cluster
 * @param rows the number of rows in the cluster
 * @param cost how large a cluster is
 * @param localHeight the maximal distance between this node and its children
 * @param parent the parent cluster of the cluster
 * @param children the children nodes of the cluster
 */
@Since("1.6.0")
class BisectingClusterNode private (
    @Since("1.6.0") val center: Vector,
    @Since("1.6.0") val rows: Long,
    @Since("1.6.0") val cost: Double,
    private var localHeight: Double,
    private var parent: Option[BisectingClusterNode],
    private var children: Seq[BisectingClusterNode]) extends Serializable {

  require(!cost.isNaN)

  @Since("1.6.0")
  def this(center: Vector, rows: Long, cost: Double) =
    this(center, rows, cost, 0.0, None, Array.empty[BisectingClusterNode])

  /**
   * Inserts a sub node as its child
   *
   * @param child inserted sub node
   */
  @Since("1.6.0")
  def insert(child: BisectingClusterNode) {
    insert(Array(child))
  }

  /**
   * Inserts sub nodes as its children
   *
   * @param children inserted sub nodes
   */
  @Since("1.6.0")
  def insert(children: Array[BisectingClusterNode]) {
    this.children = this.children ++ children
    children.foreach(child => child.parent = Some(this))
  }

  /**
   * Converts the tree into Array class
   * the sub nodes are recursively expanded
   *
   * @return an Array class which the cluster tree is expanded
   */
  @Since("1.6.0")
  def toArray: Array[BisectingClusterNode] = {
    val array = this.children.size match {
      case 0 => Array(this)
      case _ => Array(this) ++ this.children.flatMap(child => child.toArray.toIterator)
    }
    array.sortWith { case (a, b) =>
      a.getDepth < b.getDepth && a.cost < b.cost && a.rows < b.rows
    }
  }

  /**
   * Gets the depth of the cluster in the tree
   *
   * @return the depth from the root
   */
  @Since("1.6.0")
  def getDepth: Int = {
    this.parent match {
      case None => 0
      case _ => 1 + this.parent.get.getDepth
    }
  }

  /**
   * Gets the leaves nodes in the cluster tree
   */
  @Since("1.6.0")
  def getLeavesNodes: Array[BisectingClusterNode] = {
    this.toArray.filter(_.isLeaf).sortBy(_.center.toArray.sum)
  }

  @Since("1.6.0")
  def isLeaf: Boolean = this.children.isEmpty

  @Since("1.6.0")
  def getParent: Option[BisectingClusterNode] = this.parent

  @Since("1.6.0")
  def getChildren: Seq[BisectingClusterNode] = this.children

  /**
   * Gets the dendrogram height of the cluster at the cluster tree.
   * A dendrogram height is different from a local height.
   * A dendrogram height means a total height of a node in a tree.
   * A local height means a maximum distance between a node and its children.
   *
   * @return the dendrogram height
   */
  @Since("1.6.0")
  def getHeight: Double = {
    this.children.size match {
      case 0 => 0.0
      case _ => this.localHeight + this.children.map(_.getHeight).max
    }
  }

  @Since("1.6.0")
  def setLocalHeight(height: Double): Unit = this.localHeight = height

  /**
   * Converts to an adjacency list
   *
   * @return List[(fromNodeId, toNodeId, distance)]
   */
  @Since("1.6.0")
  def toAdjacencyList: Array[(Int, Int, Double)] = {
    val nodes = toArray

    var adjacencyList = Array.empty[(Int, Int, Double)]
    nodes.foreach { parent =>
      if (parent.children.size > 1) {
        val parentIndex = nodes.indexOf(parent)
        parent.children.foreach { child =>
          val childIndex = nodes.indexOf(child)
          adjacencyList = adjacencyList :+(parentIndex, childIndex, parent.localHeight)
        }
      }
    }
    adjacencyList
  }

  /**
   * Converts to a linkage matrix
   * Returned data format is fit for scipy's dendrogram function
   *
   * @return List[(node1, node2, distance, tree size)]
   */
  @Since("1.6.0")
  def toLinkageMatrix: Array[(Int, Int, Double, Int)] = {
    val nodes = toArray.sortWith { case (a, b) => a.getHeight < b.getHeight}
    val leaves = nodes.filter(_.isLeaf)
    val notLeaves = nodes.filterNot(_.isLeaf).filter(_.getChildren.size > 1)
    val clusters = leaves ++ notLeaves
    val treeMap = clusters.zipWithIndex.map { case (node, idx) => node -> idx}.toMap

    // If a node only has one-child, the child is regarded as the cluster of the child.
    // Cluster A has cluster B and Cluster B. B is a leaf. C only has cluster D.
    // ==> A merge list is (B, D), not (B, C).
    def getIndex(map: Map[BisectingClusterNode, Int], node: BisectingClusterNode): Int = {
      node.children.size match {
        case 1 => getIndex(map, node.children.head)
        case _ => map(node)
      }
    }
    clusters.filterNot(_.isLeaf).map { node =>
      (getIndex(treeMap, node.children.head),
        getIndex(treeMap, node.children(1)),
        node.getHeight,
        node.toArray.filter(_.isLeaf).length)
    }
  }
}


/**
 *  This class is used for maneging a cluster statistics
 *
 * @param rows the number of points
 * @param mean the sum of points
 * @param variance the sum of squares of points
 */
private[clustering] case class BisectingClusterStat (
    rows: Long,
    mean: BV[Double],
    variance: Double) extends Serializable {

  def isDividable: Boolean = variance > 0 && rows >= 2
}

private[clustering] object BisectingClusterStat {
  // calculate a mean vector
  def calcMean(rows: Long, sums: BV[Double]): BV[Double] = sums :/ rows.toDouble

  // calculate a variance
  def getVariance(rows: Long, sums: BV[Double], sumOfSquares: BV[Double]): Double = {
    val variances: BV[Double] = rows match {
      case n if n > 1 => sumOfSquares.:/(n.toDouble) - (sums :* sums).:/(n.toDouble * n.toDouble)
      case _ => BV.zeros[Double](sums.size)
    }
    breezeNorm(variances, 2.0)
  }
}


