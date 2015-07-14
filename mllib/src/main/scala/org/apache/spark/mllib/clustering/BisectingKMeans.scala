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

import scala.collection.{mutable, Map}

import breeze.linalg.{SparseVector => BSV, Vector => BV, norm => breezeNorm, any => breezeAny}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkException}


object BisectingKMeans extends Logging {

  private[clustering] val ROOT_INDEX_KEY: BigInt = 1

  /**
   * Finds the closes cluster's center
   *
   * @param metric a distance metric
   * @param centers centers of the clusters
   * @param point a target point
   * @return an index of the array of clusters
   */
  private[mllib]
  def findClosestCenter(metric: (BV[Double], BV[Double]) => Double)
        (centers: Seq[BV[Double]])(point: BV[Double]): Int = {
    val (closestCenter, closestIndex) =
      centers.zipWithIndex.map { case (center, idx) => (metric(center, point), idx)}.minBy(_._1)
    closestIndex
  }
}

/**
 * This is a divisive hierarchical clustering algorithm based on bisecting k-means algorithm.
 *
 * The main idea of this algorithm is based on "A comparison of document clustering techniques",
 * M. Steinbach, G. Karypis and V. Kumar. Workshop on Text Mining, KDD, 2000.
 * http://cs.fit.edu/~pkc/classes/ml-internet/papers/steinbach00tr.pdf
 *
 * @param numClusters tne number of clusters you want
 * @param clusterMap the pairs of cluster and its index as Map
 * @param maxIterations the number of maximal iterations
 * @param seed a random seed
 */
class BisectingKMeans private (
    private var numClusters: Int,
    private var clusterMap: Map[BigInt, ClusterNode],
    private var maxIterations: Int,
    private var seed: Long) extends Logging {

  /**
   * Constructs with the default configuration
   */
  def this() = this(20, mutable.ListMap.empty[BigInt, ClusterNode], 20, 1)

  /**
   * Sets the number of clusters you want
   */
  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  def getNumClusters: Int = this.numClusters

  /**
   * Sets the number of maximal iterations in each clustering step
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  def getMaxIterations: Int = this.maxIterations

  /**
   * Sets the random seed
   */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  def getSeed: Long = this.seed

  /**
   * Runs the bisecting kmeans algorithm
   * @param input RDD of vectors
   * @return model for the bisecting kmeans
   */
  def run(input: RDD[Vector]): BisectingKMeansModel = {
    val sc = input.sparkContext
    log.info(s"${sc.appName} starts a bisecting kmeans algorithm")

    var data = initData(input).cache()
    val startTime = System.currentTimeMillis()

    // `clusterStats` is described as binary tree structure
    // `clusterStats(1)` means the root of a binary tree
    var clusterStats = mutable.Map.empty[BigInt, ClusterNodeStat]
    var step = 1
    var numDividedClusters = 0
    var noMoreDividable = false
    var rddArray = Array.empty[RDD[(BigInt, BV[Double])]]
    // the number of maximum nodes of a binary tree by given parameter
    val multiplier = math.ceil(math.log10(this.numClusters) / math.log10(2.0)) + 1
    val maxAllNodesInTree = math.pow(2, multiplier).toInt

    while (clusterStats.size < maxAllNodesInTree && noMoreDividable == false) {
      log.info(s"${sc.appName} starts step ${step}")
      val leafClusters = summarize(data)
      val dividableLeafClusters = leafClusters.filter(_._2.isDividable)
      clusterStats = clusterStats ++ leafClusters

      if (dividableLeafClusters.isEmpty) {
        noMoreDividable = true
      }
      else {
        // can be clustered if the number of divided clusterStats is equal to 0
        val divided = getDividedClusters(data, dividableLeafClusters)

        // update each index
        val newData = updateClusterIndex(data, divided).cache()
        rddArray = rddArray ++ Array(data)
        data = newData

        // keep recent 2 cached RDDs in order to run more quickly
        if (rddArray.length > 1) {
          val head = rddArray.head
          head.unpersist()
          rddArray = rddArray.filterNot(_.hashCode() == head.hashCode())
        }
        clusterStats = clusterStats ++ divided
        step += 1
        log.info(s"${sc.appName} adding ${divided.size} new clusterStats at step:${step}")
      }
    }
    // unpersist kept RDDs
    rddArray.foreach(_.unpersist())

    val nodes = calcCriterions(data, clusterStats)

    // build a cluster tree by Map class which is expressed
    log.info(s"Building the cluster tree is started in ${sc.appName}")
    val root = buildTree(nodes, BisectingKMeans.ROOT_INDEX_KEY, this.numClusters)
    if (root.isEmpty) {
      new SparkException("Failed to build a cluster tree from a Map type of clusterStats")
    }

    // set the elapsed time for training
    val finishTime = (System.currentTimeMillis() - startTime) / 1000.0
    log.info(s"Elapsed Time for ${this.getClass.getSimpleName} Training: ${finishTime} [sec]")

    // make a bisecting kmeans model
    val model = new BisectingKMeansModel(root.get)
    val leavesNodes = model.getClusters
    if (leavesNodes.length < this.numClusters) {
      log.warn(s"# clusterStats is less than you want: ${leavesNodes.length} / ${numClusters}")
    }
    model
  }

  /**
   * Assigns the initial cluster index id to all data
   */
  private[clustering]
  def initData(data: RDD[Vector]): RDD[(BigInt, BV[Double])] = {
    data.map { v: Vector => (BisectingKMeans.ROOT_INDEX_KEY, v.toBreeze)}
  }

  /**
   * Summarizes data by each cluster as ClusterTree classes
   */
  private[clustering]
  def calcCriterions(
      data: RDD[(BigInt, BV[Double])],
      stats: Map[BigInt, ClusterNodeStat]): Map[BigInt, ClusterNode] = {

    // TODO: support other criteria, such as entropy
    calcAvgConsts(data, stats)
  }

  private[clustering]
  def calcAvgConsts(
      data: RDD[(BigInt, BV[Double])],
      stats: Map[BigInt, ClusterNodeStat]): Map[BigInt, ClusterNode] = {

    val bcCenters = data.sparkContext.broadcast(stats.map { case (i, stat) => i -> stat.center })
    val costs = data.mapPartitions { iter =>
      val counters = mutable.Map.empty[BigInt, (Long, Double)]
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
      i -> new ClusterNode(Vectors.fromBreeze(stat.center), stat.rows, avgCost)
    }
  }

  /**
   * Summarizes data by each cluster as Map
   */
  private[clustering]
  def summarize(data: RDD[(BigInt, BV[Double])]): Map[BigInt, ClusterNodeStat] = {
    val stats = data.mapPartitions { iter =>
      // calculate the accumulation of the all point in a partition and count the rows
      val map = mutable.Map.empty[BigInt, (BV[Double], Double, BV[Double])]
      iter.foreach { case (idx: BigInt, point: BV[Double]) =>
        // get a map value or else get a sparse vector
        val (sumBV, n, sumOfSquares) = map
            .getOrElse(idx, (BSV.zeros[Double](point.size), 0.0, BSV.zeros[Double](point.size)))
        map(idx) = (sumBV + point, n + 1.0, sumOfSquares + (point :* point))
      }
      map.toIterator
    }.reduceByKey { case ((sum1, n1, sumOfSquares1), (sum2, n2, sumOfSquares2)) =>
      // sum the accumulation and the count in the all partition
      (sum1 + sum2, n1 + n2, sumOfSquares1 + sumOfSquares2)
    }.collect().toMap

    stats.map {case (i, stat) => i -> new ClusterNodeStat(stat._2.toLong, stat._1, stat._3)}
  }

  /**
   * Gets the new divided centers
   */
  private[clustering]
  def getDividedClusters(data: RDD[(BigInt, BV[Double])],
    leafClusters: Map[BigInt, ClusterNodeStat]): Map[BigInt, ClusterNodeStat] = {
    val sc = data.sparkContext
    val appName = sc.appName

    // get keys of dividable clusters
    val dividableClusters = leafClusters.filter { case (idx, cluster) => cluster.isDividable }
    if (dividableClusters.isEmpty) {
      log.info(s"There is no dividable clusters in ${appName}.")
      return Map.empty[BigInt, ClusterNodeStat]
    }

    // divide input data
    val dividableData = data.filter { case (idx, point) => dividableClusters.contains(idx)}
    divide(dividableData, dividableClusters)
  }

  /**
   * Divides the input data
   *
   * @param data the pairs of cluster index and point which you want to divide
   * @param currentStats the cluster stats you want to divide AS a Map class
   * @return divided clusters as Map
   */
  private[clustering]
  def divide(
      data: RDD[(BigInt, BV[Double])],
      currentStats: Map[BigInt, ClusterNodeStat]): Map[BigInt, ClusterNodeStat] = {

    val sc = data.sparkContext
    var newCenters = initChildCenters(data, currentStats)
    var bcNewCenters = sc.broadcast(newCenters)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val bcMetric = sc.broadcast(metric)

    val vectorSize = newCenters(newCenters.keySet.min).size
    var stats = Map.empty[BigInt, (BV[Double], Double, BV[Double])]

    var subIter = 0
    var diffVariances = Double.MaxValue
    var oldVariances = Double.MaxValue
    var variances = Double.MaxValue
    while (subIter < this.maxIterations && diffVariances > 10E-4) {
      // calculate summary of each cluster
      val eachStats = data.mapPartitions { iter =>
        val map = mutable.Map.empty[BigInt, (BV[Double], Double, BV[Double])]
        iter.foreach { case (idx, point) =>
          // calculate next index number
          val childrenCenters = Array(2 * idx, 2 * idx + 1)
              .filter(x => bcNewCenters.value.contains(x)).map(bcNewCenters.value(_))
          if (childrenCenters.length >= 1) {
            val closestIndex =
              BisectingKMeans.findClosestCenter(bcMetric.value)(childrenCenters)(point)
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

      variances = stats.map { case (idx, (sum, n, sumOfSquares)) =>
        math.pow(sumOfSquares.toArray.sum, 1.0 / sumOfSquares.size)
      }.sum
      diffVariances = math.abs(oldVariances - variances) / oldVariances
      oldVariances = variances
      subIter += 1
    }

    stats.map { case (i, stat) => i -> new ClusterNodeStat(stat._2.toLong, stat._1, stat._3) }
  }

  /**
   * Gets the initial centers for bisect k-means
   */
  private[clustering]
  def initChildCenters(
      data: RDD[(BigInt, BV[Double])],
      stats: Map[BigInt, ClusterNodeStat]): Map[BigInt, BV[Double]] = {

    // Since the combination sampleByKey and groupByKey is more expensive,
    // this as follows would be better.
    val bcIndeces = data.sparkContext.broadcast(stats.keySet)
    val samples = data.mapPartitions { iter =>
      val map = mutable.Map.empty[BigInt, mutable.ArrayBuffer[BV[Double]]]

      bcIndeces.value.foreach {i => map(i) = mutable.ArrayBuffer.empty[BV[Double]]}
      val LOCAL_SAMPLE_SIZE = 20
      iter.foreach { case (i, point) =>
        map(i).append(point)
        // to avoid to increase the memory usage on each map thread,
        // the number of elements is cut off at the right time.
        if (map(i).size > LOCAL_SAMPLE_SIZE) {
          val elements = map(i).toSeq.sortWith((a, b) => breezeNorm(a, 1.0) < breezeNorm(b, 1.0))
          map(i) = mutable.ArrayBuffer(elements.head, elements.last)
        }
      }

      // in order to reduce the shuffle size, take only two elements
      map.filterNot(_._2.isEmpty).map { case (i, points) =>
        val elements = map(i).toSeq.sortWith((a, b) => breezeNorm(a, 1.0) < breezeNorm(b, 1.0))
        i -> mutable.ArrayBuffer(elements.head, elements.last)
      }.toIterator
    }.reduceByKey { case (points1, points2) =>
      points1.union(points2)
    }.collect()

    samples.flatMap { case (i, points) =>
      val elements = points.toSeq.sortWith((a, b) => breezeNorm(a, 1.0) < breezeNorm(b, 1.0))
      Array((2 * i, elements.head), (2 * i + 1, elements.last))
    }.toMap
  }

  /**
   * Builds a cluster tree from a Map of clusters
   *
   * @param treeMap divided clusters as a Map class
   * @param rootIndex index you want to start
   * @param numClusters the number of clusters you want
   * @return a built cluster tree
   */
  private[clustering]
  def buildTree(
      treeMap: Map[BigInt, ClusterNode],
      rootIndex: BigInt,
      numClusters: Int): Option[ClusterNode] = {

    // if there is no index in the Map
    if (!treeMap.contains(rootIndex)) return None

    // build a cluster tree if the queue is empty or until the number of leaf clusters is enough
    var numLeavesClusters = 1
    val root = treeMap(rootIndex)
    var leavesQueue = Map(rootIndex -> root)
    while (leavesQueue.nonEmpty && numLeavesClusters < numClusters) {
      // pick up the cluster whose variance is the maximum in the queue
      val mostScattered = leavesQueue.maxBy(_._2.criterion)
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

      log.info(s"Total Leaves Clusters: ${numLeavesClusters} / ${numClusters}. " +
          s"Cluster ${childrenIndexes.mkString(",")} are merged.")
    }
    Some(root)
  }

  /**
   * Updates the indexes of clusters which is divided to its children indexes
   */
  private[clustering]
  def updateClusterIndex(
      data: RDD[(BigInt, BV[Double])],
      dividedClusters: Map[BigInt, ClusterNodeStat]): RDD[(BigInt, BV[Double])] = {

    // extract the centers of the clusters
    val sc = data.sparkContext
    var centers = dividedClusters.map { case (idx, cluster) => (idx, cluster.center)}
    val bcCenters = sc.broadcast(centers)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val bcMetric = sc.broadcast(metric)

    // update the indexes to their children indexes
    data.map { case (idx, point) =>
      val childrenIndexes = Array(2 * idx, 2 * idx + 1).filter(c => bcCenters.value.contains(c))
      childrenIndexes.length match {
        // stay the index if the number of children is not enough
        case s if s < 2 => (idx, point)
        // update the indexes
        case _ => {
          val nextCenters = childrenIndexes.map(bcCenters.value(_))
          val closestIndex = BisectingKMeans
              .findClosestCenter(bcMetric.value)(nextCenters)(point)
          val nextIndex = 2 * idx + closestIndex
          (nextIndex, point)
        }
      }
    }
  }
}

private[this]
case class ClusterNodeStat (
    rows: Long,
    sums: BV[Double],
    sumOfSquares: BV[Double]) extends Serializable {

  // initialization
  val center: BV[Double] = sums :/ rows.toDouble
  val variances: BV[Double] = rows match {
    case n if n > 1 => sumOfSquares.:/(n.toDouble) - (sums :* sums).:/(n.toDouble * n.toDouble)
    case _ => BV.zeros[Double](sums.size)
  }

  def isDividable: Boolean = breezeAny(variances) && rows >= 2
}

/**
 * A cluster as a tree node which can have its sub nodes
 *
 * @param center the center of the cluster
 * @param rows the number of rows in the cluster
 * @param criterion the norm of variance vector
 * @param localHeight the maximal distance between this node and its children
 * @param parent the parent cluster of the cluster
 * @param children the children nodes of the cluster
 */
class ClusterNode private (
    val center: Vector,
    val rows: Long,
    val criterion: Double,
    private var localHeight: Double,
    private var parent: Option[ClusterNode],
    private var children: Seq[ClusterNode]) extends Serializable {

  require(!criterion.isNaN)

  def this(center: Vector, rows: Long, criterion: Double) =
    this(center, rows, criterion, 0.0, None, Array.empty[ClusterNode])

  /**
   * Inserts a sub node as its child
   *
   * @param child inserted sub node
   */
  def insert(child: ClusterNode) {
    insert(Array(child))
  }

  /**
   * Inserts sub nodes as its children
   *
   * @param children inserted sub nodes
   */
  def insert(children: Array[ClusterNode]) {
    this.children = this.children ++ children
    children.foreach(child => child.parent = Some(this))
  }

  /**
   * Converts the tree into Array class
   * the sub nodes are recursively expanded
   *
   * @return an Array class which the cluster tree is expanded
   */
  def toArray: Array[ClusterNode] = {
    val array = this.children.size match {
      case 0 => Array(this)
      case _ => Array(this) ++ this.children.flatMap(child => child.toArray.toIterator)
    }
    array.sortWith { case (a, b) =>
      a.getDepth < b.getDepth && a.criterion < b.criterion && a.rows < b.rows
    }
  }

  /**
   * Gets the depth of the cluster in the tree
   *
   * @return the depth from the root
   */
  def getDepth: Int = {
    this.parent match {
      case None => 0
      case _ => 1 + this.parent.get.getDepth
    }
  }

  /**
   * Gets the leaves nodes in the cluster tree
   */
  def getLeavesNodes: Array[ClusterNode] = {
    this.toArray.filter(_.isLeaf).sortBy(_.center.toArray.sum)
  }

  def isLeaf: Boolean = this.children.isEmpty

  def getParent: Option[ClusterNode] = this.parent

  def getChildren: Seq[ClusterNode] = this.children

  /**
   * Gets the dendrogram height of the cluster at the cluster tree.
   * A dendrogram height is different from a local height.
   * A dendrogram height means a total height of a node in a tree.
   * A local height means a maximum distance between a node and its children.
   *
   * @return the dendrogram height
   */
  def getHeight: Double = {
    this.children.size match {
      case 0 => 0.0
      case _ => this.localHeight + this.children.map(_.getHeight).max
    }
  }

  private[mllib]
  def setLocalHeight(height: Double) = this.localHeight = height

  /**
   * Converts to an adjacency list
   *
   * @return List[(fromNodeId, toNodeId, distance)]
   */
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
  def toLinkageMatrix: Array[(Int, Int, Double, Int)] = {
    val nodes = toArray.sortWith { case (a, b) => a.getHeight < b.getHeight}
    val leaves = nodes.filter(_.isLeaf)
    val notLeaves = nodes.filterNot(_.isLeaf).filter(_.getChildren.size > 1)
    val clusters = leaves ++ notLeaves
    val treeMap = clusters.zipWithIndex.map { case (node, idx) => node -> idx}.toMap

    // If a node only has one-child, the child is regarded as the cluster of the child.
    // Cluster A has cluster B and Cluster B. B is a leaf. C only has cluster D.
    // ==> A merge list is (B, D), not (B, C).
    def getIndex(map: Map[ClusterNode, Int], node: ClusterNode): Int = {
      node.children.size match {
        case 1 => getIndex(map, node.children.head)
        case _ => map(node)
      }
    }
    clusters.filterNot(_.isLeaf).map { node =>
      (getIndex(treeMap, node.children.head),
          getIndex(treeMap, node.children(1)),
          node.getHeight,
          node.toArray.filter(_.isLeaf).size)
    }
  }
}
