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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => breezeNorm}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{Logging, SparkException}

import scala.collection.{Map, mutable}


object HierarchicalClustering extends Logging {

  private[clustering] val ROOT_INDEX_KEY: Long = 1

  /**
   * Finds the closes cluster's center
   *
   * @param metric a distance metric
   * @param centers centers of the clusters
   * @param point a target point
   * @return an index of the array of clusters
   */
  private[mllib]
  def findClosestCenter(metric: Function2[BV[Double], BV[Double], Double])
        (centers: Seq[BV[Double]])(point: BV[Double]): Int = {
    val (closestCenter, closestIndex) =
      centers.zipWithIndex.map { case (center, idx) => (metric(center, point), idx)}.minBy(_._1)
    closestIndex
  }
}

/**
 * This is a divisive hierarchical clustering algorithm based on bi-sect k-means algorithm.
 *
 * The main idea of this algorithm is based on "A comparison of document clustering techniques",
 * M. Steinbach, G. Karypis and V. Kumar. Workshop on Text Mining, KDD, 2000.
 * http://cs.fit.edu/~pkc/classes/ml-internet/papers/steinbach00tr.pdf
 *
 * @param numClusters tne number of clusters you want
 * @param clusterMap the pairs of cluster and its index as Map
 * @param maxIterations the number of maximal iterations
 * @param maxRetries the number of maximum retries
 * @param seed a random seed
 */
class HierarchicalClustering private (
  private var numClusters: Int,
  private var clusterMap: Map[Long, ClusterTree],
  private var maxIterations: Int,
  private var maxRetries: Int,
  private var seed: Long) extends Logging {

  /**
   * Constructs with the default configuration
   */
  def this() = this(20, mutable.ListMap.empty[Long, ClusterTree], 20, 10, 1)

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

  def getSubIterations: Int = this.maxIterations

  /**
   * Sets the number of maximum retries of each clustering step
   */
  def setMaxRetries(maxRetries: Int): this.type = {
    this.maxRetries = maxRetries
    this
  }

  def getMaxRetries: Int = this.maxRetries

  /**
   * Sets the random seed
   */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  def getSeed: Long = this.seed

  /**
   * Runs the hierarchical clustering algorithm
   * @param input RDD of vectors
   * @return model for the hierarchical clustering
   */
  def run(input: RDD[Vector]): HierarchicalClusteringModel = {
    val sc = input.sparkContext
    log.info(s"${sc.appName} starts a hierarchical clustering algorithm")

    var data = initData(input).cache()
    val startTime = System.currentTimeMillis()

    // `clusters` is described as binary tree structure
    // `clusters(1)` means the root of a binary tree
    var clusters = summarizeAsClusters(data)
    var leafClusters = clusters
    var step = 1
    var numDividedClusters = 0
    var noMoreDividable = false
    var rddArray = Array.empty[RDD[(Long, BV[Double])]]
    // the number of maximum nodes of a binary tree by given parameter
    val multiplier = math.ceil(math.log10(this.numClusters) / math.log10(2.0)) + 1
    val maxAllNodesInTree = math.pow(2, multiplier).toInt

    while (clusters.size < maxAllNodesInTree && noMoreDividable == false) {
      log.info(s"${sc.appName} starts step ${step}")

      // enough to be clustered if the number of divided clusters is equal to 0
      val divided = getDividedClusters(data, leafClusters)
      if (divided.size == 0) {
        noMoreDividable = true
      }
      else {
        // update each index
        val newData = updateClusterIndex(data, divided).cache()
        rddArray = rddArray ++ Array(data)
        data = newData

        // keep recent 2 cached RDDs in order to run more quickly
        if (rddArray.size > 1) {
          val head = rddArray.head
          head.unpersist()
          rddArray = rddArray.filterNot(_.hashCode() == head.hashCode())
        }

        // merge the divided clusters with the map as the cluster tree
        clusters = clusters ++ divided
        numDividedClusters = data.map(_._1).distinct().count().toInt
        leafClusters = divided
        step += 1

        log.info(s"${sc.appName} adding ${divided.size} new clusters at step:${step}")
      }
    }
    // unpersist kept RDDs
    rddArray.foreach(_.unpersist())

    // build a cluster tree by Map class which is expressed
    log.info(s"Building the cluster tree is started in ${sc.appName}")
    val root = buildTree(clusters, HierarchicalClustering.ROOT_INDEX_KEY, this.numClusters)
    if (root == None) {
      new SparkException("Failed to build a cluster tree from a Map type of clusters")
    }

    // set the elapsed time for training
    val finishTime = (System.currentTimeMillis() - startTime) / 1000.0
    log.info(s"Elapsed Time for Hierarchical Clustering Training: ${finishTime} [sec]")

    // make a hierarchical clustering model
    val model = new HierarchicalClusteringModel(root.get)
    val leavesNodes = model.getClusters()
    if (leavesNodes.size < this.numClusters) {
      log.warn(s"# clusters is less than you have expected: ${leavesNodes.size} / ${numClusters}. ")
    }
    model
  }

  /**
   * Assigns the initial cluster index id to all data
   */
  private[clustering]
  def initData(data: RDD[Vector]): RDD[(Long, BV[Double])] = {
    data.map { v: Vector => (HierarchicalClustering.ROOT_INDEX_KEY, v.toBreeze)}.cache
  }

  /**
   * Summarizes data by each cluster as ClusterTree2 classes
   */
  private[clustering]
  def summarizeAsClusters(data: RDD[(Long, BV[Double])]): Map[Long, ClusterTree] = {
    // summarize input data
    val stats = summarize(data)

    // convert statistics to ClusterTree class
    stats.map { case (i, (sum, n, sumOfSquares)) =>
      val center = Vectors.fromBreeze(sum :/ n)
      val variances = n match {
        case n if n > 1 => Vectors.fromBreeze(sumOfSquares.:*(n) - (sum :* sum) :/ (n * (n - 1.0)))
        case _ => Vectors.zeros(sum.size)
      }
      (i, new ClusterTree(center, n.toLong, variances))
    }.toMap
  }

  /**
   * Summarizes data by each cluster as Map
   */
  private[clustering]
  def summarize(data: RDD[(Long, BV[Double])]): Map[Long, (BV[Double], Double, BV[Double])] = {
    data.mapPartitions { iter =>
      // calculate the accumulation of the all point in a partition and count the rows
      val map = mutable.Map.empty[Long, (BV[Double], Double, BV[Double])]
      iter.foreach { case (idx: Long, point: BV[Double]) =>
        // get a map value or else get a sparse vector
        val (sumBV, n, sumOfSquares) = map.get(idx)
            .getOrElse(BSV.zeros[Double](point.size), 0.0, BSV.zeros[Double](point.size))
        map(idx) = (sumBV + point, n + 1.0, sumOfSquares + (point :* point))
      }
      map.toIterator
    }.reduceByKey { case ((sum1, n1, sumOfSquares1), (sum2, n2, sumOfSquares2)) =>
      // sum the accumulation and the count in the all partition
      (sum1 + sum2, n1 + n2, sumOfSquares1 + sumOfSquares2)
    }.collect().toMap
  }

  /**
   * Gets the initial centers for bi-sect k-means
   */
  private[clustering]
  def initChildrenCenter(clusters: Map[Long, BV[Double]]): Map[Long, BV[Double]] = {
    val rand = new XORShiftRandom()
    rand.setSeed(this.seed)

    clusters.flatMap { case (idx, center) =>
      val childrenIndexes = Array(2 * idx, 2 * idx + 1)
      val relativeErrorCoefficient = 0.001
      Array(
        (2 * idx, center.map(elm => elm - (elm * relativeErrorCoefficient * rand.nextDouble()))),
        (2 * idx + 1, center.map(elm => elm + (elm * relativeErrorCoefficient * rand.nextDouble())))
      )
    }.toMap
  }

  /**
   * Gets the new divided centers
   */
  private[clustering]
  def getDividedClusters(data: RDD[(Long, BV[Double])],
    dividedClusters: Map[Long, ClusterTree]): Map[Long, ClusterTree] = {
    val sc = data.sparkContext
    val appName = sc.appName

    // get keys of dividable clusters
    val dividableKeys = dividedClusters.filter { case (idx, cluster) =>
      cluster.variances.toArray.sum > 0.0 && cluster.records >= 2
    }.keySet
    if (dividableKeys.size == 0) {
      log.info(s"There is no dividable clusters in ${appName}.")
      return Map.empty[Long, ClusterTree]
    }

    // divide input data
    var dividableData = data.filter { case (idx, point) => dividableKeys.contains(idx)}
    var dividableClusters = dividedClusters.filter { case (k, v) => dividableKeys.contains(k)}
    val idealIndexes = dividableKeys.flatMap(idx => Array(2 * idx, 2 * idx + 1).toIterator)
    var stats = divide(data, dividableClusters)

    // if there is clusters which is failed to be divided,
    // retry to divide only failed clusters again and again
    var tryTimes = 1
    while (stats.size < dividableKeys.size * 2 && tryTimes <= this.maxRetries) {
      // get the indexes of clusters which is failed to be divided
      val failedIndexes = idealIndexes.filterNot(stats.keySet.contains).map(idx => (idx / 2).toLong)
      val failedCenters = dividedClusters.filter { case (idx, clstr) => failedIndexes.contains(idx)}
      log.info(s"# failed clusters is ${failedCenters.size} of ${dividableKeys.size}" +
          s"at ${tryTimes} times in ${appName}")

      // divide the failed clusters again
      sc.broadcast(failedIndexes)
      dividableData = data.filter { case (idx, point) => failedIndexes.contains(idx)}
      val missingStats = divide(dividableData, failedCenters)
      stats = stats ++ missingStats
      tryTimes += 1
    }

    // make children clusters
    stats.filter { case (i, (sum, n, sumOfSquares)) => n > 0}
        .map { case (i, (sum, n, sumOfSquares)) =>
      val center = Vectors.fromBreeze(sum :/ n)
      val variances = n match {
        case 1 => Vectors.sparse(sum.size, Array(), Array())
        case _ => Vectors.fromBreeze(sumOfSquares.:*(n) - (sum :* sum) :/ (n * (n - 1.0)))
      }
      val child = new ClusterTree(center, n.toLong, variances)
      (i, child)
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
  def buildTree(treeMap: Map[Long, ClusterTree],
    rootIndex: Long,
    numClusters: Int): Option[ClusterTree] = {

    // if there is no index in the Map
    if (!treeMap.contains(rootIndex)) return None

    // build a cluster tree if the queue is empty or until the number of leaves clusters is enough
    var numLeavesClusters = 1
    val root = treeMap(rootIndex)
    var leavesQueue = Map(rootIndex -> root)
    while (leavesQueue.size > 0 && numLeavesClusters < numClusters) {
      // pick up the cluster whose variance is the maximum in the queue
      val mostScattered = leavesQueue.maxBy(_._2.variancesNorm)
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
        leavesQueue = leavesQueue ++ childrenIndexes.map(i => (i -> treeMap(i))).toMap
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
   * Divides the input data
   *
   * @param data the pairs of cluster index and point which you want to divide
   * @param clusters the clusters you want to divide AS a Map class
   * @return divided clusters as Map
   */
  private[clustering]
  def divide(data: RDD[(Long, BV[Double])],
    clusters: Map[Long, ClusterTree]): Map[Long, (BV[Double], Double, BV[Double])] = {

    val sc = data.sparkContext
    val centers = clusters.map { case (idx, cluster) => (idx, cluster.center.toBreeze)}
    var newCenters = initChildrenCenter(centers)
    if (newCenters.size == 0) {
      return Map.empty[Long, (BV[Double], Double, BV[Double])]
    }
    sc.broadcast(newCenters)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    sc.broadcast(metric)

    val vectorSize = newCenters(newCenters.keySet.min).size
    var stats = newCenters.keys.map { idx =>
      (idx, (BSV.zeros[Double](vectorSize).toVector, 0.0, BSV.zeros[Double](vectorSize).toVector))
    }.toMap

    var subIter = 0
    var diffVariances = Double.MaxValue
    var oldVariances = Double.MaxValue
    var variances = Double.MaxValue
    while (subIter < this.maxIterations && diffVariances > 10E-4) {
      // calculate summary of each cluster
      val eachStats = data.mapPartitions { iter =>
        val map = mutable.Map.empty[Long, (BV[Double], Double, BV[Double])]
        iter.foreach { case (idx, point) =>
          // calculate next index number
          val childrenCenters = Array(2 * idx, 2 * idx + 1).filter(newCenters.keySet.contains(_))
              .map(newCenters(_)).toArray
          if (childrenCenters.size >= 1) {
            val closestIndex =
              HierarchicalClustering.findClosestCenter(metric)(childrenCenters)(point)
            val nextIndex = 2 * idx + closestIndex

            // get a map value or else get a sparse vector
            val (sumBV, n, sumOfSquares) = map.get(nextIndex)
                .getOrElse(BSV.zeros[Double](point.size), 0.0, BSV.zeros[Double](point.size))
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

      // update summary of each cluster
      stats = eachStats.toMap

      variances = stats.map { case (idx, (sum, n, sumOfSquares)) =>
        math.pow(sumOfSquares.toArray.sum, 1.0 / sumOfSquares.size)
      }.sum
      diffVariances = math.abs(oldVariances - variances) / oldVariances
      oldVariances = variances
      subIter += 1
    }
    stats
  }

  /**
   * Updates the indexes of clusters which is divided to its children indexes
   */
  private[clustering]
  def updateClusterIndex(
    data: RDD[(Long, BV[Double])],
    dividedClusters: Map[Long, ClusterTree]): RDD[(Long, BV[Double])] = {
    // extract the centers of the clusters
    val sc = data.sparkContext
    var centers = dividedClusters.map { case (idx, cluster) => (idx, cluster.center)}
    sc.broadcast(centers)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    sc.broadcast(metric)

    // update the indexes to their children indexes
    data.map { case (idx, point) =>
      val childrenIndexes = Array(2 * idx, 2 * idx + 1).filter(centers.keySet.contains(_))
      childrenIndexes.size match {
        // stay the index if the number of children is not enough
        case s if s < 2 => (idx, point)
        // update the indexes
        case _ => {
          val nextCenters = childrenIndexes.map(centers(_)).map(_.toBreeze)
          val closestIndex = HierarchicalClustering.findClosestCenter(metric)(nextCenters)(point)
          val nextIndex = 2 * idx + closestIndex
          (nextIndex, point)
        }
      }
    }
  }
}

/**
 * A cluster as a tree node which can have its sub nodes
 *
 * @param center the center of the cluster
 * @param records the number of rows in the cluster
 * @param variances variance vectors
 * @param variancesNorm the norm of variance vector
 * @param localHeight the maximal distance between this node and its children
 * @param parent the parent cluster of the cluster
 * @param children the children nodes of the cluster
 */
class ClusterTree private (
  val center: Vector,
  val records: Long,
  val variances: Vector,
  val variancesNorm: Double,
  private var localHeight: Double,
  private var parent: Option[ClusterTree],
  private var children: Seq[ClusterTree]) extends Serializable {

  require(!variancesNorm.isNaN)

  def this(center: Vector, rows: Long, variances: Vector) =
    this(center, rows, variances, breezeNorm(variances.toBreeze, 2.0),
      0.0, None, Array.empty[ClusterTree])

  /**
   * Inserts sub nodes as its children
   *
   * @param children inserted sub nodes
   */
  def insert(children: Array[ClusterTree]) {
    this.children = this.children ++ children
    children.foreach(child => child.parent = Some(this))
  }

  /**
   * Inserts a sub node as its child
   *
   * @param child inserted sub node
   */
  def insert(child: ClusterTree) {
    insert(Array(child))
  }

  /**
   * Converts the tree into Array class
   * the sub nodes are recursively expanded
   *
   * @return an Array class which the cluster tree is expanded
   */
  def toArray(): Array[ClusterTree] = {
    val array = this.children.size match {
      case 0 => Array(this)
      case _ => Array(this) ++ this.children.flatMap(child => child.toArray().toIterator)
    }
    array.sortWith { case (a, b) =>
      a.getDepth() < b.getDepth() && a.variances.toArray.sum < b.variances.toArray.sum
    }
  }

  /**
   * Gets the depth of the cluster in the tree
   *
   * @return the depth from the root
   */
  def getDepth(): Int = {
    this.parent match {
      case None => 0
      case _ => 1 + this.parent.get.getDepth()
    }
  }

  /**
   * Gets the leaves nodes in the cluster tree
   */
  def getLeavesNodes(): Array[ClusterTree] = {
    this.toArray().filter(_.isLeaf()).sortBy(_.center.toArray.sum)
  }

  def isLeaf(): Boolean = (this.children.size == 0)

  def getParent(): Option[ClusterTree] = this.parent

  def getChildren(): Seq[ClusterTree] = this.children

  /**
   * Gets the dendrogram height of the cluster at the cluster tree
   *
   * @return the dendrogram height
   */
  def getHeight(): Double = {
    this.children.size match {
      case 0 => 0.0
      case _ => this.localHeight + this.children.map(_.getHeight()).max
    }
  }

  private[mllib]
  def setLocalHeight(height: Double) = (this.localHeight = height)
}
