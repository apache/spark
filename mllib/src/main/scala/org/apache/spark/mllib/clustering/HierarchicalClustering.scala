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
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

/**
 * This trait is used for the configuration of the hierarchical clustering
 */
sealed
trait HierarchicalClusteringConf extends Serializable {
  this: HierarchicalClustering =>

  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  def getNumClusters(): Int = this.numClusters

  def setNumRetries(numRetries: Int): this.type = {
    this.numRetries = numRetries
    this
  }

  def getNumRetries(): Int = this.numRetries

  def setSubIterations(subIterations: Int): this.type = {
    this.subIterations = subIterations
    this
  }

  def getSubIterations(): Int = this.subIterations

  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  def getEpsilon(): Double = this.epsilon

  def setRandomSeed(seed: Int): this.type = {
    this.randomSeed = seed
    this
  }

  def getRandomSeed(): Int = this.randomSeed

  def setRandomRange(range: Double): this.type = {
    this.randomRange = range
    this
  }
}


/**
 * This is a divisive hierarchical clustering algorithm based on bi-sect k-means algorithm.
 *
 * The main idea of this algorithm is derived from:
 * "A comparison of document clustering techniques",
 * M. Steinbach, G. Karypis and V. Kumar. Workshop on Text Mining, KDD, 2000.
 * http://cs.fit.edu/~pkc/classes/ml-internet/papers/steinbach00tr.pdf
 *
 * @param numClusters the number of clusters you want
 * @param subIterations the number of iterations at digging
 * @param epsilon the threshold to stop the sub-iterations
 * @param randomSeed uses in sampling data for initializing centers in each sub iterations
 * @param randomRange the range coefficient to generate random points in each clustering step
 */
class HierarchicalClustering(
  private[mllib] var numClusters: Int,
  private[mllib] var subIterations: Int,
  private[mllib] var numRetries: Int,
  private[mllib] var epsilon: Double,
  private[mllib] var randomSeed: Int,
  private[mllib] var randomRange: Double)
    extends Serializable with Logging with HierarchicalClusteringConf {

  /**
   * Constructs with the default configuration
   */
  def this() = this(20, 20, 10, 10E-4, 1, 0.1)

  /** Shows the parameters */
  override def toString(): String = {
    Array(
      s"numClusters:${numClusters}",
      s"subIterations:${subIterations}",
      s"numRetries:${numRetries}",
      s"epsilon:${epsilon}",
      s"randomSeed:${randomSeed}",
      s"randomRange:${randomRange}"
    ).mkString(", ")
  }

  /**
   * Trains a hierarchical clustering model with the given configuration
   *
   * @param data training points
   * @return a model for hierarchical clustering
   */
  def run(data: RDD[Vector]): HierarchicalClusteringModel = {
    validateData(data)
    logInfo(s"Run with ${this}")

    val startTime = System.currentTimeMillis() // to measure the execution time
    val clusterTree = ClusterTree.fromRDD(data) // make the root node
    val model = new HierarchicalClusteringModel(clusterTree)
    val statsUpdater = new ClusterTreeStatsUpdater()

    var node: Option[ClusterTree] = Some(model.clusterTree)
    statsUpdater(node.get)

    // If the followed conditions are satisfied, and then stop the training.
    //   1. There is no splittable cluster
    //   2. The number of the splitted clusters is greater than that of given clusters
    var totalVariance = Double.MaxValue
    var newTotalVariance = model.clusterTree.getVariance().get
    var step = 1
    while (node != None
        && model.clusterTree.getTreeSize() < this.numClusters) {

      // split some times in order not to be wrong clustering result
      var isMerged = false
      for (i <- 1 to this.numRetries) {
        if (node.get.getVariance().get > this.epsilon && isMerged == false) {
          var subNodes = split(node.get).map(subNode => statsUpdater(subNode))
          if (subNodes.size == 2) {
            // insert the nodes to the tree
            node.get.insert(subNodes.toList)
            // calculate the local dendrogram height
            val dist = breezeNorm(subNodes(0).center.toBreeze - subNodes(1).center.toBreeze, 2)
            node.get.height = Some(dist)
            // unpersist unnecessary cache because its children nodes are cached
            node.get.data.unpersist()
            logInfo(s"the number of cluster is ${model.clusterTree.getTreeSize()} at step ${step}")
            isMerged = true
          }
        }
      }
      node.get.isVisited = true

      // update the total variance and select the next splittable node
      node = nextNode(model.clusterTree)
      step += 1
    }

    model.isTrained = true
    model.trainTime = (System.currentTimeMillis() - startTime).toInt
    model
  }

  /**
   * validate the given data to train
   */
  private def validateData(data: RDD[Vector]) {
    require(this.numClusters <= data.count(), "# clusters must be less than # data rows")
  }

  /**
   * Selects the next node to split
   */
  private[clustering] def nextNode(clusterTree: ClusterTree): Option[ClusterTree] = {
    // select the max variance of clusters which are leaves of a tree
    clusterTree.toSeq().filter(tree => tree.isSplittable() && !tree.isVisited) match {
      case list if list.isEmpty => None
      case list => Some(list.maxBy(_.getVariance()))
    }
  }

  /**
   * Takes the initial centers for bi-sect k-means
   */
  private[clustering] def takeInitCenters(centers: Vector): Array[BV[Double]] = {
    val random = new XORShiftRandom()
    Array(
      centers.toBreeze.map(elm => elm - random.nextDouble() * elm * this.randomRange),
      centers.toBreeze.map(elm => elm + random.nextDouble() * elm * this.randomRange)
    )
  }

  /**
   * Splits the given cluster (tree) with bi-sect k-means
   *
   * @param clusterTree the splitted cluster
   * @return an array of ClusterTree. its size is generally 2, but its size can be 1
   */
  private def split(clusterTree: ClusterTree): Array[ClusterTree] = {
    val startTime = System.currentTimeMillis()
    val data = clusterTree.data
    val sc = data.sparkContext
    var centers = takeInitCenters(clusterTree.center)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    sc.broadcast(metric)

    // If the following conditions are satisfied, the iteration is stopped
    //   1. the relative error is less than that of configuration
    //   2. the number of executed iteration is greater than that of configuration
    //   3. the number of centers is equal to one. if one means that the cluster is not splittable
    var numIter = 0
    var error = Double.MaxValue
    while (error > this.epsilon
        && numIter < this.subIterations
        && centers.size > 1) {
      val startTimeOfIter = System.currentTimeMillis()

      sc.broadcast(centers)
      val newCenters = data.mapPartitions { iter =>
        // calculate the accumulation of the all point in a partition and count the rows
        val map = scala.collection.mutable.Map.empty[Int, (BV[Double], Int)]
        iter.foreach { point =>
          val idx = ClusterTree.findClosestCenter(metric)(centers)(point)
          val (sumBV, n) = map.get(idx)
              .getOrElse((new BSV[Double](Array(), Array(), point.size), 0))
          map(idx) = (sumBV + point, n + 1)
        }
        map.toIterator
      }.reduceByKeyLocally {
        // sum the accumulation and the count in the all partition
        case ((p1, n1), (p2, n2)) => (p1 + p2, n1 + n2)
      }.map { case ((idx: Int, (center: BV[Double], counts: Int))) =>
        center :/ counts.toDouble
      }

      val normSum = centers.map(v => breezeNorm(v, 2.0)).sum
      val newNormSum = newCenters.map(v => breezeNorm(v, 2.0)).sum
      error = math.abs((normSum - newNormSum) / normSum)
      centers = newCenters.toArray
      numIter += 1

      logInfo(s"${numIter} iterations is finished" +
          s" for ${System.currentTimeMillis() - startTimeOfIter}" +
          s" at ${getClass}.split")
    }

    val vectors = centers.map(center => Vectors.fromBreeze(center))
    val nodes = centers.size match {
      case 1 => Array(new ClusterTree(vectors(0), data))
      case 2 => {
        val closest = data.map(p => (ClusterTree.findClosestCenter(metric)(centers)(p), p))
        centers.zipWithIndex.map { case (center, i) =>
          val subData = closest.filter(_._1 == i).map(_._2)
          subData.cache
          new ClusterTree(vectors(i), subData)
        }
      }
      case _ => throw new RuntimeException(s"something wrong with # centers:${centers.size}")
    }
    logInfo(s"${this.getClass.getSimpleName}.split end" +
        s" with total iterations" +
        s" for ${System.currentTimeMillis() - startTime}")
    nodes
  }
}

/**
 * top-level methods for calling the hierarchical clustering algorithm
 */
object HierarchicalClustering {

  /**
   * Trains a hierarchical clustering model with the given data
   *
   * @param data trained data
   * @param numClusters the maximum number of clusters you want
   * @return a hierarchical clustering model
   */
  def train(data: RDD[Vector], numClusters: Int): HierarchicalClusteringModel = {
    val app = new HierarchicalClustering().setNumClusters(numClusters)
    app.run(data)
  }

  /**
   * Trains a hierarchical clustering model with the given data
   *
   * @param data trained data
   * @param numClusters the maximum number of clusters you want
   * @param subIterations the iteration of
   * @param numRetries the number of retries when the clustering can't be succeeded
   * @param epsilon the relative error that bisecting is satisfied
   * @param randomSeed the randomseed to generate the initial vectors for each bisecting
   * @param randomRange the range of error to genrate the initial vectors for each bisecting
   * @return a hierarchical clustering model
   */
  def train(
    data: RDD[Vector],
    numClusters: Int,
    subIterations: Int,
    numRetries: Int,
    epsilon: Double,
    randomSeed: Int,
    randomRange: Double): HierarchicalClusteringModel = {
    val algo = new HierarchicalClustering()
        .setNumClusters(numClusters)
        .setSubIterations(subIterations)
        .setNumRetries(numRetries)
        .setEpsilon(epsilon)
        .setRandomSeed(randomSeed)
        .setRandomRange(randomRange)
    algo.run(data)
  }
}


/**
 * A cluster as a tree node which can have its sub nodes
 *
 * @param data the data in the cluster
 * @param center the center of the cluster
 * @param variance the statistics for splitting of the cluster
 * @param dataSize the data size of its data
 * @param children the sub node(s) of the cluster
 * @param parent the parent node of the cluster
 */
private[mllib]
class ClusterTree private (
  val center: Vector,
  private[mllib] val data: RDD[BV[Double]],
  private[mllib] var height: Option[Double],
  private[mllib] var variance: Option[Double],
  private[mllib] var dataSize: Option[Long],
  private[mllib] var children: List[ClusterTree],
  private[mllib] var parent: Option[ClusterTree],
  private[mllib] var isVisited: Boolean) extends Serializable {

  def this(center: Vector, data: RDD[BV[Double]]) =
    this(center, data, None, None, None, List.empty[ClusterTree], None, false)

  override def toString(): String = {
    val elements = Array(
      s"hashCode:${this.hashCode()}",
      s"depth:${this.getDepth()}",
      s"dataSize:${this.dataSize.get}",
      s"variance:${this.variance.get}",
      s"parent:${this.parent.hashCode()}",
      s"children:${this.children.map(_.hashCode())}",
      s"isLeaf:${this.isLeaf()}",
      s"isVisited:${this.isVisited}"
    )
    elements.mkString(", ")
  }

  /**
   * Inserts sub nodes as its children
   *
   * @param children inserted sub nodes
   */
  def insert(children: List[ClusterTree]): Unit = {
    this.children = this.children ++ children
    children.foreach(child => child.parent = Some(this))
  }

  /**
   * Inserts a sub node as its child
   *
   * @param child inserted sub node
   */
  def insert(child: ClusterTree): Unit = insert(List(child))

  /**
   * Converts the tree into Seq class
   * the sub nodes are recursively expanded
   *
   * @return Seq class which the cluster tree is expanded
   */
  def toSeq(): Seq[ClusterTree] = {
    this.children.size match {
      case 0 => Seq(this)
      case _ => Seq(this) ++ this.children.map(child => child.toSeq()).flatten
    }
  }

  /**
   * Gets the all clusters which are leaves in the cluster tree
   * @return the Seq of the clusters
   */
  def getClusters(): Seq[ClusterTree] = {
    toSeq().filter(_.isLeaf()).sortWith { case (a, b) =>
      a.getDepth() < b.getDepth() &&
          breezeNorm(a.center.toBreeze, 2) < breezeNorm(b.center.toBreeze, 2)
    }
  }

  /**
   * Gets the depth of the cluster in the tree
   *
   * @return the depth
   */
  def getDepth(): Int = {
    this.parent match {
      case None => 0
      case _ => 1 + this.parent.get.getDepth()
    }
  }

  /**
   * Gets the dendrogram height of the cluster at the cluster tree
   *
   * @return the dendrogram height
   */
  def getHeight(): Double = {
    this.children.size match {
      case 0 => 0.0
      case _ => this.height.get + this.children.map(_.getHeight()).max
    }
  }

  /**
   * Assigns the closest cluster with a vector
   * @param metric distance metric
   * @param v the vector you want to assign to
   * @return the closest cluster
   */
  private[mllib]
  def assignCluster(metric: Function2[BV[Double], BV[Double], Double])(v: Vector): ClusterTree = {
    this.children.size match {
      case 0 => this
      case 2 => {
        val distances = this.children.map(tree => metric(tree.center.toBreeze, v.toBreeze))
        val minIndex = distances.indexOf(distances.min)
        this.children(minIndex).assignCluster(metric)(v)
      }
      case _ =>
        throw new UnsupportedOperationException(s"something wrong with # nodes, ${children.size}")
    }
  }

  /**
   * Assigns the closest cluster index of the clusters with a vector
   * @param metric distance metric
   * @param vector the vector you want to assign to
   * @return the closest cluster index of the all clusters
   */
  private[mllib]
  def assignClusterIndex(metric: Function2[BV[Double], BV[Double], Double])(vector: Vector): Int = {
    val assignedTree = this.assignCluster(metric)(vector)
    this.getClusters().indexOf(assignedTree)
  }

  /**
   * Gets the number of the clusters in the tree. The clusters are only leaves
   *
   * @return the number of the clusters in the tree
   */
  def getTreeSize(): Int = this.toSeq().filter(_.isLeaf()).size

  def getVariance(): Option[Double] = this.variance

  def getDataSize(): Option[Long] = this.dataSize

  def getParent(): Option[ClusterTree] = this.parent

  def getChildren(): List[ClusterTree] = this.children

  def isLeaf(): Boolean = (this.children.size == 0)

  /**
   * The flag that the cluster is splittable
   *
   * @return true is splittable
   */
  def isSplittable(): Boolean = {
    this.isLeaf && this.getDataSize != None && this.getDataSize.get >= 2
  }
}

/**
 * Companion object for ClusterTree class
 */
object ClusterTree {

  /**
   * Converts `RDD[Vector]` into a ClusterTree instance
   *
   * @param data the data in a cluster
   * @return a ClusterTree instance
   */
  def fromRDD(data: RDD[Vector]): ClusterTree = {
    val breezeData = data.map(_.toBreeze).cache
    // calculates the cluster center
    val pointStat = breezeData.mapPartitions { iter =>
      iter match {
        case iter if iter.isEmpty => Iterator.empty
        case _ => {
          val stat = iter.map(v => (v, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
          Iterator(stat)
        }
      }
    }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val center = Vectors.fromBreeze(pointStat._1.:/(pointStat._2.toDouble))
    new ClusterTree(center, breezeData)
  }

  private[mllib]
  def findClosestCenter(metric: Function2[BV[Double], BV[Double], Double])
        (centers: Array[BV[Double]])
        (point: BV[Double]): Int = {
    centers.zipWithIndex.map { case (center, idx) => (idx, metric(center, point))}.minBy(_._2)._1
  }
}

/**
 * Calculates the sum of the variances of the cluster
 */
private[clustering]
class ClusterTreeStatsUpdater private (private var first: Option[BV[Double]])
    extends Function1[ClusterTree, ClusterTree] with Serializable {

  def this() = this(None)

  /**
   * Calculates the sum of the variances in the cluster
   *
   * @param clusterTree the cluster tree
   * @return the sum of the variances
   */
  def apply(clusterTree: ClusterTree): ClusterTree = {
    val data = clusterTree.data
    if (this.first == None) this.first = Some(data.first())
    def zeroVector(): BV[Double] = {
      val vector = first.get match {
        case dense if first.get.isInstanceOf[BDV[Double]] => Vectors.zeros(first.get.size)
        case sparse if first.get.isInstanceOf[BSV[Double]] => Vectors.sparse(first.get.size, Seq())
        case _ => throw new UnsupportedOperationException(s"unexpected variable type")
      }
      vector.toBreeze
    }

    // mapper for each partition
    val eachStats = data.mapPartitions { iter =>
      var n = 0.0
      var sum = zeroVector()
      var sumOfSquares = zeroVector()
      val diff = zeroVector()
      iter.foreach { point =>
        n += 1.0
        sum = sum + point
        sumOfSquares = sumOfSquares + (point :* point)
      }
      Iterator((n, sum, sumOfSquares))
    }

    // reducer
    val (n, sum, sumOfSquares) = eachStats.reduce {
      case ((nA, sumA, sumOfSquareA), (nB, sumB, sumOfSquareB)) =>
        val nAB = nA + nB
        val sumAB = sumA + sumB
        val sumOfSquareAB = sumOfSquareA + sumOfSquareB
        (nAB, sumAB, sumOfSquareAB)
    }
    // set the number of rows
    clusterTree.dataSize = Some(n.toLong)
    // set the sum of the variances of each element
    val variance = n match {
      case n if n > 1 => (sumOfSquares.:*(n) - (sum :* sum)) :/ (n * (n - 1.0))
      case _ => zeroVector()
    }
    clusterTree.variance = Some(variance.toArray.sum / this.first.get.size)

    clusterTree
  }
}
