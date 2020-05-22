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

package org.apache.spark.ml.clustering

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.InstanceBlock
import org.apache.spark.ml.linalg.{DenseMatrix, Vector, Vectors}
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.linalg.BLAS.axpy
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * K-means clustering using RDD[InstanceBlock] as input format.
 */
class KMeansBlocksImpl (
    private var k: Int,
    private var maxIterations: Int,
    private var initializationMode: String,
    private var initializationSteps: Int,
    private var epsilon: Double,
    private var seed: Long,
    private var distanceMeasure: String) extends Serializable with Logging {

  /**
   * Constructs a KMeans instance with default parameters: {k: 2, maxIterations: 20,
   * initializationMode: "k-means||", initializationSteps: 2, epsilon: 1e-4, seed: random,
   * distanceMeasure: "euclidean"}.
   */
  def this() = this(2, 20, KMeansBlocksImpl.K_MEANS_PARALLEL, 2, 1e-4, Utils.random.nextLong(),
    DistanceMeasure.EUCLIDEAN)

  // Initial cluster centers can be provided as a KMeansModel object rather than using the
  // random or k-means|| initializationMode
  private var initialModel: Option[KMeansModel] = None

  private def VectorsToDenseMatrix(vectors: Array[VectorWithNorm]): DenseMatrix = {

    val v: Array[Vector] = vectors.map(_.vector)

    VectorsToDenseMatrix(v.toIterator)
  }

  private def VectorsToDenseMatrix(vectors: Iterator[Vector]): DenseMatrix = {
    val vector_array = vectors.toArray
    val column_num = vector_array(0).size
    val row_num = vector_array.length

    val values = new Array[Double](row_num * column_num)
    var rowIndex = 0

    // convert to column-major dense matrix
    for (vector <- vector_array) {
      for ((value, index) <- vector.toArray.zipWithIndex) {
        values(index * row_num + rowIndex) = value
      }
      rowIndex = rowIndex + 1
    }

    new DenseMatrix(row_num, column_num, values)
  }

  private def computeSquaredDistances(points_matrix: DenseMatrix,
                                      points_square_sums: DenseMatrix,
                                      centers_matrix: DenseMatrix,
                                      centers_square_sums: DenseMatrix): DenseMatrix = {
    // (x - y)^2 = x^2 + y^2 - 2 * x * y

    // Add up squared sums of points and centers (x^2 + y^2)
    val ret: DenseMatrix = computeMatrixSum(points_square_sums, centers_square_sums)

    // use GEMM to compute squared distances, (2*x*y) can be decomposed to matrix multiply
    val alpha = -2.0
    val beta = 1.0
    BLAS.gemm(alpha, points_matrix, centers_matrix.transpose, beta, ret)

    ret
  }

  private def computePointsSquareSum(points_matrix: DenseMatrix,
                                     centers_num: Int): DenseMatrix = {
    val points_num = points_matrix.numRows
    val ret = DenseMatrix.zeros(points_num, centers_num)
    for ((row, index) <- points_matrix.rowIter.zipWithIndex) {
      val square = BLAS.dot(row, row)
      for (i <- 0 until centers_num)
        ret(index, i) = square
    }
    ret
  }

  private def computeCentersSquareSum(centers_matrix: DenseMatrix,
                                      points_num: Int): DenseMatrix = {
    val centers_num = centers_matrix.numRows
    val ret = DenseMatrix.zeros(points_num, centers_num)
    for ((row, index) <- centers_matrix.rowIter.zipWithIndex) {
      val square = BLAS.dot(row, row)
      for (i <- 0 until points_num)
        ret(i, index) = square
    }
    ret
  }

  // use GEMM to compute matrix sum
  private def computeMatrixSum(matrix1: DenseMatrix,
                               matrix2: DenseMatrix): DenseMatrix = {
    val column_num = matrix1.numCols
    val eye = DenseMatrix.eye(column_num)
    val alpha = 1.0
    val beta = 1.0
    BLAS.gemm(alpha, matrix1, eye, beta, matrix2)
    matrix2
  }

  private def findClosest(distances: DenseMatrix,
      weights: Array[Double]): (Array[Int], Array[Double]) = {
    val points_num = distances.numRows
    val ret_closest = new Array[Int](points_num)
    val ret_cost = new Array[Double](points_num)

    for ((row, index) <- distances.rowIter.zipWithIndex) {
      var closest = 0
      var cost = row(0)
      for (i <- 1 until row.size) {
        if (row(i) < cost) {
          closest = i
          cost = row(i)
        }
      }
      ret_closest(index) = closest

      // use weighted squared distance as cost
      if (weights.nonEmpty) {
        ret_cost(index) = cost * weights(index)
      } else {
        ret_cost(index) = cost
      }
    }

    (ret_closest, ret_cost)

  }

    def runWithWeight(data: RDD[InstanceBlock],
                      instr: Option[Instrumentation]): KMeansModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val distanceMeasureInstance = DistanceMeasure.decodeFromString(this.distanceMeasure)

    val centers = if (initializationMode == KMeansBlocksImpl.RANDOM) {
      initRandom(data)
    } else {
      initKMeansParallel(data, distanceMeasureInstance)
    }

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(f"Initialization with $initializationMode took $initTimeInSeconds%.3f seconds.")

    var converged = false
    var cost = 0.0
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    instr.foreach(_.logNumFeatures(centers.head.vector.size))

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < maxIterations && !converged) {
      // Convert center vectors to dense matrix
      val centers_matrix = VectorsToDenseMatrix(centers)

      val costAccum = sc.doubleAccumulator
      val bcCenters = sc.broadcast(centers_matrix)

      val centers_num = centers_matrix.numRows
      val centers_dim = centers_matrix.numCols

      // Compute squared sums for points
      val data_square_sums: RDD[DenseMatrix] = data.mapPartitions { blocks =>
        blocks.map { block =>
          computePointsSquareSum(DenseMatrix.fromML(block.matrix.toDense), centers_num) }
      }

      // Find the new centers
      val collected = data.zip(data_square_sums).flatMap {
        case (blocks, points_square_sums) =>
          val centers_matrix = bcCenters.value
          val points_num = blocks.matrix.numRows

          val sums = Array.fill(centers_num)(Vectors.zeros(centers_dim))
          val counts = Array.fill(centers_num)(0L)

          // Compute squared sums for centers
          val centers_square_sums = computeCentersSquareSum(centers_matrix, points_num)

          // Compute squared distances
          val distances = computeSquaredDistances(
            DenseMatrix.fromML(blocks.matrix.toDense), points_square_sums,
            centers_matrix, centers_square_sums)

          val (bestCenters, weightedCosts) = findClosest(distances, blocks.weights)

          for (cost <- weightedCosts)
            costAccum.add(cost)

          // sums points around best center
          for ((row, index) <- blocks.matrix.rowIter.zipWithIndex) {
            val bestCenter = bestCenters(index)
            if (blocks.weights.nonEmpty) {
              axpy(blocks.weights(index), Vectors.fromML(row), sums(bestCenter))
            } else {
              axpy(1, Vectors.fromML(row), sums(bestCenter))
            }
            counts(bestCenter) += 1
          }

          counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
        }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
          axpy(1.0, sum2, sum1)
          (sum1, count1 + count2)
        }.collectAsMap()

      if (iteration == 0) {
        instr.foreach(_.logNumExamples(collected.values.map(_._2).sum))
      }

      val newCenters = collected.mapValues { case (sum, count) =>
        distanceMeasureInstance.centroid(sum, count)
      }

      bcCenters.destroy()

      // Update the cluster centers and costs
      converged = true
      newCenters.foreach { case (j, newCenter) =>
        if (converged &&
          !distanceMeasureInstance.isCenterConverged(centers(j), newCenter, epsilon)) {
          converged = false
        }
        centers(j) = newCenter
      }

      cost = costAccum.value
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

    if (iteration == maxIterations) {
      logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    logInfo(s"The cost is $cost.")

    new KMeansModel(centers.map { c => c.vector },
      distanceMeasure, cost, iteration)
  }

  /**
   * Initialize a set of cluster centers at random.
   */
  private def initRandom(data: RDD[InstanceBlock]): Array[VectorWithNorm] = {

    val vectorWithNorms: RDD[VectorWithNorm] = data.flatMap(_.matrix.rowIter)
      .map(Vectors.fromML(_)).map(new VectorWithNorm(_))


    // Select without replacement; may still produce duplicates if the data has < k distinct
    // points, so deduplicate the centroids to match the behavior of k-means|| in the same situation

    vectorWithNorms.takeSample(false, k, new XORShiftRandom(this.seed).nextInt())
      .map(_.vector).distinct.map(new VectorWithNorm(_))
  }

  /**
   * Initialize a set of cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private[clustering] def initKMeansParallel(rdd_matrix: RDD[InstanceBlock],
      distanceMeasureInstance: DistanceMeasure): Array[VectorWithNorm] = {

    val data: RDD[VectorWithNorm] = rdd_matrix.flatMap(_.matrix.rowIter)
      .map(Vectors.fromML(_)).map(new VectorWithNorm(_))

    // Initialize empty centers and point costs.
    var costs = data.map(_ => Double.PositiveInfinity)

    // Initialize the first center to a random point.
    val seed = new XORShiftRandom(this.seed).nextInt()
    val sample = data.takeSample(false, 1, seed)
    // Could be empty if data is empty; fail with a better message early:
    require(sample.nonEmpty, s"No samples available from $data")

    val centers = ArrayBuffer[VectorWithNorm]()
    var newCenters = Array(sample.head.toDense)
    centers ++= newCenters

    // On each step, sample 2 * k points on average with probability proportional
    // to their squared distance from the centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    val bcNewCentersList = ArrayBuffer[Broadcast[_]]()
    while (step < initializationSteps) {
      val bcNewCenters = data.context.broadcast(newCenters)
      bcNewCentersList += bcNewCenters
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
        math.min(distanceMeasureInstance.pointCost(bcNewCenters.value, point), cost)
      }.persist(StorageLevel.MEMORY_AND_DISK)
      val sumCosts = costs.sum()

      bcNewCenters.unpersist()
      preCosts.unpersist()

      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointCosts) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        pointCosts.filter { case (_, c) => rand.nextDouble() < 2.0 * c * k / sumCosts }.map(_._1)
      }.collect()
      newCenters = chosen.map(_.toDense)
      centers ++= newCenters
      step += 1
    }

    costs.unpersist()
    bcNewCentersList.foreach(_.destroy())

    val distinctCenters = centers.map(_.vector).distinct.map(new VectorWithNorm(_)).toArray

    if (distinctCenters.length <= k) {
      distinctCenters
    } else {
      // Finally, we might have a set of more than k distinct candidate centers; weight each
      // candidate by the number of points in the dataset mapping to it and run a local k-means++
      // on the weighted centers to pick k of them
      val bcCenters = data.context.broadcast(distinctCenters)
      val countMap = data
        .map(distanceMeasureInstance.findClosest(bcCenters.value, _)._1)
        .countByValue()

      bcCenters.destroy()

      val myWeights = distinctCenters.indices.map(countMap.getOrElse(_, 0L).toDouble).toArray
      LocalKMeans.kMeansPlusPlus(0, distinctCenters, myWeights, k, 30)
    }
  }
}


/**
 * Top-level methods for calling K-means clustering.
 */
object KMeansBlocksImpl {

  // Initialization mode names
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"

  private[spark] def validateInitMode(initMode: String): Boolean = {
    initMode match {
      case KMeansBlocksImpl.RANDOM => true
      case KMeansBlocksImpl.K_MEANS_PARALLEL => true
      case _ => false
    }
  }
}

