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

package org.apache.spark.ml.feature

import scala.collection.mutable

import org.apache.spark.ml.linalg._
import org.apache.spark.rdd.RDD

/**
 * Class that represents an instance of weighted data point with label and features.
 *
 * @param label Label for this data point.
 * @param weight The weight of this instance.
 * @param features The vector of features for this data point.
 */
private[spark] case class Instance(label: Double, weight: Double, features: Vector)


/**
 * Class that represents an block of instance.
 * If all weights are 1, then an empty array is stored.
 */
private[spark] case class InstanceBlock(
    labels: Array[Double],
    weights: Array[Double],
    matrix: Matrix) {
  require(labels.length == matrix.numRows)
  require(matrix.isTransposed)
  if (weights.nonEmpty) {
    require(labels.length == weights.length)
  }

  def size: Int = labels.length

  def numFeatures: Int = matrix.numCols

  def instanceIterator: Iterator[Instance] = {
    if (weights.nonEmpty) {
      labels.iterator.zip(weights.iterator).zip(matrix.rowIter)
        .map { case ((label, weight), vec) => Instance(label, weight, vec) }
    } else {
      labels.iterator.zip(matrix.rowIter)
        .map { case (label, vec) => Instance(label, 1.0, vec) }
    }
  }

  def getLabel(i: Int): Double = labels(i)

  def labelIter: Iterator[Double] = labels.iterator

  @transient lazy val getWeight: Int => Double = {
    if (weights.nonEmpty) {
      (i: Int) => weights(i)
    } else {
      (i: Int) => 1.0
    }
  }

  def weightIter: Iterator[Double] = {
    if (weights.nonEmpty) {
      weights.iterator
    } else {
      Iterator.fill(size)(1.0)
    }
  }

  // directly get the non-zero iterator of i-th row vector without array copy or slice
  @transient lazy val getNonZeroIter: Int => Iterator[(Int, Double)] = {
    matrix match {
      case dm: DenseMatrix =>
        (i: Int) =>
          val start = numFeatures * i
          Iterator.tabulate(numFeatures)(j =>
            (j, dm.values(start + j))
          ).filter(_._2 != 0)
      case sm: SparseMatrix =>
        (i: Int) =>
          val start = sm.colPtrs(i)
          val end = sm.colPtrs(i + 1)
          Iterator.tabulate(end - start)(j =>
            (sm.rowIndices(start + j), sm.values(start + j))
          ).filter(_._2 != 0)
    }
  }
}

private[spark] object InstanceBlock {

  /**
   * Suggested value for BlockSizeInMB in Level-2 routine cases.
   * According to performance tests of BLAS routine (see SPARK-31714) and
   * LinearSVC (see SPARK-32907), 1.0 MB should be an acceptable value for
   * linear models using Level-2 routine (GEMV) to perform prediction and
   * gradient computation.
   */
  val DefaultBlockSizeInMB = 1.0

  private def getBlockMemUsage(
      numCols: Long,
      numRows: Long,
      nnz: Long,
      allUnitWeight: Boolean): Long = {
    val doubleBytes = java.lang.Double.BYTES
    val arrayHeader = 12L
    val denseSize = Matrices.getDenseSize(numCols, numRows)
    val sparseSize = Matrices.getSparseSize(nnz, numRows + 1)
    val matrixSize = math.min(denseSize, sparseSize)
    if (allUnitWeight) {
      matrixSize + doubleBytes * numRows + arrayHeader * 2
    } else {
      matrixSize + doubleBytes * numRows * 2 + arrayHeader * 2
    }
  }

  def fromInstances(instances: Seq[Instance]): InstanceBlock = {
    val labels = instances.map(_.label).toArray
    val weights = if (instances.exists(_.weight != 1)) {
      instances.map(_.weight).toArray
    } else {
      Array.emptyDoubleArray
    }
    val matrix = Matrices.fromVectors(instances.map(_.features))
    new InstanceBlock(labels, weights, matrix)
  }

  def blokify(instances: RDD[Instance], blockSize: Int): RDD[InstanceBlock] = {
    instances.mapPartitions(_.grouped(blockSize).map(InstanceBlock.fromInstances))
  }

  def blokifyWithMaxMemUsage(
      instanceIterator: Iterator[Instance],
      maxMemUsage: Long): Iterator[InstanceBlock] = {
    require(maxMemUsage > 0)

    new Iterator[InstanceBlock]() {
      private var numCols = -1L

      override def hasNext: Boolean = instanceIterator.hasNext

      override def next(): InstanceBlock = {
        val buff = mutable.ArrayBuilder.make[Instance]
        var buffCnt = 0L
        var buffNnz = 0L
        var buffUnitWeight = true
        var blockMemUsage = 0L

        while (instanceIterator.hasNext && blockMemUsage < maxMemUsage) {
          val instance = instanceIterator.next()
          if (numCols < 0L) numCols = instance.features.size
          require(numCols == instance.features.size)

          buff += instance
          buffCnt += 1L
          buffNnz += instance.features.numNonzeros
          buffUnitWeight &&= (instance.weight == 1)
          blockMemUsage = getBlockMemUsage(numCols, buffCnt, buffNnz, buffUnitWeight)
        }

        // the block memory usage may slightly exceed threshold, not a big issue.
        // and this ensure even if one row exceed block limit, each block has one row.
        InstanceBlock.fromInstances(buff.result())
      }
    }
  }

  def blokifyWithMaxMemUsage(
      instances: RDD[Instance],
      maxMemUsage: Long): RDD[InstanceBlock] = {
    require(maxMemUsage > 0)
    instances.mapPartitions(iter => blokifyWithMaxMemUsage(iter, maxMemUsage))
  }
}


/**
 * Case class that represents an instance of data point with
 * label, weight, offset and features.
 * This is mainly used in GeneralizedLinearRegression currently.
 *
 * @param label Label for this data point.
 * @param weight The weight of this instance.
 * @param offset The offset used for this data point.
 * @param features The vector of features for this data point.
 */
private[ml] case class OffsetInstance(
    label: Double,
    weight: Double,
    offset: Double,
    features: Vector) {

  /** Converts to an [[Instance]] object by leaving out the offset. */
  def toInstance: Instance = Instance(label, weight, features)

}
