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
 */
private[spark] class InstanceBlock(
    val labels: Array[Double],
    val weights: Array[Double],
    val featureMatrix: Matrix) {
  require(labels.length == featureMatrix.numRows)
  require(featureMatrix.isTransposed)
  if (weights.nonEmpty) {
    require(labels.length == weights.length)
  }

  def size: Int = labels.length

  def numFeatures: Int = featureMatrix.numCols

  def instanceIterator: Iterator[Instance] = {
    if (weights.nonEmpty) {
      labels.iterator.zip(weights.iterator).zip(featureMatrix.rowIter)
        .map { case ((label, weight), vec) => Instance(label, weight, vec) }
    } else {
      labels.iterator.zip(featureMatrix.rowIter)
        .map { case (label, vec) => Instance(label, 1.0, vec) }
    }
  }

  def getLabel(i: Int): Double = labels(i)

  def labelIter: Iterator[Double] = labels.iterator

  def getWeight(i: Int): Double = {
    if (weights.nonEmpty) {
      weights(i)
    } else {
      1.0
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
  val getNonZeroIter: Int => Iterator[(Int, Double)] = {
    featureMatrix match {
      case dm: DenseMatrix =>
        (i: Int) =>
          val start = numFeatures * i
          Iterator.tabulate(numFeatures)(j => (j, dm.values(start + j)))
            .filter(_._2 != 0)
      case sm: SparseMatrix =>
        (i: Int) =>
          val start = sm.colPtrs(i)
          val end = sm.colPtrs(i + 1)
          Iterator.tabulate(end - start)(i =>
            (sm.rowIndices(start + i), sm.values(start + i))
          ).filter(_._2 != 0)
    }
  }
}

private[spark] object InstanceBlock {

  def fromInstances(instances: Seq[Instance]): InstanceBlock = {
    val labels = instances.map(_.label).toArray
    val weights = if (instances.exists(_.weight != 1)) {
      instances.map(_.weight).toArray
    } else {
      Array.emptyDoubleArray
    }
    val numRows = instances.length
    val numCols = instances.head.features.size
    val denseSize = Matrices.getDenseSize(numCols, numRows)
    val nnz = instances.iterator.map(_.features.numNonzeros).sum
    val sparseSize = Matrices.getSparseSize(nnz, numRows + 1)
    val matrix = if (denseSize < sparseSize) {
      val values = Array.ofDim[Double](numRows * numCols)
      var offset = 0
      var j = 0
      while (j < numRows) {
        instances(j).features.foreachNonZero { (i, v) =>
          values(offset + i) = v
        }
        offset += numCols
        j += 1
      }
      new DenseMatrix(numRows, numCols, values, true)
    } else {
      val colIndices = mutable.ArrayBuilder.make[Int]
      val values = mutable.ArrayBuilder.make[Double]
      val rowPtrs = mutable.ArrayBuilder.make[Int]
      var rowPtr = 0
      rowPtrs += 0
      var j = 0
      while (j < numRows) {
        var nnz = 0
        instances(j).features.foreachNonZero { (i, v) =>
          colIndices += i
          values += v
          nnz += 1
        }
        rowPtr += nnz
        rowPtrs += rowPtr
        j += 1
      }
      new SparseMatrix(numRows, numCols, rowPtrs.result(),
        colIndices.result(), values.result(), true)
    }
    new InstanceBlock(labels, weights, matrix)
  }

  def blokify(instances: RDD[Instance], blockSize: Int): RDD[InstanceBlock] = {
    instances.mapPartitions(_.grouped(blockSize).map(InstanceBlock.fromInstances))
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
