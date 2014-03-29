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
package org.apache.spark.mllib.rdd

import breeze.linalg.{Vector => BV, DenseVector => BDV}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils._
import org.apache.spark.rdd.RDD

/**
 * Extra functions available on RDDs of [[org.apache.spark.mllib.linalg.Vector Vector]] through an
 * implicit conversion. Import `org.apache.spark.MLContext._` at the top of your program to use
 * these functions.
 */
class VectorRDDFunctions(self: RDD[Vector]) extends Serializable {

  /**
   * Compute the mean of each `Vector` in the RDD.
   */
  def rowMeans(): RDD[Double] = {
    self.map(x => x.toArray.sum / x.size)
  }

  /**
   * Compute the norm-2 of each `Vector` in the RDD.
   */
  def rowNorm2(): RDD[Double] = {
    self.map(x => math.sqrt(x.toArray.map(x => x*x).sum))
  }

  /**
   * Compute the standard deviation of each `Vector` in the RDD.
   */
  def rowSDs(): RDD[Double] = {
    val means = self.rowMeans()
    self.zip(means)
      .map{ case(x, m) => x.toBreeze - m }
      .map{ x => math.sqrt(x.toArray.map(x => x*x).sum / x.size) }
  }

  /**
   * Compute the mean of each column in the RDD.
   */
  def colMeans(): Vector = colMeans(self.take(1).head.size)

  /**
   * Compute the mean of each column in the RDD with `size` as the dimension of each `Vector`.
   */
  def colMeans(size: Int): Vector = {
    Vectors.fromBreeze(self.map(_.toBreeze).aggregate((BV.zeros[Double](size), 0.0))(
      seqOp = (c, v) => (c, v) match {
        case ((prev, cnt), current) =>
          (((prev :* cnt) + current) :/ (cnt + 1.0), cnt + 1.0)
      },
      combOp = (lhs, rhs) => (lhs, rhs) match {
        case ((lhsVec, lhsCnt), (rhsVec, rhsCnt)) =>
          ((lhsVec :* lhsCnt) + (rhsVec :* rhsCnt) :/ (lhsCnt + rhsCnt), lhsCnt + rhsCnt)
      }
    )._1)
  }

  /**
   * Compute the norm-2 of each column in the RDD.
   */
  def colNorm2(): Vector = colNorm2(self.take(1).head.size)

  /**
   * Compute the norm-2 of each column in the RDD with `size` as the dimension of each `Vector`.
   */
  def colNorm2(size: Int): Vector = Vectors.fromBreeze(self.map(_.toBreeze)
    .aggregate(BV.zeros[Double](size))(
      seqOp = (c, v) => c + (v :* v),
      combOp = (lhs, rhs) => lhs + rhs
    ).map(math.sqrt)
  )

  /**
   * Compute the standard deviation of each column in the RDD.
   */
  def colSDs(): Vector = colSDs(self.take(1).head.size)

  /**
   * Compute the standard deviation of each column in the RDD with `size` as the dimension of each
   * `Vector`.
   */
  def colSDs(size: Int): Vector = {
    val means = self.colMeans()
    Vectors.fromBreeze(self.map(x => x.toBreeze - means.toBreeze)
      .aggregate((BV.zeros[Double](size), 0.0))(
        seqOp = (c, v) => (c, v) match {
          case ((prev, cnt), current) =>
            (((prev :* cnt) + (current :* current)) :/ (cnt + 1.0), cnt + 1.0)
        },
        combOp = (lhs, rhs) => (lhs, rhs) match {
          case ((lhsVec, lhsCnt), (rhsVec, rhsCnt)) =>
            ((lhsVec :* lhsCnt) + (rhsVec :* rhsCnt) :/ (lhsCnt + rhsCnt), lhsCnt + rhsCnt)
        }
      )._1.map(math.sqrt)
    )
  }

  /**
   * Find the optional max or min vector in the RDD.
   */
  private def maxMinOption(cmp: (Vector, Vector) => Boolean): Option[Vector] = {
    def cmpMaxMin(x1: Vector, x2: Vector) = if (cmp(x1, x2)) x1 else x2
    self.mapPartitions { iterator =>
      Seq(iterator.reduceOption(cmpMaxMin)).iterator
    }.collect { case Some(x) => x }.collect().reduceOption(cmpMaxMin)
  }

  /**
   * Find the optional max vector in the RDD, `None` will be returned if there is no elements at
   * all.
   */
  def maxOption(cmp: (Vector, Vector) => Boolean) = maxMinOption(cmp)

  /**
   * Find the optional min vector in the RDD, `None` will be returned if there is no elements at
   * all.
   */
  def minOption(cmp: (Vector, Vector) => Boolean) = maxMinOption(!cmp(_, _))

  /**
   * Filter the vectors whose standard deviation is not zero.
   */
  def rowShrink(): RDD[Vector] = self.zip(self.rowSDs()).filter(_._2 != 0.0).map(_._1)

  /**
   * Filter each column of the RDD whose standard deviation is not zero.
   */
  def colShrink(): RDD[Vector] = {
    val sds = self.colSDs()
    self.take(1).head.toBreeze.isInstanceOf[BDV[Double]] match {
      case true =>
        self.map{ v =>
          Vectors.dense(v.toArray.zip(sds.toArray).filter{case (x, m) => m != 0.0}.map(_._1))
        }
      case false =>
        self.map { v =>
          val filtered = v.toArray.zip(sds.toArray).filter{case (x, m) => m != 0.0}.map(_._1)
          val denseVector = Vectors.dense(filtered).toBreeze
          val size = denseVector.size
          val iterElement = denseVector.activeIterator.toSeq
          Vectors.sparse(size, iterElement)
        }
    }
  }
}
