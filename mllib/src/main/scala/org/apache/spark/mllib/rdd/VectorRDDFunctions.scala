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

import breeze.linalg.{Vector => BV, *}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils._
import org.apache.spark.rdd.RDD

/**
 * Extra functions available on RDDs of [[org.apache.spark.mllib.linalg.Vector Vector]] through an implicit conversion.
 * Import `org.apache.spark.MLContext._` at the top of your program to use these functions.
 */
class VectorRDDFunctions(self: RDD[Vector]) extends Serializable {

  def rowMeans(): RDD[Double] = {
    self.map(x => x.toArray.sum / x.size)
  }

  def rowNorm2(): RDD[Double] = {
    self.map(x => math.sqrt(x.toArray.map(x => x*x).sum))
  }

  def rowSDs(): RDD[Double] = {
    val means = self.rowMeans()
    self.zip(means)
      .map{ case(x, m) => x.toBreeze - m }
      .map{ x => math.sqrt(x.toArray.map(x => x*x).sum / x.size) }
  }

  def colMeansOption(): Vector = {
    ???
  }

  def colNorm2Option(): Vector = {
    ???
  }

  def colSDsOption(): Vector = {
    ???
  }

  def colMeans(): Vector = {
    Vectors.fromBreeze(self.map(_.toBreeze).zipWithIndex().fold((BV.zeros(1), 0L)) {
      case ((lhsVec, lhsCnt), (rhsVec, rhsCnt)) =>
        val totalNow: BV[Double] = lhsVec :* lhsCnt.asInstanceOf[Double]
        val totalNew: BV[Double] = (totalNow + rhsVec) :/ rhsCnt.asInstanceOf[Double]
        (totalNew, rhsCnt)
    }._1)
  }

  def colNorm2(): Vector = Vectors.fromBreeze(
    breezeVector = self.map(_.toBreeze).fold(BV.zeros(1)) {
      case (lhs, rhs) => lhs + rhs :* rhs
  }.map(math.sqrt))

  def colSDs(): Vector = {
    val means = this.colMeans()
    Vectors.fromBreeze(
      breezeVector = self.map(x => x.toBreeze - means.toBreeze)
        .zipWithIndex()
        .fold((BV.zeros(1), 0L)) {
          case ((lhsVec, lhsCnt), (rhsVec, rhsCnt)) =>
            val totalNow: BV[Double] = lhsVec :* lhsCnt.asInstanceOf[Double]
            val totalNew: BV[Double] = (totalNow + rhsVec :* rhsVec) :/ rhsCnt.asInstanceOf[Double]
            (totalNew, rhsCnt)
    }._1.map(math.sqrt))
  }

  private def maxMinOption(cmp: (Vector, Vector) => Boolean): Option[Vector] = {
    def cmpMaxMin(x1: Vector, x2: Vector) = if (cmp(x1, x2)) x1 else x2
    self.mapPartitions { iterator =>
      Seq(iterator.reduceOption(cmpMaxMin)).iterator
    }.collect { case Some(x) => x }.collect().reduceOption(cmpMaxMin)
  }

  def maxOption(cmp: (Vector, Vector) => Boolean) = maxMinOption(cmp)

  def minOption(cmp: (Vector, Vector) => Boolean) = maxMinOption(!cmp(_, _))

  def rowShrink(): RDD[Vector] = {
    ???
  }

  def colShrink(): RDD[Vector] = {
    ???
  }
}
