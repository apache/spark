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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Vectors, DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}


private case class DenseVectorLabeledPoint(label: Double, features: DenseVector)
private case class SparseVectorLabeledPoint(label: Double, features: SparseVector)

class DatasetSuite extends FunSuite with LocalSparkContext {

  test("SQL and Vector") {
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    val points = Seq(
      LabeledPoint(1.0, Vectors.dense(Array(1.0, 2.0))),
      LabeledPoint(2.0, Vectors.dense(Array(3.0, 6.0))),
      LabeledPoint(3.0, Vectors.dense(Array(3.0, 3.0))),
      LabeledPoint(4.0, Vectors.dense(Array(4.0, 8.0))))
    val data: RDD[LabeledPoint] = sc.parallelize(points)
    val labels = data.select('label).map { case Row(label: Double) => label }.collect().toSet
    assert(labels == Set(1.0, 2.0, 3.0, 4.0))
    val features = data.select('features).map { case Row(features: Vector) => features }.collect()
    assert(features.size === 4)
    assert(features.forall(_.size == 2))
  }

  test("SQL and DenseVector") {
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    val points = Seq(
      DenseVectorLabeledPoint(1.0, new DenseVector(Array(1.0, 2.0))),
      DenseVectorLabeledPoint(2.0, new DenseVector(Array(3.0, 6.0))),
      DenseVectorLabeledPoint(3.0, new DenseVector(Array(3.0, 3.0))),
      DenseVectorLabeledPoint(4.0, new DenseVector(Array(4.0, 8.0))))
    val data: RDD[DenseVectorLabeledPoint] = sc.parallelize(points)
    val labels = data.select('label).map { case Row(label: Double) => label }.collect().toSet
    assert(labels == Set(1.0, 2.0, 3.0, 4.0))
    val features =
      data.select('features).map { case Row(features: DenseVector) => features }.collect()
    assert(features.size === 4)
    assert(features.forall(_.size == 2))
  }

  test("SQL and SparseVector") {
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    val vSize = 2
    val points = Seq(
      SparseVectorLabeledPoint(1.0, new SparseVector(vSize, Array(0, 1), Array(1.0, 2.0))),
      SparseVectorLabeledPoint(2.0, new SparseVector(vSize, Array(0, 1), Array(3.0, 6.0))),
      SparseVectorLabeledPoint(3.0, new SparseVector(vSize, Array(0, 1), Array(3.0, 3.0))),
      SparseVectorLabeledPoint(4.0, new SparseVector(vSize, Array(0, 1), Array(4.0, 8.0))))
    val data: RDD[SparseVectorLabeledPoint] = sc.parallelize(points)
    val labels = data.select('label).map { case Row(label: Double) => label }.collect().toSet
    assert(labels == Set(1.0, 2.0, 3.0, 4.0))
    val features =
      data.select('features).map { case Row(features: SparseVector) => features }.collect()
    assert(features.size === 4)
    assert(features.forall(_.size == 2))
  }
}
