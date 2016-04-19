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

package org.apache.spark.mllib.linalg.udt

import scala.beans.{BeanInfo, BeanProperty}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

@BeanInfo
private[mllib] case class MyVectorPoint(
    @BeanProperty label: Double,
    @BeanProperty vector: Vector)

@BeanInfo
private[mllib] case class MyMatrixPoint(
    @BeanProperty label: Double,
    @BeanProperty matrix: Matrix)

class UDTSuite extends SparkFunSuite with MLlibTestSparkContext {
  import testImplicits._

  test("preloaded VectorUDT") {
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0, 2.0)
    val sv0 = Vectors.sparse(2, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(2, Array(1), Array(2.0))

    val vectorRDD = Seq(
      MyVectorPoint(1.0, dv0),
      MyVectorPoint(2.0, dv1),
      MyVectorPoint(3.0, sv0),
      MyVectorPoint(4.0, sv1)).toDF()

    val labels: RDD[Double] = vectorRDD.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 4)
    assert(labelsArrays.sorted === Array(1.0, 2.0, 3.0, 4.0))

    val vectors: RDD[Vector] =
      vectorRDD.select('vector).rdd.map { case Row(v: Vector) => v }
    val vectorsArrays: Array[Vector] = vectors.collect()
    assert(vectorsArrays.contains(dv0))
    assert(vectorsArrays.contains(dv1))
    assert(vectorsArrays.contains(sv0))
    assert(vectorsArrays.contains(sv1))
  }

  test("preloaded MatrixUDT") {
    val dm1 = new DenseMatrix(2, 2, Array(0.9, 1.2, 2.3, 9.8))
    val dm2 = new DenseMatrix(3, 2, Array(0.0, 1.21, 2.3, 9.8, 9.0, 0.0))
    val dm3 = new DenseMatrix(0, 0, Array())
    val sm1 = dm1.toSparse
    val sm2 = dm2.toSparse
    val sm3 = dm3.toSparse

    val matrixRDD = Seq(
      MyMatrixPoint(1.0, dm1),
      MyMatrixPoint(2.0, dm2),
      MyMatrixPoint(3.0, dm3),
      MyMatrixPoint(4.0, sm1),
      MyMatrixPoint(5.0, sm2),
      MyMatrixPoint(6.0, sm3)).toDF()

    val labels: RDD[Double] = matrixRDD.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 6)
    assert(labelsArrays.sorted === Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))

    val matrices: RDD[Matrix] =
      matrixRDD.select('matrix).rdd.map { case Row(v: Matrix) => v }
    val matrixArrays: Array[Matrix] = matrices.collect()
    assert(matrixArrays.contains(dm1))
    assert(matrixArrays.contains(dm2))
    assert(matrixArrays.contains(dm3))
    assert(matrixArrays.contains(sm1))
    assert(matrixArrays.contains(sm2))
    assert(matrixArrays.contains(sm3))
  }
}
