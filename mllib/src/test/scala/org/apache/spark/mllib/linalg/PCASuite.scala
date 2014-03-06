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

package org.apache.spark.mllib.linalg

import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.jblas.{DoubleMatrix, Singular, MatrixFunctions}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.util._

import org.jblas._

class PCASuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  val EPSILON = 1e-3

  // Return jblas matrix from sparse matrix RDD
  def getDenseMatrix(matrix: SparseMatrix) : DoubleMatrix = {
    val data = matrix.data
    val m = matrix.m
    val n = matrix.n
    val ret = DoubleMatrix.zeros(m, n)
    matrix.data.toArray.map(x => ret.put(x.i, x.j, x.mval))
    ret
  }

  def assertMatrixEquals(a: DoubleMatrix, b: DoubleMatrix) {
    assert(a.rows == b.rows && a.columns == b.columns, "dimension mismatch")
    val diff = DoubleMatrix.zeros(a.rows, a.columns)
    Array.tabulate(a.rows, a.columns){(i, j) =>
      diff.put(i, j,
          Math.min(Math.abs(a.get(i, j) - b.get(i, j)),
          Math.abs(a.get(i, j) + b.get(i, j))))  }
    assert(diff.norm1 < EPSILON, "matrix mismatch: " + diff.norm1)
  }

  test("full rank matrix pca") {
    val m = 5
    val n = 3
    val data = sc.makeRDD(Array.tabulate(m,n){ (a, b) =>
      MatrixEntry(a, b, Math.sin(a+b+a*b)) }.flatten )
    val a = LAUtils.spToDense(SparseMatrix(data, m, n))

    val realPCAArray = Array((0,0,-0.2579), (0,1,-0.6602), (0,2,0.7054),
                        (1,0,-0.1448), (1,1,0.7483),  (1,2,0.6474),
                        (2,0,0.9553),  (2,1,-0.0649),  (2,2,0.2886))
    val realPCA = sc.makeRDD(realPCAArray.map(x => MatrixEntry(x._1, x._2, x._3)))

    val coeffs = LAUtils.denseToSp(new PCA().computePCA(a, n))

    assertMatrixEquals(getDenseMatrix(SparseMatrix(realPCA,n,n)), getDenseMatrix(coeffs))  
  }

  test("sparse matrix full rank matrix pca") {
    val m = 5
    val n = 3
    val data = sc.makeRDD(Array.tabulate(m,n){ (a, b) =>
      MatrixEntry(a, b, Math.sin(a+b+a*b)) }.flatten.drop(1) )
    val a = LAUtils.spToDense(SparseMatrix(data, m, n))

    val realPCAArray = Array((0,0,-0.2579), (0,1,-0.6602), (0,2,0.7054),
                        (1,0,-0.1448), (1,1,0.7483),  (1,2,0.6474),
                        (2,0,0.9553),  (2,1,-0.0649),  (2,2,0.2886))
    val realPCA = sc.makeRDD(realPCAArray.map(x => MatrixEntry(x._1, x._2, x._3)))

    val coeffs = LAUtils.denseToSp(new PCA().computePCA(a, n))

    assertMatrixEquals(getDenseMatrix(SparseMatrix(realPCA,n,n)), getDenseMatrix(coeffs))
  }

  test("truncated matrix pca") {
    val m = 5
    val n = 3
    val data = sc.makeRDD(Array.tabulate(m,n){ (a, b) =>
      MatrixEntry(a, b, Math.sin(a+b+a*b)) }.flatten )
    val a = LAUtils.spToDense(SparseMatrix(data, m, n))

    val realPCAArray = Array((0,0,-0.2579), (0,1,-0.6602),
                        (1,0,-0.1448), (1,1,0.7483),
                        (2,0,0.9553),  (2,1,-0.0649))
    val realPCA = sc.makeRDD(realPCAArray.map(x => MatrixEntry(x._1, x._2, x._3)))

    val k = 2
    val coeffs = LAUtils.denseToSp(new PCA().computePCA(a, k))

    assertMatrixEquals(getDenseMatrix(SparseMatrix(realPCA,n,k)), getDenseMatrix(coeffs))
  }
}


