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

import org.jblas._

class SVDSuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  val EPSILON = 1e-4

  // Return jblas matrix from sparse matrix RDD
  def getDenseMatrix(matrix:RDD[((Int, Int), Double)], m:Int, n:Int) : DoubleMatrix = {
    val ret = DoubleMatrix.zeros(m, n)
    matrix.toArray.map(x => ret.put(x._1._1-1, x._1._2-1, x._2))
    ret
  }

  def assertMatrixEquals(a:DoubleMatrix, b:DoubleMatrix) {
    assert(a.rows == b.rows && a.columns == b.columns, "dimension mismatch")
    val diff = DoubleMatrix.zeros(a.rows, a.columns)
    Array.tabulate(a.rows, a.columns){(i,j) =>
      diff.put(i,j,
          Math.min(Math.abs(a.get(i,j)-b.get(i,j)),
          Math.abs(a.get(i,j)+b.get(i,j))))  }
    assert(diff.norm1 < EPSILON, "matrix mismatch: " + diff.norm1)
  }

  test("full rank matrix svd") {
    val m = 10
    val n = 3
    val data = sc.makeRDD(Array.tabulate(m,n){ (a,b)=>
      ((a+1,b+1), (a+2).toDouble*(b+1)/(1+a+b)) }.flatten )
    val min_svalue = 1.0e-8

    val (u, s, v) = SVD.sparseSVD(data, m, n, min_svalue)
    
    val densea = getDenseMatrix(data, m, n)
    val svd = Singular.sparseSVD(densea)

    val retu = getDenseMatrix(u,m,n)
    val rets = getDenseMatrix(s,n,n)
    val retv = getDenseMatrix(v,n,n)
  
    // check individual decomposition  
    assertMatrixEquals(retu, svd(0))
    assertMatrixEquals(rets, DoubleMatrix.diag(svd(1)))
    assertMatrixEquals(retv, svd(2))

     // check multiplication guarantee
    assertMatrixEquals(retu.mmul(rets).mmul(retv.transpose), densea)  
  }

 test("rank one matrix svd") {
    val m = 10
    val n = 3   
    val data = sc.makeRDD(Array.tabulate(m,n){ (a,b)=>
      ((a+1,b+1), 1.0) }.flatten )
    val min_svalue = 1.0e-4

    val (u, s, v) = SVD.sparseSVD(data, m, n, min_svalue)
    val retrank = s.toArray.length

    assert(retrank == 1, "rank returned not one")

    val densea = getDenseMatrix(data, m, n)
    val svd = Singular.sparseSVD(densea)

    val retu = getDenseMatrix(u,m,retrank)
    val rets = getDenseMatrix(s,retrank,retrank)
    val retv = getDenseMatrix(v,n,retrank)

    // check individual decomposition  
    assertMatrixEquals(retu, svd(0).getColumn(0))
    assertMatrixEquals(rets, DoubleMatrix.diag(svd(1).getRow(0)))
    assertMatrixEquals(retv, svd(2).getColumn(0))

     // check multiplication guarantee
    assertMatrixEquals(retu.mmul(rets).mmul(retv.transpose), densea)  
  }

 test("truncated with min singular value") {
    val m = 10
    val n = 3
    val data = sc.makeRDD(Array.tabulate(m,n){ (a,b)=>
      ((a+1,b+1), (a+2).toDouble*(b+1)/(1+a+b)) }.flatten )
    
    val min_svalue = 5.0 // only one svalue above this

    val (u, s, v) = SVD.sparseSVD(data, m, n, min_svalue)
    val retrank = s.toArray.length

    val densea = getDenseMatrix(data, m, n)
    val svd = Singular.sparseSVD(densea)

    val retu = getDenseMatrix(u,m,retrank)
    val rets = getDenseMatrix(s,retrank,retrank)
    val retv = getDenseMatrix(v,n,retrank)

    assert(retrank == 1, "rank returned not one")
    
    // check individual decomposition  
    assertMatrixEquals(retu, svd(0).getColumn(0))
    assertMatrixEquals(rets, DoubleMatrix.diag(svd(1).getRow(0)))
    assertMatrixEquals(retv, svd(2).getColumn(0))
  }
}
