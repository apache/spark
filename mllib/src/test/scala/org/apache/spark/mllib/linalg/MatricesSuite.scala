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

import java.util.Random

import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.ClassTag

import breeze.linalg.{CSCMatrix, Matrix => BM}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.ml.{linalg => newlinalg}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.serializer.KryoSerializer

class MatricesSuite extends SparkFunSuite {
  test("kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    def check[T: ClassTag](t: T): Unit = {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }

    val m = 3
    val n = 2
    val denseValues = Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0)
    val denseMat = Matrices.dense(m, n, denseValues).asInstanceOf[DenseMatrix]

    val sparseValues = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val sparseMat =
      Matrices.sparse(m, n, colPtrs, rowIndices, sparseValues).asInstanceOf[SparseMatrix]
    check(denseMat)
    check(sparseMat)
  }

  test("dense matrix construction") {
    val m = 3
    val n = 2
    val values = Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0)
    val mat = Matrices.dense(m, n, values).asInstanceOf[DenseMatrix]
    assert(mat.numRows === m)
    assert(mat.numCols === n)
    assert(mat.values.eq(values), "should not copy data")
  }

  test("dense matrix construction with wrong dimension") {
    intercept[RuntimeException] {
      Matrices.dense(3, 2, Array(0.0, 1.0, 2.0))
    }
  }

  test("sparse matrix construction") {
    val m = 3
    val n = 4
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 2, 4, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val mat = Matrices.sparse(m, n, colPtrs, rowIndices, values).asInstanceOf[SparseMatrix]
    assert(mat.numRows === m)
    assert(mat.numCols === n)
    assert(mat.values.eq(values), "should not copy data")
    assert(mat.colPtrs.eq(colPtrs), "should not copy data")
    assert(mat.rowIndices.eq(rowIndices), "should not copy data")

    val entries: Array[(Int, Int, Double)] = Array((2, 2, 3.0), (1, 0, 1.0), (2, 0, 2.0),
        (1, 2, 2.0), (2, 2, 2.0), (1, 2, 2.0), (0, 0, 0.0))

    val mat2 = SparseMatrix.fromCOO(m, n, entries)
    assert(mat.asBreeze === mat2.asBreeze)
    assert(mat2.values.length == 4)
  }

  test("sparse matrix construction with wrong number of elements") {
    intercept[IllegalArgumentException] {
      Matrices.sparse(3, 2, Array(0, 1), Array(1, 2, 1), Array(0.0, 1.0, 2.0))
    }

    intercept[IllegalArgumentException] {
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(0.0, 1.0, 2.0))
    }
  }

  test("index in matrices incorrect input") {
    val sm = Matrices.sparse(3, 2, Array(0, 2, 3), Array(1, 2, 1), Array(0.0, 1.0, 2.0))
    val dm = Matrices.dense(3, 2, Array(0.0, 2.3, 1.4, 3.2, 1.0, 9.1))
    Array(sm, dm).foreach { mat =>
      intercept[IllegalArgumentException] { mat.index(4, 1) }
      intercept[IllegalArgumentException] { mat.index(1, 4) }
      intercept[IllegalArgumentException] { mat.index(-1, 2) }
      intercept[IllegalArgumentException] { mat.index(1, -2) }
    }
  }

  test("equals") {
    val dm1 = Matrices.dense(2, 2, Array(0.0, 1.0, 2.0, 3.0))
    assert(dm1 === dm1)
    assert(dm1 !== dm1.transpose)

    val dm2 = Matrices.dense(2, 2, Array(0.0, 2.0, 1.0, 3.0))
    assert(dm1 === dm2.transpose)

    val sm1 = dm1.asInstanceOf[DenseMatrix].toSparse
    assert(sm1 === sm1)
    assert(sm1 === dm1)
    assert(sm1 !== sm1.transpose)

    val sm2 = dm2.asInstanceOf[DenseMatrix].toSparse
    assert(sm1 === sm2.transpose)
    assert(sm1 === dm2.transpose)
  }

  test("matrix copies are deep copies") {
    val m = 3
    val n = 2

    val denseMat = Matrices.dense(m, n, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val denseCopy = denseMat.copy

    assert(!denseMat.toArray.eq(denseCopy.toArray))

    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val sparseMat = Matrices.sparse(m, n, colPtrs, rowIndices, values)
    val sparseCopy = sparseMat.copy

    assert(!sparseMat.toArray.eq(sparseCopy.toArray))
  }

  test("matrix indexing and updating") {
    val m = 3
    val n = 2
    val allValues = Array(0.0, 1.0, 2.0, 3.0, 4.0, 0.0)

    val denseMat = new DenseMatrix(m, n, allValues)

    assert(denseMat(0, 1) === 3.0)
    assert(denseMat(0, 1) === denseMat.values(3))
    assert(denseMat(0, 1) === denseMat(3))
    assert(denseMat(0, 0) === 0.0)

    denseMat.update(0, 0, 10.0)
    assert(denseMat(0, 0) === 10.0)
    assert(denseMat.values(0) === 10.0)

    val sparseValues = Array(1.0, 2.0, 3.0, 4.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 0, 1)
    val sparseMat = new SparseMatrix(m, n, colPtrs, rowIndices, sparseValues)

    assert(sparseMat(0, 1) === 3.0)
    assert(sparseMat(0, 1) === sparseMat.values(2))
    assert(sparseMat(0, 0) === 0.0)

    intercept[NoSuchElementException] {
      sparseMat.update(0, 0, 10.0)
    }

    intercept[NoSuchElementException] {
      sparseMat.update(2, 1, 10.0)
    }

    sparseMat.update(0, 1, 10.0)
    assert(sparseMat(0, 1) === 10.0)
    assert(sparseMat.values(2) === 10.0)
  }

  test("toSparse, toDense") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val allValues = Array(1.0, 2.0, 0.0, 0.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(0, 1, 1, 2)

    val spMat1 = new SparseMatrix(m, n, colPtrs, rowIndices, values)
    val deMat1 = new DenseMatrix(m, n, allValues)

    val spMat2 = deMat1.toSparse
    val deMat2 = spMat1.toDense

    assert(spMat1.asBreeze === spMat2.asBreeze)
    assert(deMat1.asBreeze === deMat2.asBreeze)
  }

  test("map, update") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val allValues = Array(1.0, 2.0, 0.0, 0.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(0, 1, 1, 2)

    val spMat1 = new SparseMatrix(m, n, colPtrs, rowIndices, values)
    val deMat1 = new DenseMatrix(m, n, allValues)
    val deMat2 = deMat1.map(_ * 2)
    val spMat2 = spMat1.map(_ * 2)
    deMat1.update(_ * 2)
    spMat1.update(_ * 2)

    assert(spMat1.toArray === spMat2.toArray)
    assert(deMat1.toArray === deMat2.toArray)
  }

  test("transpose") {
    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val dAT = dA.transpose
    val sAT = sA.transpose
    val dATexpected =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sATexpected =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    assert(dAT.asBreeze === dATexpected.asBreeze)
    assert(sAT.asBreeze === sATexpected.asBreeze)
    assert(dA(1, 0) === dAT(0, 1))
    assert(dA(2, 1) === dAT(1, 2))
    assert(sA(1, 0) === sAT(0, 1))
    assert(sA(2, 1) === sAT(1, 2))

    assert(!dA.toArray.eq(dAT.toArray), "has to have a new array")
    assert(dA.values.eq(dAT.transpose.values), "should not copy array")

    assert(dAT.toSparse.asBreeze === sATexpected.asBreeze)
    assert(sAT.toDense.asBreeze === dATexpected.asBreeze)
  }

  test("foreachActive") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val allValues = Array(1.0, 2.0, 0.0, 0.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(0, 1, 1, 2)

    val sp = new SparseMatrix(m, n, colPtrs, rowIndices, values)
    val dn = new DenseMatrix(m, n, allValues)

    val dnMap = MutableMap[(Int, Int), Double]()
    dn.foreachActive { (i, j, value) =>
      dnMap.put((i, j), value)
    }
    assert(dnMap.size === 6)
    assert(dnMap((0, 0)) === 1.0)
    assert(dnMap((1, 0)) === 2.0)
    assert(dnMap((2, 0)) === 0.0)
    assert(dnMap((0, 1)) === 0.0)
    assert(dnMap((1, 1)) === 4.0)
    assert(dnMap((2, 1)) === 5.0)

    val spMap = MutableMap[(Int, Int), Double]()
    sp.foreachActive { (i, j, value) =>
      spMap.put((i, j), value)
    }
    assert(spMap.size === 4)
    assert(spMap((0, 0)) === 1.0)
    assert(spMap((1, 0)) === 2.0)
    assert(spMap((1, 1)) === 4.0)
    assert(spMap((2, 1)) === 5.0)
  }

  test("horzcat, vertcat, eye, speye") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val allValues = Array(1.0, 2.0, 0.0, 0.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(0, 1, 1, 2)
    // transposed versions
    val allValuesT = Array(1.0, 0.0, 2.0, 4.0, 0.0, 5.0)
    val colPtrsT = Array(0, 1, 3, 4)
    val rowIndicesT = Array(0, 0, 1, 1)

    val spMat1 = new SparseMatrix(m, n, colPtrs, rowIndices, values)
    val deMat1 = new DenseMatrix(m, n, allValues)
    val spMat1T = new SparseMatrix(n, m, colPtrsT, rowIndicesT, values)
    val deMat1T = new DenseMatrix(n, m, allValuesT)

    // should equal spMat1 & deMat1 respectively
    val spMat1TT = spMat1T.transpose
    val deMat1TT = deMat1T.transpose

    val deMat2 = Matrices.eye(3)
    val spMat2 = Matrices.speye(3)
    val deMat3 = Matrices.eye(2)
    val spMat3 = Matrices.speye(2)

    val spHorz = Matrices.horzcat(Array(spMat1, spMat2))
    val spHorz2 = Matrices.horzcat(Array(spMat1, deMat2))
    val spHorz3 = Matrices.horzcat(Array(deMat1, spMat2))
    val deHorz1 = Matrices.horzcat(Array(deMat1, deMat2))
    val deHorz2 = Matrices.horzcat(Array.empty[Matrix])

    assert(deHorz1.numRows === 3)
    assert(spHorz2.numRows === 3)
    assert(spHorz3.numRows === 3)
    assert(spHorz.numRows === 3)
    assert(deHorz1.numCols === 5)
    assert(spHorz2.numCols === 5)
    assert(spHorz3.numCols === 5)
    assert(spHorz.numCols === 5)
    assert(deHorz2.numRows === 0)
    assert(deHorz2.numCols === 0)
    assert(deHorz2.toArray.length === 0)

    assert(deHorz1 ~== spHorz2.asInstanceOf[SparseMatrix].toDense absTol 1e-15)
    assert(spHorz2 ~== spHorz3 absTol 1e-15)
    assert(spHorz(0, 0) === 1.0)
    assert(spHorz(2, 1) === 5.0)
    assert(spHorz(0, 2) === 1.0)
    assert(spHorz(1, 2) === 0.0)
    assert(spHorz(1, 3) === 1.0)
    assert(spHorz(2, 4) === 1.0)
    assert(spHorz(1, 4) === 0.0)
    assert(deHorz1(0, 0) === 1.0)
    assert(deHorz1(2, 1) === 5.0)
    assert(deHorz1(0, 2) === 1.0)
    assert(deHorz1(1, 2) == 0.0)
    assert(deHorz1(1, 3) === 1.0)
    assert(deHorz1(2, 4) === 1.0)
    assert(deHorz1(1, 4) === 0.0)

    // containing transposed matrices
    val spHorzT = Matrices.horzcat(Array(spMat1TT, spMat2))
    val spHorz2T = Matrices.horzcat(Array(spMat1TT, deMat2))
    val spHorz3T = Matrices.horzcat(Array(deMat1TT, spMat2))
    val deHorz1T = Matrices.horzcat(Array(deMat1TT, deMat2))

    assert(deHorz1T ~== deHorz1 absTol 1e-15)
    assert(spHorzT ~== spHorz absTol 1e-15)
    assert(spHorz2T ~== spHorz2 absTol 1e-15)
    assert(spHorz3T ~== spHorz3 absTol 1e-15)

    intercept[IllegalArgumentException] {
      Matrices.horzcat(Array(spMat1, spMat3))
    }

    intercept[IllegalArgumentException] {
      Matrices.horzcat(Array(deMat1, spMat3))
    }

    val spVert = Matrices.vertcat(Array(spMat1, spMat3))
    val deVert1 = Matrices.vertcat(Array(deMat1, deMat3))
    val spVert2 = Matrices.vertcat(Array(spMat1, deMat3))
    val spVert3 = Matrices.vertcat(Array(deMat1, spMat3))
    val deVert2 = Matrices.vertcat(Array.empty[Matrix])

    assert(deVert1.numRows === 5)
    assert(spVert2.numRows === 5)
    assert(spVert3.numRows === 5)
    assert(spVert.numRows === 5)
    assert(deVert1.numCols === 2)
    assert(spVert2.numCols === 2)
    assert(spVert3.numCols === 2)
    assert(spVert.numCols === 2)
    assert(deVert2.numRows === 0)
    assert(deVert2.numCols === 0)
    assert(deVert2.toArray.length === 0)

    assert(deVert1 ~== spVert2.asInstanceOf[SparseMatrix].toDense absTol 1e-15)
    assert(spVert2 ~== spVert3 absTol 1e-15)
    assert(spVert(0, 0) === 1.0)
    assert(spVert(2, 1) === 5.0)
    assert(spVert(3, 0) === 1.0)
    assert(spVert(3, 1) === 0.0)
    assert(spVert(4, 1) === 1.0)
    assert(deVert1(0, 0) === 1.0)
    assert(deVert1(2, 1) === 5.0)
    assert(deVert1(3, 0) === 1.0)
    assert(deVert1(3, 1) === 0.0)
    assert(deVert1(4, 1) === 1.0)

    // containing transposed matrices
    val spVertT = Matrices.vertcat(Array(spMat1TT, spMat3))
    val deVert1T = Matrices.vertcat(Array(deMat1TT, deMat3))
    val spVert2T = Matrices.vertcat(Array(spMat1TT, deMat3))
    val spVert3T = Matrices.vertcat(Array(deMat1TT, spMat3))

    assert(deVert1T ~== deVert1 absTol 1e-15)
    assert(spVertT ~== spVert absTol 1e-15)
    assert(spVert2T ~== spVert2 absTol 1e-15)
    assert(spVert3T ~== spVert3 absTol 1e-15)

    intercept[IllegalArgumentException] {
      Matrices.vertcat(Array(spMat1, spMat2))
    }

    intercept[IllegalArgumentException] {
      Matrices.vertcat(Array(deMat1, spMat2))
    }
  }

  test("zeros") {
    val mat = Matrices.zeros(2, 3).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 3)
    assert(mat.values.forall(_ == 0.0))
  }

  test("ones") {
    val mat = Matrices.ones(2, 3).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 3)
    assert(mat.values.forall(_ == 1.0))
  }

  test("eye") {
    val mat = Matrices.eye(2).asInstanceOf[DenseMatrix]
    assert(mat.numCols === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 0.0, 0.0, 1.0))
  }

  test("rand") {
    val rng = mock[Random]
    when(rng.nextDouble()).thenReturn(1.0, 2.0, 3.0, 4.0)
    val mat = Matrices.rand(2, 2, rng).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
  }

  test("randn") {
    val rng = mock[Random]
    when(rng.nextGaussian()).thenReturn(1.0, 2.0, 3.0, 4.0)
    val mat = Matrices.randn(2, 2, rng).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
  }

  test("diag") {
    val mat = Matrices.diag(Vectors.dense(1.0, 2.0)).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 0.0, 0.0, 2.0))
  }

  test("sprand") {
    val rng = mock[Random]
    when(rng.nextInt(4)).thenReturn(0, 1, 1, 3, 2, 2, 0, 1, 3, 0)
    when(rng.nextDouble()).thenReturn(1.0, 2.0, 3.0, 4.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
    val mat = SparseMatrix.sprand(4, 4, 0.25, rng)
    assert(mat.numRows === 4)
    assert(mat.numCols === 4)
    assert(mat.rowIndices.toSeq === Seq(3, 0, 2, 1))
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
    val mat2 = SparseMatrix.sprand(2, 3, 1.0, rng)
    assert(mat2.rowIndices.toSeq === Seq(0, 1, 0, 1, 0, 1))
    assert(mat2.colPtrs.toSeq === Seq(0, 2, 4, 6))
  }

  test("sprandn") {
    val rng = mock[Random]
    when(rng.nextInt(4)).thenReturn(0, 1, 1, 3, 2, 2, 0, 1, 3, 0)
    when(rng.nextGaussian()).thenReturn(1.0, 2.0, 3.0, 4.0)
    val mat = SparseMatrix.sprandn(4, 4, 0.25, rng)
    assert(mat.numRows === 4)
    assert(mat.numCols === 4)
    assert(mat.rowIndices.toSeq === Seq(3, 0, 2, 1))
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
  }

  test("MatrixUDT") {
    val dm1 = new DenseMatrix(2, 2, Array(0.9, 1.2, 2.3, 9.8))
    val dm2 = new DenseMatrix(3, 2, Array(0.0, 1.21, 2.3, 9.8, 9.0, 0.0))
    val dm3 = new DenseMatrix(0, 0, Array())
    val sm1 = dm1.toSparse
    val sm2 = dm2.toSparse
    val sm3 = dm3.toSparse
    val mUDT = new MatrixUDT()
    Seq(dm1, dm2, dm3, sm1, sm2, sm3).foreach {
        mat => assert(mat.toArray === mUDT.deserialize(mUDT.serialize(mat)).toArray)
    }
    assert(mUDT.typeName == "matrix")
    assert(mUDT.simpleString == "matrix")
  }

  test("toString") {
    val empty = Matrices.ones(0, 0)
    empty.toString(0, 0)

    val mat = Matrices.rand(5, 10, new Random())
    mat.toString(-1, -5)
    mat.toString(0, 0)
    mat.toString(Int.MinValue, Int.MinValue)
    mat.toString(Int.MaxValue, Int.MaxValue)
    var lines = mat.toString(6, 50).split('\n')
    assert(lines.length == 5 && lines.forall(_.length <= 50))

    lines = mat.toString(5, 100).split('\n')
    assert(lines.length == 5 && lines.forall(_.length <= 100))
  }

  test("numNonzeros and numActives") {
    val dm1 = Matrices.dense(3, 2, Array(0, 0, -1, 1, 0, 1))
    assert(dm1.numNonzeros === 3)
    assert(dm1.numActives === 6)

    val sm1 = Matrices.sparse(3, 2, Array(0, 2, 3), Array(0, 2, 1), Array(0.0, -1.2, 0.0))
    assert(sm1.numNonzeros === 1)
    assert(sm1.numActives === 3)
  }

  test("fromBreeze with sparse matrix") {
    // colPtr.last does NOT always equal to values.length in breeze SCSMatrix and
    // invocation of compact() may be necessary. Refer to SPARK-11507
    val bm1: BM[Double] = new CSCMatrix[Double](
      Array(1.0, 1, 1), 3, 3, Array(0, 1, 2, 3), Array(0, 1, 2))
    val bm2: BM[Double] = new CSCMatrix[Double](
      Array(1.0, 2, 2, 4), 3, 3, Array(0, 0, 2, 4), Array(1, 2, 1, 2))
    val sum = bm1 + bm2
    Matrices.fromBreeze(sum)
  }

  test("Test FromBreeze when Breeze.CSCMatrix.rowIndices has trailing zeros. - SPARK-20687") {
    // (2, 0, 0)
    // (2, 0, 0)
    val mat1Brz = Matrices.sparse(2, 3, Array(0, 2, 2, 2), Array(0, 1), Array(2, 2)).asBreeze
    // (2, 1E-15, 1E-15)
    // (2, 1E-15, 1E-15)
    val mat2Brz = Matrices.sparse(2, 3,
      Array(0, 2, 4, 6),
      Array(0, 0, 0, 1, 1, 1),
      Array(2, 1E-15, 1E-15, 2, 1E-15, 1E-15)).asBreeze
    val t1Brz = mat1Brz - mat2Brz
    val t2Brz = mat2Brz - mat1Brz
    // The following operations raise exceptions on un-patch Matrices.fromBreeze
    val t1 = Matrices.fromBreeze(t1Brz)
    val t2 = Matrices.fromBreeze(t2Brz)
    // t1 == t1Brz && t2 == t2Brz
    assert((t1.asBreeze - t1Brz).iterator.map((x) => math.abs(x._2)).sum < 1E-15)
    assert((t2.asBreeze - t2Brz).iterator.map((x) => math.abs(x._2)).sum < 1E-15)
  }

  test("row/col iterator") {
    val dm = new DenseMatrix(3, 2, Array(0, 1, 2, 3, 4, 0))
    val sm = dm.toSparse
    val rows = Seq(Vectors.dense(0, 3), Vectors.dense(1, 4), Vectors.dense(2, 0))
    val cols = Seq(Vectors.dense(0, 1, 2), Vectors.dense(3, 4, 0))
    for (m <- Seq(dm, sm)) {
      assert(m.rowIter.toSeq === rows)
      assert(m.colIter.toSeq === cols)
      assert(m.transpose.rowIter.toSeq === cols)
      assert(m.transpose.colIter.toSeq === rows)
    }
  }

  test("conversions between new local linalg and mllib linalg") {
    val dm: DenseMatrix = new DenseMatrix(3, 2, Array(0.0, 0.0, 1.0, 0.0, 2.0, 3.5))
    val sm: SparseMatrix = dm.toSparse
    val sm0: Matrix = sm.asInstanceOf[Matrix]
    val dm0: Matrix = dm.asInstanceOf[Matrix]

    def compare(oldM: Matrix, newM: newlinalg.Matrix): Unit = {
      assert(oldM.toArray === newM.toArray)
      assert(oldM.numCols === newM.numCols)
      assert(oldM.numRows === newM.numRows)
    }

    val newSM: newlinalg.SparseMatrix = sm.asML
    val newDM: newlinalg.DenseMatrix = dm.asML
    val newSM0: newlinalg.Matrix = sm0.asML
    val newDM0: newlinalg.Matrix = dm0.asML
    assert(newSM0.isInstanceOf[newlinalg.SparseMatrix])
    assert(newDM0.isInstanceOf[newlinalg.DenseMatrix])
    compare(sm, newSM)
    compare(dm, newDM)
    compare(sm0, newSM0)
    compare(dm0, newDM0)

    val oldSM: SparseMatrix = SparseMatrix.fromML(newSM)
    val oldDM: DenseMatrix = DenseMatrix.fromML(newDM)
    val oldSM0: Matrix = Matrices.fromML(newSM0)
    val oldDM0: Matrix = Matrices.fromML(newDM0)
    assert(oldSM0.isInstanceOf[SparseMatrix])
    assert(oldDM0.isInstanceOf[DenseMatrix])
    compare(oldSM, newSM)
    compare(oldDM, newDM)
    compare(oldSM0, newSM0)
    compare(oldDM0, newDM0)
  }

  test("implicit conversions between new local linalg and mllib linalg") {

    def mllibMatrixToTriple(m: Matrix): (Array[Double], Int, Int) =
      (m.toArray, m.numCols, m.numRows)

    def mllibDenseMatrixToTriple(m: DenseMatrix): (Array[Double], Int, Int) =
      (m.toArray, m.numCols, m.numRows)

    def mllibSparseMatrixToTriple(m: SparseMatrix): (Array[Double], Int, Int) =
      (m.toArray, m.numCols, m.numRows)

    def mlMatrixToTriple(m: newlinalg.Matrix): (Array[Double], Int, Int) =
      (m.toArray, m.numCols, m.numRows)

    def mlDenseMatrixToTriple(m: newlinalg.DenseMatrix): (Array[Double], Int, Int) =
      (m.toArray, m.numCols, m.numRows)

    def mlSparseMatrixToTriple(m: newlinalg.SparseMatrix): (Array[Double], Int, Int) =
      (m.toArray, m.numCols, m.numRows)

    def compare(m1: (Array[Double], Int, Int), m2: (Array[Double], Int, Int)): Unit = {
      assert(m1._1 === m2._1)
      assert(m1._2 === m2._2)
      assert(m1._3 === m2._3)
    }

    val dm: DenseMatrix = new DenseMatrix(3, 2, Array(0.0, 0.0, 1.0, 0.0, 2.0, 3.5))
    val sm: SparseMatrix = dm.toSparse
    val sm0: Matrix = sm.asInstanceOf[Matrix]
    val dm0: Matrix = dm.asInstanceOf[Matrix]

    val newSM: newlinalg.SparseMatrix = sm.asML
    val newDM: newlinalg.DenseMatrix = dm.asML
    val newSM0: newlinalg.Matrix = sm0.asML
    val newDM0: newlinalg.Matrix = dm0.asML

    import org.apache.spark.mllib.linalg.MatrixImplicits._

    compare(mllibMatrixToTriple(dm0), mllibMatrixToTriple(newDM0))
    compare(mllibMatrixToTriple(sm0), mllibMatrixToTriple(newSM0))

    compare(mllibDenseMatrixToTriple(dm), mllibDenseMatrixToTriple(newDM))
    compare(mllibSparseMatrixToTriple(sm), mllibSparseMatrixToTriple(newSM))

    compare(mlMatrixToTriple(dm0), mlMatrixToTriple(newDM))
    compare(mlMatrixToTriple(sm0), mlMatrixToTriple(newSM0))

    compare(mlDenseMatrixToTriple(dm), mlDenseMatrixToTriple(newDM))
    compare(mlSparseMatrixToTriple(sm), mlSparseMatrixToTriple(newSM))
  }
}
