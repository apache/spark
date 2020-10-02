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

package org.apache.spark.mllib.linalg.distributed

import java.util.Arrays

import scala.util.Random

import breeze.linalg.{norm => brzNorm, svd => brzSvd, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.abs

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.MAX_RESULT_SIZE
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._

class RowMatrixSuite extends SparkFunSuite with MLlibTestSparkContext {

  val m = 4
  val n = 3
  val arr = Array(0.0, 3.0, 6.0, 9.0, 1.0, 4.0, 7.0, 0.0, 2.0, 5.0, 8.0, 1.0)
  val denseData = Seq(
    Vectors.dense(0.0, 1.0, 2.0),
    Vectors.dense(3.0, 4.0, 5.0),
    Vectors.dense(6.0, 7.0, 8.0),
    Vectors.dense(9.0, 0.0, 1.0)
  )
  val sparseData = Seq(
    Vectors.sparse(3, Seq((1, 1.0), (2, 2.0))),
    Vectors.sparse(3, Seq((0, 3.0), (1, 4.0), (2, 5.0))),
    Vectors.sparse(3, Seq((0, 6.0), (1, 7.0), (2, 8.0))),
    Vectors.sparse(3, Seq((0, 9.0), (2, 1.0)))
  )

  val principalComponents = BDM(
    (0.0, 1.0, 0.0),
    (math.sqrt(2.0) / 2.0, 0.0, math.sqrt(2.0) / 2.0),
    (math.sqrt(2.0) / 2.0, 0.0, - math.sqrt(2.0) / 2.0))
  val explainedVariance = BDV(4.0 / 7.0, 3.0 / 7.0, 0.0)

  var denseMat: RowMatrix = _
  var sparseMat: RowMatrix = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    denseMat = new RowMatrix(sc.parallelize(denseData, 2))
    sparseMat = new RowMatrix(sc.parallelize(sparseData, 2))
  }

  test("size") {
    assert(denseMat.numRows() === m)
    assert(denseMat.numCols() === n)
    assert(sparseMat.numRows() === m)
    assert(sparseMat.numCols() === n)
  }

  test("empty rows") {
    val rows = sc.parallelize(Seq[Vector](), 1)
    val emptyMat = new RowMatrix(rows)
    intercept[RuntimeException] {
      emptyMat.numCols()
    }
    intercept[RuntimeException] {
      emptyMat.numRows()
    }
  }

  test("toBreeze") {
    val expected = BDM(
      (0.0, 1.0, 2.0),
      (3.0, 4.0, 5.0),
      (6.0, 7.0, 8.0),
      (9.0, 0.0, 1.0))
    for (mat <- Seq(denseMat, sparseMat)) {
      assert(mat.toBreeze() === expected)
    }
  }

  test("gram") {
    val expected =
      Matrices.dense(n, n, Array(126.0, 54.0, 72.0, 54.0, 66.0, 78.0, 72.0, 78.0, 94.0))
    for (mat <- Seq(denseMat, sparseMat)) {
      val G = mat.computeGramianMatrix()
      assert(G.asBreeze === expected.asBreeze)
    }
  }

  test("getTreeAggregateIdealDepth") {
    val nbPartitions = 100
    val vectors = sc.emptyRDD[Vector]
      .repartition(nbPartitions)
    val rowMat = new RowMatrix(vectors)

    assert(rowMat.getTreeAggregateIdealDepth(100 * 1024 * 1024) === 2)
    assert(rowMat.getTreeAggregateIdealDepth(110 * 1024 * 1024) === 3)
    assert(rowMat.getTreeAggregateIdealDepth(700 * 1024 * 1024) === 10)

    val zeroSizeException = intercept[Exception]{
      rowMat.getTreeAggregateIdealDepth(0)
    }
    assert(zeroSizeException.getMessage.contains("zero-size object to aggregate"))
    val objectBiggerThanResultSize = intercept[Exception]{
      rowMat.getTreeAggregateIdealDepth(1100 * 1024 * 1024)
    }
    assert(objectBiggerThanResultSize.getMessage.contains("it's bigger than maxResultSize"))
  }

  test("SPARK-33043: getTreeAggregateIdealDepth with unlimited driver size") {
    val originalMaxResultSize = sc.conf.get[Long](MAX_RESULT_SIZE)
    sc.conf.set(MAX_RESULT_SIZE, 0L)
    try {
      val nbPartitions = 100
      val vectors = sc.emptyRDD[Vector]
        .repartition(nbPartitions)
      val rowMat = new RowMatrix(vectors)
      assert(rowMat.getTreeAggregateIdealDepth(700 * 1024 * 1024) === 1)
    } finally {
      sc.conf.set(MAX_RESULT_SIZE, originalMaxResultSize)
    }
  }

  test("similar columns") {
    val colMags = Vectors.dense(math.sqrt(126), math.sqrt(66), math.sqrt(94))
    val expected = BDM(
      (0.0, 54.0, 72.0),
      (0.0, 0.0, 78.0),
      (0.0, 0.0, 0.0))

    for (i <- 0 until n; j <- 0 until n) {
      expected(i, j) /= (colMags(i) * colMags(j))
    }

    for (mat <- Seq(denseMat, sparseMat)) {
      val G = mat.columnSimilarities(0.11).toBreeze()
      for (i <- 0 until n; j <- 0 until n) {
        if (expected(i, j) > 0) {
          val actual = expected(i, j)
          val estimate = G(i, j)
          assert(math.abs(actual - estimate) / actual < 0.2,
            s"Similarities not close enough: $actual vs $estimate")
        }
      }
    }

    for (mat <- Seq(denseMat, sparseMat)) {
      val G = mat.columnSimilarities()
      assert(closeToZero(G.toBreeze() - expected))
    }

    for (mat <- Seq(denseMat, sparseMat)) {
      val G = mat.columnSimilaritiesDIMSUM(colMags.toArray, 150.0)
      assert(closeToZero(G.toBreeze() - expected))
    }
  }

  test("svd of a full-rank matrix") {
    for (mat <- Seq(denseMat, sparseMat)) {
      for (mode <- Seq("auto", "local-svd", "local-eigs", "dist-eigs")) {
        val localMat = mat.toBreeze()
        val brzSvd.SVD(localU, localSigma, localVt) = brzSvd(localMat)
        val localV: BDM[Double] = localVt.t.toDenseMatrix
        for (k <- 1 to n) {
          val skip = (mode == "local-eigs" || mode == "dist-eigs") && k == n
          if (!skip) {
            val svd = mat.computeSVD(k, computeU = true, 1e-9, 300, 1e-10, mode)
            val U = svd.U
            val s = svd.s
            val V = svd.V
            assert(U.numRows() === m)
            assert(U.numCols() === k)
            assert(s.size === k)
            assert(V.numRows === n)
            assert(V.numCols === k)
            assertColumnEqualUpToSign(U.toBreeze(), localU, k)
            assertColumnEqualUpToSign(V.asBreeze.asInstanceOf[BDM[Double]], localV, k)
            assert(closeToZero(s.asBreeze.asInstanceOf[BDV[Double]] - localSigma(0 until k)))
          }
        }
        val svdWithoutU = mat.computeSVD(1, computeU = false, 1e-9, 300, 1e-10, mode)
        assert(svdWithoutU.U === null)
      }
    }
  }

  test("svd of a low-rank matrix") {
    val rows = sc.parallelize(Array.fill(4)(Vectors.dense(1.0, 1.0, 1.0)), 2)
    val mat = new RowMatrix(rows, 4, 3)
    for (mode <- Seq("auto", "local-svd", "local-eigs", "dist-eigs")) {
      val svd = mat.computeSVD(2, computeU = true, 1e-6, 300, 1e-10, mode)
      assert(svd.s.size === 1, s"should not return zero singular values but got ${svd.s}")
      assert(svd.U.numRows() === 4)
      assert(svd.U.numCols() === 1)
      assert(svd.V.numRows === 3)
      assert(svd.V.numCols === 1)
    }
  }

  test("validate k in svd") {
    for (mat <- Seq(denseMat, sparseMat)) {
      intercept[IllegalArgumentException] {
        mat.computeSVD(-1)
      }
    }
  }

  def closeToZero(G: BDM[Double]): Boolean = {
    G.valuesIterator.map(math.abs).sum < 1e-6
  }

  def closeToZero(v: BDV[Double]): Boolean = {
    brzNorm(v, 1.0) < 1e-6
  }

  def assertColumnEqualUpToSign(A: BDM[Double], B: BDM[Double], k: Int): Unit = {
    assert(A.rows === B.rows)
    for (j <- 0 until k) {
      val aj = A(::, j)
      val bj = B(::, j)
      assert(closeToZero(aj - bj) || closeToZero(aj + bj),
        s"The $j-th columns mismatch: $aj and $bj")
    }
  }

  test("pca") {
    for (mat <- Seq(denseMat, sparseMat); k <- 1 to n) {
      val (pc, expVariance) = mat.computePrincipalComponentsAndExplainedVariance(k)
      assert(pc.numRows === n)
      assert(pc.numCols === k)
      assertColumnEqualUpToSign(pc.asBreeze.asInstanceOf[BDM[Double]], principalComponents, k)
      assert(
        closeToZero(BDV(expVariance.toArray) -
        BDV(Arrays.copyOfRange(explainedVariance.data, 0, k))))
      // Check that this method returns the same answer
      assert(pc === mat.computePrincipalComponents(k))
    }
  }

  test("multiply a local matrix") {
    val B = Matrices.dense(n, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    for (mat <- Seq(denseMat, sparseMat)) {
      val AB = mat.multiply(B)
      assert(AB.numRows() === m)
      assert(AB.numCols() === 2)
      assert(AB.rows.collect().toSeq === Seq(
        Vectors.dense(5.0, 14.0),
        Vectors.dense(14.0, 50.0),
        Vectors.dense(23.0, 86.0),
        Vectors.dense(2.0, 32.0)
      ))
    }
  }

  test("compute column summary statistics") {
    for (mat <- Seq(denseMat, sparseMat)) {
      val summary = mat.computeColumnSummaryStatistics()
      // Run twice to make sure no internal states are changed.
      for (k <- 0 to 1) {
        assert(summary.mean === Vectors.dense(4.5, 3.0, 4.0), "mean mismatch")
        assert(summary.variance === Vectors.dense(15.0, 10.0, 10.0), "variance mismatch")
        assert(summary.count === m, "count mismatch.")
        assert(summary.numNonzeros === Vectors.dense(3.0, 3.0, 4.0), "nnz mismatch")
        assert(summary.max === Vectors.dense(9.0, 7.0, 8.0), "max mismatch")
        assert(summary.min === Vectors.dense(0.0, 0.0, 1.0), "column mismatch.")
        assert(summary.normL2 === Vectors.dense(math.sqrt(126), math.sqrt(66), math.sqrt(94)),
          "magnitude mismatch.")
        assert(summary.normL1 === Vectors.dense(18.0, 12.0, 16.0), "L1 norm mismatch")
      }
    }
  }

  test("QR Decomposition") {
    for (mat <- Seq(denseMat, sparseMat)) {
      val result = mat.tallSkinnyQR(true)
      val expected = breeze.linalg.qr.reduced(mat.toBreeze())
      val calcQ = result.Q
      val calcR = result.R
      assert(closeToZero(abs(expected.q) - abs(calcQ.toBreeze())))
      assert(closeToZero(abs(expected.r) - abs(calcR.asBreeze.asInstanceOf[BDM[Double]])))
      assert(closeToZero(calcQ.multiply(calcR).toBreeze - mat.toBreeze()))
      // Decomposition without computing Q
      val rOnly = mat.tallSkinnyQR(computeQ = false)
      assert(rOnly.Q == null)
      assert(closeToZero(abs(expected.r) - abs(rOnly.R.asBreeze.asInstanceOf[BDM[Double]])))
    }
  }

  test("dense vector covariance accuracy (SPARK-26158)") {
    val denseData = Seq(
      Vectors.dense(100000.000004, 199999.999999),
      Vectors.dense(100000.000012, 200000.000002),
      Vectors.dense(99999.9999931, 200000.000003),
      Vectors.dense(99999.9999977, 200000.000001)
    )
    val denseMat = new RowMatrix(sc.parallelize(denseData, 2))

    val result = denseMat.computeCovariance()
    val expected = breeze.linalg.cov(denseMat.toBreeze())
    assert(closeToZero(abs(expected) - abs(result.asBreeze.asInstanceOf[BDM[Double]])))
  }

  test("compute covariance") {
    for (mat <- Seq(denseMat, sparseMat)) {
      val result = mat.computeCovariance()
      val expected = breeze.linalg.cov(mat.toBreeze())
      assert(closeToZero(abs(expected) - abs(result.asBreeze.asInstanceOf[BDM[Double]])))
    }
  }

  test("covariance matrix is symmetric (SPARK-10875)") {
    val rdd = RandomRDDs.normalVectorRDD(sc, 100, 10, 0, 0)
    val matrix = new RowMatrix(rdd)
    val cov = matrix.computeCovariance()
    for (i <- 0 until cov.numRows; j <- 0 until i) {
      assert(cov(i, j) === cov(j, i))
    }
  }

  test("QR decomposition should aware of empty partition (SPARK-16369)") {
    val mat: RowMatrix = new RowMatrix(sc.parallelize(denseData, 1))
    val qrResult = mat.tallSkinnyQR(true)

    val matWithEmptyPartition = new RowMatrix(sc.parallelize(denseData, 8))
    val qrResult2 = matWithEmptyPartition.tallSkinnyQR(true)

    assert(qrResult.Q.numCols() === qrResult2.Q.numCols(), "Q matrix ncol not match")
    assert(qrResult.Q.numRows() === qrResult2.Q.numRows(), "Q matrix nrow not match")
    qrResult.Q.rows.collect().zip(qrResult2.Q.rows.collect())
      .foreach(x => assert(x._1 ~== x._2 relTol 1E-8, "Q matrix not match"))

    qrResult.R.toArray.zip(qrResult2.R.toArray)
      .foreach(x => assert(x._1 ~== x._2 relTol 1E-8, "R matrix not match"))
  }
}

class RowMatrixClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  var mat: RowMatrix = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val m = 4
    val n = 200000
    val rows = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => Vectors.dense(Array.fill(n)(random.nextDouble())))
    }
    mat = new RowMatrix(rows)
  }

  test("task size should be small in svd") {
    val svd = mat.computeSVD(1, computeU = true)
  }

  test("task size should be small in summarize") {
    val summary = mat.computeColumnSummaryStatistics()
  }
}
