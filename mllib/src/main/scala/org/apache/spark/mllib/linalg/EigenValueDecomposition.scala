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

import org.apache.spark.annotation.Experimental
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.netlib.util.{intW, doubleW}
import com.github.fommil.netlib.ARPACK

/**
 * :: Experimental ::
 * Represents eigenvalue decomposition factors.
 */
@Experimental
case class EigenValueDecomposition[VType](s: Vector, V: VType)

object EigenValueDecomposition {
  /**
   * Compute the leading k eigenvalues and eigenvectors on a symmetric square matrix using ARPACK.
   * The caller needs to ensure that the input matrix is real symmetric. This function requires
   * memory for `n*(4*k+4)` doubles.
   *
   * @param mul a function that multiplies the symmetric matrix with a Vector.
   * @param n dimension of the square matrix (maximum Int.MaxValue).
   * @param k number of leading eigenvalues required.
   * @param tol tolerance of the eigs computation.
   * @return a dense vector of eigenvalues in descending order and a dense matrix of eigenvectors
   *         (columns of the matrix). The number of computed eigenvalues might be smaller than k.
   */
  private[mllib] def symmetricEigs(mul: Vector => Vector, n: Int, k: Int, tol: Double)
    : (BDV[Double], BDM[Double]) = {
    require(n > k, s"Number of required eigenvalues $k must be smaller than matrix dimension $n")

    val arpack  = ARPACK.getInstance()

    val tolW = new doubleW(tol)
    val nev = new intW(k)
    val ncv = scala.math.min(2*k,n)

    val bmat = "I"
    val which = "LM"

    var iparam = new Array[Int](11)
    iparam(0) = 1
    iparam(2) = 300
    iparam(6) = 1

    var ido = new intW(0)
    var info = new intW(0)
    var resid:Array[Double] = new Array[Double](n)
    var v = new Array[Double](n*ncv)
    var workd = new Array[Double](3*n)
    var workl = new Array[Double](ncv*(ncv+8))
    var ipntr = new Array[Int](11)

    // first call to ARPACK
    arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr, workd,
      workl, workl.length, info)

    val w = BDV(workd)

    while(ido.`val` !=99) {
      if (ido.`val` != -1 && ido.`val` != 1)
        throw new IllegalStateException("ARPACK returns ido = " + ido.`val`)
      // multiply working vector with the matrix
      val inputOffset = ipntr(0) - 1
      val outputOffset = ipntr(1) - 1
      val x = w(inputOffset until inputOffset + n)
      val y = w(outputOffset until outputOffset + n)
      y := BDV(mul(Vectors.fromBreeze(x)).toArray)
      // call ARPACK
      arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr,
        workd, workl, workl.length, info)
    }

    if (info.`val` != 0)
      throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val`)

    val d = new Array[Double](nev.`val`)
    val select = new Array[Boolean](ncv)
    val z = java.util.Arrays.copyOfRange(v, 0, nev.`val` * n)

    arpack.dseupd(true, "A", select, d, z, n, 0.0, bmat, n, which, nev, tol, resid, ncv, v, n,
      iparam, ipntr, workd, workl, workl.length, info)

    val computed = iparam(4)

    val eigenPairs = java.util.Arrays.copyOfRange(d, 0, computed).zipWithIndex.map{
      r => (r._1, java.util.Arrays.copyOfRange(z, r._2 * n, r._2 * n + n))
    }

    val sortedEigenPairs = eigenPairs.sortBy(-1 * _._1)

    // copy eigenvectors in descending order of eigenvalues
    val sortedU = BDM.zeros[Double](n, computed)
    sortedEigenPairs.zipWithIndex.map{
      r => {
        for (i <- 0 until n) {
          sortedU.data(r._2 * n + i) = r._1._2(i)
        }
      }
    }

    (BDV(sortedEigenPairs.map(_._1)), sortedU)
  }
}