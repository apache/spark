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

package edu.uci.eecs.spectralLDA.utils

import breeze.linalg.{max, min, DenseMatrix, DenseVector}
import scala.language.postfixOps
import scala.util.control.Breaks._
import scalaxy.loops._

object NonNegativeAdjustment {
  /** Projection of one eigenvector matrix from the CP decomposition into l1-simplex
   *
   * Given an eigenvector w, we have to decide whether to return proj(w) or proj(-w) as
   * the result.
   *
   * We notice that the Duchi algorithm produces the same result for any w with a
   * parallel shift. We thus compute proj(w - min(w)) and proj((-w) - min(-w)) and compare
   * the shift "theta" from the Duchi algorithm. We retain the one with smaller shift "theta".
   *
   * Ref:
   * Duchi, John, Efficient Projections onto the l1-Ball for Learning in High Dimensions, 2008
   *
   * @param M     One eigenvector matrix from the result of CP decomposition
   * @return      {best of proj(w) or proj(-w), where w is each column of M}
   */
  def simplexProj_Matrix(M: DenseMatrix[Double]): DenseMatrix[Double] = {
    val M_onSimplex = DenseMatrix.zeros[Double](M.rows, M.cols)

    for (i <- 0 until M.cols optimized) {
      val (projectedVector, theta) = simplexProj(M(::, i) - min(M(::, i)))
      val (projectedVectorRev, thetaRev) = simplexProj(- M(::, i) - min(- M(::, i)))

      if (theta < thetaRev) {
        M_onSimplex(::, i) := projectedVector
      }
      else {
        M_onSimplex(::, i) := projectedVectorRev
      }
    }

    M_onSimplex
  }

  /** Projection of a vector onto a simplex
   *
   * Given a length-n vector V, find a vector W=(w_i)_{1\le i\le n} in the simplex that
   * \sum_{i=1}^n w_i=1, w_i>0 \forall i, by minimising the Euclidean distance between V and W.
   *
   * Ref:
   * Duchi, John, Efficient Projections onto the l1-Ball for Learning in High Dimensions, 2008
   *
   * @param V  The input vector
   * @return   Projected vector and the shift
   */
  def simplexProj(V: DenseVector[Double]): (DenseVector[Double], Double) = {
    // val z:Double = 1.0
    val len: Int = V.length
    val U: DenseVector[Double] = DenseVector(V.copy.toArray.sortWith(_ > _))
    val cums: DenseVector[Double] = DenseVector(AlgebraUtil.Cumsum(U.toArray).map(x => x-1))
    val Index: DenseVector[Double] = DenseVector((1 to (len + 1)).toArray.map(x => 1.0/x.toDouble))
    val InterVec: DenseVector[Double] = cums :* Index
    val TobefindMax: DenseVector[Double] = U - InterVec
    var maxIndex : Int = 0
    // find maxIndex
    breakable{
      for (i <- 0 until len optimized) {
        if (TobefindMax(len - i - 1) > 0) {
          maxIndex = len - i - 1
          break()
        }
      }
    }
    val theta: Double = InterVec(maxIndex)
    val P_norm: DenseVector[Double] = max(V - theta, 0.0)
    (P_norm, theta)
  }
}

