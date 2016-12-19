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

/**
 * Utils for algebric operations.
 * Created by furongh on 11/2/15.
 */

import breeze.linalg._
import scala.language.postfixOps
import scalaxy.loops._


object AlgebraUtil {
  def matrixNormalization(B: DenseMatrix[Double]): DenseMatrix[Double] = {
    val A: DenseMatrix[Double] = B.copy
    val colNorms: DenseVector[Double] = norm(A(::, *)).t.toDenseVector

    for (i <- 0 until A.cols optimized) {
      A(::, i) :/= colNorms(i)
    }
    A
  }

  def isConverged(oldA: DenseMatrix[Double],
                  newA: DenseMatrix[Double],
                  dotProductThreshold: Double = 0.99): Boolean = {
    if (oldA == null || oldA.size == 0) {
      return false
    }

    val dprod = diag(oldA.t * newA)

    all(dprod :> dotProductThreshold)
  }

  def Cumsum(xs: Array[Double]): Array[Double] = {
    xs.scanLeft(0.0)(_ + _).tail
  }
}
