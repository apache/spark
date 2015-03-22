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

package breeze.optimize.linear

import breeze.linalg.{DenseMatrix, DenseVector, norm}
import breeze.numerics.abs
import breeze.optimize.proximal.QuadraticMinimizer
import breeze.util.Implicits._
import breeze.util.SerializableLogging

/**
 * Power Method to compute maximum eigen value and companion object to compute
 * minimum eigen value through inverse power iterations
 * @author debasish83
 */
class PowerMethod(maxIters: Int = 10,tolerance: Double = 1E-5) extends SerializableLogging {

  import PowerMethod.{BDM, BDV}

  case class State private[PowerMethod] (eigenValue: Double, eigenVector: BDV, ay: BDV, iter: Int, converged: Boolean)

  //memory allocation for the eigen vector result
  def normalize(ynorm: BDV, y: BDV) = {
    val normInit = norm(y)
    ynorm := y
    ynorm *= 1.0 / normInit
  }

  def initialState(n: Int): State = {
    val ynorm = DenseVector.zeros[Double](n)
    val ay = DenseVector.zeros[Double](n)
    State(0, ynorm, ay, 0, false)
  }

  def reset(A: BDM, y: BDV, init: State): State = {
    import init._
    require(eigenVector.length == y.length, s"PowerMethod:reset mismatch in state dimension")
    normalize(eigenVector, y)
    QuadraticMinimizer.gemv(1.0, A, eigenVector, 0.0, ay)
    val lambda = nextEigen(eigenVector, ay)
    State(lambda, eigenVector, ay, 0, false)
  }

  //in-place modification of eigen vector
  def nextEigen(eigenVector: BDV, ay: BDV) = {
    val lambda = eigenVector dot ay
    eigenVector := ay
    val norm1 = norm(ay)
    eigenVector *= 1.0/norm1
    if (lambda < 0.0) eigenVector *= -1.0
    lambda
  }

  def iterations(A: BDM, y: BDV): Iterator[State] = {
    val init = initialState(y.length)
    iterations(A, y, init)
  }

  def iterations(A: BDM, y: BDV, initialState: State): Iterator[State] = Iterator.iterate(reset(A, y, initialState)) { state =>
    import state._
    QuadraticMinimizer.gemv(1.0, A, eigenVector, 0.0, ay)
    val lambda = nextEigen(eigenVector, ay)
    val val_dif = abs(lambda - eigenValue)
    if (val_dif <= tolerance || iter > maxIters) State(lambda, eigenVector, ay, iter + 1, true)
    else State(lambda, eigenVector, ay, iter + 1, false)
  }.takeUpToWhere(_.converged)

  def iterateAndReturnState(A: BDM, y: BDV): State = {
    iterations(A, y).last
  }

  def eigen(A: BDM, y: BDV): Double = {
    iterateAndReturnState(A, y).eigenValue
  }
}

object PowerMethod {

  type BDV = DenseVector[Double]
  type BDM = DenseMatrix[Double]

  def inverse(maxIters: Int = 10, tolerance: Double = 1E-5) : PowerMethod = new PowerMethod(maxIters, tolerance) {
    override def reset(A: BDM, y: BDV, init: State): State = {
      import init._
      require(eigenVector.length == y.length, s"InversePowerMethod:reset mismatch in state dimension")
      normalize(eigenVector, y)
      ay := eigenVector
      QuadraticMinimizer.dpotrs(A, ay)
      val lambda = nextEigen(eigenVector, ay)
      State(lambda, eigenVector, ay, 0, false)
    }

    override def iterations(A: BDM, y: BDV, initialState: State): Iterator[State] = Iterator.iterate(reset(A, y, initialState)) { state =>
      import state._
      ay := eigenVector
      QuadraticMinimizer.dpotrs(A, ay)
      val lambda = nextEigen(eigenVector, ay)
      val val_dif = abs(lambda - eigenValue)
      if (val_dif <= tolerance || iter > maxIters) State(lambda, eigenVector, ay, iter + 1, true)
      else State(lambda, eigenVector, ay, iter + 1, false)
    }.takeUpToWhere(_.converged)
  }
}