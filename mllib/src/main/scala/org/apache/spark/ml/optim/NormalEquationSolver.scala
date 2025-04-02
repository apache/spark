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
package org.apache.spark.ml.optim

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}

import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vectors}
import org.apache.spark.mllib.linalg.CholeskyDecomposition

/**
 * A class to hold the solution to the normal equations A^T^ W A x = A^T^ W b.
 *
 * @param coefficients The least squares coefficients. The last element in the coefficients
 *                     is the intercept when bias is added to A.
 * @param aaInv An option containing the upper triangular part of (A^T^ W A)^-1^, in column major
 *              format. None when an optimization program is used to solve the normal equations.
 * @param objectiveHistory Option containing the objective history when an optimization program is
 *                         used to solve the normal equations. None when an analytic solver is used.
 */
private[optim] class NormalEquationSolution(
    val coefficients: Array[Double],
    val aaInv: Option[Array[Double]],
    val objectiveHistory: Option[Array[Double]])

/**
 * Interface for classes that solve the normal equations locally.
 */
private[optim] sealed trait NormalEquationSolver {

  /** Solve the normal equations from summary statistics. */
  def solve(
      bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector): NormalEquationSolution
}

/**
 * A class that solves the normal equations directly, using Cholesky decomposition.
 */
private[optim] class CholeskySolver extends NormalEquationSolver {

  override def solve(
      bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector): NormalEquationSolution = {
    val k = abBar.size
    val x = CholeskyDecomposition.solve(aaBar.values, abBar.values)
    val aaInv = CholeskyDecomposition.inverse(aaBar.values, k)

    new NormalEquationSolution(x, Some(aaInv), None)
  }
}

/**
 * A class for solving the normal equations using Quasi-Newton optimization methods.
 */
private[optim] class QuasiNewtonSolver(
    fitIntercept: Boolean,
    maxIter: Int,
    tol: Double,
    l1RegFunc: Option[(Int) => Double]) extends NormalEquationSolver {

  override def solve(
      bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector): NormalEquationSolution = {
    val numFeatures = aBar.size
    val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures
    val initialCoefficientsWithIntercept = new Array[Double](numFeaturesPlusIntercept)
    if (fitIntercept) {
      initialCoefficientsWithIntercept(numFeaturesPlusIntercept - 1) = bBar
    }

    val costFun =
      new NormalEquationCostFun(bBar, bbBar, abBar, aaBar, aBar, fitIntercept, numFeatures)
    val optimizer = l1RegFunc.map { func =>
      new BreezeOWLQN[Int, BDV[Double]](maxIter, 10, func, tol)
    }.getOrElse(new BreezeLBFGS[BDV[Double]](maxIter, 10, tol))

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      new BDV[Double](initialCoefficientsWithIntercept))

    val arrayBuilder = mutable.ArrayBuilder.make[Double]
    var state: optimizer.State = null
    while (states.hasNext) {
      state = states.next()
      arrayBuilder += state.adjustedValue
    }
    val x = state.x.toArray.clone()
    new NormalEquationSolution(x, None, Some(arrayBuilder.result()))
  }

  /**
   * NormalEquationCostFun implements Breeze's DiffFunction[T] for the normal equation.
   * It returns the loss and gradient with L2 regularization at a particular point (coefficients).
   * It's used in Breeze's convex optimization routines.
   */
  private class NormalEquationCostFun(
      bBar: Double,
      bbBar: Double,
      ab: DenseVector,
      aa: DenseVector,
      aBar: DenseVector,
      fitIntercept: Boolean,
      numFeatures: Int) extends DiffFunction[BDV[Double]] {

    private val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures

    override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
      val coef = Vectors.fromBreeze(coefficients).toDense
      if (fitIntercept) {
        var j = 0
        var dotProd = 0.0
        val coefValues = coef.values
        val aBarValues = aBar.values
        while (j < numFeatures) {
          dotProd += coefValues(j) * aBarValues(j)
          j += 1
        }
        coefValues(numFeatures) = bBar - dotProd
      }
      val aax = new DenseVector(new Array[Double](numFeaturesPlusIntercept))
      BLAS.dspmv(numFeaturesPlusIntercept, 1.0, aa, coef, 1.0, aax)
      // loss = 1/2 (b^T W b - 2 x^T A^T W b + x^T A^T W A x)
      val loss = 0.5 * bbBar - BLAS.dot(ab, coef) + 0.5 * BLAS.dot(coef, aax)
      // gradient = A^T W A x - A^T W b
      BLAS.axpy(-1.0, ab, aax)
      (loss, aax.asBreeze.toDenseVector)
    }
  }
}

/**
 * Exception thrown when solving a linear system Ax = b for which the matrix A is non-invertible
 * (singular).
 */
private[spark] class SingularMatrixException(message: String, cause: Throwable)
  extends IllegalArgumentException(message, cause) {

  def this(message: String) = this(message, null)
}
