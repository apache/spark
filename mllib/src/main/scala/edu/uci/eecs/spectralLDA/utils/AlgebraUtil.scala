package edu.uci.eecs.spectralLDA.utils

/**
 * Utils for algebric operations.
 * Created by furongh on 11/2/15.
 */

import breeze.linalg._
import scalaxy.loops._
import scala.language.postfixOps

object AlgebraUtil {

  def matrixNormalization(B: DenseMatrix[Double]): DenseMatrix[Double] = {
    val A: DenseMatrix[Double] = B.copy
    val colNorms: DenseVector[Double] = norm(A(::, *)).toDenseVector

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
    println(s"dot(oldA, newA): ${diag(oldA.t * newA)}")

    all(dprod :> dotProductThreshold)
  }

  def Cumsum(xs: Array[Double]): Array[Double] = {
    xs.scanLeft(0.0)(_ + _).tail
  }
  
}
