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

package org.apache.spark.mllib.linalg.distance

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Matrix


/**
 * validator trait for distance function
 *
 * a distance function is required to satisfy the following conditions
 * 1. d(x, y) >= 0 (non-negative)
 * 2. d(x, y) = 0 if and only if x = y (identity of indiscernibles)
 * 3. d(x, y) = d(y, x) (symmetry)
 * 4. d(x, z) <= d(x, y) + d(y, z) (triangle inequality)
 *
 * @see http://en.wikipedia.org/wiki/Distance_metric
 */
@Experimental
private[distance]
trait DistanceFunctionValidator extends Function1[Matrix, Boolean]

/**
 * a validator for checking non-negative
 */
@Experimental
private[distance]
object NonNegativeValidator extends DistanceFunctionValidator {

  /**
   * validate non-negative
   * d(x, y) >= 0
   *
   * @param matrix distance matrix
   * @return Boolean
   */
  override def apply(matrix: Matrix): Boolean = {
    val tests = for (i <- 1 to (matrix.numRows - 1); j <- i to (matrix.numCols - 1)) yield {
      if(matrix(i, j) >= 0.0) true else false
    }
    tests.forall(_ == true)
  }
}

/**
 * a validator for identity of indiscernibles
 */
@Experimental
private[distance]
object IdentityOfIndiscerniblesValidator extends DistanceFunctionValidator {

  /**
   * validate identity of indiscernibles
   * d(x, y) = d(y, x) if and only if x = y
   *
   * @param matrix distance matrix
   * @return Boolean
   */
  override def apply(matrix: Matrix): Boolean = {
    val tests = for (i <- 1 to (matrix.numRows - 1)) yield {
      if(matrix(i, i) == 0.0) true else false
    }
    tests.forall(_ == true)
  }
}

/**
 * a validator for symmetry
 */
@Experimental
private[distance]
object SymmetryValidator extends DistanceFunctionValidator {

  /**
   * validate symmetry
   * d(x, y) = d(y, x)
   *
   * @param matrix distance matrix
   * @return Boolean
   */
  override def apply(matrix: Matrix): Boolean = {
    val tests = for (i <- 1 to (matrix.numRows - 1); j <- i to (matrix.numCols - 1)) yield {
      if(matrix(i, j) == matrix(j, i)) true else false
    }
    tests.forall(_ == true)
  }
}

/**
 * a validator for triangle inequality
 */
@Experimental
private[distance]
object TriangleInequalityValidator extends DistanceFunctionValidator {

  val EPSILON_EXPONENT = -10

  /**
   * validate triangle inequality
   * d(x, z) <= d(x, y) + d(y, z)
   *
   * @param matrix distance matrix
   * @return Boolean
   */
  override def apply(matrix: Matrix): Boolean = {
    val size = matrix.numRows - 1
    val tests = for (
      i <- 1 to size;
      j <- 0 to size if j != i;
      k <- 0 to size if k != i && k != j) yield {

      // if too small number like 8.881784197001252E-16 cannot be compare 0.0 in Scala
      val diff = matrix(i, k) - (matrix(i, j) + matrix(j, k))
      if (diff < 0.0 || Math.log10(diff) <= EPSILON_EXPONENT) true else false
    }
    tests.forall(_ == true)
  }
}
