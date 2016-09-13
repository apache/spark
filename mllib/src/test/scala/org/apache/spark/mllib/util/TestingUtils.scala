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

package org.apache.spark.mllib.util

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.util.TestingUtils._

object TestingUtils {

  case class CompareVectorRightSide(
    fun: (Vector, Vector, Double) => Boolean, y: Vector, eps: Double, method: String)

  /**
   * Implicit class for comparing two vectors using relative tolerance or absolute tolerance.
   */
  implicit class VectorWithAlmostEquals(val x: Vector) {

    /**
     * When the difference of two vectors are within eps, returns true; otherwise, returns false.
     */
    def ~=(r: CompareVectorRightSide): Boolean = r.fun(x, r.y, r.eps)

    /**
     * When the difference of two vectors are within eps, returns false; otherwise, returns true.
     */
    def !~=(r: CompareVectorRightSide): Boolean = !r.fun(x, r.y, r.eps)

    /**
     * Throws exception when the difference of two vectors are NOT within eps;
     * otherwise, returns true.
     */
    def ~==(r: CompareVectorRightSide): Boolean = {
      if (!r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Expected $x and ${r.y} to be within ${r.eps}${r.method} for all elements.", 0)
      }
      true
    }

    /**
     * Throws exception when the difference of two vectors are within eps; otherwise, returns true.
     */
    def !~==(r: CompareVectorRightSide): Boolean = {
      if (r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Did not expect $x and ${r.y} to be within ${r.eps}${r.method} for all elements.", 0)
      }
      true
    }

    /**
     * Comparison using absolute tolerance.
     */
    def absTol(eps: Double): CompareVectorRightSide = CompareVectorRightSide(
      (x: Vector, y: Vector, eps: Double) => {
        x.size == y.size && x.toArray.zip(y.toArray).forall(x => x._1 ~= x._2 absTol eps)
      }, x, eps, ABS_TOL_MSG)

    /**
     * Comparison using relative tolerance. Note that comparing against sparse vector
     * with elements having value of zero will raise exception because it involves with
     * comparing against zero.
     */
    def relTol(eps: Double): CompareVectorRightSide = CompareVectorRightSide(
      (x: Vector, y: Vector, eps: Double) => {
        x.size == y.size && x.toArray.zip(y.toArray).forall(x => x._1 ~= x._2 relTol eps)
      }, x, eps, REL_TOL_MSG)

    override def toString: String = x.toString
  }

  case class CompareMatrixRightSide(
     fun: (Matrix, Matrix, Double) => Boolean, y: Matrix, eps: Double, method: String)

  /**
   * Implicit class for comparing two matrices using relative tolerance or absolute tolerance.
   */
  implicit class MatrixWithAlmostEquals(val x: Matrix) {

    /**
     * When the difference of two matrices are within eps, returns true; otherwise, returns false.
     */
    def ~=(r: CompareMatrixRightSide): Boolean = r.fun(x, r.y, r.eps)

    /**
     * When the difference of two matrices are within eps, returns false; otherwise, returns true.
     */
    def !~=(r: CompareMatrixRightSide): Boolean = !r.fun(x, r.y, r.eps)

    /**
     * Throws exception when the difference of two matrices are NOT within eps;
     * otherwise, returns true.
     */
    def ~==(r: CompareMatrixRightSide): Boolean = {
      if (!r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Expected \n$x\n and \n${r.y}\n to be within ${r.eps}${r.method} for all elements.", 0)
      }
      true
    }

    /**
     * Throws exception when the difference of two matrices are within eps; otherwise, returns true.
     */
    def !~==(r: CompareMatrixRightSide): Boolean = {
      if (r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Did not expect \n$x\n and \n${r.y}\n to be within " +
            "${r.eps}${r.method} for all elements.", 0)
      }
      true
    }

    /**
     * Comparison using absolute tolerance.
     */
    def absTol(eps: Double): CompareMatrixRightSide = CompareMatrixRightSide(
      (x: Matrix, y: Matrix, eps: Double) => {
        x.numRows == y.numRows && x.numCols == y.numCols &&
          x.toArray.zip(y.toArray).forall(x => x._1 ~= x._2 absTol eps)
      }, x, eps, ABS_TOL_MSG)

    /**
     * Comparison using relative tolerance. Note that comparing against sparse vector
     * with elements having value of zero will raise exception because it involves with
     * comparing against zero.
     */
    def relTol(eps: Double): CompareMatrixRightSide = CompareMatrixRightSide(
      (x: Matrix, y: Matrix, eps: Double) => {
        x.numRows == y.numRows && x.numCols == y.numCols &&
          x.toArray.zip(y.toArray).forall(x => x._1 ~= x._2 relTol eps)
      }, x, eps, REL_TOL_MSG)

    override def toString: String = x.toString
  }

}
