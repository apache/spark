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

package org.apache.spark.ml.feature

import scala.annotation.tailrec

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.types.DataType

/**
 * :: AlphaComponent ::
 * Perform feature expansion in a polynomial space. As said in wikipedia of Polynomial Expansion,
 * which is available at [[http://en.wikipedia.org/wiki/Polynomial_expansion]], "In mathematics, an
 * expansion of a product of sums expresses it as a sum of products by using the fact that
 * multiplication distributes over addition". Take a 2-variable feature vector as an example:
 * `(x, y)`, if we want to expand it with degree 2, then we get `(x, y, x * x, x * y, y * y)`.
 */
@AlphaComponent
class PolynomialMapper extends UnaryTransformer[Vector, Vector, PolynomialMapper] {

  /**
   * The polynomial degree to expand, which should be larger than 1.
   * @group param
   */
  val degree = new IntParam(this, "degree", "the polynomial degree to expand", Some(2))

  /** @group getParam */
  def getDegree: Int = get(degree)

  /** @group setParam */
  def setDegree(value: Int): this.type = set(degree, value)

  override protected def createTransformFunc(paramMap: ParamMap): Vector => Vector = {
    PolynomialMapper.transform(getDegree)
  }

  override protected def outputDataType: DataType = new VectorUDT()
}

object PolynomialMapper {
  /**
   * The number that combines k items from N items without repeat, i.e. the binomial coefficient.
   */
  private def binomialCoefficient(N: Int, k: Int): Int = {
    (N - k + 1 to N).product / (1 to k).product
  }

  /**
   * The number of monomials of a `numVariables` vector after expanding at a specific polynomial
   * degree `degree`.
   */
  private def numMonomials(degree: Int, numVariables: Int): Int = {
    binomialCoefficient(numVariables + degree - 1, degree)
  }

  /**
   * The number of monomials of a `numVariables` vector after expanding from polynomial degree 1 to
   * polynomial degree `degree`.
   */
  private def numExpandedDims(degree: Int, numVariables: Int): Int = {
    binomialCoefficient(numVariables + degree, numVariables) - 1
  }

  /**
   * Given a pre-built array of Double, fill it with expanded monomials until a given polynomial
   * degree.
   * @param values the array of Double, which represents a dense vector.
   * @param prevStart the start offset of elements that filled in the last function call.
   * @param prevLen the length of elements that filled in the last function.
   * @param currDegree the current degree that we want to expand.
   * @param finalDegree the final expected degree that we want to expand.
   * @param nVariables number of variables in the original feature vector.
   */
  @tailrec
  private def fillDenseVector(values: Array[Double], prevStart: Int, prevLen: Int, currDegree: Int,
        finalDegree: Int, nVariables: Int): Unit = {

    if (currDegree > finalDegree) {
      return
    }

    val currExpandedVecFrom = prevStart + prevLen
    val currExpandedVecLen = numMonomials(currDegree, nVariables)

    var leftIndex = 0
    var currIndex = currExpandedVecFrom

    while (leftIndex < nVariables) {
      val numToKeep = numMonomials(currDegree - 1, nVariables - leftIndex)
      val prevVecStartIndex = prevStart + prevLen - numToKeep

      var rightIndex = 0
      while (rightIndex < numToKeep) {
        values(currIndex) =
          values(leftIndex) * values(prevVecStartIndex + rightIndex)
        currIndex += 1
        rightIndex += 1
      }

      leftIndex += 1
    }

    fillDenseVector(values, currExpandedVecFrom, currExpandedVecLen, currDegree + 1, finalDegree,
      nVariables)
  }

  /**
   * For polynomial expanding a `SparseVector`, we treat it as a dense vector and call
   * `fillDenseVector` to fill in the `values` of `SparseVector`. For its `indices` part, we encode
   * the indices from `nVariables` one by one, because we do not care of the real indices.
   */
  private def fillPseudoSparseVectorIndices(indices: Array[Int], startFrom: Int, startWith: Int) = {
    var i = startFrom
    var j = startWith
    while (i < indices.size) {
      indices(i) = j
      i += 1
      j += 1
    }
  }

  /**
   * Transform a vector of variables into a larger vector which stores the polynomial expansion from
   * degree 1 to degree `degree`.
   */
  private def transform(degree: Int)(feature: Vector): Vector = {
    val expectedDims = numExpandedDims(degree, feature.size)
    feature match {
      case f: DenseVector =>
        val originalDims = f.size
        val res = Array.fill[Double](expectedDims)(0.0)
        for (i <- 0 until f.size) {
          res(i) = f(i)
        }
        fillDenseVector(res, 0, originalDims, 2, degree, originalDims)
        Vectors.dense(res)

      case f: SparseVector =>
        val originalDims = f.indices.size
        val expandedDims = numExpandedDims(degree, f.indices.size)
        val resIndices = Array.fill[Int](expandedDims)(0)
        val resValues = Array.fill[Double](expandedDims)(0.0)
        for (i <- 0 until f.indices.size) {
          resIndices(i) = f.indices(i)
          resValues(i) = f.values(i)
        }
        fillDenseVector(resValues, 0, f.indices.size, 2, degree, originalDims)
        fillPseudoSparseVectorIndices(resIndices, f.indices.size, feature.size)
        Vectors.sparse(expectedDims, resIndices, resValues)
    }
  }
}
