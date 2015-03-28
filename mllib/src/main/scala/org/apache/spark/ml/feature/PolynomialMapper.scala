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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.types.DataType

/**
 * :: AlphaComponent ::
 * Normalize a vector to have unit norm using the given p-norm.
 */
@AlphaComponent
class PolynomialMapper extends UnaryTransformer[Vector, Vector, PolynomialMapper] {

  /**
   * Normalization in L^p^ space, p = 2 by default.
   * @group param
   */
  val degree = new IntParam(this, "degree", "the polynomial degree to expand", Some(1))

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
   * Multiply two polynomials.
   */
  private def expandVector(lhs: Vector, rhs: Vector): Vector = {
    (lhs, rhs) match {
      case (l: DenseVector, r: DenseVector) =>
        Vectors.dense(l.toArray.flatMap(lx => r.toArray.map(rx => lx * rx)))
      case (SparseVector(lLen, lIdx, lVal), SparseVector(rLen, rIdx, rVal)) =>
        val len = lLen * rLen
        val idx = lIdx.flatMap(li => rIdx.map(ri => li * lLen + ri))
        val value = lVal.flatMap(lv => rVal.map(rv => lv * rv))
        Vectors.sparse(len, idx, value)
      case _ => throw new Exception("vector types are not match.")
    }
  }

  /**
   * Transform a vector of variables into a larger vector which stores the polynomial expansion from
   * degree 1 to degree `degree`.
   */
  private def transform(degree: Int)(feature: Vector): Vector = {
    feature match {
      case f: DenseVector =>
        (2 to degree).foldLeft(Array(feature.copy)) { (vectors, _) =>
          vectors ++ Array(expandVector(feature, vectors.last))
        }.reduce((lhs, rhs) => Vectors.dense(lhs.toArray ++ rhs.toArray))
      case f: SparseVector =>
        (2 to degree).foldLeft(Array(feature.copy)) { (vectors, _) =>
          vectors ++ Array(expandVector(feature, vectors.last))
        }.reduce { case (SparseVector(lLen, lIdx, lVal), SparseVector(rLen, rIdx, rVal)) =>
            Vectors.sparse(lLen + rLen, lIdx ++ rIdx.map(_ + lLen), lVal ++ rVal)
        }
      case _ => throw new Exception("vector type is invalid.")
    }
  }
}
