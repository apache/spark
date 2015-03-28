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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

private case class DataSet2(features: Vector)

class PolynomialMapperSuite extends FunSuite with MLlibTestSparkContext {

  @transient var data: Array[Vector] = _
  @transient var dataFrame: DataFrame = _
  @transient var polynomialMapper: PolynomialMapper = _
  @transient var oneDegreeExpansion: Array[Vector] = _
  @transient var threeDegreeExpansion: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(
      Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.6, -1.1, -3.0),
      Vectors.sparse(3, Seq())
    )

    oneDegreeExpansion = data

    threeDegreeExpansion = Array(
      Vectors.sparse(
        19,Array(0,1,3,4,6,9,10,12,15),Array(-2.0,2.3,4.0,-4.6,5.29,-8.0,9.2,-10.58,12.17)),
      Vectors.dense(Array.fill[Double](19)(0.0)),
      Vectors.dense(0.6,-1.1,-3.0,0.36,-0.66,-1.8,1.21,3.3,9.0,0.216,-0.396,-1.08,0.73,1.98,5.4,
        -1.33,-3.63,-9.9,-27.0),
      Vectors.sparse(19, Seq())
    )

    val sqlContext = new SQLContext(sc)
    dataFrame = sqlContext.createDataFrame(sc.parallelize(data, 2).map(DataSet2))
    polynomialMapper = new PolynomialMapper()
      .setInputCol("features")
      .setOutputCol("poly_features")
  }

  def collectResult(result: DataFrame): Array[Vector] = {
    result.select("poly_features").collect().map {
      case Row(features: Vector) => features
    }
  }

  def assertTypeOfVector(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall {
      case (v1: DenseVector, v2: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after normalization.")
  }

  def assertValues(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall { (vector1, vector2) =>
      vector1 ~== vector2 absTol 1E-1
    }, "The vector value is not correct after normalization.")
  }

  test("Polynomial expansion with default parameter") {
    val result = collectResult(polynomialMapper.transform(dataFrame))

    assertTypeOfVector(data, result)

    assertValues(result, oneDegreeExpansion)
  }

  test("Polynomial expansion with setter") {
    polynomialMapper.setDegree(3)

    val result = collectResult(polynomialMapper.transform(dataFrame))

    assertTypeOfVector(data, result)

    assertValues(result, threeDegreeExpansion)
  }
}

