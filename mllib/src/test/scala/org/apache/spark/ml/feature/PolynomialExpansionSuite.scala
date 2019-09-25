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

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.Row

class PolynomialExpansionSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new PolynomialExpansion)
  }

  private val data = Array(
    Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
    Vectors.dense(-2.0, 2.3),
    Vectors.dense(0.0, 0.0, 0.0),
    Vectors.dense(0.6, -1.1, -3.0),
    Vectors.sparse(3, Seq())
  )

  private val twoDegreeExpansion: Array[Vector] = Array(
    Vectors.sparse(9, Array(0, 1, 2, 3, 4), Array(-2.0, 4.0, 2.3, -4.6, 5.29)),
    Vectors.dense(-2.0, 4.0, 2.3, -4.6, 5.29),
    Vectors.dense(new Array[Double](9)),
    Vectors.dense(0.6, 0.36, -1.1, -0.66, 1.21, -3.0, -1.8, 3.3, 9.0),
    Vectors.sparse(9, Array.empty, Array.empty))

  private val threeDegreeExpansion: Array[Vector] = Array(
    Vectors.sparse(19, Array(0, 1, 2, 3, 4, 5, 6, 7, 8),
      Array(-2.0, 4.0, -8.0, 2.3, -4.6, 9.2, 5.29, -10.58, 12.17)),
    Vectors.dense(-2.0, 4.0, -8.0, 2.3, -4.6, 9.2, 5.29, -10.58, 12.17),
    Vectors.dense(new Array[Double](19)),
    Vectors.dense(0.6, 0.36, 0.216, -1.1, -0.66, -0.396, 1.21, 0.726, -1.331, -3.0, -1.8,
      -1.08, 3.3, 1.98, -3.63, 9.0, 5.4, -9.9, -27.0),
    Vectors.sparse(19, Array.empty, Array.empty))

  def assertTypeOfVector(lhs: Vector, rhs: Vector): Unit = {
    assert((lhs, rhs) match {
      case (v1: DenseVector, v2: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after polynomial expansion.")
  }

  def assertValues(lhs: Vector, rhs: Vector): Unit = {
    assert(lhs ~== rhs absTol 1e-1, "The vector value is not correct after polynomial expansion.")
  }

  test("Polynomial expansion with default parameter") {
    val df = data.zip(twoDegreeExpansion).toSeq.toDF("features", "expected")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")

    testTransformer[(Vector, Vector)](df, polynomialExpansion, "polyFeatures", "expected") {
      case Row(expanded: Vector, expected: Vector) =>
        assertTypeOfVector(expanded, expected)
        assertValues(expanded, expected)
    }
  }

  test("Polynomial expansion with setter") {
    val df = data.zip(threeDegreeExpansion).toSeq.toDF("features", "expected")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    testTransformer[(Vector, Vector)](df, polynomialExpansion, "polyFeatures", "expected") {
      case Row(expanded: Vector, expected: Vector) =>
        assertTypeOfVector(expanded, expected)
        assertValues(expanded, expected)
    }
  }

  test("Polynomial expansion with degree 1 is identity on vectors") {
    val df = data.zip(data).toSeq.toDF("features", "expected")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(1)

    testTransformer[(Vector, Vector)](df, polynomialExpansion, "polyFeatures", "expected") {
      case Row(expanded: Vector, expected: Vector) =>
        assertValues(expanded, expected)
    }
  }

  test("read/write") {
    val t = new PolynomialExpansion()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setDegree(3)
    testDefaultReadWrite(t)
  }

  test("SPARK-17027. Integer overflow in PolynomialExpansion.getPolySize") {
    val data: Array[(Vector, Int, Int)] = Array(
      (Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0), 3002, 4367),
      (Vectors.sparse(5, Seq((0, 1.0), (4, 5.0))), 3002, 4367),
      (Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 8007, 12375)
    )

    val df = data.toSeq.toDF("features", "expectedPoly10size", "expectedPoly11size")

    val t = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")

    for (i <- Seq(10, 11)) {
      testTransformer[(Vector, Int, Int)](
        df,
        t.setDegree(i),
        s"expectedPoly${i}size",
        "polyFeatures") { case Row(size: Int, expected: Vector) =>
            assert(size === expected.size)
      }
    }
  }
}

