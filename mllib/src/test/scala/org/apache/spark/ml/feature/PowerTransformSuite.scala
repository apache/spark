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

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.Row

class PowerTransformSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Array[Vector] = _
  @transient var resWithYeoJohnson: Array[Vector] = _
  @transient var resWithBoxCox: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(
      Vectors.dense(1.28331718, 1.18092228, 0.84160269),
      Vectors.dense(0.94293279, 1.60960836, 0.3879099),
      Vectors.dense(1.35235668, 0.21715673, 1.09977091)
    )

    resWithYeoJohnson = Array(
      Vectors.dense(1.88609649e+02, 1.64321816e+00, 1.26875990e+00),
      Vectors.dense(4.39620682e+01, 2.44852195e+00, 4.76941835e-01),
      Vectors.dense(2.46706192e+02, 2.33862299e-01, 1.83629062e+00)
    )

    resWithBoxCox = Array(
      Vectors.dense(0.49024348, 0.17881995, -0.15637811),
      Vectors.dense(-0.05102892, 0.58863196, -0.57612414),
      Vectors.dense(0.69420008, -0.84857822, 0.10051454)
    )
  }

  test("params") {
    ParamsSuite.checkParams(new PowerTransform)
    val lambda = Vectors.dense(1, 0.5, 3)
    val model = new PowerTransformModel("ptm", lambda)
    ParamsSuite.checkParams(model)
  }

  private def assertResult: Row => Unit = {
    case Row(vector1: Vector, vector2: Vector) =>
      assert(vector1 ~== vector2 relTol 1E-5,
        "The vector value is not correct after transformation.")
  }

  test("Yeo-Johnson") {
    /*
      Using the following Python code to load the data and train the model using
      scikit-learn package.

      from sklearn.preprocessing import PowerTransformer
      import numpy as np

      X = np.array([[1.28331718, 1.18092228, 0.84160269],
                    [0.94293279, 1.60960836, 0.3879099],
                    [1.35235668, 0.21715673, 1.09977091]], dtype=np.float)
      pt = PowerTransformer(standardize=False)
      ptm = pt.fit(X)

      >>> ptm.lambdas_
      array([9.00955644, 1.72211468, 2.16092368])
      >>> ptm.transform(X)
      array([[1.88609649e+02, 1.64321816e+00, 1.26875990e+00],
             [4.39620682e+01, 2.44852195e+00, 4.76941835e-01],
             [2.46706192e+02, 2.33862299e-01, 1.83629062e+00]])
     */

    val df = data.zip(resWithYeoJohnson).toSeq.toDF("features", "expected")
    val pt = new PowerTransform()
      .setInputCol("features")
      .setOutputCol("transformed")
      .setModelType("yeo-johnson")

    val ptm = pt.fit(df)
    assert(ptm.lambda ~== Vectors.dense(9.00955644, 1.72211468, 2.16092368) relTol 1e-5)

    val transformed = ptm.transform(df)
    checkVectorSizeOnDF(transformed, "transformed", ptm.numFeatures)

    testTransformer[(Vector, Vector)](df, ptm, "transformed", "expected")(
      assertResult)
  }

  test("Box-Cox") {
    /*
      Python code:

      pt = PowerTransformer(method="box-cox", standardize=False)
      ptm = pt.fit(X)

      >>> ptm.lambdas_
      array([4.92011835, 0.86296595, 1.15354434])
      >>> ptm.transform(X)
      array([[ 0.49024348,  0.17881995, -0.15637811],
             [-0.05102892,  0.58863196, -0.57612414],
             [ 0.69420008, -0.84857822,  0.10051454]])
     */

    val df = data.zip(resWithBoxCox).toSeq.toDF("features", "expected")
    val pt = new PowerTransform()
      .setInputCol("features")
      .setOutputCol("transformed")
      .setModelType("box-cox")

    val ptm = pt.fit(df)
    assert(ptm.lambda ~== Vectors.dense(4.92011835, 0.86296595, 1.15354434) relTol 1e-5)

    val transformed = ptm.transform(df)
    checkVectorSizeOnDF(transformed, "transformed", ptm.numFeatures)

    testTransformer[(Vector, Vector)](df, ptm, "transformed", "expected")(
      assertResult)
  }

  test("PowerTransform read/write") {
    val pt = new PowerTransform()
      .setInputCol("features")
      .setOutputCol("transformed")
      .setModelType("box-cox")
    testDefaultReadWrite(pt)
  }

  test("PowerTransformModel read/write") {
    val lambda = Vectors.dense(1, 0.5, 3)
    val instance = new PowerTransformModel("ptm", lambda)
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.numFeatures === instance.numFeatures)
    assert(newInstance.lambda === instance.lambda)
  }

  test("PowerTransform.makeBins") {
    val seq1 = Seq(
      (0, 0.0, 1L),
      (0, 1.0, 1L),
      (0, 2.0, 2L),
      (1, 1.0, 1L),
      (1, 2.0, 1L),
      (2, 0.5, 3L)
    )

    assert(PowerTransform.makeBins(seq1.iterator, Map.empty[Int, Long]).toSeq === seq1)
    assert(PowerTransform.makeBins(seq1.iterator, Map(0 -> 2L, 1 -> 1L, 2 -> 3L)).toSeq ===
      Seq((0, 0.5, 2L), (0, 2.0, 2L), (1, 1.0, 1L), (1, 2.0, 1L), (2, 0.5, 3L)))
    assert(PowerTransform.makeBins(seq1.iterator, Map(0 -> 4L, 2 -> 5L)).toSeq ===
      Seq((0, 1.25, 4L), (1, 1.0, 1L), (1, 2.0, 1L), (2, 0.5, 3L)))
    assert(PowerTransform.makeBins(seq1.iterator, Map(0 -> 3L, 1 -> 2L)).toSeq ===
      Seq((0, 1.25, 4L), (1, 1.5, 2L), (2, 0.5, 3L)))
    assert(PowerTransform.makeBins(seq1.iterator, Map(0 -> 5L, 1 -> 5L, 2 -> 1L)).toSeq ===
      Seq((0, 1.25, 4L), (1, 1.5, 2L), (2, 0.5, 3L)))

    val seq2 = Seq((0, 0.0, 1L))

    assert(PowerTransform.makeBins(seq2.iterator, Map.empty[Int, Long]).toSeq === seq2)
    assert(PowerTransform.makeBins(seq2.iterator, Map(0 -> 2L, 1 -> 1L, 2 -> 3L)).toSeq === seq2)
  }
}
