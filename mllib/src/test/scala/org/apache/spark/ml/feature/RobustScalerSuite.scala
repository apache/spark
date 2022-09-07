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

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.Row

class RobustScalerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Array[Vector] = _
  @transient var resWithScaling: Array[Vector] = _
  @transient var resWithCentering: Array[Vector] = _
  @transient var resWithBoth: Array[Vector] = _
  @transient var dataWithNaN: Array[Vector] = _
  @transient var resWithNaN: Array[Vector] = _
  @transient var highDimData: Array[Vector] = _
  @transient var highDimRes: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // median = [2.0, -2.0]
    // 1st quartile = [1.0, -3.0]
    // 3st quartile = [3.0, -1.0]
    // quantile range = IQR = [2.0, 2.0]
    data = Array(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(2.0, -2.0),
      Vectors.dense(3.0, -3.0),
      Vectors.dense(4.0, -4.0)
    )

    /*
      Using the following Python code to load the data and train the model using
      scikit-learn package.

      from sklearn.preprocessing import RobustScaler
      import numpy as np
      X = np.array([[0, 0], [1, -1], [2, -2], [3, -3], [4, -4]], dtype=np.float)
      scaler = RobustScaler(with_centering=True, with_scaling=False).fit(X)

      >>> scaler.center_
      array([ 2., -2.])
      >>> scaler.scale_
      array([2., 2.])
      >>> scaler.transform(X)
      array([[-2.,  2.],
             [-1.,  1.],
             [ 0.,  0.],
             [ 1., -1.],
             [ 2., -2.]])
     */
    resWithCentering = Array(
      Vectors.dense(-2.0, 2.0),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(2.0, -2.0)
    )

    /*
      Python code:

      scaler = RobustScaler(with_centering=False, with_scaling=True).fit(X)
      >>> scaler.transform(X)
      array([[ 0. ,  0. ],
             [ 0.5, -0.5],
             [ 1. , -1. ],
             [ 1.5, -1.5],
             [ 2. , -2. ]])
     */
    resWithScaling = Array(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.5, -0.5),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(1.5, -1.5),
      Vectors.dense(2.0, -2.0)
    )

    /*
      Python code:

      scaler = RobustScaler(with_centering=True, with_scaling=True).fit(X)
      >>> scaler.transform(X)
      array([[-1. ,  1. ],
            [-0.5,  0.5],
            [ 0. ,  0. ],
            [ 0.5, -0.5],
            [ 1. , -1. ]])
     */
    resWithBoth = Array(
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-0.5, 0.5),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.5, -0.5),
      Vectors.dense(1.0, -1.0)
    )

    dataWithNaN = Array(
      Vectors.dense(0.0, Double.NaN),
      Vectors.dense(Double.NaN, 0.0),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(2.0, -2.0),
      Vectors.dense(3.0, -3.0),
      Vectors.dense(4.0, -4.0)
    )

    resWithNaN = Array(
      Vectors.dense(0.0, Double.NaN),
      Vectors.dense(Double.NaN, 0.0),
      Vectors.dense(0.5, -0.5),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(1.5, -1.5),
      Vectors.dense(2.0, -2.0)
    )

    // median = [2.0, ...]
    // 1st quartile = [1.0, ...]
    // 3st quartile = [3.0, ...]
    // quantile range = IQR = [2.0, ...]
    highDimData = Array(
      Vectors.dense(Array.fill(2000)(0.0)),
      Vectors.dense(Array.fill(2000)(1.0)),
      Vectors.dense(Array.fill(2000)(2.0)),
      Vectors.dense(Array.fill(2000)(3.0)),
      Vectors.dense(Array.fill(2000)(4.0))
    )

    highDimRes = Array(
      Vectors.dense(Array.fill(2000)(0.0)),
      Vectors.dense(Array.fill(2000)(0.5)),
      Vectors.dense(Array.fill(2000)(1.0)),
      Vectors.dense(Array.fill(2000)(1.5)),
      Vectors.dense(Array.fill(2000)(2.0))
    )
  }


  private def assertResult: Row => Unit = {
    case Row(vector1: Vector, vector2: Vector) =>
      assert(vector1 ~== vector2 absTol 1E-5,
        "The vector value is not correct after transformation.")
  }

  test("params") {
    ParamsSuite.checkParams(new RobustScaler)
    ParamsSuite.checkParams(new RobustScalerModel("empty",
      Vectors.dense(1.0), Vectors.dense(2.0)))
  }

  test("Scaling with default parameter") {
    val df0 = data.zip(resWithScaling).toSeq.toDF("features", "expected")

    val robustScalerEst0 = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
    val robustScaler0 = robustScalerEst0.fit(df0)
    MLTestingUtils.checkCopyAndUids(robustScalerEst0, robustScaler0)

    testTransformer[(Vector, Vector)](df0, robustScaler0, "scaled_features", "expected")(
      assertResult)
  }

  test("Scaling with setter") {
    val df1 = data.zip(resWithBoth).toSeq.toDF("features", "expected")
    val df2 = data.zip(resWithCentering).toSeq.toDF("features", "expected")
    val df3 = data.zip(data).toSeq.toDF("features", "expected")

    val robustScaler1 = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithCentering(true)
      .setWithScaling(true)
      .fit(df1)

    val robustScaler2 = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithCentering(true)
      .setWithScaling(false)
      .fit(df2)

    val robustScaler3 = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithCentering(false)
      .setWithScaling(false)
      .fit(df3)

    testTransformer[(Vector, Vector)](df1, robustScaler1, "scaled_features", "expected")(
      assertResult)
    testTransformer[(Vector, Vector)](df2, robustScaler2, "scaled_features", "expected")(
      assertResult)
    testTransformer[(Vector, Vector)](df3, robustScaler3, "scaled_features", "expected")(
      assertResult)
  }

  test("sparse data and withCentering") {
    val someSparseData = data.zipWithIndex.map {
      case (vec, i) => if (i % 2 == 0) vec.toSparse else vec
    }
    val df = someSparseData.zip(resWithCentering).toSeq.toDF("features", "expected")
    val robustScaler = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithCentering(true)
      .setWithScaling(false)
      .fit(df)
    testTransformer[(Vector, Vector)](df, robustScaler, "scaled_features", "expected")(
      assertResult)
  }

  test("deal with NaN values") {
    val df0 = dataWithNaN.zip(resWithNaN).toSeq.toDF("features", "expected")

    val robustScalerEst0 = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
    val robustScaler0 = robustScalerEst0.fit(df0)
    MLTestingUtils.checkCopyAndUids(robustScalerEst0, robustScaler0)

    testTransformer[(Vector, Vector)](df0, robustScaler0, "scaled_features", "expected")(
      assertResult)
  }

  test("deal with high-dim dataset") {
    val df0 = highDimData.zip(highDimRes).toSeq.toDF("features", "expected")

    val robustScalerEst0 = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
    val robustScaler0 = robustScalerEst0.fit(df0)
    MLTestingUtils.checkCopyAndUids(robustScalerEst0, robustScaler0)

    testTransformer[(Vector, Vector)](df0, robustScaler0, "scaled_features", "expected")(
      assertResult)
  }

  test("RobustScaler read/write") {
    val t = new RobustScaler()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setWithCentering(false)
      .setWithScaling(true)
    testDefaultReadWrite(t)
  }

  test("RobustScalerModel read/write") {
    val instance = new RobustScalerModel("myRobustScalerModel",
      Vectors.dense(1.0, 2.0), Vectors.dense(3.0, 4.0))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.range === instance.range)
    assert(newInstance.median === instance.median)
  }

}
