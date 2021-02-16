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

class StandardScalerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Array[Vector] = _
  @transient var resWithStd: Array[Vector] = _
  @transient var resWithMean: Array[Vector] = _
  @transient var resWithBoth: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(
      Vectors.dense(-2.0, 2.3, 0.0),
      Vectors.dense(0.0, -5.1, 1.0),
      Vectors.dense(1.7, -0.6, 3.3)
    )
    resWithMean = Array(
      Vectors.dense(-1.9, 3.433333333333, -1.433333333333),
      Vectors.dense(0.1, -3.966666666667, -0.433333333333),
      Vectors.dense(1.8, 0.533333333333, 1.866666666667)
    )
    resWithStd = Array(
      Vectors.dense(-1.079898494312, 0.616834091415, 0.0),
      Vectors.dense(0.0, -1.367762550529, 0.590968109266),
      Vectors.dense(0.917913720165, -0.160913241239, 1.950194760579)
    )
    resWithBoth = Array(
      Vectors.dense(-1.0259035695965, 0.920781324866, -0.8470542899497),
      Vectors.dense(0.0539949247156, -1.063815317078, -0.256086180682),
      Vectors.dense(0.9719086448809, 0.143033992212, 1.103140470631)
    )
  }

  private def assertResult: Row => Unit = {
    case Row(vector1: Vector, vector2: Vector) =>
      assert(vector1 ~== vector2 absTol 1E-5,
        "The vector value is not correct after standardization.")
  }

  test("params") {
    ParamsSuite.checkParams(new StandardScaler)
    ParamsSuite.checkParams(new StandardScalerModel("empty",
      Vectors.dense(1.0), Vectors.dense(2.0)))
  }

  test("Standardization with default parameter") {
    val df0 = data.zip(resWithStd).toSeq.toDF("features", "expected")

    val standardScalerEst0 = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standardized_features")
    val standardScaler0 = standardScalerEst0.fit(df0)
    MLTestingUtils.checkCopyAndUids(standardScalerEst0, standardScaler0)

    testTransformer[(Vector, Vector)](df0, standardScaler0, "standardized_features", "expected")(
      assertResult)
  }

  test("Standardization with setter") {
    val df1 = data.zip(resWithBoth).toSeq.toDF("features", "expected")
    val df2 = data.zip(resWithMean).toSeq.toDF("features", "expected")
    val df3 = data.zip(data).toSeq.toDF("features", "expected")

    val standardScaler1 = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standardized_features")
      .setWithMean(true)
      .setWithStd(true)
      .fit(df1)

    val standardScaler2 = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standardized_features")
      .setWithMean(true)
      .setWithStd(false)
      .fit(df2)

    val standardScaler3 = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standardized_features")
      .setWithMean(false)
      .setWithStd(false)
      .fit(df3)

    testTransformer[(Vector, Vector)](df1, standardScaler1, "standardized_features", "expected")(
      assertResult)
    testTransformer[(Vector, Vector)](df2, standardScaler2, "standardized_features", "expected")(
      assertResult)
    testTransformer[(Vector, Vector)](df3, standardScaler3, "standardized_features", "expected")(
      assertResult)
  }

  test("sparse data and withMean") {
    val someSparseData = Array(
      Vectors.sparse(3, Array(0, 1), Array(-2.0, 2.3)),
      Vectors.sparse(3, Array(1, 2), Array(-5.1, 1.0)),
      Vectors.dense(1.7, -0.6, 3.3)
    )
    val df = someSparseData.zip(resWithMean).toSeq.toDF("features", "expected")
    val standardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standardized_features")
      .setWithMean(true)
      .setWithStd(false)
      .fit(df)
    testTransformer[(Vector, Vector)](df, standardScaler, "standardized_features", "expected")(
      assertResult)
  }

  test("StandardScaler read/write") {
    val t = new StandardScaler()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setWithStd(false)
      .setWithMean(true)
    testDefaultReadWrite(t)
  }

  test("StandardScalerModel read/write") {
    val instance = new StandardScalerModel("myStandardScalerModel",
      Vectors.dense(1.0, 2.0), Vectors.dense(3.0, 4.0))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.std === instance.std)
    assert(newInstance.mean === instance.mean)
  }

}
