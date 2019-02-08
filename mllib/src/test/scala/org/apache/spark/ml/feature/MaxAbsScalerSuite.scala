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
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.sql.Row

class MaxAbsScalerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("MaxAbsScaler fit basic case") {
    val data = Array(
      Vectors.dense(1, 0, 100),
      Vectors.dense(2, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(-2, -100)),
      Vectors.sparse(3, Array(0), Array(-1.5)))

    val expected: Array[Vector] = Array(
      Vectors.dense(0.5, 0, 1),
      Vectors.dense(1, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(-1, -1)),
      Vectors.sparse(3, Array(0), Array(-0.75)))

    val df = data.zip(expected).toSeq.toDF("features", "expected")
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaled")

    val model = scaler.fit(df)
    testTransformer[(Vector, Vector)](df, model, "expected", "scaled") {
      case Row(expectedVec: Vector, actualVec: Vector) =>
        assert(expectedVec === actualVec,
          s"MaxAbsScaler error: Expected $expectedVec but computed $actualVec")
    }

    MLTestingUtils.checkCopyAndUids(scaler, model)
  }

  test("MaxAbsScaler read/write") {
    val t = new MaxAbsScaler()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }

  test("MaxAbsScalerModel read/write") {
    val instance = new MaxAbsScalerModel(
      "myMaxAbsScalerModel", Vectors.dense(1.0, 10.0))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.maxAbs === instance.maxAbs)
  }

}
