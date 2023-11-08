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
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.Row

class ElementwiseProductSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("streaming transform") {
    val scalingVec = Vectors.dense(0.1, 10.0)
    val data = Seq(
      (Vectors.dense(0.1, 1.0), Vectors.dense(0.01, 10.0)),
      (Vectors.dense(0.0, -1.1), Vectors.dense(0.0, -11.0))
    )
    val df = spark.createDataFrame(data).toDF("features", "expected")
    val ep = new ElementwiseProduct()
      .setInputCol("features")
      .setOutputCol("actual")
      .setScalingVec(scalingVec)
    testTransformer[(Vector, Vector)](df, ep, "actual", "expected") {
      case Row(actual: Vector, expected: Vector) =>
        assert(actual ~== expected relTol 1e-14)
    }
  }

  test("read/write") {
    val ep = new ElementwiseProduct()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setScalingVec(Vectors.dense(0.1, 0.2))
    testDefaultReadWrite(ep)
  }
}
