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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext

class ElementwiseProductSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("ElementwiseProduct should throw adequate exception on input type mismatch") {
    val data = Seq(Tuple1("string value"))

    val df = sqlContext.createDataFrame(data).toDF("features")

    val elementwiseProduct = new ElementwiseProduct()
      .setInputCol("features")
      .setOutputCol("scaled_features")

    val thrown = intercept[IllegalArgumentException] {
      elementwiseProduct.transform(df).collect()
    }
    assert(thrown.getClass === classOf[IllegalArgumentException])
    assert(
      thrown.getMessage == "requirement failed: Input type must be VectorUDT but got StringType.")
  }

  test("read/write") {
    val ep = new ElementwiseProduct()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setScalingVec(Vectors.dense(0.1, 0.2))
    testDefaultReadWrite(ep)
  }
}
