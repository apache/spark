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

import org.apache.spark.ml.util.MLTest

class TargetEncoderSuite extends MLTest {

  test("TargetEncoderSuite") {

    val df = spark.createDataFrame(
      Seq(
        (12.0, 1.0, 1.0),
        (12.0, 0.0, 1.0),
        (14.0, 1.0, 0.0),
        (14.0, 2.0, 0.0),
        (1.0, 1.0, 1.0),
        (0.0, 3.0, 1.0))
    ).toDF("col1", "col2", "label")


    val inputColumns = Array("col1", "col2")
    val outColumns = Array("out1", "out2")

    val encoder = new TargetEncoder()
      .setInputCols(inputColumns)
      .setOutputCols(outColumns)
//      .save("D:\\tmp\\ml_save\\TargetEncoder_t2")

//    TargetEncoder.read.load("D:\\tmp\\ml_save")

    val targetEncoderModel = encoder.fit(df)

    targetEncoderModel.transform(df).show()

  }
}
