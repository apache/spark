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

class WeightOfEvidenceEstimatorSuite extends MLTest {

  test("WeightOfEvidenceEstimatorSuite") {

    val df = spark.createDataFrame(Seq(
      (2.3, 12.3, 1.3, 3.3, 1.0),
      (2.3, 14.6, 2.3, 1.3, 1.0),
      (6.3, 14.6, 3.3, 2.3, 0.0),
      (3.3, 14.6, 4.3, 3.3, 1.0),
      (2.3, 3.3, 3.3, 4.4, 1.0),
      (2.3, 1.0, 3.3, 4.4, 0.0),
      (2.3, 12.3, 2.3, 4.4, 0.0)
    )).toDF("col1", "col2", "col3", "col4", "label")

    val inputColumns = Array("col2", "col3")
    val outColumns = Array("out2", "out3")

    val woe = new WeightOfEvidenceEstimator()
      .setInputCols(inputColumns)
      .setOutputCols(outColumns)

    val model = woe.fit(df)

    val frame = model.transform(df)

    frame.show()
  }
}
