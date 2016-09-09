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

package org.apache.spark.ml.r

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.{RFormula, RFormulaModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RWrapperUtilsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("avoid libsvm data column name conflicting") {
    val rFormula = new RFormula().setFormula("label ~ features")
    val data = spark.read.format("libsvm").load("../data/mllib/sample_libsvm_data.txt")

    // if not checking column name, then IllegalArgumentException
    intercept[IllegalArgumentException] {
      rFormula.fit(data)
    }

    // after checking, model build is ok
    RWrapperUtils.checkDataColumns(rFormula, data)

    assert(rFormula.getLabelCol == "label_output")
    assert(rFormula.getFeaturesCol == "features_output")

    val model = rFormula.fit(data)
    assert(model.isInstanceOf[RFormulaModel])

    assert(model.getLabelCol == "label_output")
    assert(model.getFeaturesCol == "features_output")
  }

  test("generate unique name by appending a sequence number") {
    val originalName = "label"
    val fieldNames = Array("label_output", "label_output1", "label_output2")
    val newName = RWrapperUtils.convertToUniqueName(originalName, fieldNames)

    assert(newName === "label_output3")
  }

}
