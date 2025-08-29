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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper

class ThetaSketchUtilsSuite extends SparkFunSuite with SQLHelper {

  test("checkLgNomLongs: accepts values within valid range") {
    val validValues =
      Seq(ThetaSketchUtils.MIN_LG_NOM_LONGS, 10, 20, ThetaSketchUtils.MAX_LG_NOM_LONGS)
    validValues.foreach { value =>
      // There should be no error here.
      ThetaSketchUtils.checkLgNomLongs(value)
    }
  }


  test("checkLgNomLongs: throws exception for values below minimum") {
    val invalidValues = Seq(ThetaSketchUtils.MIN_LG_NOM_LONGS - 1, 0, -5)
    invalidValues.foreach { value =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          ThetaSketchUtils.checkLgNomLongs(value)
        },
        condition = "THETA_INVALID_LG_NOM_ENTRIES",
        parameters = Map(
          "min" -> ThetaSketchUtils.MIN_LG_NOM_LONGS.toString,
          "max" -> ThetaSketchUtils.MAX_LG_NOM_LONGS.toString,
          "value" -> value.toString
        )
      )
    }
  }

  test("checkLgNomLongs: throws exception for values above maximum") {
    val invalidValues = Seq(ThetaSketchUtils.MAX_LG_NOM_LONGS + 1, 30, 100)
    invalidValues.foreach { value =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          ThetaSketchUtils.checkLgNomLongs(value)
        },
        condition = "THETA_INVALID_LG_NOM_ENTRIES",
        parameters = Map(
          "min" -> ThetaSketchUtils.MIN_LG_NOM_LONGS.toString,
          "max" -> ThetaSketchUtils.MAX_LG_NOM_LONGS.toString,
          "value" -> value.toString
        )
      )
    }
  }
}
