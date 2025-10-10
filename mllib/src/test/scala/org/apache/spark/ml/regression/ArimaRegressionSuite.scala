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
package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Unit tests for ArimaRegression and ArimaRegressionModel.
 */
class ArimaRegressionSuite extends SparkFunSuite with org.apache.spark.sql.test.SharedSparkSession {

  import testImplicits._

  test("ARIMA model basic fit and transform") {
    val spark = sparkSession
    import spark.implicits._

    val data = Seq(
      (1, 100.0),
      (2, 102.0),
      (3, 101.0),
      (4, 103.0),
      (5, 104.0)
    ).toDF("t", "y")

    val arima = new ArimaRegression()
      .setP(1)
      .setD(1)
      .setQ(1)

    val model = arima.fit(data)
    val transformed = model.transform(data)

    assert(transformed.columns.contains("prediction"), "Output should include 'prediction' column.")
    assert(transformed.count() == data.count(), "Output row count should match input.")
  }

  test("ARIMA model schema validation and parameter setting") {
    val arima = new ArimaRegression()
      .setP(2)
      .setD(1)
      .setQ(1)

    assert(arima.getP == 2)
    assert(arima.getD == 1)
    assert(arima.getQ == 1)

    val schema = org.apache.spark.sql.types.StructType.fromDDL("y DOUBLE")
    val outputSchema = arima.transformSchema(schema)
    assert(outputSchema.fieldNames.contains("prediction"))
  }

  test("ARIMA model copy and persistence") {
    val spark = sparkSession
    import spark.implicits._

    val data = Seq(
      (1, 10.0),
      (2, 12.0),
      (3, 11.0)
    ).toDF("t", "y")

    val arima = new ArimaRegression().setP(1).setD(1).setQ(1)
    val model = arima.fit(data)

    val copied = model.copy(org.apache.spark.ml.param.ParamMap.empty)
    assert(copied.getP == model.getP)
    assert(copied.getD == model.getD)
    assert(copied.getQ == model.getQ)
  }

  test("ARIMA model handles missing y column gracefully") {
    val spark = sparkSession
    import spark.implicits._
    val invalidDF = Seq((1, 2.0)).toDF("t", "value")
    val arima = new ArimaRegression()

    intercept[IllegalArgumentException] {
      arima.fit(invalidDF)
    }
  }
}
