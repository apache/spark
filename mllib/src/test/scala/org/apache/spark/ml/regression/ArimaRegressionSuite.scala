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

/**
 * Unit tests for ArimaRegression and ArimaRegressionModel.
 */
class ArimaRegressionSuite extends SparkFunSuite
    with org.apache.spark.sql.test.SharedSparkSession {

  import testImplicits._

  test("ArimaRegression fits deterministic AR(1) series") {
    val phiTrue = 0.6
    val interceptTrue = 0.4
    val mean = interceptTrue / (1 - phiTrue)

    val n = 60
    val values = new Array[Double](n)
    values(0) = mean + 0.5
    var i = 1
    while (i < n) {
      val noise = math.sin(i.toDouble) * 0.05
      values(i) = interceptTrue + phiTrue * values(i - 1) + noise
      i += 1
    }

    val data = values.toSeq.toDF("y")

    val model = new ArimaRegression()
      .setP(1)
      .setD(0)
      .setQ(0)
      .fit(data)

    assert(model.phi.length == 1)
    assert(math.abs(model.phi.head - phiTrue) < 0.05)
    assert(math.abs(model.intercept - interceptTrue) < 0.05)

    val transformed = model.transform(data)
    val preds = transformed.select("prediction").as[Double].collect()
    val actual = data.select("y").as[Double].collect()

    // Skip the first few observations to avoid warm-up effects.
    var idx = 5
    while (idx < actual.length) {
      assert(math.abs(preds(idx) - actual(idx)) < 0.5)
      idx += 1
    }
  }

  test("ArimaRegressionModel reconstructs levels when d > 0 and p = 0") {
    val values = (1 to 8).map(_.toDouble)
    val data = values.toDF("y")

    val model = new ArimaRegression()
      .setP(0)
      .setD(1)
      .setQ(0)
      .fit(data)

    assert(model.phi.isEmpty)

    val preds = model.transform(data).select("prediction").as[Double].collect()
    preds.zip(values).foreach { case (pred, actual) =>
      assert(math.abs(pred - actual) < 1e-6)
    }
  }

  test("ArimaRegression enforces q = 0 for now") {
    val data = Seq(1.0, 2.0, 3.1, 4.2, 5.4).toDF("y")
    val arima = new ArimaRegression().setP(1).setD(0).setQ(1)

    val err = intercept[UnsupportedOperationException] {
      arima.fit(data)
    }
    assert(err.getMessage.contains("q = 0"))
  }

  test("difference helper supports higher-order differencing") {
    val base = Array(1.0, 3.0, 6.0, 10.0)
    val result = ArimaRegression.difference(base, 2)
    assert(result.sameElements(Array(1.0, 1.0)))
  }

  test("ArimaRegression requires y column of DoubleType") {
    val data = Seq((1, 2.0)).toDF("t", "value")
    val arima = new ArimaRegression()

    intercept[IllegalArgumentException] {
      arima.fit(data)
    }

    val schema = org.apache.spark.sql.types.StructType.fromDDL("y DOUBLE")
    val outputSchema = arima.transformSchema(schema)
    assert(outputSchema.fieldNames.contains("prediction"))
  }
}
