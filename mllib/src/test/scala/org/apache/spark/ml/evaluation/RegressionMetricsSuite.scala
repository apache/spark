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

package org.apache.spark.ml.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RegressionMetricsSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("RegressionMetrics get same result as MLlib") {
    val spark = this.spark
    import spark.implicits._

    val predictionAndLabels = sc.parallelize(
      Array(77.0, 85, 62, 55, 63, 88, 57, 81, 51).zip(
      Array(72.08, 91.88, 65.48, 52.28, 62.18, 81.98, 58.88, 78.68, 55.58))
    )

    val mllibMetrics = new org.apache.spark.mllib.evaluation.RegressionMetrics(
      predictionAndLabels)

    val df = predictionAndLabels.toDF("prediction", "label")
    val metrics = new RegressionMetrics(df)
    assert(metrics.rootMeanSquaredError == mllibMetrics.rootMeanSquaredError)
    assert(metrics.meanSquaredError == mllibMetrics.meanSquaredError)
    assert(metrics.r2 == mllibMetrics.r2)
    assert(metrics.meanAbsoluteError == mllibMetrics.meanAbsoluteError)
  }

}
