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

class MulticlassClassificationMetricsSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("MulticlassClassificationMetricsSuite get same result as MLlib") {
    val spark = this.spark
    import spark.implicits._

    val predictionAndLabels = sc.parallelize(
      Seq((0.0, 0.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0),
        (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)), 2)

    val mllibMetrics = new org.apache.spark.mllib.evaluation.MulticlassMetrics(
      predictionAndLabels)

    val df = predictionAndLabels.toDF("prediction", "label")
    val metrics = new MultiClassClassificationMetrics(df)
    assert(metrics.accuracy == mllibMetrics.accuracy)
    assert(metrics.weightedFMeasure == mllibMetrics.weightedFMeasure)
    assert(metrics.weightedPrecision == mllibMetrics.weightedPrecision)
    assert(metrics.weightedRecall == mllibMetrics.weightedRecall)
  }

}

