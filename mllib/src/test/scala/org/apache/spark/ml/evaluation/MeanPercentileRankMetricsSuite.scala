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
import org.apache.spark.mllib.util.MLlibTestSparkContext

class MeanPercentileRankMetricsSuite extends SparkFunSuite with MLlibTestSparkContext{

  import testImplicits._

  test("Mean Percentile Rank metrics : Long type") {
    val predictionAndLabels = Seq(
        (111216304L, Array(111216304L)),
        (108848657L, Array.empty[Long])
      ).toDF(Seq("label", "prediction"): _*)
    val mpr = new MeanPercentileRankMetrics(predictionAndLabels, "prediction", "label")
    assert(mpr.meanPercentileRank === 0.5)
  }

  test("Mean Percentile Rank metrics : String type") {
    val predictionAndLabels = Seq(
        ("item1", Array("item2", "item1")),
        ("item2", Array("item1")),
        ("item3", Array("item3", "item1", "item2"))
      ).toDF(Seq("label", "prediction"): _*)
    val mpr = new MeanPercentileRankMetrics(predictionAndLabels, "prediction", "label")
    assert(mpr.meanPercentileRank === 0.5)
  }

}
