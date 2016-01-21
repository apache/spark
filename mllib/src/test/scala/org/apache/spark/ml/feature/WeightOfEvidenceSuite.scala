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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class WoETransformerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new WoETransformer())
  }

  test("Weight of Evidence with String feature and Boolean label") {
    val df = sqlContext.createDataFrame(Seq(
      (true, "ab", 0.2827),
      (false, "ab", 0.2827),
      (true, "ab", 0.2827),
      (false, "cd", -0.4055),
      (true, "cd", -0.4055)
    )).toDF("label", "feature", "expected")

    val woe = new WoETransformer().setInputCol("feature").setOutputCol("woe")
    val model = woe.fit(df)
    assert (model.transform(df).collect().forall(r =>
      r.getAs[Double]("woe") ~== r.getAs[Double]("expected") absTol 1E-3
    ))
    assert(WoETransformer.getInformationValue(df, "feature", "label") ~== 0.1147 absTol 1E-3)
  }

  test("Weight of Evidence with Int feature and Int label") {
    val df = sqlContext.createDataFrame(Seq(
      (1, 100, 0.2827),
      (0, 100, 0.2827),
      (1, 100, 0.2827),
      (0, 200, -0.4055),
      (1, 200, -0.4055)
    )).toDF("label", "feature", "expected")

    val woe = new WoETransformer().setInputCol("feature").setOutputCol("woe")
    val model = woe.fit(df)
    assert (model.transform(df).collect().forall(r =>
      r.getAs[Double]("woe") ~== r.getAs[Double]("expected") absTol 1E-3
    ))
    assert(WoETransformer.getInformationValue(df, "feature", "label") ~== 0.1147 absTol 1E-3)
  }

  test("Weight of Evidence with Double feature and Double label") {
    val df = sqlContext.createDataFrame(Seq(
      (1.0, 100.0, 0.2827),
      (0.0, 100.0, 0.2827),
      (1.0, 100.0, 0.2827),
      (0.0, 200.0, -0.4055),
      (1.0, 200.0, -0.4055)
    )).toDF("label", "feature", "expected")

    val woe = new WoETransformer().setInputCol("feature").setOutputCol("woe")
    val model = woe.fit(df)
    assert (model.transform(df).collect().forall(r =>
      r.getAs[Double]("woe") ~== r.getAs[Double]("expected") absTol 1E-3
    ))
    assert(WoETransformer.getInformationValue(df, "feature", "label") ~== 0.1147 absTol 1E-3)
  }

  test("Weight of Evidence with 0 postive or 0 negative") {
    val df = sqlContext.createDataFrame(Seq(
      (true, "ab", 0.4005),
      (false, "ab", 0.4005),
      (true, "ab", 0.4005),
      (true, "cd", 5.0156),
      (true, "cd", 5.0156),
      (false, "ef", -5.5910),
      (false, "ef", -5.5910)
    )).toDF("label", "feature", "expected")

    val woe = new WoETransformer().setInputCol("feature").setOutputCol("woe")
    val model = woe.fit(df)
    assert (model.transform(df).collect().forall(r =>
      r.getAs[Double]("woe") ~== r.getAs[Double]("expected") absTol 1E-3
    ))
    assert(WoETransformer.getInformationValue(df, "feature", "label") ~== 6.3018 absTol 1E-3)
  }
}
