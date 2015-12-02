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

package org.apache.spark.ml.tuning.bandit

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class BanditValidatorSuite
  extends SparkFunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sqlContext = new SQLContext(sc)
    dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2))
  }

  test("bandit validation with logistic regression") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 10))
      .build()
    val eval = new BinaryClassificationEvaluator
    val bv = new BanditValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3)
    val bvModel = bv.fit(dataset)

    // copied model must have the same paren.
    // Stop copy model first because the limitation of Controllable.
    // MLTestingUtils.checkCopy(bvModel)

    val parent = bvModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
  }
}
