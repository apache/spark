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

package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.bandit.{BanditValidator, StaticSearch}
// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BanditValidatorExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BanditValidatorExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val splits = data.randomSplit(Array(0.7, 0.3))
    val training = splits(0).cache()
    val test = splits(1).cache()

    // Define the base estimator
    val lr = new LogisticRegression()

    // Define the bandit validator
    val banditval = new BanditValidator()
      .setEstimator(lr)
      .setEvaluator(new BinaryClassificationEvaluator)

    // Build the parameters grid
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.elasticNetParam, Array(1.0, 0.1, 0.01))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()
    banditval.setEstimatorParamMaps(paramGrid)
    banditval.setNumFolds(3)

    // Select a search strategy
    val search = new StaticSearch
    banditval.setSearchStrategy(search)
    banditval.setMaxIter(50)
    banditval.setStepsPerPulling(1)

    val bvModel = banditval.fit(training)
    bvModel.transform(test.toDF()).show()
    // $example off$

    sc.stop()
  }
}
