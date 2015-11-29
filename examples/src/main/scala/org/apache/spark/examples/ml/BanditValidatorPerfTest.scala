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

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.bandit._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BanditValidatorPerfTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("Cooking")

    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._

    val data = MLUtils.loadLibSVMFile(sc, "/Users/panda/data/small_datasets/a1a").map {
      case LabeledPoint(label: Double, features: Vector) =>
        LabeledPoint(if (label < 0) 0 else label, features)
    }

    val dataset = data.toDF()

    val splits = dataset.randomSplit(Array(0.7, 0.3))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val singleClassifier = new LogisticRegression().setMaxIter(3)

    val params = new ParamGridBuilder()
      .addGrid(singleClassifier.elasticNetParam, Array(1.0, 0.1, 0.01))
      .addGrid(singleClassifier.regParam, Array(0.1, 0.01))
      .addGrid(singleClassifier.fitIntercept, Array(true, false))
      .build()

    val eval = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val banditVal = new BanditValidator[LogisticRegressionModel]()
      .setEstimator(singleClassifier)
      .setEstimatorParamMaps(params)
      .setNumFolds(3)
      .setEvaluator(eval)

    val result = Array(
      new StaticSearch,
      new SimpleBanditSearch,
      new SuccessiveEliminationSearch,
      new ExponentialWeightsSearch,
      new LILUCBSearch,
      new LUCBSearch,
      new SuccessiveHalvingSearch,
      new SuccessiveRejectSearch
    ).map { search =>
      banditVal.setSearchStrategy(search)
      val model = banditVal.fit(training)
      val auc = eval.evaluate(model.transform(test))
      (search.getClass.getSimpleName, auc)
    }

    result.foreach { x =>
      println(s"${x._1} -> ${x._2}")
    }
    sc.stop()
  }
}
