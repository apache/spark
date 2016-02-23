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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
// $example on$
import org.apache.spark.mllib.optimization.L1Updater
// $example off$
import org.apache.spark.mllib.util.MLUtils

object L1UpdaterExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("L1UpdaterExample")
    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    // $example on$
    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.
      setNumIterations(200).
      setRegParam(0.1).
      setUpdater(new L1Updater)
    val modelL1 = svmAlg.run(training)
    // $example off$

    // Clear the default threshold.
    modelL1.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = modelL1.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    modelL1.save(sc, "target/tmp/myL1UpdaterModel")
    val sameModel = SVMModel.load(sc, "target/tmp/myL1UpdaterModel")

    sc.stop()
  }
}
// scalastyle:on println
