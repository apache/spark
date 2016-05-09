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

package org.apache.spark.examples.mllib

import org.apache.spark.mllib.classification.PSLogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}


object PSLogisticRegressionExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"PSLogisticRegressionExample")
    val sc = new SparkContext(conf)

    val input = args(0)
    val numIterations = args(1).toInt
    val stepSize = args(2).toDouble
    val miniBatchFraction = args(3).toDouble
    val partition = args(4).toInt

    val (training, test) = if (args.length == 6) {
      val testFile = args(5)

      val training = MLUtils.loadLibSVMFile(sc, input, 123, partition).map { p =>
        val label = if (p.label == 1.0) 1.0 else 0.0
        LabeledPoint(label, p.features)
      }.cache()
      val test = MLUtils.loadLibSVMFile(sc, testFile, 123).map { p =>
        val label = if (p.label == 1.0) 1.0 else 0.0
        LabeledPoint(label, p.features)
      }.cache()

      val numTraining = training.count()
      val numTest = test.count()
      println(s"Training: $numTraining, test: $numTest.")

      (training, test)
    } else {
      val examples = MLUtils.loadLibSVMFile(sc, input, -1, partition).map { p =>
        val label = if (p.label == 1.0) 1.0 else 0.0
        LabeledPoint(label, p.features)
      }.cache()
      val splits = examples.randomSplit(Array(0.8, 0.2))
      val training = splits(0).cache()
      val test = splits(1).cache()

      val numTraining = training.count()
      val numTest = test.count()
      println(s"Training: $numTraining, test: $numTest.")

      examples.unpersist(blocking = false)

      (training, test)
    }

    val numFeatureTraining = training.take(1).head.features.size
    val numFeatureTest = test.take(1).head.features.size
    println(s"feature in training: $numFeatureTraining, feature in test: $numFeatureTest")

    val model = PSLogisticRegression.train(sc, training, numIterations, stepSize, miniBatchFraction)


    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

    sc.stop()
  }
}
