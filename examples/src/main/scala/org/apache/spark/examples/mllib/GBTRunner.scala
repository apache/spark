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

import scopt.OptionParser

import org.apache.spark.mllib.classification.GBTClassifier
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.util.Utils


/**
 * An example runner for Gradient Boosting using decision trees as weak learners. Run with
 * {{{
 * ./bin/run-example mllib.GBTRunner [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 *
 * Note: This script treats all features as real-valued (not categorical).
 *       To include categorical features, modify categoricalFeaturesInfo.
 */
object GBTRunner {

  private case class Params(
      input: String = null,
      testInput: String = "",
      dataFormat: String = "libsvm",
      algo: String = "Classification",
      maxDepth: Int = 5,
      numIterations: Int = 10,
      fracTest: Double = 0.2) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("GBTRunner") {
      head("GBTRunner: an example Gradient Boosted Trees app.")
      opt[String]("algo")
        .text(s"algorithm (Classification, Regression), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = x))
      opt[Int]("maxDepth")
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("numIterations")
        .text(s"number of iterations of boosting," + s" default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing.  If given option testInput, " +
          s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[String]("testInput")
        .text(s"input path to test dataset.  If given, option fracTest is ignored." +
          s" default: ${defaultParams.testInput}")
        .action((x, c) => c.copy(testInput = x))
      opt[String]("<dataFormat>")
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
        .action((x, c) => c.copy(dataFormat = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName(s"GBTRunner with $params")
    val sc = new SparkContext(conf)
    println(s"GBTRunner with parameters:\n$params")

    // Load training and test data and cache it.
    val (training, test, numClasses) = DecisionTreeRunner.loadDatasets(sc, params.input,
      params.dataFormat, params.testInput, params.algo, params.fracTest)

    params.algo.toLowerCase match {
      case "classification" =>
        runClassification(params, training, test, numClasses)
      case "regression" =>
        runRegression(params, training, test)
      case _ => throw new IllegalArgumentException(s"Algorithm ${params.algo} not supported.")
    }

    sc.stop()
  }

  private def runClassification(
      params: Params,
      training: RDD[LabeledPoint],
      test: RDD[LabeledPoint],
      numClasses: Int): Unit = {
    val randomSeed = Utils.random.nextInt()
    val gbt = new GBTClassifier()
      .setNumIterations(params.numIterations)
      .setMaxDepth(params.maxDepth)
      .setSeed(randomSeed)
    val startTime = System.nanoTime()
    val model = gbt.run(training, Map.empty[Int, Int], numClasses)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")
    if (model.totalNumNodes < 30) {
      println(model.toDebugString) // Print full model.
    } else {
      println(model) // Print model summary.
    }
    val trainAccuracy =
      new MulticlassMetrics(training.map(lp => (model.predict(lp.features), lp.label))).precision
    println(s"Train accuracy = $trainAccuracy")
    val testAccuracy =
      new MulticlassMetrics(test.map(lp => (model.predict(lp.features), lp.label))).precision
    println(s"Test accuracy = $testAccuracy")
  }

  private def runRegression(
      params: Params,
      training: RDD[LabeledPoint],
      test: RDD[LabeledPoint]): Unit = {
    val randomSeed = Utils.random.nextInt()
    val gbt = new GBTClassifier()
      .setNumIterations(params.numIterations)
      .setMaxDepth(params.maxDepth)
      .setSeed(randomSeed)
    val startTime = System.nanoTime()
    val model = gbt.run(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")
    if (model.totalNumNodes < 30) {
      println(model.toDebugString) // Print full model.
    } else {
      println(model) // Print model summary.
    }
    val trainMSE = DecisionTreeRunner.meanSquaredError(model, training)
    println(s"Train mean squared error = $trainMSE")
    val testMSE = DecisionTreeRunner.meanSquaredError(model, test)
    println(s"Test mean squared error = $testMSE")
  }
}
