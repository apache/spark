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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.rdd.DatasetInfo
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTreeClassifier, DecisionTreeRegressor}
import org.apache.spark.mllib.tree.configuration.{DTClassifierParams, DTRegressorParams}
import org.apache.spark.mllib.tree.impurity.{ClassificationImpurities, RegressionImpurities}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * An example runner for decision tree. Run with
 * {{{
 * ./bin/spark-example org.apache.spark.examples.mllib.DecisionTreeRunner [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 *
 * Note: This script treats all features as real-valued (not categorical).
 *       To include categorical features, modify
 *       [[org.apache.spark.mllib.rdd.DatasetInfo.categoricalFeaturesInfo]].
 */
object DecisionTreeRunner {

  case class Params(
      input: String = null,
      dataFormat: String = null,
      algo: String = "classification",
      impurity: Option[String] = None,
      maxDepth: Int = 4,
      maxBins: Int = 100,
      fracTest: Double = 0.2)

  def main(args: Array[String]) {
    val defaultParams = Params()
    val defaultCImpurity = ClassificationImpurities.impurityName(new DTClassifierParams().impurity)
    val defaultRImpurity = RegressionImpurities.impurityName(new DTRegressorParams().impurity)

    val parser = new OptionParser[Params]("DecisionTreeRunner") {
      head("DecisionTreeRunner: an example decision tree app.")
      opt[String]("algo")
        .text(s"algorithm (classification, regression), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = x))
      opt[String]("impurity")
        .text(
          s"impurity type\n" +
          s"\tFor classification: ${ClassificationImpurities.names.mkString(",")}\n" +
          s"\t  default: $defaultCImpurity" +
          s"\tFor regression: ${RegressionImpurities.names.mkString(",")}\n" +
          s"\t  default: $defaultRImpurity")
        .action((x, c) => c.copy(impurity = Some(x)))
      opt[Int]("maxDepth")
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing, default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      arg[String]("<input>")
        .text("input paths to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      arg[String]("<dataFormat>")
        .text("data format: dense/libsvm")
        .required()
        .action((x, c) => c.copy(dataFormat = x))
      checkConfig { params =>
        if (!List("classification", "regression").contains(params.algo)) {
          failure(s"Did not recognize Algo: ${params.algo}")
        }
        if (params.impurity != None) {
          if ((params.algo == "classification" &&
                !ClassificationImpurities.names.contains(params.impurity)) ||
              (params.algo == "regression" &&
                !RegressionImpurities.names.contains(params.impurity))) {
            failure(s"Algo ${params.algo} is not compatible with impurity ${params.impurity}.")
          }
        }
        if (params.fracTest < 0 || params.fracTest > 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1].")
        }
        success
      }
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName("DecisionTreeRunner")
    val sc = new SparkContext(conf)

    // Load training data and cache it.
    val origExamples = params.dataFormat match {
      case "dense" => MLUtils.loadLabeledPoints(sc, params.input).cache()
      case "libsvm" => MLUtils.loadLibSVMFile(sc, params.input, multiclass = true).cache()
    }
    // For classification, re-index classes if needed.
    val (examples, numClasses) = params.algo match {
      case "classification" => {
        // classCounts: class --> # examples in class
        val classCounts = origExamples.map(_.label).countByValue
        val numClasses = classCounts.size
        // classIndex: class --> index in 0,...,numClasses-1
        val classIndex = {
          if (classCounts.keySet != Set[Double](0.0, 1.0)) {
            classCounts.keys.toList.sorted.zipWithIndex.toMap
          } else {
            Map[Double, Int]()
          }
        }
        val examples = {
          if (classIndex.isEmpty) {
            origExamples
          } else {
            origExamples.map(lp => LabeledPoint(classIndex(lp.label), lp.features))
          }
        }
        println(s"numClasses = $numClasses.")
        println(s"Per-class example fractions, counts:")
        println(s"Class\tFrac\tCount")
        classCounts.keys.toList.sorted.foreach(c => {
          val frac = classCounts(c) / (0.0 + examples.count())
          println(s"$c\t$frac\t${classCounts(c)}")
        })
        (examples, numClasses)
      }
      case "regression" => {
        (origExamples, 0)
      }
      case _ => {
        throw new IllegalArgumentException("Algo ${params.algo} not supported.")
      }
    }

    // Split into training, test.
    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()
    val numTraining = training.count()
    val numTest = test.count()

    println(s"numTraining = $numTraining, numTest = $numTest.")

    examples.unpersist(blocking = false)

    val numFeatures = examples.take(1)(0).features.size
    val datasetInfo = new DatasetInfo(numClasses, numFeatures)

    params.algo match {
      case "classification" => {
        val dtParams = DecisionTreeClassifier.defaultParams()
        dtParams.maxDepth = params.maxDepth
        dtParams.maxBins = params.maxBins
        if (params.impurity != None) {
          dtParams.impurity = ClassificationImpurities.impurity(params.impurity.get)
        }
        val dtLearner = new DecisionTreeClassifier(dtParams)
        val model = dtLearner.train(training, datasetInfo)
        model.print()
        val accuracy = accuracyScore(model, test)
        println(s"Test accuracy = $accuracy.")
      }
      case "regression" => {
        val dtParams = DecisionTreeRegressor.defaultParams()
        dtParams.maxDepth = params.maxDepth
        dtParams.maxBins = params.maxBins
        if (params.impurity != None) {
          dtParams.impurity = RegressionImpurities.impurity(params.impurity.get)
        }
        val dtLearner = new DecisionTreeRegressor(dtParams)
        val model = dtLearner.train(training, datasetInfo)
        model.print()
        val mse = meanSquaredError(model, test)
        println(s"Test mean squared error = $mse.")
      }
      case _ => {
        throw new IllegalArgumentException("Algo ${params.algo} not supported.")
      }
    }

    sc.stop()
  }

  /**
   * Calculates the classifier accuracy.
   */
  private def accuracyScore(
      model: DecisionTreeModel,
      data: RDD[LabeledPoint]): Double = {
    val correctCount = data.filter(y => model.predict(y.features) == y.label).count()
    val count = data.count()
    correctCount.toDouble / count
  }

  /**
   * Calculates the mean squared error for regression.
   */
  private def meanSquaredError(
      model: DecisionTreeModel,
      data: RDD[LabeledPoint]): Double = {
    data.map { y =>
      val err = model.predict(y.features) - y.label
      err * err
    }.mean()
  }
}
