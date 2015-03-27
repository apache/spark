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

import scala.language.reflectiveCalls

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{RandomForestClassifier, DecisionTreeClassifier}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.impl.tree.PredictionModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{DecisionTreeRegressor, LabeledPoint,
  RandomForestRegressor}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
 * An example runner for decision trees and random forests. Run with
 * {{{
 * ./bin/run-example mllib.DecisionTreeRunner [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 *
 * Note: This script treats all features as real-valued (not categorical).
 *       To include categorical features, modify categoricalFeaturesInfo.
 */
object DecisionTreeRunner {

  private case class Params(
      input: String = null,
      testInput: String = "",
      dataFormat: String = "libsvm",
      algo: String = "Classification",
      maxDepth: Int = 5,
      impurity: String = "auto",
      maxBins: Int = 32,
      minInstancesPerNode: Int = 1,
      minInfoGain: Double = 0.0,
      numTrees: Int = 1,
      featuresPerNode: String = "auto",
      fracTest: Double = 0.2,
      cacheNodeIds: Boolean = false,
      checkpointDir: Option[String] = None,
      checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DecisionTreeRunner") {
      head("DecisionTreeRunner: an example decision tree app.")
      opt[String]("algo")
        .text(s"algorithm (Classification, Regression), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = x))
      opt[String]("impurity")
        .text(s"impurity type" +
        s" (for Classification: ${DecisionTreeClassifier.supportedImpurities.mkString(",")}; " +
        s" for Regression: ${DecisionTreeRegressor.supportedImpurities.mkString(",")}), " +
        s"default: ${defaultParams.impurity}")
        .action((x, c) => c.copy(impurity = x))
      opt[Int]("maxDepth")
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[Int]("minInstancesPerNode")
        .text(s"min number of instances required at child nodes to create the parent split," +
          s" default: ${defaultParams.minInstancesPerNode}")
        .action((x, c) => c.copy(minInstancesPerNode = x))
      opt[Double]("minInfoGain")
        .text(s"min info gain required to create a split, default: ${defaultParams.minInfoGain}")
        .action((x, c) => c.copy(minInfoGain = x))
      opt[Int]("numTrees")
        .text(s"number of trees (1 = decision tree, 2+ = random forest)," +
          s" default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(numTrees = x))
      opt[String]("featuresPerNode")
        .text(s"feature subset sampling strategy" +
          s" (${RandomForestClassifier.supportedFeaturesPerNode.mkString(", ")}}), " +
          s"default: ${defaultParams.featuresPerNode}")
        .action((x, c) => c.copy(featuresPerNode = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing.  If given option testInput, " +
          s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[Boolean]("cacheNodeIds")
        .text(s"whether to use node Id cache during training, " +
          s"default: ${defaultParams.cacheNodeIds}")
        .action((x, c) => c.copy(cacheNodeIds = x))
      opt[String]("checkpointDir")
        .text(s"checkpoint directory where intermediate node Id caches will be stored, " +
         s"default: ${defaultParams.checkpointDir match {
           case Some(strVal) => strVal
           case None => "None"
         }}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"how often to checkpoint the node Id cache, " +
         s"default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
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

  /**
   * Load training and test data from files.
   * @param input  Path to input dataset.
   * @param dataFormat  "libsvm" or "dense"
   * @param testInput  Path to test dataset.
   * @param algo  Classification or Regression
   * @param fracTest  Fraction of input data to hold out for testing.  Ignored if testInput given.
   * @return  (training dataset, test dataset, number of classes),
   *          where the number of classes is inferred from data (and set to 0 for Regression)
   */
  private[mllib] def loadDatasets(
      sc: SparkContext,
      input: String,
      dataFormat: String,
      testInput: String,
      algo: String,
      fracTest: Double): (RDD[LabeledPoint], RDD[LabeledPoint], Int) = {
    // Load training data and cache it.
    val origExamples = dataFormat match {
      case "dense" => MLUtils.loadLabeledPoints(sc, input).cache()
      case "libsvm" => MLUtils.loadLibSVMFile(sc, input).cache()
    }
    // For classification, re-index classes if needed.
    val (examples, classIndexMap, numClasses) = algo.toLowerCase match {
      case "classification" =>
        // classCounts: class --> # examples in class
        val classCounts = origExamples.map(_.label).countByValue()
        val sortedClasses = classCounts.keys.toList.sorted
        val numClasses = classCounts.size
        // classIndexMap: class --> index in 0,...,numClasses-1
        val classIndexMap = {
          if (classCounts.keySet != Set(0.0, 1.0)) {
            sortedClasses.zipWithIndex.toMap
          } else {
            Map[Double, Int]()
          }
        }
        val examples = {
          if (classIndexMap.isEmpty) {
            origExamples
          } else {
            origExamples.map(lp => LabeledPoint(classIndexMap(lp.label), lp.features))
          }
        }
        val numExamples = examples.count()
        println(s"numClasses = $numClasses.")
        println(s"Per-class example fractions, counts:")
        println(s"Class\tFrac\tCount")
        sortedClasses.foreach { c =>
          val frac = classCounts(c) / numExamples.toDouble
          println(s"$c\t$frac\t${classCounts(c)}")
        }
        (examples, classIndexMap, numClasses)
      case "regression" =>
        (origExamples, null, 0)
      case _ =>
        throw new IllegalArgumentException(s"Algorithm $algo not supported.")
    }

    // Create training, test sets.
    val splits = if (testInput != "") {
      // Load testInput.
      val numFeatures = examples.take(1)(0).features.size
      val origTestExamples = dataFormat match {
        case "dense" => MLUtils.loadLabeledPoints(sc, testInput)
        case "libsvm" => MLUtils.loadLibSVMFile(sc, testInput, numFeatures)
      }
      algo.toLowerCase match {
        case "classification" =>
          // classCounts: class --> # examples in class
          val testExamples = {
            if (classIndexMap.isEmpty) {
              origTestExamples
            } else {
              origTestExamples.map(lp => LabeledPoint(classIndexMap(lp.label), lp.features))
            }
          }
          Array(examples, testExamples)
        case "regression" =>
          Array(examples, origTestExamples)
        case _ =>
          throw new IllegalArgumentException(s"Algorithm $algo not supported.")
      }
    } else {
      // Split input into training, test.
      examples.randomSplit(Array(1.0 - fracTest, fracTest))
    }
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"numTraining = $numTraining, numTest = $numTest.")

    examples.unpersist(blocking = false)

    (training, test, numClasses)
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName(s"DecisionTreeRunner with $params")
    val sc = new SparkContext(conf)
    println(s"DecisionTreeRunner with parameters:\n$params")

    params.checkpointDir.foreach(sc.setCheckpointDir)

    // Load training and test data and cache it.
    val (training, test, numClasses) = loadDatasets(sc, params.input, params.dataFormat,
      params.testInput, params.algo, params.fracTest)

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
    val impurity = if (params.impurity == "auto") "gini" else params.impurity
    if (params.numTrees == 1) {
      val dt = new DecisionTreeClassifier()
        .setImpurity(impurity)
        .setMaxDepth(params.maxDepth)
        .setMaxBins(params.maxBins)
        .setMinInstancesPerNode(params.minInstancesPerNode)
        .setMinInfoGain(params.minInfoGain)
        .setCacheNodeIds(params.cacheNodeIds)
        .setCheckpointInterval(params.checkpointInterval)
      val startTime = System.nanoTime()
      val model = dt.run(training, Map.empty[Int, Int], numClasses)
      val elapsedTime = (System.nanoTime() - startTime) / 1e9
      println(s"Training time: $elapsedTime seconds")
      if (model.numNodes < 20) {
        println(model.toDebugString) // Print full model.
      } else {
        println(model) // Print model summary.
      }
      printAccuracy(model, training, test)
    } else {
      val randomSeed = Utils.random.nextInt()
      val rf = new RandomForestClassifier()
        .setImpurity(impurity)
        .setMaxDepth(params.maxDepth)
        .setMaxBins(params.maxBins)
        .setMinInstancesPerNode(params.minInstancesPerNode)
        .setMinInfoGain(params.minInfoGain)
        .setCacheNodeIds(params.cacheNodeIds)
        .setCheckpointInterval(params.checkpointInterval)
        .setNumTrees(params.numTrees)
        .setFeaturesPerNode(params.featuresPerNode)
        .setSeed(randomSeed)
      val startTime = System.nanoTime()
      val model = rf.run(training, Map.empty[Int, Int], numClasses)
      val elapsedTime = (System.nanoTime() - startTime) / 1e9
      println(s"Training time: $elapsedTime seconds")
      if (model.totalNumNodes < 30) {
        println(model.toDebugString) // Print full model.
      } else {
        println(model) // Print model summary.
      }
      printAccuracy(model, training, test)
    }
  }

  private def runRegression(
      params: Params,
      training: RDD[LabeledPoint],
      test: RDD[LabeledPoint]): Unit = {
    val impurity = if (params.impurity == "auto") "variance" else params.impurity
    if (params.numTrees == 1) {
      val dt = new DecisionTreeRegressor()
        .setImpurity(impurity)
        .setMaxDepth(params.maxDepth)
        .setMaxBins(params.maxBins)
        .setMinInstancesPerNode(params.minInstancesPerNode)
        .setMinInfoGain(params.minInfoGain)
        .setCacheNodeIds(params.cacheNodeIds)
        .setCheckpointInterval(params.checkpointInterval)
      val startTime = System.nanoTime()
      val model = dt.run(training)
      val elapsedTime = (System.nanoTime() - startTime) / 1e9
      println(s"Training time: $elapsedTime seconds")
      if (model.numNodes < 20) {
        println(model.toDebugString) // Print full model.
      } else {
        println(model) // Print model summary.
      }
      printMSE(model, training, test)
    } else {
      val randomSeed = Utils.random.nextInt()
      val rf = new RandomForestRegressor()
        .setImpurity(impurity)
        .setMaxDepth(params.maxDepth)
        .setMaxBins(params.maxBins)
        .setMinInstancesPerNode(params.minInstancesPerNode)
        .setMinInfoGain(params.minInfoGain)
        .setCacheNodeIds(params.cacheNodeIds)
        .setCheckpointInterval(params.checkpointInterval)
        .setNumTrees(params.numTrees)
        .setFeaturesPerNode(params.featuresPerNode)
        .setSeed(randomSeed)
      val startTime = System.nanoTime()
      val model = rf.run(training)
      val elapsedTime = (System.nanoTime() - startTime) / 1e9
      println(s"Training time: $elapsedTime seconds")
      if (model.totalNumNodes < 30) {
        println(model.toDebugString) // Print full model.
      } else {
        println(model) // Print model summary.
      }
      printMSE(model, training, test)
    }
  }

  /** Prints the accuracy for classification. */
  private[mllib] def printAccuracy(
      model: { def predict(features: Vector): Double },
      training: RDD[LabeledPoint],
      test: RDD[LabeledPoint]): Unit = {
    val trainAccuracy =
      new MulticlassMetrics(training.map(lp => (model.predict(lp.features), lp.label))).precision
    println(s"Train accuracy = $trainAccuracy")
    val testAccuracy =
      new MulticlassMetrics(test.map(lp => (model.predict(lp.features), lp.label))).precision
    println(s"Test accuracy = $testAccuracy")
  }

  /** Prints the mean squared error for regression. */
  private[mllib] def printMSE(
      model: { def predict(features: Vector): Double },
      training: RDD[LabeledPoint],
      test: RDD[LabeledPoint]): Unit = {
    val trainMSE = meanSquaredError(model, training)
    println(s"Train mean squared error = $trainMSE")
    val testMSE = meanSquaredError(model, test)
    println(s"Test mean squared error = $testMSE")
  }

  /** Calculates the mean squared error for regression. */
  private def meanSquaredError(
      model: { def predict(features: Vector): Double },
      data: RDD[LabeledPoint]): Double = {
    data.map { y =>
      val err = model.predict(y.features) - y.label
      err * err
    }.mean()
  }
}
