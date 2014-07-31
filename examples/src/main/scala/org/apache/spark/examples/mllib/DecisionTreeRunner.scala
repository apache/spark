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
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, impurity}
import org.apache.spark.mllib.tree.configuration.{Algo, Strategy}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * An example runner for decision tree. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DecisionTreeRunner [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DecisionTreeRunner {

  object ImpurityType extends Enumeration {
    type ImpurityType = Value
    val Gini, Entropy, Variance = Value
  }

  import ImpurityType._

  case class Params(
      input: String = null,
      algo: Algo = Classification,
      numClassesForClassification: Int = 2,
      maxDepth: Int = 5,
      impurity: ImpurityType = Gini,
      maxBins: Int = 100)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DecisionTreeRunner") {
      head("DecisionTreeRunner: an example decision tree app.")
      opt[String]("algo")
        .text(s"algorithm (${Algo.values.mkString(",")}), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = Algo.withName(x)))
      opt[String]("impurity")
        .text(s"impurity type (${ImpurityType.values.mkString(",")}), " +
          s"default: ${defaultParams.impurity}")
        .action((x, c) => c.copy(impurity = ImpurityType.withName(x)))
      opt[Int]("maxDepth")
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("numClassesForClassification")
        .text(s"number of classes for classification, "
          + s"default: ${defaultParams.numClassesForClassification}")
        .action((x, c) => c.copy(numClassesForClassification = x))
      opt[Int]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      arg[String]("<input>")
        .text("input paths to labeled examples in dense format (label,f0 f1 f2 ...)")
        .required()
        .action((x, c) => c.copy(input = x))
      checkConfig { params =>
        if (params.algo == Classification &&
            (params.impurity == Gini || params.impurity == Entropy)) {
          success
        } else if (params.algo == Regression && params.impurity == Variance) {
          success
        } else {
          failure(s"Algo ${params.algo} is not compatible with impurity ${params.impurity}.")
        }
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
    val examples = MLUtils.loadLabeledPoints(sc, params.input).cache()

    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()

    println(s"numTraining = $numTraining, numTest = $numTest.")

    examples.unpersist(blocking = false)

    val impurityCalculator = params.impurity match {
      case Gini => impurity.Gini
      case Entropy => impurity.Entropy
      case Variance => impurity.Variance
    }

    val strategy
      = new Strategy(
          algo = params.algo,
          impurity = impurityCalculator,
          maxDepth = params.maxDepth,
          maxBins = params.maxBins,
          numClassesForClassification = params.numClassesForClassification)
    val model = DecisionTree.train(training, strategy)

    if (params.algo == Classification) {
      val accuracy = accuracyScore(model, test)
      println(s"Test accuracy = $accuracy.")
    }

    if (params.algo == Regression) {
      val mse = meanSquaredError(model, test)
      println(s"Test mean squared error = $mse.")
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
  private def meanSquaredError(tree: DecisionTreeModel, data: RDD[LabeledPoint]): Double = {
    data.map { y =>
      val err = tree.predict(y.features) - y.label
      err * err
    }.mean()
  }
}
