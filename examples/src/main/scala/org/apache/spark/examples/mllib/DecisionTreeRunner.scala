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

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration._
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

/**
 * An example runner for decision tree. Run with
 * {{{
 * ./bin/spark-example org.apache.spark.examples.mllib.DecisionTreeRunner [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DecisionTreeRunner extends Logging {

  private val usage =
    """
      |Usage: DecisionTreeRunner --algo <Classification, Regression> --trainDataDir path
      |  --testDataDir path --maxDepth num [--impurity <Gini,Entropy,Variance>] [--maxBins num]
    """.stripMargin

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println(usage)
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("DecisionTreeRunner")
    val sc = new SparkContext(conf)

    val argList = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--algo" :: string :: tail => nextOption(map ++ Map('algo -> string), tail)
        case "--impurity" :: string :: tail => nextOption(map ++ Map('impurity -> string), tail)
        case "--maxDepth" :: string :: tail => nextOption(map ++ Map('maxDepth -> string), tail)
        case "--maxBins" :: string :: tail => nextOption(map ++ Map('maxBins -> string), tail)
        case "--trainDataDir" :: string :: tail => nextOption(map ++ Map('trainDataDir -> string)
          , tail)
        case "--testDataDir" :: string :: tail => nextOption(map ++ Map('testDataDir -> string),
          tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail => logError("Unknown option " + option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(), argList)
    logDebug(options.toString())

    // Load training data.
    val trainData = loadLabeledData(sc, options.get('trainDataDir).get.toString)

    // Identify the type of algorithm.
    val algoStr =  options.get('algo).get.toString
    val algo = algoStr match {
      case "Classification" => Classification
      case "Regression" => Regression
    }

    // Identify the type of impurity.
    val impurityStr = options.getOrElse('impurity,
      if (algo == Classification) "Gini" else "Variance").toString
    val impurity = impurityStr match {
      case "Gini" => Gini
      case "Entropy" => Entropy
      case "Variance" => Variance
    }

    val maxDepth = options.getOrElse('maxDepth, "1").toString.toInt
    val maxBins = options.getOrElse('maxBins, "100").toString.toInt

    val strategy = new Strategy(algo, impurity, maxDepth, maxBins)
    val model = DecisionTree.train(trainData, strategy)

    // Load test data.
    val testData = loadLabeledData(sc, options.get('testDataDir).get.toString)

    // Measure algorithm accuracy
    if (algo == Classification) {
      val accuracy = accuracyScore(model, testData)
      logDebug("accuracy = " + accuracy)
    }

    if (algo == Regression) {
      val mse = meanSquaredError(model, testData)
      logDebug("mean square error = " + mse)
    }

    sc.stop()
  }

  /**
   * Load labeled data from a file. The data format used here is
   * <L>, <f1> <f2> ...,
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of LabeledPoint. Each labeled point has two elements: the first element is
   *         the label, and the second element represents the feature values (an array of Double).
   */
  private def loadLabeledData(sc: SparkContext, dir: String): RDD[LabeledPoint] = {
    sc.textFile(dir).map { line =>
      val parts = line.trim().split(",")
      val label = parts(0).toDouble
      val features = Vectors.dense(parts.slice(1,parts.length).map(_.toDouble))
      LabeledPoint(label, features)
    }
  }

  // TODO: Port this method to a generic metrics package.
  /**
   * Calculates the classifier accuracy.
   */
  private def accuracyScore(model: DecisionTreeModel, data: RDD[LabeledPoint],
                            threshold: Double = 0.5): Double = {
    def predictedValue(features: Vector) = {
      if (model.predict(features) < threshold) 0.0 else 1.0
    }
    val correctCount = data.filter(y => predictedValue(y.features) == y.label).count()
    val count = data.count()
    logDebug("correct prediction count = " +  correctCount)
    logDebug("data count = " + count)
    correctCount.toDouble / count
  }

  // TODO: Port this method to a generic metrics package
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
