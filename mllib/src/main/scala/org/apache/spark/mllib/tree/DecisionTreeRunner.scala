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
package org.apache.spark.mllib.tree

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel

object DecisionTreeRunner extends Logging {


  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "DecisionTree")
    val data = loadLabeledData(sc, args(1))
    val maxDepth = args(2).toInt
    val maxBins = args(3).toInt

    val strategy = new Strategy(kind = "classification", impurity = Gini, maxDepth = maxDepth, maxBins = maxBins)
    val model = new DecisionTree(strategy).train(data)

    val accuracy = accuracyScore(model, data)
    logDebug("accuracy = " + accuracy)

    sc.stop()
  }

  /**
   * Load labeled data from a file. The data format used here is
   * <L>, <f1> <f2> ...
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of LabeledPoint. Each labeled point has two elements: the first element is
   *         the label, and the second element represents the feature values (an array of Double).
   */
  def loadLabeledData(sc: SparkContext, dir: String): RDD[LabeledPoint] = {
    sc.textFile(dir).map { line =>
      val parts = line.trim().split(",")
      val label = parts(0).toDouble
      val features = parts.slice(1,parts.length).map(_.toDouble)
      LabeledPoint(label, features)
    }
  }

  //TODO: Port them to a metrics package
  def accuracyScore(model : DecisionTreeModel, data : RDD[LabeledPoint]) : Double = {
    val correctCount = data.filter(y => model.predict(y.features) == y.label).count()
    val count = data.count()
    logDebug("correct prediction count = " +  correctCount)
    logDebug("data count = " + count)
    correctCount.toDouble / count
  }



}
