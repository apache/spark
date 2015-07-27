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

package org.apache.spark.mllib.pmml.export

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.{Strategy, Algo}
import org.apache.spark.mllib.tree.impurity.{Gini, Impurities}
import org.apache.spark.mllib.tree.{DecisionTree, DecisionTreeSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class DecisionTreePMMLModelExportSuite extends SparkFunSuite with MLlibTestSparkContext{

  test("DecisionTree Regression Model PMML Export"){
    val arrLabeledPoints = DecisionTreeSuite.generateOrderedLabeledPoints()
    val rddLabeledPoints = sc.parallelize(arrLabeledPoints)
    val decisionTreeWithRegression = DecisionTree.train(rddLabeledPoints,Algo.Regression,Impurities.fromString("variance"),3)

    val pmmlExporter = PMMLModelExportFactory.createPMMLModelExport(decisionTreeWithRegression)

    assert(pmmlExporter.isInstanceOf[DecisionTreePMMLModelExport])
    val pmmlModel = pmmlExporter.getPmml.getDataDictionary
    println("PMML for Decision tree regressor")
    println(decisionTreeWithRegression.toPMML())
  }

  test("DecisionTree Classification Model PMML Export"){
    val maxBins = 2 * (math.pow(2, 3 - 1).toInt - 1) // just enough bins to allow unordered features
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 10,
      numClasses = 5, maxBins = maxBins,
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))

    val decisionTreeWithRegression = DecisionTree.train(rdd, strategy)
    //val decisionTreeWithRegression = DecisionTree.trainClassifier(rddLabeledPoints,3,categoricalFeaturesInfo = Map(0 -> 3, 1-> 3),"gini",4,100)

    val pmmlExporter = PMMLModelExportFactory.createPMMLModelExport(decisionTreeWithRegression)

    assert(pmmlExporter.isInstanceOf[DecisionTreePMMLModelExport])
    val pmmlModel = pmmlExporter.getPmml.getDataDictionary
    println("PMML for Decision tree classifier")
    println(decisionTreeWithRegression.toPMML())
  }
}
