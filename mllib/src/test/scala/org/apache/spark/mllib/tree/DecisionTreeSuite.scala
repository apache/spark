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

import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.jblas._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Variance}
import org.apache.spark.mllib.tree.model.Filter
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo._
import scala.collection.mutable
import org.apache.spark.mllib.tree.configuration.FeatureType._

class DecisionTreeSuite extends FunSuite with BeforeAndAfterAll {

  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("split and bin calculation"){
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Gini,3,100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    assert(splits.length==2)
    assert(bins.length==2)
    assert(splits(0).length==99)
    assert(bins(0).length==100)
  }

  test("split and bin calculation for categorical variables"){
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Gini,3,100,categoricalFeaturesInfo = Map(0 -> 2, 1-> 2))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    assert(splits.length==2)
    assert(bins.length==2)
    assert(splits(0).length==99)
    assert(bins(0).length==100)

    //Checking splits

    assert(splits(0)(0).feature == 0)
    assert(splits(0)(0).threshold == Double.MinValue)
    assert(splits(0)(0).featureType == Categorical)
    assert(splits(0)(0).categories.length == 1)
    assert(splits(0)(0).categories.contains(1.0))


    assert(splits(0)(1).feature == 0)
    assert(splits(0)(1).threshold == Double.MinValue)
    assert(splits(0)(1).featureType == Categorical)
    assert(splits(0)(1).categories.length == 2)
    assert(splits(0)(1).categories.contains(1.0))
    assert(splits(0)(1).categories.contains(0.0))

    assert(splits(0)(2) == null)

    assert(splits(1)(0).feature == 1)
    assert(splits(1)(0).threshold == Double.MinValue)
    assert(splits(1)(0).featureType == Categorical)
    assert(splits(1)(0).categories.length == 1)
    assert(splits(1)(0).categories.contains(0.0))


    assert(splits(1)(1).feature == 1)
    assert(splits(1)(1).threshold == Double.MinValue)
    assert(splits(1)(1).featureType == Categorical)
    assert(splits(1)(1).categories.length == 2)
    assert(splits(1)(1).categories.contains(1.0))
    assert(splits(1)(1).categories.contains(0.0))

    assert(splits(1)(2) == null)


    // Checks bins

    assert(bins(0)(0).category == 1.0)
    assert(bins(0)(0).lowSplit.categories.length == 0)
    assert(bins(0)(0).highSplit.categories.length == 1)
    assert(bins(0)(0).highSplit.categories.contains(1.0))

    assert(bins(0)(1).category == 0.0)
    assert(bins(0)(1).lowSplit.categories.length == 1)
    assert(bins(0)(1).lowSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.length == 2)
    assert(bins(0)(1).highSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.contains(0.0))

    assert(bins(0)(2).category == Double.MaxValue)

    assert(bins(1)(0).category == 0.0)
    assert(bins(1)(0).lowSplit.categories.length == 0)
    assert(bins(1)(0).highSplit.categories.length == 1)
    assert(bins(1)(0).highSplit.categories.contains(0.0))

    assert(bins(1)(1).category == 1.0)
    assert(bins(1)(1).lowSplit.categories.length == 1)
    assert(bins(1)(1).lowSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.length == 2)
    assert(bins(1)(1).highSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.contains(1.0))

    assert(bins(1)(2).category == Double.MaxValue)

  }

  test("split and bin calculations for categorical variables with no sample for one category"){
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Gini,3,100,categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)

    //Checking splits

    assert(splits(0)(0).feature == 0)
    assert(splits(0)(0).threshold == Double.MinValue)
    assert(splits(0)(0).featureType == Categorical)
    assert(splits(0)(0).categories.length == 1)
    assert(splits(0)(0).categories.contains(1.0))

    assert(splits(0)(1).feature == 0)
    assert(splits(0)(1).threshold == Double.MinValue)
    assert(splits(0)(1).featureType == Categorical)
    assert(splits(0)(1).categories.length == 2)
    assert(splits(0)(1).categories.contains(1.0))
    assert(splits(0)(1).categories.contains(0.0))

    assert(splits(0)(2).feature == 0)
    assert(splits(0)(2).threshold == Double.MinValue)
    assert(splits(0)(2).featureType == Categorical)
    assert(splits(0)(2).categories.length == 3)
    assert(splits(0)(2).categories.contains(1.0))
    assert(splits(0)(2).categories.contains(0.0))
    assert(splits(0)(2).categories.contains(2.0))

    assert(splits(0)(3) == null)

    assert(splits(1)(0).feature == 1)
    assert(splits(1)(0).threshold == Double.MinValue)
    assert(splits(1)(0).featureType == Categorical)
    assert(splits(1)(0).categories.length == 1)
    assert(splits(1)(0).categories.contains(0.0))

    assert(splits(1)(1).feature == 1)
    assert(splits(1)(1).threshold == Double.MinValue)
    assert(splits(1)(1).featureType == Categorical)
    assert(splits(1)(1).categories.length == 2)
    assert(splits(1)(1).categories.contains(1.0))
    assert(splits(1)(1).categories.contains(0.0))

    assert(splits(1)(2).feature == 1)
    assert(splits(1)(2).threshold == Double.MinValue)
    assert(splits(1)(2).featureType == Categorical)
    assert(splits(1)(2).categories.length == 3)
    assert(splits(1)(2).categories.contains(1.0))
    assert(splits(1)(2).categories.contains(0.0))
    assert(splits(1)(2).categories.contains(2.0))

    assert(splits(1)(3) == null)


    // Checks bins

    assert(bins(0)(0).category == 1.0)
    assert(bins(0)(0).lowSplit.categories.length == 0)
    assert(bins(0)(0).highSplit.categories.length == 1)
    assert(bins(0)(0).highSplit.categories.contains(1.0))

    assert(bins(0)(1).category == 0.0)
    assert(bins(0)(1).lowSplit.categories.length == 1)
    assert(bins(0)(1).lowSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.length == 2)
    assert(bins(0)(1).highSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.contains(0.0))

    assert(bins(0)(2).category == 2.0)
    assert(bins(0)(2).lowSplit.categories.length == 2)
    assert(bins(0)(2).lowSplit.categories.contains(1.0))
    assert(bins(0)(2).lowSplit.categories.contains(0.0))
    assert(bins(0)(2).highSplit.categories.length == 3)
    assert(bins(0)(2).highSplit.categories.contains(1.0))
    assert(bins(0)(2).highSplit.categories.contains(0.0))
    assert(bins(0)(2).highSplit.categories.contains(2.0))

    assert(bins(0)(3).category == Double.MaxValue)

    assert(bins(1)(0).category == 0.0)
    assert(bins(1)(0).lowSplit.categories.length == 0)
    assert(bins(1)(0).highSplit.categories.length == 1)
    assert(bins(1)(0).highSplit.categories.contains(0.0))

    assert(bins(1)(1).category == 1.0)
    assert(bins(1)(1).lowSplit.categories.length == 1)
    assert(bins(1)(1).lowSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.length == 2)
    assert(bins(1)(1).highSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.contains(1.0))

    assert(bins(1)(2).category == 2.0)
    assert(bins(1)(2).lowSplit.categories.length == 2)
    assert(bins(1)(2).lowSplit.categories.contains(0.0))
    assert(bins(1)(2).lowSplit.categories.contains(1.0))
    assert(bins(1)(2).highSplit.categories.length == 3)
    assert(bins(1)(2).highSplit.categories.contains(0.0))
    assert(bins(1)(2).highSplit.categories.contains(1.0))
    assert(bins(1)(2).highSplit.categories.contains(2.0))

    assert(bins(1)(3).category == Double.MaxValue)


  }

  test("classification stump with all categorical variables"){
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Gini,3,100,categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    strategy.numBins = 100
    val bestSplits = DecisionTree.findBestSplits(rdd,new Array(7),strategy,0,Array[List[Filter]](),splits,bins)

    val split = bestSplits(0)._1
    assert(split.categories.length == 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType == Categorical)
    assert(split.threshold == Double.MinValue)

    val stats = bestSplits(0)._2
    assert(stats.gain > 0)
    assert(stats.predict > 0.4)
    assert(stats.predict < 0.5)
    assert(stats.impurity > 0.2)

  }

  test("regression stump with all categorical variables"){
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Regression,Variance,3,100,categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    strategy.numBins = 100
    val bestSplits = DecisionTree.findBestSplits(rdd,new Array(7),strategy,0,Array[List[Filter]](),splits,bins)

    val split = bestSplits(0)._1
    assert(split.categories.length == 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType == Categorical)
    assert(split.threshold == Double.MinValue)

    val stats = bestSplits(0)._2
    assert(stats.gain > 0)
    assert(stats.predict > 0.4)
    assert(stats.predict < 0.5)
    assert(stats.impurity > 0.2)
  }


  test("stump with fixed label 0 for Gini"){
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Gini,3,100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    assert(splits.length==2)
    assert(splits(0).length==99)
    assert(bins.length==2)
    assert(bins(0).length==100)
    assert(splits(0).length==99)
    assert(bins(0).length==100)

    strategy.numBins = 100
    val bestSplits = DecisionTree.findBestSplits(rdd,new Array(7),strategy,0,Array[List[Filter]](),splits,bins)
    assert(bestSplits.length == 1)
    assert(0==bestSplits(0)._1.feature)
    assert(10==bestSplits(0)._1.threshold)
    assert(0==bestSplits(0)._2.gain)
    assert(0==bestSplits(0)._2.leftImpurity)
    assert(0==bestSplits(0)._2.rightImpurity)

  }

  test("stump with fixed label 1 for Gini"){
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Gini,3,100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    assert(splits.length==2)
    assert(splits(0).length==99)
    assert(bins.length==2)
    assert(bins(0).length==100)
    assert(splits(0).length==99)
    assert(bins(0).length==100)

    strategy.numBins = 100
    val bestSplits = DecisionTree.findBestSplits(rdd,Array(0.0),strategy,0,Array[List[Filter]](),splits,bins)
    assert(bestSplits.length == 1)
    assert(0==bestSplits(0)._1.feature)
    assert(10==bestSplits(0)._1.threshold)
    assert(0==bestSplits(0)._2.gain)
    assert(0==bestSplits(0)._2.leftImpurity)
    assert(0==bestSplits(0)._2.rightImpurity)
    assert(1==bestSplits(0)._2.predict)

  }


  test("stump with fixed label 0 for Entropy"){
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Entropy,3,100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    assert(splits.length==2)
    assert(splits(0).length==99)
    assert(bins.length==2)
    assert(bins(0).length==100)
    assert(splits(0).length==99)
    assert(bins(0).length==100)

    strategy.numBins = 100
    val bestSplits = DecisionTree.findBestSplits(rdd,Array(0.0),strategy,0,Array[List[Filter]](),splits,bins)
    assert(bestSplits.length == 1)
    assert(0==bestSplits(0)._1.feature)
    assert(10==bestSplits(0)._1.threshold)
    assert(0==bestSplits(0)._2.gain)
    assert(0==bestSplits(0)._2.leftImpurity)
    assert(0==bestSplits(0)._2.rightImpurity)
    assert(0==bestSplits(0)._2.predict)
  }

  test("stump with fixed label 1 for Entropy"){
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification,Entropy,3,100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    assert(splits.length==2)
    assert(splits(0).length==99)
    assert(bins.length==2)
    assert(bins(0).length==100)
    assert(splits(0).length==99)
    assert(bins(0).length==100)

    strategy.numBins = 100
    val bestSplits = DecisionTree.findBestSplits(rdd,Array(0.0),strategy,0,Array[List[Filter]](),splits,bins)
    assert(bestSplits.length == 1)
    assert(0==bestSplits(0)._1.feature)
    assert(10==bestSplits(0)._1.threshold)
    assert(0==bestSplits(0)._2.gain)
    assert(0==bestSplits(0)._2.leftImpurity)
    assert(0==bestSplits(0)._2.rightImpurity)
    assert(1==bestSplits(0)._2.predict)
  }


}

object DecisionTreeSuite {

  def generateOrderedLabeledPointsWithLabel0() : Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000){
      val lp = new LabeledPoint(0.0,Array(i.toDouble,1000.0-i))
      arr(i) = lp
    }
    arr
  }


  def generateOrderedLabeledPointsWithLabel1() : Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000){
      val lp = new LabeledPoint(1.0,Array(i.toDouble,999.0-i))
      arr(i) = lp
    }
    arr
  }

  def generateCategoricalDataPoints() : Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000){
      if (i < 600){
        arr(i) = new LabeledPoint(1.0,Array(0.0,1.0))
      } else {
        arr(i) = new LabeledPoint(0.0,Array(1.0,0.0))
      }
    }
    arr
  }

}
