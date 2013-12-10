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
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.model.Filter

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
    val arr = DecisionTreeSuite.generateReverseOrderedLabeledPoints()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy("regression",Gini,3,100,"sort")
    val (splits, bins) = DecisionTree.find_splits_bins(rdd,strategy)
    assert(splits.length==2)
    assert(bins.length==2)
    assert(splits(0).length==99)
    assert(bins(0).length==100)
    println(splits(1)(98))
  }

  test("stump"){
    val arr = DecisionTreeSuite.generateReverseOrderedLabeledPoints()
    assert(arr.length == 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy("regression",Gini,3,100,"sort")
    val (splits, bins) = DecisionTree.find_splits_bins(rdd,strategy)
    assert(splits.length==2)
    assert(splits(0).length==99)
    assert(bins.length==2)
    assert(bins(0).length==100)
    assert(splits(0).length==99)
    assert(bins(0).length==100)
    println(splits(1)(98))
    DecisionTree.findBestSplits(rdd,Array(0.0),strategy,0,Array[List[Filter]](),splits,bins)
  }

}

object DecisionTreeSuite {

  def generateReverseOrderedLabeledPoints() : Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000){
      val lp = new LabeledPoint(1.0,Array(i.toDouble,1000.0-i))
      arr(i) = lp
    }
    arr
  }

}
