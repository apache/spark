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
package org.apache.spark.mllib.fim

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
 * scala test unit
 * using Practical Machine Learning Book data test the apriori algorithm result by minSupport from 0.9 to 0.1
 */
class AprioriSuite extends FunSuite with LocalSparkContext {

  test("test FIM with AprioriByBroadcast dataset 1")
  {
    val arr = AprioriSuite.createFIMDataSet1()
    val target: (RDD[Array[String]], Double, SparkContext) => Array[(Set[String], Int)]= AprioriByBroadcast.apriori

    val dataSet = sc.parallelize(arr)
    val rdd = dataSet.map(line => line.split(" "))

    for (i <- 1 to 9){
      println(s"frequent item set with support ${i/10d}")
      target(rdd, i/10d, sc).foreach(x => print("(" + x._1 + "), "))
      println()
    }
  }

  test("test FIM with AprioriByBroadcast dataset 2")
  {
    val arr = AprioriSuite.createFIMDataSet2()
    val target: (RDD[Array[String]], Double, SparkContext) => Array[(Set[String], Int)]= AprioriByBroadcast.apriori

    val dataSet = sc.parallelize(arr)
    val rdd = dataSet.map(line => line.split(" "))

    assert(target(rdd,0.9,sc).length == 0)

    assert(target(rdd,0.8,sc).length == 1)

    assert(target(rdd,0.7,sc).length == 1)

    assert(target(rdd,0.6,sc).length == 2)

    assert(target(rdd,0.5,sc).length == 18)

    assert(target(rdd,0.4,sc).length == 18)

    assert(target(rdd,0.3,sc).length == 54)

    assert(target(rdd,0.2,sc).length == 54)

    assert(target(rdd,0.1,sc).length == 625)

  }

  test("test FIM with AprioriByCartesian dataset 1")
  {
    val arr = AprioriSuite.createFIMDataSet1()
    val target: (RDD[Array[String]], Double, SparkContext) => Array[(Set[String], Int)]= AprioriByCartesian.apriori

    val dataSet = sc.parallelize(arr)
    val rdd = dataSet.map(line => line.split(" "))

    for (i <- 1 to 9){
      println(s"frequent item set with support ${i/10d}")
      target(rdd, i/10d, sc).foreach(x => print("(" + x._1 + "), "))
      println()
    }

    
  }
  test("test FIM with AprioriByCartesian dataset 2")
  {
    val arr = AprioriSuite.createFIMDataSet2()
    val target: (RDD[Array[String]], Double, SparkContext) => Array[(Set[String], Int)]= AprioriByCartesian.apriori

    val dataSet = sc.parallelize(arr)
    val rdd = dataSet.map(line => line.split(" "))

    assert(target(rdd,0.9,sc).length == 0)

    assert(target(rdd,0.8,sc).length == 1)

    assert(target(rdd,0.7,sc).length == 1)

    assert(target(rdd,0.6,sc).length == 2)

    assert(target(rdd,0.5,sc).length == 18)

    assert(target(rdd,0.4,sc).length == 18)

    assert(target(rdd,0.3,sc).length == 54)

    assert(target(rdd,0.2,sc).length == 54)

    assert(target(rdd,0.1,sc).length == 625)

  }

}

/**
 * create dataset
 */
object AprioriSuite
{
  /**
   * create dataset using Practical Machine Learning Book data
   * @return dataset
   */
  def createFIMDataSet1():Array[String] = {
    val arr = Array[String](
      "1 3 4",
      "2 3 5",
      "1 2 3 5",
      "2 5")
    arr
  }

  def createFIMDataSet2():Array[String] = {
    val arr = Array[String](
      "r z h j p",
      "z y x w v u t s",
      "z",
      "r x n o s",
      "y r x z q t p",
      "y z x e q s t m")
    arr
  }
}

