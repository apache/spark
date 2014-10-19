/*
 * Licensed until the Apache Software Foundation (ASF) under one or more
 * contribuuntilr license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file until You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed until in writing, software
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
 * using Practical Machine Learning Book data set to test apriori algorithm
 */
class AprioriSuite extends FunSuite with LocalSparkContext {

  test("test FIM with Apriori dataset 1")
  {

    // input data set
    val input = Array[String](
      "1 3 4",
      "2 3 5",
      "1 2 3 5",
      "2 5")

    // correct FIS answers
    val answer1 = Array((Set("4")), (Set("5")), (Set("2")), (Set("3")), (Set("1")), (Set("4", "1")), (Set("5", "2")), (Set("3", "1")), (Set("5", "3")), (Set("2", "3")), (Set("2", "1")), (Set("5", "1")), (Set("4", "3")), (Set("5", "2", "3")), (Set("3", "1", "5")), (Set("3", "1", "2")), (Set("4", "1", "3")), (Set("5", "2", "1")), (Set("5", "2", "3", "1")))
    val answer2 = Array((Set("4")), (Set("5")), (Set("2")), (Set("3")), (Set("1")), (Set("4", "1")), (Set("5", "2")), (Set("3", "1")), (Set("5", "3")), (Set("2", "3")), (Set("2", "1")), (Set("5", "1")), (Set("4", "3")), (Set("5", "2", "3")), (Set("3", "1", "5")), (Set("3", "1", "2")), (Set("4", "1", "3")), (Set("5", "2", "1")), (Set("5", "2", "3", "1")))
    val answer3 = Array((Set("5")), (Set("2")), (Set("3")), (Set("1")), (Set("5", "2")), (Set("3", "1")), (Set("5", "3")), (Set("2", "3")), (Set("5", "2", "3")))
    val answer4 = Array((Set("5")), (Set("2")), (Set("3")), (Set("1")), (Set("5", "2")), (Set("3", "1")), (Set("5", "3")), (Set("2", "3")), (Set("5", "2", "3")))
    val answer5 = Array((Set("5")), (Set("2")), (Set("3")), (Set("1")), (Set("5", "2")), (Set("3", "1")), (Set("5", "3")), (Set("2", "3")), (Set("5", "2", "3")))
    val answer6 = Array((Set("5")), (Set("2")), (Set("3")), (Set("5", "2")))
    val answer7 = Array((Set("5")), (Set("2")), (Set("3")), (Set("5", "2")))
    val answer8 = Array()
    val answer9 = Array()

    val target: (RDD[Array[String]], Double, SparkContext) => Array[(Set[String], Int)]= Apriori.apriori

    val dataSet = sc.parallelize(input)
    val rdd = dataSet.map(line => line.split(" "))

    val result9 = target(rdd, 0.9, sc)
    assert(result9.length == answer9.length)

    val result8 = target(rdd, 0.8, sc)
    assert(result8.length == answer8.length)

    val result7 = target(rdd, 0.7, sc)
    assert(result7.length == answer7.length)
    for (i <- 0 until result7.length){
      assert(answer7(i).equals(result7(i)._1))
    }

    val result6 = target(rdd, 0.6, sc)
    assert(result6.length == answer6.length)
    for (i <- 0 until result6.length)
      assert(answer6(i).equals(result6(i)._1))

    val result5 = target(rdd, 0.5, sc)
    assert(result5.length == answer5.length)
    for (i <- 0 until result5.length)
      assert(answer5(i).equals(result5(i)._1))

    val result4 = target(rdd, 0.4, sc)
    assert(result4.length == answer4.length)
    for (i <- 0 until result4.length)
      assert(answer4(i).equals(result4(i)._1))

    val result3 = target(rdd, 0.3, sc)
    assert(result3.length == answer3.length)
    for (i <- 0 until result3.length)
      assert(answer3(i).equals(result3(i)._1))

    val result2 = target(rdd, 0.2, sc)
    assert(result2.length == answer2.length)
    for (i <- 0 until result2.length)
      assert(answer2(i).equals(result2(i)._1))

    val result1 = target(rdd, 0.1, sc)
    assert(result1.length == answer1.length)
    for (i <- 0 until result1.length)
      assert(answer1(i).equals(result1(i)._1))
  }

  test("test FIM with Apriori dataset 2")
  {

    // input data set
    val input = Array[String](
      "r z h j p",
      "z y x w v u t s",
      "z",
      "r x n o s",
      "y r x z q t p",
      "y z x e q s t m")

    val target: (RDD[Array[String]], Double, SparkContext) => Array[(Set[String], Int)]= Apriori.apriori

    val dataSet = sc.parallelize(input)
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
