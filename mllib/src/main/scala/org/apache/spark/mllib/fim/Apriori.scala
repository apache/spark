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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.{Logging, SparkContext}

/**
 * This object implements Apriori algorithm using Spark to find frequent item set in the given data set.
 */
object Apriori extends Logging with Serializable {

  /**
   * Generate the first round FIS(frequent item set) from input data set. Returns single distinct item that
   * appear greater than minCount times.
   * 
   * @param dataSet input data set
   * @param minCount the minimum appearance time that computed from minimum degree of support
   * @return FIS
   */
  private def genFirstRoundFIS(dataSet: RDD[Set[String]],
                        minCount: Double): RDD[(Set[String], Int)] = {
    dataSet.flatMap(line => line)
      .map(v => (v, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .map(x => (Set(x._1), x._2))
  }

  /**
   * Scan the input data set and filter out the eligible FIS
   * @param candidate candidate FIS
   * @param minCount the minimum appearance time that computed from minimum degree of support
   * @return FIS
   */
  private def scanAndFilter(dataSet: RDD[Set[String]],
               candidate: RDD[Set[String]],
               minCount: Double,
               sc: SparkContext): RDD[(Set[String], Int)] = {

    dataSet.cartesian(candidate).map(x =>
      if (x._2.subsetOf(x._1)) {
        (x._2, 1)
      } else {
        (x._2, 0)
      }).reduceByKey(_+_).filter(x => x._2 >= minCount)
  }

  /**
   * Generate the next round of FIS candidate using this round FIS
   * @param FISk
   * @param k
   * @return candidate FIS
   */
  private def generateCombination(FISk: RDD[Set[String]],
                 k: Int): RDD[Set[String]] = {
    FISk.cartesian(FISk)
      .map(x => x._1 ++ x._2)
      .filter(x => x.size == k)
      .distinct()
  }

  /**
   * Function of apriori algorithm implementation.
   *
   * @param input  Input data set to find frequent item set
   * @param minSupport The minimum degree of support
   * @param sc SparkContext to use
   * @return frequent item sets in a array
   */
  def apriori(input: RDD[Array[String]],
                 minSupport: Double,
                 sc: SparkContext): Array[(Set[String], Int)] = {

    /*
     * This apriori implementation uses cartesian of two RDD, input data set and candidate
     * FIS (frequent item set).
     * The resulting FIS are computed in two steps:
     * The first step, find eligible distinct item in data set.
     * The second step, loop in k round, in each round generate candidate FIS and filter out eligible FIS
     */

    // calculate minimum appearance count for minimum degree of support
    val dataSetLen: Long = input.count()
    val minCount = minSupport * dataSetLen

    // This algorithm finds frequent item set, so convert each element of RDD to set to
    // eliminate duplicated item if any
    val dataSet = input.map(_.toSet)

    // FIS is the result to return
    val FIS = collection.mutable.ArrayBuffer[RDD[(Set[String], Int)]]()
    val FIS1: RDD[(Set[String], Int)] = genFirstRoundFIS(dataSet, minCount)
    if (FIS1.count() < 0) {
      return Array[(Set[String], Int)]()
    }

    FIS += FIS1

    // FIS for round k
    var FISk = FIS1
    // round counter
    var k = 2

    while (FIS(k - 2).count() > 1) {

      // generate candidate FIS
      val candidate: RDD[Set[String]] = generateCombination(FIS(k - 2).map(x => x._1), k)

      // filter out eligible FIS
      FISk = scanAndFilter(dataSet, candidate, minCount, sc)

      // add it to the result and go to next round
      FIS += FISk
      k = k + 1
    }

    // convert all FIS to array before returning them
    val retArr = collection.mutable.ArrayBuffer[(Set[String], Int)]()
    for (l <- FIS) {
      retArr.appendAll(l.collect())
    }
    retArr.toArray
  }

  private def printFISk(FIS: RDD[(Set[String], Int)], k: Int) {
    print("FIS" + (k - 2) + " size " + FIS.count() + " value: ")
    FIS.collect().foreach(x => print("(" + x._1 + ", " + x._2 + ") "))
    println()
  }
  
  private def printCk(Ck: RDD[Set[String]], k: Int) {
    print("C" + (k - 2) + " size "+ Ck.count() + " value: ")
    Ck.collect().foreach(print)
    println()
  }
}
