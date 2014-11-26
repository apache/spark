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

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast._

/**
 * calculate frequent item set using FPGrowth algorithm with dada set and minSupport
 * the FPGrowth algorithm have two step task
 * step one is scaning data db to get L1 by minSuppprt
 * step two is scan data db once to get Lk
 */
class FPGrowth extends Logging with Serializable {

  /**
   * FPGrowth algorithm：
   * step 1：calculate L1 by minSupport
   * step 2: calculate Ln by FP-Tree
   * @param sc sparkContext
   * @param RDD  For mining frequent item sets dataset
   * @param minSuport The minimum degree of support
   * @return frequent item sets
   */
  def FPGrowth(RDD: RDD[Array[String]],
               minSuport: Double,
               sc: SparkContext): Array[(String, Int)] = {
    val count = RDD.count()
    logDebug("data set count:" + count)
    val minCount = minSuport * count
    logDebug("minSuppot count:" + minSuport)

    //one times scan data db to get L1
    val L1 = FPGStepOne(RDD, minCount)
    logDebug("L1 length:" + L1.length)
    logDebug("L1:" + L1)

    //two times scan data db to get Ln
    val Ln = FPGStepTwo(sc, RDD, minCount, L1)
    //add L1 and Ln to get fim
    val fim = L1 ++ Ln

    return fim

  }

  /**
   * Step 1: calculate L1 by min support
   * @param RDD  For mining frequent item sets dataset
   * @param minCount The minimum degree of support
   * @return L1
   */
  def FPGStepOne(RDD: RDD[Array[String]],
                 minCount: Double): Array[(String, Int)] = {
    RDD.flatMap(v => v)
      .map(v => (v, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .collect()
      .distinct
      .sortWith(_._2 > _._2)
  }

  /**
   * step 2: using PFP-Tree to calculate the fim
   * @param sc sparkContext
   * @param RDD  For mining frequent item sets dataset
   * @param minCount The minimum degree of support
   * @param L1 frenauent item set as length 1
   * @return Ln
   */
  def FPGStepTwo(sc: SparkContext,
                 RDD: RDD[Array[String]],
                 minCount: Double,
                 L1: Array[(String, Int)]): Array[(String, Int)] = {
    //broadcast L1
    val bdL1 = sc.broadcast(L1)
    //val bdL1List = bdL1.value

    RDD.flatMap(line => L12LineMap(line, bdL1))
      .groupByKey()
      .flatMap(line => FPTree(line, minCount))
      .collect()

  }

  /**
   * create CFP-Tree
   * give L1,example:a 2,b 4,c 3  and give line,example: a,b,c,d
   * after calculate,the result is:
   * a,(b,c)
   * c,(b)
   * note,the result have not b,()
   * @param line dataset line
   * @param bdL1 L1
   * @return CFP-Tree
   */
  def L12LineMap(line: Array[String],
                 bdL1: Broadcast[Array[(String, Int)]]): Array[(String, Array[String])] = {
    // broadcast value
    val bdL1List = bdL1.value
    // the result variable
    var lineArrayBuffer = collection.mutable.ArrayBuffer[(String, Int)]()

    for (item <- line) {

      val opt = bdL1List.find(_._1.equals(item))

      if (opt != None) {
        lineArrayBuffer ++= opt
      }

    }

    // sort array
    val lineArray = lineArrayBuffer
      .sortWith(_._1 > _._1)
      .sortWith(_._2 > _._2)
      .toArray


    var arrArrayBuffer = collection.mutable.ArrayBuffer[(String, Array[String])]()

    /**
     * give (a,4) (b 3),(c,3),after
     * b，（(a,4)
     * c，（(a,4) (b 3)）
     */
    var arrBuffer = collection.mutable.ArrayBuffer[String]()
    for (item <- lineArray) {
      val arr = lineArray.take(lineArray.indexOf(item))

      arrBuffer.clear()

      if (arr.length > 0) {
        for (tempArr <- arr) {
          //remain key
          arrBuffer += tempArr._1
        }
        arrArrayBuffer += ((item._1, arrBuffer.toArray))
      }

    }

    return arrArrayBuffer.toArray

  }

  /**
   * genarate fim set by FPTree,everyone node have a CPFTree that can combination frenquent item
   * @param line dataset line
   * @param minCount The minimum degree of support
   * @return fim
   */
  def FPTree(line: (String, Iterable[Array[String]]), minCount: Double): Array[(String, Int)] = {
    // frequently item
    val key = line._1
    // the set of construction CPFTree
    val value = line._2

    val _lineBuffer = collection.mutable.ArrayBuffer[(String, Int)]()
    val map = scala.collection.mutable.Map[String, Int]()
    // tree step
    var k = 1
    // loop the data set while k>0
    while (k > 0) {
      map.clear()

      // loop data set
      for (it <- value) {
        if (it.length >= k) {
          // from m get n combinations,using scala method
          val lineCom = it.toList.combinations(k).toList

          // add key to combination
          for (item <- lineCom) {
            // sort array
            val list2key: List[String] = (item :+ key)
              .sortWith(_ > _)

            val s = list2key.mkString(" ")

            if (map.get(s) == None) {
              map(s) = 1
            }
            else {
              map(s) = map.apply(s) + 1
            }
          }
        }
      }

      var line: Array[(String, Int)] = null

      if (map.size != 0) {
        // get fim set
        val lineTemp = map.filter(_._2 >= minCount)

        if (lineTemp.size != 0) {
          line = lineTemp.toArray.array
          _lineBuffer ++= line
        }

      }

      // reset k value
      if ((line == null) || (line.length == 0)) {
        k = 0
      }
      else {
        k = k + 1
      }

    }

    return _lineBuffer.toArray

  }

}

