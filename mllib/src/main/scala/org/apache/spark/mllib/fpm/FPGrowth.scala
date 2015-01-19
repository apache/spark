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

package org.apache.spark.mllib.fpm

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, Map}

/**
 * This class implements Parallel FPGrowth algorithm to do frequent pattern matching on input data.
 * Parallel FPGrowth (PFP) partitions computation in such a way that each machine executes an
 * independent group of mining tasks. More detail of this algorithm can be found at
 * http://infolab.stanford.edu/~echang/recsys08-69.pdf
 */
class FPGrowth private(private var minSupport: Double) extends Logging with Serializable {

  /**
   * Constructs a FPGrowth instance with default parameters:
   * {minSupport: 0.5}
   */
  def this() = this(0.5)

  /**
   * set the minimal support level, default is 0.5
   * @param minSupport minimal support level
   */
  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  /**
   * Compute a FPGrowth Model that contains frequent pattern result.
   * @param data input data set
   * @return FPGrowth Model
   */
  def run(data: RDD[Array[String]]): FPGrowthModel = {
    val model = runAlgorithm(data)
    model
  }

  /**
   * Implementation of PFP.
   */
  private def runAlgorithm(data: RDD[Array[String]]): FPGrowthModel = {
    val count = data.count()
    val minCount = minSupport * count
    val single = generateSingleItem(data, minCount)
    val combinations = generateCombinations(data, minCount, single)
    new FPGrowthModel(single ++ combinations)
  }

  /**
   * Generate single item pattern by filtering the input data using minimal support level
   */
  private def generateSingleItem(
      data: RDD[Array[String]],
      minCount: Double): Array[(String, Int)] = {
    data.flatMap(v => v)
      .map(v => (v, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .collect()
      .distinct
      .sortWith(_._2 > _._2)
  }

  /**
   * Generate combination of items by computing on FPTree,
   * the computation is done on each FPTree partitions.
   */
  private def generateCombinations(
      data: RDD[Array[String]],
      minCount: Double,
      singleItem: Array[(String, Int)]): Array[(String, Int)] = {
    val single = data.context.broadcast(singleItem)
    data.flatMap(basket => createFPTree(basket, single))
      .groupByKey()
      .flatMap(partition => runFPTree(partition, minCount))
      .collect()
  }

  /**
   * Create FP-Tree partition for the giving basket
   */
  private def createFPTree(
      basket: Array[String],
      singleItem: Broadcast[Array[(String, Int)]]): Array[(String, Array[String])] = {
    var output = ArrayBuffer[(String, Array[String])]()
    var combination = ArrayBuffer[String]()
    val single = singleItem.value
    var items = ArrayBuffer[(String, Int)]()

    // Filter the basket by single item pattern
    val iterator = basket.iterator
    while (iterator.hasNext){
      val item = iterator.next
      val opt = single.find(_._1.equals(item))
      if (opt != None) {
        items ++= opt
      }
    }

    // Sort it and create the item combinations
    val sortedItems = items.sortWith(_._1 > _._1).sortWith(_._2 > _._2).toArray
    val itemIterator = sortedItems.iterator
    while (itemIterator.hasNext) {
      combination.clear()
      val item = itemIterator.next
      val firstNItems = sortedItems.take(sortedItems.indexOf(item))
      if (firstNItems.length > 0) {
        val iterator = firstNItems.iterator
        while (iterator.hasNext) {
          val elem = iterator.next
          combination += elem._1
        }
        output += ((item._1, combination.toArray))
      }
    }
    output.toArray
  }

  /**
   * Generate frequent pattern by walking through the FPTree
   */
  private def runFPTree(
      partition: (String, Iterable[Array[String]]),
      minCount: Double): Array[(String, Int)] = {
    val key = partition._1
    val value = partition._2
    val output = ArrayBuffer[(String, Int)]()
    val map = Map[String, Int]()

    // Walk through the FPTree partition to generate all combinations that satisfy
    // the minimal support level.
    var k = 1
    while (k > 0) {
      map.clear()
      val iterator = value.iterator
      while (iterator.hasNext) {
        val pattern = iterator.next
        if (pattern.length >= k) {
          val combination = pattern.toList.combinations(k).toList
          val itemIterator = combination.iterator
          while (itemIterator.hasNext){
            val item = itemIterator.next
            val list2key: List[String] = (item :+ key).sortWith(_ > _)
            val newKey = list2key.mkString(" ")
            if (map.get(newKey) == None) {
              map(newKey) = 1
            } else {
              map(newKey) = map.apply(newKey) + 1
            }
          }
        }
      }
      var eligible: Array[(String, Int)] = null
      if (map.size != 0) {
        val candidate = map.filter(_._2 >= minCount)
        if (candidate.size != 0) {
          eligible = candidate.toArray
          output ++= eligible
        }
      }
      if ((eligible == null) || (eligible.length == 0)) {
        k = 0
      } else {
        k = k + 1
      }
    }
    output.toArray
  }
}

/**
 * Top-level methods for calling FPGrowth.
 */
object FPGrowth{

  /**
   * Generate a FPGrowth Model using the given minimal support level.
   *
   * @param data input baskets stored as `RDD[Array[String]]`
   * @param minSupport minimal support level, for example 0.5
   */
  def train(data: RDD[Array[String]], minSupport: Double): FPGrowthModel = {
    new FPGrowth().setMinSupport(minSupport).run(data)
  }
}

