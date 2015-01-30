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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD



/**
 * This class implements Parallel FP-growth algorithm to do frequent pattern matching on input data.
 * Parallel FPGrowth (PFP) partitions computation in such a way that each machine executes an
 * independent group of mining tasks. More detail of this algorithm can be found at
 * [[http://dx.doi.org/10.1145/1454008.1454027, PFP]], and the original FP-growth paper can be found at
 * [[http://dx.doi.org/10.1145/335191.335372, FP-growth]]
 *
 * @param minSupport the minimal support level of the frequent pattern, any pattern appears more than
 *                   (minSupport * size-of-the-dataset) times will be output
 */
class FPGrowth private(private var minSupport: Double) extends Logging with Serializable {

  /**
   * Constructs a FPGrowth instance with default parameters:
   * {minSupport: 0.3}
   */
  def this() = this(0.3)

  /**
   * set the minimal support level, default is 0.3
   * @param minSupport minimal support level
   */
  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  /**
   * Compute a FPGrowth Model that contains frequent pattern result.
   * @param data input data set, each element contains a transaction
   * @return FPGrowth Model
   */
  def run(data: RDD[Array[String]]): FPGrowthModel = {
    val count = data.count()
    val minCount = minSupport * count
    val single = generateSingleItem(data, minCount)
    val combinations = generateCombinations(data, minCount, single)
    val all = single.map(v => (Array[String](v._1), v._2)).union(combinations)
    new FPGrowthModel(all.collect())
  }

  /**
   * Generate single item pattern by filtering the input data using minimal support level
   * @return array of frequent pattern with its count
   */
  private def generateSingleItem(
      data: RDD[Array[String]],
      minCount: Double): RDD[(String, Long)] = {
    val single = data.flatMap(v => v.toSet)
      .map(v => (v, 1L))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .sortBy(_._2)
    single
  }

  /**
   * Generate combination of frequent pattern by computing on FPTree,
   * the computation is done on each FPTree partitions.
   * @return array of frequent pattern with its count
   */
  private def generateCombinations(
      data: RDD[Array[String]],
      minCount: Double,
      singleItem: RDD[(String, Long)]): RDD[(Array[String], Long)] = {
    val single = data.context.broadcast(singleItem.collect())
    data.flatMap(transaction => createConditionPatternBase(transaction, single))
      .aggregateByKey(new FPTree)(
        (aggregator, condPattBase) => aggregator.add(condPattBase),
        (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
      .flatMap(partition => partition._2.mine(minCount, partition._1))
  }

  /**
   * Create FP-Tree partition for the giving basket
   * @return an array contains a tuple, whose first element is the single
   *         item (hash key) and second element is its condition pattern base
   */
  private def createConditionPatternBase(
      transaction: Array[String],
      singleBC: Broadcast[Array[(String, Long)]]): Array[(String, Array[String])] = {
    var output = ArrayBuffer[(String, Array[String])]()
    var combination = ArrayBuffer[String]()
    var items = ArrayBuffer[(String, Long)]()
    val single = singleBC.value
    val singleMap = single.toMap

    // Filter the basket by single item pattern and sort
    // by single item and its count
    val candidates = transaction
      .filter(singleMap.contains)
      .map(item => (item, singleMap(item)))
      .sortBy(_._1)
      .sortBy(_._2)
      .toArray

    val itemIterator = candidates.iterator
    while (itemIterator.hasNext) {
      combination.clear()
      val item = itemIterator.next()
      val firstNItems = candidates.take(candidates.indexOf(item))
      if (firstNItems.length > 0) {
        val iterator = firstNItems.iterator
        while (iterator.hasNext) {
          val elem = iterator.next()
          combination += elem._1
        }
        output += ((item._1, combination.toArray))
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

