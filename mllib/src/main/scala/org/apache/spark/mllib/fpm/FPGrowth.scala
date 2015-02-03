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

import java.{util => ju}

import scala.collection.mutable

import org.apache.spark.{SparkException, HashPartitioner, Logging, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class FPGrowthModel(val freqItemsets: RDD[(Array[String], Long)]) extends Serializable

/**
 * This class implements Parallel FP-growth algorithm to do frequent pattern matching on input data.
 * Parallel FPGrowth (PFP) partitions computation in such a way that each machine executes an
 * independent group of mining tasks. More detail of this algorithm can be found at
 * [[http://dx.doi.org/10.1145/1454008.1454027, PFP]], and the original FP-growth paper can be
 * found at [[http://dx.doi.org/10.1145/335191.335372, FP-growth]]
 *
 * @param minSupport the minimal support level of the frequent pattern, any pattern appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 * @param numPartitions number of partitions used by parallel FP-growth
 */
class FPGrowth private (
    private var minSupport: Double,
    private var numPartitions: Int) extends Logging with Serializable {

  /**
   * Constructs a FPGrowth instance with default parameters:
   * {minSupport: 0.3, numPartitions: auto}
   */
  def this() = this(0.3, -1)

  /**
   * Sets the minimal support level (default: 0.3).
   */
  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  /**
   * Sets the number of partitions used by parallel FP-growth (default: same as input data).
   */
  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  /**
   * Computes an FP-Growth model that contains frequent itemsets.
   * @param data input data set, each element contains a transaction
   * @return an [[FPGrowthModel]]
   */
  def run(data: RDD[Array[String]]): FPGrowthModel = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    new FPGrowthModel(freqItemsets)
  }

  /**
   * Generates frequent items by filtering the input data using minimal support level.
   * @param minCount minimum count for frequent itemsets
   * @param partitioner partitioner used to distribute items
   * @return array of frequent pattern ordered by their frequencies
   */
  private def genFreqItems(
      data: RDD[Array[String]],
      minCount: Long,
      partitioner: Partitioner): Array[String] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
  }

  /**
   * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
   * @param data transactions
   * @param minCount minimum count for frequent itemsets
   * @param freqItems frequent items
   * @param partitioner partitioner used to distribute transactions
   * @return an RDD of (frequent itemset, count)
   */
  private def genFreqItemsets(
      data: RDD[Array[String]],
      minCount: Long,
      freqItems: Array[String],
      partitioner: Partitioner): RDD[(Array[String], Long)] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
    .flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }.map { case (ranks, count) =>
      (ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  /**
   * Generates conditional transactions.
   * @param transaction a transaction
   * @param itemToRank map from item to their rank
   * @param partitioner partitioner used to distribute transactions
   * @return a map of (target partition, conditional transaction)
   */
  private def genCondTransactions(
      transaction: Array[String],
      itemToRank: Map[String, Int],
      partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}
