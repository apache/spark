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

package org.apache.spark.shuffle.hash

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.util.collection.ExternalSorter

private[spark] class HashShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C]
{
  require(endPartition == startPartition + 1,
    "Hash shuffle currently only supports fetching one partition")

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val ser = Serializer.getSerializer(dep.serializer)
    val iter = BlockStoreShuffleFetcher.fetch(handle.shuffleId, startPartition, context, ser)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        new InterruptibleIterator(context, dep.aggregator.get.combineCombinersByKey(iter, context))
      } else {
        new InterruptibleIterator(context, dep.aggregator.get.combineValuesByKey(iter, context))
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")

      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter = new ExternalSorter[K, C, C](ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics.memoryBytesSpilled += sorter.memoryBytesSpilled
        context.taskMetrics.diskBytesSpilled += sorter.diskBytesSpilled
        sorter.iterator
      case None =>
        aggregatedIter
    }
  }
}
