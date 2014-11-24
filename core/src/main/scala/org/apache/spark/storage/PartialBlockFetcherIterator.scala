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

package org.apache.spark.storage

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.util.Try

import org.apache.spark.{SparkEnv, MapOutputTracker, Logging, TaskContext}
import org.apache.spark.network.shuffle. ShuffleClient
import org.apache.spark.serializer.Serializer

private[spark]
class PartialBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    var statuses: Array[(BlockManagerId, Long)],
    serializer: Serializer,
    shuffleId: Int,
    reduceId: Int)
  extends Iterator[(BlockId, Try[Iterator[Any]])] with Logging {

  private val mapOutputFetchInterval = SparkEnv.get.conf.getInt("spark.reducer.mapOutput.fetchInterval", 1000)

  private var iterator:Iterator[(BlockId, Try[Iterator[Any]])] = null

  // Track the map outputs we've delegated
  private val delegatedStatuses = new HashSet[Int]()

  private var fetchTime:Int = 1

  initialize()

  // Get the updated map output
  private def updateStatuses() {
    fetchTime += 1
    logDebug("Still missing " + statuses.filter(_ == null).size +
      " map outputs for reduce " + reduceId + " of shuffle " + shuffleId +" next fetchTime="+ fetchTime)
    val update = SparkEnv.get.mapOutputTracker.getUpdatedStatus(shuffleId, reduceId)
    statuses = update
  }

  private def readyStatuses = (0 until statuses.size).filter(statuses(_) != null)

  // Check if there's new map outputs available
  private def newStatusesReady = readyStatuses.exists(!delegatedStatuses.contains(_))

  private def getIterator() = {
    while (!newStatusesReady) {
      Thread.sleep(mapOutputFetchInterval)
      updateStatuses()
    }
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (index <- readyStatuses if !delegatedStatuses.contains(index)) {
      splitsByAddress.getOrElseUpdate(statuses(index)._1, ArrayBuffer()) += ((index, statuses(index)._2))
      delegatedStatuses += index
    }
    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(shuffleId, s._1, reduceId), s._2)))
    }
    logDebug("Delegating " + blocksByAddress.map(_._2.size).sum +
      " blocks to a new iterator for reduce " + reduceId + " of shuffle " + shuffleId)
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.shuffleClient,
      blockManager,
      blocksByAddress,
      serializer,
      SparkEnv.get.conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)
    blockFetcherItr
  }

  private[this] def initialize(){
    iterator = getIterator()
  }

  override def hasNext: Boolean = {
    // Firstly see if the delegated iterators have more blocks for us
    if (iterator.hasNext) {
      return true
    }
    // If we have blocks not delegated yet, try to delegate them to a new iterator
    // and depend on the iterator to tell us if there are valid blocks.
    while (delegatedStatuses.size < statuses.size) {
      iterator = getIterator()
      if (iterator.hasNext) {
        return true
      }
    }
    false
  }

  override def next(): (BlockId, Try[Iterator[Any]]) = {
    return iterator.next()
  }
}
