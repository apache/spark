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

package org.apache.spark

import scala.collection.mutable

import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.CompletionIterator

private[spark] class BlockStoreSortMergeShuffleFetcher
  extends SortMergeShuffleFetcher with Logging {

  def fetch[K: Ordering](
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      serializer: Serializer)
    : Iterator[Product2[K, Any]] =
  {

    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    val splitsByAddress = new mutable.HashMap[BlockManagerId, mutable.ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, mutable.ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(shuffleId, s._1, reduceId), s._2)))
    }

    def unpackBlock(blockPair: (BlockId, Option[Iterator[Any]])): Iterator[Product2[K, Any]] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Some(block) => {
          block.asInstanceOf[Iterator[Product2[K, Any]]]
        }
        case None => {
          blockId match {
            case ShuffleBlockId(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, null)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }

    val blockFetcherItr = blockManager.getMultiple(blocksByAddress, serializer)
    val itrs = blockFetcherItr.map(unpackBlock).toArray.map(_.buffered)
    val itr = Iterator.continually {
      ((None: Option[K]) /: itrs) {
          case (opt, itr) if (itr.hasNext) =>
            opt.map(key => implicitly[Ordering[K]].min(key, itr.head._1)).orElse(Some(itr.head._1))
          case (opt, _) => opt
        }
    }.takeWhile(_.isDefined).map(_.get).flatMap { key =>
      itrs.flatMap { itr =>
        Iterator.continually {
          if (itr.hasNext && implicitly[Ordering[K]].equiv(key, itr.head._1)) {
            Some(itr.next)
          } else None
        }.takeWhile(_.isDefined).map(_.get)
      }
    }

    val completionIter = CompletionIterator[Product2[K, Any], Iterator[Product2[K, Any]]](itr, {
      val shuffleMetrics = new ShuffleReadMetrics
      shuffleMetrics.shuffleFinishTime = System.currentTimeMillis
      shuffleMetrics.fetchWaitTime = blockFetcherItr.fetchWaitTime
      shuffleMetrics.remoteBytesRead = blockFetcherItr.remoteBytesRead
      shuffleMetrics.totalBlocksFetched = blockFetcherItr.totalBlocks
      shuffleMetrics.localBlocksFetched = blockFetcherItr.numLocalBlocks
      shuffleMetrics.remoteBlocksFetched = blockFetcherItr.numRemoteBlocks
      context.taskMetrics.shuffleReadMetrics = Some(shuffleMetrics)
    })

    new InterruptibleIterator[Product2[K, Any]](context, completionIter)
  }
}
