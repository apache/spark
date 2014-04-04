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

import scala.concurrent.Future

import akka.actor.Actor

import org.apache.spark.{Logging, MapOutputTracker}
import org.apache.spark.storage.BlockManagerMessages._

/**
 * An actor to take commands from the master to execute options. For example,
 * this is used to remove blocks from the slave's BlockManager.
 */
private[storage]
class BlockManagerSlaveActor(
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
  extends Actor with Logging {

  import context.dispatcher

  // Operations that involve removing blocks may be slow and should be done asynchronously
  override def receive = {
    case RemoveBlock(blockId) =>
      val removeBlock = Future { blockManager.removeBlock(blockId) }
      removeBlock.onFailure { case t: Throwable =>
        logError("Error in removing block " + blockId, t)
      }

    case RemoveRdd(rddId) =>
      val removeRdd = Future { sender ! blockManager.removeRdd(rddId) }
      removeRdd.onFailure { case t: Throwable =>
        logError("Error in removing RDD " + rddId, t)
      }

    case RemoveShuffle(shuffleId) =>
      val removeShuffle = Future {
        blockManager.shuffleBlockManager.removeShuffle(shuffleId)
        if (mapOutputTracker != null) {
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
      }
      removeShuffle.onFailure { case t: Throwable =>
        logError("Error in removing shuffle " + shuffleId, t)
      }

    case RemoveBroadcast(broadcastId, tellMaster) =>
      val removeBroadcast = Future { blockManager.removeBroadcast(broadcastId, tellMaster) }
      removeBroadcast.onFailure { case t: Throwable =>
        logError("Error in removing broadcast " + broadcastId, t)
      }

    case GetBlockStatus(blockId, _) =>
      sender ! blockManager.getStatus(blockId)
  }
}
