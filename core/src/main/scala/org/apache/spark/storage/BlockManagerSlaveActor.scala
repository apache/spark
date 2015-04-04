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

import akka.actor.{ActorRef, Actor}

import org.apache.spark.{Logging, MapOutputTracker, SparkEnv}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.ActorLogReceive

/**
 * An actor to take commands from the master to execute options. For example,
 * this is used to remove blocks from the slave's BlockManager.
 */
private[storage]
class BlockManagerSlaveActor(
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
  extends Actor with ActorLogReceive with Logging {

  import context.dispatcher

  // Operations that involve removing blocks may be slow and should be done asynchronously
  override def receiveWithLogging: PartialFunction[Any, Unit] = {
    case RemoveBlock(blockId) =>
      doAsync[Boolean]("removing block " + blockId, sender) {
        blockManager.removeBlock(blockId)
        true
      }

    case RemoveRdd(rddId) =>
      doAsync[Int]("removing RDD " + rddId, sender) {
        blockManager.removeRdd(rddId)
      }

    case RemoveShuffle(shuffleId) =>
      doAsync[Boolean]("removing shuffle " + shuffleId, sender) {
        if (mapOutputTracker != null) {
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
        SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
      }

    case RemoveBroadcast(broadcastId, _) =>
      doAsync[Int]("removing broadcast " + broadcastId, sender) {
        blockManager.removeBroadcast(broadcastId, tellMaster = true)
      }

    case GetBlockStatus(blockId, _) =>
      sender ! blockManager.getStatus(blockId)

    case GetMatchingBlockIds(filter, _) =>
      sender ! blockManager.getMatchingBlockIds(filter)
  }

  private def doAsync[T](actionMessage: String, responseActor: ActorRef)(body: => T) {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onSuccess { case response =>
      logDebug("Done " + actionMessage + ", response is " + response)
      responseActor ! response
      logDebug("Sent response: " + response + " to " + responseActor)
    }
    future.onFailure { case t: Throwable =>
      logError("Error in " + actionMessage, t)
      responseActor ! null.asInstanceOf[T]
    }
  }
}
