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

package org.apache.spark.barrier

import java.util.{Timer, TimerTask}

import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}

class BarrierCoordinator(
    numTasks: Int,
    timeout: Long,
    override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

  private var epoch = 0

  private val timer = new Timer("BarrierCoordinator epoch increment timer")

  private val syncRequests = new scala.collection.mutable.ArrayBuffer[RpcCallContext](numTasks)

  private def replyIfGetAllSyncRequest(): Unit = {
    if (syncRequests.length == numTasks) {
      syncRequests.foreach(_.reply(()))
      syncRequests.clear()
      epoch += 1
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case IncreaseEpoch(previousEpoch) =>
      if (previousEpoch == epoch) {
        syncRequests.foreach(_.sendFailure(new RuntimeException(
          s"The coordinator cannot get all barrier sync requests within $timeout ms.")))
        syncRequests.clear()
        epoch += 1
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestToSync(epoch) =>
      if (epoch == this.epoch) {
        if (syncRequests.isEmpty) {
          val currentEpoch = epoch
          timer.schedule(new TimerTask {
            override def run(): Unit = {
              // self can be null after this RPC endpoint is stopped.
              if (self != null) self.send(IncreaseEpoch(currentEpoch))
            }
          }, timeout)
        }

        syncRequests += context
        replyIfGetAllSyncRequest()
      }
  }

  override def onStop(): Unit = timer.cancel()
}

private[barrier] sealed trait BarrierCoordinatorMessage extends Serializable

private[barrier] case class RequestToSync(epoch: Int) extends BarrierCoordinatorMessage

private[barrier] case class IncreaseEpoch(previousEpoch: Int) extends BarrierCoordinatorMessage
