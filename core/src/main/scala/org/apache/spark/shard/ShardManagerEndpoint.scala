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

package org.apache.spark.shard

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{SHARD_ID, SHARD_SET_ID}
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEnv}
import org.apache.spark.shard.ShardManagerMessages.InstallReplica
import org.apache.spark.util.ThreadUtils

private[spark] class ShardManagerEndpoint(val rpcEnv: RpcEnv, shardManager: ShardManager)
    extends IsolatedThreadSafeRpcEndpoint
    with Logging {

  private implicit val asyncEc: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("shard-manager-async-thread-pool", 8))

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case InstallReplica(setId, shardId) =>
      doAsync[Boolean](
        log"install replica (${MDC(SHARD_SET_ID, setId)}, ${MDC(SHARD_ID, shardId)})",
        context) {
        shardManager.installReplica(setId, shardId)
        true
      }
  }

  private def doAsync[T](
      actionMessage: MessageWithContext,
      context: RpcCallContext)(body: => T): Unit = {
    val future = Future {
      logDebug(actionMessage.message)
      body
    }
    future.onComplete {
      case scala.util.Success(response) =>
        logDebug(s"Done ${actionMessage.message}, response is $response")
        context.reply(response)
      case scala.util.Failure(t) =>
        logError(log"Error in " + actionMessage, t)
        context.sendFailure(t)
    }
  }

  override def onStop(): Unit = {
    asyncEc.shutdown()
  }

}
