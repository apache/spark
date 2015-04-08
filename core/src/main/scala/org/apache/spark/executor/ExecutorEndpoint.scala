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

package org.apache.spark.executor

import org.apache.spark.rpc.{RpcEnv, RpcCallContext, RpcEndpoint}
import org.apache.spark.util.Utils

/**
 * Driver -> Executor message to trigger a thread dump.
 */
private[spark] case object TriggerThreadDump

/**
 * [[RpcEndpoint]] that runs inside of executors to enable driver -> executor RPC.
 */
private[spark]
class ExecutorEndpoint(override val rpcEnv: RpcEnv, executorId: String) extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case TriggerThreadDump =>
      context.reply(Utils.getThreadDump())
  }

}

object ExecutorEndpoint {
  val EXECUTOR_ENDPOINT_NAME = "ExecutorEndpoint"
}
