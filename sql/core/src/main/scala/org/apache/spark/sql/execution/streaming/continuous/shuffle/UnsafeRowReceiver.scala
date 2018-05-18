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

package org.apache.spark.sql.execution.streaming.continuous.shuffle

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * Messages for the UnsafeRowReceiver endpoint. Either an incoming row or an epoch marker.
 */
private[shuffle] sealed trait UnsafeRowReceiverMessage extends Serializable
private[shuffle] case class ReceiverRow(row: UnsafeRow) extends UnsafeRowReceiverMessage
private[shuffle] case class ReceiverEpochMarker() extends UnsafeRowReceiverMessage

/**
 * RPC endpoint for receiving rows into a continuous processing shuffle task. Continuous shuffle
 * writers will send rows here, with continuous shuffle readers polling for new rows as needed.
 *
 * TODO: Support multiple source tasks. We need to output a single epoch marker once all
 * source tasks have sent one.
 */
private[shuffle] class UnsafeRowReceiver(
      queueSize: Int,
      override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {
  // Note that this queue will be drained from the main task thread and populated in the RPC
  // response thread.
  private val queue = new ArrayBlockingQueue[UnsafeRowReceiverMessage](queueSize)

  // Exposed for testing to determine if the endpoint gets stopped on task end.
  private[shuffle] val stopped = new AtomicBoolean(false)

  override def onStop(): Unit = {
    stopped.set(true)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case r: UnsafeRowReceiverMessage =>
      queue.put(r)
      context.reply(())
  }

  /**
   * Take the next row, blocking until it's ready.
   */
  def take(): UnsafeRowReceiverMessage = queue.take()
}
