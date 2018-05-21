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

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.NextIterator

/**
 * Messages for the UnsafeRowReceiver endpoint. Either an incoming row or an epoch marker.
 */
private[shuffle] sealed trait UnsafeRowReceiverMessage extends Serializable {
  def writerId: Int
}
private[shuffle] case class ReceiverRow(writerId: Int, row: UnsafeRow)
  extends UnsafeRowReceiverMessage
private[shuffle] case class ReceiverEpochMarker(writerId: Int) extends UnsafeRowReceiverMessage

/**
 * RPC endpoint for receiving rows into a continuous processing shuffle task. Continuous shuffle
 * writers will send rows here, with continuous shuffle readers polling for new rows as needed.
 *
 * TODO: Support multiple source tasks. We need to output a single epoch marker once all
 * source tasks have sent one.
 */
private[shuffle] class UnsafeRowReceiver(
      queueSize: Int,
      numShuffleWriters: Int,
      override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with ContinuousShuffleReader with Logging {
  // Note that this queue will be drained from the main task thread and populated in the RPC
  // response thread.
  private val queues = Array.fill(numShuffleWriters) {
    new ArrayBlockingQueue[UnsafeRowReceiverMessage](queueSize)
  }

  // Exposed for testing to determine if the endpoint gets stopped on task end.
  private[shuffle] val stopped = new AtomicBoolean(false)

  override def onStop(): Unit = {
    stopped.set(true)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case r: UnsafeRowReceiverMessage =>
      queues(r.writerId).put(r)
      context.reply(())
  }

  override def read(): Iterator[UnsafeRow] = {
    new NextIterator[UnsafeRow] {
      private val numWriterEpochMarkers = new AtomicInteger(0)

      private val executor = Executors.newFixedThreadPool(numShuffleWriters)
      private val completion = new ExecutorCompletionService[UnsafeRowReceiverMessage](executor)

      private def completionTask(writerId: Int) = new Callable[UnsafeRowReceiverMessage] {
        override def call(): UnsafeRowReceiverMessage = queues(writerId).take()
      }

      (0 until numShuffleWriters).foreach(writerId => completion.submit(completionTask(writerId)))

      override def getNext(): UnsafeRow = {
        completion.take().get() match {
          case ReceiverRow(writerId, r) =>
            // Start reading the next element in the queue we just took from.
            completion.submit(completionTask(writerId))
            r
            // TODO use writerId
          case ReceiverEpochMarker(writerId) =>
            // Don't read any more from this queue. If all the writers have sent epoch markers,
            // the epoch is over; otherwise we need rows from one of the remaining writers.
            val writersCompleted = numWriterEpochMarkers.incrementAndGet()
            if (writersCompleted == numShuffleWriters) {
              finished = true
              null
            } else {
              getNext()
            }
        }
      }

      override def close(): Unit = {
        executor.shutdownNow()
      }
    }
  }
}
