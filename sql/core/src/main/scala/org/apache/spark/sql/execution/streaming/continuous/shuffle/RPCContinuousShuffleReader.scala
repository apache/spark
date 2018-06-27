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
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.NextIterator

/**
 * Messages for the RPCContinuousShuffleReader endpoint. Either an incoming row or an epoch marker.
 *
 * Each message comes tagged with writerId, identifying which writer the message is coming
 * from. The receiver will only begin the next epoch once all writers have sent an epoch
 * marker ending the current epoch.
 */
private[shuffle] sealed trait RPCContinuousShuffleMessage extends Serializable {
  def writerId: Int
}
private[shuffle] case class ReceiverRow(writerId: Int, row: UnsafeRow)
  extends RPCContinuousShuffleMessage
private[shuffle] case class ReceiverEpochMarker(writerId: Int) extends RPCContinuousShuffleMessage

/**
 * RPC endpoint for receiving rows into a continuous processing shuffle task. Continuous shuffle
 * writers will send rows here, with continuous shuffle readers polling for new rows as needed.
 *
 * TODO: Support multiple source tasks. We need to output a single epoch marker once all
 * source tasks have sent one.
 */
private[continuous] class RPCContinuousShuffleReader(
      queueSize: Int,
      numShuffleWriters: Int,
      epochIntervalMs: Long,
      override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with ContinuousShuffleReader with Logging {
  // Note that this queue will be drained from the main task thread and populated in the RPC
  // response thread.
  private val queues = Array.fill(numShuffleWriters) {
    new ArrayBlockingQueue[RPCContinuousShuffleMessage](queueSize)
  }

  // Exposed for testing to determine if the endpoint gets stopped on task end.
  private[shuffle] val stopped = new AtomicBoolean(false)

  override def onStop(): Unit = {
    stopped.set(true)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case r: RPCContinuousShuffleMessage =>
      // Note that this will block a thread the shared RPC handler pool!
      // The TCP based shuffle handler (SPARK-24541) will avoid this problem.
      queues(r.writerId).put(r)
      context.reply(())
  }

  override def read(): Iterator[UnsafeRow] = {
    new NextIterator[UnsafeRow] {
      // An array of flags for whether each writer ID has gotten an epoch marker.
      private val writerEpochMarkersReceived = Array.fill(numShuffleWriters)(false)

      private val executor = Executors.newFixedThreadPool(numShuffleWriters)
      private val completion = new ExecutorCompletionService[RPCContinuousShuffleMessage](executor)

      private def completionTask(writerId: Int) = new Callable[RPCContinuousShuffleMessage] {
        override def call(): RPCContinuousShuffleMessage = queues(writerId).take()
      }

      // Initialize by submitting tasks to read the first row from each writer.
      (0 until numShuffleWriters).foreach(writerId => completion.submit(completionTask(writerId)))

      /**
       * In each call to getNext(), we pull the next row available in the completion queue, and then
       * submit another task to read the next row from the writer which returned it.
       *
       * When a writer sends an epoch marker, we note that it's finished and don't submit another
       * task for it in this epoch. The iterator is over once all writers have sent an epoch marker.
       */
      override def getNext(): UnsafeRow = {
        var nextRow: UnsafeRow = null
        while (!finished && nextRow == null) {
          completion.poll(epochIntervalMs, TimeUnit.MILLISECONDS) match {
            case null =>
              // Try again if the poll didn't wait long enough to get a real result.
              // But we should be getting at least an epoch marker every checkpoint interval.
              val writerIdsUncommitted = writerEpochMarkersReceived.zipWithIndex.collect {
                case (flag, idx) if !flag => idx
              }
              logWarning(
                s"Completion service failed to make progress after $epochIntervalMs ms. Waiting " +
                  s"for writers ${writerIdsUncommitted.mkString(",")} to send epoch markers.")

            // The completion service guarantees this future will be available immediately.
            case future => future.get() match {
              case ReceiverRow(writerId, r) =>
                // Start reading the next element in the queue we just took from.
                completion.submit(completionTask(writerId))
                nextRow = r
              case ReceiverEpochMarker(writerId) =>
                // Don't read any more from this queue. If all the writers have sent epoch markers,
                // the epoch is over; otherwise we need to loop again to poll from the remaining
                // writers.
                writerEpochMarkersReceived(writerId) = true
                if (writerEpochMarkersReceived.forall(_ == true)) {
                  finished = true
                }
            }
          }
        }

        nextRow
      }

      override def close(): Unit = {
        executor.shutdownNow()
      }
    }
  }
}
