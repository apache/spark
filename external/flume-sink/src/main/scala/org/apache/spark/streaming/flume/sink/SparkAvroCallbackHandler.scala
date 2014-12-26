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
package org.apache.spark.streaming.flume.sink

import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.flume.Channel
import org.apache.commons.lang.RandomStringUtils
import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
 * Class that implements the SparkFlumeProtocol, that is used by the Avro Netty Server to process
 * requests. Each getEvents, ack and nack call is forwarded to an instance of this class.
 * @param threads Number of threads to use to process requests.
 * @param channel The channel that the sink pulls events from
 * @param transactionTimeout Timeout in millis after which the transaction if not acked by Spark
 *                           is rolled back.
 */
// Flume forces transactions to be thread-local. So each transaction *must* be committed, or
// rolled back from the thread it was originally created in. So each getEvents call from Spark
// creates a TransactionProcessor which runs in a new thread, in which the transaction is created
// and events are pulled off the channel. Once the events are sent to spark,
// that thread is blocked and the TransactionProcessor is saved in a map,
// until an ACK or NACK comes back or the transaction times out (after the specified timeout).
// When the response comes or a timeout is hit, the TransactionProcessor is retrieved and then
// unblocked, at which point the transaction is committed or rolled back.

private[flume] class SparkAvroCallbackHandler(val threads: Int, val channel: Channel,
  val transactionTimeout: Int, val backOffInterval: Int) extends SparkFlumeProtocol with Logging {
  val transactionExecutorOpt = Option(Executors.newFixedThreadPool(threads,
    new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("Spark Sink Processor Thread - %d").build()))
  // Protected by `sequenceNumberToProcessor`
  private val sequenceNumberToProcessor = mutable.HashMap[CharSequence, TransactionProcessor]()
  // This sink will not persist sequence numbers and reuses them if it gets restarted.
  // So it is possible to commit a transaction which may have been meant for the sink before the
  // restart.
  // Since the new txn may not have the same sequence number we must guard against accidentally
  // committing a new transaction. To reduce the probability of that happening a random string is
  // prepended to the sequence number. Does not change for life of sink
  private val seqBase = RandomStringUtils.randomAlphanumeric(8)
  private val seqCounter = new AtomicLong(0)

  // Protected by `sequenceNumberToProcessor`
  private var stopped = false

  @volatile private var isTest = false
  private var testLatch: CountDownLatch = null

  /**
   * Returns a bunch of events to Spark over Avro RPC.
   * @param n Maximum number of events to return in a batch
   * @return [[EventBatch]] instance that has a sequence number and an array of at most n events
   */
  override def getEventBatch(n: Int): EventBatch = {
    logDebug("Got getEventBatch call from Spark.")
    val sequenceNumber = seqBase + seqCounter.incrementAndGet()
    createProcessor(sequenceNumber, n) match {
      case Some(processor) =>
        transactionExecutorOpt.foreach(_.submit(processor))
        // Wait until a batch is available - will be an error if error message is non-empty
        val batch = processor.getEventBatch
        if (SparkSinkUtils.isErrorBatch(batch)) {
          // Remove the processor if it is an error batch since no ACK is sent.
          removeAndGetProcessor(sequenceNumber)
          logWarning("Received an error batch - no events were received from channel! ")
        }
        batch
      case None =>
        new EventBatch("Spark sink has been stopped!", "", java.util.Collections.emptyList())
    }
  }

  private def createProcessor(seq: String, n: Int): Option[TransactionProcessor] = {
    sequenceNumberToProcessor.synchronized {
      if (!stopped) {
        val processor = new TransactionProcessor(
          channel, seq, n, transactionTimeout, backOffInterval, this)
        sequenceNumberToProcessor.put(seq, processor)
        if (isTest) {
          processor.countDownWhenBatchAcked(testLatch)
        }
        Some(processor)
      } else {
        None
      }
    }
  }

  /**
   * Called by Spark to indicate successful commit of a batch
   * @param sequenceNumber The sequence number of the event batch that was successful
   */
  override def ack(sequenceNumber: CharSequence): Void = {
    logDebug("Received Ack for batch with sequence number: " + sequenceNumber)
    completeTransaction(sequenceNumber, success = true)
    null
  }

  /**
   * Called by Spark to indicate failed commit of a batch
   * @param sequenceNumber The sequence number of the event batch that failed
   * @return
   */
  override def nack(sequenceNumber: CharSequence): Void = {
    completeTransaction(sequenceNumber, success = false)
    logInfo("Spark failed to commit transaction. Will reattempt events.")
    null
  }

  /**
   * Helper method to commit or rollback a transaction.
   * @param sequenceNumber The sequence number of the batch that was completed
   * @param success Whether the batch was successful or not.
   */
  private def completeTransaction(sequenceNumber: CharSequence, success: Boolean) {
    removeAndGetProcessor(sequenceNumber).foreach(processor => {
      processor.batchProcessed(success)
    })
  }

  /**
   * Helper method to remove the TxnProcessor for a Sequence Number. Can be used to avoid a leak.
   * @param sequenceNumber
   * @return An `Option` of the transaction processor for the corresponding batch. Note that this
   *         instance is no longer tracked and the caller is responsible for that txn processor.
   */
  private[sink] def removeAndGetProcessor(sequenceNumber: CharSequence):
      Option[TransactionProcessor] = {
    sequenceNumberToProcessor.synchronized {
      sequenceNumberToProcessor.remove(sequenceNumber.toString)
    }
  }

  private[sink] def countDownWhenBatchAcked(latch: CountDownLatch) {
    testLatch = latch
    isTest = true
  }

  /**
   * Shuts down the executor used to process transactions.
   */
  def shutdown() {
    logInfo("Shutting down Spark Avro Callback Handler")
    sequenceNumberToProcessor.synchronized {
      stopped = true
      sequenceNumberToProcessor.values.foreach(_.shutdown())
    }
    transactionExecutorOpt.foreach(_.shutdownNow())
  }
}
