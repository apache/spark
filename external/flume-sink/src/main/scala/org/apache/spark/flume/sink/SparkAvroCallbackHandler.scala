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
package org.apache.spark.flume.sink

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.commons.lang.RandomStringUtils
import org.apache.flume.Channel
import org.apache.spark.flume.{EventBatch, SparkFlumeProtocol}
import org.slf4j.LoggerFactory

/**
 * Class that implements the SparkFlumeProtocol, that is used by the Avro Netty Server to process
 * requests. Each getEvents, ack and nack call is forwarded to an instance of this class.
 * @param threads Number of threads to use to process requests.
 * @param channel The channel that the sink pulls events from
 * @param transactionTimeout Timeout in millis after which the transaction if not acked by Spark
 *                           is rolled back.
 */
private class SparkAvroCallbackHandler(val threads: Int, val channel: Channel,
  val transactionTimeout: Int, val backOffInterval: Int) extends SparkFlumeProtocol {
  private val LOG = LoggerFactory.getLogger(classOf[SparkAvroCallbackHandler])
  val transactionExecutorOpt = Option(Executors.newFixedThreadPool(threads,
    new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("Spark Sink Processor Thread - %d").build()))
  private val processorMap = new ConcurrentHashMap[CharSequence, TransactionProcessor]()
  // This sink will not persist sequence numbers and reuses them if it gets restarted.
  // So it is possible to commit a transaction which may have been meant for the sink before the
  // restart.
  // Since the new txn may not have the same sequence number we must guard against accidentally
  // committing a new transaction. To reduce the probability of that happening a random string is
  // prepended to the sequence number. Does not change for life of sink
  private val seqBase = RandomStringUtils.randomAlphanumeric(8)
  private val seqCounter = new AtomicLong(0)

  /**
   * Returns a bunch of events to Spark over Avro RPC.
   * @param n Maximum number of events to return in a batch
   * @return [[EventBatch]] instance that has a sequence number and an array of at most n events
   */
  override def getEventBatch(n: Int): EventBatch = {
    val sequenceNumber = seqBase + seqCounter.incrementAndGet()
    val processor = new TransactionProcessor(channel, sequenceNumber,
      n, transactionTimeout, backOffInterval, this)
    transactionExecutorOpt.map(executor => {
      executor.submit(processor)
    })
    // Wait until a batch is available - will be an error if
    processor.getEventBatch
  }

  /**
   * Called by Spark to indicate successful commit of a batch
   * @param sequenceNumber The sequence number of the event batch that was successful
   */
  override def ack(sequenceNumber: CharSequence): Void = {
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
    LOG.info("Spark failed to commit transaction. Will reattempt events.")
    null
  }

  /**
   * Helper method to commit or rollback a transaction.
   * @param sequenceNumber The sequence number of the batch that was completed
   * @param success Whether the batch was successful or not.
   */
  private def completeTransaction(sequenceNumber: CharSequence, success: Boolean) {
    Option(removeAndGetProcessor(sequenceNumber)).map(processor => {
      processor.batchProcessed(success)
    })
  }

  /**
   * Helper method to remove the TxnProcessor for a Sequence Number. Can be used to avoid a leak.
   * @param sequenceNumber
   * @return The transaction processor for the corresponding batch. Note that this instance is no
   *         longer tracked and the caller is responsible for that txn processor.
   */
  private[flume] def removeAndGetProcessor(sequenceNumber: CharSequence): TransactionProcessor = {
    processorMap.remove(sequenceNumber.toString) // The toString is required!
  }

  /**
   * Shuts down the executor used to process transactions.
   */
  def shutdown() {
    transactionExecutorOpt.map(executor => {
      executor.shutdownNow()
    })
  }
}
