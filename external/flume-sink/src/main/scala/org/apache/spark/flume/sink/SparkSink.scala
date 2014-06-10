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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.Breaks

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.avro.ipc.NettyServer
import org.apache.avro.ipc.specific.SpecificResponder
import org.apache.commons.lang.RandomStringUtils
import org.apache.flume.Sink.Status
import org.apache.flume.conf.{ConfigurationException, Configurable}
import org.apache.flume.sink.AbstractSink
import org.apache.flume.{Channel, Transaction, FlumeException, Context}
import org.slf4j.LoggerFactory

import org.apache.spark.flume.{SparkSinkEvent, EventBatch, SparkFlumeProtocol}

/**
 * A sink that uses Avro RPC to run a server that can be polled by Spark's
 * FlumePollingInputDStream. This sink has the following configuration parameters:
 *
 * hostname - The hostname to bind to. Default: 0.0.0.0
 * port - The port to bind to. (No default - mandatory)
 * timeout - Time in seconds after which a transaction is rolled back,
 * if an ACK is not received from Spark within that time
 * threads - Number of threads to use to receive requests from Spark (Default: 10)
 *
 */
// Flume forces transactions to be thread-local. So each transaction *must* be committed, or
// rolled back from the thread it was originally created in. So each getEvents call from Spark
// creates a TransactionProcessor which runs in a new thread, in which the transaction is created
// and events are pulled off the channel. Once the events are sent to spark,
// that thread is blocked and the TransactionProcessor is saved in a map,
// until an ACK or NACK comes back or the transaction times out (after the specified timeout).
// When the response comes, the TransactionProcessor is retrieved and then unblocked,
// at which point the transaction is committed or rolled back.
class SparkSink extends AbstractSink with Configurable {

  // Size of the pool to use for holding transaction processors.
  private var poolSize: Integer = SparkSinkConfig.DEFAULT_THREADS

  // Timeout for each transaction. If spark does not respond in this much time,
  // rollback the transaction
  private var transactionTimeout = SparkSinkConfig.DEFAULT_TRANSACTION_TIMEOUT

  // Address info to bind on
  private var hostname: String = SparkSinkConfig.DEFAULT_HOSTNAME
  private var port: Int = 0

  // Handle to the server
  private var serverOpt: Option[NettyServer] = None

  // The handler that handles the callback from Avro
  private var handler: Option[SparkAvroCallbackHandler] = None

  // Latch that blocks off the Flume framework from wasting 1 thread.
  private val blockingLatch = new CountDownLatch(1)

  override def start() {
    handler = Option(new SparkAvroCallbackHandler(poolSize, getChannel, transactionTimeout))
    val responder = new SpecificResponder(classOf[SparkFlumeProtocol], handler.get)
    // Using the constructor that takes specific thread-pools requires bringing in netty
    // dependencies which are being excluded in the build. In practice,
    // Netty dependencies are already available on the JVM as Flume would have pulled them in.
    serverOpt = Option(new NettyServer(responder, new InetSocketAddress(hostname, port)))
    serverOpt.map(server => {
      server.start()
    })
    super.start()
  }

  override def stop() {
    handler.map(callbackHandler => {
      callbackHandler.shutdown()
    })
    serverOpt.map(server => {
      server.close()
      server.join()
    })
    blockingLatch.countDown()
    super.stop()
  }

  /**
   * @param ctx
   */
  override def configure(ctx: Context) {
    import SparkSinkConfig._
    hostname = ctx.getString(CONF_HOSTNAME, DEFAULT_HOSTNAME)
    port = Option(ctx.getInteger(CONF_PORT)).
      getOrElse(throw new ConfigurationException("The port to bind to must be specified"))
    poolSize = ctx.getInteger(THREADS, DEFAULT_THREADS)
    transactionTimeout = ctx.getInteger(CONF_TRANSACTION_TIMEOUT, DEFAULT_TRANSACTION_TIMEOUT)
  }

  override def process(): Status = {
    // This method is called in a loop by the Flume framework - block it until the sink is
    // stopped to save CPU resources. The sink runner will interrupt this thread when the sink is
    // being shut down.
    blockingLatch.await()
    Status.BACKOFF
  }
}

/**
 * Class that implements the SparkFlumeProtocol, that is used by the Avro Netty Server to process
 * requests. Each getEvents, ack and nack call is forwarded to an instance of this class.
 * @param threads Number of threads to use to process requests.
 * @param channel The channel that the sink pulls events from
 * @param transactionTimeout Timeout in millis after which the transaction if not acked by Spark
 *                           is rolled back.
 */
private class SparkAvroCallbackHandler(val threads: Int, val channel: Channel,
  val transactionTimeout: Int) extends SparkFlumeProtocol {
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
      n, transactionTimeout, this)
    transactionExecutorOpt.map(executor => {
      executor.submit(processor)
    })
    // Wait until a batch is available - can be null if some error was thrown
    processor.getEventBatch match {
      case ErrorEventBatch => throw new FlumeException("Something went wrong. No events" +
        " retrieved from channel.")
      case eventBatch: EventBatch =>
        processorMap.put(sequenceNumber, processor)
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sent " + eventBatch.getEvents.size() +
            " events with sequence number: " + eventBatch.getSequenceNumber)
        }
        eventBatch
    }
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

/**
 * Object representing an empty batch returned by the txn processor due to some error.
 */
case object ErrorEventBatch extends EventBatch

// Flume forces transactions to be thread-local (horrible, I know!)
// So the sink basically spawns a new thread to pull the events out within a transaction.
// The thread fills in the event batch object that is set before the thread is scheduled.
// After filling it in, the thread waits on a condition - which is released only
// when the success message comes back for the specific sequence number for that event batch.
/**
 * This class represents a transaction on the Flume channel. This class runs a separate thread
 * which owns the transaction. The thread is blocked until the success call for that transaction
 * comes back with an ACK or NACK.
 * @param channel The channel from which to pull events
 * @param seqNum The sequence number to use for the transaction. Must be unique
 * @param maxBatchSize The maximum number of events to process per batch
 * @param transactionTimeout Time in seconds after which a transaction must be rolled back
 *                           without waiting for an ACK from Spark
 * @param parent The parent [[SparkAvroCallbackHandler]] instance, for reporting timeouts
 */
private class TransactionProcessor(val channel: Channel, val seqNum: String,
  var maxBatchSize: Int, val transactionTimeout: Int,
  val parent: SparkAvroCallbackHandler) extends Callable[Void] {

  private val LOG = LoggerFactory.getLogger(classOf[TransactionProcessor])

  // If a real batch is not returned, we always have to return an error batch.
  @volatile private var eventBatch: EventBatch = ErrorEventBatch

  // Synchronization primitives
  val batchGeneratedLatch = new CountDownLatch(1)
  val batchAckLatch = new CountDownLatch(1)

  // Sanity check to ensure we don't loop like crazy
  val totalAttemptsToRemoveFromChannel = Int.MaxValue / 2

  // OK to use volatile, since the change would only make this true (otherwise it will be
  // changed to false - we never apply a negation operation to this) - which means the transaction
  // succeeded.
  @volatile private var batchSuccess = false

  // The transaction that this processor would handle
  var txOpt: Option[Transaction] = None

  /**
   * Get an event batch from the channel. This method will block until a batch of events is
   * available from the channel. If no events are available after a large number of attempts of
   * polling the channel, this method will return [[ErrorEventBatch]].
   *
   * @return An [[EventBatch]] instance with sequence number set to [[seqNum]], filled with a
   *         maximum of [[maxBatchSize]] events
   */
  def getEventBatch: EventBatch = {
    batchGeneratedLatch.await()
    eventBatch
  }

  /**
   * This method is to be called by the sink when it receives an ACK or NACK from Spark. This
   * method is a no-op if it is called after [[transactionTimeout]] has expired since
   * [[getEventBatch]] returned a batch of events.
   * @param success True if an ACK was received and the transaction should be committed, else false.
   */
  def batchProcessed(success: Boolean) {
    if (LOG.isDebugEnabled) {
      LOG.debug("Batch processed for sequence number: " + seqNum)
    }
    batchSuccess = success
    batchAckLatch.countDown()
  }

  /**
   * Populates events into the event batch. If the batch cannot be populated,
   * this method will not set the event batch which will stay [[ErrorEventBatch]]
   */
  private def populateEvents() {
    try {
      txOpt = Option(channel.getTransaction)
      txOpt.map(tx => {
        tx.begin()
        val events = new util.ArrayList[SparkSinkEvent](maxBatchSize)
        val loop = new Breaks
        var gotEventsInThisTxn = false
        var loopCounter: Int = 0
        loop.breakable {
          while (events.size() < maxBatchSize
            && loopCounter < totalAttemptsToRemoveFromChannel) {
            loopCounter += 1
            Option(channel.take()) match {
              case Some(event) =>
                events.add(new SparkSinkEvent(toCharSequenceMap(event.getHeaders),
                  ByteBuffer.wrap(event.getBody)))
                gotEventsInThisTxn = true
              case None =>
                if (!gotEventsInThisTxn) {
                  TimeUnit.MILLISECONDS.sleep(500)
                } else {
                  loop.break()
                }
            }
          }
        }
        if (!gotEventsInThisTxn) {
          throw new FlumeException("Tried too many times, didn't get any events from the channel")
        }
        // At this point, the events are available, so fill them into the event batch
        eventBatch = new EventBatch(seqNum, events)
      })
    } catch {
      case e: Throwable =>
        LOG.error("Error while processing transaction.", e)
        try {
          txOpt.map(tx => {
            rollbackAndClose(tx, close = true)
          })
        } finally {
          // Avro might serialize the exception and cause a NACK,
          // so don't bother with the transaction
          txOpt = None
        }
    } finally {
      batchGeneratedLatch.countDown()
    }
  }

  /**
   * Waits for upto [[transactionTimeout]] seconds for an ACK. If an ACK comes in,
   * this method commits the transaction with the channel. If the ACK does not come in within
   * that time or a NACK comes in, this method rolls back the transaction.
   */
  private def processAckOrNack() {
    batchAckLatch.await(transactionTimeout, TimeUnit.SECONDS)
    txOpt.map(tx => {
      if (batchSuccess) {
        try {
          tx.commit()
        } catch {
          case e: Throwable =>
            rollbackAndClose(tx, close = false) // tx will be closed later anyway
        } finally {
          tx.close()
        }
      } else {
        rollbackAndClose(tx, close = true)
        // This might have been due to timeout or a NACK. Either way the following call does not
        // cause issues. This is required to ensure the TransactionProcessor instance is not leaked
        parent.removeAndGetProcessor(seqNum)
      }
    })
  }

  /**
   * Helper method to rollback and optionally close a transaction
   * @param tx The transaction to rollback
   * @param close Whether the transaction should be closed or not after rolling back
   */
  private def rollbackAndClose(tx: Transaction, close: Boolean) {
    try {
      tx.rollback()
      LOG.warn("Spark was unable to successfully process the events. Transaction is being " +
        "rolled back.")
    } catch {
      case e: Throwable =>
        LOG.error("Error rolling back transaction. Rollback may have failed!", e)
    } finally {
      if (close) {
        tx.close()
      }
    }
  }

  /**
   * Helper method to convert a Map[String, String] to Map[CharSequence, CharSequence]
   * @param inMap The map to be converted
   * @return The converted map
   */
  private def toCharSequenceMap(inMap: java.util.Map[String, String]): java.util.Map[CharSequence,
    CharSequence] = {
    val charSeqMap = new util.HashMap[CharSequence, CharSequence](inMap.size())
    charSeqMap.putAll(inMap)
    charSeqMap
  }

  override def call(): Void = {
    populateEvents()
    processAckOrNack()
    null
  }
}

/**
 * Configuration parameters and their defaults.
 */
object SparkSinkConfig {
  val THREADS = "threads"
  val DEFAULT_THREADS = 10

  val CONF_TRANSACTION_TIMEOUT = "timeout"
  val DEFAULT_TRANSACTION_TIMEOUT = 60

  val CONF_HOSTNAME = "hostname"
  val DEFAULT_HOSTNAME = "0.0.0.0"

  val CONF_PORT = "port"
}
