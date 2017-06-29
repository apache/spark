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
package org.apache.spark.streaming.kinesis

import java.util.List

import scala.util.Random
import scala.util.control.NonFatal

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, KinesisClientLibDependencyException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.internal.Logging

/**
 * Kinesis-specific implementation of the Kinesis Client Library (KCL) IRecordProcessor.
 * This implementation operates on the Array[Byte] from the KinesisReceiver.
 * The Kinesis Worker creates an instance of this KinesisRecordProcessor for each
 * shard in the Kinesis stream upon startup.  This is normally done in separate threads,
 * but the KCLs within the KinesisReceivers will balance themselves out if you create
 * multiple Receivers.
 *
 * @param receiver Kinesis receiver
 * @param workerId for logging purposes
 */
private[kinesis] class KinesisRecordProcessor[T](receiver: KinesisReceiver[T], workerId: String)
  extends IRecordProcessor with Logging {

  // shardId populated during initialize()
  @volatile
  private var shardId: String = _

  /**
   * The Kinesis Client Library calls this method during IRecordProcessor initialization.
   *
   * @param shardId assigned by the KCL to this particular RecordProcessor.
   */
  override def initialize(shardId: String) {
    this.shardId = shardId
    logInfo(s"Initialized workerId $workerId with shardId $shardId")
  }

  /**
   * This method is called by the KCL when a batch of records is pulled from the Kinesis stream.
   * This is the record-processing bridge between the KCL's IRecordProcessor.processRecords()
   * and Spark Streaming's Receiver.store().
   *
   * @param batch list of records from the Kinesis stream shard
   * @param checkpointer used to update Kinesis when this batch has been processed/stored
   *   in the DStream
   */
  override def processRecords(batch: List[Record], checkpointer: IRecordProcessorCheckpointer) {
    if (!receiver.isStopped()) {
      try {
        // Limit the number of processed records from Kinesis stream. This is because the KCL cannot
        // control the number of aggregated records to be fetched even if we set `MaxRecords`
        // in `KinesisClientLibConfiguration`. For example, if we set 10 to the number of max
        // records in a worker and a producer aggregates two records into one message, the worker
        // possibly 20 records every callback function called.
        val maxRecords = receiver.getCurrentLimit
        for (start <- 0 until batch.size by maxRecords) {
          val miniBatch = batch.subList(start, math.min(start + maxRecords, batch.size))
          receiver.addRecords(shardId, miniBatch)
          logDebug(s"Stored: Worker $workerId stored ${miniBatch.size} records " +
            s"for shardId $shardId")
        }
        receiver.setCheckpointer(shardId, checkpointer)
      } catch {
        case NonFatal(e) =>
          /*
           *  If there is a failure within the batch, the batch will not be checkpointed.
           *  This will potentially cause records since the last checkpoint to be processed
           *     more than once.
           */
          logError(s"Exception:  WorkerId $workerId encountered and exception while storing " +
              s" or checkpointing a batch for workerId $workerId and shardId $shardId.", e)

          /* Rethrow the exception to the Kinesis Worker that is managing this RecordProcessor. */
          throw e
      }
    } else {
      /* RecordProcessor has been stopped. */
      logInfo(s"Stopped:  KinesisReceiver has stopped for workerId $workerId" +
          s" and shardId $shardId.  No more records will be processed.")
    }
  }

  /**
   * Kinesis Client Library is shutting down this Worker for 1 of 2 reasons:
   * 1) the stream is resharding by splitting or merging adjacent shards
   *     (ShutdownReason.TERMINATE)
   * 2) the failed or latent Worker has stopped sending heartbeats for whatever reason
   *     (ShutdownReason.ZOMBIE)
   *
   * @param checkpointer used to perform a Kinesis checkpoint for ShutdownReason.TERMINATE
   * @param reason for shutdown (ShutdownReason.TERMINATE or ShutdownReason.ZOMBIE)
   */
  override def shutdown(
      checkpointer: IRecordProcessorCheckpointer,
      reason: ShutdownReason): Unit = {
    logInfo(s"Shutdown:  Shutting down workerId $workerId with reason $reason")
    // null if not initialized before shutdown:
    if (shardId == null) {
      logWarning(s"No shardId for workerId $workerId?")
    } else {
      reason match {
        /*
         * TERMINATE Use Case.  Checkpoint.
         * Checkpoint to indicate that all records from the shard have been drained and processed.
         * It's now OK to read from the new shards that resulted from a resharding event.
         */
        case ShutdownReason.TERMINATE => receiver.removeCheckpointer(shardId, checkpointer)

        /*
         * ZOMBIE Use Case or Unknown reason.  NoOp.
         * No checkpoint because other workers may have taken over and already started processing
         *    the same records.
         * This may lead to records being processed more than once.
         * Return null so that we don't checkpoint
         */
        case _ => receiver.removeCheckpointer(shardId, null)
      }
    }
  }
}

private[kinesis] object KinesisRecordProcessor extends Logging {
  /**
   * Retry the given amount of times with a random backoff time (millis) less than the
   *   given maxBackOffMillis
   *
   * @param expression expression to evaluate
   * @param numRetriesLeft number of retries left
   * @param maxBackOffMillis: max millis between retries
   *
   * @return evaluation of the given expression
   * @throws Unretryable exception, unexpected exception,
   *  or any exception that persists after numRetriesLeft reaches 0
   */
  @annotation.tailrec
  def retryRandom[T](expression: => T, numRetriesLeft: Int, maxBackOffMillis: Int): T = {
    util.Try { expression } match {
      /* If the function succeeded, evaluate to x. */
      case util.Success(x) => x
      /* If the function failed, either retry or throw the exception */
      case util.Failure(e) => e match {
        /* Retry:  Throttling or other Retryable exception has occurred */
        case _: ThrottlingException | _: KinesisClientLibDependencyException
            if numRetriesLeft > 1 =>
          val backOffMillis = Random.nextInt(maxBackOffMillis)
          Thread.sleep(backOffMillis)
          logError(s"Retryable Exception:  Random backOffMillis=${backOffMillis}", e)
          retryRandom(expression, numRetriesLeft - 1, maxBackOffMillis)
        /* Throw:  Shutdown has been requested by the Kinesis Client Library. */
        case _: ShutdownException =>
          logError(s"ShutdownException:  Caught shutdown exception, skipping checkpoint.", e)
          throw e
        /* Throw:  Non-retryable exception has occurred with the Kinesis Client Library */
        case _: InvalidStateException =>
          logError(s"InvalidStateException:  Cannot save checkpoint to the DynamoDB table used" +
              s" by the Amazon Kinesis Client Library.  Table likely doesn't exist.", e)
          throw e
        /* Throw:  Unexpected exception has occurred */
        case _ =>
          logError(s"Unexpected, non-retryable exception.", e)
          throw e
      }
    }
  }
}
