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

import scala.util.Random
import scala.util.control.NonFatal

import software.amazon.kinesis.exceptions.{InvalidStateException, KinesisClientLibDependencyException, ShutdownException, ThrottlingException}
import software.amazon.kinesis.lifecycle.events.{InitializationInput, LeaseLostInput, ProcessRecordsInput, ShardEndedInput, ShutdownRequestedInput}
import software.amazon.kinesis.processor.ShardRecordProcessor

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{RETRY_INTERVAL, SHARD_ID, WORKER_URL}

/**
 * Kinesis-specific implementation of the Kinesis Client Library (KCL) IRecordProcessor.
 * This implementation operates on the Array[Byte] from the KinesisReceiver.
 * The Kinesis scheduler creates an instance of this KinesisRecordProcessor for each
 * shard in the Kinesis stream upon startup.  This is normally done in separate threads,
 * but the KCLs within the KinesisReceivers will balance themselves out if you create
 * multiple Receivers.
 *
 * @param receiver Kinesis receiver
 * @param schedulerId for logging purposes
 */
private[kinesis] class KinesisRecordProcessor[T](receiver: KinesisReceiver[T], schedulerId: String)
  extends ShardRecordProcessor with Logging {

  // shardId populated during initialize()
  @volatile
  private var shardId: String = _

  /**
   * The Kinesis Client Library calls this method during ShardRecordProcessor initialization.
   * @param initializationInput contains parameters to the ShardRecordProcessor
   * initialize method
   */
  override def initialize(initializationInput: InitializationInput): Unit = {
    this.shardId = initializationInput.shardId
    logInfo(log"Initialized schedulerId ${MDC(WORKER_URL, schedulerId)} " +
      log"with shardId ${MDC(SHARD_ID, shardId)}")
  }

  /**
   * This method is called by the KCL when a batch of records is pulled from the Kinesis stream.
   * This is the record-processing bridge between the KCL's ShardRecordProcessor.processRecords()
   * and Spark Streaming's Receiver
   *
   * @param processRecordsInput Provides the records to be processed as well as information and
   * capabilities related to them (eg checkpointing).
   */
  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val batch = processRecordsInput.records
    val checkpointer = processRecordsInput.checkpointer
    if (!receiver.isStopped()) {
      try {
        // Limit the number of processed records from Kinesis stream. This is because the KCL cannot
        // control the number of aggregated records to be fetched even if we set `MaxRecords`
        // in `PollingConfig`. For example, if we set 10 to the number of max records
        // in a scheduler and a producer aggregates two records into one message, the scheduler
        // possibly 20 records every callback function called.
        val maxRecords = receiver.getCurrentLimit
        for (start <- 0 until batch.size by maxRecords) {
          val miniBatch = batch.subList(start, math.min(start + maxRecords, batch.size))
          receiver.addRecords(shardId, miniBatch)
          logDebug(s"Stored: Scheduler $schedulerId stored ${miniBatch.size} records " +
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
          logError(log"Exception: SchedulerId ${MDC(WORKER_URL, schedulerId)} encountered and " +
            log"exception while storing or checkpointing a batch for schedulerId " +
            log"${MDC(WORKER_URL, schedulerId)} and shardId ${MDC(SHARD_ID, shardId)}.", e)

          /* Rethrow the exception to the Kinesis scheduler that is managing
           this RecordProcessor. */
          throw e
      }
    } else {
      /* RecordProcessor has been stopped. */
      logInfo(log"Stopped: KinesisReceiver has stopped for schedulerId " +
        log"${MDC(WORKER_URL, schedulerId)} and shardId ${MDC(SHARD_ID, shardId)}. " +
        log"No more records will be processed.")
    }
  }

  /**
   * Called when the lease that tied to this Kinesis record processor has been lost.
   * Once the lease has been lost the record processor can no longer checkpoint.
   *
   * @param leaseLostInput gives access to information related to the loss of the lease.
   * Currently this has no functionality.
   */
  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
    logInfo(log"The lease for shardId: ${MDC(WORKER_URL, schedulerId)} is lost.")
  }

  /**
   * Called when the shard that this Kinesis record processor is handling has been completed.
   * Once a shard has been completed no further records will ever arrive on that shard.
   *
   * When this is called the record processor <b>must</b> checkpoint. Otherwise an exception
   * will be thrown and the all child shards of this shard will not make progress.
   *
   * @param shardEndedInput provides access to a checkpointer method for completing
   * processing of the shard.
   */
  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    log.info(s"Reached shard end. Checkpointing for shardId: $shardId")
    if (shardId == null) {
      logWarning("shardId is not initialized for this record processor.")
    } else {
      receiver.removeCheckpointer(shardId, shardEndedInput.checkpointer)
    }
  }

  /**
   * Called when the Scheduler has been requested to shutdown. This is called while the
   * Kinesis record processor still holds the lease so checkpointing is possible. Once this method
   * has completed the lease for the record processor is released, and
   * {@link # leaseLost ( LeaseLostInput )} will be called at a later time.
   *
   * @param shutdownRequestedInput provides access to a checkpointer allowing a record
   * processor to checkpoint before the shutdown is completed.
   */
  override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = {
    logInfo(log"Shutdown: Shutting down schedulerId: ${MDC(WORKER_URL, schedulerId)} ")
    if (shardId == null) {
      logWarning(log"No shardId for schedulerId ${MDC(WORKER_URL, schedulerId)}?")
    } else {
      receiver.removeCheckpointer(shardId, shutdownRequestedInput.checkpointer)
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
          logError(log"Retryable Exception: Random " +
            log"backOffMillis=${MDC(RETRY_INTERVAL, backOffMillis)}", e)
          retryRandom(expression, numRetriesLeft - 1, maxBackOffMillis)
        /* Throw:  Shutdown has been requested by the Kinesis Client Library. */
        case _: ShutdownException =>
          logError(s"ShutdownException: Caught shutdown exception, skipping checkpoint.", e)
          throw e
        /* Throw:  Non-retryable exception has occurred with the Kinesis Client Library */
        case _: InvalidStateException =>
          logError(s"InvalidStateException: Cannot save checkpoint to the DynamoDB table used" +
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
