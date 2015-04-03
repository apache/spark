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

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.Random

import org.apache.spark.Logging

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

/**
 * Kinesis-specific implementation of the Kinesis Client Library (KCL) IRecordProcessor.
 * This implementation operates on the Array[Byte] from the KinesisReceiver.
 * The Kinesis Worker creates an instance of this KinesisRecordProcessor upon startup.
 *
 * @param receiver Kinesis receiver
 * @param workerId for logging purposes
 * @param checkpointState represents the checkpoint state including the next checkpoint time.
 *   It's injected here for mocking purposes.
 */
private[kinesis] class KinesisRecordProcessor(
    receiver: KinesisReceiver,
    workerId: String,
    checkpointState: KinesisCheckpointState) extends IRecordProcessor with Logging {

  /* shardId to be populated during initialize() */
  var shardId: String = _

  /**
   * The Kinesis Client Library calls this method during IRecordProcessor initialization.
   *
   * @param shardId assigned by the KCL to this particular RecordProcessor.
   */
  override def initialize(shardId: String) {
    logInfo(s"Initialize:  Initializing workerId $workerId with shardId $shardId")
    this.shardId = shardId
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
        /*
         * Note:  If we try to store the raw ByteBuffer from record.getData(), the Spark Streaming
         * Receiver.store(ByteBuffer) attempts to deserialize the ByteBuffer using the
         *   internally-configured Spark serializer (kryo, etc).
         * This is not desirable, so we instead store a raw Array[Byte] and decouple
         *   ourselves from Spark's internal serialization strategy.
         */
        batch.foreach(record => receiver.store(record.getData().array()))
        
        logDebug(s"Stored:  Worker $workerId stored ${batch.size} records for shardId $shardId")

        /*
         * Checkpoint the sequence number of the last record successfully processed/stored 
         *   in the batch.
         * In this implementation, we're checkpointing after the given checkpointIntervalMillis.
         * Note that this logic requires that processRecords() be called AND that it's time to 
         *   checkpoint.  I point this out because there is no background thread running the 
         *   checkpointer.  Checkpointing is tested and trigger only when a new batch comes in.
         * If the worker is shutdown cleanly, checkpoint will happen (see shutdown() below).
         * However, if the worker dies unexpectedly, a checkpoint may not happen.
         * This could lead to records being processed more than once.
         */
        if (checkpointState.shouldCheckpoint()) {
          /* Perform the checkpoint */
          KinesisRecordProcessor.retryRandom(checkpointer.checkpoint(), 4, 100)

          /* Update the next checkpoint time */
          checkpointState.advanceCheckpoint()

          logDebug(s"Checkpoint:  WorkerId $workerId completed checkpoint of ${batch.size}" +
              s" records for shardId $shardId")
          logDebug(s"Checkpoint:  Next checkpoint is at " +
              s" ${checkpointState.checkpointClock.getTimeMillis()} for shardId $shardId")
        }
      } catch {
        case e: Throwable => {
          /*
           *  If there is a failure within the batch, the batch will not be checkpointed.
           *  This will potentially cause records since the last checkpoint to be processed
           *     more than once.
           */
          logError(s"Exception:  WorkerId $workerId encountered and exception while storing " +
              " or checkpointing a batch for workerId $workerId and shardId $shardId.", e)

          /* Rethrow the exception to the Kinesis Worker that is managing this RecordProcessor.*/
          throw e
        }
      }
    } else {
      /* RecordProcessor has been stopped. */
      logInfo(s"Stopped:  The Spark KinesisReceiver has stopped for workerId $workerId" + 
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
  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) {
    logInfo(s"Shutdown:  Shutting down workerId $workerId with reason $reason")
    reason match {
      /*
       * TERMINATE Use Case.  Checkpoint.
       * Checkpoint to indicate that all records from the shard have been drained and processed.
       * It's now OK to read from the new shards that resulted from a resharding event.
       */
      case ShutdownReason.TERMINATE => 
        KinesisRecordProcessor.retryRandom(checkpointer.checkpoint(), 4, 100)

      /*
       * ZOMBIE Use Case.  NoOp.
       * No checkpoint because other workers may have taken over and already started processing
       *    the same records.
       * This may lead to records being processed more than once.
       */
      case ShutdownReason.ZOMBIE =>

      /* Unknown reason.  NoOp */
      case _ =>
    }
  }
}

private[kinesis] object KinesisRecordProcessor extends Logging {
  /**
   * Retry the given amount of times with a random backoff time (millis) less than the
   *   given maxBackOffMillis
   *
   * @param expression expression to evalute
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
        case _: ThrottlingException | _: KinesisClientLibDependencyException if numRetriesLeft > 1
          => {
               val backOffMillis = Random.nextInt(maxBackOffMillis)
               Thread.sleep(backOffMillis)
               logError(s"Retryable Exception:  Random backOffMillis=${backOffMillis}", e)
               retryRandom(expression, numRetriesLeft - 1, maxBackOffMillis)
             }
        /* Throw:  Shutdown has been requested by the Kinesis Client Library.*/
        case _: ShutdownException => {
          logError(s"ShutdownException:  Caught shutdown exception, skipping checkpoint.", e)
          throw e
        }
        /* Throw:  Non-retryable exception has occurred with the Kinesis Client Library */
        case _: InvalidStateException => {
          logError(s"InvalidStateException:  Cannot save checkpoint to the DynamoDB table used" +
              s" by the Amazon Kinesis Client Library.  Table likely doesn't exist.", e)
          throw e
        }
        /* Throw:  Unexpected exception has occurred */
        case _ => {
          logError(s"Unexpected, non-retryable exception.", e)
          throw e
        }
      }
    }
  }
}
