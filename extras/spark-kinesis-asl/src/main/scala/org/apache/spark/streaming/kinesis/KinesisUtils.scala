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

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import org.apache.spark.Logging
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import scala.util.Random
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.util.ManualClock
import org.apache.spark.streaming.util.Clock
import org.apache.spark.streaming.util.SystemClock

/**
 * Facade to create the Scala-based or Java-based streams.
 * Also, contains a reusable utility methods.
 */
object KinesisUtils extends Logging {
  /**
   * Create an InputDStream that pulls messages from a Kinesis stream.
   *
   * @param StreamingContext object
   * @param app name
   * @param stream name
   * @param endpoint
   * @param checkpoint interval (millis) for Kinesis checkpointing (not Spark checkpointing).
   * See the Kinesis Spark Streaming documentation for more details on the different types of checkpoints.
   * The default is TRIM_HORIZON to avoid potential data loss.  However, this presents the risk of processing records more than once.
   * @param in the absence of Kinesis checkpoint info, this is the worker's initial starting position in the stream.
   * The values are either the beginning of the stream per Kinesis' limit of 24 hours (InitialPositionInStream.TRIM_HORIZON)
   *       or the tip of the stream using InitialPositionInStream.LATEST.
   * The default is StorageLevel.MEMORY_AND_DISK_2 which replicates in-memory and on-disk to 2 nodes total (primary and secondary)
   *
   * @return ReceiverInputDStream[Array[Byte]]
   */
  def createStream(
    ssc: StreamingContext,
    app: String,
    stream: String,
    endpoint: String,
    checkpointIntervalMillis: Long,
    initialPositionInStream: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2): ReceiverInputDStream[Array[Byte]] = {

    ssc.receiverStream(new KinesisReceiver(app, stream, endpoint, checkpointIntervalMillis, initialPositionInStream, storageLevel))
  }

  /**
   * Create a Java-friendly InputDStream that pulls messages from a Kinesis stream.
   *
   * @param JavaStreamingContext object
   * @param app name
   * @param stream name
   * @param endpoint
   * @param checkpoint interval (millis) for Kinesis checkpointing (not Spark checkpointing).
   * See the Kinesis Spark Streaming documentation for more details on the different types of checkpoints.
   * The default is TRIM_HORIZON to avoid potential data loss.  However, this presents the risk of processing records more than once.
   * @param in the absence of Kinesis checkpoint info, this is the worker's initial starting position in the stream.
   * The values are either the beginning of the stream per Kinesis' limit of 24 hours (InitialPositionInStream.TRIM_HORIZON)
   *       or the tip of the stream using InitialPositionInStream.LATEST.
   * The default is StorageLevel.MEMORY_AND_DISK_2 which replicates in-memory and on-disk to 2 nodes total (primary and secondary)
   *
   * @return JavaReceiverInputDStream[Array[Byte]]
   */
  def createJavaStream(
    jssc: JavaStreamingContext,
    app: String,
    stream: String,
    endpoint: String,
    checkpointIntervalMillis: Long,
    initialPositionInStream: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2): JavaReceiverInputDStream[Array[Byte]] = {

    jssc.receiverStream(new KinesisReceiver(app, stream, endpoint, checkpointIntervalMillis, initialPositionInStream, storageLevel))
  }

  /**
   * Create checkpoint state using the existing system clock
   * @param checkpointIntervalMillis
   */
  def createCheckpointState(checkpointIntervalMillis: Long): CheckpointState = {
    new CheckpointState(checkpointIntervalMillis)
  }

  /**
   * Retry the given amount of times with a random backoff time (millis) less than the given maxBackOffMillis
   *
   * @param expression expression to evalute
   * @param numRetriesLeft number of retries left
   * @param maxBackOffMillis: max millis between retries
   *
   * @return Evaluation of the given expression
   * @throws Unretryable exception, unexpected exception,
   *  or any exception that persists after numRetriesLeft reaches 0
   */
  @annotation.tailrec
  def retry[T](expression: => T, numRetriesLeft: Int, maxBackOffMillis: Int): T = {
    util.Try { expression } match {
      /** If the function succeeded, evaluate to x. */
      case util.Success(x) => x
      /** If the function failed, either retry or throw the exception */
      case util.Failure(e) => e match {
        /** Retry:  Throttling or other Retryable exception has occurred */
        case _: ThrottlingException | _: KinesisClientLibDependencyException if numRetriesLeft > 1 => {
          val backOffMillis = Random.nextInt(maxBackOffMillis)
          Thread.sleep(backOffMillis)
          logError(s"Retryable Exception:  Random backOffMillis=${backOffMillis}", e)
          retry(expression, numRetriesLeft - 1, maxBackOffMillis)
        }
        /** Throw:  Shutdown has been requested by the Kinesis Client Library.*/
        case _: ShutdownException => {
          logError(s"ShutdownException:  Caught shutdown exception, skipping checkpoint.", e)
          throw e
        }
        /** Throw:  Non-retryable exception has occurred with the Kinesis Client Library */
        case _: InvalidStateException => {
          logError(s"InvalidStateException:  Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.  Table likely doesn't exist.", e)
          throw e
        }
        /** Throw:  Unexpected exception has occurred */
        case _ => {
          logError(s"Unexpected, non-retryable exception.", e)
          throw e
        }
      }
    }
  }
}
