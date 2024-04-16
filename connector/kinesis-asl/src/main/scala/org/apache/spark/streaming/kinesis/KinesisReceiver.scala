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

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.WORKER_URL
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.AtTimestamp
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Custom AWS Kinesis-specific implementation of Spark Streaming's Receiver.
 * This implementation relies on the Kinesis Client Library (KCL) Worker as described here:
 * https://github.com/awslabs/amazon-kinesis-client
 *
 * The way this Receiver works is as follows:
 *
 *  - The receiver starts a KCL Worker, which is essentially runs a threadpool of multiple
 *    KinesisRecordProcessor
 *  - Each KinesisRecordProcessor receives data from a Kinesis shard in batches. Each batch is
 *    inserted into a Block Generator, and the corresponding range of sequence numbers is recorded.
 *  - When the block generator defines a block, then the recorded sequence number ranges that were
 *    inserted into the block are recorded separately for being used later.
 *  - When the block is ready to be pushed, the block is pushed and the ranges are reported as
 *    metadata of the block. In addition, the ranges are used to find out the latest sequence
 *    number for each shard that can be checkpointed through the DynamoDB.
 *  - Periodically, each KinesisRecordProcessor checkpoints the latest successfully stored sequence
 *    number for it own shard.
 *
 * @param streamName   Kinesis stream name
 * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
 * @param regionName  Region name used by the Kinesis Client Library for
 *                    DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
 * @param initialPosition  Instance of [[KinesisInitialPosition]]
 *                         In the absence of Kinesis checkpoint info, this is the
 *                         worker's initial starting position in the stream.
 *                         The values are either the beginning of the stream
 *                         per Kinesis' limit of 24 hours
 *                         ([[KinesisInitialPositions.TrimHorizon]]) or
 *                         the tip of the stream ([[KinesisInitialPositions.Latest]]).
 * @param checkpointAppName  Kinesis application name. Kinesis Apps are mapped to Kinesis Streams
 *                 by the Kinesis Client Library.  If you change the App name or Stream name,
 *                 the KCL will throw errors.  This usually requires deleting the backing
 *                 DynamoDB table with the same name this Kinesis application.
 * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
 *                            See the Kinesis Spark Streaming documentation for more
 *                            details on the different types of checkpoints.
 * @param storageLevel Storage level to use for storing the received objects
 * @param kinesisCreds SparkAWSCredentials instance that will be used to generate the
 *                     AWSCredentialsProvider passed to the KCL to authorize Kinesis API calls.
 * @param cloudWatchCreds Optional SparkAWSCredentials instance that will be used to generate the
 *                        AWSCredentialsProvider passed to the KCL to authorize CloudWatch API
 *                        calls. Will use kinesisCreds if value is None.
 * @param dynamoDBCreds Optional SparkAWSCredentials instance that will be used to generate the
 *                      AWSCredentialsProvider passed to the KCL to authorize DynamoDB API calls.
 *                      Will use kinesisCreds if value is None.
 */
private[kinesis] class KinesisReceiver[T](
    val streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPosition: KinesisInitialPosition,
    checkpointAppName: String,
    checkpointInterval: Duration,
    storageLevel: StorageLevel,
    messageHandler: Record => T,
    kinesisCreds: SparkAWSCredentials,
    dynamoDBCreds: Option[SparkAWSCredentials],
    cloudWatchCreds: Option[SparkAWSCredentials],
    metricsLevel: MetricsLevel,
    metricsEnabledDimensions: Set[String])
  extends Receiver[T](storageLevel) with Logging { receiver =>

  /*
   * =================================================================================
   * The following vars are initialize in the onStart() method which executes in the
   * Spark worker after this Receiver is serialized and shipped to the worker.
   * =================================================================================
   */

  /**
   * workerId is used by the KCL should be based on the ip address of the actual Spark Worker
   * where this code runs (not the driver's IP address.)
   */
  @volatile private var workerId: String = null

  /**
   * Worker is the core client abstraction from the Kinesis Client Library (KCL).
   * A worker can process more than one shards from the given stream.
   * Each shard is assigned its own IRecordProcessor and the worker run multiple such
   * processors.
   */
  @volatile private var worker: Worker = null
  @volatile private var workerThread: Thread = null

  /** BlockGenerator used to generates blocks out of Kinesis data */
  @volatile private var blockGenerator: BlockGenerator = null

  /**
   * Sequence number ranges added to the current block being generated.
   * Accessing and updating of this map is synchronized by locks in BlockGenerator.
   */
  private val seqNumRangesInCurrentBlock = new mutable.ArrayBuffer[SequenceNumberRange]

  /** Sequence number ranges of data added to each generated block */
  private val blockIdToSeqNumRanges = new ConcurrentHashMap[StreamBlockId, SequenceNumberRanges]

  /**
   * The centralized kinesisCheckpointer that checkpoints based on the given checkpointInterval.
   */
  @volatile private var kinesisCheckpointer: KinesisCheckpointer = null

  /**
   * Latest sequence number ranges that have been stored successfully.
   * This is used for checkpointing through KCL */
  private val shardIdToLatestStoredSeqNum = new ConcurrentHashMap[String, String]

  /**
   * This is called when the KinesisReceiver starts and must be non-blocking.
   * The KCL creates and manages the receiving/processing thread pool through Worker.run().
   */
  override def onStart(): Unit = {
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

    workerId = Utils.localHostName() + ":" + UUID.randomUUID()

    kinesisCheckpointer = new KinesisCheckpointer(receiver, checkpointInterval, workerId)
    val kinesisProvider = kinesisCreds.provider

    val kinesisClientLibConfiguration = {
      val baseClientLibConfiguration = new KinesisClientLibConfiguration(
        checkpointAppName,
        streamName,
        kinesisProvider,
        dynamoDBCreds.map(_.provider).getOrElse(kinesisProvider),
        cloudWatchCreds.map(_.provider).getOrElse(kinesisProvider),
        workerId)
        .withKinesisEndpoint(endpointUrl)
        .withTaskBackoffTimeMillis(500)
        .withRegionName(regionName)
        .withMetricsLevel(metricsLevel)
        .withMetricsEnabledDimensions(metricsEnabledDimensions.asJava)

      // Update the Kinesis client lib config with timestamp
      // if InitialPositionInStream.AT_TIMESTAMP is passed
      initialPosition match {
        case ts: AtTimestamp =>
          baseClientLibConfiguration.withTimestampAtInitialPositionInStream(ts.getTimestamp)
        case _ =>
          baseClientLibConfiguration.withInitialPositionInStream(initialPosition.getPosition)
      }
    }

   /*
    *  RecordProcessorFactory creates impls of IRecordProcessor.
    *  IRecordProcessor adapts the KCL to our Spark KinesisReceiver via the
    *  IRecordProcessor.processRecords() method.
    *  We're using our custom KinesisRecordProcessor in this case.
    */
    val recordProcessorFactory = new IRecordProcessorFactory {
      override def createProcessor: IRecordProcessor =
        new KinesisRecordProcessor(receiver, workerId)
    }

    worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration)
    workerThread = new Thread() {
      override def run(): Unit = {
        try {
          worker.run()
        } catch {
          case NonFatal(e) =>
            restart("Error running the KCL worker in Receiver", e)
        }
      }
    }

    blockIdToSeqNumRanges.clear()
    blockGenerator.start()

    workerThread.setName(s"Kinesis Receiver ${streamId}")
    workerThread.setDaemon(true)
    workerThread.start()

    logInfo(log"Started receiver with workerId ${MDC(WORKER_URL, workerId)}")
  }

  /**
   * This is called when the KinesisReceiver stops.
   * The KCL worker.shutdown() method stops the receiving/processing threads.
   * The KCL will do its best to drain and checkpoint any in-flight records upon shutdown.
   */
  override def onStop(): Unit = {
    if (workerThread != null) {
      if (worker != null) {
        worker.shutdown()
        worker = null
      }
      workerThread.join()
      workerThread = null
      logInfo(log"Stopped receiver for workerId ${MDC(WORKER_URL, workerId)}")
    }
    workerId = null
    if (kinesisCheckpointer != null) {
      kinesisCheckpointer.shutdown()
      kinesisCheckpointer = null
    }
  }

  /** Add records of the given shard to the current block being generated */
  private[kinesis] def addRecords(shardId: String, records: java.util.List[Record]): Unit = {
    if (records.size > 0) {
      val dataIterator = records.iterator().asScala.map(messageHandler)
      val metadata = SequenceNumberRange(streamName, shardId,
        records.get(0).getSequenceNumber(), records.get(records.size() - 1).getSequenceNumber(),
        records.size())
      blockGenerator.addMultipleDataWithCallback(dataIterator, metadata)
    }
  }

  /** Return the current rate limit defined in [[BlockGenerator]]. */
  private[kinesis] def getCurrentLimit: Int = {
    assert(blockGenerator != null)
    math.min(blockGenerator.getCurrentLimit, Int.MaxValue).toInt
  }

  /** Get the latest sequence number for the given shard that can be checkpointed through KCL */
  private[kinesis] def getLatestSeqNumToCheckpoint(shardId: String): Option[String] = {
    Option(shardIdToLatestStoredSeqNum.get(shardId))
  }

  /**
   * Set the checkpointer that will be used to checkpoint sequence numbers to DynamoDB for the
   * given shardId.
   */
  def setCheckpointer(shardId: String, checkpointer: IRecordProcessorCheckpointer): Unit = {
    assert(kinesisCheckpointer != null, "Kinesis Checkpointer not initialized!")
    kinesisCheckpointer.setCheckpointer(shardId, checkpointer)
  }

  /**
   * Remove the checkpointer for the given shardId. The provided checkpointer will be used to
   * checkpoint one last time for the given shard. If `checkpointer` is `null`, then we will not
   * checkpoint.
   */
  def removeCheckpointer(shardId: String, checkpointer: IRecordProcessorCheckpointer): Unit = {
    assert(kinesisCheckpointer != null, "Kinesis Checkpointer not initialized!")
    kinesisCheckpointer.removeCheckpointer(shardId, checkpointer)
  }

  /**
   * Remember the range of sequence numbers that was added to the currently active block.
   * Internally, this is synchronized with `finalizeRangesForCurrentBlock()`.
   */
  private def rememberAddedRange(range: SequenceNumberRange): Unit = {
    seqNumRangesInCurrentBlock += range
  }

  /**
   * Finalize the ranges added to the block that was active and prepare the ranges buffer
   * for next block. Internally, this is synchronized with `rememberAddedRange()`.
   */
  private def finalizeRangesForCurrentBlock(blockId: StreamBlockId): Unit = {
    blockIdToSeqNumRanges.put(blockId,
      SequenceNumberRanges(seqNumRangesInCurrentBlock.toArray.toImmutableArraySeq))
    seqNumRangesInCurrentBlock.clear()
    logDebug(s"Generated block $blockId has $blockIdToSeqNumRanges")
  }

  /** Store the block along with its associated ranges */
  private def storeBlockWithRanges(
      blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[T]): Unit = {
    val rangesToReportOption = Option(blockIdToSeqNumRanges.remove(blockId))
    if (rangesToReportOption.isEmpty) {
      stop("Error while storing block into Spark, could not find sequence number ranges " +
        s"for block $blockId")
      return
    }

    val rangesToReport = rangesToReportOption.get
    var attempt = 0
    var stored = false
    var throwable: Throwable = null
    while (!stored && attempt <= 3) {
      try {
        store(arrayBuffer, rangesToReport)
        stored = true
      } catch {
        case NonFatal(th) =>
          attempt += 1
          throwable = th
      }
    }
    if (!stored) {
      stop("Error while storing block into Spark", throwable)
    }

    // Update the latest sequence number that have been successfully stored for each shard
    // Note that we are doing this sequentially because the array of sequence number ranges
    // is assumed to be
    rangesToReport.ranges.foreach { range =>
      shardIdToLatestStoredSeqNum.put(range.shardId, range.toSeqNumber)
    }
  }

  /**
   * Class to handle blocks generated by this receiver's block generator. Specifically, in
   * the context of the Kinesis Receiver, this handler does the following.
   *
   * - When an array of records is added to the current active block in the block generator,
   *   this handler keeps track of the corresponding sequence number range.
   * - When the currently active block is ready to sealed (not more records), this handler
   *   keep track of the list of ranges added into this block in another H
   */
  private class GeneratedBlockHandler extends BlockGeneratorListener {

    /**
     * Callback method called after a data item is added into the BlockGenerator.
     * The data addition, block generation, and calls to onAddData and onGenerateBlock
     * are all synchronized through the same lock.
     */
    def onAddData(data: Any, metadata: Any): Unit = {
      rememberAddedRange(metadata.asInstanceOf[SequenceNumberRange])
    }

    /**
     * Callback method called after a block has been generated.
     * The data addition, block generation, and calls to onAddData and onGenerateBlock
     * are all synchronized through the same lock.
     */
    def onGenerateBlock(blockId: StreamBlockId): Unit = {
      finalizeRangesForCurrentBlock(blockId)
    }

    /** Callback method called when a block is ready to be pushed / stored. */
    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      storeBlockWithRanges(blockId,
        arrayBuffer.asInstanceOf[mutable.ArrayBuffer[T]])
    }

    /** Callback called in case of any error in internal of the BlockGenerator */
    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }
}
