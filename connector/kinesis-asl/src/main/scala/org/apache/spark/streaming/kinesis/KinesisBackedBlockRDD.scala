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

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model._

import org.apache.spark._
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{ERROR, RETRY_COUNT}
import org.apache.spark.rdd.{BlockRDD, BlockRDDPartition}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.NextIterator


/** Class representing a range of Kinesis sequence numbers. Both sequence numbers are inclusive. */
private[kinesis]
case class SequenceNumberRange(
    streamName: String,
    shardId: String,
    fromSeqNumber: String,
    toSeqNumber: String,
    recordCount: Int)

/** Class representing an array of Kinesis sequence number ranges */
private[kinesis]
case class SequenceNumberRanges(ranges: Seq[SequenceNumberRange]) {
  def isEmpty(): Boolean = ranges.isEmpty

  def nonEmpty(): Boolean = ranges.nonEmpty

  override def toString(): String = ranges.mkString("SequenceNumberRanges(", ", ", ")")
}

private[kinesis]
object SequenceNumberRanges {
  def apply(range: SequenceNumberRange): SequenceNumberRanges = {
    new SequenceNumberRanges(Seq(range))
  }
}


/** Partition storing the information of the ranges of Kinesis sequence numbers to read */
private[kinesis]
class KinesisBackedBlockRDDPartition(
    idx: Int,
    blockId: BlockId,
    val isBlockIdValid: Boolean,
    val seqNumberRanges: SequenceNumberRanges
  ) extends BlockRDDPartition(blockId, idx)

/**
 * A BlockRDD where the block data is backed by Kinesis, which can accessed using the
 * sequence numbers of the corresponding blocks.
 */
private[kinesis]
class KinesisBackedBlockRDD[T: ClassTag](
    sc: SparkContext,
    val regionName: String,
    val endpointUrl: String,
    @transient private val _blockIds: Array[BlockId],
    @transient val arrayOfseqNumberRanges: Array[SequenceNumberRanges],
    @transient private val isBlockIdValid: Array[Boolean] = Array.empty,
    val messageHandler: Record => T = KinesisInputDStream.defaultMessageHandler _,
    val kinesisCreds: SparkAWSCredentials = DefaultCredentials,
    val kinesisReadConfigs: KinesisReadConfigurations = KinesisReadConfigurations()
  ) extends BlockRDD[T](sc, _blockIds) {

  require(_blockIds.length == arrayOfseqNumberRanges.length,
    "Number of blockIds is not equal to the number of sequence number ranges")

  override def isValid: Boolean = true

  override def getPartitions: Array[Partition] = {
    Array.tabulate(_blockIds.length) { i =>
      val isValid = if (isBlockIdValid.length == 0) true else isBlockIdValid(i)
      new KinesisBackedBlockRDDPartition(i, _blockIds(i), isValid, arrayOfseqNumberRanges(i))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[KinesisBackedBlockRDDPartition]
    val blockId = partition.blockId

    def getBlockFromBlockManager(): Option[Iterator[T]] = {
      logDebug(s"Read partition data of $this from block manager, block $blockId")
      blockManager.get(blockId).map(_.data.asInstanceOf[Iterator[T]])
    }

    def getBlockFromKinesis(): Iterator[T] = {
      val credentials = kinesisCreds.provider.getCredentials
      partition.seqNumberRanges.ranges.iterator.flatMap { range =>
        new KinesisSequenceRangeIterator(credentials, endpointUrl, regionName,
          range, kinesisReadConfigs).map(messageHandler)
      }
    }
    if (partition.isBlockIdValid) {
      getBlockFromBlockManager().getOrElse { getBlockFromKinesis() }
    } else {
      getBlockFromKinesis()
    }
  }
}


/**
 * An iterator that return the Kinesis data based on the given range of sequence numbers.
 * Internally, it repeatedly fetches sets of records starting from the fromSequenceNumber,
 * until the endSequenceNumber is reached.
 */
private[kinesis]
class KinesisSequenceRangeIterator(
    credentials: AWSCredentials,
    endpointUrl: String,
    regionId: String,
    range: SequenceNumberRange,
    kinesisReadConfigs: KinesisReadConfigurations) extends NextIterator[Record] with Logging {

  private val client = new AmazonKinesisClient(credentials)
  private val streamName = range.streamName
  private val shardId = range.shardId
  // AWS limits to maximum of 10k records per get call
  private val maxGetRecordsLimit = 10000

  private var toSeqNumberReceived = false
  private var lastSeqNumber: String = null
  private var internalIterator: Iterator[Record] = null

  client.setEndpoint(endpointUrl)

  override protected def getNext(): Record = {
    var nextRecord: Record = null
    if (toSeqNumberReceived) {
      finished = true
    } else {

      if (internalIterator == null) {

        // If the internal iterator has not been initialized,
        // then fetch records from starting sequence number
        internalIterator = getRecords(ShardIteratorType.AT_SEQUENCE_NUMBER, range.fromSeqNumber,
          range.recordCount)
      } else if (!internalIterator.hasNext) {

        // If the internal iterator does not have any more records,
        // then fetch more records after the last consumed sequence number
        internalIterator = getRecords(ShardIteratorType.AFTER_SEQUENCE_NUMBER, lastSeqNumber,
          range.recordCount)
      }

      if (!internalIterator.hasNext) {

        // If the internal iterator still does not have any data, then throw exception
        // and terminate this iterator
        finished = true
        throw new SparkException(
          s"Could not read until the end sequence number of the range: $range")
      } else {

        // Get the record, copy the data into a byte array and remember its sequence number
        nextRecord = internalIterator.next()
        lastSeqNumber = nextRecord.getSequenceNumber()

        // If the this record's sequence number matches the stopping sequence number, then make sure
        // the iterator is marked finished next time getNext() is called
        if (nextRecord.getSequenceNumber == range.toSeqNumber) {
          toSeqNumberReceived = true
        }
      }
    }
    nextRecord
  }

  override protected def close(): Unit = {
    client.shutdown()
  }

  /**
   * Get records starting from or after the given sequence number.
   */
  private def getRecords(
      iteratorType: ShardIteratorType,
      seqNum: String,
      recordCount: Int): Iterator[Record] = {
    val shardIterator = getKinesisIterator(iteratorType, seqNum)
    val result = getRecordsAndNextKinesisIterator(shardIterator, recordCount)
    result._1
  }

  /**
   * Get the records starting from using a Kinesis shard iterator (which is a progress handle
   * to get records from Kinesis), and get the next shard iterator for next consumption.
   */
  private def getRecordsAndNextKinesisIterator(
      shardIterator: String,
      recordCount: Int): (Iterator[Record], String) = {
    val getRecordsRequest = new GetRecordsRequest
    getRecordsRequest.setRequestCredentials(credentials)
    getRecordsRequest.setShardIterator(shardIterator)
    getRecordsRequest.setLimit(Math.min(recordCount, this.maxGetRecordsLimit))
    val getRecordsResult = retryOrTimeout[GetRecordsResult](
      s"getting records using shard iterator") {
        client.getRecords(getRecordsRequest)
      }
    // De-aggregate records, if KPL was used in producing the records. The KCL automatically
    // handles de-aggregation during regular operation. This code path is used during recovery
    val recordIterator = UserRecord.deaggregate(getRecordsResult.getRecords)
    (recordIterator.iterator().asScala, getRecordsResult.getNextShardIterator)
  }

  /**
   * Get the Kinesis shard iterator for getting records starting from or after the given
   * sequence number.
   */
  private def getKinesisIterator(
      iteratorType: ShardIteratorType,
      sequenceNumber: String): String = {
    val getShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setRequestCredentials(credentials)
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    getShardIteratorRequest.setShardIteratorType(iteratorType.toString)
    getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber)
    val getShardIteratorResult = retryOrTimeout[GetShardIteratorResult](
        s"getting shard iterator from sequence number $sequenceNumber") {
          client.getShardIterator(getShardIteratorRequest)
        }
    getShardIteratorResult.getShardIterator
  }

  /** Helper method to retry Kinesis API request with exponential backoff and timeouts */
  private def retryOrTimeout[T](message: String)(body: => T): T = {
    val startTimeNs = System.nanoTime()
    var retryCount = 0
    var result: Option[T] = None
    var lastError: Throwable = null
    var waitTimeInterval = kinesisReadConfigs.retryWaitTimeMs

    def isTimedOut = {
      val retryTimeoutNs = TimeUnit.MILLISECONDS.toNanos(kinesisReadConfigs.retryTimeoutMs)
      (System.nanoTime() - startTimeNs) >= retryTimeoutNs
    }
    def isMaxRetryDone = retryCount >= kinesisReadConfigs.maxRetries

    while (result.isEmpty && !isTimedOut && !isMaxRetryDone) {
      if (retryCount > 0) {  // wait only if this is a retry
        Thread.sleep(waitTimeInterval)
        waitTimeInterval *= 2  // if you have waited, then double wait time for next round
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) =>
          lastError = t
           t match {
             case ptee: ProvisionedThroughputExceededException =>
               logWarning(log"Error while ${MDC(ERROR, message)} " +
                 log"[attempt = ${MDC(RETRY_COUNT, retryCount + 1)}]", ptee)
             case e: Throwable =>
               throw new SparkException(s"Error while $message", e)
           }
      }
      retryCount += 1
    }
    result.getOrElse {
      if (isTimedOut) {
        throw new SparkException(
          s"Timed out after ${kinesisReadConfigs.retryTimeoutMs} ms while " +
          s"$message, last exception: ", lastError)
      } else {
        throw new SparkException(
          s"Gave up after $retryCount retries while $message, last exception: ", lastError)
      }
    }
  }
}
