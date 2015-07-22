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

import scala.collection.JavaConversions._

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._

import org.apache.spark._
import org.apache.spark.rdd.{BlockRDD, BlockRDDPartition}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.NextIterator


/** Class representing a range of Kinesis sequence numbers */
private[kinesis]
case class SequenceNumberRange(
    streamName: String, shardId: String, fromSeqNumber: String, toSeqNumber: String)

/** Class representing an array of Kinesis sequence number ranges */
private[kinesis]
case class SequenceNumberRanges(ranges: Array[SequenceNumberRange]) {
  def isEmpty(): Boolean = ranges.isEmpty
  def nonEmpty(): Boolean = ranges.nonEmpty
  override def toString(): String = ranges.mkString("SequenceNumberRanges(", ", ", ")")
}

private[kinesis]
object SequenceNumberRanges {
  def apply(range: SequenceNumberRange): SequenceNumberRanges = {
    new SequenceNumberRanges(Array(range))
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
class KinesisBackedBlockRDD(
    sc: SparkContext,
    regionId: String,
    endpointUrl: String,
    @transient blockIds: Array[BlockId],
    @transient arrayOfseqNumberRanges: Array[SequenceNumberRanges],
    @transient isBlockIdValid: Array[Boolean] = Array.empty,
    awsCredentialsOption: Option[SerializableAWSCredentials] = None
) extends BlockRDD[Array[Byte]](sc, blockIds) {

  require(blockIds.length == arrayOfseqNumberRanges.length,
    "Number of blockIds is not equal to the number of sequence number ranges")

  override def isValid(): Boolean = true

  override def getPartitions: Array[Partition] = {
    Array.tabulate(blockIds.length) { i =>
      val isValid = if (isBlockIdValid.length == 0) true else isBlockIdValid(i)
      new KinesisBackedBlockRDDPartition(i, blockIds(i), isValid, arrayOfseqNumberRanges(i))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[KinesisBackedBlockRDDPartition]
    val blockId = partition.blockId

    def getBlockFromBlockManager(): Option[Iterator[Array[Byte]]] = {
      logDebug(s"Read partition data of $this from block manager, block $blockId")
      blockManager.get(blockId).map(_.data.asInstanceOf[Iterator[Array[Byte]]])
    }

    def getBlockFromKinesis(): Iterator[Array[Byte]] = {
      val credenentials = awsCredentialsOption.getOrElse {
        new DefaultAWSCredentialsProviderChain().getCredentials()
      }
      partition.seqNumberRanges.ranges.iterator.flatMap { range =>
        new KinesisSequenceRangeIterator(credenentials, endpointUrl, regionId, range)
      }
    }
    if (partition.isBlockIdValid) {
      getBlockFromBlockManager().getOrElse { getBlockFromKinesis() }
    } else {
      getBlockFromKinesis()
    }
  }
}


/** An iterator that return the Kinesis data based on the given range of Sequence numbers */
private[kinesis]
class KinesisSequenceRangeIterator(
    credentials: AWSCredentials,
    endpointUrl: String,
    regionId: String,
    range: SequenceNumberRange
  ) extends NextIterator[Array[Byte]] {

  private val backoffTimeMillis = 1000
  private val client = new AmazonKinesisClient(credentials)

  private var toSeqNumberReceived = false
  private var lastSeqNumber: String = null
  private var internalIterator: Iterator[Record] = null

  client.setEndpoint(endpointUrl, "kinesis", regionId)

  override protected def getNext(): Array[Byte] = {
    var nextBytes: Array[Byte] = null
    if (toSeqNumberReceived) {
      finished = true
    } else {

      if (internalIterator == null) {

        // If the internal iterator has not been initialized,
        // then fetch records from starting sequence number
        getRecords(ShardIteratorType.AT_SEQUENCE_NUMBER, range.fromSeqNumber)
      } else if (!internalIterator.hasNext) {

        // If the internal iterator does not have any more records,
        // then fetch more records after the last consumed sequence number
        getRecords(ShardIteratorType.AFTER_SEQUENCE_NUMBER, lastSeqNumber)
      }

      if (!internalIterator.hasNext) {

        // If the internal iterator still does not have any data, then throw exception
        // and terminate this iterator
        finished = true
        throw new SparkException(s"Could not read until the specified end sequence number: $range")
      } else {

        // Get the record, and remember its sequence number
        val nextRecord = internalIterator.next()
        nextBytes = nextRecord.getData().array()
        lastSeqNumber = nextRecord.getSequenceNumber()

        // If the this record's sequence number matches the stopping sequence number, then make sure
        // the iterator is marked finished next time getNext() is called
        if (nextRecord.getSequenceNumber == range.toSeqNumber) {
          toSeqNumberReceived = true
        }
      }

    }
    nextBytes
  }

  override protected def close(): Unit = { }

  private def getRecords(iteratorType: ShardIteratorType, seqNum: String): Unit = {
    val shardIterator = getKinesisIterator(range.streamName, range.shardId, iteratorType, seqNum)
    var records: Seq[Record] = null
    do {
      try {
        val getResult = getRecordsAndNextKinesisIterator(
          range.streamName, range.shardId, shardIterator)
        records = getResult._1
      } catch {
        case ptee: ProvisionedThroughputExceededException =>
          Thread.sleep(backoffTimeMillis)
      }
    } while (records == null || records.length == 0)  // TODO: put a limit on the number of retries
    if (records != null && records.nonEmpty) {
      internalIterator = records.iterator
    }
  }

  private def getRecordsAndNextKinesisIterator(
      streamName: String,
      shardId: String,
      shardIterator: String
    ): (Seq[Record], String) = {
    val getRecordsRequest = new GetRecordsRequest
    getRecordsRequest.setRequestCredentials(credentials)
    getRecordsRequest.setShardIterator(shardIterator)
    val getRecordsResult = client.getRecords(getRecordsRequest)
    (getRecordsResult.getRecords, getRecordsResult.getNextShardIterator)
  }

  private def getKinesisIterator(
      streamName: String,
      shardId: String,
      iteratorType: ShardIteratorType,
      sequenceNumber: String
    ): String = {
    val getShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setRequestCredentials(credentials)
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    getShardIteratorRequest.setShardIteratorType(iteratorType.toString)
    getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber)
    val getShardIteratorResult = client.getShardIterator(getShardIteratorRequest)
    getShardIteratorResult.getShardIterator
  }
}
