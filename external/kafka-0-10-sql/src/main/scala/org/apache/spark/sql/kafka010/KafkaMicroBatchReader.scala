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

package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, SerializedOffset}
import org.apache.spark.sql.kafka010.KafkaSourceProvider.{INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE, INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.UninterruptibleThread

/**
 * A [[MicroBatchReader]] that reads data from Kafka.
 *
 * The [[KafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
 * a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
 * example if the last record in a Kafka topic "t", partition 2 is offset 5, then
 * KafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
 * with the semantics of `KafkaConsumer.position()`.
 *
 * Zero data lost is not guaranteed when topics are deleted. If zero data lost is critical, the user
 * must make sure all messages in a topic have been processed when deleting a topic.
 *
 * There is a known issue caused by KAFKA-1894: the query using Kafka maybe cannot be stopped.
 * To avoid this issue, you should make sure stopping the query before stopping the Kafka brokers
 * and not use wrong broker addresses.
 */
private[kafka010] class KafkaMicroBatchReader(
    kafkaOffsetReader: KafkaOffsetReader,
    executorKafkaParams: ju.Map[String, Object],
    options: DataSourceOptions,
    metadataPath: String,
    startingOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean)
  extends MicroBatchReader with Logging {

  private var startPartitionOffsets: PartitionOffsetMap = _
  private var endPartitionOffsets: PartitionOffsetMap = _

  private val pollTimeoutMs = options.getLong(
    "kafkaConsumer.pollTimeoutMs",
    SparkEnv.get.conf.getTimeAsSeconds("spark.network.timeout", "120s") * 1000L)

  private val maxOffsetsPerTrigger =
    Option(options.get("maxOffsetsPerTrigger").orElse(null)).map(_.toLong)

  private val rangeCalculator = KafkaOffsetRangeCalculator(options)
  /**
   * Lazily initialize `initialPartitionOffsets` to make sure that `KafkaConsumer.poll` is only
   * called in StreamExecutionThread. Otherwise, interrupting a thread while running
   * `KafkaConsumer.poll` may hang forever (KAFKA-1894).
   */
  private lazy val initialPartitionOffsets = getOrCreateInitialPartitionOffsets()

  override def setOffsetRange(start: ju.Optional[Offset], end: ju.Optional[Offset]): Unit = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    startPartitionOffsets = Option(start.orElse(null))
        .map(_.asInstanceOf[KafkaSourceOffset].partitionToOffsets)
        .getOrElse(initialPartitionOffsets)

    endPartitionOffsets = Option(end.orElse(null))
        .map(_.asInstanceOf[KafkaSourceOffset].partitionToOffsets)
        .getOrElse {
          val latestPartitionOffsets = kafkaOffsetReader.fetchLatestOffsets()
          maxOffsetsPerTrigger.map { maxOffsets =>
            rateLimit(maxOffsets, startPartitionOffsets, latestPartitionOffsets)
          }.getOrElse {
            latestPartitionOffsets
          }
        }
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    // Find the new partitions, and get their earliest offsets
    val newPartitions = endPartitionOffsets.keySet.diff(startPartitionOffsets.keySet)
    val newPartitionInitialOffsets = kafkaOffsetReader.fetchEarliestOffsets(newPartitions.toSeq)
    if (newPartitionInitialOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionInitialOffsets.keySet)
      reportDataLoss(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed")
    }
    logInfo(s"Partitions added: $newPartitionInitialOffsets")
    newPartitionInitialOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      reportDataLoss(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed")
    }

    // Find deleted partitions, and report data loss if required
    val deletedPartitions = startPartitionOffsets.keySet.diff(endPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    // Use the end partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = endPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionInitialOffsets.contains(tp) || startPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = rangeCalculator.getRanges(
      fromOffsets = startPartitionOffsets ++ newPartitionInitialOffsets,
      untilOffsets = endPartitionOffsets,
      executorLocations = getSortedExecutorList())

    // Reuse Kafka consumers only when all the offset ranges have distinct TopicPartitions,
    // that is, concurrent tasks will not read the same TopicPartitions.
    val reuseKafkaConsumer = offsetRanges.map(_.topicPartition).toSet.size == offsetRanges.size

    // Generate factories based on the offset ranges
    offsetRanges.map { range =>
      new KafkaMicroBatchInputPartition(
        range, executorKafkaParams, pollTimeoutMs, failOnDataLoss, reuseKafkaConsumer
      ): InputPartition[InternalRow]
    }.asJava
  }

  override def getStartOffset: Offset = {
    KafkaSourceOffset(startPartitionOffsets)
  }

  override def getEndOffset: Offset = {
    KafkaSourceOffset(endPartitionOffsets)
  }

  override def deserializeOffset(json: String): Offset = {
    KafkaSourceOffset(JsonUtils.partitionOffsets(json))
  }

  override def readSchema(): StructType = KafkaOffsetReader.kafkaSchema

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    kafkaOffsetReader.close()
  }

  override def toString(): String = s"KafkaV2[$kafkaOffsetReader]"

  /**
   * Read initial partition offsets from the checkpoint, or decide the offsets and write them to
   * the checkpoint.
   */
  private def getOrCreateInitialPartitionOffsets(): PartitionOffsetMap = {
    // Make sure that `KafkaConsumer.poll` is only called in StreamExecutionThread.
    // Otherwise, interrupting a thread while running `KafkaConsumer.poll` may hang forever
    // (KAFKA-1894).
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    // SparkSession is required for getting Hadoop configuration for writing to checkpoints
    assert(SparkSession.getActiveSession.nonEmpty)

    val metadataLog =
      new KafkaSourceInitialOffsetWriter(SparkSession.getActiveSession.get, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(p) =>
          kafkaOffsetReader.fetchSpecificOffsets(p, reportDataLoss)
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
      limit: Long,
      from: PartitionOffsetMap,
      until: PartitionOffsetMap): PartitionOffsetMap = {
    val fromNew = kafkaOffsetReader.fetchEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
    val sizes = until.flatMap {
      case (tp, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(tp).orElse(fromNew.get(tp)).flatMap { begin =>
          val size = end - begin
          logDebug(s"rateLimit $tp size is $size")
          if (size > 0) Some(tp -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (tp, end) =>
          tp -> sizes.get(tp).map { size =>
            val begin = from.get(tp).getOrElse(fromNew(tp))
            val prorate = limit * (size / total)
            // Don't completely starve small topicpartitions
            val off = begin + (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  private def getSortedExecutorList(): Array[String] = {

    def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
      if (a.host == b.host) {
        a.executorId > b.executorId
      } else {
        a.host > b.host
      }
    }

    val bm = SparkEnv.get.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  /**
   * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`.
   * Otherwise, just log a warning.
   */
  private def reportDataLoss(message: String): Unit = {
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE")
    } else {
      logWarning(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE")
    }
  }

  /** A version of [[HDFSMetadataLog]] specialized for saving the initial offsets. */
  class KafkaSourceInitialOffsetWriter(sparkSession: SparkSession, metadataPath: String)
    extends HDFSMetadataLog[KafkaSourceOffset](sparkSession, metadataPath) {

    val VERSION = 1

    override def serialize(metadata: KafkaSourceOffset, out: OutputStream): Unit = {
      out.write(0) // A zero byte is written to support Spark 2.1.0 (SPARK-19517)
      val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
      writer.write("v" + VERSION + "\n")
      writer.write(metadata.json)
      writer.flush
    }

    override def deserialize(in: InputStream): KafkaSourceOffset = {
      in.read() // A zero byte is read to support Spark 2.1.0 (SPARK-19517)
      val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
      // HDFSMetadataLog guarantees that it never creates a partial file.
      assert(content.length != 0)
      if (content(0) == 'v') {
        val indexOfNewLine = content.indexOf("\n")
        if (indexOfNewLine > 0) {
          val version = parseVersion(content.substring(0, indexOfNewLine), VERSION)
          KafkaSourceOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
        } else {
          throw new IllegalStateException(
            s"Log file was malformed: failed to detect the log file version line.")
        }
      } else {
        // The log was generated by Spark 2.1.0
        KafkaSourceOffset(SerializedOffset(content))
      }
    }
  }
}

/** A [[InputPartition]] for reading Kafka data in a micro-batch streaming query. */
private[kafka010] case class KafkaMicroBatchInputPartition(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    reuseKafkaConsumer: Boolean) extends InputPartition[InternalRow] {

  override def preferredLocations(): Array[String] = offsetRange.preferredLoc.toArray

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new KafkaMicroBatchInputPartitionReader(offsetRange, executorKafkaParams, pollTimeoutMs,
      failOnDataLoss, reuseKafkaConsumer)
}

/** A [[InputPartitionReader]] for reading Kafka data in a micro-batch streaming query. */
private[kafka010] case class KafkaMicroBatchInputPartitionReader(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    reuseKafkaConsumer: Boolean) extends InputPartitionReader[InternalRow] with Logging {

  private val consumer = KafkaDataConsumer.acquire(
    offsetRange.topicPartition, executorKafkaParams, reuseKafkaConsumer)

  private val rangeToRead = resolveRange(offsetRange)
  private val converter = new KafkaRecordToUnsafeRowConverter

  private var nextOffset = rangeToRead.fromOffset
  private var nextRow: UnsafeRow = _

  override def next(): Boolean = {
    if (nextOffset < rangeToRead.untilOffset) {
      val record = consumer.get(nextOffset, rangeToRead.untilOffset, pollTimeoutMs, failOnDataLoss)
      if (record != null) {
        nextRow = converter.toUnsafeRow(record)
        nextOffset = record.offset + 1
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): UnsafeRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit = {
    consumer.release()
  }

  private def resolveRange(range: KafkaOffsetRange): KafkaOffsetRange = {
    if (range.fromOffset < 0 || range.untilOffset < 0) {
      // Late bind the offset range
      val availableOffsetRange = consumer.getAvailableOffsetRange()
      val fromOffset = if (range.fromOffset < 0) {
        assert(range.fromOffset == KafkaOffsetRangeLimit.EARLIEST,
          s"earliest offset ${range.fromOffset} does not equal ${KafkaOffsetRangeLimit.EARLIEST}")
        availableOffsetRange.earliest
      } else {
        range.fromOffset
      }
      val untilOffset = if (range.untilOffset < 0) {
        assert(range.untilOffset == KafkaOffsetRangeLimit.LATEST,
          s"latest offset ${range.untilOffset} does not equal ${KafkaOffsetRangeLimit.LATEST}")
        availableOffsetRange.latest
      } else {
        range.untilOffset
      }
      KafkaOffsetRange(range.topicPartition, fromOffset, untilOffset, None)
    } else {
      range
    }
  }
}
