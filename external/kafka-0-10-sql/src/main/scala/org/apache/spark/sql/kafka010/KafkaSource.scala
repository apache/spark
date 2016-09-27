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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka010.KafkaSource._
import org.apache.spark.sql.types._

/**
 * A [[Source]] that uses Kafka's own [[KafkaConsumer]] API to reads data from Kafka. The design
 * for this source is as follows.
 *
 * - The [[KafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
 *   a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
 *   example if the last record in a Kafka topic "t", partition 2 is offset 5, then
 *   KafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
 *   with the semantics of `KafkaConsumer.position()`.
 *
 * - The [[ConsumerStrategy]] class defines which Kafka topics and partitions should be read
 *   by this source. These strategies directly correspond to the different consumption options
 *   in . This class is designed to return a configured
 *   [[KafkaConsumer]] that is used by the [[KafkaSource]] to query for the offsets.
 *   See the docs on [[org.apache.spark.sql.kafka010.KafkaSource.ConsumerStrategy]] for
 *   more details.
 *
 * - The [[KafkaSource]] written to do the following.
 *
 *  - As soon as the source is created, the pre-configured KafkaConsumer returned by the
 *    [[ConsumerStrategy]] is used to query the initial offsets that this source should
 *    start reading from. This used to create the first batch.
 *
 *   - `getOffset()` uses the KafkaConsumer to query the latest available offsets, which are
 *     returned as a [[KafkaSourceOffset]].
 *
 *   - `getBatch()` returns a DF that reads from the 'start offset' until the 'end offset' in
 *     for each partition. The end offset is excluded to be consistent with the semantics of
 *     [[KafkaSourceOffset]] and `KafkaConsumer.position()`.
 *
 *   - The DF returned is based on [[KafkaSourceRDD]] which is constructed such that the
 *     data from Kafka topic + partition is consistently read by the same executors across
 *     batches, and cached KafkaConsumers in the executors can be reused efficiently. See the
 *     docs on [[KafkaSourceRDD]] for more details.
 *
 * Zero data lost is not guaranteed when topics are deleted. If zero data lost is critical, the user
 * must make sure all messages in a topic have been processed when deleting a topic.
 */
private[kafka010] case class KafkaSource(
    sqlContext: SQLContext,
    consumerStrategy: ConsumerStrategy,
    executorKafkaParams: ju.Map[String, Object],
    sourceOptions: Map[String, String])
  extends Source with Logging {

  private val consumer = consumerStrategy.createConsumer()
  private val sc = sqlContext.sparkContext
  private val initialPartitionOffsets = fetchPartitionOffsets(seekToLatest = false)
  logInfo(s"Initial offsets: $initialPartitionOffsets")

  override def schema: StructType = KafkaSource.kafkaSchema

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    val offset = KafkaSourceOffset(fetchPartitionOffsets(seekToLatest = true))
    logDebug(s"GetOffset: ${offset.partitionToOffsets.toSeq.map(_.toString).sorted}")
    Some(offset)
  }

  /** Returns the data that is between the offsets [`start`, `end`), i.e. end is exclusive. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"GetBatch called with start = $start, end = $end")
    val untilPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(end)
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        KafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionOffsets = if (newPartitions.nonEmpty) {
      fetchNewPartitionEarliestOffsets(newPartitions.toSeq)
    } else {
      Map.empty[TopicPartition, Long]
    }
    if (newPartitionOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionOffsets.keySet)
      logWarning(s"Partitions removed: ${deletedPartitions}, some data may have been missed")
    }
    logInfo(s"Partitions added: $newPartitionOffsets")
    newPartitionOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      logWarning(s"Added partition $p starts from $o instead of 0, some data may have been missed")
    }

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    logWarning(s"Partitions removed: $deletedPartitions, some data may have been missed")

    // Sort the partitions and current list of executors to consistently assign each partition
    // to the executor. This allows cached KafkaConsumers in the executors to be re-used to
    // read the same partition in every batch.
    val topicPartitionOrdering = new Ordering[TopicPartition] {
      override def compare(l: TopicPartition, r: TopicPartition): Int = {
        implicitly[Ordering[(String, Long)]].compare(
          (l.topic, l.partition),
          (r.topic, r.partition))
      }
    }

    // Use the until partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val sortedTopicPartitions = untilPartitionOffsets.keySet.filter { tp =>
      newPartitionOffsets.contains(tp) || fromPartitionOffsets.contains(tp)
    }.toSeq.sorted(topicPartitionOrdering)
    logDebug("Sorted topicPartitions: " + sortedTopicPartitions.mkString(", "))

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = sortedTopicPartitions.map { tp =>
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse {
        newPartitionOffsets.getOrElse(tp, {
          // This should not happen since newPartitionOffsets contains all partitions not in
          // fromPartitionOffsets
          throw new IllegalStateException(s"$tp doesn't have a from offset")
        })
      }
      val untilOffset = untilPartitionOffsets(tp)
      val preferredLoc = if (numExecutors > 0) {
        Some(sortedExecutors(positiveMod(tp.hashCode, numExecutors)))
      } else None
      KafkaSourceRDDOffsetRange(tp, fromOffset, untilOffset, preferredLoc)
    }.filter { range =>
      if (range.untilOffset < range.fromOffset) {
        logWarning(s"Partition ${range.topicPartition} was deleted and then added, " +
          "some data may have been missed")
        false
      } else {
        true
      }
    }.toArray

    // Create a RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = new KafkaSourceRDD(
      sc, executorKafkaParams, offsetRanges).map { cr =>
        Row(cr.checksum, cr.key, cr.offset, cr.partition, cr.serializedKeySize,
          cr.serializedValueSize, cr.timestamp, cr.timestampType.id, cr.topic, cr.value)
    }

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))
    sqlContext.createDataFrame(rdd, schema)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    consumer.close()
  }

  override def toString(): String = s"KafkaSource[$consumerStrategy]"

  /**
   * Fetch the offset of a partition, either the latest offsets or the current offsets in the
   * KafkaConsumer.
   */
  private def fetchPartitionOffsets(
      seekToLatest: Boolean): Map[TopicPartition, Long] = {

    // Poll to get the latest assigned partitions
    logTrace("\tPolling")
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    logDebug(s"\tPartitioned assigned to consumer: $partitions")

    // Get the current or latest offset of each partition
    if (seekToLatest) {
      consumer.seekToEnd(partitions)
      logDebug("\tSeeked to the end")
    }
    logTrace("Getting positions")
    val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
    logDebug(s"Got offsets for partition : $partitionOffsets")
    partitionOffsets
  }

  /** Fetch the earliest offsets for newly discovered partitions */
  private def fetchNewPartitionEarliestOffsets(
      newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {

    // Poll to get the latest assigned partitions
    logTrace("\tPolling")
    consumer.poll(0)
    val partitions = consumer.assignment()
    logDebug(s"\tPartitioned assigned to consumer: $partitions")

    // Get the earliest offset of each partition
    consumer.seekToBeginning(partitions)
    val partitionToOffsets = newPartitions.filter { p =>
      // When deleting topics happen at the same time, some partitions may not be in `partitions`.
      // So we need to ignore them
      partitions.contains(p)
    }.map(p => p -> consumer.position(p)).toMap
    logDebug(s"Got offsets for new partitions: $partitionToOffsets")
    partitionToOffsets
  }

  /**
   * Helper function that does multiple retries on the a body of code that returns offsets.
   * Retries are needed to handle transient failures. For e.g. race conditions between getting
   * assignment and getting position while topics/partitions are deleted can cause NPEs.
   */
  private def withRetries(
      body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = synchronized {

    var result: Option[Map[TopicPartition, Long]] = None
    var attempt = 1
    var lastException: Throwable = null
    while (result.isEmpty && attempt <= MAX_OFFSET_FETCH_ATTEMPTS) {
      try {
        result = Some(body)
      } catch {
        case NonFatal(e) =>
          lastException = e
          e.printStackTrace()
          logWarning(s"Error in attempt $attempt getting Kafka offsets: ", e)
          attempt += 1
          Thread.sleep(OFFSET_FETCH_ATTEMPT_INTERVAL_MS)
      }
    }
    if (result.isEmpty) {
      assert(attempt > MAX_OFFSET_FETCH_ATTEMPTS)
      assert(lastException != null)
      throw lastException
    }
    result.get
  }

  private def positiveMod(a: Long, b: Int): Int = ((a % b).toInt + b) % b
}


/** Companion object for the [[KafkaSource]]. */
private[kafka010] object KafkaSource {

  val MAX_OFFSET_FETCH_ATTEMPTS = 3
  val OFFSET_FETCH_ATTEMPT_INTERVAL_MS = 10

  def kafkaSchema: StructType = StructType(Seq(
    StructField("checksum", LongType),
    StructField("key", BinaryType),
    StructField("offset", LongType),
    StructField("partition", IntegerType),
    StructField("serializedKeySize", IntegerType),
    StructField("serializedValueSize", IntegerType),
    StructField("timestamp", LongType),
    StructField("timestampType", IntegerType),
    StructField("topic", StringType),
    StructField("value", BinaryType)
  ))

  sealed trait ConsumerStrategy {
    def createConsumer(): Consumer[Array[Byte], Array[Byte]]
  }

  case class SubscribeStrategy(topics: Seq[String], kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy {
    override def createConsumer(): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
      consumer.subscribe(topics.asJava)
      consumer.poll(0)
      consumer
    }

    override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
  }

  case class SubscribePatternStrategy(
    topicPattern: String, kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy {
    override def createConsumer(): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
      consumer.subscribe(
        ju.regex.Pattern.compile(topicPattern),
        new NoOpConsumerRebalanceListener())
      consumer.poll(0)
      consumer
    }

    override def toString: String = s"SubscribePattern[$topicPattern]"
  }

  def getSortedExecutorList(sc: SparkContext): Array[String] = {
    def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
      if (a.host == b.host) { a.executorId > b.executorId } else { a.host > b.host }
    }

    val bm = sc.env.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }
}


/** An [[Offset]] for the [[KafkaSource]]. */
private[kafka010]
case class KafkaSourceOffset(partitionToOffsets: Map[TopicPartition, Long]) extends Offset {
  override def toString(): String = {
    partitionToOffsets.toSeq.sortBy(_._1.toString).mkString("[", ", ", "]")
  }
}

/** Companion object of the [[KafkaSourceOffset]] */
private[kafka010] object KafkaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TopicPartition, Long] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
   * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
   * tuples.
   */
  def apply(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.map { case(t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }
}


