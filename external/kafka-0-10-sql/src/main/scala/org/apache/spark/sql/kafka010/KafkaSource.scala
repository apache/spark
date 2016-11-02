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

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer, OffsetOutOfRangeException}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka010.KafkaSource._
import org.apache.spark.sql.types._
import org.apache.spark.util.UninterruptibleThread

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
 *   in . This class is designed to return a configured [[KafkaConsumer]] that is used by the
 *   [[KafkaSource]] to query for the offsets. See the docs on
 *   [[org.apache.spark.sql.kafka010.KafkaSource.ConsumerStrategy]] for more details.
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
 *
 * There is a known issue caused by KAFKA-1894: the query using KafkaSource maybe cannot be stopped.
 * To avoid this issue, you should make sure stopping the query before stopping the Kafka brokers
 * and not use wrong broker addresses.
 */
private[kafka010] case class KafkaSource(
    sqlContext: SQLContext,
    consumerStrategy: ConsumerStrategy,
    executorKafkaParams: ju.Map[String, Object],
    sourceOptions: Map[String, String],
    metadataPath: String,
    startingOffsets: StartingOffsets,
    failOnDataLoss: Boolean)
  extends Source with Logging {

  private val sc = sqlContext.sparkContext

  private val pollTimeoutMs = sourceOptions.getOrElse("kafkaConsumer.pollTimeoutMs", "512").toLong

  private val maxOffsetFetchAttempts =
    sourceOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private val offsetFetchAttemptIntervalMs =
    sourceOptions.getOrElse("fetchOffset.retryIntervalMs", "10").toLong

  private val maxOffsetsPerTrigger =
    sourceOptions.get("maxOffsetsPerTrigger").map(_.toLong)

  /**
   * A KafkaConsumer used in the driver to query the latest Kafka offsets. This only queries the
   * offsets and never commits them.
   */
  private val consumer = consumerStrategy.createConsumer()

  /**
   * Lazily initialize `initialPartitionOffsets` to make sure that `KafkaConsumer.poll` is only
   * called in StreamExecutionThread. Otherwise, interrupting a thread while running
   * `KafkaConsumer.poll` may hang forever (KAFKA-1894).
   */
  private lazy val initialPartitionOffsets = {
    val metadataLog = new HDFSMetadataLog[KafkaSourceOffset](sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsets => KafkaSourceOffset(fetchEarliestOffsets())
        case LatestOffsets => KafkaSourceOffset(fetchLatestOffsets())
        case SpecificOffsets(p) => KafkaSourceOffset(fetchSpecificStartingOffsets(p))
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  private var currentPartitionOffsets: Option[Map[TopicPartition, Long]] = None

  override def schema: StructType = KafkaSource.kafkaSchema

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    val latest = fetchLatestOffsets()
    val offsets = maxOffsetsPerTrigger match {
      case None =>
        latest
      case Some(limit) if currentPartitionOffsets.isEmpty =>
        rateLimit(limit, initialPartitionOffsets, latest)
      case Some(limit) =>
        rateLimit(limit, currentPartitionOffsets.get, latest)
    }

    currentPartitionOffsets = Some(offsets)
    logDebug(s"GetOffset: ${offsets.toSeq.map(_.toString).sorted}")
    Some(KafkaSourceOffset(offsets))
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
      limit: Long,
      from: Map[TopicPartition, Long],
      until: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    val fromNew = fetchNewPartitionEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
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
            logDebug(s"rateLimit $tp prorated amount is $prorate")
            // Don't completely starve small topicpartitions
            val off = begin + (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
            logDebug(s"rateLimit $tp new offset is $off")
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  /**
   * Returns the data that is between the offsets
   * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
   * exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

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
    val newPartitionOffsets = fetchNewPartitionEarliestOffsets(newPartitions.toSeq)
    if (newPartitionOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionOffsets.keySet)
      reportDataLoss(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed")
    }
    logInfo(s"Partitions added: $newPartitionOffsets")
    newPartitionOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      reportDataLoss(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed")
    }

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    // Use the until partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = untilPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionOffsets.contains(tp) || fromPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = topicPartitions.map { tp =>
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse {
        newPartitionOffsets.getOrElse(tp, {
          // This should not happen since newPartitionOffsets contains all partitions not in
          // fromPartitionOffsets
          throw new IllegalStateException(s"$tp doesn't have a from offset")
        })
      }
      val untilOffset = untilPartitionOffsets(tp)
      val preferredLoc = if (numExecutors > 0) {
        // This allows cached KafkaConsumers in the executors to be re-used to read the same
        // partition in every batch.
        Some(sortedExecutors(floorMod(tp.hashCode, numExecutors)))
      } else None
      KafkaSourceRDDOffsetRange(tp, fromOffset, untilOffset, preferredLoc)
    }.filter { range =>
      if (range.untilOffset < range.fromOffset) {
        reportDataLoss(s"Partition ${range.topicPartition}'s offset was changed from " +
          s"${range.fromOffset} to ${range.untilOffset}, some data may have been missed")
        false
      } else {
        true
      }
    }.toArray

    // Create a RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = new KafkaSourceRDD(
      sc, executorKafkaParams, offsetRanges, pollTimeoutMs).map { cr =>
      Row(cr.key, cr.value, cr.topic, cr.partition, cr.offset, cr.timestamp, cr.timestampType.id)
    }

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    // On recovery, getBatch will get called before getOffset
    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = Some(untilPartitionOffsets)
    }

    sqlContext.createDataFrame(rdd, schema)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    consumer.close()
  }

  override def toString(): String = s"KafkaSource[$consumerStrategy]"

  /**
   * Set consumer position to specified offsets, making sure all assignments are set.
   */
  private def fetchSpecificStartingOffsets(
      partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    val result = withRetriesWithoutInterrupt {
      // Poll to get the latest assigned partitions
      consumer.poll(0)
      val partitions = consumer.assignment()
      consumer.pause(partitions)
      assert(partitions.asScala == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest, if you don't care.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions.asScala}")
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")

      partitionOffsets.foreach {
        case (tp, -1) => consumer.seekToEnd(ju.Arrays.asList(tp))
        case (tp, -2) => consumer.seekToBeginning(ju.Arrays.asList(tp))
        case (tp, off) => consumer.seek(tp, off)
      }
      partitionOffsets.map {
        case (tp, _) => tp -> consumer.position(tp)
      }
    }
    partitionOffsets.foreach {
      case (tp, off) if off != -1 && off != -2 =>
        if (result(tp) != off) {
          reportDataLoss(
            s"startingOffsets for $tp was $off but consumer reset to ${result(tp)}")
        }
      case _ =>
        // no real way to check that beginning or end is reasonable
    }
    result
  }

  /**
   * Fetch the earliest offsets of partitions.
   */
  private def fetchEarliestOffsets(): Map[TopicPartition, Long] = withRetriesWithoutInterrupt {
    // Poll to get the latest assigned partitions
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    logDebug(s"Partitions assigned to consumer: $partitions. Seeking to the beginning")

    consumer.seekToBeginning(partitions)
    val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
    logDebug(s"Got earliest offsets for partition : $partitionOffsets")
    partitionOffsets
  }

  /**
   * Fetch the latest offset of partitions.
   */
  private def fetchLatestOffsets(): Map[TopicPartition, Long] = withRetriesWithoutInterrupt {
    // Poll to get the latest assigned partitions
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    logDebug(s"Partitions assigned to consumer: $partitions. Seeking to the end.")

    consumer.seekToEnd(partitions)
    val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
    logDebug(s"Got latest offsets for partition : $partitionOffsets")
    partitionOffsets
  }

  /**
   * Fetch the earliest offsets for newly discovered partitions. The return result may not contain
   * some partitions if they are deleted.
   */
  private def fetchNewPartitionEarliestOffsets(
      newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] =
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      withRetriesWithoutInterrupt {
        // Poll to get the latest assigned partitions
        consumer.poll(0)
        val partitions = consumer.assignment()
        consumer.pause(partitions)
        logDebug(s"\tPartitions assigned to consumer: $partitions")

        // Get the earliest offset of each partition
        consumer.seekToBeginning(partitions)
        val partitionOffsets = newPartitions.filter { p =>
          // When deleting topics happen at the same time, some partitions may not be in
          // `partitions`. So we need to ignore them
          partitions.contains(p)
        }.map(p => p -> consumer.position(p)).toMap
        logDebug(s"Got earliest offsets for new partitions: $partitionOffsets")
        partitionOffsets
      }
    }

  /**
   * Helper function that does multiple retries on the a body of code that returns offsets.
   * Retries are needed to handle transient failures. For e.g. race conditions between getting
   * assignment and getting position while topics/partitions are deleted can cause NPEs.
   *
   * This method also makes sure `body` won't be interrupted to workaround a potential issue in
   * `KafkaConsumer.poll`. (KAFKA-1894)
   */
  private def withRetriesWithoutInterrupt(
      body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    // Make sure `KafkaConsumer.poll` won't be interrupted (KAFKA-1894)
    assert(Thread.currentThread().isInstanceOf[StreamExecutionThread])

    synchronized {
      var result: Option[Map[TopicPartition, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts
        && !Thread.currentThread().isInterrupted) {
        Thread.currentThread match {
          case ut: UninterruptibleThread =>
            // "KafkaConsumer.poll" may hang forever if the thread is interrupted (E.g., the query
            // is stopped)(KAFKA-1894). Hence, we just make sure we don't interrupt it.
            //
            // If the broker addresses are wrong, or Kafka cluster is down, "KafkaConsumer.poll" may
            // hang forever as well. This cannot be resolved in KafkaSource until Kafka fixes the
            // issue.
            ut.runUninterruptibly {
              try {
                result = Some(body)
              } catch {
                case x: OffsetOutOfRangeException =>
                  reportDataLoss(x.getMessage)
                case NonFatal(e) =>
                  lastException = e
                  logWarning(s"Error in attempt $attempt getting Kafka offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
              }
            }
          case _ =>
            throw new IllegalStateException(
              "Kafka APIs must be executed on a o.a.spark.util.UninterruptibleThread")
        }
      }
      if (Thread.interrupted()) {
        throw new InterruptedException()
      }
      if (result.isEmpty) {
        assert(attempt > maxOffsetFetchAttempts)
        assert(lastException != null)
        throw lastException
      }
      result.get
    }
  }

  /**
   * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`.
   * Otherwise, just log a warning.
   */
  private def reportDataLoss(message: String): Unit = {
    if (failOnDataLoss) {
      throw new IllegalStateException(message +
        ". Set the source option 'failOnDataLoss' to 'false' if you want to ignore these checks.")
    } else {
      logWarning(message)
    }
  }
}


/** Companion object for the [[KafkaSource]]. */
private[kafka010] object KafkaSource {

  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", LongType),
    StructField("timestampType", IntegerType)
  ))

  sealed trait ConsumerStrategy {
    def createConsumer(): Consumer[Array[Byte], Array[Byte]]
  }

  case class AssignStrategy(partitions: Array[TopicPartition], kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy {
    override def createConsumer(): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
      consumer.assign(ju.Arrays.asList(partitions: _*))
      consumer
    }

    override def toString: String = s"Assign[${partitions.mkString(", ")}]"
  }

  case class SubscribeStrategy(topics: Seq[String], kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy {
    override def createConsumer(): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
      consumer.subscribe(topics.asJava)
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
      consumer
    }

    override def toString: String = s"SubscribePattern[$topicPattern]"
  }

  private def getSortedExecutorList(sc: SparkContext): Array[String] = {
    val bm = sc.env.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  private def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
    if (a.host == b.host) { a.executorId > b.executorId } else { a.host > b.host }
  }

  private def floorMod(a: Long, b: Int): Int = ((a % b).toInt + b) % b
}
