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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{OFFSETS, RETRY_COUNT, TOPIC_PARTITION_OFFSET}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.kafka010.KafkaSourceProvider.StrategyOnNoMatchStartingOffset
import org.apache.spark.util.{UninterruptibleThread, UninterruptibleThreadRunner}
import org.apache.spark.util.ArrayImplicits._

/**
 * This class uses Kafka's own [[org.apache.kafka.clients.consumer.KafkaConsumer]] API to
 * read data offsets from Kafka.
 * The [[ConsumerStrategy]] class defines which Kafka topics and partitions should be read
 * by this source. These strategies directly correspond to the different consumption options
 * in. This class is designed to return a configured
 * [[org.apache.kafka.clients.consumer.KafkaConsumer]] that is used by the
 * [[KafkaSource]] to query for the offsets. See the docs on
 * [[org.apache.spark.sql.kafka010.ConsumerStrategy]]
 * for more details.
 *
 * Note: This class is not ThreadSafe
 */
private[kafka010] class KafkaOffsetReaderConsumer(
    consumerStrategy: ConsumerStrategy,
    override val driverKafkaParams: ju.Map[String, Object],
    readerOptions: CaseInsensitiveMap[String],
    driverGroupIdPrefix: String) extends KafkaOffsetReader with Logging {

  /**
   * [[UninterruptibleThreadRunner]] ensures that all
   * [[org.apache.kafka.clients.consumer.KafkaConsumer]] communication called in an
   * [[UninterruptibleThread]]. In the case of streaming queries, we are already running in an
   * [[UninterruptibleThread]], however for batch mode this is not the case.
   */
  val uninterruptibleThreadRunner = new UninterruptibleThreadRunner("Kafka Offset Reader")

  /**
   * Place [[groupId]] and [[nextId]] here so that they are initialized before any consumer is
   * created -- see SPARK-19564.
   */
  private var groupId: String = null
  private var nextId = 0

  /**
   * A KafkaConsumer used in the driver to query the latest Kafka offsets. This only queries the
   * offsets and never commits them.
   */
  @volatile protected var _consumer: Consumer[Array[Byte], Array[Byte]] = null

  protected def consumer: Consumer[Array[Byte], Array[Byte]] = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    if (_consumer == null) {
      val newKafkaParams = new ju.HashMap[String, Object](driverKafkaParams)
      if (driverKafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG) == null) {
        newKafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, nextGroupId())
      }
      _consumer = consumerStrategy.createConsumer(newKafkaParams)
    }
    _consumer
  }

  private[kafka010] val maxOffsetFetchAttempts =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_NUM_RETRY, "3").toInt

  /**
   * Number of partitions to read from Kafka. If this value is greater than the number of Kafka
   * topicPartitions, we will split up  the read tasks of the skewed partitions to multiple Spark
   * tasks. The number of Spark tasks will be *approximately* `numPartitions`. It can be less or
   * more depending on rounding errors or Kafka partitions that didn't receive any new data.
   */
  private val minPartitions =
    readerOptions.get(KafkaSourceProvider.MIN_PARTITIONS_OPTION_KEY).map(_.toInt)

  private val rangeCalculator = new KafkaOffsetRangeCalculator(minPartitions)

  private[kafka010] val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_RETRY_INTERVAL_MS, "1000").toLong

  /**
   * Whether we should divide Kafka TopicPartitions with a lot of data into smaller Spark tasks.
   */
  private def shouldDivvyUpLargePartitions(numTopicPartitions: Int): Boolean = {
    minPartitions.map(_ > numTopicPartitions).getOrElse(false)
  }

  private def nextGroupId(): String = {
    groupId = driverGroupIdPrefix + "-" + nextId
    nextId += 1
    groupId
  }

  override def toString(): String = consumerStrategy.toString

  override def close(): Unit = {
    if (_consumer != null) uninterruptibleThreadRunner.runUninterruptibly { stopConsumer() }
    uninterruptibleThreadRunner.shutdown()
  }

  /**
   * @return The Set of TopicPartitions for a given topic
   */
  private def fetchTopicPartitions(): Set[TopicPartition] =
    uninterruptibleThreadRunner.runUninterruptibly {
      assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
      // Poll to get the latest assigned partitions
      consumer.poll(0)
      val partitions = consumer.assignment()
      consumer.pause(partitions)
      partitions.asScala.toSet
  }

  override def fetchPartitionOffsets(
      offsetRangeLimit: KafkaOffsetRangeLimit,
      isStartingOffsets: Boolean): Map[TopicPartition, Long] = {
    def validateTopicPartitions(partitions: Set[TopicPartition],
      partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
      assert(partitions == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions}")
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")
      partitionOffsets
    }
    val partitions = fetchTopicPartitions()
    // Obtain TopicPartition offsets with late binding support
    offsetRangeLimit match {
      case EarliestOffsetRangeLimit => partitions.map {
        case tp => tp -> KafkaOffsetRangeLimit.EARLIEST
      }.toMap
      case LatestOffsetRangeLimit => partitions.map {
        case tp => tp -> KafkaOffsetRangeLimit.LATEST
      }.toMap
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        validateTopicPartitions(partitions, partitionOffsets)
      case SpecificTimestampRangeLimit(partitionTimestamps, strategy) =>
        fetchSpecificTimestampBasedOffsets(partitionTimestamps,
          isStartingOffsets, strategy).partitionToOffsets
      case GlobalTimestampRangeLimit(timestamp, strategy) =>
        fetchGlobalTimestampBasedOffsets(timestamp,
          isStartingOffsets, strategy).partitionToOffsets
    }
  }

  override def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long],
      reportDataLoss: (String, () => Throwable) => Unit): KafkaSourceOffset = {
    val fnAssertParametersWithPartitions: ju.Set[TopicPartition] => Unit = { partitions =>
      assert(partitions.asScala == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest, if you don't care.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions.asScala}")
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")
    }

    val fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long] = { _ =>
      partitionOffsets
    }

    val fnAssertFetchedOffsets: Map[TopicPartition, Long] => Unit = { fetched =>
      partitionOffsets.foreach {
        case (tp, off) if off != KafkaOffsetRangeLimit.LATEST &&
          off != KafkaOffsetRangeLimit.EARLIEST =>
          if (fetched(tp) != off) {
            reportDataLoss(
              s"startingOffsets for $tp was $off but consumer reset to ${fetched(tp)}",
              () =>
                KafkaExceptions.startOffsetReset(tp, off, fetched(tp)))
          }
        case _ =>
        // no real way to check that beginning or end is reasonable
      }
    }

    fetchSpecificOffsets0(fnAssertParametersWithPartitions, fnRetrievePartitionOffsets,
      fnAssertFetchedOffsets)
  }

  override def fetchSpecificTimestampBasedOffsets(
      partitionTimestamps: Map[TopicPartition, Long],
      isStartingOffsets: Boolean,
      strategyOnNoMatchStartingOffset: StrategyOnNoMatchStartingOffset.Value)
    : KafkaSourceOffset = {

    val fnAssertParametersWithPartitions: ju.Set[TopicPartition] => Unit = { partitions =>
      assert(partitions.asScala == partitionTimestamps.keySet,
        "If starting/endingOffsetsByTimestamp contains specific offsets, you must specify all " +
          s"topics. Specified: ${partitionTimestamps.keySet} Assigned: ${partitions.asScala}")
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionTimestamps")
    }

    val fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long] = { _ =>
      val converted = partitionTimestamps.map { case (tp, timestamp) =>
        tp -> java.lang.Long.valueOf(timestamp)
      }.asJava

      val offsetForTime: ju.Map[TopicPartition, OffsetAndTimestamp] =
        consumer.offsetsForTimes(converted)

      readTimestampOffsets(
        offsetForTime.asScala.toMap,
        isStartingOffsets,
        strategyOnNoMatchStartingOffset,
        partitionTimestamps)
    }

    val fnAssertFetchedOffsets: Map[TopicPartition, Long] => Unit = { _ => }

    fetchSpecificOffsets0(fnAssertParametersWithPartitions, fnRetrievePartitionOffsets,
      fnAssertFetchedOffsets)
  }

  override def fetchGlobalTimestampBasedOffsets(
      timestamp: Long,
      isStartingOffsets: Boolean,
      strategyOnNoMatchStartingOffset: StrategyOnNoMatchStartingOffset.Value)
    : KafkaSourceOffset = {

    val fnAssertParametersWithPartitions: ju.Set[TopicPartition] => Unit = { partitions =>
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $timestamp")
    }

    val fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long] = { tps =>
      val converted = tps.asScala.map(_ -> java.lang.Long.valueOf(timestamp)).toMap.asJava

      val offsetForTime: ju.Map[TopicPartition, OffsetAndTimestamp] =
        consumer.offsetsForTimes(converted)

      readTimestampOffsets(
        offsetForTime.asScala.toMap,
        isStartingOffsets,
        strategyOnNoMatchStartingOffset,
        _ => timestamp)
    }

    val fnAssertFetchedOffsets: Map[TopicPartition, Long] => Unit = { _ => }

    fetchSpecificOffsets0(fnAssertParametersWithPartitions, fnRetrievePartitionOffsets,
      fnAssertFetchedOffsets)
  }

  private def readTimestampOffsets(
      tpToOffsetMap: Map[TopicPartition, OffsetAndTimestamp],
      isStartingOffsets: Boolean,
      strategyOnNoMatchStartingOffset: StrategyOnNoMatchStartingOffset.Value,
      partitionTimestampFn: TopicPartition => Long): Map[TopicPartition, Long] = {

    tpToOffsetMap.map { case (tp, offsetSpec) =>
      val offset = if (offsetSpec == null) {
        if (isStartingOffsets) {
          strategyOnNoMatchStartingOffset match {
            case StrategyOnNoMatchStartingOffset.ERROR =>
              // This is to match the old behavior - we used assert to check the condition.
              // scalastyle:off throwerror
              throw new AssertionError("No offset " +
                s"matched from request of topic-partition $tp and timestamp " +
                s"${partitionTimestampFn(tp)}.")
              // scalastyle:on throwerror

            case StrategyOnNoMatchStartingOffset.LATEST =>
              KafkaOffsetRangeLimit.LATEST
          }
        } else {
          KafkaOffsetRangeLimit.LATEST
        }
      } else {
        offsetSpec.offset()
      }

      tp -> offset
    }
  }

  private def fetchSpecificOffsets0(
      fnAssertParametersWithPartitions: ju.Set[TopicPartition] => Unit,
      fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long],
      fnAssertFetchedOffsets: Map[TopicPartition, Long] => Unit): KafkaSourceOffset = {
    val fetched = partitionsAssignedToConsumer {
      partitions => {
        fnAssertParametersWithPartitions(partitions)

        val partitionOffsets = fnRetrievePartitionOffsets(partitions)

        partitionOffsets.foreach {
          case (tp, KafkaOffsetRangeLimit.LATEST) =>
            consumer.seekToEnd(ju.Arrays.asList(tp))
          case (tp, KafkaOffsetRangeLimit.EARLIEST) =>
            consumer.seekToBeginning(ju.Arrays.asList(tp))
          case (tp, off) => consumer.seek(tp, off)
        }

        partitionOffsets.map {
          case (tp, _) => tp -> consumer.position(tp)
        }
      }
    }

    fnAssertFetchedOffsets(fetched)

    KafkaSourceOffset(fetched)
  }

  override def fetchEarliestOffsets(): Map[TopicPartition, Long] = partitionsAssignedToConsumer(
    partitions => {
      logDebug("Seeking to the beginning")

      consumer.seekToBeginning(partitions)
      val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
      logDebug(s"Got earliest offsets for partition : $partitionOffsets")
      partitionOffsets
    }, fetchingEarliestOffset = true)

  /**
   * Specific to `KafkaOffsetReaderConsumer`:
   * Kafka may return earliest offsets when we are requesting latest offsets if `poll` is called
   * right before `seekToEnd` (KAFKA-7703). As a workaround, we will call `position` right after
   * `poll` to wait until the potential offset request triggered by `poll(0)` is done.
   */
  override def fetchLatestOffsets(
      knownOffsets: Option[PartitionOffsetMap]): PartitionOffsetMap =
    partitionsAssignedToConsumer { partitions => {
      logDebug("Seeking to the end.")

      if (knownOffsets.isEmpty) {
        consumer.seekToEnd(partitions)
        partitions.asScala.map(p => p -> consumer.position(p)).toMap
      } else {
        var partitionOffsets: PartitionOffsetMap = Map.empty

        /**
         * Compare `knownOffsets` and `partitionOffsets`. Returns all partitions that have incorrect
         * latest offset (offset in `knownOffsets` is great than the one in `partitionOffsets`).
         */
        def findIncorrectOffsets(): Seq[(TopicPartition, Long, Long)] = {
          val incorrectOffsets = ArrayBuffer[(TopicPartition, Long, Long)]()
          partitionOffsets.foreach { case (tp, offset) =>
            knownOffsets.foreach(_.get(tp).foreach { knownOffset =>
              if (knownOffset > offset) {
                val incorrectOffset = (tp, knownOffset, offset)
                incorrectOffsets += incorrectOffset
              }
            })
          }
          incorrectOffsets.toSeq
        }

        // Retry to fetch latest offsets when detecting incorrect offsets. We don't use
        // `withRetriesWithoutInterrupt` to retry because:
        //
        // - `withRetriesWithoutInterrupt` will reset the consumer for each attempt but a fresh
        //    consumer has a much bigger chance to hit KAFKA-7703.
        // - Avoid calling `consumer.poll(0)` which may cause KAFKA-7703.
        var incorrectOffsets: Seq[(TopicPartition, Long, Long)] = Nil
        var attempt = 0
        do {
          consumer.seekToEnd(partitions)
          partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
          attempt += 1

          incorrectOffsets = findIncorrectOffsets()
          if (incorrectOffsets.nonEmpty) {
            logWarning(log"Found incorrect offsets in some partitions " +
              log"(partition, previous offset, fetched offset): ${MDC(OFFSETS, incorrectOffsets)}")
            if (attempt < maxOffsetFetchAttempts) {
              logWarning("Retrying to fetch latest offsets because of incorrect offsets")
              Thread.sleep(offsetFetchAttemptIntervalMs)
            }
          }
        } while (incorrectOffsets.nonEmpty && attempt < maxOffsetFetchAttempts)

        logDebug(s"Got latest offsets for partition : $partitionOffsets")
        partitionOffsets
      }
    }
  }

  override def fetchEarliestOffsets(
      newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      partitionsAssignedToConsumer(partitions => {
        // Get the earliest offset of each partition
        consumer.seekToBeginning(partitions)
        val partitionOffsets = newPartitions.filter { p =>
          // When deleting topics happen at the same time, some partitions may not be in
          // `partitions`. So we need to ignore them
          partitions.contains(p)
        }.map(p => p -> consumer.position(p)).toMap
        logDebug(s"Got earliest offsets for new partitions: $partitionOffsets")
        partitionOffsets
      }, fetchingEarliestOffset = true)
    }
  }

  override def getOffsetRangesFromUnresolvedOffsets(
      startingOffsets: KafkaOffsetRangeLimit,
      endingOffsets: KafkaOffsetRangeLimit): Seq[KafkaOffsetRange] = {
    val fromPartitionOffsets = fetchPartitionOffsets(startingOffsets, isStartingOffsets = true)
    val untilPartitionOffsets = fetchPartitionOffsets(endingOffsets, isStartingOffsets = false)

    // Obtain topicPartitions in both from and until partition offset, ignoring
    // topic partitions that were added and/or deleted between the two above calls.
    if (fromPartitionOffsets.keySet != untilPartitionOffsets.keySet) {
      implicit val topicOrdering: Ordering[TopicPartition] = Ordering.by(t => t.topic())
      val fromTopics = fromPartitionOffsets.keySet.toList.sorted.mkString(",")
      val untilTopics = untilPartitionOffsets.keySet.toList.sorted.mkString(",")
      throw new IllegalStateException("different topic partitions " +
        s"for starting offsets topics[${fromTopics}] and " +
        s"ending offsets topics[${untilTopics}]")
    }

    // Calculate offset ranges
    val offsetRangesBase = untilPartitionOffsets.keySet.map { tp =>
      val fromOffset = fromPartitionOffsets.getOrElse(tp,
        // This should not happen since topicPartitions contains all partitions not in
        // fromPartitionOffsets
        throw new IllegalStateException(s"$tp doesn't have a from offset"))
      val untilOffset = untilPartitionOffsets(tp)
      KafkaOffsetRange(tp, fromOffset, untilOffset, None)
    }.toSeq

    if (shouldDivvyUpLargePartitions(offsetRangesBase.size)) {
      val fromOffsetsMap =
        offsetRangesBase.map(range => (range.topicPartition, range.fromOffset)).toMap
      val untilOffsetsMap =
        offsetRangesBase.map(range => (range.topicPartition, range.untilOffset)).toMap

      // No need to report data loss here
      val resolvedFromOffsets =
        fetchSpecificOffsets(fromOffsetsMap, (_, _) => ()).partitionToOffsets
      val resolvedUntilOffsets =
        fetchSpecificOffsets(untilOffsetsMap, (_, _) => ()).partitionToOffsets
      val ranges = offsetRangesBase.map(_.topicPartition).map { tp =>
        KafkaOffsetRange(tp, resolvedFromOffsets(tp), resolvedUntilOffsets(tp), preferredLoc = None)
      }
      val divvied = rangeCalculator.getRanges(ranges).groupBy(_.topicPartition)
      divvied.flatMap { case (tp, splitOffsetRanges) =>
        if (splitOffsetRanges.length == 1) {
          Seq(KafkaOffsetRange(tp, fromOffsetsMap(tp), untilOffsetsMap(tp), None))
        } else {
          // the list can't be empty
          val first = splitOffsetRanges.head.copy(fromOffset = fromOffsetsMap(tp))
          val end = splitOffsetRanges.last.copy(untilOffset = untilOffsetsMap(tp))
          Seq(first) ++ splitOffsetRanges.drop(1).dropRight(1) :+ end
        }
      }.toArray.toImmutableArraySeq
    } else {
      offsetRangesBase
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

  override def getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets: PartitionOffsetMap,
      untilPartitionOffsets: PartitionOffsetMap,
      reportDataLoss: (String, () => Throwable) => Unit): Seq[KafkaOffsetRange] = {
    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionInitialOffsets = fetchEarliestOffsets(newPartitions.toSeq)
    if (newPartitionInitialOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionInitialOffsets.keySet)
      reportDataLoss(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed",
        () =>
          KafkaExceptions.initialOffsetNotFoundForPartitions(deletedPartitions))
    }
    logInfo(log"Partitions added: ${MDC(TOPIC_PARTITION_OFFSET, newPartitionInitialOffsets)}")
    newPartitionInitialOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      reportDataLoss(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed",
        () => KafkaExceptions.addedPartitionDoesNotStartFromZero(p, o))
    }

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      val (message, config) =
        if (driverKafkaParams.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
          (s"$deletedPartitions are gone.${KafkaSourceProvider.CUSTOM_GROUP_ID_ERROR_MESSAGE}",
            Some(ConsumerConfig.GROUP_ID_CONFIG))
        } else {
          (s"$deletedPartitions are gone. Some data may have been missed.", None)
        }

      reportDataLoss(
        message,
        () =>
          KafkaExceptions.partitionsDeleted(deletedPartitions, config))
    }

    // Use the until partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = untilPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionInitialOffsets.contains(tp) || fromPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    val fromOffsets = fromPartitionOffsets ++ newPartitionInitialOffsets
    val untilOffsets = untilPartitionOffsets
    val ranges = topicPartitions.map { tp =>
      val fromOffset = fromOffsets(tp)
      val untilOffset = untilOffsets(tp)
      if (untilOffset < fromOffset) {
        reportDataLoss(
          s"Partition $tp's offset was changed from " +
            s"$fromOffset to $untilOffset, some data may have been missed",
          () =>
            KafkaExceptions.partitionOffsetChanged(tp, fromOffset, untilOffset))
      }
      KafkaOffsetRange(tp, fromOffset, untilOffset, preferredLoc = None)
    }
    rangeCalculator.getRanges(ranges, getSortedExecutorList().toImmutableArraySeq)
  }

  private def partitionsAssignedToConsumer(
      body: ju.Set[TopicPartition] => Map[TopicPartition, Long],
      fetchingEarliestOffset: Boolean = false)
    : Map[TopicPartition, Long] = uninterruptibleThreadRunner.runUninterruptibly {

    withRetriesWithoutInterrupt {
      // Poll to get the latest assigned partitions
      consumer.poll(0)
      val partitions = consumer.assignment()

      if (!fetchingEarliestOffset) {
        // Call `position` to wait until the potential offset request triggered by `poll(0)` is
        // done. This is a workaround for KAFKA-7703, which an async `seekToBeginning` triggered by
        // `poll(0)` may reset offsets that should have been set by another request.
        partitions.asScala.map(p => p -> consumer.position(p)).foreach(_ => {})
      }

      consumer.pause(partitions)
      logDebug(s"Partitions assigned to consumer: $partitions.")
      body(partitions)
    }
  }

  /**
   * Helper function that does multiple retries on a body of code that returns offsets.
   * Retries are needed to handle transient failures. For e.g. race conditions between getting
   * assignment and getting position while topics/partitions are deleted can cause NPEs.
   *
   * This method also makes sure `body` won't be interrupted to workaround a potential issue in
   * `KafkaConsumer.poll`. (KAFKA-1894)
   */
  private def withRetriesWithoutInterrupt(
      body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    // Make sure `KafkaConsumer.poll` won't be interrupted (KAFKA-1894)
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

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
                case NonFatal(e) =>
                  lastException = e
                  logWarning(
                    log"Error in attempt ${MDC(RETRY_COUNT, attempt)} getting Kafka offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
                  resetConsumer()
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

  private def stopConsumer(): Unit = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    if (_consumer != null) _consumer.close()
  }

  private def resetConsumer(): Unit = synchronized {
    stopConsumer()
    _consumer = null  // will automatically get reinitialized again
  }
}
