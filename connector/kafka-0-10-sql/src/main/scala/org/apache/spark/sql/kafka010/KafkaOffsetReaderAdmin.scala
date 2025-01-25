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
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.admin.{Admin, ListOffsetsOptions, ListOffsetsResult, OffsetSpec}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.common.requests.OffsetFetchResponse

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{NUM_RETRY, OFFSETS, TOPIC_PARTITION_OFFSET}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.kafka010.KafkaSourceProvider.StrategyOnNoMatchStartingOffset
import org.apache.spark.util.ArrayImplicits._

/**
 * This class uses Kafka's own [[Admin]] API to read data offsets from Kafka.
 * The [[ConsumerStrategy]] class defines which Kafka topics and partitions should be read
 * by this source. These strategies directly correspond to the different consumption options
 * in. This class is designed to return a configured [[Admin]] that is used by the
 * [[KafkaSource]] to query for the offsets. See the docs on
 * [[org.apache.spark.sql.kafka010.ConsumerStrategy]]
 * for more details.
 *
 * Note: This class is not ThreadSafe
 */
private[kafka010] class KafkaOffsetReaderAdmin(
    consumerStrategy: ConsumerStrategy,
    override val driverKafkaParams: ju.Map[String, Object],
    readerOptions: CaseInsensitiveMap[String],
    driverGroupIdPrefix: String) extends KafkaOffsetReader with Logging {

  private[kafka010] val maxOffsetFetchAttempts =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_NUM_RETRY, "3").toInt

  private[kafka010] val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_RETRY_INTERVAL_MS, "1000").toLong

  /**
   * An AdminClient used in the driver to query the latest Kafka offsets.
   * This only queries the offsets because AdminClient has no functionality to commit offsets like
   * KafkaConsumer.
   */
  @volatile protected var _admin: Admin = null

  protected def admin: Admin = synchronized {
    if (_admin == null) {
      _admin = consumerStrategy.createAdmin(driverKafkaParams)
    }
    _admin
  }

  lazy val isolationLevel: IsolationLevel = {
    Option(driverKafkaParams.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)) match {
      case Some(s: String) => IsolationLevel.valueOf(s.toUpperCase(Locale.ROOT))
      case None => IsolationLevel.valueOf(
        ConsumerConfig.DEFAULT_ISOLATION_LEVEL.toUpperCase(Locale.ROOT))
      case _ => throw new IllegalArgumentException(s"${ConsumerConfig.ISOLATION_LEVEL_CONFIG} " +
        "must be either not defined or with type String")
    }
  }

  private lazy val listOffsetsOptions = new ListOffsetsOptions(isolationLevel)

  private def listOffsets(admin: Admin, listOffsetsParams: ju.Map[TopicPartition, OffsetSpec]) = {
    admin.listOffsets(listOffsetsParams, listOffsetsOptions).all().get().asScala
      .map(result => result._1 -> result._2.offset()).toMap
  }

  /**
   * Number of partitions to read from Kafka. If this value is greater than the number of Kafka
   * topicPartitions, we will split up  the read tasks of the skewed partitions to multiple Spark
   * tasks. The number of Spark tasks will be *approximately* `numPartitions`. It can be less or
   * more depending on rounding errors or Kafka partitions that didn't receive any new data.
   */
  private val minPartitions =
    readerOptions.get(KafkaSourceProvider.MIN_PARTITIONS_OPTION_KEY).map(_.toInt)
  private val maxRecordsPerPartition =
    readerOptions.get(KafkaSourceProvider.MAX_RECORDS_PER_PARTITION_OPTION_KEY).map(_.toLong)

  private val rangeCalculator =
    new KafkaOffsetRangeCalculator(minPartitions, maxRecordsPerPartition)

  /**
   * Whether we should divide Kafka TopicPartitions with a lot of data into smaller Spark tasks.
   */
  private def shouldDivvyUpLargePartitions(offsetRanges: Seq[KafkaOffsetRange]): Boolean = {
    minPartitions.map(_ > offsetRanges.size).getOrElse(false) ||
    offsetRanges.exists(_.size > maxRecordsPerPartition.getOrElse(Long.MaxValue))
  }

  override def toString(): String = consumerStrategy.toString

  override def close(): Unit = {
    stopAdmin()
  }

  override def fetchPartitionOffsets(
      offsetRangeLimit: KafkaOffsetRangeLimit,
      isStartingOffsets: Boolean): Map[TopicPartition, Long] = {
    def validateTopicPartitions(partitions: Set[TopicPartition],
      partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
      if (partitions != partitionOffsets.keySet) {
        throw KafkaExceptions.startOffsetDoesNotMatchAssigned(partitionOffsets.keySet, partitions)
      }
      logDebug(s"Assigned partitions: $partitions. Seeking to $partitionOffsets")
      partitionOffsets
    }
    val partitions = consumerStrategy.assignedTopicPartitions(admin)
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
      case SpecificTimestampRangeLimit(partitionTimestamps, strategyOnNoMatchingStartingOffset) =>
        fetchSpecificTimestampBasedOffsets(partitionTimestamps, isStartingOffsets,
          strategyOnNoMatchingStartingOffset).partitionToOffsets
      case GlobalTimestampRangeLimit(timestamp, strategyOnNoMatchingStartingOffset) =>
        fetchGlobalTimestampBasedOffsets(timestamp, isStartingOffsets,
          strategyOnNoMatchingStartingOffset).partitionToOffsets
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
      logDebug(s"Assigned partitions: $partitions. Seeking to $partitionOffsets")
    }

    val fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long] = { _ =>
      partitionOffsets
    }

    fetchSpecificOffsets0(fnAssertParametersWithPartitions, fnRetrievePartitionOffsets)
  }

  override def fetchSpecificTimestampBasedOffsets(
      partitionTimestamps: Map[TopicPartition, Long],
      isStartingOffsets: Boolean,
      strategyOnNoMatchStartingOffset: StrategyOnNoMatchStartingOffset.Value)
    : KafkaSourceOffset = {

    val fnAssertParametersWithPartitions: ju.Set[TopicPartition] => Unit = { partitions =>
      val specifiedPartitions = partitionTimestamps.keySet
      val assignedPartitions = partitions.asScala.toSet
      if (specifiedPartitions != assignedPartitions) {
        throw KafkaExceptions.timestampOffsetDoesNotMatchAssigned(
          isStartingOffsets, specifiedPartitions, assignedPartitions)
      }
      logDebug(s"Assigned partitions: $partitions. Seeking to $partitionTimestamps")
    }

    val fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long] = { _ =>
      val listOffsetsParams = partitionTimestamps.map { case (tp, timestamp) =>
        tp -> OffsetSpec.forTimestamp(timestamp)
      }.asJava

      readTimestampOffsets(
        admin.listOffsets(listOffsetsParams, listOffsetsOptions).all().get().asScala.toMap,
        isStartingOffsets,
        strategyOnNoMatchStartingOffset,
        partitionTimestamps)
    }

    fetchSpecificOffsets0(fnAssertParametersWithPartitions, fnRetrievePartitionOffsets)
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
      val listOffsetsParams = tps.asScala.map { tp =>
        tp -> OffsetSpec.forTimestamp(timestamp)
      }.toMap.asJava

      readTimestampOffsets(
        admin.listOffsets(listOffsetsParams, listOffsetsOptions).all().get().asScala.toMap,
        isStartingOffsets,
        strategyOnNoMatchStartingOffset,
        _ => timestamp
      )
    }

    fetchSpecificOffsets0(fnAssertParametersWithPartitions, fnRetrievePartitionOffsets)
  }

  private def readTimestampOffsets(
      tpToOffsetMap: Map[TopicPartition, ListOffsetsResult.ListOffsetsResultInfo],
      isStartingOffsets: Boolean,
      strategyOnNoMatchStartingOffset: StrategyOnNoMatchStartingOffset.Value,
      partitionTimestampFn: TopicPartition => Long): Map[TopicPartition, Long] = {

    tpToOffsetMap.map { case (tp, offsetSpec) =>
      val offset = if (offsetSpec.offset() == OffsetFetchResponse.INVALID_OFFSET) {
        if (isStartingOffsets) {
          strategyOnNoMatchStartingOffset match {
            case StrategyOnNoMatchStartingOffset.ERROR =>
              throw new IllegalArgumentException("No offset " +
                s"matched from request of topic-partition $tp and timestamp " +
                s"${partitionTimestampFn(tp)}.")

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
      fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long]
    ): KafkaSourceOffset = {
    val fetched = partitionsAssignedToAdmin {
      partitions => {
        fnAssertParametersWithPartitions(partitions)

        val partitionOffsets = fnRetrievePartitionOffsets(partitions)

        val listOffsetsParams = partitionOffsets.filter { case (_, off) =>
          off == KafkaOffsetRangeLimit.LATEST || off == KafkaOffsetRangeLimit.EARLIEST
        }.map { case (tp, off) =>
          off match {
            case KafkaOffsetRangeLimit.LATEST =>
              tp -> OffsetSpec.latest()
            case KafkaOffsetRangeLimit.EARLIEST =>
              tp -> OffsetSpec.earliest()
          }
        }
        val resolvedPartitionOffsets = listOffsets(admin, listOffsetsParams.asJava)

        partitionOffsets.map { case (tp, off) =>
          off match {
            case KafkaOffsetRangeLimit.LATEST =>
              tp -> resolvedPartitionOffsets(tp)
            case KafkaOffsetRangeLimit.EARLIEST =>
              tp -> resolvedPartitionOffsets(tp)
            case _ =>
              tp -> off
          }
        }
      }
    }

    KafkaSourceOffset(fetched)
  }

  override def fetchEarliestOffsets(): Map[TopicPartition, Long] = partitionsAssignedToAdmin(
    partitions => {
      val listOffsetsParams = partitions.asScala.map(p => p -> OffsetSpec.earliest()).toMap.asJava
      val partitionOffsets = listOffsets(admin, listOffsetsParams)
      logDebug(s"Got earliest offsets for partitions: $partitionOffsets")
      partitionOffsets
    })

  override def fetchLatestOffsets(
      knownOffsets: Option[PartitionOffsetMap]): PartitionOffsetMap =
    partitionsAssignedToAdmin { partitions => {
      val listOffsetsParams = partitions.asScala.map(_ -> OffsetSpec.latest()).toMap.asJava
      if (knownOffsets.isEmpty) {
        val partitionOffsets = listOffsets(admin, listOffsetsParams)
        logDebug(s"Got latest offsets for partitions: $partitionOffsets")
        partitionOffsets
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
          // toSeq seems redundant but it's needed for Scala 2.13
          incorrectOffsets.toSeq
        }

        // Retry to fetch latest offsets when detecting incorrect offsets. We don't use
        // `withRetries` to retry because:
        //
        // - `withRetries` will reset the admin for each attempt but a fresh
        //    admin has a much bigger chance to hit KAFKA-7703 like issues.
        var incorrectOffsets: Seq[(TopicPartition, Long, Long)] = Nil
        var attempt = 0
        do {
          partitionOffsets = listOffsets(admin, listOffsetsParams)
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

        logDebug(s"Got latest offsets for partitions: $partitionOffsets")
        partitionOffsets
      }
    }
  }

  override def fetchEarliestOffsets(
      newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      partitionsAssignedToAdmin(partitions => {
        // Get the earliest offset of each partition
        val listOffsetsParams = newPartitions.filter { newPartition =>
          // When deleting topics happen at the same time, some partitions may not be in
          // `partitions`. So we need to ignore them
          partitions.contains(newPartition)
        }.map(partition => partition -> OffsetSpec.earliest()).toMap.asJava
        val partitionOffsets = listOffsets(admin, listOffsetsParams)
        logDebug(s"Got earliest offsets for new partitions: $partitionOffsets")
        partitionOffsets
      })
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
        throw new IllegalStateException(s"$tp doesn't have a from offset")
      )
      val untilOffset = untilPartitionOffsets(tp)
      KafkaOffsetRange(tp, fromOffset, untilOffset, None)
    }.toSeq

    if (shouldDivvyUpLargePartitions(offsetRangesBase)) {
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

  private def getSortedExecutorList: Array[String] = {
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
          (s"$deletedPartitions are gone. ${KafkaSourceProvider.CUSTOM_GROUP_ID_ERROR_MESSAGE}",
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
    rangeCalculator.getRanges(ranges, getSortedExecutorList.toImmutableArraySeq)
  }

  private def partitionsAssignedToAdmin(
      body: ju.Set[TopicPartition] => Map[TopicPartition, Long])
    : Map[TopicPartition, Long] = {

    withRetries {
      val partitions = consumerStrategy.assignedTopicPartitions(admin).asJava
      logDebug(s"Partitions assigned: $partitions.")
      body(partitions)
    }
  }

  /**
   * Helper function that does multiple retries on a body of code that returns offsets.
   * Retries are needed to handle transient failures. For e.g. race conditions between getting
   * assignment and getting position while topics/partitions are deleted can cause NPEs.
   */
  private def withRetries(body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    synchronized {
      var result: Option[Map[TopicPartition, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts
        && !Thread.currentThread().isInterrupted) {
        try {
          result = Some(body)
        } catch {
          case NonFatal(e) =>
            lastException = e
            logWarning(
              log"Error in attempt ${MDC(NUM_RETRY, attempt)} getting Kafka offsets: ", e)
            attempt += 1
            Thread.sleep(offsetFetchAttemptIntervalMs)
            resetAdmin()
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

  private def stopAdmin(): Unit = synchronized {
    if (_admin != null) _admin.close()
  }

  private def resetAdmin(): Unit = synchronized {
    stopAdmin()
    _admin = null  // will automatically get reinitialized again
  }
}
