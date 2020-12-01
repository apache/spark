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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.kafka.clients.admin.{Admin, ListOffsetsOptions, OffsetSpec}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.common.requests.OffsetFetchResponse

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.util.{UninterruptibleThread, UninterruptibleThreadRunner}

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
   * [[UninterruptibleThreadRunner]] ensures that all [[Admin]] communication called in an
   * [[UninterruptibleThread]]. In the case of streaming queries, we are already running in an
   * [[UninterruptibleThread]], however for batch mode this is not the case.
   */
  val uninterruptibleThreadRunner = new UninterruptibleThreadRunner("Kafka Offset Reader")

  /**
   * An AdminClient used in the driver to query the latest Kafka offsets.
   * This only queries the offsets because AdminClient has no functionality to commit offsets like
   * KafkaConsumer.
   */
  @volatile protected var _admin: Admin = null

  protected def admin: Admin = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
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

  private val rangeCalculator = new KafkaOffsetRangeCalculator(minPartitions)

  /**
   * Whether we should divide Kafka TopicPartitions with a lot of data into smaller Spark tasks.
   */
  private def shouldDivvyUpLargePartitions(numTopicPartitions: Int): Boolean = {
    minPartitions.map(_ > numTopicPartitions).getOrElse(false)
  }

  override def toString(): String = consumerStrategy.toString

  /**
   * Closes the connection to Kafka, and cleans up state.
   */
  override def close(): Unit = {
    if (_admin != null) uninterruptibleThreadRunner.runUninterruptibly { stopAdmin() }
    uninterruptibleThreadRunner.shutdown()
  }

  /**
   * Fetch the partition offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]] and [[KafkaOffsetRangeLimit]].
   */
  override def fetchPartitionOffsets(
      offsetRangeLimit: KafkaOffsetRangeLimit,
      isStartingOffsets: Boolean): Map[TopicPartition, Long] = {
    def validateTopicPartitions(partitions: Set[TopicPartition],
      partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
      assert(partitions == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions}")
      logDebug(s"Assigned partitions: $partitions. Seeking to $partitionOffsets")
      partitionOffsets
    }
    val partitions = uninterruptibleThreadRunner.runUninterruptibly {
      consumerStrategy.assignedTopicPartitions(admin)
    }
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
      case SpecificTimestampRangeLimit(partitionTimestamps) =>
        fetchSpecificTimestampBasedOffsets(partitionTimestamps,
          failsOnNoMatchingOffset = isStartingOffsets).partitionToOffsets
    }
  }

  /**
   * Resolves the specific offsets based on Kafka seek positions.
   * This method resolves offset value -1 to the latest and -2 to the
   * earliest Kafka seek position.
   *
   * @param partitionOffsets the specific offsets to resolve
   * @param reportDataLoss callback to either report or log data loss depending on setting
   */
  override def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long],
      reportDataLoss: String => Unit): KafkaSourceOffset = {
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
      failsOnNoMatchingOffset: Boolean): KafkaSourceOffset = {
    val fnAssertParametersWithPartitions: ju.Set[TopicPartition] => Unit = { partitions =>
      assert(partitions.asScala == partitionTimestamps.keySet,
        "If starting/endingOffsetsByTimestamp contains specific offsets, you must specify all " +
          s"topics. Specified: ${partitionTimestamps.keySet} Assigned: ${partitions.asScala}")
      logDebug(s"Assigned partitions: $partitions. Seeking to $partitionTimestamps")
    }

    val fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long] = { _ => {
        val listOffsetsParams = partitionTimestamps.map { case (tp, timestamp) =>
          tp -> OffsetSpec.forTimestamp(timestamp)
        }.asJava
        admin.listOffsets(listOffsetsParams, listOffsetsOptions).all().get().asScala.map {
          case (tp, offsetSpec) =>
            if (failsOnNoMatchingOffset) {
              assert(offsetSpec.offset() != OffsetFetchResponse.INVALID_OFFSET, "No offset " +
                s"matched from request of topic-partition $tp and timestamp " +
                s"${partitionTimestamps(tp)}.")
            }

            if (offsetSpec.offset() == OffsetFetchResponse.INVALID_OFFSET) {
              tp -> KafkaOffsetRangeLimit.LATEST
            } else {
              tp -> offsetSpec.offset()
            }
        }.toMap
      }
    }

    fetchSpecificOffsets0(fnAssertParametersWithPartitions, fnRetrievePartitionOffsets)
  }

  private def fetchSpecificOffsets0(
      fnAssertParametersWithPartitions: ju.Set[TopicPartition] => Unit,
      fnRetrievePartitionOffsets: ju.Set[TopicPartition] => Map[TopicPartition, Long]
    ): KafkaSourceOffset = {
    val fetched = partitionsAssignedToConsumer {
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

  /**
   * Fetch the earliest offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]].
   */
  override def fetchEarliestOffsets(): Map[TopicPartition, Long] = partitionsAssignedToConsumer(
    partitions => {
      val listOffsetsParams = partitions.asScala.map(p => p -> OffsetSpec.earliest()).toMap.asJava
      val partitionOffsets = listOffsets(admin, listOffsetsParams)
      logDebug(s"Got earliest offsets for partitions: $partitionOffsets")
      partitionOffsets
    })

  /**
   * Fetch the latest offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]].
   *
   * Kafka may return earliest offsets when we are requesting latest offsets if `poll` is called
   * right before `seekToEnd` (KAFKA-7703). As a workaround, we will call `position` right after
   * `poll` to wait until the potential offset request triggered by `poll(0)` is done.
   *
   * In addition, to avoid other unknown issues, we also use the given `knownOffsets` to audit the
   * latest offsets returned by Kafka. If we find some incorrect offsets (a latest offset is less
   * than an offset in `knownOffsets`), we will retry at most `maxOffsetFetchAttempts` times. When
   * a topic is recreated, the latest offsets may be less than offsets in `knownOffsets`. We cannot
   * distinguish this with KAFKA-7703, so we just return whatever we get from Kafka after retrying.
   */
  override def fetchLatestOffsets(
      knownOffsets: Option[PartitionOffsetMap]): PartitionOffsetMap =
    partitionsAssignedToConsumer { partitions => {
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
          var incorrectOffsets = ArrayBuffer[(TopicPartition, Long, Long)]()
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
        // `withRetriesWithoutInterrupt` to retry because:
        //
        // - `withRetriesWithoutInterrupt` will reset the consumer for each attempt but a fresh
        //    consumer has a much bigger chance to hit KAFKA-7703.
        // - Avoid calling `consumer.poll(0)` which may cause KAFKA-7703.
        var incorrectOffsets: Seq[(TopicPartition, Long, Long)] = Nil
        var attempt = 0
        do {
          partitionOffsets = listOffsets(admin, listOffsetsParams)
          attempt += 1

          incorrectOffsets = findIncorrectOffsets()
          if (incorrectOffsets.nonEmpty) {
            logWarning("Found incorrect offsets in some partitions " +
              s"(partition, previous offset, fetched offset): $incorrectOffsets")
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

  /**
   * Fetch the earliest offsets for specific topic partitions.
   * The return result may not contain some partitions if they are deleted.
   */
  override def fetchEarliestOffsets(
      newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      partitionsAssignedToConsumer(partitions => {
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

  /**
   * Return the offset ranges for a Kafka batch query. If `minPartitions` is set, this method may
   * split partitions to respect it. Since offsets can be early and late binding which are evaluated
   * on the executors, in order to divvy up the partitions we need to perform some substitutions. We
   * don't want to send exact offsets to the executors, because data may age out before we can
   * consume the data. This method makes some approximate splitting, and replaces the special offset
   * values in the final output.
   */
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
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse {
        // This should not happen since topicPartitions contains all partitions not in
        // fromPartitionOffsets
        throw new IllegalStateException(s"$tp doesn't have a from offset")
      }
      val untilOffset = untilPartitionOffsets(tp)
      KafkaOffsetRange(tp, fromOffset, untilOffset, None)
    }.toSeq

    if (shouldDivvyUpLargePartitions(offsetRangesBase.size)) {
      val fromOffsetsMap =
        offsetRangesBase.map(range => (range.topicPartition, range.fromOffset)).toMap
      val untilOffsetsMap =
        offsetRangesBase.map(range => (range.topicPartition, range.untilOffset)).toMap

      // No need to report data loss here
      val resolvedFromOffsets = fetchSpecificOffsets(fromOffsetsMap, _ => ()).partitionToOffsets
      val resolvedUntilOffsets = fetchSpecificOffsets(untilOffsetsMap, _ => ()).partitionToOffsets
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
      }.toArray.toSeq
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

  /**
   * Return the offset ranges for a Kafka streaming batch. If `minPartitions` is set, this method
   * may split partitions to respect it. If any data lost issue is detected, `reportDataLoss` will
   * be called.
   */
  override def getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets: PartitionOffsetMap,
      untilPartitionOffsets: PartitionOffsetMap,
      reportDataLoss: String => Unit): Seq[KafkaOffsetRange] = {
    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionInitialOffsets = fetchEarliestOffsets(newPartitions.toSeq)
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

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      val message = if (driverKafkaParams.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
        s"$deletedPartitions are gone. ${KafkaSourceProvider.CUSTOM_GROUP_ID_ERROR_MESSAGE}"
      } else {
        s"$deletedPartitions are gone. Some data may have been missed."
      }
      reportDataLoss(message)
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
        reportDataLoss(s"Partition $tp's offset was changed from " +
          s"$fromOffset to $untilOffset, some data may have been missed")
      }
      KafkaOffsetRange(tp, fromOffset, untilOffset, preferredLoc = None)
    }
    rangeCalculator.getRanges(ranges, getSortedExecutorList)
  }

  private def partitionsAssignedToConsumer(
      body: ju.Set[TopicPartition] => Map[TopicPartition, Long])
    : Map[TopicPartition, Long] = uninterruptibleThreadRunner.runUninterruptibly {

    withRetriesWithoutInterrupt {
      val partitions = consumerStrategy.assignedTopicPartitions(admin).asJava
      logDebug(s"Partitions assigned: $partitions.")
      body(partitions)
    }
  }

  /**
   * Helper function that does multiple retries on a body of code that returns offsets.
   * Retries are needed to handle transient failures. For e.g. race conditions between getting
   * assignment and getting position while topics/partitions are deleted can cause NPEs.
   *
   * This method also makes sure `body` won't be interrupted to workaround similar issues like in
   * `KafkaConsumer.poll`. (KAFKA-1894)
   */
  private def withRetriesWithoutInterrupt(
      body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    synchronized {
      var result: Option[Map[TopicPartition, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts
        && !Thread.currentThread().isInterrupted) {
        Thread.currentThread match {
          case ut: UninterruptibleThread =>
            ut.runUninterruptibly {
              try {
                result = Some(body)
              } catch {
                case NonFatal(e) =>
                  lastException = e
                  logWarning(s"Error in attempt $attempt getting Kafka offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
                  resetAdmin()
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

  private def stopAdmin(): Unit = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    if (_admin != null) _admin.close()
  }

  private def resetAdmin(): Unit = synchronized {
    stopAdmin()
    _admin = null  // will automatically get reinitialized again
  }
}
