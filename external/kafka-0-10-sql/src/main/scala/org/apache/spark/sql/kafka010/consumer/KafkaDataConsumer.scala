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

package org.apache.spark.sql.kafka010.consumer

import java.{util => ju}
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetOutOfRangeException}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.{KafkaConfigUpdater, KafkaTokenUtil}
import org.apache.spark.sql.kafka010.KafkaSourceProvider._
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer.{AvailableOffsetRange, UNKNOWN_OFFSET}
import org.apache.spark.util.{ShutdownHookManager, UninterruptibleThread}

/**
 * This class simplifies the usages of Kafka consumer in Spark SQL Kafka connector.
 *
 * NOTE: Like KafkaConsumer, this class is not thread-safe.
 * NOTE for contributors: It is possible for the instance to be used from multiple callers,
 * so all the methods should not rely on current cursor and use seek manually.
 */
private[kafka010] class InternalKafkaConsumer(
    val topicPartition: TopicPartition,
    val kafkaParams: ju.Map[String, Object]) extends Closeable with Logging {

  val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

  // Exposed for testing
  private[consumer] val clusterConfig = KafkaTokenUtil.findMatchingTokenClusterConfig(
    SparkEnv.get.conf, kafkaParams.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      .asInstanceOf[String])

  // Kafka consumer is not able to give back the params instantiated with so we need to store it.
  // It must be updated whenever a new consumer is created.
  // Exposed for testing
  private[consumer] var kafkaParamsWithSecurity: ju.Map[String, Object] = _
  private val consumer = createConsumer()

  /**
   * Poll messages from Kafka starting from `offset` and returns a pair of "list of consumer record"
   * and "offset after poll". The list of consumer record may be empty if the Kafka consumer fetches
   * some messages but all of them are not visible messages (either transaction messages,
   * or aborted messages when `isolation.level` is `read_committed`).
   *
   * @throws OffsetOutOfRangeException if `offset` is out of range.
   * @throws TimeoutException if the consumer position is not changed after polling. It means the
   *                          consumer polls nothing before timeout.
   */
  def fetch(offset: Long, pollTimeoutMs: Long):
      (ju.List[ConsumerRecord[Array[Byte], Array[Byte]]], Long, AvailableOffsetRange) = {

    // Seek to the offset because we may call seekToBeginning or seekToEnd before this.
    seek(offset)
    val p = consumer.poll(Duration.ofMillis(pollTimeoutMs))
    val r = p.records(topicPartition)
    logDebug(s"Polled $groupId ${p.partitions()}  ${r.size}")
    val offsetAfterPoll = consumer.position(topicPartition)
    logDebug(s"Offset changed from $offset to $offsetAfterPoll after polling")
    val range = getAvailableOffsetRange()
    val fetchedData = (r, offsetAfterPoll, range)
    if (r.isEmpty) {
      // We cannot fetch anything after `poll`. Two possible cases:
      // - `offset` is out of range so that Kafka returns nothing. `OffsetOutOfRangeException` will
      //   be thrown.
      // - Cannot fetch any data before timeout. `TimeoutException` will be thrown.
      // - Fetched something but all of them are not invisible. This is a valid case and let the
      //   caller handles this.
      if (offset < range.earliest || offset >= range.latest) {
        throw new OffsetOutOfRangeException(
          Map(topicPartition -> java.lang.Long.valueOf(offset)).asJava)
      } else if (offset == offsetAfterPoll) {
        throw new TimeoutException(
          s"Cannot fetch record for offset $offset in $pollTimeoutMs milliseconds")
      }
    }
    fetchedData
  }

  /**
   * Return the available offset range of the current partition. It's a pair of the earliest offset
   * and the latest offset.
   */
  def getAvailableOffsetRange(): AvailableOffsetRange = {
    consumer.seekToBeginning(Set(topicPartition).asJava)
    val earliestOffset = consumer.position(topicPartition)
    consumer.seekToEnd(Set(topicPartition).asJava)
    val latestOffset = consumer.position(topicPartition)
    AvailableOffsetRange(earliestOffset, latestOffset)
  }

  override def close(): Unit = {
    consumer.close()
  }

  /** Create a KafkaConsumer to fetch records for `topicPartition` */
  private def createConsumer(): KafkaConsumer[Array[Byte], Array[Byte]] = {
    kafkaParamsWithSecurity = KafkaConfigUpdater("executor", kafkaParams.asScala.toMap)
      .setAuthenticationConfigIfNeeded(clusterConfig)
      .build()
    val c = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParamsWithSecurity)
    val tps = new ju.ArrayList[TopicPartition]()
    tps.add(topicPartition)
    c.assign(tps)
    c
  }

  private def seek(offset: Long): Unit = {
    logDebug(s"Seeking to $groupId $topicPartition $offset")
    consumer.seek(topicPartition, offset)
  }
}

/**
 * The internal object to store the fetched data from Kafka consumer and the next offset to poll.
 *
 * @param _records the pre-fetched Kafka records.
 * @param _nextOffsetInFetchedData the next offset in `records`. We use this to verify if we
 *                                 should check if the pre-fetched data is still valid.
 * @param _offsetAfterPoll the Kafka offset after calling `poll`. We will use this offset to
 *                           poll when `records` is drained.
 * @param _availableOffsetRange the available offset range in Kafka when polling the records.
 */
private[consumer] case class FetchedData(
    private var _records: ju.ListIterator[ConsumerRecord[Array[Byte], Array[Byte]]],
    private var _nextOffsetInFetchedData: Long,
    private var _offsetAfterPoll: Long,
    private var _availableOffsetRange: AvailableOffsetRange) {

  def withNewPoll(
      records: ju.ListIterator[ConsumerRecord[Array[Byte], Array[Byte]]],
      offsetAfterPoll: Long,
      availableOffsetRange: AvailableOffsetRange): FetchedData = {
    this._records = records
    this._nextOffsetInFetchedData = UNKNOWN_OFFSET
    this._offsetAfterPoll = offsetAfterPoll
    this._availableOffsetRange = availableOffsetRange
    this
  }

  /** Whether there are more elements */
  def hasNext: Boolean = _records.hasNext

  /** Move `records` forward and return the next record. */
  def next(): ConsumerRecord[Array[Byte], Array[Byte]] = {
    val record = _records.next()
    _nextOffsetInFetchedData = record.offset + 1
    record
  }

  /** Move `records` backward and return the previous record. */
  def previous(): ConsumerRecord[Array[Byte], Array[Byte]] = {
    assert(_records.hasPrevious, "fetchedData cannot move back")
    val record = _records.previous()
    _nextOffsetInFetchedData = record.offset
    record
  }

  /** Reset the internal pre-fetched data. */
  def reset(): Unit = {
    _records = ju.Collections.emptyListIterator()
    _nextOffsetInFetchedData = UNKNOWN_OFFSET
    _offsetAfterPoll = UNKNOWN_OFFSET
    _availableOffsetRange = AvailableOffsetRange(UNKNOWN_OFFSET, UNKNOWN_OFFSET)
  }

  /**
   * Returns the next offset in `records`. We use this to verify if we should check if the
   * pre-fetched data is still valid.
   */
  def nextOffsetInFetchedData: Long = _nextOffsetInFetchedData

  /**
   * Returns the next offset to poll after draining the pre-fetched records.
   */
  def offsetAfterPoll: Long = _offsetAfterPoll

  /**
   * Returns the tuple of earliest and latest offsets that is the available offset range when
   * polling the records.
   */
  def availableOffsetRange: (Long, Long) =
    (_availableOffsetRange.earliest, _availableOffsetRange.latest)
}

/**
 * The internal object returned by the `fetchRecord` method. If `record` is empty, it means it is
 * invisible (either a transaction message, or an aborted message when the consumer's
 * `isolation.level` is `read_committed`), and the caller should use `nextOffsetToFetch` to fetch
 * instead.
 */
private[consumer] case class FetchedRecord(
    var record: ConsumerRecord[Array[Byte], Array[Byte]],
    var nextOffsetToFetch: Long) {

  def withRecord(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      nextOffsetToFetch: Long): FetchedRecord = {
    this.record = record
    this.nextOffsetToFetch = nextOffsetToFetch
    this
  }
}

/**
 * This class helps caller to read from Kafka leveraging consumer pool as well as fetched data pool.
 * This class throws error when data loss is detected while reading from Kafka.
 *
 * NOTE for contributors: we need to ensure all the public methods to initialize necessary resources
 * via calling `getOrRetrieveConsumer` and `getOrRetrieveFetchedData`.
 */
private[kafka010] class KafkaDataConsumer(
    topicPartition: TopicPartition,
    kafkaParams: ju.Map[String, Object],
    consumerPool: InternalKafkaConsumerPool,
    fetchedDataPool: FetchedDataPool) extends Logging {
  import KafkaDataConsumer._

  private val isTokenProviderEnabled =
    HadoopDelegationTokenManager.isServiceEnabled(SparkEnv.get.conf, "kafka")

  // Exposed for testing
  @volatile private[consumer] var _consumer: Option[InternalKafkaConsumer] = None
  @volatile private var _fetchedData: Option[FetchedData] = None

  private val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]
  private val cacheKey = CacheKey(groupId, topicPartition)

  /**
   * The fetched record returned from the `fetchRecord` method. This is a reusable private object to
   * avoid memory allocation.
   */
  private val fetchedRecord: FetchedRecord = FetchedRecord(null, UNKNOWN_OFFSET)

  /**
   * Get the record for the given offset if available.
   *
   * If the record is invisible (either a
   * transaction message, or an aborted message when the consumer's `isolation.level` is
   * `read_committed`), it will be skipped and this method will try to fetch next available record
   * within [offset, untilOffset).
   *
   * This method also will try its best to detect data loss. If `failOnDataLoss` is `true`, it will
   * throw an exception when we detect an unavailable offset. If `failOnDataLoss` is `false`, this
   * method will try to fetch next available record within [offset, untilOffset).
   *
   * When this method tries to skip offsets due to either invisible messages or data loss and
   * reaches `untilOffset`, it will return `null`.
   *
   * @param offset         the offset to fetch.
   * @param untilOffset    the max offset to fetch. Exclusive.
   * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
   * @param failOnDataLoss When `failOnDataLoss` is `true`, this method will either return record at
   *                       offset if available, or throw exception.when `failOnDataLoss` is `false`,
   *                       this method will either return record at offset if available, or return
   *                       the next earliest available record less than untilOffset, or null. It
   *                       will not throw any exception.
   */
  def get(
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long,
      failOnDataLoss: Boolean):
    ConsumerRecord[Array[Byte], Array[Byte]] = runUninterruptiblyIfPossible {
    require(offset < untilOffset,
      s"offset must always be less than untilOffset [offset: $offset, untilOffset: $untilOffset]")

    val consumer = getOrRetrieveConsumer()
    val fetchedData = getOrRetrieveFetchedData(offset)

    logDebug(s"Get $groupId $topicPartition nextOffset ${fetchedData.nextOffsetInFetchedData} " +
      s"requested $offset")

    // The following loop is basically for `failOnDataLoss = false`. When `failOnDataLoss` is
    // `false`, first, we will try to fetch the record at `offset`. If no such record exists, then
    // we will move to the next available offset within `[offset, untilOffset)` and retry.
    // If `failOnDataLoss` is `true`, the loop body will be executed only once.
    var toFetchOffset = offset
    var fetchedRecord: FetchedRecord = null
    // We want to break out of the while loop on a successful fetch to avoid using "return"
    // which may cause a NonLocalReturnControl exception when this method is used as a function.
    var isFetchComplete = false

    while (toFetchOffset != UNKNOWN_OFFSET && !isFetchComplete) {
      try {
        fetchedRecord = fetchRecord(consumer, fetchedData, toFetchOffset, untilOffset,
          pollTimeoutMs, failOnDataLoss)
        if (fetchedRecord.record != null) {
          isFetchComplete = true
        } else {
          toFetchOffset = fetchedRecord.nextOffsetToFetch
          if (toFetchOffset >= untilOffset) {
            fetchedData.reset()
            toFetchOffset = UNKNOWN_OFFSET
          } else {
            logDebug(s"Skipped offsets [$offset, $toFetchOffset]")
          }
        }
      } catch {
        case e: OffsetOutOfRangeException =>
          // When there is some error thrown, it's better to use a new consumer to drop all cached
          // states in the old consumer. We don't need to worry about the performance because this
          // is not a common path.
          releaseConsumer()
          fetchedData.reset()

          reportDataLoss(topicPartition, groupId, failOnDataLoss,
            s"Cannot fetch offset $toFetchOffset", e)
          toFetchOffset = getEarliestAvailableOffsetBetween(consumer, toFetchOffset, untilOffset)
      }
    }

    if (isFetchComplete) {
      fetchedRecord.record
    } else {
      fetchedData.reset()
      null
    }
  }

  /**
   * Return the available offset range of the current partition. It's a pair of the earliest offset
   * and the latest offset.
   */
  def getAvailableOffsetRange(): AvailableOffsetRange = runUninterruptiblyIfPossible {
    val consumer = getOrRetrieveConsumer()
    consumer.getAvailableOffsetRange()
  }

  /**
   * Release borrowed objects in data reader to the pool. Once the instance is created, caller
   * must call method after using the instance to make sure resources are not leaked.
   */
  def release(): Unit = {
    releaseConsumer()
    releaseFetchedData()
  }

  private def releaseConsumer(): Unit = {
    if (_consumer.isDefined) {
      consumerPool.returnObject(_consumer.get)
      _consumer = None
    }
  }

  private def releaseFetchedData(): Unit = {
    if (_fetchedData.isDefined) {
      fetchedDataPool.release(cacheKey, _fetchedData.get)
      _fetchedData = None
    }
  }

  /**
   * Return the next earliest available offset in [offset, untilOffset). If all offsets in
   * [offset, untilOffset) are invalid (e.g., the topic is deleted and recreated), it will return
   * `UNKNOWN_OFFSET`.
   */
  private def getEarliestAvailableOffsetBetween(
      consumer: InternalKafkaConsumer,
      offset: Long,
      untilOffset: Long): Long = {
    val range = consumer.getAvailableOffsetRange()
    logWarning(s"Some data may be lost. Recovering from the earliest offset: ${range.earliest}")

    val topicPartition = consumer.topicPartition
    val groupId = consumer.groupId
    if (offset >= range.latest || range.earliest >= untilOffset) {
      // [offset, untilOffset) and [earliestOffset, latestOffset) have no overlap,
      // either
      // --------------------------------------------------------
      //         ^                 ^         ^         ^
      //         |                 |         |         |
      //   earliestOffset   latestOffset   offset   untilOffset
      //
      // or
      // --------------------------------------------------------
      //      ^          ^              ^                ^
      //      |          |              |                |
      //   offset   untilOffset   earliestOffset   latestOffset
      val warningMessage =
      s"""
         |The current available offset range is $range.
         | Offset $offset is out of range, and records in [$offset, $untilOffset) will be
         | skipped ${additionalMessage(topicPartition, groupId, failOnDataLoss = false)}
        """.stripMargin
      logWarning(warningMessage)
      UNKNOWN_OFFSET
    } else if (offset >= range.earliest) {
      // -----------------------------------------------------------------------------
      //         ^            ^                  ^                                 ^
      //         |            |                  |                                 |
      //   earliestOffset   offset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      //
      // This will happen when a topic is deleted and recreated, and new data are pushed very fast,
      // then we will see `offset` disappears first then appears again. Although the parameters
      // are same, the state in Kafka cluster is changed, so the outer loop won't be endless.
      logWarning(s"Found a disappeared offset $offset. Some data may be lost " +
        s"${additionalMessage(topicPartition, groupId, failOnDataLoss = false)}")
      offset
    } else {
      // ------------------------------------------------------------------------------
      //      ^           ^                       ^                                 ^
      //      |           |                       |                                 |
      //   offset   earliestOffset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      val warningMessage =
      s"""
         |The current available offset range is $range.
         | Offset ${offset} is out of range, and records in [$offset, ${range.earliest}) will be
         | skipped ${additionalMessage(topicPartition, groupId, failOnDataLoss = false)}
        """.stripMargin
      logWarning(warningMessage)
      range.earliest
    }
  }

  /**
   * Get the fetched record for the given offset if available.
   *
   * If the record is invisible (either a  transaction message, or an aborted message when the
   * consumer's `isolation.level` is `read_committed`), it will return a `FetchedRecord` with the
   * next offset to fetch.
   *
   * This method also will try the best to detect data loss. If `failOnDataLoss` is `true`, it will
   * throw an exception when we detect an unavailable offset. If `failOnDataLoss` is `false`, this
   * method will return `null` if the next available record is within [offset, untilOffset).
   *
   * @throws OffsetOutOfRangeException if `offset` is out of range
   * @throws TimeoutException if cannot fetch the record in `pollTimeoutMs` milliseconds.
   */
  private def fetchRecord(
      consumer: InternalKafkaConsumer,
      fetchedData: FetchedData,
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long,
      failOnDataLoss: Boolean): FetchedRecord = {
    if (offset != fetchedData.nextOffsetInFetchedData) {
      // This is the first fetch, or the fetched data has been reset.
      // Fetch records from Kafka and update `fetchedData`.
      fetchData(consumer, fetchedData, offset, pollTimeoutMs)
    } else if (!fetchedData.hasNext) { // The last pre-fetched data has been drained.
      if (offset < fetchedData.offsetAfterPoll) {
        // Offsets in [offset, fetchedData.offsetAfterPoll) are invisible. Return a record to ask
        // the next call to start from `fetchedData.offsetAfterPoll`.
        val nextOffsetToFetch = fetchedData.offsetAfterPoll
        fetchedData.reset()
        return fetchedRecord.withRecord(null, nextOffsetToFetch)
      } else {
        // Fetch records from Kafka and update `fetchedData`.
        fetchData(consumer, fetchedData, offset, pollTimeoutMs)
      }
    }

    if (!fetchedData.hasNext) {
      // When we reach here, we have already tried to poll from Kafka. As `fetchedData` is still
      // empty, all messages in [offset, fetchedData.offsetAfterPoll) are invisible. Return a
      // record to ask the next call to start from `fetchedData.offsetAfterPoll`.
      assert(offset <= fetchedData.offsetAfterPoll,
        s"seek to $offset and poll but the offset was reset to ${fetchedData.offsetAfterPoll}")
      fetchedRecord.withRecord(null, fetchedData.offsetAfterPoll)
    } else {
      val record = fetchedData.next()
      // In general, Kafka uses the specified offset as the start point, and tries to fetch the next
      // available offset. Hence we need to handle offset mismatch.
      if (record.offset > offset) {
        val (earliestOffset, _) = fetchedData.availableOffsetRange
        if (earliestOffset <= offset) {
          // `offset` is still valid but the corresponding message is invisible. We should skip it
          // and jump to `record.offset`. Here we move `fetchedData` back so that the next call of
          // `fetchRecord` can just return `record` directly.
          fetchedData.previous()
          return fetchedRecord.withRecord(null, record.offset)
        }
        // This may happen when some records aged out but their offsets already got verified
        if (failOnDataLoss) {
          reportDataLoss(consumer.topicPartition, consumer.groupId, failOnDataLoss = true,
            s"Cannot fetch records in [$offset, ${record.offset})")
          // Never happen as "reportDataLoss" will throw an exception
          throw new IllegalStateException(
            "reportDataLoss didn't throw an exception when 'failOnDataLoss' is true")
        } else if (record.offset >= untilOffset) {
          reportDataLoss(consumer.topicPartition, consumer.groupId, failOnDataLoss = false,
            s"Skip missing records in [$offset, $untilOffset)")
          // Set `nextOffsetToFetch` to `untilOffset` to finish the current batch.
          fetchedRecord.withRecord(null, untilOffset)
        } else {
          reportDataLoss(consumer.topicPartition, consumer.groupId, failOnDataLoss = false,
            s"Skip missing records in [$offset, ${record.offset})")
          fetchedRecord.withRecord(record, fetchedData.nextOffsetInFetchedData)
        }
      } else if (record.offset < offset) {
        // This should not happen. If it does happen, then we probably misunderstand Kafka internal
        // mechanism.
        throw new IllegalStateException(
          s"Tried to fetch $offset but the returned record offset was ${record.offset}")
      } else {
        fetchedRecord.withRecord(record, fetchedData.nextOffsetInFetchedData)
      }
    }
  }

  /**
   * Poll messages from Kafka starting from `offset` and update `fetchedData`. `fetchedData` may be
   * empty if the Kafka consumer fetches some messages but all of them are not visible messages
   * (either transaction messages, or aborted messages when `isolation.level` is `read_committed`).
   *
   * @throws OffsetOutOfRangeException if `offset` is out of range.
   * @throws TimeoutException if the consumer position is not changed after polling. It means the
   *                          consumer polls nothing before timeout.
   */
  private def fetchData(
      consumer: InternalKafkaConsumer,
      fetchedData: FetchedData,
      offset: Long,
      pollTimeoutMs: Long): Unit = {
    val (records, offsetAfterPoll, range) = consumer.fetch(offset, pollTimeoutMs)
    fetchedData.withNewPoll(records.listIterator, offsetAfterPoll, range)
  }

  private[kafka010] def getOrRetrieveConsumer(): InternalKafkaConsumer = {
    if (!_consumer.isDefined) {
      retrieveConsumer()
    }
    require(_consumer.isDefined, "Consumer must be defined")
    if (isTokenProviderEnabled && KafkaTokenUtil.needTokenUpdate(
        _consumer.get.kafkaParamsWithSecurity, _consumer.get.clusterConfig)) {
      logDebug("Cached consumer uses an old delegation token, invalidating.")
      releaseConsumer()
      consumerPool.invalidateKey(cacheKey)
      fetchedDataPool.invalidate(cacheKey)
      retrieveConsumer()
    }
    _consumer.get
  }

  private def retrieveConsumer(): Unit = {
    _consumer = Option(consumerPool.borrowObject(cacheKey, kafkaParams))
    require(_consumer.isDefined, "borrowing consumer from pool must always succeed.")
  }

  private def getOrRetrieveFetchedData(offset: Long): FetchedData = _fetchedData match {
    case None =>
      _fetchedData = Option(fetchedDataPool.acquire(cacheKey, offset))
      require(_fetchedData.isDefined, "acquiring fetched data from cache must always succeed.")
      _fetchedData.get

    case Some(fetchedData) => fetchedData
  }

  /**
   * Return an addition message including useful message and instruction.
   */
  private def additionalMessage(
      topicPartition: TopicPartition,
      groupId: String,
      failOnDataLoss: Boolean): String = {
    if (failOnDataLoss) {
      s"(GroupId: $groupId, TopicPartition: $topicPartition). " +
        s"$INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE"
    } else {
      s"(GroupId: $groupId, TopicPartition: $topicPartition). " +
        s"$INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE"
    }
  }

  /**
   * Throw an exception or log a warning as per `failOnDataLoss`.
   */
  private def reportDataLoss(
      topicPartition: TopicPartition,
      groupId: String,
      failOnDataLoss: Boolean,
      message: String,
      cause: Throwable = null): Unit = {
    val finalMessage = s"$message ${additionalMessage(topicPartition, groupId, failOnDataLoss)}"
    reportDataLoss0(failOnDataLoss, finalMessage, cause)
  }

  private def runUninterruptiblyIfPossible[T](body: => T): T = Thread.currentThread match {
    case ut: UninterruptibleThread =>
      ut.runUninterruptibly(body)
    case _ =>
      logWarning("KafkaDataConsumer is not running in UninterruptibleThread. " +
        "It may hang when KafkaDataConsumer's methods are interrupted because of KAFKA-1894")
      body
  }
}

private[kafka010] object KafkaDataConsumer extends Logging {
  val UNKNOWN_OFFSET = -2L

  case class AvailableOffsetRange(earliest: Long, latest: Long)

  case class CacheKey(groupId: String, topicPartition: TopicPartition) {
    def this(topicPartition: TopicPartition, kafkaParams: ju.Map[String, Object]) =
      this(kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String], topicPartition)
  }

  private val sparkConf = SparkEnv.get.conf
  private val consumerPool = new InternalKafkaConsumerPool(sparkConf)
  private val fetchedDataPool = new FetchedDataPool(sparkConf)

  ShutdownHookManager.addShutdownHook { () =>
    try {
      fetchedDataPool.shutdown()
      consumerPool.close()
    } catch {
      case e: Throwable =>
        logWarning("Ignoring Exception while shutting down pools from shutdown hook", e)
    }
  }

  /**
   * Get a data reader for groupId, assigned to topic and partition.
   * If matching consumer doesn't already exist, will be created using kafkaParams.
   * The returned data reader must be released explicitly.
   */
  def acquire(
      topicPartition: TopicPartition,
      kafkaParams: ju.Map[String, Object]): KafkaDataConsumer = {
    if (TaskContext.get != null && TaskContext.get.attemptNumber >= 1) {
      val cacheKey = new CacheKey(topicPartition, kafkaParams)

      // If this is reattempt at running the task, then invalidate cached consumer if any.
      consumerPool.invalidateKey(cacheKey)

      // invalidate all fetched data for the key as well
      // sadly we can't pinpoint specific data and invalidate cause we don't have unique id
      fetchedDataPool.invalidate(cacheKey)
    }

    new KafkaDataConsumer(topicPartition, kafkaParams, consumerPool, fetchedDataPool)
  }

  private def reportDataLoss0(
      failOnDataLoss: Boolean,
      finalMessage: String,
      cause: Throwable = null): Unit = {
    if (failOnDataLoss) {
      if (cause != null) {
        throw new IllegalStateException(finalMessage, cause)
      } else {
        throw new IllegalStateException(finalMessage)
      }
    } else {
      if (cause != null) {
        logWarning(finalMessage, cause)
      } else {
        logWarning(finalMessage)
      }
    }
  }

}
