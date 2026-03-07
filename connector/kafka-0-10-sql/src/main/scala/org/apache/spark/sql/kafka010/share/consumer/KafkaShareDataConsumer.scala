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

package org.apache.spark.sql.kafka010.share.consumer

import java.{util => ju}
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaShareConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.kafka010.{KafkaConfigUpdater, KafkaTokenUtil}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.kafka010.share._

/**
 * Internal wrapper around Kafka's [[KafkaShareConsumer]] for use in Spark executors.
 *
 * Unlike the traditional [[org.apache.spark.sql.kafka010.consumer.InternalKafkaConsumer]],
 * this consumer:
 * 1. Subscribes to topics (not assigned to specific partitions)
 * 2. Tracks acquired records for acknowledgment
 * 3. Supports explicit acknowledgment with ACCEPT/RELEASE/REJECT semantics
 *
 * NOTE: Like KafkaShareConsumer, this class is not thread-safe.
 *
 * @param shareGroupId The share group identifier
 * @param topics Set of topics to subscribe to
 * @param kafkaParams Kafka consumer configuration parameters
 * @param lockTimeoutMs Acquisition lock timeout in milliseconds
 */
private[kafka010] class InternalKafkaShareConsumer(
    val shareGroupId: String,
    val topics: Set[String],
    val kafkaParams: ju.Map[String, Object],
    val lockTimeoutMs: Long = 30000L) extends Closeable with Logging {

  // Track acquired records for acknowledgment
  private val acquiredRecords = new ConcurrentHashMap[RecordKey, ShareInFlightRecord]()

  // Statistics
  private val totalRecordsPolled = new AtomicLong(0)
  private val totalRecordsAcknowledged = new AtomicLong(0)
  private val totalPollCalls = new AtomicInteger(0)

  // Exposed for testing
  private[consumer] val clusterConfig = KafkaTokenUtil.findMatchingTokenClusterConfig(
    SparkEnv.get.conf, kafkaParams.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      .asInstanceOf[String])

  // Kafka consumer - using var for token refresh support
  private[consumer] var kafkaParamsWithSecurity: ju.Map[String, Object] = _
  private var _consumer: KafkaShareConsumer[Array[Byte], Array[Byte]] = createConsumer()

  private def consumer: KafkaShareConsumer[Array[Byte], Array[Byte]] = _consumer

  /**
   * Poll for new records from subscribed topics.
   * Records are automatically acquired and tracked for acknowledgment.
   *
   * @param pollTimeoutMs Maximum time to block waiting for records
   * @return List of acquired records wrapped as ShareInFlightRecord
   */
  def poll(pollTimeoutMs: Long): Seq[ShareInFlightRecord] = {
    val records = consumer.poll(Duration.ofMillis(pollTimeoutMs))
    val acquiredAt = System.currentTimeMillis()
    val result = ArrayBuffer[ShareInFlightRecord]()

    totalPollCalls.incrementAndGet()

    records.forEach { record =>
      // Get delivery count from record headers if available (Kafka 4.x)
      val deliveryCount = getDeliveryCount(record)

      val inFlightRecord = ShareInFlightRecord(
        record = record,
        acquiredAt = acquiredAt,
        lockTimeoutMs = lockTimeoutMs,
        deliveryCount = deliveryCount
      )

      acquiredRecords.put(inFlightRecord.recordKey, inFlightRecord)
      result += inFlightRecord
      totalRecordsPolled.incrementAndGet()
    }

    logDebug(s"Polled ${result.size} records from share group $shareGroupId")
    result.toSeq
  }

  /**
   * Acknowledge a single record with the specified type.
   *
   * @param key The record key to acknowledge
   * @param ackType The acknowledgment type (ACCEPT, RELEASE, REJECT)
   * @return true if acknowledgment was recorded, false if record not found
   */
  def acknowledge(key: RecordKey, ackType: AcknowledgmentType.AcknowledgmentType): Boolean = {
    val record = acquiredRecords.get(key)
    if (record != null) {
      val updatedRecord = record.withAcknowledgment(ackType)
      acquiredRecords.put(key, updatedRecord)
      totalRecordsAcknowledged.incrementAndGet()

      // Apply acknowledgment to Kafka consumer
      ackType match {
        case AcknowledgmentType.ACCEPT =>
          consumer.acknowledge(record.record)
        case AcknowledgmentType.RELEASE =>
          consumer.acknowledge(record.record, org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE)
        case AcknowledgmentType.REJECT =>
          consumer.acknowledge(record.record, org.apache.kafka.clients.consumer.AcknowledgeType.REJECT)
      }
      true
    } else {
      logWarning(s"Record $key not found for acknowledgment")
      false
    }
  }

  /**
   * Acknowledge multiple records with the specified type.
   */
  def acknowledgeAll(
      keys: Iterable[RecordKey],
      ackType: AcknowledgmentType.AcknowledgmentType): Int = {
    keys.count(key => acknowledge(key, ackType))
  }

  /**
   * Acknowledge a record by offset for a specific TopicPartition.
   */
  def acknowledgeByOffset(
      tp: TopicPartition,
      offset: Long,
      ackType: AcknowledgmentType.AcknowledgmentType): Boolean = {
    acknowledge(RecordKey(tp, offset), ackType)
  }

  /**
   * Acknowledge all pending records as ACCEPT.
   * Used for implicit acknowledgment mode.
   */
  def acknowledgeAllPendingAsAccept(): Int = {
    var count = 0
    acquiredRecords.forEach { (key, record) =>
      if (record.acknowledgment.isEmpty) {
        acknowledge(key, AcknowledgmentType.ACCEPT)
        count += 1
      }
    }
    count
  }

  /**
   * Release all pending records (for failure recovery).
   * This allows Kafka to redeliver them to other consumers.
   */
  def releaseAllPending(): Int = {
    var count = 0
    acquiredRecords.forEach { (key, record) =>
      if (record.acknowledgment.isEmpty) {
        acknowledge(key, AcknowledgmentType.RELEASE)
        count += 1
      }
    }
    count
  }

  /**
   * Commit all pending acknowledgments synchronously.
   * This ensures durability of acknowledgments in Kafka.
   */
  def commitSync(): Unit = {
    consumer.commitSync()
    logDebug(s"Committed acknowledgments for share group $shareGroupId")
  }

  /**
   * Commit acknowledgments asynchronously.
   */
  def commitAsync(): Unit = {
    consumer.commitAsync()
  }

  /**
   * Get all acquired records that haven't been acknowledged yet.
   */
  def getPendingRecords: Seq[ShareInFlightRecord] = {
    acquiredRecords.values().asScala.filter(_.acknowledgment.isEmpty).toSeq
  }

  /**
   * Get acquired records grouped by TopicPartition.
   */
  def getRecordsByPartition: Map[TopicPartition, Seq[ShareInFlightRecord]] = {
    acquiredRecords.values().asScala.groupBy(_.topicPartition).toMap
  }

  /**
   * Get acknowledgments ready for commit, grouped by TopicPartition.
   */
  def getAcknowledgmentsForCommit: Map[TopicPartition, Map[Long, AcknowledgmentType.AcknowledgmentType]] = {
    acquiredRecords.values().asScala
      .filter(_.acknowledgment.isDefined)
      .groupBy(_.topicPartition)
      .map { case (tp, recs) =>
        tp -> recs.map(r => r.offset -> r.acknowledgment.get).toMap
      }
      .toMap
  }

  /**
   * Clear acknowledged records from tracking (after successful commit).
   */
  def clearAcknowledged(): Unit = {
    val keysToRemove = acquiredRecords.asScala
      .filter(_._2.acknowledgment.isDefined)
      .keys
      .toSeq
    keysToRemove.foreach(acquiredRecords.remove)
  }

  /**
   * Check if any records have expired locks.
   */
  def hasExpiredLocks: Boolean = {
    val now = System.currentTimeMillis()
    acquiredRecords.values().asScala.exists(_.isLockExpired(now))
  }

  /**
   * Get records with expired locks.
   */
  def getExpiredLockRecords: Seq[ShareInFlightRecord] = {
    val now = System.currentTimeMillis()
    acquiredRecords.values().asScala.filter(_.isLockExpired(now)).toSeq
  }

  /**
   * Get statistics about this consumer.
   */
  def getStats: ShareConsumerStats = ShareConsumerStats(
    shareGroupId = shareGroupId,
    totalRecordsPolled = totalRecordsPolled.get(),
    totalRecordsAcknowledged = totalRecordsAcknowledged.get(),
    totalPollCalls = totalPollCalls.get(),
    pendingRecords = acquiredRecords.values().asScala.count(_.acknowledgment.isEmpty),
    acknowledgedRecords = acquiredRecords.values().asScala.count(_.acknowledgment.isDefined)
  )

  override def close(): Unit = {
    // Release any pending records before closing
    val pendingCount = releaseAllPending()
    if (pendingCount > 0) {
      logWarning(s"Released $pendingCount pending records before closing share consumer")
      try {
        commitSync()
      } catch {
        case e: Exception =>
          logWarning(s"Failed to commit releases on close: ${e.getMessage}")
      }
    }
    consumer.close()
  }

  def wakeup(): Unit = {
    consumer.wakeup()
  }

  /** Create the underlying Kafka share consumer */
  private def createConsumer(): KafkaShareConsumer[Array[Byte], Array[Byte]] = {
    kafkaParamsWithSecurity = KafkaConfigUpdater("executor", kafkaParams.asScala.toMap)
      .setAuthenticationConfigIfNeeded(clusterConfig)
      .build()

    val c = new KafkaShareConsumer[Array[Byte], Array[Byte]](kafkaParamsWithSecurity)
    c.subscribe(topics.asJava)
    c
  }

  /**
   * Extract delivery count from record headers.
   * Kafka 4.x adds delivery count information to share group records.
   */
  private def getDeliveryCount(record: ConsumerRecord[Array[Byte], Array[Byte]]): Short = {
    // TODO: Extract from Kafka 4.x headers when available
    // For now, default to 1 (first delivery)
    1
  }
}

/**
 * Statistics for a share consumer.
 */
case class ShareConsumerStats(
    shareGroupId: String,
    totalRecordsPolled: Long,
    totalRecordsAcknowledged: Long,
    totalPollCalls: Int,
    pendingRecords: Int,
    acknowledgedRecords: Int)

/**
 * Configuration for acknowledgment behavior.
 */
sealed trait AcknowledgmentMode
object AcknowledgmentMode {
  /** Automatically acknowledge all records as ACCEPT when batch completes */
  case object Implicit extends AcknowledgmentMode
  /** Require explicit acknowledgment via foreachBatch or user code */
  case object Explicit extends AcknowledgmentMode

  def fromString(s: String): AcknowledgmentMode = s.toLowerCase match {
    case "implicit" | "auto" => Implicit
    case "explicit" | "manual" => Explicit
    case _ => throw new IllegalArgumentException(s"Unknown acknowledgment mode: $s")
  }
}

/**
 * Helper object for acquiring KafkaShareDataConsumer instances.
 * Manages consumer lifecycle including token refresh.
 */
private[kafka010] class KafkaShareDataConsumer(
    shareGroupId: String,
    topics: Set[String],
    kafkaParams: ju.Map[String, Object],
    lockTimeoutMs: Long,
    consumerPool: InternalKafkaShareConsumerPool) extends Logging {

  private val isTokenProviderEnabled =
    HadoopDelegationTokenManager.isServiceEnabled(SparkEnv.get.conf, "kafka")

  @volatile private[consumer] var _consumer: Option[InternalKafkaShareConsumer] = None

  // Tracking stats
  private var startTimestampNano: Long = System.nanoTime()
  private var totalTimeReadNanos: Long = 0
  private var totalRecordsRead: Long = 0

  /**
   * Get or create the internal consumer.
   */
  def getOrRetrieveConsumer(): InternalKafkaShareConsumer = {
    if (_consumer.isEmpty) {
      retrieveConsumer()
    }
    require(_consumer.isDefined, "Consumer must be defined")

    // Check if token refresh is needed
    if (isTokenProviderEnabled && KafkaTokenUtil.needTokenUpdate(
        _consumer.get.kafkaParamsWithSecurity, _consumer.get.clusterConfig)) {
      logDebug("Cached share consumer uses an old delegation token, invalidating.")
      releaseConsumer()
      consumerPool.invalidateKey(ShareConsumerCacheKey(shareGroupId, topics))
      retrieveConsumer()
    }

    _consumer.get
  }

  /**
   * Poll for records from the share group.
   */
  def poll(pollTimeoutMs: Long): Seq[ShareInFlightRecord] = {
    val consumer = getOrRetrieveConsumer()
    val startTime = System.nanoTime()
    val records = consumer.poll(pollTimeoutMs)
    totalTimeReadNanos += (System.nanoTime() - startTime)
    totalRecordsRead += records.size
    records
  }

  /**
   * Acknowledge a record.
   */
  def acknowledge(key: RecordKey, ackType: AcknowledgmentType.AcknowledgmentType): Boolean = {
    _consumer.exists(_.acknowledge(key, ackType))
  }

  /**
   * Commit acknowledgments synchronously.
   */
  def commitSync(): Unit = {
    _consumer.foreach(_.commitSync())
  }

  /**
   * Release the consumer back to the pool.
   */
  def release(): Unit = {
    val kafkaMeta = _consumer
      .map(c => s"shareGroupId=${c.shareGroupId} topics=${c.topics}")
      .getOrElse("")
    val walTime = System.nanoTime() - startTimestampNano

    val taskCtx = TaskContext.get()
    val taskContextInfo = if (taskCtx != null) {
      s" for taskId=${taskCtx.taskAttemptId()} partitionId=${taskCtx.partitionId()}."
    } else {
      "."
    }

    logInfo(s"From Kafka share group $kafkaMeta read $totalRecordsRead records, " +
      s"taking ${totalTimeReadNanos / NANOS_PER_MILLIS.toDouble} ms, " +
      s"during time span of ${walTime / NANOS_PER_MILLIS.toDouble} ms$taskContextInfo")

    releaseConsumer()
  }

  private def retrieveConsumer(): Unit = {
    _consumer = Option(consumerPool.borrowObject(
      ShareConsumerCacheKey(shareGroupId, topics),
      kafkaParams,
      lockTimeoutMs
    ))
    startTimestampNano = System.nanoTime()
    totalTimeReadNanos = 0
    totalRecordsRead = 0
    require(_consumer.isDefined, "borrowing consumer from pool must always succeed.")
  }

  private def releaseConsumer(): Unit = {
    if (_consumer.isDefined) {
      consumerPool.returnObject(_consumer.get)
      _consumer = None
    }
  }
}

/**
 * Cache key for share consumer pool.
 */
case class ShareConsumerCacheKey(shareGroupId: String, topics: Set[String]) {
  override def hashCode(): Int = shareGroupId.hashCode * 31 + topics.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case other: ShareConsumerCacheKey =>
      this.shareGroupId == other.shareGroupId && this.topics == other.topics
    case _ => false
  }
}

/**
 * Companion object for acquiring KafkaShareDataConsumer instances.
 */
object KafkaShareDataConsumer extends Logging {
  private val sparkConf = SparkEnv.get.conf
  private val consumerPool = new InternalKafkaShareConsumerPool(sparkConf)

  /**
   * Acquire a share data consumer for the given share group and topics.
   */
  def acquire(
      shareGroupId: String,
      topics: Set[String],
      kafkaParams: ju.Map[String, Object],
      lockTimeoutMs: Long = 30000L): KafkaShareDataConsumer = {
    if (TaskContext.get() != null && TaskContext.get().attemptNumber() >= 1) {
      // If this is a reattempt, invalidate cached consumer
      consumerPool.invalidateKey(ShareConsumerCacheKey(shareGroupId, topics))
    }

    new KafkaShareDataConsumer(shareGroupId, topics, kafkaParams, lockTimeoutMs, consumerPool)
  }
}

