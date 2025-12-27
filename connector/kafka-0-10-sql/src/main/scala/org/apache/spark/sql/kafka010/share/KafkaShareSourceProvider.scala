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

package org.apache.spark.sql.kafka010.share

import java.{util => ju}
import java.util.{Locale, UUID}

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.KafkaConfigUpdater
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.kafka010.share.consumer.AcknowledgmentMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Provider for Kafka Share Group data source.
 *
 * This provider enables Spark Structured Streaming to consume from Kafka
 * using the share group protocol introduced in Kafka 4.x (KIP-932).
 *
 * Key differences from the standard Kafka source:
 * 1. Uses share groups instead of consumer groups
 * 2. Multiple consumers can read from the same partition concurrently
 * 3. Acknowledgment-based delivery (ACCEPT/RELEASE/REJECT)
 * 4. Non-sequential offset tracking
 *
 * Usage:
 * {{{
 * spark.readStream
 *   .format("kafka-share")
 *   .option("kafka.bootstrap.servers", "localhost:9092")
 *   .option("kafka.share.group.id", "my-share-group")
 *   .option("subscribe", "topic1,topic2")
 *   .load()
 * }}}
 */
private[kafka010] class KafkaShareSourceProvider
    extends DataSourceRegister
    with SimpleTableProvider
    with Logging {

  import KafkaShareSourceProvider._

  override def shortName(): String = "kafka-share"

  override def getTable(options: CaseInsensitiveStringMap): KafkaShareTable = {
    val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)
    new KafkaShareTable(includeHeaders)
  }
}

/**
 * Table implementation for Kafka Share source.
 */
class KafkaShareTable(includeHeaders: Boolean)
    extends Table with SupportsRead {

  import KafkaShareSourceProvider._

  override def name(): String = "KafkaShareTable"

  override def schema(): StructType = KafkaShareRecordSchema.getSchema(includeHeaders)

  override def capabilities(): ju.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    () => new KafkaShareScan(options, includeHeaders)
  }
}

/**
 * Scan implementation for Kafka Share source.
 */
class KafkaShareScan(options: CaseInsensitiveStringMap, includeHeaders: Boolean)
    extends Scan {

  override def readSchema(): StructType = KafkaShareRecordSchema.getSchema(includeHeaders)

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options.asCaseSensitiveMap().asScala.toMap)
    validateOptions(caseInsensitiveOptions)

    val shareGroupId = getShareGroupId(caseInsensitiveOptions)
    val topics = getTopics(caseInsensitiveOptions)
    val executorKafkaParams = kafkaParamsForExecutors(caseInsensitiveOptions, shareGroupId)
    val acknowledgmentMode = getAcknowledgmentMode(caseInsensitiveOptions)
    val exactlyOnceStrategy = getExactlyOnceStrategy(caseInsensitiveOptions)

    new KafkaShareMicroBatchStream(
      shareGroupId = shareGroupId,
      topics = topics,
      executorKafkaParams = executorKafkaParams,
      options = options,
      metadataPath = checkpointLocation,
      acknowledgmentMode = acknowledgmentMode,
      exactlyOnceStrategy = exactlyOnceStrategy
    )
  }

  private def validateOptions(params: CaseInsensitiveMap[String]): Unit = {
    // Validate bootstrap servers
    if (!params.contains(s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Option 'kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}' must be specified")
    }

    // Validate share group ID
    if (!params.contains(SHARE_GROUP_ID) && !params.contains(s"kafka.$SHARE_GROUP_ID")) {
      throw new IllegalArgumentException(
        s"Option '$SHARE_GROUP_ID' must be specified for Kafka share source")
    }

    // Validate topics
    val hasSubscribe = params.contains(SUBSCRIBE)
    val hasSubscribePattern = params.contains(SUBSCRIBE_PATTERN)

    if (!hasSubscribe && !hasSubscribePattern) {
      throw new IllegalArgumentException(
        s"One of '$SUBSCRIBE' or '$SUBSCRIBE_PATTERN' must be specified")
    }

    if (hasSubscribe && hasSubscribePattern) {
      throw new IllegalArgumentException(
        s"Only one of '$SUBSCRIBE' or '$SUBSCRIBE_PATTERN' can be specified")
    }

    // Validate acknowledgment mode
    params.get(ACKNOWLEDGMENT_MODE).foreach { mode =>
      try {
        AcknowledgmentMode.fromString(mode)
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid acknowledgment mode: '$mode'. Must be 'implicit' or 'explicit'")
      }
    }

    // Validate exactly-once strategy
    params.get(EXACTLY_ONCE_STRATEGY).foreach { strategy =>
      try {
        ExactlyOnceStrategy.fromString(strategy)
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid exactly-once strategy: '$strategy'. " +
            "Must be 'none', 'idempotent', 'two-phase-commit', or 'checkpoint-dedup'")
      }
    }

    // Warn about unsupported options from traditional Kafka source
    val unsupportedOptions = Seq(
      "startingOffsets", "endingOffsets", "startingTimestamp", "endingTimestamp",
      "assign" // Share groups subscribe to topics, not assign partitions
    )
    unsupportedOptions.foreach { opt =>
      if (params.contains(opt)) {
        logWarning(s"Option '$opt' is not applicable for Kafka share source and will be ignored")
      }
    }
  }

  private def getShareGroupId(params: CaseInsensitiveMap[String]): String = {
    params.getOrElse(SHARE_GROUP_ID,
      params.getOrElse(s"kafka.$SHARE_GROUP_ID",
        throw new IllegalArgumentException(s"Share group ID not specified")))
  }

  private def getTopics(params: CaseInsensitiveMap[String]): Set[String] = {
    params.get(SUBSCRIBE) match {
      case Some(topics) =>
        topics.split(",").map(_.trim).filter(_.nonEmpty).toSet
      case None =>
        // Pattern subscription - return empty set, will be handled differently
        Set.empty
    }
  }

  private def getAcknowledgmentMode(params: CaseInsensitiveMap[String]): AcknowledgmentMode = {
    params.get(ACKNOWLEDGMENT_MODE)
      .map(AcknowledgmentMode.fromString)
      .getOrElse(AcknowledgmentMode.Implicit)
  }

  private def getExactlyOnceStrategy(params: CaseInsensitiveMap[String]): ExactlyOnceStrategy = {
    params.get(EXACTLY_ONCE_STRATEGY)
      .map(ExactlyOnceStrategy.fromString)
      .getOrElse(ExactlyOnceStrategy.None)
  }

  private def kafkaParamsForExecutors(
      params: CaseInsensitiveMap[String],
      shareGroupId: String): ju.Map[String, Object] = {
    val specifiedParams = params
      .filter { case (k, _) => k.toLowerCase(Locale.ROOT).startsWith("kafka.") }
      .map { case (k, v) => k.substring(6) -> v } // Remove "kafka." prefix

    KafkaConfigUpdater("executor", specifiedParams.toMap)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      .set("group.id", shareGroupId) // Share group ID
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .build()
  }
}

/**
 * Configuration options for Kafka Share source.
 */
object KafkaShareSourceProvider {
  // Topic subscription options
  val SUBSCRIBE = "subscribe"
  val SUBSCRIBE_PATTERN = "subscribePattern"

  // Share group options
  val SHARE_GROUP_ID = "kafka.share.group.id"
  val SHARE_LOCK_TIMEOUT = "kafka.share.lock.timeout.ms"
  val SHARE_PARALLELISM = "kafka.share.parallelism"

  // Acknowledgment options
  val ACKNOWLEDGMENT_MODE = "kafka.share.acknowledgment.mode"

  // Exactly-once options
  val EXACTLY_ONCE_STRATEGY = "kafka.share.exactly.once.strategy"
  val DEDUP_COLUMNS = "kafka.share.dedup.columns"

  // Consumer options
  val CONSUMER_POLL_TIMEOUT = "kafka.consumer.poll.timeout.ms"
  val MAX_RECORDS_PER_BATCH = "kafka.share.max.records.per.batch"

  // Output options
  val INCLUDE_HEADERS = "includeHeaders"

  // Default values
  val DEFAULT_LOCK_TIMEOUT_MS = 30000L
  val DEFAULT_POLL_TIMEOUT_MS = 512L
  val DEFAULT_ACKNOWLEDGMENT_MODE = "implicit"
  val DEFAULT_EXACTLY_ONCE_STRATEGY = "none"
}

