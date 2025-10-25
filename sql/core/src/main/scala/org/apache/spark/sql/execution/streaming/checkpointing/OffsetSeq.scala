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

package org.apache.spark.sql.execution.streaming.checkpointing

import scala.language.existentials

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, DEFAULT_VALUE, NEW_VALUE, OLD_VALUE, TIP}
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream}
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingAggregationStateManager
import org.apache.spark.sql.execution.streaming.operators.stateful.flatmapgroupswithstate.FlatMapGroupsWithStateExecHelper
import org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager
import org.apache.spark.sql.execution.streaming.runtime.{MultipleWatermarkPolicy, StreamProgress}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._


/**
 * A collection of named offsets, used to track the progress of processing data from one or more
 * [[Source]]s that are present in a streaming query. This is similar to simplified, single-instance
 * vector clock that must progress linearly forward.
 *
 * All sources are tracked by name - either user-provided names or auto-generated names
 * from sourceId.
 */
case class OffsetSeq(
    offsets: Map[String, OffsetV2],
    metadata: Option[OffsetSeqMetadata] = None) {

  /**
   * Unpacks offsets into [[StreamProgress]] by associating each named offset with the
   * corresponding source from the ordered list.
   *
   * @deprecated Use toStreamProgress(namedSources: Map[String, SparkDataStream]) instead
   */
  @deprecated("Use toStreamProgress with named sources map", "3.6.0")
  def toStreamProgress(sources: Seq[SparkDataStream]): StreamProgress = {
    val sourceOffsetPairs = sources.zipWithIndex.flatMap { case (source, index) =>
      offsets.get(index.toString).map(offset => (source, offset))
    }
    new StreamProgress ++ sourceOffsetPairs
  }

  /**
   * Unpacks named offsets into [[StreamProgress]] by associating each named offset with the
   * corresponding source.
   *
   * @param namedSources Map from source name to SparkDataStream
   * @return The StreamProgress containing the offset mapping
   */
  def toStreamProgress(namedSources: Map[String, SparkDataStream]): StreamProgress = {
    // Map existing checkpoint offsets to current sources
    val existingSourceOffsets = offsets.flatMap { case (name, offset) =>
      namedSources.get(name) match {
        case Some(source) => Some((source, offset))
        case None =>
          // Source from checkpoint no longer exists in current query - ignore
          None
      }
    }

    // For source evolution: identify new sources not in checkpoint and start them from beginning
    val newSources = namedSources.values.filterNot { source =>
      offsets.exists { case (name, _) => namedSources.get(name).contains(source) }
    }

    // New sources start with null offset (beginning)
    val newSourceOffsets = newSources.map(source => (source, null.asInstanceOf[OffsetV2]))

    new StreamProgress ++ (existingSourceOffsets ++ newSourceOffsets)
  }

  /**
   * Returns the named offsets map.
   */
  def getNamedOffsets: Map[String, OffsetV2] = offsets

  override def toString: String = {
    offsets.map { case (name, offset) => s"$name: ${offset.json}" }.mkString("{", ", ", "}")
  }
}

object OffsetSeq {

  /**
   * Returns a [[OffsetSeq]] with a variable sequence of offsets.
   * @deprecated Use fillNamed instead for explicit source naming
   */
  @deprecated("Use fillNamed with explicit source names", "3.6.0")
  def fill(offsets: OffsetV2*): OffsetSeq = fill(None, offsets: _*)

  /**
   * Returns a [[OffsetSeq]] with metadata and a variable sequence of offsets.
   * @deprecated Use fillNamed instead for explicit source naming
   */
  @deprecated("Use fillNamed with explicit source names", "3.6.0")
  def fill(metadata: Option[String], offsets: OffsetV2*): OffsetSeq = {
    val namedOffsets = offsets.zipWithIndex.filter(_._1 != null).map {
      case (offset, index) => index.toString -> offset
    }.toMap
    OffsetSeq(namedOffsets, metadata.map(OffsetSeqMetadata.apply))
  }

  /**
   * Creates an OffsetSeq with named sources and metadata.
   *
   * @param metadata Optional metadata string
   * @param namedOffsets Map from source name to offset
   * @return OffsetSeq with named sources
   */
  def fillNamed(metadata: Option[String], namedOffsets: Map[String, OffsetV2]): OffsetSeq = {
    OffsetSeq(namedOffsets, metadata.map(OffsetSeqMetadata.apply))
  }

  /**
   * Creates an OffsetSeq with named sources.
   * This is used for new V2 format checkpoints.
   *
   * @param namedOffsets Map from source name to offset
   * @return OffsetSeq with named sources
   */
  def fillNamed(namedOffsets: Map[String, OffsetV2]): OffsetSeq = {
    fillNamed(None, namedOffsets)
  }

  /**
   * Creates an OffsetSeq from source names, automatically generating ordinal names
   * for sources without explicit names.
   *
   * @param metadata Optional metadata string
   * @param sources Sequence of (sourceName, offset) pairs where sourceName may be None
   * @return OffsetSeq with appropriate naming
   */
  def fromSources(metadata: Option[String], sources: Seq[(Option[String], OffsetV2)]): OffsetSeq = {
    val namedSources = sources.zipWithIndex.collect {
      case ((Some(name), offset), _) => name -> offset
      case ((None, offset), index) => index.toString -> offset
    }.toMap

    fillNamed(metadata, namedSources)
  }

  /**
   * Validates a source name according to the naming rules.
   *
   * @param name The source name to validate
   * @throws IllegalArgumentException if the name is invalid
   */
  def validateSourceName(name: String): Unit = {
    require(name != null && name.nonEmpty, "Source name cannot be null or empty")
    require(name.length <= 64, s"Source name '$name' exceeds maximum length of 64 characters")
    require(name.matches("^[a-zA-Z0-9_-]+$"),
      s"Source name '$name' contains invalid characters. Only alphanumeric, underscore, " +
      "and hyphen characters are allowed")
  }
}


/**
 * Contains metadata associated with a [[OffsetSeq]]. This information is
 * persisted to the offset log in the checkpoint location via the [[OffsetSeq]] metadata field.
 *
 * @param batchWatermarkMs: The current eventTime watermark, used to
 * bound the lateness of data that will processed. Time unit: milliseconds
 * @param batchTimestampMs: The current batch processing timestamp.
 * Time unit: milliseconds
 * @param conf: Additional conf_s to be persisted across batches, e.g. number of shuffle partitions.
 * CAVEAT: This does not apply the logic we handle in [[OffsetSeqMetadata]] object, e.g.
 * deducing the default value of SQL config if the entry does not exist in the offset log,
 * resolving the re-bind of config key (the config key in offset log is not same with the
 * actual key in session), etc. If you need to get the value with applying the logic, use
 * [[OffsetSeqMetadata.readValue()]].
 */
case class OffsetSeqMetadata(
    batchWatermarkMs: Long = 0,
    batchTimestampMs: Long = 0,
    conf: Map[String, String] = Map.empty) {
  def json: String = Serialization.write(this)(OffsetSeqMetadata.format)
}

object OffsetSeqMetadata extends Logging {
  private implicit val format: Formats = Serialization.formats(NoTypeHints)
  /**
   * These configs are related to streaming query execution and should not be changed across
   * batches of a streaming query. The values of these configs are persisted into the offset
   * log in the checkpoint position.
   */
  private val relevantSQLConfs = Seq(
    STATE_STORE_PROVIDER_CLASS, STREAMING_MULTIPLE_WATERMARK_POLICY,
    FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION, STREAMING_AGGREGATION_STATE_FORMAT_VERSION,
    STREAMING_JOIN_STATE_FORMAT_VERSION, STATE_STORE_COMPRESSION_CODEC,
    STATE_STORE_ROCKSDB_FORMAT_VERSION, STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION,
    PRUNE_FILTERS_CAN_PRUNE_STREAMING_SUBPLAN, STREAMING_STATE_STORE_ENCODING_FORMAT
  )

  /**
   * This is an extension of `relevantSQLConfs`. The characteristic is the same, but we persist the
   * value of config A as config B in offset log. This exists for compatibility purpose e.g. if
   * user upgrades Spark and runs a streaming query, but has to downgrade due to some issues.
   *
   * A config should be only bound to either `relevantSQLConfs` or `rebindSQLConfs` (key or value).
   */
  private val rebindSQLConfsSessionToOffsetLog: Map[ConfigEntry[_], ConfigEntry[_]] = {
    Map(
      // TODO: The proper way to handle this is to make the number of partitions in the state
      //  metadata as the source of truth, but it requires major changes if we want to take care
      //  of compatibility.
      STATEFUL_SHUFFLE_PARTITIONS_INTERNAL -> SHUFFLE_PARTITIONS
    )
  }

  /**
   * Reversed index of `rebindSQLConfsSessionToOffsetLog`.
   */
  private val rebindSQLConfsOffsetLogToSession: Map[ConfigEntry[_], ConfigEntry[_]] =
    rebindSQLConfsSessionToOffsetLog.map { case (k, v) => (v, k) }.toMap

  /**
   * Default values of relevant configurations that are used for backward compatibility.
   * As new configurations are added to the metadata, existing checkpoints may not have those
   * confs. The values in this list ensures that the confs without recovered values are
   * set to a default value that ensure the same behavior of the streaming query as it was before
   * the restart.
   *
   * Note, that this is optional; set values here if you *have* to override existing session conf
   * with a specific default value for ensuring same behavior of the query as before.
   */
  private val relevantSQLConfDefaultValues = Map[String, String](
    STREAMING_MULTIPLE_WATERMARK_POLICY.key -> MultipleWatermarkPolicy.DEFAULT_POLICY_NAME,
    FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key ->
      FlatMapGroupsWithStateExecHelper.legacyVersion.toString,
    STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key ->
      StreamingAggregationStateManager.legacyVersion.toString,
    STREAMING_JOIN_STATE_FORMAT_VERSION.key ->
      SymmetricHashJoinStateManager.legacyVersion.toString,
    STATE_STORE_COMPRESSION_CODEC.key -> CompressionCodec.LZ4,
    STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION.key -> "false",
    PRUNE_FILTERS_CAN_PRUNE_STREAMING_SUBPLAN.key -> "true",
    STREAMING_STATE_STORE_ENCODING_FORMAT.key -> "unsaferow"
  )

  def readValue[T](metadataLog: OffsetSeqMetadata, confKey: ConfigEntry[T]): String = {
    readValueOpt(metadataLog, confKey).getOrElse(confKey.defaultValueString)
  }

  def readValueOpt[T](
      metadataLog: OffsetSeqMetadata,
      confKey: ConfigEntry[T]): Option[String] = {
    val actualKey = if (rebindSQLConfsSessionToOffsetLog.contains(confKey)) {
      rebindSQLConfsSessionToOffsetLog(confKey)
    } else confKey

    metadataLog.conf.get(actualKey.key).orElse(relevantSQLConfDefaultValues.get(actualKey.key))
  }

  def apply(json: String): OffsetSeqMetadata = Serialization.read[OffsetSeqMetadata](json)

  def apply(
      batchWatermarkMs: Long,
      batchTimestampMs: Long,
      sessionConf: RuntimeConfig): OffsetSeqMetadata = {
    val confs = relevantSQLConfs.map { conf => conf.key -> sessionConf.get(conf.key) }.toMap
    val confsFromRebind = rebindSQLConfsSessionToOffsetLog.map {
      case (confInSession, confInOffsetLog) =>
        confInOffsetLog.key -> sessionConf.get(confInSession.key)
    }.toMap
    OffsetSeqMetadata(batchWatermarkMs, batchTimestampMs, confs++ confsFromRebind)
  }

  /** Set the SparkSession configuration with the values in the metadata */
  def setSessionConf(metadata: OffsetSeqMetadata, sessionConf: SQLConf): Unit = {
    def setOneSessionConf(confKeyInOffsetLog: String, confKeyInSession: String): Unit = {
      metadata.conf.get(confKeyInOffsetLog) match {

        case Some(valueInMetadata) =>
          // Config value exists in the metadata, update the session config with this value
          val optionalValueInSession = sessionConf.getConfString(confKeyInSession, null)
          if (optionalValueInSession != null && optionalValueInSession != valueInMetadata) {
            logWarning(log"Updating the value of conf '${MDC(CONFIG, confKeyInSession)}' in " +
              log"current session from '${MDC(OLD_VALUE, optionalValueInSession)}' " +
              log"to '${MDC(NEW_VALUE, valueInMetadata)}'.")
          }
          sessionConf.setConfString(confKeyInSession, valueInMetadata)

        case None =>
          // For backward compatibility, if a config was not recorded in the offset log,
          // then either inject a default value (if specified in `relevantSQLConfDefaultValues`) or
          // let the existing conf value in SparkSession prevail.
          relevantSQLConfDefaultValues.get(confKeyInOffsetLog) match {

            case Some(defaultValue) =>
              sessionConf.setConfString(confKeyInSession, defaultValue)
              logWarning(log"Conf '${MDC(CONFIG, confKeyInSession)}' was not found in the offset " +
                log"log, using default value '${MDC(DEFAULT_VALUE, defaultValue)}'")

            case None =>
              val value = sessionConf.getConfString(confKeyInSession, null)
              val valueStr = if (value != null) {
                s" Using existing session conf value '$value'."
              } else {
                " No value set in session conf."
              }
              logWarning(log"Conf '${MDC(CONFIG, confKeyInSession)}' was not found in the " +
                log"offset log. ${MDC(TIP, valueStr)}")
          }
      }
    }

    OffsetSeqMetadata.relevantSQLConfs.map(_.key).foreach { confKey =>
      setOneSessionConf(confKey, confKey)
    }

    rebindSQLConfsOffsetLogToSession.foreach {
      case (confInOffsetLog, confInSession) =>
        setOneSessionConf(confInOffsetLog.key, confInSession.key)
    }
  }
}
