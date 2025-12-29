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

trait OffsetSeqBase {
  def version: Int

  def offsets: Seq[Option[OffsetV2]]

  def metadataOpt: Option[OffsetSeqMetadataBase]

  override def toString: String = this match {
    case offsetMap: OffsetMap =>
      offsetMap.offsetsMap.map { case (sourceId, offsetOpt) =>
        s"$sourceId: ${offsetOpt.map(_.json).getOrElse("-")}"
      }.mkString("{", ", ", "}")
    case _ =>
      offsets.map(_.map(_.json).getOrElse("-")).mkString("[", ", ", "]")
  }

  /**
   * Unpacks an offset into [[StreamProgress]] by associating each offset with the
   * ordered list of sources.
   *
   * This method is typically used to associate a serialized offset with actual
   * sources (which cannot be serialized).
   */
  def toStreamProgress(sources: Seq[SparkDataStream]): StreamProgress = {
    assert(!this.isInstanceOf[OffsetMap], "toStreamProgress must be called with map")
    assert(sources.size == offsets.size, s"There are [${offsets.size}] sources in the " +
      s"checkpoint offsets and now there are [${sources.size}] sources requested by " +
      s"the query. Cannot continue.")
    new StreamProgress ++ sources.zip(offsets).collect { case (s, Some(o)) => (s, o) }
  }

  /**
   * Converts OffsetMap to StreamProgress using source ID mapping.
   * This method is specific to OffsetMap and requires a mapping from sourceId to SparkDataStream.
   */
  def toStreamProgress(
      sources: Seq[SparkDataStream],
      sourceIdToSourceMap: Map[String, SparkDataStream]): StreamProgress = {
    this match {
      case offsetMap: OffsetMap =>
        val streamProgressEntries = for {
          (sourceId, offsetOpt) <- offsetMap.offsetsMap
          offset <- offsetOpt
          source <- sourceIdToSourceMap.get(sourceId)
        } yield source -> offset
        new StreamProgress ++ streamProgressEntries
      case _ =>
        // Fallback to original method for backward compatibility
        toStreamProgress(sources)
    }
  }
}

/**
 * An ordered collection of offsets, used to track the progress of processing data from one or more
 * [[Source]]s that are present in a streaming query. This is similar to simplified, single-instance
 * vector clock that must progress linearly forward.
 */
case class OffsetSeq(
    offsets: Seq[Option[OffsetV2]],
    metadataOpt: Option[OffsetSeqMetadata] = None) extends OffsetSeqBase {
  override def version: Int = OffsetSeqLog.VERSION_1
}

object OffsetSeq {

  /**
   * Returns a [[OffsetSeq]] with a variable sequence of offsets.
   * `nulls` in the sequence are converted to `None`s.
   */
  def fill(offsets: OffsetV2*): OffsetSeq = OffsetSeq.fill(None, offsets: _*)

  /**
   * Returns a [[OffsetSeq]] with metadata and a variable sequence of offsets.
   * `nulls` in the sequence are converted to `None`s.
   */
  def fill(metadataOpt: Option[String], offsets: OffsetV2*): OffsetSeq = {
    OffsetSeq(offsets.map(Option(_)), metadataOpt.map(OffsetSeqMetadata.apply))
  }
}

/**
 * A map-based collection of offsets, used to track the progress of processing data from one or more
 * streaming sources. Each source is identified by a string key (initially sourceId.toString()).
 * This replaces the sequence-based approach with a more flexible map-based approach to support
 * named source identities.
 *
 * Unlike [[OffsetSeq]], metadata is required (not optional) for [[OffsetMap]] as it contains
 * essential information like source metadata.
 */
case class OffsetMap(
    offsetsMap: Map[String, Option[OffsetV2]],
    metadata: OffsetSeqMetadataV2) extends OffsetSeqBase {
  override def version: Int = OffsetSeqLog.VERSION_2

  // OffsetMap does not support sequence-based access
  override def offsets: Seq[Option[OffsetV2]] = {
    throw new UnsupportedOperationException(
      "OffsetMap does not support sequence-based offsets access. Use offsetsMap directly.")
  }

  override def metadataOpt: Option[OffsetSeqMetadataBase] = Some(metadata)
}

/**
 * Base trait for offset sequence metadata.
 *
 * Valid combinations of offset log versions and metadata types:
 * - VERSION_1 (sequence-based offset log): Uses [[OffsetSeq]] with [[OffsetSeqMetadata]]
 * - VERSION_2 (map-based offset log): Uses [[OffsetMap]] with [[OffsetSeqMetadataV2]]
 *
 * The metadata version must match the offset log version for proper serialization and
 * deserialization.
 */
trait OffsetSeqMetadataBase extends Serializable {
  def batchWatermarkMs: Long
  def batchTimestampMs: Long
  def conf: Map[String, String]
  def json: String
  def version: Int
  def sourceMetadataInfoOpt: Option[Map[String, SourceMetadataInfo]]
  def controlBatchInfoOpt: Option[OffsetSeqControlBatchInfo]
}

/**
 * Contains metadata associated with a [[OffsetSeq]]. This information is
 * persisted to the offset log in the checkpoint location via the [[OffsetSeq]] metadata field.
 *
 * This is VERSION_1 metadata, used with sequence-based [[OffsetSeq]].
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
    conf: Map[String, String] = Map.empty) extends OffsetSeqMetadataBase {
  override def json: String = Serialization.write(this)(OffsetSeqMetadata.format)

  override def version: Int = OffsetSeqLog.VERSION_1

  override def sourceMetadataInfoOpt: Option[Map[String, SourceMetadataInfo]] = None

  override def controlBatchInfoOpt: Option[OffsetSeqControlBatchInfo] = None
}

object OffsetSeqMetadata extends Logging {
  private[checkpointing] implicit val format: Formats = Serialization.formats(NoTypeHints)
  /**
   * These configs are related to streaming query execution and should not be changed across
   * batches of a streaming query. The values of these configs are persisted into the offset
   * log in the checkpoint position.
   */
  private[checkpointing] val relevantSQLConfs = Seq(
    STATE_STORE_PROVIDER_CLASS, STREAMING_MULTIPLE_WATERMARK_POLICY,
    FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION, STREAMING_AGGREGATION_STATE_FORMAT_VERSION,
    STREAMING_JOIN_STATE_FORMAT_VERSION, STATE_STORE_COMPRESSION_CODEC,
    STATE_STORE_ROCKSDB_FORMAT_VERSION, STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION,
    PRUNE_FILTERS_CAN_PRUNE_STREAMING_SUBPLAN, STREAMING_STATE_STORE_ENCODING_FORMAT,
    STATE_STORE_ROW_CHECKSUM_ENABLED
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
    STREAMING_STATE_STORE_ENCODING_FORMAT.key -> "unsaferow",
    STATE_STORE_ROW_CHECKSUM_ENABLED.key -> "false"
  )

  def readValue[T](metadataLog: OffsetSeqMetadataBase, confKey: ConfigEntry[T]): String = {
    readValueOpt(metadataLog, confKey).getOrElse(confKey.defaultValueString)
  }

  def readValueOpt[T](
      metadataLog: OffsetSeqMetadataBase,
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
  def setSessionConf(metadata: OffsetSeqMetadataBase, sessionConf: SQLConf): Unit = {
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

/**
 * This class is used to store the metadata for a source in the offset log within the
 * streaming checkpoint.
 *
 * @param sourceId: The ID of the source.
 * @param providerName: The name of the provider.
 * @param apiVersion: The API version for the source - whether it is DSv1 or DSv2.
 */
case class SourceMetadataInfo(
    sourceId: String,
    providerName: String,
    apiVersion: String) {
  def json: String = Serialization.write(this)(OffsetSeqMetadata.format)
}

/**
 * This class is used to store metadata about the type of control batch in the offset log.
 *
 * @param operationType The type of control batch operation.
 * @param operationInfo Additional info about the control batch operation.
 */
case class OffsetSeqControlBatchInfo(
    operationType: String,
    operationInfo: Option[String] = None) {
  def json: String = Serialization.write(this)(OffsetSeqControlBatchInfo.format)
}

object OffsetSeqControlBatchInfo {
  private implicit val format: Formats = Serialization.formats(NoTypeHints)
}

/**
 * Contains metadata associated with a [[OffsetMap]]. This information is
 * persisted to the offset log in the checkpoint location via the [[OffsetMap]] metadata field.
 *
 * This is VERSION_2 metadata, used with map-based [[OffsetMap]].
 *
 * @param batchWatermarkMs: The current eventTime watermark, used to
 * bound the lateness of data that will processed. Time unit: milliseconds
 * @param batchTimestampMs: The current batch processing timestamp.
 * Time unit: milliseconds
 * @param conf: Additional conf_s to be persisted across batches,
 * e.g. number of shuffle partitions.
 * @param sourceMetadataInfo: The source related metadata for the streaming query,
 * mapped by sourceId.
 * @param controlBatchInfo: The control batch information if the current batch is a
 * control batch. Value of None means current batch is a data batch.
 */
case class OffsetSeqMetadataV2(
    batchWatermarkMs: Long = 0,
    batchTimestampMs: Long = 0,
    conf: Map[String, String] = Map.empty,
    sourceMetadataInfo: Map[String, SourceMetadataInfo] = Map.empty,
    controlBatchInfo: Option[OffsetSeqControlBatchInfo] = None) extends OffsetSeqMetadataBase {
  override def version: Int = OffsetSeqLog.VERSION_2

  override def sourceMetadataInfoOpt: Option[Map[String, SourceMetadataInfo]] =
    Some(sourceMetadataInfo)

  override def controlBatchInfoOpt: Option[OffsetSeqControlBatchInfo] = controlBatchInfo

  override def json: String = Serialization.write(this)(OffsetSeqMetadata.format)
}

object OffsetSeqMetadataV2 {
  private implicit val format: Formats = Serialization.formats(NoTypeHints)

  def apply(json: String): OffsetSeqMetadataV2 = Serialization.read[OffsetSeqMetadataV2](json)

  def apply(
      batchWatermarkMs: Long,
      batchTimestampMs: Long,
      sessionConf: RuntimeConfig): OffsetSeqMetadataV2 = {
    val confs = OffsetSeqMetadata.relevantSQLConfs.map {
      conf => conf.key -> sessionConf.get(conf.key)
    }.toMap
    OffsetSeqMetadataV2(batchWatermarkMs, batchTimestampMs, confs)
  }
}
