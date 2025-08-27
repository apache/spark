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

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, DEFAULT_VALUE, NEW_VALUE, OLD_VALUE, TIP}
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
 * An ordered collection of offsets, used to track the progress of processing data from one or more
 * [[Source]]s that are present in a streaming query. This is similar to simplified, single-instance
 * vector clock that must progress linearly forward.
 *
 * This class supports both positional offsets (legacy V1 format) and named sources (V2 format).
 * Internally, all sources are tracked by name, with ordinal names ("0", "1", "2", etc.) used
 * when explicit names are not provided.
 */
case class OffsetSeq(
    offsets: Seq[Option[OffsetV2]],
    metadata: Option[OffsetSeqMetadata] = None,
    namedOffsets: Option[Map[String, OffsetV2]] = None) {

  /**
   * Unpacks an offset into [[StreamProgress]] by associating each offset with the ordered list of
   * sources.
   *
   * This method is typically used to associate a serialized offset with actual sources (which
   * cannot be serialized).
   */
  def toStreamProgress(sources: Seq[SparkDataStream]): StreamProgress = {
    assert(sources.size == offsets.size, s"There are [${offsets.size}] sources in the " +
      s"checkpoint offsets and now there are [${sources.size}] sources requested by the query. " +
      s"Cannot continue.")
    new StreamProgress ++ sources.zip(offsets).collect { case (s, Some(o)) => (s, o) }
  }

  /**
   * Unpacks named offsets into [[StreamProgress]] by associating each named offset with the
   * corresponding source.
   *
   * @param namedSources Map from source name to SparkDataStream
   * @return The StreamProgress containing the offset mapping
   */
  def toStreamProgress(namedSources: Map[String, SparkDataStream]): StreamProgress = {
    namedOffsets match {
      case Some(sourceOffsets) =>
        val sourceOffsetPairs = sourceOffsets.flatMap { case (name, offset) =>
          namedSources.get(name) match {
            case Some(source) => Some((source, offset))
            case None =>
              // This shouldn't happen in normal operation, but handle gracefully
              None
          }
        }
        new StreamProgress ++ sourceOffsetPairs
      case None =>
        // Fall back to positional mapping for backward compatibility
        toStreamProgress(namedSources.values.toSeq)
    }
  }

  /**
   * Creates a new OffsetSeq with named sources from this positional OffsetSeq.
   * This is used to upgrade legacy checkpoints to the new format.
   */
  def withNamedSources(sourceNames: Seq[String]): OffsetSeq = {
    require(sourceNames.size == offsets.size,
      s"Number of source names (${sourceNames.size}) must match " +
      s"number of offsets (${offsets.size})")

    val named = sourceNames.zip(offsets).collect {
      case (name, Some(offset)) => name -> offset
    }.toMap

    copy(namedOffsets = Some(named))
  }

  /**
   * Returns the named offsets map, creating one from ordinal positions if needed.
   */
  def getNamedOffsets: Map[String, OffsetV2] = {
    namedOffsets.getOrElse {
      offsets.zipWithIndex.collect {
        case (Some(offset), index) => index.toString -> offset
      }.toMap
    }
  }

  override def toString: String = {
    namedOffsets match {
      case Some(named) =>
        named.map { case (name, offset) => s"$name: ${offset.json}" }.mkString("{", ", ", "}")
      case None =>
        offsets.map(_.map(_.json).getOrElse("-")).mkString("[", ", ", "]")
    }
  }
}

object OffsetSeq {

  /**
   * Returns a [[OffsetSeq]] with a variable sequence of offsets.
   * `nulls` in the sequence are converted to `None`s.
   * This method maintains backward compatibility with V1 checkpoints.
   */
  def fill(offsets: OffsetV2*): OffsetSeq = OffsetSeq.fill(None, offsets: _*)

  /**
   * Returns a [[OffsetSeq]] with metadata and a variable sequence of offsets.
   * `nulls` in the sequence are converted to `None`s.
   * This method maintains backward compatibility with V1 checkpoints.
   */
  def fill(metadata: Option[String], offsets: OffsetV2*): OffsetSeq = {
    OffsetSeq(offsets.map(Option(_)), metadata.map(OffsetSeqMetadata.apply))
  }

  /**
   * Creates an OffsetSeq with named sources and metadata.
   * This is used for new V2 format checkpoints.
   *
   * @param metadata Optional metadata string
   * @param namedOffsets Map from source name to offset
   * @return OffsetSeq with named sources
   */
  def fillNamed(metadata: Option[String], namedOffsets: Map[String, OffsetV2]): OffsetSeq = {
    // Create empty positional offsets for backward compatibility methods
    val positionalOffsets = Seq.empty[Option[OffsetV2]]
    OffsetSeq(positionalOffsets, metadata.map(OffsetSeqMetadata.apply), Some(namedOffsets))
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
    SHUFFLE_PARTITIONS, STATE_STORE_PROVIDER_CLASS, STREAMING_MULTIPLE_WATERMARK_POLICY,
    FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION, STREAMING_AGGREGATION_STATE_FORMAT_VERSION,
    STREAMING_JOIN_STATE_FORMAT_VERSION, STATE_STORE_COMPRESSION_CODEC,
    STATE_STORE_ROCKSDB_FORMAT_VERSION, STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION,
    PRUNE_FILTERS_CAN_PRUNE_STREAMING_SUBPLAN, STREAMING_STATE_STORE_ENCODING_FORMAT
  )

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

  def apply(json: String): OffsetSeqMetadata = Serialization.read[OffsetSeqMetadata](json)

  def apply(
      batchWatermarkMs: Long,
      batchTimestampMs: Long,
      sessionConf: RuntimeConfig): OffsetSeqMetadata = {
    val confs = relevantSQLConfs.map { conf => conf.key -> sessionConf.get(conf.key) }.toMap
    OffsetSeqMetadata(batchWatermarkMs, batchTimestampMs, confs)
  }

  /** Set the SparkSession configuration with the values in the metadata */
  def setSessionConf(metadata: OffsetSeqMetadata, sessionConf: SQLConf): Unit = {
    val configs = sessionConf.getAllConfs
    OffsetSeqMetadata.relevantSQLConfs.map(_.key).foreach { confKey =>

      metadata.conf.get(confKey) match {

        case Some(valueInMetadata) =>
          // Config value exists in the metadata, update the session config with this value
          val optionalValueInSession = sessionConf.getConfString(confKey, null)
          if (optionalValueInSession != null && optionalValueInSession != valueInMetadata) {
            logWarning(log"Updating the value of conf '${MDC(CONFIG, confKey)}' in current " +
              log"session from '${MDC(OLD_VALUE, optionalValueInSession)}' " +
              log"to '${MDC(NEW_VALUE, valueInMetadata)}'.")
          }
          sessionConf.setConfString(confKey, valueInMetadata)

        case None =>
          // For backward compatibility, if a config was not recorded in the offset log,
          // then either inject a default value (if specified in `relevantSQLConfDefaultValues`) or
          // let the existing conf value in SparkSession prevail.
          relevantSQLConfDefaultValues.get(confKey) match {

            case Some(defaultValue) =>
              sessionConf.setConfString(confKey, defaultValue)
              logWarning(log"Conf '${MDC(CONFIG, confKey)}' was not found in the offset log, " +
                log"using default value '${MDC(DEFAULT_VALUE, defaultValue)}'")

            case None =>
              val value = sessionConf.getConfString(confKey, null)
              val valueStr = if (value != null) {
                s" Using existing session conf value '$value'."
              } else {
                " No value set in session conf."
              }
              logWarning(log"Conf '${MDC(CONFIG, confKey)}' was not found in the offset log. " +
                log"${MDC(TIP, valueStr)}")

          }
      }
    }
  }
}
