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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Case class for managing the internal versioning for the streaming checkpoint. Versions are
 * tracked per component (currently just the offset log; other components are managed elsewhere via
 * dedicated configs).
 */
case class StreamingCheckpointVersion(offsetLogVersion: Int) {
  override def toString: String = {
    s"StreamingCheckpointVersion(offsetLogVersion: $offsetLogVersion)"
  }
}

sealed trait CheckpointLogType
case object OffsetLogType extends CheckpointLogType

/**
 * The `CheckpointVersionManager` is responsible for managing the versioning of the streaming
 * checkpoint. It determines which version of each system-managed log format to use when starting
 * a streaming query, and validates that the requested feature set is compatible with the existing
 * checkpoint when restarting.
 *
 * Writer versions are typically used only while starting a new streaming query and are not
 * intended to be exposed directly to users; once set, they are not intended to change for the
 * lifetime of the query.
 */
object CheckpointVersionManager extends Logging {

  // Streaming checkpoint writer version 1: base version supporting DataFrame-based streaming
  // queries across the standard trigger types.
  private val CHECKPOINT_VERSION_V1 = StreamingCheckpointVersion(OffsetSeqLog.VERSION_1)

  // The current version of the streaming checkpoint. To bump this, define a new
  // `StreamingCheckpointVersion` instance with the new per-component version numbers and update
  // this constant.
  private val CURRENT_VERSION = CHECKPOINT_VERSION_V1

  def getCurrentVersion(): StreamingCheckpointVersion = CURRENT_VERSION

  /**
   * Returns the offset log format version to use for a new streaming query. We take the max of:
   *  - the current default version
   *  - the minimum required version implied by enabled features (e.g. streaming source evolution
   *    requires [[OffsetSeqLog.VERSION_2]] for OffsetMap-based named source tracking)
   *  - the configured version (via [[SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION]])
   *
   * @param sparkSessionForStream the cloned `SparkSession` for the streaming query
   */
  private def getOffsetLogVersion(sparkSessionForStream: SparkSession): Int = {
    val currentDefaultVersion = getCurrentVersion().offsetLogVersion
    val minRequiredVersion = getMinRequiredOffsetLogVersion(sparkSessionForStream)
    val configuredVersion = sparkSessionForStream.sessionState.conf.streamingOffsetLogFormatVersion
    val result = List[Int](currentDefaultVersion, minRequiredVersion, configuredVersion).max
    logInfo(s"Retrieved offset log writer version=$result")
    result
  }

  /**
   * Minimum offset log format version required by the features enabled on this session. Streaming
   * source evolution relies on the OffsetMap (sourceId -> offset) format, which is only available
   * in [[OffsetSeqLog.VERSION_2]].
   */
  private def getMinRequiredOffsetLogVersion(sparkSessionForStream: SparkSession): Int = {
    if (sparkSessionForStream.sessionState.conf.enableStreamingSourceEvolution) {
      OffsetSeqLog.VERSION_2
    } else {
      OffsetSeqLog.VERSION_1
    }
  }

  /**
   * Set the SparkSession configurations for the offset log format version.
   */
  private def setSparkSessionConfigsForOffsetLog(
      sparkSessionForStream: SparkSession,
      offsetLogFormatVersion: Int): Unit = {
    sparkSessionForStream.conf.set(
      SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key, offsetLogFormatVersion)
  }

  /**
   * Returns the format version for the given log type. Reads any feature-driven minimums from the
   * `sparkSessionForStream` config, which must be initialized before calling.
   */
  def getFormatVersionFromSession(
      sparkSessionForStream: SparkSession,
      logType: CheckpointLogType): Int = {
    logType match {
      case OffsetLogType => getOffsetLogVersion(sparkSessionForStream)
    }
  }

  /**
   * Determines the offset log format version for this query run. For existing queries, reads from
   * the last written offset log entry. For new queries, delegates to the session config (honoring
   * any feature-driven minimums).
   *
   * Also validates that the session config is compatible with the existing checkpoint. Currently,
   * enabling streaming source evolution on a checkpoint whose offset log is below VERSION_2 is
   * rejected, since the OffsetMap-based named source tracking required by source evolution is not
   * available in earlier versions.
   */
  def resolveOffsetLogVersion(
      sparkSessionForStream: SparkSession,
      latestStartedBatch: Option[(Long, OffsetSeqBase)]): Int = {
    latestStartedBatch match {
      case Some((_, offsetSeq)) =>
        val existingVersion = offsetSeq.version
        if (existingVersion < OffsetSeqLog.VERSION_2 &&
            sparkSessionForStream.sessionState.conf.enableStreamingSourceEvolution) {
          throw QueryCompilationErrors.cannotEnableSourceEvolutionOnExistingCheckpointError(
            existingVersion)
        }
        existingVersion
      case None =>
        getFormatVersionFromSession(sparkSessionForStream, OffsetLogType)
    }
  }

  def setFormatVersion(
      sparkSessionForStream: SparkSession,
      logType: CheckpointLogType,
      version: Int): Unit = {
    logType match {
      case OffsetLogType =>
        setSparkSessionConfigsForOffsetLog(sparkSessionForStream, version)
    }
  }
}
