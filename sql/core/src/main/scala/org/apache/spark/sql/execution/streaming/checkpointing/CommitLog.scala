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

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets._

import scala.io.{Source => IOSource}

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Used to write log files that represent batch commit points in structured streaming.
 * A commit log file will be written immediately after the successful completion of a
 * batch, and before processing the next batch. Here is an execution summary:
 * - trigger batch 1
 * - obtain batch 1 offsets and write to offset log
 * - process batch 1
 * - write batch 1 to completion log
 * - trigger batch 2
 * - obtain batch 2 offsets and write to offset log
 * - process batch 2
 * - write batch 2 to completion log
 * ....
 *
 * The current format of the batch completion log is:
 * line 1: version
 * line 2: metadata (optional json string)
 */
class CommitLog(
    sparkSession: SparkSession,
    path: String,
    readOnly: Boolean = false)
  extends HDFSMetadataLog[CommitMetadataBase](sparkSession, path, readOnly) {

  import CommitLog._

  // The configured commit log format version. Used as the default version when callers
  // construct metadata through [[createMetadata]].
  private[sql] val defaultVersion: Int = sparkSession.conf.get(
    SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key).toInt

  override protected[sql] def deserialize(in: InputStream): CommitMetadataBase = {
    CommitLog.readCommitMetadata(in)
  }

  override protected[sql] def serialize(metadata: CommitMetadataBase, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(s"v${metadata.version}".getBytes(UTF_8))
    out.write('\n')

    // write metadata
    out.write(metadata.json.getBytes(UTF_8))
  }

  /**
   * Factory for creating a [[CommitMetadataBase]] for the requested wire format version.
   * Defaults to the version configured via [[SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION]].
   *
   * For [[VERSION_3]], [[sinkMetadataMap]] must be non-empty.
   */
  def createMetadata(
      nextBatchWatermarkMs: Long = 0,
      stateUniqueIds: Option[Map[Long, Array[Array[String]]]] = None,
      sinkMetadataMap: Map[String, SinkMetadataInfo] = Map.empty,
      commitLogFormatVersion: Int = defaultVersion): CommitMetadataBase = {
    commitLogFormatVersion match {
      case VERSION_3 =>
        require(sinkMetadataMap.nonEmpty,
          "VERSION_3 commit log requires a non-empty sinkMetadataMap")
        CommitMetadataV3(nextBatchWatermarkMs, stateUniqueIds, sinkMetadataMap)
      case VERSION_2 =>
        CommitMetadataV2(nextBatchWatermarkMs, stateUniqueIds)
      case VERSION_1 =>
        // VERSION_1 cannot persist stateUniqueIds; withStateUniqueIds enforces this invariant
        // (it throws if stateUniqueIds is non-empty).
        CommitMetadata(nextBatchWatermarkMs).withStateUniqueIds(stateUniqueIds)
      case v =>
        throw QueryExecutionErrors.logVersionGreaterThanSupported(v, CommitLog.MAX_VERSION)
    }
  }
}

object CommitLog {
  private val EMPTY_JSON = "{}"
  val VERSION_1 = 1
  val VERSION_2 = 2
  val VERSION_3 = 3
  val MAX_VERSION: Int = VERSION_3

  /**
   * Reads a single commit log entry and dispatches to the matching
   * [[CommitMetadataBase]] subclass based on the wire format version recorded in the file.
   */
  private[spark] def readCommitMetadata(in: InputStream): CommitMetadataBase = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file in the offset commit log")
    }
    val version = MetadataVersionUtil.validateVersion(lines.next().trim, MAX_VERSION)
    val metadataJson = if (lines.hasNext) lines.next() else EMPTY_JSON
    version match {
      case VERSION_3 => CommitMetadataV3(metadataJson)
      case VERSION_2 => CommitMetadataV2(metadataJson)
      case VERSION_1 => CommitMetadata(metadataJson)
      case v => throw QueryExecutionErrors.logVersionGreaterThanSupported(v, MAX_VERSION)
    }
  }
}

/**
 * Base trait for commit log metadata. Concrete subclasses correspond to wire format versions
 * and override [[version]] accordingly.
 */
trait CommitMetadataBase extends Serializable {
  def version: Int
  def nextBatchWatermarkMs: Long
  def stateUniqueIds: Option[Map[Long, Array[Array[String]]]]

  /**
   * Returns a copy of this metadata with the given state store unique ids, preserving the
   * concrete subclass and all of its other fields. Deriving a new commit from an existing one
   * should go through this method (rather than reconstructing via [[CommitLog.createMetadata]])
   * so that version-specific fields are not silently dropped when new metadata versions are
   * introduced.
   */
  def withStateUniqueIds(
      stateUniqueIds: Option[Map[Long, Array[Array[String]]]]): CommitMetadataBase

  def json: String = Serialization.write(this)(CommitMetadata.format)
}

/**
 * Commit log metadata for [[CommitLog.VERSION_1]]. Records the watermark for the next batch only.
 *
 * @param nextBatchWatermarkMs The watermark of the next batch.
 */
case class CommitMetadata(
    nextBatchWatermarkMs: Long = 0) extends CommitMetadataBase {
  override def version: Int = CommitLog.VERSION_1
  override def stateUniqueIds: Option[Map[Long, Array[Array[String]]]] = None

  override def withStateUniqueIds(
      stateUniqueIds: Option[Map[Long, Array[Array[String]]]]): CommitMetadata = {
    require(stateUniqueIds.forall(_.isEmpty),
      s"stateUniqueIds cannot be set for commit log format version ${CommitLog.VERSION_1}; " +
        s"use version ${CommitLog.VERSION_2} to persist state store checkpoint ids.")
    this
  }
}

object CommitMetadata {
  implicit val format: Formats = Serialization.formats(NoTypeHints)

  def apply(json: String): CommitMetadata = Serialization.read[CommitMetadata](json)
}

/**
 * In Checkpoint V2, for a stateful query, the checkpoint structure looks like below:
 * 0 (operator ID)
 *     +----+
 *          | 0 (partitionID)
 *     +----+
 *          |     ......
 *          | 1 (partitionID)
 *          +----+
 *          |    |- default (storeName)
 *          |     +-----+
 *          |           |  20_unique_id_1.zip
 *          |           |  21_unique_id_2.delta
 *          |           |  22_unique_id_3.delta
 *          |           +  23_unique_id_4.delta
 *          | 2 (partitionID)
 *          +--- ......
 * In the commit log, in addition to nextBatchWatermarkMs, we also store the unique ids of the
 * state store files.
 *
 * @param nextBatchWatermarkMs The watermark of the next batch.
 * @param stateUniqueIds Map[Long, Array[Array[String]]] of map
 *                       OperatorId -> (partitionID -> array of uniqueID)
 */
case class CommitMetadataV2(
    nextBatchWatermarkMs: Long = 0,
    stateUniqueIds: Option[Map[Long, Array[Array[String]]]] = None) extends CommitMetadataBase {
  override def version: Int = CommitLog.VERSION_2

  override def withStateUniqueIds(
      stateUniqueIds: Option[Map[Long, Array[Array[String]]]]): CommitMetadataV2 =
    copy(stateUniqueIds = stateUniqueIds)
}

object CommitMetadataV2 {
  import CommitMetadata.format

  def apply(json: String): CommitMetadataV2 = Serialization.read[CommitMetadataV2](json)
}

/**
 * Commit log metadata for [[CommitLog.VERSION_3]]. Extends V2 with a map of per-sink metadata
 * keyed by sink name. This enables streaming sink evolution: each batch records the active sink
 * along with any historical sinks that were used in earlier batches but are no longer active.
 *
 * @param nextBatchWatermarkMs The watermark of the next batch.
 * @param stateUniqueIds Per-operator state store unique ids (see [[CommitMetadataV2]]).
 * @param sinkMetadataMap Map keyed by sink name. There is at most one active entry per
 *                       commit; deactivated sinks are retained to detect reuse of a sink name.
 */
case class CommitMetadataV3(
    nextBatchWatermarkMs: Long = 0,
    stateUniqueIds: Option[Map[Long, Array[Array[String]]]] = None,
    sinkMetadataMap: Map[String, SinkMetadataInfo] = Map.empty) extends CommitMetadataBase {
  override def version: Int = CommitLog.VERSION_3

  override def withStateUniqueIds(
      stateUniqueIds: Option[Map[Long, Array[Array[String]]]]): CommitMetadataV3 =
    copy(stateUniqueIds = stateUniqueIds)

  /** Returns the currently active sink's metadata, if any. */
  def activeSinkMetadataInfoOpt: Option[SinkMetadataInfo] = sinkMetadataMap.values.find(_.isActive)
}

object CommitMetadataV3 {
  implicit val format: Formats = Serialization.formats(NoTypeHints)

  def apply(json: String): CommitMetadataV3 = Serialization.read[CommitMetadataV3](json)
}

/**
 * Per-sink metadata recorded in a [[CommitMetadataV3]] entry.
 *
 * @param sinkName Sink name as supplied via `DataStreamWriter.name()`, or
 *                 `MicroBatchExecution.DEFAULT_SINK_NAME` when sink evolution is disabled.
 * @param commitOffset The latest offset committed to the sink as a JSON string
 *                     (i.e. [[OffsetV2.json()]]), or [[OffsetSeqLog.SERIALIZED_VOID_OFFSET]] if
 *                     no offset is available.
 * @param providerName Identifies the sink implementation (e.g. fully-qualified class name).
 * @param isActive Whether this sink is the active sink for the current batch. Historical sinks
 *                 are retained with `isActive = false`.
 */
case class SinkMetadataInfo(
    sinkName: String,
    commitOffset: String,
    providerName: String,
    isActive: Boolean = true) {
  def json: String = Serialization.write(this)(SinkMetadataInfo.format)
}

object SinkMetadataInfo {
  private implicit val format: Formats = Serialization.formats(NoTypeHints)

  def apply(
      sinkName: String,
      commitOffset: Option[OffsetV2],
      providerName: String,
      isActive: Boolean): SinkMetadataInfo = {
    val offsetString = commitOffset match {
      case Some(off) => off.json
      case None => OffsetSeqLog.SERIALIZED_VOID_OFFSET
    }
    new SinkMetadataInfo(sinkName, offsetString, providerName, isActive)
  }
}
