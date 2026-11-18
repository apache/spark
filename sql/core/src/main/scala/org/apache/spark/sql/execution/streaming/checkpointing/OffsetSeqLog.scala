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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.execution.streaming.runtime.SerializedOffset

/**
 * This class is used to log offsets to persistent files in HDFS.
 * Each file corresponds to a specific batch of offsets. The file
 * format contains a version string in the first line, followed
 * by a the JSON string representation of the offsets separated
 * by a newline character. If a source offset is missing, then
 * that line will contain a string value defined in the
 * SERIALIZED_VOID_OFFSET variable in [[OffsetSeqLog]] companion object.
 * For instance, when dealing with [[LongOffset]] types:
 *   v1        // version 1
 *   metadata
 *   {0}       // LongOffset 0
 *   {3}       // LongOffset 3
 *   -         // No offset for this source i.e., an invalid JSON string
 *   {2}       // LongOffset 2
 *   ...
 *
 * Version 2 format (OffsetMap):
 *   v2        // version 2
 *   metadata
 *   0:{0}     // sourceId:offset
 *   1:{3}     // sourceId:offset
 *   ...
 *
 * Version 3 wraps either payload in a reader compatibility boundary for corrected KLL state:
 *   v3        // readers without corrected KLL checkpoint semantics must reject this log
 *   v1 or v2  // the enclosed offset payload format, followed by its metadata and offsets
 */
class OffsetSeqLog(
    sparkSession: SparkSession,
    path: String,
    readOnly: Boolean = false)
  extends HDFSMetadataLog[OffsetSeqBase](sparkSession, path, readOnly) {

  override protected def deserialize(in: InputStream): OffsetSeqBase = {
    // called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }

    val versionStr = lines.next()
    val logVersion = validateVersion(versionStr, OffsetSeqLog.MAX_VERSION)
    val versionInt = if (logVersion == OffsetSeqLog.VERSION_3) {
      if (!lines.hasNext) throw new IllegalStateException("Incomplete v3 offset log")
      validateVersion(lines.next(), OffsetSeqLog.VERSION_2)
    } else logVersion

    // read metadata
    val metadata = lines.next().trim match {
      case "" => None
      case md => Some(md)
    }
    import org.apache.spark.util.ArrayImplicits._
    val offsets = if (versionInt == OffsetSeqLog.VERSION_2) {
      // deserialize the remaining lines into the offset map
      val remainingLines = lines.toArray
      // New OffsetMap format: sourceId:offset
      val offsetsMap = remainingLines.map { line =>
        val colonIndex = line.indexOf(':')
        if (colonIndex == -1) {
          throw new IllegalStateException(s"Invalid OffsetMap format: $line")
        }
        val sourceId = line.substring(0, colonIndex)
        val offsetStr = line.substring(colonIndex + 1)
        val offset = if (offsetStr == OffsetSeqLog.SERIALIZED_VOID_OFFSET) {
          None
        } else {
          Some(OffsetSeqLog.parseOffset(offsetStr))
        }
        sourceId -> offset
      }.toMap
      // V2 requires metadata
      val metadataV2 = metadata.map(OffsetSeqMetadataV2.apply).getOrElse(
        throw new IllegalStateException("VERSION_2 offset log requires metadata"))
      OffsetMap(offsetsMap, metadataV2)
    } else {
      OffsetSeq.fill(metadata,
        lines.map(OffsetSeqLog.parseOffset).toArray.toImmutableArraySeq: _*)
    }
    if (logVersion == OffsetSeqLog.VERSION_3 &&
        !offsets.metadataOpt.exists(OffsetSeqLog.requiresKllReader)) {
      throw new IllegalStateException("Version 3 offset log requires KLL reader metadata")
    }
    offsets
  }

  override protected def serialize(offsetSeq: OffsetSeqBase, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    if (offsetSeq.metadataOpt.exists(OffsetSeqLog.requiresKllReader)) {
      out.write(s"v${OffsetSeqLog.VERSION_3}\n".getBytes(UTF_8))
    }
    out.write(("v" + offsetSeq.metadataOpt.map(_.version).getOrElse(OffsetSeqLog.VERSION_1))
      .getBytes(UTF_8))

    // write metadata
    out.write('\n')
    out.write(offsetSeq.metadataOpt.map(_.json).getOrElse("").getBytes(UTF_8))

    offsetSeq match {
      case offsetMap: OffsetMap =>
        // For OffsetMap, write sourceId:offset pairs, one per line
        offsetMap.offsetsMap.foreach { case (sourceId, offsetOpt) =>
          out.write('\n')
          out.write(sourceId.getBytes(UTF_8))
          out.write(':')
          offsetOpt match {
            case Some(offset) => out.write(offset.json.getBytes(UTF_8))
            case None => out.write(OffsetSeqLog.SERIALIZED_VOID_OFFSET.getBytes(UTF_8))
          }
        }
      case _ =>
        // Original sequence-based serialization
        offsetSeq.offsets.map(_.map(_.json)).foreach { offset =>
          out.write('\n')
          offset match {
            case Some(json: String) => out.write(json.getBytes(UTF_8))
            case None => out.write(OffsetSeqLog.SERIALIZED_VOID_OFFSET.getBytes(UTF_8))
          }
        }
    }
  }

  def offsetSeqMetadataForBatchId(batchId: Long): Option[OffsetSeqMetadataBase] = {
    if (batchId < 0) {
      None
    } else {
      get(batchId).flatMap(_.metadataOpt)
    }
  }
}

object OffsetSeqLog {
  private[streaming] val VERSION_1 = 1
  private[streaming] val VERSION_2 = 2
  private[streaming] val VERSION_3 = 3
  private[streaming] val VERSION = VERSION_1  // Default version for backward compatibility
  private[streaming] val MAX_VERSION = VERSION_3
  private[spark] val SERIALIZED_VOID_OFFSET = "-"

  // A persisted reader requirement, not a session configuration. Preserve it on every rewrite.
  private val KLL_CORRECTED_STATE = "spark.sql.streaming.kllCorrectedState"

  private[sql] def requiresKllReader(metadata: OffsetSeqMetadataBase): Boolean =
    metadata.conf.get(KLL_CORRECTED_STATE).contains("true")

  private[sql] def withKllReaderBoundary(
      metadata: OffsetSeqMetadataBase): OffsetSeqMetadataBase = metadata match {
    case sequence: OffsetSeqMetadata =>
      sequence.copy(conf = sequence.conf.updated(KLL_CORRECTED_STATE, "true"))
    case map: OffsetSeqMetadataV2 =>
      map.copy(conf = map.conf.updated(KLL_CORRECTED_STATE, "true"))
  }

  private[checkpointing] def parseOffset(value: String): OffsetV2 = value match {
    case SERIALIZED_VOID_OFFSET => null
    case json => SerializedOffset(json)
  }
}
