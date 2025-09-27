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

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.{JsonMethods, Serialization}

import org.apache.spark.internal.Logging
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
 *
 * V1 format (positional sources):
 *   v1        // version 1
 *   metadata
 *   {0}       // LongOffset 0
 *   {3}       // LongOffset 3
 *   -         // No offset for this source i.e., an invalid JSON string
 *   {2}       // LongOffset 2
 *   ...
 *
 * V2 format (named sources):
 *   v2        // version 2
 *   metadata
 *   {"source-name-1": "{offset-json-1}", "source-name-2": "{offset-json-2}"}
 */
class OffsetSeqLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[OffsetSeq](sparkSession, path) with Logging {


  override protected def deserialize(in: InputStream): OffsetSeq = {
    // called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }

    val versionLine = lines.next()
    val version = MetadataVersionUtil.validateVersion(versionLine, OffsetSeqLog.VERSION_2)

    version match {
      case 1 => deserializeV1(lines)
      case 2 => deserializeV2(lines)
      case _ => throw new IllegalStateException(s"Unknown offset log version: $version")
    }
  }

  private def deserializeV1(lines: Iterator[String]): OffsetSeq = {
    def parseOffset(value: String): OffsetV2 = value match {
      case OffsetSeqLog.SERIALIZED_VOID_OFFSET => null
      case json => SerializedOffset(json)
    }

    // read metadata
    val metadata = lines.next().trim match {
      case "" => None
      case md => Some(md)
    }

    // Read positional offsets and create OffsetSeq with ordinal names for internal consistency
    import org.apache.spark.util.ArrayImplicits._
    val offsetArray = lines.map(parseOffset).toArray.toImmutableArraySeq
    val positionalSeq = OffsetSeq.fill(metadata, offsetArray: _*)

    // Convert to named format using ordinal names for internal consistency
    val ordinalNames = offsetArray.indices.map(_.toString)
    val namedOffsets = ordinalNames.zip(offsetArray).filter(_._2 != null).map {
      case (name, offset) => name -> offset
    }.toMap
    OffsetSeq(namedOffsets, metadata.map(OffsetSeqMetadata.apply))
  }

  private def deserializeV2(lines: Iterator[String]): OffsetSeq = {
    implicit val format: Formats = DefaultFormats

    // read metadata
    val metadata = lines.next().trim match {
      case "" => None
      case md => Some(md)
    }

    // read named sources JSON
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete V2 offset log file: missing sources")
    }
    val sourcesJson = lines.next()
    val sourcesMap = JsonMethods.parse(sourcesJson).extract[Map[String, String]]

    // Convert to named offsets
    val namedOffsets = sourcesMap.map { case (name, offsetJson) =>
      name -> (if (offsetJson == OffsetSeqLog.SERIALIZED_VOID_OFFSET) {
        null
      } else {
        SerializedOffset(offsetJson)
      })
    }.filter(_._2 != null) // Remove null offsets from the map

    OffsetSeq.fillNamed(metadata, namedOffsets)
  }

  override protected def serialize(offsetSeq: OffsetSeq, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller

    // Auto-detect format based on offset names
    // If all names are ordinal (0, 1, 2, etc.), use V1 for backward compatibility
    // If any name is non-ordinal, use V2
    val useV2 = offsetSeq.offsets.nonEmpty && !allOffsetsAreOrdinal(offsetSeq.offsets.keys)

    if (useV2) {
      serializeV2(offsetSeq, out)
    } else {
      serializeV1(offsetSeq, out)
    }
  }

  /**
   * Checks if all offset names are ordinal (0, 1, 2, ..., n-1) where n is the number of offsets.
   * This indicates compatibility with V1 positional format.
   */
  private def allOffsetsAreOrdinal(names: Iterable[String]): Boolean = {
    val sortedNames = names.toSeq.sorted
    sortedNames.zipWithIndex.forall { case (name, index) =>
      name == index.toString
    }
  }

  private def serializeV1(offsetSeq: OffsetSeq, out: OutputStream): Unit = {
    out.write(("v" + OffsetSeqLog.VERSION_1).getBytes(UTF_8))

    // write metadata
    out.write('\n')
    out.write(offsetSeq.metadata.map(_.json).getOrElse("").getBytes(UTF_8))

    // Convert from named offsets back to positional for V1 backward compatibility
    // Since we only use V1 for ordinal names, we can safely convert by sorting
    val namedMap = offsetSeq.getNamedOffsets
    val sortedNames = namedMap.keys.toSeq.sorted
    val offsets = sortedNames.map { name =>
      Option(namedMap(name)) // We know the name exists since we got it from keys
    }

    // write offsets, one per line
    offsets.map(_.map(_.json)).foreach { offset =>
      out.write('\n')
      offset match {
        case Some(json: String) => out.write(json.getBytes(UTF_8))
        case None => out.write(OffsetSeqLog.SERIALIZED_VOID_OFFSET.getBytes(UTF_8))
      }
    }
  }

  private def serializeV2(offsetSeq: OffsetSeq, out: OutputStream): Unit = {
    out.write(("v" + OffsetSeqLog.VERSION_2).getBytes(UTF_8))

    // write metadata
    out.write('\n')
    out.write(offsetSeq.metadata.map(_.json).getOrElse("").getBytes(UTF_8))

    // write sources as JSON map
    out.write('\n')
    implicit val format: Formats = DefaultFormats
    val namedOffsets = offsetSeq.getNamedOffsets
    val sourcesMap = namedOffsets.map { case (name, offset) =>
      name -> (if (offset == null) OffsetSeqLog.SERIALIZED_VOID_OFFSET else offset.json)
    }
    out.write(Serialization.write(sourcesMap).getBytes(UTF_8))
  }

  def offsetSeqMetadataForBatchId(batchId: Long): Option[OffsetSeqMetadata] = {
    if (batchId < 0) None else get(batchId).flatMap(_.metadata)
  }
}

object OffsetSeqLog {
  private[streaming] val VERSION_1 = 1
  private[streaming] val VERSION_2 = 2
  private[streaming] val VERSION = VERSION_1 // Default version for backward compatibility
  private val SERIALIZED_VOID_OFFSET = "-"
}
