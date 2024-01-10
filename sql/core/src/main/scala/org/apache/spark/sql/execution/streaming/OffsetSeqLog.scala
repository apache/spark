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

package org.apache.spark.sql.execution.streaming


import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets._

import scala.io.{Source => IOSource}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}

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
 */
class OffsetSeqLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[OffsetSeq](sparkSession, path) {

  override protected def deserialize(in: InputStream): OffsetSeq = {
    // called inside a try-finally where the underlying stream is closed in the caller
    def parseOffset(value: String): OffsetV2 = value match {
      case OffsetSeqLog.SERIALIZED_VOID_OFFSET => null
      case json => SerializedOffset(json)
    }
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }

    validateVersion(lines.next(), OffsetSeqLog.VERSION)

    // read metadata
    val metadata = lines.next().trim match {
      case "" => None
      case md => Some(md)
    }
    import org.apache.spark.util.ArrayImplicits._
    OffsetSeq.fill(metadata, lines.map(parseOffset).toArray.toImmutableArraySeq: _*)
  }

  override protected def serialize(offsetSeq: OffsetSeq, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(("v" + OffsetSeqLog.VERSION).getBytes(UTF_8))

    // write metadata
    out.write('\n')
    out.write(offsetSeq.metadata.map(_.json).getOrElse("").getBytes(UTF_8))

    // write offsets, one per line
    offsetSeq.offsets.map(_.map(_.json)).foreach { offset =>
      out.write('\n')
      offset match {
        case Some(json: String) => out.write(json.getBytes(UTF_8))
        case None => out.write(OffsetSeqLog.SERIALIZED_VOID_OFFSET.getBytes(UTF_8))
      }
    }
  }

  def offsetSeqMetadataForBatchId(batchId: Long): Option[OffsetSeqMetadata] = {
    if (batchId < 0) None else get(batchId).flatMap(_.metadata)
  }
}

object OffsetSeqLog {
  private[streaming] val VERSION = 1
  private val SERIALIZED_VOID_OFFSET = "-"
}
