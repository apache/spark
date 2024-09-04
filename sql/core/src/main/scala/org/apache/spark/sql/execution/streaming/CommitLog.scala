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

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession
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
class CommitLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[CommitMetadata](sparkSession, path) {

  import CommitLog._

  override protected[sql] def deserialize(in: InputStream): CommitMetadata = {
    // called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file in the offset commit log")
    }
    // TODO [SPARK-49462] This validation should be relaxed for a stateless query.
    validateVersion(lines.next().trim, VERSION)
    val metadataJson = if (lines.hasNext) lines.next() else EMPTY_JSON
    CommitMetadata(metadataJson)
  }

  override protected[sql] def serialize(metadata: CommitMetadata, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(s"v${VERSION}".getBytes(UTF_8))
    out.write('\n')

    // write metadata
    out.write(metadata.json.getBytes(UTF_8))
  }
}

object CommitLog {
  private val VERSION = SQLConf.get.stateStoreCheckpointFormatVersion
  private val EMPTY_JSON = "{}"
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
 * @param nextBatchWatermarkMs The watermark of the next batch.
 * @param stateUniqueIds Map[Long, Array[String]] of map
 *                       OperatorId -> (partitionID -> uniqueID)
 */

case class CommitMetadata(
    nextBatchWatermarkMs: Long = 0,
    stateUniqueIds: Map[Long, Array[String]] = Map.empty) {
  def json: String = Serialization.write(this)(CommitMetadata.format)
}

object CommitMetadata {
  implicit val format: Formats = Serialization.formats(NoTypeHints)

  def apply(json: String): CommitMetadata = Serialization.read[CommitMetadata](json)
}
