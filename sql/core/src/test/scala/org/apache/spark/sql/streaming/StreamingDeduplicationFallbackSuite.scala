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

package org.apache.spark.sql.streaming

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog, OffsetSeqMetadata, OffsetSeqMetadataV2}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf

/**
 * End-to-end tests for the dropDuplicates key-order fallback. A streaming query started before
 * [[SQLConf.DROP_DUPLICATES_DETERMINISTIC_KEY_ORDER]] existed has no such entry in its offset log;
 * on restart it must fall back to the legacy key order so the persisted state-store keys (bound by
 * position) still align. The session default stays true (deterministic), so these tests prove the
 * fallback is driven by the offset log, not the session. See SPARK-57489.
 */
class StreamingDeduplicationFallbackSuite extends StateStoreMetricsTest {
  import testImplicits._

  private val confKey = SQLConf.DROP_DUPLICATES_DETERMINISTIC_KEY_ORDER.key

  // A projection AFTER dropDuplicates is the case that matters: by the time the streaming query is
  // started the dedup keys are already resolved (with the recipe attached to the Deduplicate node),
  // so the offset-log fallback must recompute them from the recipe rather than rely on an
  // unresolved node surviving until query start.
  private def dedupAllColumns(input: MemoryStream[(Int, Int, Int, Int, Int)]) =
    input.toDS().toDF("a", "b", "c", "d", "e").dropDuplicates().select("a", "b", "c", "d", "e")

  /** Rewrites batch `batchId`'s offset-log metadata, applying `f` to its persisted conf map. */
  private def rewriteOffsetLogConf(
      checkpoint: File,
      batchId: Long)(f: Map[String, String] => Map[String, String]): Unit = {
    val offsetsPath = checkpoint.getCanonicalPath + "/offsets"
    val offsetLog = new OffsetSeqLog(spark, offsetsPath)
    val offsetSeq = offsetLog.get(batchId).getOrElse {
      fail(s"Missing offset log entry for batch $batchId")
    }
    val updatedMetadata = offsetSeq.metadataOpt.map {
      case m: OffsetSeqMetadata => m.copy(conf = f(m.conf))
      case m: OffsetSeqMetadataV2 => m.copy(conf = f(m.conf))
    }
    val updatedOffsetSeq = offsetSeq match {
      case o: OffsetSeq =>
        o.copy(metadataOpt = updatedMetadata.map(_.asInstanceOf[OffsetSeqMetadata]))
      case o: OffsetMap =>
        // OffsetMap's metadata is required (not optional), so fall back to the original when the
        // rewrite produced none.
        o.copy(metadata =
          updatedMetadata.map(_.asInstanceOf[OffsetSeqMetadataV2]).getOrElse(o.metadata))
    }
    // Offset log entries are immutable once written; delete before re-adding.
    val hadoopConf = spark.sessionState.newHadoopConf()
    val batchFile = new Path(offsetsPath, batchId.toString)
    batchFile.getFileSystem(hadoopConf).delete(batchFile, false)
    assert(
      offsetLog.add(batchId, updatedOffsetSeq),
      s"Failed to rewrite offset log entry for batch $batchId")
  }

  private def readPinnedConf(checkpoint: File, batchId: Long): Option[String] = {
    val offsetLog = new OffsetSeqLog(spark, checkpoint.getCanonicalPath + "/offsets")
    offsetLog.get(batchId).flatMap(_.metadataOpt.map(_.conf)).flatMap(_.get(confKey))
  }

  test("SPARK-57489: existing checkpoint without the conf falls back to the legacy key order") {
    withTempDir { checkpoint =>
      // The same MemoryStream is reused across both runs so source offsets advance across the
      // restart (a fresh stream would reset to an already-committed offset and process nothing).
      val input = MemoryStream[(Int, Int, Int, Int, Int)]

      // Run 1 with the deterministic order disabled writes the state-store keys in the legacy
      // (origin-engine) order; confirm the offset log captured the legacy value.
      withSQLConf(confKey -> "false") {
        testStream(dedupAllColumns(input), Append)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, (1, 2, 3, 4, 5)),
          CheckLastBatch((1, 2, 3, 4, 5)),
          StopStream)
      }
      assert(readPinnedConf(checkpoint, batchId = 0).contains("false"))

      // Simulate a checkpoint created before this change: drop the conf from the offset log.
      rewriteOffsetLogConf(checkpoint, batchId = 0)(_ - confKey)

      // Restart with the deterministic session default (true). If the absent offset-log entry were
      // NOT pinned to the legacy order, the new run would encode keys in schema order while the
      // restored state holds them in Set order; the previously-seen row would no longer be
      // recognized as a duplicate and would be (incorrectly) emitted, failing CheckLastBatch.
      testStream(dedupAllColumns(input), Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, (1, 2, 3, 4, 5), (6, 7, 8, 9, 10)),
        CheckLastBatch((6, 7, 8, 9, 10)),
        StopStream)
    }
  }

  test("SPARK-57489: newly started query pins the deterministic order and keeps it on restart") {
    withTempDir { checkpoint =>
      val input = MemoryStream[(Int, Int, Int, Int, Int)]

      // Session default is deterministic (true); a newly started query must pin "true" at batch 0.
      testStream(dedupAllColumns(input), Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, (1, 2, 3, 4, 5)),
        CheckLastBatch((1, 2, 3, 4, 5)),
        StopStream)
      assert(readPinnedConf(checkpoint, batchId = 0).contains("true"))

      // Restart with the session default flipped to the legacy order. The "true" pinned in the
      // offset log must override the session value, so the dedup state is still honored under the
      // deterministic order. (If the session value were used instead, the keys would be encoded in
      // the legacy order while the restored state holds them in the deterministic order, so the
      // previously-seen row would no longer be recognized as a duplicate and would be
      // (incorrectly) emitted, failing CheckLastBatch.)
      withSQLConf(confKey -> "false") {
        testStream(dedupAllColumns(input), Append)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, (1, 2, 3, 4, 5), (6, 7, 8, 9, 10)),
          CheckLastBatch((6, 7, 8, 9, 10)),
          StopStream)
      }
    }
  }
}
