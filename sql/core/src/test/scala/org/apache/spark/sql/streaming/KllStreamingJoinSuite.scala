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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf

class KllStreamingJoinSuite extends StreamTest {
  import testImplicits._

  for (version <- Seq(1, 2); provider <- Seq(
      classOf[HDFSBackedStateStoreProvider].getName,
      classOf[RocksDBStateStoreProvider].getName)) {
    test(s"KLL streaming join checkpoint layouts: state=$version provider=$provider") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> provider,
          SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        for (recorded <- Seq(None, Some(true), Some(false));
            stageLeft <- Seq(false, true); userAlias <- Seq(false, true)) {
          withTempDir { checkpoint =>
            val rollback = "spark.sql.legacy.kllAggregateDeterministic.enabled"
            val sharing = "spark.sql.streaming.aggregation.legacyKllSharing.enabled"
            val legacy = recorded.getOrElse(true)
            val left = MemoryStream[Int]
            val right = MemoryStream[Int]
            val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(v))
                |FROM VALUES (0), (1), (2) AS t(v))""".stripMargin
            def query(analysisLegacy: Boolean): DataFrame =
              withSQLConf(rollback -> analysisLegacy.toString) {
                def input(stream: MemoryStream[Int], staged: Boolean): DataFrame = {
                  val data = stream.toDF()
                  if (userAlias && staged) data.selectExpr("value", s"$scalar AS s") else data
                }
                val value = if (userAlias) (if (stageLeft) "l.s" else "r.s") else scalar
                val condition = if (stageLeft) s"l.value + $value = r.value"
                  else s"l.value = r.value + $value"
                input(left, stageLeft).as("l").join(input(right, !stageLeft).as("r"),
                  expr(condition)).selectExpr("l.value", "r.value")
              }
            // Rollback construction independently recreates the historical unstaged layout.
            val initial = query(analysisLegacy = legacy)
            val current = query(analysisLegacy = false)
            val l = if (stageLeft) 0 else 3
            val r = if (stageLeft) 3 else 0
            def checkPayload: AssertOnQuery = AssertOnQuery { query =>
              val join = query.lastExecution.executedPlan.collectFirst {
                case j: StreamingSymmetricHashJoinExec => j
              }.get
              val staged = if (stageLeft) join.left else join.right
              staged.output.size == (if (userAlias || !legacy) 2 else 1)
            }
            testStream(initial)(
              StartStream(checkpointLocation = checkpoint.getAbsolutePath,
                additionalConfs = Map(sharing -> legacy.toString)),
              AddData(left, l), AddData(right, r), CheckLastBatch(Row(l, r)),
              checkPayload, StopStream)
            val logPath = new File(checkpoint, "offsets").getAbsolutePath
            if (recorded.isEmpty) {
              val (batchId, offsets) = new OffsetSeqLog(spark, logPath).getLatest().get
              val historical = offsets match {
                case o: OffsetSeq =>
                  o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = m.conf - sharing)))
                case o: OffsetMap =>
                  o.copy(metadata = o.metadata.copy(conf = o.metadata.conf - sharing))
              }
              val path = new Path(logPath, batchId.toString)
              assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
              assert(new OffsetSeqLog(spark, logPath).add(batchId, historical))
            }
            testStream(current)(
              StartStream(checkpointLocation = checkpoint.getAbsolutePath,
                additionalConfs = Map(sharing -> (!legacy).toString)),
              AddData(right, r), CheckLastBatch(Row(l, r)),
              AddData(left, l + 1), CheckLastBatch(),
              AddData(right, r + 1), CheckLastBatch(Row(l + 1, r + 1)),
              checkPayload, StopStream)
            assert(new OffsetSeqLog(spark, logPath).getLatest().get._2
              .metadataOpt.get.conf(sharing) == legacy.toString)
          }
        }
      }
    }
  }
}
