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

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{count, timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock

class ProgressReporterSuite extends StreamTest {

  import testImplicits._

  test("no-data batch resets numRowsRemoved to zero via resetExecStatsForNoExecution") {
    // After a stateful operator evicts state rows via watermark, subsequent no-execution triggers
    // must report numRowsRemoved=0. Exercises: finishNoExecutionTrigger ->
    // resetExecStatsForNoExecution -> StateOperatorProgress.copy
    val clock = new StreamManualClock
    val input = MemoryStream[Int]
    val agg = input.toDF()
      .select(timestamp_seconds($"value") as "ts", $"value")
      .withWatermark("ts", "10 seconds")
      .groupBy(window($"ts", "10 seconds"))
      .agg(count("*") as "cnt")

    withSQLConf(SQLConf.STREAMING_POLLING_DELAY.key -> "0") {
      testStream(agg, outputMode = OutputMode.Update)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(input, 1, 2, 3),
        AdvanceManualClock(1 * 1000),
        AddData(input, 21),
        AdvanceManualClock(1 * 1000),
        Execute("verify eviction") { q =>
          val lastProgress = q.recentProgress.filter(_.stateOperators.nonEmpty).last
          val removed = lastProgress.stateOperators.head.numRowsRemoved
          assert(removed > 0, s"Expected eviction but numRowsRemoved=$removed")
        },
        // Manual clock advance schedules the next trigger; with no runnable batch the engine
        // reports progress via finishNoExecutionTrigger -> resetExecStatsForNoExecution.
        AdvanceManualClock(1 * 1000),
        AssertOnQuery("numRowsRemoved must be 0 in idle trigger after eviction") { q =>
          val progresses = q.recentProgress.filter(_.stateOperators.nonEmpty)
          val evictionIdx = progresses.lastIndexWhere(_.stateOperators.head.numRowsRemoved > 0)
          assert(evictionIdx >= 0, "Expected eviction batch in progresses")

          val idleProgresses =
            progresses.drop(evictionIdx + 1).filter(_.batchDuration == 0)
          assert(
            idleProgresses.nonEmpty,
            "Expected idle trigger progress after eviction, after eviction: " +
              progresses.drop(evictionIdx).map(p =>
                s"(batchId=${p.batchId},duration=${p.batchDuration}").mkString(", "))

          val idleProgress = idleProgresses.head
          assert(
            idleProgress.stateOperators.head.numRowsRemoved === 0,
            s"numRowsRemoved should be 0 in idle trigger but got " +
              s"${idleProgress.stateOperators.head.numRowsRemoved}")
          assert(
            idleProgress.stateOperators.head.numRowsUpdated === 0,
            s"numRowsUpdated should be 0 in idle trigger but got " +
              s"${idleProgress.stateOperators.head.numRowsUpdated}")
          assert(
            idleProgress.stateOperators.head.numRowsDroppedByWatermark === 0,
            s"numRowsDroppedByWatermark should be 0 in idle trigger but got " +
              s"${idleProgress.stateOperators.head.numRowsDroppedByWatermark}")
          true
        },
        StopStream
      )
    }
  }
}
