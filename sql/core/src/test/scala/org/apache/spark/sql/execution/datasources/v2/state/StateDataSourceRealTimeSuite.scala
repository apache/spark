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
package org.apache.spark.sql.execution.datasources.v2.state

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink,
  LowLatencyMemoryStream}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamRealTimeModeManualClockSuiteBase, TimeMode}

/**
 * State data source reads under Real-Time Mode (RTM).
 */
class StateDataSourceRealTimeSuite
    extends StreamRealTimeModeManualClockSuiteBase
    with StateDataSourceTestBase {

  import testImplicits._

  private def assertEventuallyBatchCommitted(batchId: Long): StreamAction = {
    Execute(s"Assert batch $batchId is committed") { q =>
      eventually(timeout(1.minute)) {
        assert(q.commitLog.getLatest().get._1 === batchId)
      }
    }
  }

  test("transformWithState + RTM: state data source read") {
    withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      withTempDir { tempDir =>
        val input = LowLatencyMemoryStream[String](2)
        val query = input.toDS()
          .groupByKey(x => x)
          .transformWithState(
            new StatefulProcessorWithSingleValueVar(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
        val checkpoint = tempDir.getCanonicalPath

        testStream(query, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(trigger = defaultTrigger, checkpointLocation = checkpoint),
          AddData(input, "a", "b"),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "1"), ("b", "1")),
          advanceRealTimeClock,
          assertEventuallyBatchCommitted(0),
          StopStream
        )

        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, checkpoint)
          .option(StateSourceOptions.STATE_VAR_NAME, "valueState")
          .load()

        checkAnswer(
          stateReaderDf.selectExpr(
            "key.value AS groupingKey",
            "value.id AS valueId",
            "value.name AS valueName"),
          Seq(Row("a", 1L, "dummyKey"), Row("b", 1L, "dummyKey")))
      }
    }
  }
}
