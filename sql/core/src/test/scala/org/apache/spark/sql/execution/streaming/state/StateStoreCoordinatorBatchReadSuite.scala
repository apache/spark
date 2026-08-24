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
package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}

/**
 * Regression test for SPARK-58973: Stateful operations fail in non-streaming mode
 * when StreamingQueryManager hasn't been initialized.
 *
 * This is a separate test file because:
 * - StateDataSourceTestBase.beforeEach() calls spark.streams.stateStoreCoordinator which
 *   initializes the coordinator, masking the bug in all existing state data source tests.
 * - We need private[state] access to stop the coordinator and verify coordinatorRef behavior.
 */
class StateStoreCoordinatorBatchReadSuite extends StreamTest {
  import testImplicits._

  test("SPARK-58973: coordinatorRef returns None when coordinator is not registered") {
    // Stop the coordinator to simulate a fresh session where
    // StreamingQueryManager was never initialized (lazy val in SessionState).
    spark.sessionState.streamingQueryManager.stateStoreCoordinator.stop()
    StateStore.stop()

    // Unit-level verification: coordinatorRef should gracefully return None
    // instead of throwing RpcEndpointNotFoundException.
    assert(StateStoreProvider.coordinatorRef.isEmpty,
      "coordinatorRef should return None when the coordinator endpoint is not registered")
  }

  test("SPARK-58973: reading state store succeeds when coordinator is not initialized") {
    withTempDir { tempDir =>
      val checkpointDir = tempDir.getAbsolutePath

      // Step 1: Create a stateful checkpoint via a streaming query
      val inputData = MemoryStream[Int]
      val aggregated = inputData.toDF()
        .groupBy("value")
        .count()

      testStream(aggregated, OutputMode.Update())(
        StartStream(checkpointLocation = checkpointDir),
        AddData(inputData, 1, 2, 3, 1, 2),
        CheckLastBatch((1, 2), (2, 2), (3, 1)),
        StopStream
      )

      // Step 2: Stop the coordinator to simulate a fresh session where
      // StreamingQueryManager was never initialized.
      // This reproduces the exact scenario reported in SPARK-58973.
      spark.sessionState.streamingQueryManager.stateStoreCoordinator.stop()
      StateStore.stop()

      // Step 3: Reading the state store should succeed even without the coordinator.
      // Before the fix, this would throw:
      //   SparkException: [CANNOT_LOAD_STATE_STORE.UNCATEGORIZED]
      //   Caused by: RpcEndpointNotFoundException: Cannot find endpoint:
      //     spark://StateStoreCoordinator@...
      val stateDF = spark.read
        .format("statestore")
        .load(checkpointDir)

      val rows = stateDF.collect()
      assert(rows.nonEmpty, "State store should contain data from the streaming query")
    }
  }
}
