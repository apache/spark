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

package org.apache.spark.sql.sources.v2.state

import java.io.File

import org.scalatest.{Assertions, BeforeAndAfterAll}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.state.StateDataSourceV2
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf

class StateDataSourceV2ReadSuite
  extends StateStoreTestBase
  with BeforeAndAfterAll
  with Assertions {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("reading state from simple aggregation - state format version 1") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "1"): _*) {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val stateSchema = getSchemaForLargeDataStreamingAggregationQuery(1)

        val operatorId = 0
        val batchId = 1

        val stateReadDf = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateDataSourceV2.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(StateDataSourceV2.PARAM_VERSION, batchId + 1)
          .option(StateDataSourceV2.PARAM_OPERATOR_ID, operatorId)
          .load()

        logInfo(s"Schema: ${stateReadDf.schema.treeString}")

        checkAnswer(
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "value.groupKey AS value_groupKey",
              "value.cnt AS value_cnt", "value.sum AS value_sum", "value.max AS value_max",
              "value.min AS value_min"),
          Seq(
            Row(0, 0, 4, 60, 30, 0), // 0, 10, 20, 30
            Row(1, 1, 4, 64, 31, 1), // 1, 11, 21, 31
            Row(2, 2, 4, 68, 32, 2), // 2, 12, 22, 32
            Row(3, 3, 4, 72, 33, 3), // 3, 13, 23, 33
            Row(4, 4, 4, 76, 34, 4), // 4, 14, 24, 34
            Row(5, 5, 4, 80, 35, 5), // 5, 15, 25, 35
            Row(6, 6, 4, 84, 36, 6), // 6, 16, 26, 36
            Row(7, 7, 4, 88, 37, 7), // 7, 17, 27, 37
            Row(8, 8, 4, 92, 38, 8), // 8, 18, 28, 38
            Row(9, 9, 4, 96, 39, 9) // 9, 19, 29, 39
          )
        )
      }
    }
  }

  test("reading state from simple aggregation - state format version 2") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val stateSchema = getSchemaForLargeDataStreamingAggregationQuery(2)

        val operatorId = 0
        val batchId = 1

        val stateReadDf = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateDataSourceV2.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(StateDataSourceV2.PARAM_VERSION, batchId + 1)
          .option(StateDataSourceV2.PARAM_OPERATOR_ID, operatorId)
          .load()

        logInfo(s"Schema: ${stateReadDf.schema.treeString}")

        checkAnswer(
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "value.cnt AS value_cnt",
              "value.sum AS value_sum", "value.max AS value_max", "value.min AS value_min"),
          Seq(
            Row(0, 4, 60, 30, 0), // 0, 10, 20, 30
            Row(1, 4, 64, 31, 1), // 1, 11, 21, 31
            Row(2, 4, 68, 32, 2), // 2, 12, 22, 32
            Row(3, 4, 72, 33, 3), // 3, 13, 23, 33
            Row(4, 4, 76, 34, 4), // 4, 14, 24, 34
            Row(5, 4, 80, 35, 5), // 5, 15, 25, 35
            Row(6, 4, 84, 36, 6), // 6, 16, 26, 36
            Row(7, 4, 88, 37, 7), // 7, 17, 27, 37
            Row(8, 4, 92, 38, 8), // 8, 18, 28, 38
            Row(9, 4, 96, 39, 9) // 9, 19, 29, 39
          )
        )
      }
    }
  }

  test("reading state from simple aggregation - composite key") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempDir { tempDir =>
        runCompositeKeyStreamingAggregationQuery(tempDir.getAbsolutePath)

        val stateSchema = getSchemaForCompositeKeyStreamingAggregationQuery(2)

        val operatorId = 0
        val batchId = 1

        val stateReadDf = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateDataSourceV2.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(StateDataSourceV2.PARAM_VERSION, batchId + 1)
          .option(StateDataSourceV2.PARAM_OPERATOR_ID, operatorId)
          .load()

        logInfo(s"Schema: ${stateReadDf.schema.treeString}")

        checkAnswer(
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "key.fruit AS key_fruit",
              "value.cnt AS value_cnt", "value.sum AS value_sum", "value.max AS value_max",
              "value.min AS value_min"),
          Seq(
            Row(0, "Apple", 2, 6, 6, 0),
            Row(1, "Banana", 2, 8, 7, 1),
            Row(0, "Strawberry", 2, 10, 8, 2),
            Row(1, "Apple", 2, 12, 9, 3),
            Row(0, "Banana", 2, 14, 10, 4),
            Row(1, "Strawberry", 1, 5, 5, 5)
          )
        )
      }
    }
  }
}
