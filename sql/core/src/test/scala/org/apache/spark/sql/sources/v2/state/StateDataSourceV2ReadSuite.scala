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

import org.scalatest.{Assertions, BeforeAndAfterAll}

import org.apache.spark.sql.{Encoders, Row}
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

  test("simple aggregation, state ver 1, infer schema = false") {
    testStreamingAggregation(1, inferSchema = false)
  }

  test("simple aggregation, state ver 1, infer schema = true") {
    testStreamingAggregation(1, inferSchema = true)
  }

  test("simple aggregation, state ver 2, infer schema = false") {
    testStreamingAggregation(2, inferSchema = false)
  }

  test("simple aggregation, state ver 2, infer schema = true") {
    testStreamingAggregation(2, inferSchema = true)
  }

  test("composite key aggregation, state ver 1, infer schema = false") {
    testStreamingAggregationWithCompositeKey(1, inferSchema = false)
  }

  test("composite key aggregation, state ver 1, infer schema = true") {
    testStreamingAggregationWithCompositeKey(1, inferSchema = true)
  }

  test("composite key aggregation, state ver 2, infer schema = false") {
    testStreamingAggregationWithCompositeKey(2, inferSchema = false)
  }

  test("composite key aggregation, ver 2, infer schema = true") {
    testStreamingAggregationWithCompositeKey(2, inferSchema = true)
  }

  private def testStreamingAggregation(stateVersion: Int, inferSchema: Boolean): Unit = {
    withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val operatorId = 0
        val batchId = 2

        val stateReader = spark.read
          .format("state")
          .option(StateDataSourceV2.PARAM_CHECKPOINT_LOCATION, tempDir.getAbsolutePath)
          // explicitly specifying version and operator ID to test out the functionality
          .option(StateDataSourceV2.PARAM_VERSION, batchId + 1)
          .option(StateDataSourceV2.PARAM_OPERATOR_ID, operatorId)

        val stateReadDf = if (inferSchema) {
          stateReader.load()
        } else {
          val stateSchema = getSchemaForLargeDataStreamingAggregationQuery(stateVersion)
          stateReader.schema(stateSchema).load()
        }

        logInfo(s"Schema: ${stateReadDf.schema.treeString}")

        val resultDf = if (inferSchema) {
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "value.count AS value_cnt",
              "value.sum AS value_sum", "value.max AS value_max", "value.min AS value_min")
        } else {
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "value.cnt AS value_cnt",
              "value.sum AS value_sum", "value.max AS value_max", "value.min AS value_min")
        }

        checkAnswer(
          resultDf,
          Seq(
            Row(0, 5, 60, 30, 0), // 0, 10, 20, 30
            Row(1, 5, 65, 31, 1), // 1, 11, 21, 31
            Row(2, 5, 70, 32, 2), // 2, 12, 22, 32
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

  private def testStreamingAggregationWithCompositeKey(
      stateVersion: Int,
      inferSchema: Boolean): Unit = {
    withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
      withTempDir { tempDir =>
        runCompositeKeyStreamingAggregationQuery(tempDir.getAbsolutePath)

        val stateReader = spark.read
          .format("state")
          .option(StateDataSourceV2.PARAM_CHECKPOINT_LOCATION, tempDir.getAbsolutePath)
          // skip version and operator ID to test out functionalities

        val stateReadDf = if (inferSchema) {
          stateReader.load()
        } else {
          val stateSchema = getSchemaForCompositeKeyStreamingAggregationQuery(stateVersion)
          stateReader.schema(stateSchema).load()
        }

        logInfo(s"Schema: ${stateReadDf.schema.treeString}")

        val resultDf = if (inferSchema) {
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "key.fruit AS key_fruit",
              "value.count AS value_cnt", "value.sum AS value_sum", "value.max AS value_max",
              "value.min AS value_min")
        } else {
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "key.fruit AS key_fruit",
              "value.cnt AS value_cnt", "value.sum AS value_sum", "value.max AS value_max",
              "value.min AS value_min")
        }

        checkAnswer(
          resultDf,
          Seq(
            Row(0, "Apple", 2, 6, 6, 0),
            Row(1, "Banana", 3, 9, 7, 1),
            Row(0, "Strawberry", 3, 12, 8, 2),
            Row(1, "Apple", 3, 15, 9, 3),
            Row(0, "Banana", 2, 14, 10, 4),
            Row(1, "Strawberry", 1, 5, 5, 5)
          )
        )
      }
    }
  }

  test("flatMapGroupsWithState, state ver 1, infer schema = false") {
    testFlatMapGroupsWithState(1, inferSchema = false)
  }

  test("flatMapGroupsWithState, state ver 1, infer schema = true") {
    testFlatMapGroupsWithState(1, inferSchema = true)
  }

  test("flatMapGroupsWithState, state ver 2, infer schema = false") {
    testFlatMapGroupsWithState(2, inferSchema = false)
  }

  test("flatMapGroupsWithState, state ver 2, infer schema = true") {
    testFlatMapGroupsWithState(2, inferSchema = true)
  }

  private def testFlatMapGroupsWithState(stateVersion: Int, inferSchema: Boolean): Unit = {
    withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
      withTempDir { tempDir =>
        runFlatMapGroupsWithStateQuery(tempDir.getAbsolutePath)

        val stateReader = spark.read
          .format("state")
          .option(StateDataSourceV2.PARAM_CHECKPOINT_LOCATION, tempDir.getAbsolutePath)

        val stateReadDf = if (inferSchema) {
          stateReader.load()
        } else {
          val stateSchema = getSchemaForFlatMapGroupsWithStateQuery(stateVersion)
          stateReader.schema(stateSchema).load()
        }

        val resultDf = if (stateVersion == 1) {
          stateReadDf
            .selectExpr("key.value AS key_value", "value.numEvents AS value_numEvents",
              "value.startTimestampMs AS value_startTimestampMs",
              "value.endTimestampMs AS value_endTimestampMs",
              "value.timeoutTimestamp AS value_timeoutTimestamp")
        } else { // stateVersion == 2
          stateReadDf
            .selectExpr("key.value AS key_value", "value.groupState.numEvents AS value_numEvents",
              "value.groupState.startTimestampMs AS value_startTimestampMs",
              "value.groupState.endTimestampMs AS value_endTimestampMs",
              "value.timeoutTimestamp AS value_timeoutTimestamp")
        }

        checkAnswer(
          resultDf,
          Seq(
            Row("hello", 4, 1000, 4000, 12000),
            Row("world", 2, 1000, 3000, 12000),
            Row("scala", 2, 2000, 4000, 12000)
          )
        )

        // try to read the value via case class provided in actual query
        implicit val encoder = Encoders.product[SessionInfo]
        val df = if (stateVersion == 1) {
          stateReadDf.selectExpr("value.*").drop("timeoutTimestamp").as[SessionInfo]
        } else { // state version == 2
          stateReadDf.selectExpr("value.groupState.*").as[SessionInfo]
        }

        val expected = Array(
          SessionInfo(4, 1000, 4000),
          SessionInfo(2, 1000, 3000),
          SessionInfo(2, 2000, 4000)
        )
        assert(df.collect().toSet === expected.toSet)
      }
    }
  }
}
