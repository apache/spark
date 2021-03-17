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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class StreamingSessionWindowStateManagerSuite extends SparkFunSuite with BeforeAndAfter {

  val keySchema = StructType(Seq(StructField("key", StringType, true)))
  val timeSchema = StructType(Seq(StructField("startTime", TimestampType, true),
    StructField("endTime", TimestampType, true)))
  val inputSchema = StructType(Seq(
    StructField("key", StringType, true),
    StructField("value", IntegerType, true)))
  val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  val keyAttributes = keySchema.toAttributes
  val timeAttribute = timeSchema.toAttributes.head
  val inputRowAttributes = inputSchema.toAttributes
  val inputValueAttributes = valueSchema.toAttributes

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  test("Load current state manager") {
    withTempDir { tempPath =>
      val operatorStateInfo = getOperatorStateInfo(tempPath.getAbsolutePath)
      val v1StateManager = getStreamingSessionWindowStateManager(operatorStateInfo, 1)

      assert(v1StateManager.isInstanceOf[StreamingSessionWindowStateManagerImplV1])
    }
  }

  test("Fail to load invalid state manager") {
    withTempDir { tempPath =>
      val operatorStateInfo = getOperatorStateInfo(tempPath.getAbsolutePath)
      val err = intercept[IllegalArgumentException] {
        getStreamingSessionWindowStateManager(operatorStateInfo, 2)
      }
      assert(err.getMessage.contains("Version 2 is invalid"))
    }
  }

  test("get/set start times for given key") {
    withTempDir { tempPath =>
      val operatorStateInfo = getOperatorStateInfo(tempPath.getAbsolutePath)
      val stateManager = getStreamingSessionWindowStateManager(operatorStateInfo, 1)

      val keyRow = getKeyRow("a")
      val emptyStates = stateManager.getStates(keyRow)
      assert(emptyStates.isEmpty)


      val valueRow1 = getValueRow(1)
      stateManager.putState(keyRow, 3, valueRow1)
      stateManager.putStartTimeList(keyRow, Seq(3))

      val oneStates = stateManager.getStates(keyRow)
      assert(oneStates.length == 1)
      assert(oneStates.head.equals(valueRow1))

      val valueRow2 = getValueRow(8)
      stateManager.putState(keyRow, 5, valueRow2)
      stateManager.putStartTimeList(keyRow, Seq(3, 5))

      val twoStates = stateManager.getStates(keyRow)
      assert(twoStates.length == 2)
      assert(twoStates.head.equals(valueRow1) && twoStates(1).equals(valueRow2))
    }
  }

  private def getKeyRow(key: String) = {
    val stringToUnsafeRow = UnsafeProjection.create(keySchema)
    val row = new SpecificInternalRow(keySchema)
    row.update(0, UTF8String.fromString(key))
    stringToUnsafeRow(row)
  }

  private def getValueRow(value: Int) = {
    val intToUnsafeRow = UnsafeProjection.create(valueSchema)
    val row = new SpecificInternalRow(valueSchema)
    row.setInt(0, value)
    intToUnsafeRow(row)
  }

  private def getOperatorStateInfo(
      path: String,
      queryRunId: UUID = UUID.randomUUID,
      version: Int = 0): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(path, queryRunId, operatorId = 0, version, numPartitions = 5)
  }

  private def getStreamingSessionWindowStateManager(
      stateInfo: StatefulOperatorStateInfo,
      formatVersion: Int): StreamingSessionWindowStateManager = {
    val storeConf = new StateStoreConf(new SQLConf())
    val hadoopConf: Configuration = new Configuration

    StreamingSessionWindowStateManager.createStateManager(
      keyAttributes, timeAttribute, inputValueAttributes, inputRowAttributes,
      Some(stateInfo), storeConf, hadoopConf, partitionId = 0, stateFormatVersion = formatVersion)
  }
}
