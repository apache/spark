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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StateStore, StateStoreConf, StateStoreId, StateStoreProviderId, StreamingSessionWindowStateManager}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MergingSortWithSessionWindowStateIteratorSuite extends StreamTest with BeforeAndAfter {

  private val rowSchema = new StructType().add("key1", StringType).add("key2", IntegerType)
    .add("session", new StructType().add("start", LongType).add("end", LongType))
    .add("value", LongType)
  private val rowAttributes = rowSchema.toAttributes

  private val keysWithoutSessionAttributes = rowAttributes.filter { attr =>
    List("key1", "key2").contains(attr.name)
  }

  private val sessionAttribute = rowAttributes.filter(_.name == "session").head

  private val inputValueGen = UnsafeProjection.create(rowAttributes.map(_.dataType).toArray)
  private val inputKeyGen = UnsafeProjection.create(
    keysWithoutSessionAttributes.map(_.dataType).toArray)

  before {
    SparkSession.setActiveSession(spark)
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  private val providerOptions = Seq(
    classOf[HDFSBackedStateStoreProvider].getCanonicalName,
    classOf[RocksDBStateStoreProvider].getCanonicalName).map { value =>
    (SQLConf.STATE_STORE_PROVIDER_CLASS.key, value.stripSuffix("$"))
  }

  private val availableOptions = for (
    opt1 <- providerOptions;
    opt2 <- StreamingSessionWindowStateManager.supportedVersions
  ) yield (opt1, opt2)

  availableOptions.foreach { case (providerOpt, version) =>
    withSQLConf(providerOpt) {
      test(s"StreamingSessionWindowStateManager " +
        s"provider ${providerOpt._2} state version v${version} - rows only in state") {
        testRowsOnlyInState(version)
      }

      test(s"StreamingSessionWindowStateManager " +
        s"provider ${providerOpt._2} state version v${version} - rows in both input and state") {
        testRowsInBothInputAndState(version)
      }

      test(s"StreamingSessionWindowStateManager " +
        s"provider ${providerOpt._2} state version v${version} - rows only in input") {
        testRowsOnlyInInput(version)
      }
    }
  }

  private def testRowsOnlyInState(stateFormatVersion: Int): Unit = {
    withStateManager(stateFormatVersion) { case (stateManager, store) =>
      val key = createKeyRow("a", 1)
      val values = Seq(
        createRow("a", 1, 100, 110, 1),
        createRow("a", 1, 120, 130, 2),
        createRow("a", 1, 140, 150, 3))

      stateManager.updateSessions(store, key, values)

      val iter = new MergingSortWithSessionWindowStateIterator(
        Iterator.empty,
        stateManager,
        store,
        keysWithoutSessionAttributes,
        sessionAttribute,
        rowAttributes)

      val actual = iter.map(_.copy()).toList
      assert(actual.isEmpty)
    }
  }

  private def testRowsInBothInputAndState(stateFormatVersion: Int): Unit = {
    withStateManager(stateFormatVersion) { case (stateManager, store) =>
      val key1 = createKeyRow("a", 1)
      val key1Values = Seq(
        createRow("a", 1, 100, 110, 1),
        createRow("a", 1, 120, 130, 2),
        createRow("a", 1, 140, 150, 3))

      // This is to ensure sessions will not be populated if the input doesn't have such group key
      val key2 = createKeyRow("a", 2)
      val key2Values = Seq(
        createRow("a", 2, 100, 110, 1),
        createRow("a", 2, 120, 130, 2),
        createRow("a", 2, 140, 150, 3))

      val key3 = createKeyRow("b", 1)
      val key3Values = Seq(
        createRow("b", 1, 100, 110, 1),
        createRow("b", 1, 120, 130, 2),
        createRow("b", 1, 140, 150, 3))

      stateManager.updateSessions(store, key1, key1Values)
      stateManager.updateSessions(store, key2, key2Values)
      stateManager.updateSessions(store, key3, key3Values)

      val inputsForKey1 = Seq(
        createRow("a", 1, 90, 100, 1),
        createRow("a", 1, 125, 135, 2))
      val inputsForKey3 = Seq(
        createRow("b", 1, 150, 160, 3)
      )
      val inputs = inputsForKey1 ++ inputsForKey3

      val iter = new MergingSortWithSessionWindowStateIterator(
        inputs.iterator,
        stateManager,
        store,
        keysWithoutSessionAttributes,
        sessionAttribute,
        rowAttributes)

      val actual = iter.map(_.copy()).toList
      val expected = (key1Values ++ inputsForKey1).sortBy(getSessionStart) ++
        (key3Values ++ inputsForKey3).sortBy(getSessionStart)
      assert(actual === expected.toList)
    }
  }

  private def testRowsOnlyInInput(stateFormatVersion: Int): Unit = {
    withStateManager(stateFormatVersion) { case (stateManager, store) =>
      // This is to ensure sessions will not be populated if the input doesn't have such group key
      val key1 = createKeyRow("a", 1)
      val key1Values = Seq(
        createRow("a", 1, 100, 110, 1),
        createRow("a", 1, 120, 130, 2),
        createRow("a", 1, 140, 150, 3))

      stateManager.updateSessions(store, key1, key1Values)

      val inputs = Seq(
        createRow("b", 1, 100, 110, 1),
        createRow("b", 1, 120, 130, 2),
        createRow("b", 1, 140, 150, 3))

      val iter = new MergingSortWithSessionWindowStateIterator(
        inputs.iterator,
        stateManager,
        store,
        keysWithoutSessionAttributes,
        sessionAttribute,
        rowAttributes)

      val actual = iter.map(_.copy()).toList
      assert(actual === inputs.toList)
    }
  }

  private def createRow(
      key1: String,
      key2: Int,
      sessionStart: Long,
      sessionEnd: Long,
      value: Long): UnsafeRow = {
    val sessionRow = new GenericInternalRow(Array[Any](sessionStart, sessionEnd))
    val row = new GenericInternalRow(
      Array[Any](UTF8String.fromString(key1), key2, sessionRow, value))
    inputValueGen.apply(row).copy()
  }

  private def createKeyRow(key1: String, key2: Int): UnsafeRow = {
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(key1), key2))
    inputKeyGen.apply(row).copy()
  }

  private def getSessionStart(row: UnsafeRow): Long = {
    row.getStruct(2, 2).getLong(0)
  }

  private def withStateManager(
      stateFormatVersion: Int)(
      f: (StreamingSessionWindowStateManager, StateStore) => Unit): Unit = {
    withTempDir { file =>
      val storeConf = new StateStoreConf()
      val stateInfo = StatefulOperatorStateInfo(file.getAbsolutePath, UUID.randomUUID, 0, 0, 5)

      val manager = StreamingSessionWindowStateManager.createStateManager(
        keysWithoutSessionAttributes,
        sessionAttribute,
        rowAttributes,
        stateFormatVersion)

      val storeProviderId = StateStoreProviderId(stateInfo, 0, StateStoreId.DEFAULT_STORE_NAME)
      val store = StateStore.get(
        storeProviderId, manager.getStateKeySchema, manager.getStateValueSchema,
        manager.getNumColsForPrefixKey, stateInfo.storeVersion, storeConf, new Configuration)

      try {
        f(manager, store)
      } finally {
        manager.abortIfNeeded(store)
      }
    }
  }
}
