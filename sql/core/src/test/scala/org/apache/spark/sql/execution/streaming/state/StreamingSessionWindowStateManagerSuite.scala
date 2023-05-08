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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class StreamingSessionWindowStateManagerSuite extends StreamTest with BeforeAndAfter {
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
        s"provider ${providerOpt._2} state version v${version} - extract field operations") {
        testExtractFieldOperations(version)
      }

      test(s"StreamingSessionWindowStateManager " +
        s"provider ${providerOpt._2} state version v${version} - CRUD operations") {
        testAllOperations(version)
      }
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

  private def testExtractFieldOperations(stateFormatVersion: Int): Unit = {
    withStateManager(stateFormatVersion) { case (stateManager, _) =>
      val testRow = createRow("a", 1, 100, 150, 1)
      val expectedKeyRow = createKeyRow("a", 1)

      val keyWithoutSessionRow = stateManager.extractKeyWithoutSession(testRow)
      assert(expectedKeyRow === keyWithoutSessionRow)
    }
  }

  private def testAllOperations(stateFormatVersion: Int): Unit = {
    withStateManager(stateFormatVersion) { case (stateManager, store) =>
      def updateAndVerify(keyRow: UnsafeRow, rows: Seq[UnsafeRow]): Unit = {
        stateManager.updateSessions(store, keyRow, rows)

        val expectedValues = stateManager.getSessions(store, keyRow).map(_.copy()).toList
        assert(expectedValues.toSet === rows.toSet)

        rows.foreach { row =>
          assert(!stateManager.newOrModified(store, row))
        }
      }

      val key1Row = createKeyRow("a", 1)
      val key1Values = Seq(
        createRow("a", 1, 100, 110, 1),
        createRow("a", 1, 120, 130, 2),
        createRow("a", 1, 140, 150, 3))

      updateAndVerify(key1Row, key1Values)

      val key2Row = createKeyRow("a", 2)
      val key2Values = Seq(
        createRow("a", 2, 70, 80, 1),
        createRow("a", 2, 100, 110, 2))
      updateAndVerify(key2Row, key2Values)

      val key2NewValues = Seq(
        createRow("a", 2, 70, 80, 2),
        createRow("a", 2, 80, 90, 3),
        createRow("a", 2, 90, 120, 4),
        createRow("a", 2, 140, 150, 5))
      updateAndVerify(key2Row, key2NewValues)

      val key3Row = createKeyRow("a", 3)
      val key3Values = Seq(
        createRow("a", 3, 100, 110, 1),
        createRow("a", 3, 120, 130, 2))
      updateAndVerify(key3Row, key3Values)

      val valuesOnComparison = Seq(
        // new
        (createRow("a", 3, 10, 20, 1), true),
        // modified
        (createRow("a", 3, 100, 110, 3), true),
        // exist and not modified
        (createRow("a", 3, 120, 130, 2), false))

      valuesOnComparison.foreach { case (row, expected) =>
        assert(expected === stateManager.newOrModified(store, row))
      }

      val existingRows = stateManager.iterator(store).map(_.copy()).toSet

      val removedRows = stateManager.removeByValueCondition(store,
        _.getLong(3) <= 1).map(_.copy()).toSet
      val expectedRemovedRows = Set(key1Values(0), key3Values(0))
      assert(removedRows == expectedRemovedRows)

      val afterRemovingRows = stateManager.iterator(store).map(_.copy()).toSet
      assert(existingRows.diff(afterRemovingRows) === expectedRemovedRows)
    }
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
