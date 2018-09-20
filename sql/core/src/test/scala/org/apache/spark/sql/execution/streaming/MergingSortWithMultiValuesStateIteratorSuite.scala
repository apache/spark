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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.streaming.state.{MultiValuesStateManager, StateStore, StateStoreConf}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class MergingSortWithMultiValuesStateIteratorSuite extends SharedSQLContext {

  val rowSchema = new StructType().add("key1", StringType).add("key2", IntegerType)
    .add("session", new StructType().add("start", LongType).add("end", LongType))
    .add("aggVal1", LongType).add("aggVal2", DoubleType)
  val rowAttributes = rowSchema.toAttributes

  val keysWithoutSessionSchema = rowSchema.filter(st => List("key1", "key2").contains(st.name))
  val keysWithoutSessionAttributes = rowAttributes.filter {
    attr => List("key1", "key2").contains(attr.name)
  }

  val sessionSchema = rowSchema.filter(st => st.name == "session").head
  val sessionAttribute = rowAttributes.filter(attr => attr.name == "session").head

  val valuesSchema = rowSchema.filter(st => List("aggVal1", "aggVal2").contains(st.name))
  val valuesAttributes = rowAttributes.filter {
    attr => List("aggVal1", "aggVal2").contains(attr.name)
  }

  test("no row in input data") {
    withStateManager(rowAttributes, keysWithoutSessionAttributes) { manager =>
      val iterator = new MergingSortWithMultiValuesStateIterator(None.iterator,
        manager, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      assert(!iterator.hasNext)
    }
  }

  test("no row in input data but having state") {
    withStateManager(rowAttributes, keysWithoutSessionAttributes) { manager =>
      val srow11 = createRow("a", 1, 55, 85, 50, 2.5)
      val srow12 = createRow("a", 1, 105, 140, 30, 2.0)
      appendRowToStateManager(manager, srow11, srow12)

      val iterator = new MergingSortWithMultiValuesStateIterator(None.iterator,
        manager, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      assert(!iterator.hasNext)
    }
  }

  test("no previous state") {
    withStateManager(rowAttributes, keysWithoutSessionAttributes) { manager =>
      val row1 = createRow("a", 1, 100, 110, 10, 1.1)
      val row2 = createRow("a", 1, 100, 110, 20, 1.2)
      val row3 = createRow("a", 2, 110, 120, 10, 1.1)
      val row4 = createRow("a", 2, 115, 125, 20, 1.2)
      val rows = List(row1, row2, row3, row4)

      val iterator = new MergingSortWithMultiValuesStateIterator(rows.iterator,
        manager, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      rows.foreach { row =>
        assert(iterator.hasNext)
        assertRowsEquals(row, iterator.next())
      }

      assert(!iterator.hasNext)
    }
  }

  test("multiple keys in input data and state") {
    withStateManager(rowAttributes, keysWithoutSessionAttributes) { manager =>
      // key 1 - placing sessions in state to start and end
      val row11 = createRow("a", 1, 100, 110, 10, 1.1)
      val row12 = createRow("a", 1, 100, 110, 20, 1.2)

      val srow11 = createRow("a", 1, 55, 85, 50, 2.5)
      val srow12 = createRow("a", 1, 105, 140, 30, 2.0)
      appendRowToStateManager(manager, srow11, srow12)

      // key 2 - no state
      val row21 = createRow("a", 2, 110, 120, 10, 1.1)
      val row22 = createRow("a", 2, 115, 125, 20, 1.2)

      // key 3 - placing sessions in state to only start
      val row31 = createRow("a", 3, 130, 140, 10, 1.1)
      val row32 = createRow("a", 3, 135, 145, 20, 1.2)

      val srow31 = createRow("a", 3, 105, 140, 30, 2.0)
      val srow32 = createRow("a", 3, 120, 150, 30, 2.0)
      appendRowToStateManager(manager, srow31, srow32)

      // key 4 - placing sessions in state to only end
      val row41 = createRow("a", 4, 100, 110, 10, 1.1)
      val row42 = createRow("a", 4, 100, 115, 20, 1.2)

      val srow41 = createRow("a", 4, 105, 140, 30, 2.0)
      val srow42 = createRow("a", 4, 120, 150, 30, 2.0)
      appendRowToStateManager(manager, srow41, srow42)

      // key 5 - placing sessions in state like one row and state session and another
      val row51 = createRow("a", 5, 100, 110, 10, 1.1)
      val row52 = createRow("a", 5, 120, 130, 20, 1.2)

      val srow51 = createRow("a", 5, 90, 120, 30, 2.0)
      val srow52 = createRow("a", 5, 110, 125, 30, 2.0)
      val srow53 = createRow("a", 5, 130, 150, 30, 2.0)
      appendRowToStateManager(manager, srow51, srow52, srow53)

      val rows = List(row11, row12, row21, row22, row31, row32, row41, row42, row51, row52)

      val expectedRowSequence = List(srow11, row11, row12, srow12, row21, row22, srow31, srow32,
        row31, row32, row41, row42, srow41, srow42, srow51, row51, srow52, row52, srow53)

      val iterator = new MergingSortWithMultiValuesStateIterator(rows.iterator,
        manager, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      expectedRowSequence.foreach { row =>
        assert(iterator.hasNext)
        assertRowsEquals(row, iterator.next())
      }

      assert(!iterator.hasNext)
    }
  }

  test("no keys in input data and state") {
    val noKeyRowSchema = new StructType()
      .add("session", new StructType().add("start", LongType).add("end", LongType))
      .add("aggVal1", LongType).add("aggVal2", DoubleType)
    val noKeyRowAttributes = noKeyRowSchema.toAttributes

    val noKeySessionAttribute = noKeyRowAttributes.filter(attr => attr.name == "session").head

    def createNoKeyRow(sessionStart: Long, sessionEnd: Long,
                       aggVal1: Long, aggVal2: Double): UnsafeRow = {
      val genericRow = new GenericInternalRow(4)
      val session: Array[Any] = new Array[Any](2)
      session(0) = sessionStart
      session(1) = sessionEnd

      val sessionRow = new GenericInternalRow(session)
      genericRow.update(0, sessionRow)

      genericRow.setLong(1, aggVal1)
      genericRow.setDouble(2, aggVal2)

      val rowProjection = GenerateUnsafeProjection.generate(noKeyRowAttributes, noKeyRowAttributes)
      rowProjection(genericRow)
    }

    def assertNoKeyRowsEquals(expectedRow: InternalRow, retRow: InternalRow): Unit = {
      assert(retRow.getStruct(0, 2).getLong(0) == expectedRow.getStruct(0, 2).getLong(0))
      assert(retRow.getStruct(0, 2).getLong(1) == expectedRow.getStruct(0, 2).getLong(1))
      assert(retRow.getLong(1) === expectedRow.getLong(1))
      assert(doubleEquals(retRow.getDouble(2), expectedRow.getDouble(2)))
    }

    def appendNoKeyRowToStateManager(manager: MultiValuesStateManager, rows: UnsafeRow*): Unit = {
      rows.foreach(row => manager.append(new UnsafeRow(0), row))
    }

    withStateManager(noKeyRowAttributes, Seq.empty[Attribute]) { manager =>
      // only input data
      val row1 = createNoKeyRow(100, 110, 10, 1.1)
      val row2 = createNoKeyRow(100, 110, 20, 1.2)

      val srow1 = createNoKeyRow(55, 85, 50, 2.5)
      val srow2 = createNoKeyRow(105, 140, 30, 2.0)
      appendNoKeyRowToStateManager(manager, srow1, srow2)

      val rows = List(row1, row2)

      val expectedRowSequence = List(srow1, row1, row2, srow2)

      val iterator = new MergingSortWithMultiValuesStateIterator(rows.iterator,
        manager, Seq.empty[Attribute], noKeySessionAttribute, noKeyRowAttributes)

      expectedRowSequence.foreach { row =>
        assert(iterator.hasNext)
        assertNoKeyRowsEquals(row, iterator.next())
      }

      assert(!iterator.hasNext)
    }
  }

  private def getKeyRow(row: UnsafeRow): UnsafeRow = {
    val keyProjection = GenerateUnsafeProjection.generate(keysWithoutSessionAttributes,
      rowAttributes)
    keyProjection(row)
  }

  private def createRow(key1: String, key2: Int, sessionStart: Long, sessionEnd: Long,
                        aggVal1: Long, aggVal2: Double): UnsafeRow = {
    val genericRow = new GenericInternalRow(6)
    if (key1 != null) {
      genericRow.update(0, UTF8String.fromString(key1))
    } else {
      genericRow.setNullAt(0)
    }
    genericRow.setInt(1, key2)

    val session: Array[Any] = new Array[Any](2)
    session(0) = sessionStart
    session(1) = sessionEnd

    val sessionRow = new GenericInternalRow(session)
    genericRow.update(2, sessionRow)

    genericRow.setLong(3, aggVal1)
    genericRow.setDouble(4, aggVal2)

    val rowProjection = GenerateUnsafeProjection.generate(rowAttributes, rowAttributes)
    rowProjection(genericRow)
  }

  private def appendRowToStateManager(manager: MultiValuesStateManager, rows: UnsafeRow*): Unit = {
    rows.foreach(row => manager.append(getKeyRow(row), row))
  }

  private def doubleEquals(value1: Double, value2: Double): Boolean = {
    value1 > value2 - 0.000001 && value1 < value2 + 0.000001
  }

  private def assertRowsEquals(expectedRow: InternalRow, retRow: InternalRow): Unit = {
    assert(retRow.getString(0) === expectedRow.getString(0))
    assert(retRow.getInt(1) === expectedRow.getInt(1))
    assert(retRow.getStruct(2, 2).getLong(0) == expectedRow.getStruct(2, 2).getLong(0))
    assert(retRow.getStruct(2, 2).getLong(1) == expectedRow.getStruct(2, 2).getLong(1))
    assert(retRow.getLong(3) === expectedRow.getLong(3))
    assert(doubleEquals(retRow.getDouble(3), expectedRow.getDouble(3)))
  }

  private def withStateManager(
      inputValueAttribs: Seq[Attribute],
      keyExprs: Seq[Expression])(f: MultiValuesStateManager => Unit): Unit = {

    withTempDir { file =>
      val storeConf = new StateStoreConf()
      val stateInfo = StatefulOperatorStateInfo(file.getAbsolutePath, UUID.randomUUID, 0, 0, 5)
      val manager = new MultiValuesStateManager("session-", inputValueAttribs, keyExprs,
        Some(stateInfo), storeConf, new Configuration)
      try {
        f(manager)
      } finally {
        manager.abortIfNeeded()
      }
    }
    StateStore.stop()
  }
}
