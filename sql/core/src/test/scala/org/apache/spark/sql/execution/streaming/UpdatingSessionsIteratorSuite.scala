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

import java.util.Properties

import org.apache.spark._
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.aggregate.UpdatingSessionsIterator
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class UpdatingSessionsIteratorSuite extends SharedSparkSession {

  private val rowSchema = new StructType().add("key1", StringType).add("key2", IntegerType)
    .add("session", new StructType().add("start", LongType).add("end", LongType))
    .add("aggVal1", LongType).add("aggVal2", DoubleType)
  private val rowAttributes = rowSchema.toAttributes

  private val noKeyRowAttributes = rowAttributes.filterNot { attr =>
    Seq("key1", "key2").contains(attr.name)
  }

  private val keysWithSessionAttributes = rowAttributes.filter { attr =>
    List("key1", "key2", "session").contains(attr.name)
  }

  private val sessionAttribute = rowAttributes.filter(attr => attr.name == "session").head

  private val noKeySessionAttribute = noKeyRowAttributes.filter(attr => attr.name == "session").head

  override def beforeAll(): Unit = {
    super.beforeAll()
    val taskManager = new TaskMemoryManager(new TestMemoryManager(sqlContext.sparkContext.conf), 0)
    TaskContext.setTaskContext(
      new TaskContextImpl(0, 0, 0, 0, 0, 1, taskManager, new Properties, null))
  }

  override def afterAll(): Unit = try {
    TaskContext.unset()
  } finally {
    super.afterAll()
  }

  // just copying default values to avoid bothering with SQLContext
  val inMemoryThreshold = 4096
  val spillThreshold = Int.MaxValue

  test("no row") {
    val iterator = new UpdatingSessionsIterator(None.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    assert(!iterator.hasNext)
  }

  test("only one row") {
    val rows = List(createRow("a", 1, 100, 110, 10, 1.1))

    val iterator = new UpdatingSessionsIterator(rows.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    assert(iterator.hasNext)

    val retRow = iterator.next()
    assertRowsEquals(retRow, rows.head)

    assert(!iterator.hasNext)
  }

  test("one session per key, one key") {
    val row1 = createRow("a", 1, 100, 110, 10, 1.1)
    val row2 = createRow("a", 1, 100, 110, 20, 1.2)
    val row3 = createRow("a", 1, 105, 115, 30, 1.3)
    val row4 = createRow("a", 1, 113, 123, 40, 1.4)
    val rows = List(row1, row2, row3, row4)

    val iterator = new UpdatingSessionsIterator(rows.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    val retRows = rows.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    retRows.zip(rows).foreach { case (retRow, expectedRow) =>
      // session being expanded to (100 ~ 123)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 100, 123)
    }

    assert(iterator.hasNext === false)
  }

  test("one session per key, multi keys") {
    val row1 = createRow("a", 1, 100, 110, 10, 1.1)
    val row2 = createRow("a", 1, 100, 110, 20, 1.2)
    val row3 = createRow("a", 1, 105, 115, 30, 1.3)
    val row4 = createRow("a", 1, 113, 123, 40, 1.4)
    val rows1 = List(row1, row2, row3, row4)

    val row5 = createRow("a", 2, 110, 120, 10, 1.1)
    val row6 = createRow("a", 2, 115, 125, 20, 1.2)
    val row7 = createRow("a", 2, 117, 127, 30, 1.3)
    val row8 = createRow("a", 2, 125, 135, 40, 1.4)
    val rows2 = List(row5, row6, row7, row8)

    val rowsAll = rows1 ++ rows2

    val iterator = new UpdatingSessionsIterator(rowsAll.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    val retRows1 = rows1.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }
    val retRows2 = rows2.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    retRows1.zip(rows1).foreach { case (retRow, expectedRow) =>
      // session being expanded to (100 ~ 123)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 100, 123)
    }

    retRows2.zip(rows2).foreach { case (retRow, expectedRow) =>
      // session being expanded to (110 ~ 135)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 110, 135)
    }

    assert(iterator.hasNext === false)
  }

  test("multiple sessions per key, single key") {
    val row1 = createRow("a", 1, 100, 110, 10, 1.1)
    val row2 = createRow("a", 1, 105, 115, 20, 1.2)
    val rows1 = List(row1, row2)

    val row3 = createRow("a", 1, 125, 135, 30, 1.3)
    val row4 = createRow("a", 1, 127, 137, 40, 1.4)
    val rows2 = List(row3, row4)

    val rowsAll = rows1 ++ rows2

    val iterator = new UpdatingSessionsIterator(rowsAll.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    val retRows1 = rows1.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    val retRows2 = rows2.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    retRows1.zip(rows1).foreach { case (retRow, expectedRow) =>
      // session being expanded to (100 ~ 115)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 100, 115)
    }

    retRows2.zip(rows2).foreach { case (retRow, expectedRow) =>
      // session being expanded to (125 ~ 137)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 125, 137)
    }

    assert(iterator.hasNext === false)
  }

  test("multiple sessions per key, multi keys") {
    val row1 = createRow("a", 1, 100, 110, 10, 1.1)
    val row2 = createRow("a", 1, 100, 110, 20, 1.2)
    val rows1 = List(row1, row2)

    val row3 = createRow("a", 1, 115, 125, 30, 1.3)
    val row4 = createRow("a", 1, 119, 129, 40, 1.4)
    val rows2 = List(row3, row4)

    val row5 = createRow("a", 2, 110, 120, 10, 1.1)
    val row6 = createRow("a", 2, 115, 125, 20, 1.2)
    val rows3 = List(row5, row6)

    // This is to test the edge case that the last input row creates a new session.
    val row7 = createRow("a", 2, 127, 137, 30, 1.3)
    val rows4 = List(row7)

    val rowsAll = rows1 ++ rows2 ++ rows3 ++ rows4

    val iterator = new UpdatingSessionsIterator(rowsAll.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    val retRows1 = rows1.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    val retRows2 = rows2.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    val retRows3 = rows3.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    val retRows4 = rows4.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    retRows1.zip(rows1).foreach { case (retRow, expectedRow) =>
      // session being expanded to (100 ~ 110)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 100, 110)
    }

    retRows2.zip(rows2).foreach { case (retRow, expectedRow) =>
      // session being expanded to (115 ~ 129)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 115, 129)
    }

    retRows3.zip(rows3).foreach { case (retRow, expectedRow) =>
      // session being expanded to (110 ~ 125)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 110, 125)
    }

    retRows4.zip(rows4).foreach { case (retRow, expectedRow) =>
      // session being expanded to (127 ~ 137)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 127, 137)
    }

    assert(iterator.hasNext === false)
  }

  test("throws exception if data is not sorted by session start") {
    val row1 = createRow("a", 1, 100, 110, 10, 1.1)
    val row2 = createRow("a", 1, 100, 110, 20, 1.2)
    val row3 = createRow("a", 1, 95, 105, 30, 1.3)
    val row4 = createRow("a", 1, 113, 123, 40, 1.4)
    val rows = List(row1, row2, row3, row4)

    val iterator = new UpdatingSessionsIterator(rows.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    // UpdatingSessionIterator can't detect error on hasNext
    assert(iterator.hasNext)

    // when calling next() it can detect error and throws IllegalStateException
    intercept[IllegalStateException] {
      iterator.next()
    }

    // afterwards, calling either hasNext() or next() will throw IllegalStateException
    intercept[IllegalStateException] {
      iterator.hasNext
    }

    intercept[IllegalStateException] {
      iterator.next()
    }
  }

  test("throws exception if data is not sorted by key") {
    val row1 = createRow("a", 1, 100, 110, 10, 1.1)
    val row2 = createRow("a", 2, 100, 110, 20, 1.2)
    val row3 = createRow("a", 1, 113, 123, 40, 1.4)
    val rows = List(row1, row2, row3)

    val iterator = new UpdatingSessionsIterator(rows.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    // UpdatingSessionIterator can't detect error on hasNext
    assert(iterator.hasNext)

    assertRowsEquals(row1, iterator.next())

    assert(iterator.hasNext)

    // second row itself is OK but while finding end of session it reads third row, and finds
    // its key is already finished processing, hence precondition for sorting is broken, and
    // it throws IllegalStateException
    intercept[IllegalStateException] {
      iterator.next()
    }

    // afterwards, calling either hasNext() or next() will throw IllegalStateException
    intercept[IllegalStateException] {
      iterator.hasNext
    }

    intercept[IllegalStateException] {
      iterator.next()
    }
  }

  test("no key") {
    val row1 = createNoKeyRow(100, 110, 10, 1.1)
    val row2 = createNoKeyRow(100, 110, 20, 1.2)
    val row3 = createNoKeyRow(105, 115, 30, 1.3)
    val row4 = createNoKeyRow(113, 123, 40, 1.4)
    val rows = List(row1, row2, row3, row4)

    val iterator = new UpdatingSessionsIterator(rows.iterator, Seq(noKeySessionAttribute),
      noKeySessionAttribute, noKeyRowAttributes, inMemoryThreshold, spillThreshold)

    val retRows = rows.indices.map { _ =>
      assert(iterator.hasNext)
      iterator.next()
    }

    retRows.zip(rows).foreach { case (retRow, expectedRow) =>
      // session being expanded to (100 ~ 123)
      assertNoKeyRowsEqualsWithNewSession(expectedRow, retRow, 100, 123)
    }

    assert(iterator.hasNext === false)
  }

  private def createRow(
      key1: String,
      key2: Int,
      sessionStart: Long,
      sessionEnd: Long,
      aggVal1: Long,
      aggVal2: Double): UnsafeRow = {
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

  private def assertRowsEqualsWithNewSession(
      expectedRow: InternalRow,
      retRow: InternalRow,
      newSessionStart: Long,
      newSessionEnd: Long): Unit = {
    assert(retRow.getString(0) === expectedRow.getString(0))
    assert(retRow.getInt(1) === expectedRow.getInt(1))
    assert(retRow.getStruct(2, 2).getLong(0) == newSessionStart)
    assert(retRow.getStruct(2, 2).getLong(1) == newSessionEnd)
    assert(retRow.getLong(3) === expectedRow.getLong(3))
    assert(doubleEquals(retRow.getDouble(3), expectedRow.getDouble(3)))
  }

  private def createNoKeyRow(
      sessionStart: Long,
      sessionEnd: Long,
      aggVal1: Long,
      aggVal2: Double): UnsafeRow = {
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

  private def assertNoKeyRowsEqualsWithNewSession(
      expectedRow: InternalRow,
      retRow: InternalRow,
      newSessionStart: Long,
      newSessionEnd: Long): Unit = {
    assert(retRow.getStruct(0, 2).getLong(0) == newSessionStart)
    assert(retRow.getStruct(0, 2).getLong(1) == newSessionEnd)
    assert(retRow.getLong(1) === expectedRow.getLong(1))
    assert(doubleEquals(retRow.getDouble(2), expectedRow.getDouble(2)))
  }
}
