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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal, MutableProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.aggregate.MergingSessionsIterator
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MergingSessionsIteratorSuite extends SharedSparkSession {

  private val rowSchema = new StructType().add("key1", StringType).add("key2", IntegerType)
    .add("session", new StructType().add("start", LongType).add("end", LongType))
    .add("count", LongType)
  private val rowAttributes = toAttributes(rowSchema)

  private val keysWithSessionAttributes = rowAttributes.filter { attr =>
    List("key1", "key2", "session").contains(attr.name)
  }

  private val noKeyRowAttributes = rowAttributes.filterNot { attr =>
    Seq("key1", "key2").contains(attr.name)
  }

  private val sessionAttribute = rowAttributes.filter(attr => attr.name == "session").head

  test("no row") {
    val iterator = createTestIterator(None.iterator)
    assert(!iterator.hasNext)
  }

  test("only one row") {
    val rows = List(createRow("a", 1, 100, 110))

    val iterator = createTestIterator(rows.iterator)
    assert(iterator.hasNext)

    val expectedRow = createRow("a", 1, 100, 110, 1)

    val retRow = iterator.next()
    assertRowsEquals(retRow, expectedRow)

    assert(!iterator.hasNext)
  }

  test("one session per key, one key") {
    val row1 = createRow("a", 1, 100, 110)
    val row2 = createRow("a", 1, 100, 110)
    val row3 = createRow("a", 1, 105, 115)
    val row4 = createRow("a", 1, 113, 123)
    val rows = List(row1, row2, row3, row4)

    val iterator = createTestIterator(rows.iterator)
    assert(iterator.hasNext)

    val retRow = iterator.next()
    val expectedRow = createRow("a", 1, 100, 123, 4)
    assertRowsEquals(expectedRow, retRow)

    assert(!iterator.hasNext)
  }

  test("one session per key, multi keys") {
    val rows = Seq(
      // session 1
      createRow("a", 1, 100, 110),
      createRow("a", 1, 100, 110),
      createRow("a", 1, 105, 115),
      createRow("a", 1, 113, 123),

      // session 2
      createRow("a", 2, 110, 120),
      createRow("a", 2, 115, 125),
      createRow("a", 2, 117, 127),
      createRow("a", 2, 125, 135)
    )

    val iterator = createTestIterator(rows.iterator)
    assert(iterator.hasNext)

    val expectedRow1 = createRow("a", 1, 100, 123, 4)
    assertRowsEquals(expectedRow1, iterator.next())

    assert(iterator.hasNext)

    val expectedRow2 = createRow("a", 2, 110, 135, 4)
    assertRowsEquals(expectedRow2, iterator.next())

    assert(!iterator.hasNext)
  }

  test("multiple sessions per key, single key") {
    val rows = Seq(
      // session 1
      createRow("a", 1, 100, 110),
      createRow("a", 1, 105, 115),

      // session 2
      createRow("a", 1, 125, 135),
      createRow("a", 1, 127, 137)
    )

    val iterator = createTestIterator(rows.iterator)
    assert(iterator.hasNext)

    val expectedRow1 = createRow("a", 1, 100, 115, 2)
    assertRowsEquals(expectedRow1, iterator.next())

    assert(iterator.hasNext)

    val expectedRow2 = createRow("a", 1, 125, 137, 2)
    assertRowsEquals(expectedRow2, iterator.next())

    assert(!iterator.hasNext)
  }

  test("multiple sessions per key, multi keys") {
    val rows = Seq(
      // session 1
      createRow("a", 1, 100, 110),
      createRow("a", 1, 100, 110),

      // session 2
      createRow("a", 1, 115, 125),
      createRow("a", 1, 119, 129),

      // session 3
      createRow("a", 2, 110, 120),
      createRow("a", 2, 115, 125),

      // session 4
      createRow("a", 2, 127, 137),
      createRow("a", 2, 135, 145)
    )

    val iterator = createTestIterator(rows.iterator)
    assert(iterator.hasNext)

    val expectedRow1 = createRow("a", 1, 100, 110, 2)
    assertRowsEquals(expectedRow1, iterator.next())

    assert(iterator.hasNext)

    val expectedRow2 = createRow("a", 1, 115, 129, 2)
    assertRowsEquals(expectedRow2, iterator.next())

    assert(iterator.hasNext)

    val expectedRow3 = createRow("a", 2, 110, 125, 2)
    assertRowsEquals(expectedRow3, iterator.next())

    assert(iterator.hasNext)

    val expectedRow4 = createRow("a", 2, 127, 145, 2)
    assertRowsEquals(expectedRow4, iterator.next())

    assert(!iterator.hasNext)
  }

  test("throws exception if data is not sorted by session start") {
    val rows = Seq(
      createRow("a", 1, 100, 110),
      createRow("a", 1, 100, 110),
      createRow("a", 1, 95, 105),
      createRow("a", 1, 113, 123)
    )

    val iterator = createTestIterator(rows.iterator)

    // MergingSessionsIterator can't detect error on hasNext
    assert(iterator.hasNext)

    // when calling next() it can detect error and throws IllegalStateException
    checkError(
      exception = intercept[SparkException] {
        iterator.next()
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Input iterator is not sorted based on session!"))

    // afterwards, calling either hasNext() or next() will throw IllegalStateException
    checkError(
      exception = intercept[SparkException] {
        iterator.hasNext
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "The iterator is already corrupted."))

    checkError(
      exception = intercept[SparkException] {
        iterator.next()
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "The iterator is already corrupted."))
  }

  test("no key") {
    val rows = Seq(
      createNoKeyRow(100, 110),
      createNoKeyRow(100, 110),
      createNoKeyRow(105, 115),
      createNoKeyRow(113, 123)
    )

    val iterator = createNoKeyTestIterator(rows.iterator)
    assert(iterator.hasNext)

    val expectedRow = createNoKeyRow(100, 123, 4)
    assertNoKeyRowsEquals(expectedRow, iterator.next())

    assert(!iterator.hasNext)
  }

  private def createTestIterator(iterator: Iterator[InternalRow]): MergingSessionsIterator = {
    createTestIterator(iterator, isNoKey = false)
  }

  private def createNoKeyTestIterator(iterator: Iterator[InternalRow]): MergingSessionsIterator = {
    createTestIterator(iterator, isNoKey = true)
  }

  private def createTestIterator(
      iterator: Iterator[InternalRow],
      isNoKey: Boolean): MergingSessionsIterator = {
    val countFunc = Count(Literal.create(1L, LongType))
    val countAggExpr = countFunc.toAggregateExpression()
    val countRetAttr = countAggExpr.resultAttribute

    val aggregateExpressions = Seq(countAggExpr)
    val aggregateAttributes = Seq(countRetAttr)

    val initialInputBufferOffset = 1

    val groupingExpressions = if (isNoKey) {
      Seq(sessionAttribute)
    } else {
      keysWithSessionAttributes
    }
    val resultExpressions = groupingExpressions ++ aggregateAttributes

    new MergingSessionsIterator(
      partIndex = 0,
      groupingExpressions = groupingExpressions,
      sessionExpression = sessionAttribute,
      valueAttributes = resultExpressions,
      inputIterator = iterator,
      aggregateExpressions = aggregateExpressions,
      aggregateAttributes = aggregateAttributes,
      initialInputBufferOffset = initialInputBufferOffset,
      resultExpressions = resultExpressions,
      newMutableProjection = (expressions, inputSchema) =>
        MutableProjection.create(expressions, inputSchema),
      numOutputRows = SQLMetrics.createMetric(sparkContext, "output rows")
    )
  }

  private def createRow(
      key1: String,
      key2: Int,
      sessionStart: Long,
      sessionEnd: Long,
      countValue: Long = 0): UnsafeRow = {
    val genericRow = new GenericInternalRow(4)
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

    genericRow.setLong(3, countValue)

    val rowProjection = GenerateUnsafeProjection.generate(rowAttributes, rowAttributes)
    rowProjection(genericRow)
  }

  private def assertRowsEquals(expectedRow: InternalRow, retRow: InternalRow): Unit = {
    assert(retRow.getString(0) === expectedRow.getString(0))
    assert(retRow.getInt(1) === expectedRow.getInt(1))
    assert(retRow.getStruct(2, 2).getLong(0) == expectedRow.getStruct(2, 2).getLong(0))
    assert(retRow.getStruct(2, 2).getLong(1) == expectedRow.getStruct(2, 2).getLong(1))
    assert(retRow.getLong(3) === expectedRow.getLong(3))
  }

  private def createNoKeyRow(
      sessionStart: Long,
      sessionEnd: Long,
      countValue: Long = 0): UnsafeRow = {
    val genericRow = new GenericInternalRow(2)
    val session: Array[Any] = new Array[Any](2)
    session(0) = sessionStart
    session(1) = sessionEnd

    val sessionRow = new GenericInternalRow(session)
    genericRow.update(0, sessionRow)

    genericRow.setLong(1, countValue)

    val rowProjection = GenerateUnsafeProjection.generate(noKeyRowAttributes, noKeyRowAttributes)
    rowProjection(genericRow)
  }

  private def assertNoKeyRowsEquals(expectedRow: InternalRow, retRow: InternalRow): Unit = {
    assert(retRow.getStruct(0, 2).getLong(0) == expectedRow.getStruct(0, 2).getLong(0))
    assert(retRow.getStruct(0, 2).getLong(1) == expectedRow.getStruct(0, 2).getLong(1))
    assert(retRow.getLong(1) === expectedRow.getLong(1))
  }
}
