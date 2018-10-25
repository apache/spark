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
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.streaming.state.{SessionWindowLinkedListState, StateStore, StateStoreConf}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class MergingSortWithSessionWindowLinkedListStateIteratorSuite extends SharedSQLContext {

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

  val keyProjection = GenerateUnsafeProjection.generate(keysWithoutSessionAttributes, rowAttributes)
  val sessionProjection = GenerateUnsafeProjection.generate(Seq(sessionAttribute), rowAttributes)

  test("no row in input data") {
    withSessionWindowLinkedListState(rowAttributes, keysWithoutSessionAttributes) { state =>
      val iterator = new MergingSortWithSessionWindowLinkedListStateIterator(None.iterator,
        state, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      assert(!iterator.hasNext)
    }
  }

  test("no row in input data but having state") {
    withSessionWindowLinkedListState(rowAttributes, keysWithoutSessionAttributes) { state =>
      val srow11 = createRow("a", 1, 55, 85, 50, 2.5)
      val srow12 = createRow("a", 1, 105, 140, 30, 2.0)

      setRowsInState(state, keyProjection(srow11), srow11, srow12)

      val iterator = new MergingSortWithSessionWindowLinkedListStateIterator(None.iterator,
        state, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      assert(!iterator.hasNext)
    }
  }

  test("no previous state") {
    withSessionWindowLinkedListState(rowAttributes, keysWithoutSessionAttributes) { state =>
      val row1 = createRow("a", 1, 100, 110, 10, 1.1)
      val row2 = createRow("a", 1, 100, 110, 20, 1.2)
      val row3 = createRow("a", 2, 110, 120, 10, 1.1)
      val row4 = createRow("a", 2, 115, 125, 20, 1.2)
      val rows = List(row1, row2, row3, row4)

      val iterator = new MergingSortWithSessionWindowLinkedListStateIterator(rows.iterator,
        state, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      rows.foreach { row =>
        assert(iterator.hasNext)
        assertRowsEquals(row, iterator.next())
      }

      assert(!iterator.hasNext)
    }
  }

  test("single previous state") {
    withSessionWindowLinkedListState(rowAttributes, keysWithoutSessionAttributes) { state =>
      // key1 - events are earlier than state
      val row11 = createRow("a", 1, 100, 110, 10, 1.1)
      val row12 = createRow("a", 1, 110, 120, 20, 1.2)

      // below will not be picked up since the session is not matched to new events
      val srow11 = createRow("a", 1, 200, 220, 10, 1.3)
      setRowsInState(state, keyProjection(srow11), srow11)

      // key2 - events are later than state
      // below will not be picked up since the session is not matched to new events
      val srow21 = createRow("a", 2, 50, 70, 10, 1.1)

      val row21 = createRow("a", 2, 100, 110, 10, 1.1)
      val row22 = createRow("a", 2, 110, 120, 20, 1.2)
      setRowsInState(state, keyProjection(srow21), srow21)

      // key3 - events are enclosing the state
      val row31 = createRow("a", 3, 90, 100, 10, 1.1)
      val srow31 = createRow("a", 3, 100, 110, 10, 1.1)
      val row32 = createRow("a", 3, 105, 115, 20, 1.2)
      setRowsInState(state, keyProjection(srow31), srow31)

      val rows = List(row11, row12) ++ List(row21, row22) ++ List(row31, row32)

      val expectedRows = List(row11, row12) ++ List(row21, row22) ++
        List(row31, srow31, row32)

      val iterator = new MergingSortWithSessionWindowLinkedListStateIterator(rows.iterator,
        state, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      expectedRows.foreach { row =>
        assert(iterator.hasNext, "Iterator.hasNext is false while we expected row " +
          s"${getTupleFromRow(row)}")
        assertRowsEquals(row, iterator.next())
      }

      assert(!iterator.hasNext)
    }
  }

  test("only emitting sessions in state which enclose events") {
    withSessionWindowLinkedListState(rowAttributes, keysWithoutSessionAttributes) { state =>
      // below example is group by line separated

      val row1 = createRow("a", 1, 10, 20, 1, 1.1)
      val row2 = createRow("a", 1, 20, 30, 1, 1.1)
      val row3 = createRow("a", 1, 30, 40, 1, 1.1)
      val srow1 = createRow("a", 1, 40, 60, 2, 2.2)
      val row4 = createRow("a", 1, 40, 50, 1, 1.1)

      // below will not be picked up since the session is not matched to new events
      val srow2 = createRow("a", 1, 80, 90, 2, 2.2)
      val srow3 = createRow("a", 1, 100, 110, 2, 2.2)

      val srow4 = createRow("a", 1, 120, 130, 2, 2.2)
      val row5 = createRow("a", 1, 125, 135, 1, 1.1)
      val row6 = createRow("a", 1, 140, 150, 1, 1.1)

      // below will not be picked up since the session is not matched to new events
      val srow5 = createRow("a", 1, 180, 200, 2, 2.2)
      val srow6 = createRow("a", 1, 220, 260, 2, 2.2)

      setRowsInState(state, keyProjection(srow1), srow1, srow2, srow3, srow4, srow5, srow6)

      val rows = List(row1, row2, row3, row4, row5, row6)

      val expectedRowSequence = List(row1, row2, row3, srow1, row4, srow4,
        row5, row6)

      val iterator = new MergingSortWithSessionWindowLinkedListStateIterator(rows.iterator,
        state, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      expectedRowSequence.foreach { row =>
        assert(iterator.hasNext)
        assertRowsEquals(row, iterator.next())
      }

      assert(!iterator.hasNext)
    }
  }

  test("multiple keys in input data and state") {
    withSessionWindowLinkedListState(rowAttributes, keysWithoutSessionAttributes) { state =>
      // key 1 - placing sessions in state to start and end
      val srow11 = createRow("a", 1, 85, 105, 50, 2.5)
      val row11 = createRow("a", 1, 100, 110, 10, 1.1)
      val row12 = createRow("a", 1, 100, 110, 20, 1.2)
      val srow12 = createRow("a", 1, 105, 140, 30, 2.0)

      val key1 = keyProjection(srow11)
      setRowsInState(state, key1, srow11, srow12)

      val rowsForKey1 = List(row11, row12)
      val expectedForKey1 = List(srow11, row11, row12, srow12)

      // key 2 - no state
      val row21 = createRow("a", 2, 110, 120, 10, 1.1)
      val row22 = createRow("a", 2, 115, 125, 20, 1.2)

      val rowsForKey2 = List(row21, row22)
      val expectedForKey2 = List(row21, row22)

      // key 3 - placing sessions in state to only start

      // below will not be picked up since the session is not matched to new events
      val srow31 = createRow("a", 3, 105, 115, 30, 2.0)
      val srow32 = createRow("a", 3, 120, 125, 30, 2.0)

      val row31 = createRow("a", 3, 130, 140, 10, 1.1)
      val row32 = createRow("a", 3, 135, 145, 20, 1.2)

      val key3 = keyProjection(srow31)
      setRowsInState(state, key3, srow31, srow32)

      val rowsForKey3 = List(row31, row32)
      val expectedForKey3 = List(row31, row32)

      // key 4 - placing sessions in state to only end
      val row41 = createRow("a", 4, 100, 110, 10, 1.1)
      val row42 = createRow("a", 4, 100, 115, 20, 1.2)

      // below will not be picked up since the session is not matched to new events
      val srow41 = createRow("a", 4, 120, 140, 30, 2.0)
      val srow42 = createRow("a", 4, 150, 180, 30, 2.0)

      val key4 = keyProjection(srow41)
      setRowsInState(state, key4, srow41, srow42)

      val rowsForKey4 = List(row41, row42)
      val expectedForKey4 = List(row41, row42)

      // key 5 - placing sessions in state like one row and state session and another
      val srow51 = createRow("a", 5, 90, 120, 30, 2.0)
      val row51 = createRow("a", 5, 100, 110, 10, 1.1)
      val srow52 = createRow("a", 5, 130, 155, 30, 2.0)
      val row52 = createRow("a", 5, 140, 160, 20, 1.2)
      val srow53 = createRow("a", 5, 160, 190, 30, 2.0)

      val key5 = keyProjection(srow51)
      setRowsInState(state, key5, srow51, srow52, srow53)

      val rowsForKey5 = List(row51, row52)
      val expectedForKey5 = List(srow51, row51, srow52, row52, srow53)

      val rows = rowsForKey1 ++ rowsForKey2 ++ rowsForKey3 ++ rowsForKey4 ++ rowsForKey5

      val expectedRowSequence = expectedForKey1 ++ expectedForKey2 ++ expectedForKey3 ++
        expectedForKey4 ++ expectedForKey5

      val iterator = new MergingSortWithSessionWindowLinkedListStateIterator(rows.iterator,
        state, keysWithoutSessionAttributes, sessionAttribute, rowAttributes)

      expectedRowSequence.foreach { row =>
        assert(iterator.hasNext, s"Iterator closed while we expect ${getTupleFromRow(row)}")
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

    def setNoKeyRowsInState(state: SessionWindowLinkedListState, rows: UnsafeRow*)
      : Unit = {
      def getSessionStart(row: UnsafeRow): Long = {
        row.getStruct(0, 2).getLong(0)
      }

      val key = new UnsafeRow(0)
      val iter = rows.sortBy(getSessionStart).iterator

      var prevSessionStart: Option[Long] = None
      while (iter.hasNext) {
        val row = iter.next()
        val sessionStart = getSessionStart(row)
        if (prevSessionStart.isDefined) {
          state.addAfter(key, sessionStart, row, prevSessionStart.get)
        } else {
          state.setHead(key, sessionStart, row)
        }

        prevSessionStart = Some(sessionStart)
      }
    }

    withSessionWindowLinkedListState(noKeyRowAttributes, Seq.empty[Attribute]) { state =>
      // this will not be picked up because the session in state is not enclosing events
      val srow1 = createNoKeyRow(10, 16, 10, 21)
      val srow2 = createNoKeyRow(17, 27, 2, 39)

      val srow3 = createNoKeyRow(35, 40, 1, 35)
      val row1 = createNoKeyRow(40, 45, 10, 45)
      setNoKeyRowsInState(state, srow1, srow2, srow3)

      val rows = List(row1)

      val expectedRowSequence = List(srow3, row1)

      val iterator = new MergingSortWithSessionWindowLinkedListStateIterator(rows.iterator,
        state, Seq.empty[Attribute], noKeySessionAttribute, noKeyRowAttributes)

      expectedRowSequence.foreach { row =>
        assert(iterator.hasNext)
        assertNoKeyRowsEquals(row, iterator.next())
      }

      assert(!iterator.hasNext)
    }
  }

  private def setRowsInState(state: SessionWindowLinkedListState, key: UnsafeRow,
                             rows: UnsafeRow*): Unit = {
    def getSessionStart(row: UnsafeRow): Long = {
      row.getStruct(2, 2).getLong(0)
    }

    val iter = rows.sortBy(getSessionStart).iterator

    var prevSessionStart: Option[Long] = None
    while (iter.hasNext) {
      val row = iter.next()
      val sessionStart = getSessionStart(row)
      if (prevSessionStart.isDefined) {
        state.addAfter(key, sessionStart, row, prevSessionStart.get)
      } else {
        state.setHead(key, sessionStart, row)
      }

      prevSessionStart = Some(sessionStart)
    }
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

  private def doubleEquals(value1: Double, value2: Double): Boolean = {
    value1 > value2 - 0.000001 && value1 < value2 + 0.000001
  }

  private def getTupleFromRow(row: InternalRow): (String, Int, Long, Long, Long, Double) = {
    (row.getString(0), row.getInt(1), row.getStruct(2, 2).getLong(0),
      row.getStruct(2, 2).getLong(1), row.getLong(3), row.getDouble(4))
  }

  private def assertRowsEquals(expectedRow: InternalRow, retRow: InternalRow): Unit = {
    
    val tupleFromExpectedRow = getTupleFromRow(expectedRow)
    val tupleFromInternalRow = getTupleFromRow(retRow)
    try {
      assert(tupleFromExpectedRow._1 === tupleFromInternalRow._1)
      assert(tupleFromExpectedRow._2 === tupleFromInternalRow._2)
      assert(tupleFromExpectedRow._3 === tupleFromInternalRow._3)
      assert(tupleFromExpectedRow._4 === tupleFromInternalRow._4)
      assert(tupleFromExpectedRow._5 === tupleFromInternalRow._5)
      assert(doubleEquals(tupleFromExpectedRow._6, tupleFromInternalRow._6))
    } catch {
      case e: TestFailedException =>
        throw new TestFailedException(s"$tupleFromExpectedRow did not equal $tupleFromInternalRow",
          e, e.failedCodeStackDepth)
    }
  }

  private def withSessionWindowLinkedListState(
      inputValueAttribs: Seq[Attribute],
      keyAttribs: Seq[Attribute])(f: SessionWindowLinkedListState => Unit): Unit = {

    withTempDir { file =>
      val storeConf = new StateStoreConf()
      val stateInfo = StatefulOperatorStateInfo(file.getAbsolutePath, UUID.randomUUID, 0, 0, 5)

      val state = new SessionWindowLinkedListState(s"session-${stateInfo.operatorId}-",
        inputValueAttribs, keyAttribs, Some(stateInfo), storeConf, new Configuration)
      try {
        f(state)
      } finally {
        state.abortIfNeeded()
      }
    }
    StateStore.stop()
  }
}
