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

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

class MergingSessionsIteratorSuite extends SharedSparkSession {

  val rowSchema = new StructType().add("key1", StringType).add("key2", IntegerType)
    .add("session", new StructType().add("start", LongType).add("end", LongType))
    .add("aggVal1", LongType).add("aggVal2", DoubleType)
  val rowAttributes = rowSchema.toAttributes

  val keysWithSessionSchema = rowSchema.filter { attr =>
    List("key1", "key2", "session").contains(attr.name)
  }
  val keysWithSessionAttributes = rowAttributes.filter { attr =>
    List("key1", "key2", "session").contains(attr.name)
  }

  val sessionSchema = rowSchema.filter(st => st.name == "session").head
  val sessionAttribute = rowAttributes.filter(attr => attr.name == "session").head

  val valuesSchema = rowSchema.filter(st => List("aggVal1", "aggVal2").contains(st.name))
  val valuesAttributes = rowAttributes.filter {
    attr => List("aggVal1", "aggVal2").contains(attr.name)
  }

/*

<< MergingSessionsExec >>

requiredChildDistributionExpressions = {Some@9252} "Some(ArrayBuffer(id#12))"
 value = {ArrayBuffer@9263} "ArrayBuffer" size = 1
requiredChildDistributionOption = {None$@9253} "None"
groupingExpressions = {ArrayBuffer@9254} "ArrayBuffer" size = 2
 0 = {AttributeReference@9255} "session_window#25"
 1 = {AttributeReference@9267} "id#12"
sessionExpression = {AttributeReference@9255} "session_window#25"
 name = "session_window"
 dataType = {StructType@9271} "StructType" size = 2
 nullable = false
 metadata = {Metadata@9272} "{"spark.sessionWindow":true}"
 exprId = {ExprId@9273} "ExprId(25,dd71ade5-9781-46bb-bc2d-5f45fe1a1e2d)"
 qualifier = {Nil$@9274} "Nil$" size = 0
 references = {AttributeSet@9275} "AttributeSet" size = 1
 bitmap$trans$0 = true
 deterministic = true
 _references = null
 resolved = true
 canonicalized = {AttributeReference@9276} "none#25"
 Expression.bitmap$trans$0 = false
 bitmap$0 = 7
 containsChild = {Set$EmptySet$@9277} "Set$EmptySet$" size = 0
 _hashCode = 0
 allChildren = null
 origin = {Origin@9278} "Origin(None,None)"
 tags = {HashMap@9279} "HashMap" size = 0
 TreeNode.bitmap$0 = 1
aggregateExpressions = {ArrayBuffer@9256} "ArrayBuffer" size = 2
 0 = {AggregateExpression@9355} "merge_count(1)"
  resultAttribute = null
  canonicalized = null
  references = null
  aggregateFunction = {Count@9364} "count(1)"
  mode = {PartialMerge$@9365} "PartialMerge"
  isDistinct = false
  filter = {None$@9253} "None"
  resultId = {ExprId@9366} "ExprId(20,dd71ade5-9781-46bb-bc2d-5f45fe1a1e2d)"
  bitmap$trans$0 = 0
  bitmap$0 = false
  deterministic = false
  _references = null
  resolved = false
  Expression.canonicalized = null
  Expression.bitmap$trans$0 = false
  Expression.bitmap$0 = 0
  containsChild = null
  _hashCode = 0
  allChildren = null
  origin = {Origin@9367} "Origin(None,None)"
  tags = {HashMap@9368} "HashMap" size = 0
  TreeNode.bitmap$0 = 0
 1 = {AggregateExpression@9356} "merge_sum(value#11)"
  resultAttribute = null
  canonicalized = null
  references = null
  aggregateFunction = {Sum@9371} "sum(value#11)"
  mode = {PartialMerge$@9365} "PartialMerge"
  isDistinct = false
  filter = {None$@9253} "None"
  resultId = {ExprId@9372} "ExprId(23,dd71ade5-9781-46bb-bc2d-5f45fe1a1e2d)"
  bitmap$trans$0 = 0
  bitmap$0 = false
  deterministic = false
  _references = null
  resolved = false
  Expression.canonicalized = null
  Expression.bitmap$trans$0 = false
  Expression.bitmap$0 = 0
  containsChild = null
  _hashCode = 0
  allChildren = null
  origin = {Origin@9367} "Origin(None,None)"
  tags = {HashMap@9373} "HashMap" size = 0
  TreeNode.bitmap$0 = 0
aggregateAttributes = {ArrayBuffer@9257} "ArrayBuffer" size = 2
 0 = {AttributeReference@9360} "count(1)#20L"
  name = "count(1)"
  dataType = {LongType$@9377} "LongType"
  nullable = false
  metadata = {Metadata@9437} "{}"
  exprId = {ExprId@9366} "ExprId(20,dd71ade5-9781-46bb-bc2d-5f45fe1a1e2d)"
  qualifier = {Nil$@9274} "Nil$" size = 0
  references = null
  bitmap$trans$0 = false
  deterministic = false
  _references = null
  resolved = true
  canonicalized = null
  Expression.bitmap$trans$0 = false
  bitmap$0 = 2
  containsChild = {Set$EmptySet$@9277} "Set$EmptySet$" size = 0
  _hashCode = 0
  allChildren = null
  origin = {Origin@9385} "Origin(None,None)"
  tags = {HashMap@9442} "HashMap" size = 0
  TreeNode.bitmap$0 = 1
 1 = {AttributeReference@9361} "sum(value#11)#23L"
  name = "sum(value#11)"
  dataType = {LongType$@9377} "LongType"
  nullable = true
  metadata = {Metadata@9437} "{}"
  exprId = {ExprId@9372} "ExprId(23,dd71ade5-9781-46bb-bc2d-5f45fe1a1e2d)"
  qualifier = {Nil$@9274} "Nil$" size = 0
  references = null
  bitmap$trans$0 = false
  deterministic = false
  _references = null
  resolved = true
  canonicalized = null
  Expression.bitmap$trans$0 = false
  bitmap$0 = 2
  containsChild = {Set$EmptySet$@9277} "Set$EmptySet$" size = 0
  _hashCode = 0
  allChildren = null
  origin = {Origin@9385} "Origin(None,None)"
  tags = {HashMap@9438} "HashMap" size = 0
  TreeNode.bitmap$0 = 1
initialInputBufferOffset = 2
resultExpressions = {ArrayBuffer@9258} "ArrayBuffer" size = 4
 0 = {AttributeReference@9255} "session_window#25"
 1 = {AttributeReference@9267} "id#12"
 2 = {AttributeReference@9446} "count#54L"
 3 = {AttributeReference@9447} "sum#55L"
child = {HashAggregateExec@9259} "HashAggregate(keys=[session_window#25, id#12], functions=[partial_count(1), partial_sum(value#11)], output=[session_window#25, id#12, count#54L, sum#55L])\n+- PlanLater Project [named_struct(start, precisetimestampconversion(precisetimestampconversion(cast(_1#3 as timestamp), TimestampType, LongType), LongType, TimestampType), end, precisetimestampconversion((precisetimestampconversion(cast(_1#3 as timestamp), TimestampType, LongType) + 10000000), LongType, TimestampType)) AS session_window#25, _2#4 AS value#11, _3#5 AS id#12]\n"

<< MergingSessionsIterator >>

partIndex = 1
groupingExpressions = {ArrayBuffer@13034} "ArrayBuffer" size = 2
 0 = {AttributeReference@13035} "session_window#25"
 1 = {AttributeReference@13060} "id#12"
sessionExpression = {AttributeReference@13035} "session_window#25"
 name = "session_window"
 dataType = {StructType@13064} "StructType" size = 2
 nullable = false
 metadata = {Metadata@13065} "{"spark.sessionWindow":true}"
 exprId = {ExprId@13066} "ExprId(25,a01591e6-1d42-469b-89fe-93886ef20ed2)"
 qualifier = {Nil$@13067} "Nil$" size = 0
 references = null
 bitmap$trans$0 = false
 deterministic = false
 _references = null
 resolved = true
 canonicalized = null
 Expression.bitmap$trans$0 = false
 bitmap$0 = 2
 containsChild = null
 _hashCode = 0
 allChildren = null
 origin = {Origin@13068} "Origin(None,None)"
 tags = {HashMap@13069} "HashMap" size = 0
 TreeNode.bitmap$0 = 0
valueAttributes = {ArrayBuffer@13036} "ArrayBuffer" size = 4
 0 = {AttributeReference@13035} "session_window#25"
 1 = {AttributeReference@13060} "id#12"
 2 = {AttributeReference@13120} "count#54L"
 3 = {AttributeReference@13121} "sum#55L"
inputIterator = {WholeStageCodegenExec$$anon$1@13037} "<iterator>"
 buffer = {GeneratedClass$GeneratedIteratorForCodegenStage2@13126}
 durationMs = {SQLMetric@13127} "SQLMetric(id: 220, name: Some(duration), value: -1)"
aggregateExpressions = {ArrayBuffer@13038} "ArrayBuffer" size = 2
 0 = {AggregateExpression@13130} "merge_count(1)"
 1 = {AggregateExpression@13131} "merge_sum(value#11)"
aggregateAttributes = {ArrayBuffer@13039} "ArrayBuffer" size = 2
 0 = {AttributeReference@13135} "count(1)#20L"
 1 = {AttributeReference@13136} "sum(value#11)#23L"
initialInputBufferOffset = 2
resultExpressions = {ArrayBuffer@13040} "ArrayBuffer" size = 4
 0 = {AttributeReference@13035} "session_window#25"
 1 = {AttributeReference@13060} "id#12"
 2 = {AttributeReference@13120} "count#54L"
 3 = {AttributeReference@13121} "sum#55L"
newMutableProjection = {MergingSessionsExec$lambda@13041} "org.apache.spark.sql.execution.aggregate.MergingSessionsExec$$Lambda$2770/254372732@3d4735c5"
numOutputRows = {SQLMetric@13042} "SQLMetric(id: 216, name: Some(number of output rows), value: 0)"
*/

  test("no row") {
    val iterator = new UpdatingSessionIterator(None.iterator, keysWithSessionAttributes,
      sessionAttribute, rowAttributes, inMemoryThreshold, spillThreshold)

    assert(!iterator.hasNext)
  }

  test("only one row") {
    val rows = List(createRow("a", 1, 100, 110, 10, 1.1))

    val iterator = new UpdatingSessionIterator(rows.iterator, keysWithSessionAttributes,
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

    val iterator = new UpdatingSessionIterator(rows.iterator, keysWithSessionAttributes,
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

    val iterator = new UpdatingSessionIterator(rowsAll.iterator, keysWithSessionAttributes,
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

    val iterator = new UpdatingSessionIterator(rowsAll.iterator, keysWithSessionAttributes,
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

    val row7 = createRow("a", 2, 127, 137, 30, 1.3)
    val row8 = createRow("a", 2, 135, 145, 40, 1.4)
    val rows4 = List(row7, row8)

    val rowsAll = rows1 ++ rows2 ++ rows3 ++ rows4

    val iterator = new UpdatingSessionIterator(rowsAll.iterator, keysWithSessionAttributes,
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
      // session being expanded to (127 ~ 145)
      assertRowsEqualsWithNewSession(expectedRow, retRow, 127, 145)
    }

    assert(iterator.hasNext === false)
  }

  test("throws exception if data is not sorted by session start") {
    val row1 = createRow("a", 1, 100, 110, 10, 1.1)
    val row2 = createRow("a", 1, 100, 110, 20, 1.2)
    val row3 = createRow("a", 1, 95, 105, 30, 1.3)
    val row4 = createRow("a", 1, 113, 123, 40, 1.4)
    val rows = List(row1, row2, row3, row4)

    val iterator = new UpdatingSessionIterator(rows.iterator, keysWithSessionAttributes,
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

    val iterator = new UpdatingSessionIterator(rows.iterator, keysWithSessionAttributes,
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
    val noKeyRowSchema = new StructType()
      .add("session", new StructType().add("start", LongType).add("end", LongType))
      .add("aggVal1", LongType).add("aggVal2", DoubleType)
    val noKeyRowAttributes = noKeyRowSchema.toAttributes

    val noKeySessionAttribute = noKeyRowAttributes.filter(attr => attr.name == "session").head

    def createNoKeyRow(
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

    def assertNoKeyRowsEqualsWithNewSession(
        expectedRow: InternalRow,
        retRow: InternalRow,
        newSessionStart: Long,
        newSessionEnd: Long): Unit = {
      assert(retRow.getStruct(0, 2).getLong(0) == newSessionStart)
      assert(retRow.getStruct(0, 2).getLong(1) == newSessionEnd)
      assert(retRow.getLong(1) === expectedRow.getLong(1))
      assert(doubleEquals(retRow.getDouble(2), expectedRow.getDouble(2)))
    }

    val row1 = createNoKeyRow(100, 110, 10, 1.1)
    val row2 = createNoKeyRow(100, 110, 20, 1.2)
    val row3 = createNoKeyRow(105, 115, 30, 1.3)
    val row4 = createNoKeyRow(113, 123, 40, 1.4)
    val rows = List(row1, row2, row3, row4)

    val iterator = new UpdatingSessionIterator(rows.iterator, Seq(noKeySessionAttribute),
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
}