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

package org.apache.spark.sql.execution.aggregate

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.types.{LongType, TimestampType}

// FIXME: javadoc!!
class UpdatingSessionIterator(
    iter: Iterator[InternalRow],
    groupWithoutSessionExpressions: Seq[Expression],
    sessionExpression: Expression,
    inputSchema: Seq[Attribute],
    inMemoryThreshold: Int,
    spillThreshold: Int) extends Iterator[InternalRow] {

  val sessionIndex = inputSchema.indexOf(sessionExpression)

  val valuesExpressions: Seq[Attribute] = inputSchema.diff(groupWithoutSessionExpressions)
    .diff(Seq(sessionExpression))

  val keysProjection = GenerateUnsafeProjection.generate(groupWithoutSessionExpressions,
    inputSchema)
  val sessionProjection = GenerateUnsafeProjection.generate(Seq(sessionExpression), inputSchema)

  var currentKeys: InternalRow = _
  var currentSessionStart: Long = Long.MaxValue
  var currentSessionEnd: Long = Long.MinValue

  var currentRows: ExternalAppendOnlyUnsafeRowArray = new ExternalAppendOnlyUnsafeRowArray(
    inMemoryThreshold, spillThreshold)

  var returnRows: ExternalAppendOnlyUnsafeRowArray = _
  var returnRowsIter: Iterator[InternalRow] = _
  var errorOnIterator: Boolean = false

  val processedKeys: mutable.HashSet[InternalRow] = new mutable.HashSet[InternalRow]()

  override def hasNext: Boolean = {
    assertIteratorNotCorrupted()

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      return true
    }

    if (returnRowsIter != null) {
      returnRowsIter = null
      returnRows.clear()
    }

    iter.hasNext
  }

  override def next(): InternalRow = {
    assertIteratorNotCorrupted()

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      return returnRowsIter.next()
    }

    var exitCondition = false
    while (iter.hasNext && !exitCondition) {
      // we are going to modify the row, so we should make sure multiple objects are not
      // referencing same memory, which could be possible when optimizing iterator
      // without this, multiple rows in same key will be returned with same content
      val row = iter.next().copy()

      val keys = keysProjection(row).copy()
      val session = sessionProjection(row).copy()
      val sessionRow = session.getStruct(0, 2)
      val sessionStart = sessionRow.getLong(0)
      val sessionEnd = sessionRow.getLong(1)

      if (currentKeys == null) {
        startNewSession(row, keys, sessionStart, sessionEnd)
      } else if (keys != currentKeys) {
        closeCurrentSession(keyChanged = true)
        processedKeys.add(currentKeys)
        startNewSession(row, keys, sessionStart, sessionEnd)
        exitCondition = true
      } else {
        if (sessionStart < currentSessionStart) {
          handleBrokenPreconditionForSort()
        } else if (sessionStart <= currentSessionEnd) {
          // expanding session length if needed
          expandEndOfCurrentSession(sessionEnd)
          currentRows.add(row.asInstanceOf[UnsafeRow])
        } else {
          closeCurrentSession(keyChanged = false)
          startNewSession(row, keys, sessionStart, sessionEnd)
          exitCondition = true
        }
      }
    }

    if (!iter.hasNext) {
      // no further row: closing session
      closeCurrentSession(keyChanged = false)
    }

    // here returnRowsIter should be able to provide at least one row
    require(returnRowsIter != null && returnRowsIter.hasNext)

    returnRowsIter.next()
  }

  private def expandEndOfCurrentSession(sessionEnd: Long): Unit = {
    if (sessionEnd > currentSessionEnd) {
      currentSessionEnd = sessionEnd
    }
  }

  private def startNewSession(row: InternalRow, keys: UnsafeRow, sessionStart: Long,
                              sessionEnd: Long): Unit = {
    if (processedKeys.contains(keys)) {
      handleBrokenPreconditionForSort()
    }

    currentKeys = keys
    currentSessionStart = sessionStart
    currentSessionEnd = sessionEnd

    currentRows.clear()
    currentRows.add(row.asInstanceOf[UnsafeRow])
  }

  private def handleBrokenPreconditionForSort(): Unit = {
    errorOnIterator = true
    throw new IllegalStateException("The iterator must be sorted by key and session start!")
  }

  private def closeCurrentSession(keyChanged: Boolean): Unit = {
    // FIXME: Convert to JoinRow if possible to reduce codegen for unsafe projection
    // FIXME: Same approach on MergingSessionsIterator.generateGroupingKey doesn't work here, why?
    val sessionStruct = CreateNamedStruct(
      Literal("start") ::
        PreciseTimestampConversion(
          Literal(currentSessionStart, LongType), LongType, TimestampType) ::
        Literal("end") ::
        PreciseTimestampConversion(
          Literal(currentSessionEnd, LongType), LongType, TimestampType) ::
        Nil)

    val convertedAllExpressions = inputSchema.map { x =>
      BindReferences.bindReference[Expression](x, inputSchema)
    }

    val newSchemaExpressions = convertedAllExpressions.indices.map { idx =>
      if (idx == sessionIndex) {
        sessionStruct
      } else {
        convertedAllExpressions(idx)
      }
    }

    returnRows = currentRows
    currentRows = new ExternalAppendOnlyUnsafeRowArray(
      inMemoryThreshold, spillThreshold)

    val currentRowsIter = returnRows.generateIterator().map { internalRow =>
      // FIXME: is there any way to change this?
      val proj = UnsafeProjection.create(newSchemaExpressions, inputSchema)
      proj(internalRow)
    }

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      returnRowsIter = returnRowsIter ++ currentRowsIter
    } else {
      returnRowsIter = currentRowsIter
    }

    if (keyChanged) processedKeys.add(currentKeys)

    currentKeys = null
    currentSessionStart = Long.MaxValue
    currentSessionEnd = Long.MinValue
  }

  private def assertIteratorNotCorrupted(): Unit = {
    if (errorOnIterator) {
      throw new IllegalStateException("The iterator is already corrupted.")
    }
  }

}
