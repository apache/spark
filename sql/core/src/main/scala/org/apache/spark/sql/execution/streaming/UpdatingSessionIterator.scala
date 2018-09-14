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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{LongType, TimestampType}

// FIXME: javadoc!!
class UpdatingSessionIterator(
    iter: Iterator[InternalRow],
    groupWithoutSessionExpressions: Seq[Expression],
    sessionExpression: Expression,
    inputSchema: Seq[Attribute]) extends Iterator[InternalRow] {

  val sessionIndex = inputSchema.indexOf(sessionExpression)

  val valuesExpressions: Seq[Attribute] = inputSchema.diff(groupWithoutSessionExpressions)
    .diff(Seq(sessionExpression))

  var currentKeys: InternalRow = _
  var currentSessionStart: Long = Long.MaxValue
  var currentSessionEnd: Long = Long.MinValue

  val currentRows: mutable.MutableList[InternalRow] = new mutable.MutableList[InternalRow]()

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
    }

    iter.hasNext
  }

  override def next(): InternalRow = {
    assertIteratorNotCorrupted()

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      System.err.println(s"DEBUG: has remaining returnRowsIter - not going into loop - " +
        s"current session - currentKeys: $currentKeys / " +
        s"currentRows: $currentRows / currentSessionStart: $currentSessionStart / " +
        s"currentSessionEnd: $currentSessionEnd")

      return returnRowsIter.next()
    }

    var exitCondition = false
    while (iter.hasNext && !exitCondition) {
      // we are going to modify the row, so we should make sure multiple objects are not
      // referencing same memory, which could be possible when optimizing iterator
      // without this, multiple rows in same key will be returned with same content
      val row = iter.next().copy()

      val keysProjection = GenerateUnsafeProjection.generate(groupWithoutSessionExpressions,
        inputSchema)
      val sessionProjection = GenerateUnsafeProjection.generate(Seq(sessionExpression), inputSchema)

      val keys = keysProjection(row)
      val session = sessionProjection(row)
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
          currentRows += row

          System.err.println(s"DEBUG: - adding row: $row / currentRows: $currentRows")

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

    System.err.println(s"DEBUG: end of loop - current session - currentKeys: $currentKeys / " +
      s"currentRows: $currentRows / currentSessionStart: $currentSessionStart / " +
      s"currentSessionEnd: $currentSessionEnd")

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
    currentRows += row

    System.err.println(s"DEBUG: started new session - currentKeys: $currentKeys / " +
      s"currentRows: $currentRows / currentSessionStart: $currentSessionStart / " +
      s"currentSessionEnd: $currentSessionEnd")
  }

  private def handleBrokenPreconditionForSort(): Unit = {
    errorOnIterator = true
    throw new IllegalStateException("The iterator must be sorted by key and session start!")
  }

  private def closeCurrentSession(keyChanged: Boolean): Unit = {
    System.err.println(s"DEBUG: closing current session - currentKeys: $currentKeys / " +
      s"currentRows: $currentRows / currentSessionStart: $currentSessionStart / " +
      s"currentSessionEnd: $currentSessionEnd")

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

    val returnRows = currentRows.map { internalRow =>
      val proj = UnsafeProjection.create(newSchemaExpressions, inputSchema)
      proj(internalRow)
    }.toList

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      returnRowsIter = returnRowsIter ++ returnRows.iterator
    } else {
      returnRowsIter = returnRows.iterator
    }

    // FIXME: DEBUG
    val (rIter, tmpReturnRowsIter) = returnRowsIter.duplicate
    returnRowsIter = rIter
    System.err.println(s"DEBUG: closing current session - return rows iter will return: ${tmpReturnRowsIter.toList}")

    if (keyChanged) processedKeys.add(currentKeys)

    currentKeys = null
    currentSessionStart = Long.MaxValue
    currentSessionEnd = Long.MinValue
    currentRows.clear()
  }

  private def assertIteratorNotCorrupted(): Unit = {
    if (errorOnIterator) {
      throw new IllegalStateException("The iterator is already corrupted.")
    }
  }

}
