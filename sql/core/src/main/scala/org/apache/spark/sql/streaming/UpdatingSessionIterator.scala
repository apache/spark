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

package org.apache.spark.sql.streaming

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, CreateNamedStruct, Expression, Literal, PreciseTimestampConversion, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{LongType, TimestampType}

class UpdatingSessionIterator(
    iter: Iterator[InternalRow],
    groupWithoutSessionExpressions: Seq[Expression],
    sessionExpression: Expression,
    aggregateExpressions: Seq[Expression],
    inputSchema: Seq[Attribute]) extends Iterator[InternalRow] {

  var currentKeys: InternalRow = _
  var currentSessionStart: Long = Long.MaxValue
  var currentSessionEnd: Long = Long.MinValue

  var currentRows: mutable.MutableList[InternalRow] = _

  var returnRowsIter: Iterator[InternalRow] = _
  var errorOnIterator: Boolean = false

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
      return returnRowsIter.next()
    }

    var exitCondition = false
    while (iter.hasNext && !exitCondition) {
      val row = iter.next()

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
        closeCurrentSession()
        startNewSession(row, keys, sessionStart, sessionEnd)
        exitCondition = true
      } else {
        if (sessionStart < currentSessionStart) {
          errorOnIterator = true
          throw new IllegalStateException("The iterator must be sorted by key and session start!")
        } else if (sessionStart <= currentSessionEnd) {
          // expanding session length if needed
          expandEndOfCurrentSession(sessionEnd)
          currentRows += row
        } else {
          closeCurrentSession()
          startNewSession(row, keys, sessionStart, sessionEnd)
          exitCondition = true
        }
      }
    }

    if (!iter.hasNext) {
      // no further row: closing session
      closeCurrentSession()
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
    currentKeys = keys
    currentSessionStart = sessionStart
    currentSessionEnd = sessionEnd
    currentRows = new mutable.MutableList[InternalRow]()
    currentRows += row
  }

  private def closeCurrentSession(): Unit = {
    val convertedGroupWithoutSessionExpressions = groupWithoutSessionExpressions.map { x =>
      BindReferences.bindReference[Expression](x, inputSchema)
    }
    val convertedAggregateExpressions = aggregateExpressions.map {
      x => BindReferences.bindReference[Expression](x, inputSchema)
    }

    val returnRows = currentRows.map { internalRow =>
      val sessionStruct = CreateNamedStruct(
        Literal("start") ::
          PreciseTimestampConversion(
            Literal(currentSessionStart, LongType), LongType, TimestampType) ::
          Literal("end") ::
          PreciseTimestampConversion(
            Literal(currentSessionEnd, LongType), LongType, TimestampType) ::
          Nil)

      val valueExpressions = convertedGroupWithoutSessionExpressions ++ Seq(sessionStruct) ++
        convertedAggregateExpressions

      val proj = GenerateUnsafeProjection.generate(valueExpressions, inputSchema)
      proj(internalRow)
    }.toList

    returnRowsIter = returnRows.iterator

    currentKeys = null
    currentSessionStart = Long.MaxValue
    currentSessionEnd = Long.MinValue
    currentRows = null
  }

  private def assertIteratorNotCorrupted(): Unit = {
    if (errorOnIterator) {
      throw new IllegalStateException("The iterator is already corrupted.")
    }
  }

}
