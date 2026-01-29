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

package org.apache.spark.sql.execution.streaming.operators.stateful.join

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager.ValueAndMatchPair
import org.apache.spark.sql.types.BooleanType

/**
 * Converter between the value row stored in state store and the (actual value, match) pair.
 */
trait StreamingSymmetricHashJoinValueRowConverter {
  /** Defines the schema of the value row (the value side of K-V in state store). */
  def valueAttributes: Seq[Attribute]

  /**
   * Convert the value row to (actual value, match) pair.
   *
   * NOTE: implementations should ensure the result row is NOT reused during execution, so
   * that caller can safely read the value in any time.
   */
  def convertValue(value: UnsafeRow): ValueAndMatchPair

  /**
   * Build the value row from (actual value, match) pair. This is expected to be called just
   * before storing to the state store.
   *
   * NOTE: depending on the implementation, the result row "may" be reused during execution
   * (to avoid initialization of object), so the caller should ensure that the logic doesn't
   * affect by such behavior. Call copy() against the result row if needed.
   */
  def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow
}

class StreamingSymmetricHashJoinValueRowConverterFormatV1(
    inputValueAttributes: Seq[Attribute]) extends StreamingSymmetricHashJoinValueRowConverter {
  override val valueAttributes: Seq[Attribute] = inputValueAttributes

  override def convertValue(value: UnsafeRow): ValueAndMatchPair = {
    if (value != null) ValueAndMatchPair(value, false) else null
  }

  override def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow = value
}

class StreamingSymmetricHashJoinValueRowConverterFormatV2(
    inputValueAttributes: Seq[Attribute]) extends StreamingSymmetricHashJoinValueRowConverter {
  private val valueWithMatchedExprs = inputValueAttributes :+ Literal(true)
  private val indexOrdinalInValueWithMatchedRow = inputValueAttributes.size

  private val valueWithMatchedRowGenerator = UnsafeProjection.create(valueWithMatchedExprs,
    inputValueAttributes)

  override val valueAttributes: Seq[Attribute] = inputValueAttributes :+
    AttributeReference("matched", BooleanType)()

  // Projection to generate key row from (value + matched) row
  private val valueRowGenerator = UnsafeProjection.create(
    inputValueAttributes, valueAttributes)

  override def convertValue(value: UnsafeRow): ValueAndMatchPair = {
    if (value != null) {
      ValueAndMatchPair(valueRowGenerator(value).copy(),
        value.getBoolean(indexOrdinalInValueWithMatchedRow))
    } else {
      null
    }
  }

  override def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow = {
    val row = valueWithMatchedRowGenerator(value)
    row.setBoolean(indexOrdinalInValueWithMatchedRow, matched)
    row
  }
}

object StreamingSymmetricHashJoinValueRowConverter {
  def create(
      inputValueAttributes: Seq[Attribute],
      stateFormatVersion: Int): StreamingSymmetricHashJoinValueRowConverter = {
    stateFormatVersion match {
      case 1 => new StreamingSymmetricHashJoinValueRowConverterFormatV1(inputValueAttributes)
      case 2 | 3 | 4 =>
        new StreamingSymmetricHashJoinValueRowConverterFormatV2(inputValueAttributes)
      case _ => throw new IllegalArgumentException ("Incorrect state format version! " +
        s"version $stateFormatVersion")
    }
  }
}
