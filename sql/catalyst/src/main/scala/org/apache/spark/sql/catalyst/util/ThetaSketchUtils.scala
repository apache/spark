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

package org.apache.spark.sql.catalyst.util

import org.apache.datasketches.common.SketchesArgumentException
import org.apache.datasketches.memory.{Memory, MemoryBoundsException}
import org.apache.datasketches.theta.CompactSketch
import org.apache.datasketches.tuple.{Sketch, Sketches, Summary, TupleSketchIterator}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryDeserializer}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryDeserializer}

import org.apache.spark.sql.errors.QueryExecutionErrors

object ThetaSketchUtils {
  /*
   * Bounds copied from DataSketches' ThetaUtil. These define the valid range for lgNomEntries,
   * which is the log-base-2 of the nominal number of entries that determines the sketch size.
   * The actual number of buckets in the sketch = 2^lgNomEntries.
   * MIN_LG_NOM_LONGS = 4 means minimum 16 buckets (2^4), MAX_LG_NOM_LONGS = 26 means maximum
   * ~67 million buckets (2^26). These bounds ensure reasonable memory usage while maintaining
   * sketch accuracy for cardinality estimation.
   */
  final val MIN_LG_NOM_LONGS = 4
  final val MAX_LG_NOM_LONGS = 26
  final val DEFAULT_LG_NOM_LONGS = 12

  /**
   * Validates the lgNomLongs parameter for Theta/Tuple sketch size. Throws a Spark SQL exception
   * if the value is out of bounds.
   *
   * @param lgNomLongs
   *   Log2 of nominal entries
   * @param prettyName
   *   The display name of the function/expression for error messages
   */
  def checkLgNomLongs(lgNomLongs: Int, prettyName: String): Unit = {
    if (lgNomLongs < MIN_LG_NOM_LONGS || lgNomLongs > MAX_LG_NOM_LONGS) {
      throw QueryExecutionErrors.sketchInvalidLgNomEntries(
        function = prettyName,
        min = MIN_LG_NOM_LONGS,
        max = MAX_LG_NOM_LONGS,
        value = lgNomLongs)
    }
  }

  /**
   * Aggregates numeric summaries from a tuple sketch iterator based on the specified mode.
   * This method provides compile-time exhaustiveness checking through pattern matching on
   * the sealed TupleSummaryMode trait.
   *
   * @param iterator
   *   The tuple sketch iterator to aggregate
   * @param mode
   *   The aggregation mode (Sum, Min, Max, or AlwaysOne)
   * @param getValue
   *   Function to extract the numeric value from the iterator
   * @param num
   *   Implicit Numeric instance for the value type
   * @tparam S
   *   The summary type
   * @tparam V
   *   The value type
   * @return
   *   The aggregated value
   */
  def aggregateNumericSummaries[S <: Summary, V](
      iterator: TupleSketchIterator[S],
      mode: TupleSummaryMode,
      getValue: TupleSketchIterator[S] => V)(implicit num: Numeric[V]): V = {

    mode match {
      case TupleSummaryMode.Sum =>
        var sum = num.zero
        while (iterator.next()) {
          sum = num.plus(sum, getValue(iterator))
        }
        sum

      case TupleSummaryMode.Min =>
        var min: Option[V] = None
        while (iterator.next()) {
          val value = getValue(iterator)
          min = min match {
            case Some(m) => Some(num.min(m, value))
            case None => Some(value)
          }
        }
        min.getOrElse(num.zero)

      case TupleSummaryMode.Max =>
        var max: Option[V] = None
        while (iterator.next()) {
          val value = getValue(iterator)
          max = max match {
            case Some(m) => Some(num.max(m, value))
            case None => Some(value)
          }
        }
        max.getOrElse(num.zero)

      case TupleSummaryMode.AlwaysOne =>
        var count = num.zero
        while (iterator.next()) {
          count = num.plus(count, num.one)
        }
        count
    }
  }

  /**
   * Deserializes a binary tuple sketch representation into a Sketch with the
   * appropriate summary type.
   *
   * @param bytes
   *   The binary sketch data to deserialize
   * @param deserializer
   *   The summary deserializer for the target summary type
   * @param prettyName
   *   The display name of the function/expression for error messages
   * @tparam U
   *   The summary type, inferred from the deserializer
   * @return
   *   A deserialized sketch with summary type U
   */
  def heapifyTupleSketch[U <: Summary](
      bytes: Array[Byte],
      deserializer: org.apache.datasketches.tuple.SummaryDeserializer[U],
      prettyName: String): Sketch[U] = {
    val memory =
      try {
        Memory.wrap(bytes)
      } catch {
        case _: NullPointerException | _: MemoryBoundsException =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }

    try {
      Sketches.heapifySketch(memory, deserializer)
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Deserializes a Double summary type binary tuple sketch representation into a Sketch.
   *
   * @param bytes
   *   The binary sketch data to deserialize
   * @param prettyName
   *   The display name of the function/expression for error messages
   * @return
   *   A deserialized sketch
   */
  def heapifyDoubleTupleSketch(bytes: Array[Byte], prettyName: String): Sketch[DoubleSummary] = {
    heapifyTupleSketch(bytes, new DoubleSummaryDeserializer(), prettyName)
  }

  /**
   * Deserializes an Integer summary type binary tuple sketch representation into a Sketch.
   *
   * @param bytes
   *   The binary sketch data to deserialize
   * @param prettyName
   *   The display name of the function/expression for error messages
   * @return
   *   A deserialized sketch
   */
  def heapifyIntegerTupleSketch(
      bytes: Array[Byte],
      prettyName: String): Sketch[IntegerSummary] = {
    heapifyTupleSketch(bytes, new IntegerSummaryDeserializer(), prettyName)
  }

  /**
   * Wraps a byte array into a DataSketches CompactSketch object. This method safely deserializes
   * a compact Theta sketch from its binary representation, handling potential deserialization
   * errors by throwing appropriate Spark SQL exceptions.
   *
   * @param bytes
   *   The binary representation of a compact theta sketch
   * @param prettyName
   *   The display name of the function/expression for error messages
   * @return
   *   A CompactSketch object wrapping the provided bytes
   */
  def wrapCompactSketch(bytes: Array[Byte], prettyName: String): CompactSketch = {
    val memory =
      try {
        Memory.wrap(bytes)
      } catch {
        case _: NullPointerException | _: MemoryBoundsException =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }

    try {
      CompactSketch.wrap(memory)
    } catch {
      case _: SketchesArgumentException | _: MemoryBoundsException =>
        throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }
  }
}
