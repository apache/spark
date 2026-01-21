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

import java.util.Locale

import org.apache.datasketches.memory.{Memory, MemoryBoundsException}
import org.apache.datasketches.tuple.{Sketch, Sketches, Summary, TupleSketchIterator}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryDeserializer}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryDeserializer}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.unsafe.types.UTF8String

/**
 * Sealed trait representing valid summary modes for tuple sketches. This provides type-safe
 * mode handling with compile-time exhaustiveness checking and prevents invalid modes from
 * being created.
 */
sealed trait TupleSummaryMode {
  def toDoubleSummaryMode: DoubleSummary.Mode

  def toIntegerSummaryMode: IntegerSummary.Mode

  def toString: String
}

object TupleSummaryMode {
  case object Sum extends TupleSummaryMode {
    def toDoubleSummaryMode: DoubleSummary.Mode = DoubleSummary.Mode.Sum
    def toIntegerSummaryMode: IntegerSummary.Mode = IntegerSummary.Mode.Sum
    override def toString: String = "sum"
  }

  case object Min extends TupleSummaryMode {
    def toDoubleSummaryMode: DoubleSummary.Mode = DoubleSummary.Mode.Min
    def toIntegerSummaryMode: IntegerSummary.Mode = IntegerSummary.Mode.Min
    override def toString: String = "min"
  }

  case object Max extends TupleSummaryMode {
    def toDoubleSummaryMode: DoubleSummary.Mode = DoubleSummary.Mode.Max
    def toIntegerSummaryMode: IntegerSummary.Mode = IntegerSummary.Mode.Max
    override def toString: String = "max"
  }

  case object AlwaysOne extends TupleSummaryMode {
    def toDoubleSummaryMode: DoubleSummary.Mode = DoubleSummary.Mode.AlwaysOne
    def toIntegerSummaryMode: IntegerSummary.Mode = IntegerSummary.Mode.AlwaysOne
    override def toString: String = "alwaysone"
  }

  /** All valid modes */
  val validModes: Seq[TupleSummaryMode] = Seq(Sum, Min, Max, AlwaysOne)

  /** String representations of valid modes for error messages */
  val validModeStrings: Seq[String] = validModes.map(_.toString)

  /**
   * Parses a string into a TupleSummaryMode. This is the single entry point for string-to-mode
   * conversion, ensuring validation happens once.
   *
   * @param s The mode string to parse
   * @param functionName The display name of the function/expression for error messages
   * @return The corresponding TupleSummaryMode
   * @throws QueryExecutionErrors.tupleInvalidMode if the mode string is invalid
   */
  def fromString(s: String, functionName: String): TupleSummaryMode = {
    s.toLowerCase(Locale.ROOT) match {
      case "sum" => Sum
      case "min" => Min
      case "max" => Max
      case "alwaysone" => AlwaysOne
      case _ => throw QueryExecutionErrors.tupleInvalidMode(functionName, s, validModeStrings)
    }
  }
}

/**
 * Trait for TupleSketch expressions that use the lgNomEntries parameter. Provides
 * validation and extraction functionality for the log-base-2 of nominal entries.
 */
trait SketchSize extends Expression {

  /** log-base-2 of nominal entries (determines sketch size). */
  def lgNomEntries: Expression

  /** Returns the pretty name of the aggregation function for error messages. */
  protected def prettyName: String

  /**
   * Validates that lgNomEntries parameter is a constant and within valid range (4-26).
   */
  protected def checkLgNomEntriesParameter(): TypeCheckResult = {
    if (!lgNomEntries.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "lgNomEntries",
          "inputType" -> "int",
          "inputExpr" -> lgNomEntries.sql))
    } else if (lgNomEntries.eval() == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "lgNomEntries"))
    } else {
      try {
        ThetaSketchUtils.checkLgNomLongs(lgNomEntriesInput, prettyName)
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: Exception =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    }
  }

  /**
   * Validates and extracts the lgNomEntries parameter value. Ensures the value is a constant and
   * within valid range (4-26)
   */
  protected lazy val lgNomEntriesInput: Int = {
    lgNomEntries.eval().asInstanceOf[Int]
  }
}

/**
 * Trait for TupleSketch expressions that use the mode parameter. Provides validation
 * and extraction functionality for the aggregation mode.
 *
 * Tuple sketches extend Theta sketches by associating summary values with each key. When
 * performing set operations (union, intersection) or building sketches, duplicate keys may appear
 * with different summary values. The mode parameter determines how to combine these values: 'sum'
 * adds them together, 'min' keeps the smallest, 'max' keeps the largest, and 'alwaysone' sets all
 * summary values to 1 (effectively behaving like a Theta sketch).
 */
trait SummaryAggregateMode extends Expression {

  /** Aggregation mode for numeric summaries (sum, min, max, alwaysone). */
  def mode: Expression

  /** Returns the pretty name of the aggregation function for error messages. */
  protected def prettyName: String

  /**
   * Validates and parses the mode parameter into a TupleSummaryMode. This lazy val ensures
   * string parsing happens only once and provides compile-time type safety for all subsequent
   * uses. Validation is performed by TupleSummaryMode.fromString which throws an exception for
   * invalid modes.
   */
  protected lazy val modeEnum: TupleSummaryMode = {
    val modeStr = mode.eval().asInstanceOf[UTF8String].toString
    TupleSummaryMode.fromString(modeStr, prettyName)
  }

  /**
   * Validates that mode parameter is a constant string (sum, min, max, alwaysone) and can be
   * successfully parsed into a TupleSummaryMode. Forces evaluation of modeEnum to trigger
   * validation.
   */
  protected def checkModeParameter(): TypeCheckResult = {
    if (!mode.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters =
          Map("inputName" -> "mode", "inputType" -> "string", "inputExpr" -> mode.sql))
    } else if (mode.eval() == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "mode"))
    } else {
      try {
        // Force evaluation to validate the mode
        modeEnum
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: Exception =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    }
  }
}

object TupleSketchUtils {
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
  def heapifySketch[U <: Summary](
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
  def heapifyDoubleSketch(bytes: Array[Byte], prettyName: String): Sketch[DoubleSummary] = {
    heapifySketch(bytes, new DoubleSummaryDeserializer(), prettyName)
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
  def heapifyIntegerSketch(
      bytes: Array[Byte],
      prettyName: String): Sketch[IntegerSummary] = {
    heapifySketch(bytes, new IntegerSummaryDeserializer(), prettyName)
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
}
