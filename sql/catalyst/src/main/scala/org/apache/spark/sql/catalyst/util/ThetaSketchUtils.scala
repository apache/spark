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
import org.apache.datasketches.tuple.{Sketch, Sketches, Summary, SummaryDeserializer, SummaryFactory, SummarySetOperations, UpdatableSummary}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryDeserializer, DoubleSummaryFactory, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryDeserializer, IntegerSummaryFactory, IntegerSummarySetOperations}
import org.apache.datasketches.tuple.strings.{ArrayOfStringsSummaryDeserializer, ArrayOfStringsSummaryFactory, ArrayOfStringsSummarySetOperations}

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.unsafe.types.UTF8String

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

  // Summary type constants
  final val SUMMARY_TYPE_DOUBLE = "double"
  final val SUMMARY_TYPE_INTEGER = "integer"
  final val SUMMARY_TYPE_STRING = "string"

  // Mode constants
  final val MODE_SUM = "sum"
  final val MODE_MIN = "min"
  final val MODE_MAX = "max"
  final val MODE_ALWAYSONE = "alwaysone"

  final val VALID_SUMMARY_TYPES: Seq[String] =
    Seq(SUMMARY_TYPE_DOUBLE, SUMMARY_TYPE_INTEGER, SUMMARY_TYPE_STRING)

  final val VALID_MODES: Seq[String] = Seq(MODE_SUM, MODE_MIN, MODE_MAX, MODE_ALWAYSONE)

  /**
   * Validates the lgNomLongs parameter for Theta sketch size. Throws a Spark SQL exception if the
   * value is out of bounds.
   *
   * @param lgNomLongs
   *   Log2 of nominal entries
   */
  def checkLgNomLongs(lgNomLongs: Int, prettyName: String): Unit = {
    if (lgNomLongs < MIN_LG_NOM_LONGS || lgNomLongs > MAX_LG_NOM_LONGS) {
      throw QueryExecutionErrors.thetaInvalidLgNomEntries(
        function = prettyName,
        min = MIN_LG_NOM_LONGS,
        max = MAX_LG_NOM_LONGS,
        value = lgNomLongs)
    }
  }

  /**
   * Validates and normalizes the summary type parameter. Converts the input to lowercase and
   * trims whitespace, then validates it's one of the supported types.
   *
   * @param summaryType
   *   The summary type string to validate
   * @param prettyName
   *   The display name of the function/expression for error messages
   */
  def checkSummaryType(summaryType: String, prettyName: String): Unit = {
    if (!VALID_SUMMARY_TYPES.contains(summaryType)) {
      throw QueryExecutionErrors.tupleInvalidSummaryType(
        prettyName,
        summaryType,
        VALID_SUMMARY_TYPES)
    }
  }

  /**
   * Validates and normalizes the aggregation mode parameter. Converts the input to lowercase and
   * trims whitespace, then validates it's one of the supported modes.
   *
   * @param mode
   *   The mode string to validate
   * @param prettyName
   *   The display name of the function/expression for error messages
   */
  def checkMode(mode: String, prettyName: String): Unit = {
    if (!VALID_MODES.contains(mode)) {
      throw QueryExecutionErrors.tupleInvalidMode(prettyName, mode, VALID_MODES)
    }
  }

  /**
   * Converts the mode string input to DoubleSummary.Mode enum. Used for double summary type
   * operations.
   *
   * @param modeInput
   *   The mode string to convert
   * @return
   *   The corresponding DoubleSummary.Mode enum value
   */
  def getDoubleSummaryMode(modeInput: String): DoubleSummary.Mode = {
    modeInput match {
      case MODE_SUM => DoubleSummary.Mode.Sum
      case MODE_MIN => DoubleSummary.Mode.Min
      case MODE_MAX => DoubleSummary.Mode.Max
      case MODE_ALWAYSONE => DoubleSummary.Mode.AlwaysOne
    }
  }

  /**
   * Converts the mode string input to IntegerSummary.Mode enum. Used for integer summary type
   * operations.
   *
   * @param modeInput
   *   The mode string to convert
   * @return
   *   The corresponding IntegerSummary.Mode enum value
   */
  def getIntegerSummaryMode(modeInput: String): IntegerSummary.Mode = {
    modeInput match {
      case MODE_SUM => IntegerSummary.Mode.Sum
      case MODE_MIN => IntegerSummary.Mode.Min
      case MODE_MAX => IntegerSummary.Mode.Max
      case MODE_ALWAYSONE => IntegerSummary.Mode.AlwaysOne
    }
  }

  /**
   * Creates the appropriate SummaryFactory based on summary type and mode. Used for creating
   * UpdatableSketch instances that need to construct new summary objects. For numeric types
   * (double/integer), the factory is configured with the aggregation mode.
   *
   * Note: The return type uses UpdatableSummary[Any] with asInstanceOf cast because we need to
   * handle multiple concrete summary types (DoubleSummary, IntegerSummary, ArrayOfStringsSummary)
   * in a single method. The actual concrete type is determined by summaryTypeInput and remains
   * consistent within a single aggregate operation. This approach avoids code duplication for
   * each summary type.
   *
   * @param summaryTypeInput
   *   The summary type string
   * @param modeInput
   *   The mode string
   * @return
   *   The appropriate SummaryFactory instance
   */
  def getSummaryFactory(
      summaryTypeInput: String,
      modeInput: String): SummaryFactory[UpdatableSummary[Any]] = {
    val summaryFactory = summaryTypeInput match {
      case SUMMARY_TYPE_DOUBLE =>
        new DoubleSummaryFactory(getDoubleSummaryMode(modeInput))
      case SUMMARY_TYPE_INTEGER =>
        new IntegerSummaryFactory(getIntegerSummaryMode(modeInput))
      case SUMMARY_TYPE_STRING =>
        new ArrayOfStringsSummaryFactory()
    }
    summaryFactory.asInstanceOf[SummaryFactory[UpdatableSummary[Any]]]
  }

  /**
   * Creates the appropriate SummarySetOperations based on summary type and mode. Used for Union
   * and Intersection operations that need to merge summary values from multiple sketches
   * according to the specified aggregation mode.
   *
   * Note: The return type uses Summary with asInstanceOf cast because we need to handle multiple
   * concrete summary types in a single method. The cast is safe as long as all operations within
   * a single aggregate use the same summaryTypeInput.
   *
   * @param summaryTypeInput
   *   The summary type string
   * @param modeInput
   *   The mode string
   * @return
   *   The appropriate SummarySetOperations instance
   */
  def getSummarySetOperations(
      summaryTypeInput: String,
      modeInput: String): SummarySetOperations[Summary] = {
    val ops = summaryTypeInput match {
      case SUMMARY_TYPE_DOUBLE =>
        new DoubleSummarySetOperations(getDoubleSummaryMode(modeInput))
      case SUMMARY_TYPE_INTEGER =>
        val mode = getIntegerSummaryMode(modeInput)
        new IntegerSummarySetOperations(mode, mode)
      case SUMMARY_TYPE_STRING =>
        new ArrayOfStringsSummarySetOperations()
    }

    ops.asInstanceOf[SummarySetOperations[Summary]]
  }

  /**
   * Creates the appropriate SummaryDeserializer based on summary type. Used for deserializing
   * binary sketch representations back into CompactSketch objects.
   *
   * Note: The return type uses Summary with asInstanceOf cast to handle multiple concrete
   * deserializer types in a single method. Type consistency is guaranteed by the caller passing
   * the same summaryTypeInput used during serialization.
   *
   * @param summaryTypeInput
   *   The summary type string
   * @return
   *   The appropriate SummaryDeserializer instance
   */
  def getSummaryDeserializer(summaryTypeInput: String): SummaryDeserializer[Summary] = {
    val deserializer = summaryTypeInput match {
      case SUMMARY_TYPE_DOUBLE =>
        new DoubleSummaryDeserializer()
      case SUMMARY_TYPE_INTEGER =>
        new IntegerSummaryDeserializer()
      case SUMMARY_TYPE_STRING =>
        new ArrayOfStringsSummaryDeserializer()
    }

    deserializer.asInstanceOf[SummaryDeserializer[Summary]]
  }

  /**
   * Deserializes a binary sketch representation into a CompactSketch wrapped in
   * FinalizedTupleSketch state. If the buffer is empty, creates a new aggregation buffer. Uses
   * the appropriate deserializer based on the configured summary type.
   *
   * Note: The return type Sketch[Summary] uses asInstanceOf because heapifySketch returns
   * Sketch[_ <: Summary] with the concrete type determined by the deserializer. The cast is safe
   * as type consistency is maintained through summaryTypeInput.
   *
   * @param buffer
   *   The binary sketch data to deserialize
   * @param summaryTypeInput
   *   The summary type string
   * @param prettyName
   *   The display name of the function/expression for error messages
   * @return
   *   A TupleSketchState containing the deserialized sketch
   */
  def heapifyTupleSketch(
      buffer: Array[Byte],
      summaryTypeInput: String,
      prettyName: String): Sketch[Summary] = {
    val mem = Memory.wrap(buffer)
    val sketch =
      try {
        Sketches.heapifySketch(mem, getSummaryDeserializer(summaryTypeInput))
      } catch {
        case e: Exception =>
          throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName, e.getMessage)
      }

    sketch.asInstanceOf[Sketch[Summary]]
  }

  /**
   * Converts and validates a summary value to the expected type based on the configured summary
   * type. Performs type coercion where appropriate (e.g., Float to Double for double summaries).
   *
   * @param summaryTypeInput
   *   The expected summary type (double, integer, or string)
   * @param summaryValue
   *   The actual summary value to convert
   * @param prettyName
   *   The display name of the function/expression for error messages
   * @return
   *   The converted summary value as the appropriate type (Double, Int, or UTF8String)
   * @throws QueryExecutionErrors
   *   if the summary value type doesn't match or cannot be converted to the expected type
   */
  def convertSummaryValue(
      summaryTypeInput: String,
      summaryValue: Any,
      prettyName: String): Any = {
    (summaryTypeInput, summaryValue) match {
      case (SUMMARY_TYPE_DOUBLE, d: Double) => d
      case (SUMMARY_TYPE_DOUBLE, f: Float) => f.toDouble
      case (SUMMARY_TYPE_INTEGER, i: Integer) => i
      case (SUMMARY_TYPE_STRING, arr: ArrayData) =>
        (0 until arr.numElements()).map(i => arr.getUTF8String(i).toString).toArray
      case (SUMMARY_TYPE_STRING, s: UTF8String) => Array(s.toString)
      case _ =>
        val actualType = summaryValue.getClass.getSimpleName
        throw QueryExecutionErrors.tupleInvalidSummaryValueType(
          prettyName,
          summaryTypeInput,
          actualType)
    }
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
