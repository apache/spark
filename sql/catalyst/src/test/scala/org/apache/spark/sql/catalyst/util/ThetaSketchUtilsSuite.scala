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

import org.apache.datasketches.theta.UpdateSketch
import org.apache.datasketches.tuple.UpdatableSketchBuilder
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryFactory}
import org.apache.datasketches.tuple.aninteger.IntegerSummary

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper

class ThetaSketchUtilsSuite extends SparkFunSuite with SQLHelper {

  test("checkLgNomLongs: accepts values within valid range") {
    val validValues =
      Seq(ThetaSketchUtils.MIN_LG_NOM_LONGS, 10, 20, ThetaSketchUtils.MAX_LG_NOM_LONGS)
    validValues.foreach { value =>
      // There should be no error here.
      ThetaSketchUtils.checkLgNomLongs(value, "test_function")
    }
  }

  test("checkLgNomLongs: throws exception for values below minimum") {
    val invalidValues = Seq(ThetaSketchUtils.MIN_LG_NOM_LONGS - 1, 0, -5)
    invalidValues.foreach { value =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          ThetaSketchUtils.checkLgNomLongs(value, "test_function")
        },
        condition = "SKETCH_INVALID_LG_NOM_ENTRIES",
        parameters = Map(
          "function" -> "`test_function`",
          "min" -> ThetaSketchUtils.MIN_LG_NOM_LONGS.toString,
          "max" -> ThetaSketchUtils.MAX_LG_NOM_LONGS.toString,
          "value" -> value.toString))
    }
  }

  test("checkLgNomLongs: throws exception for values above maximum") {
    val invalidValues = Seq(ThetaSketchUtils.MAX_LG_NOM_LONGS + 1, 30, 100)
    invalidValues.foreach { value =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          ThetaSketchUtils.checkLgNomLongs(value, "test_function")
        },
        condition = "SKETCH_INVALID_LG_NOM_ENTRIES",
        parameters = Map(
          "function" -> "`test_function`",
          "min" -> ThetaSketchUtils.MIN_LG_NOM_LONGS.toString,
          "max" -> ThetaSketchUtils.MAX_LG_NOM_LONGS.toString,
          "value" -> value.toString))
    }
  }

  test("wrapCompactSketch: successfully wraps valid sketch bytes") {
    // Create a valid sketch and get its bytes.
    val updateSketch = UpdateSketch.builder().build()
    updateSketch.update("test1")
    updateSketch.update("test2")
    updateSketch.update("test3")
    val compactSketch = updateSketch.compact
    val validBytes = compactSketch.toByteArrayCompressed

    // Test that wrapCompactSketch can successfully wrap the valid bytes
    val wrappedSketch = ThetaSketchUtils.wrapCompactSketch(validBytes, "test_function")

    assert(wrappedSketch != null)
    assert(wrappedSketch.getEstimate == compactSketch.getEstimate)
    assert(wrappedSketch.getRetainedEntries == compactSketch.getRetainedEntries)
  }

  test("wrapCompactSketch: throws exception for null bytes") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        ThetaSketchUtils.wrapCompactSketch(null, "test_function")
      },
      condition = "THETA_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map("function" -> "`test_function`"))
  }

  test("wrapCompactSketch: throws exception for empty bytes") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        ThetaSketchUtils.wrapCompactSketch(Array.empty[Byte], "test_function")
      },
      condition = "THETA_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map("function" -> "`test_function`"))
  }

  test("wrapCompactSketch: throws exception for invalid bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    checkError(
      exception = intercept[SparkRuntimeException] {
        ThetaSketchUtils.wrapCompactSketch(invalidBytes, "test_function")
      },
      condition = "THETA_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map("function" -> "`test_function`"))
  }

  test("checkMode: accepts valid modes") {
    val validModes = Seq(
      ThetaSketchUtils.MODE_SUM,
      ThetaSketchUtils.MODE_MIN,
      ThetaSketchUtils.MODE_MAX,
      ThetaSketchUtils.MODE_ALWAYSONE)
    validModes.foreach { mode =>
      // Should not throw any exception
      ThetaSketchUtils.checkMode(mode, "test_function")
    }
  }

  test("checkMode: throws exception for invalid modes") {
    val invalidModes = Seq("invalid", "average", "count", "multiply", "")
    invalidModes.foreach { mode =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          ThetaSketchUtils.checkMode(mode, "test_function")
        },
        condition = "TUPLE_INVALID_SKETCH_MODE",
        parameters = Map(
          "function" -> "`test_function`",
          "mode" -> mode,
          "validModes" -> ThetaSketchUtils.VALID_MODES.mkString(", ")))
    }
  }

  test("heapifyDoubleTupleSketch: successfully deserializes valid tuple sketch bytes") {
    // Create a valid tuple sketch and get its bytes
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 1.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 3.0)

    val compactSketch = updateSketch.compact()
    val validBytes = compactSketch.toByteArray

    // Test that heapifyDoubleTupleSketch can successfully deserialize the valid bytes
    val heapifiedSketch = ThetaSketchUtils.heapifyDoubleTupleSketch(validBytes, "test_function")

    assert(heapifiedSketch != null)
    assert(heapifiedSketch.getEstimate == compactSketch.getEstimate)
    assert(heapifiedSketch.getRetainedEntries == compactSketch.getRetainedEntries)
  }

  test("heapifyDoubleTupleSketch: throws exception for invalid bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    checkError(
      exception = intercept[SparkRuntimeException] {
        ThetaSketchUtils.heapifyDoubleTupleSketch(invalidBytes, "test_function")
      },
      condition = "TUPLE_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map(
        "function" -> "`test_function`",
        "reason" -> "Possible corruption: Invalid Family: COMPACT"))
  }

  test("getDoubleSummaryMode: returns correct mode for valid strings") {
    assert(ThetaSketchUtils.getDoubleSummaryMode("sum") == DoubleSummary.Mode.Sum)
    assert(ThetaSketchUtils.getDoubleSummaryMode("min") == DoubleSummary.Mode.Min)
    assert(ThetaSketchUtils.getDoubleSummaryMode("max") == DoubleSummary.Mode.Max)
    assert(ThetaSketchUtils.getDoubleSummaryMode("alwaysone") == DoubleSummary.Mode.AlwaysOne)
  }

  test("getIntegerSummaryMode: returns correct mode for valid strings") {
    assert(ThetaSketchUtils.getIntegerSummaryMode("sum") == IntegerSummary.Mode.Sum)
    assert(ThetaSketchUtils.getIntegerSummaryMode("min") == IntegerSummary.Mode.Min)
    assert(ThetaSketchUtils.getIntegerSummaryMode("max") == IntegerSummary.Mode.Max)
    assert(ThetaSketchUtils.getIntegerSummaryMode("alwaysone") == IntegerSummary.Mode.AlwaysOne)
  }

  test("heapifyIntegerTupleSketch: successfully deserializes valid tuple sketch bytes") {
    import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryFactory}
    // Create a valid integer tuple sketch and get its bytes
    val summaryFactory = new IntegerSummaryFactory(IntegerSummary.Mode.Sum)
    val updateSketch =
      new UpdatableSketchBuilder[java.lang.Integer, IntegerSummary](summaryFactory)
        .build()

    updateSketch.update("test1", 1)
    updateSketch.update("test2", 2)
    updateSketch.update("test3", 3)

    val compactSketch = updateSketch.compact()
    val validBytes = compactSketch.toByteArray

    // Test that heapifyIntegerTupleSketch can successfully deserialize the valid bytes
    val heapifiedSketch = ThetaSketchUtils.heapifyIntegerTupleSketch(validBytes, "test_function")

    assert(heapifiedSketch != null)
    assert(heapifiedSketch.getEstimate == compactSketch.getEstimate)
    assert(heapifiedSketch.getRetainedEntries == compactSketch.getRetainedEntries)
  }

  test("heapifyIntegerTupleSketch: throws exception for invalid bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    checkError(
      exception = intercept[SparkRuntimeException] {
        ThetaSketchUtils.heapifyIntegerTupleSketch(invalidBytes, "test_function")
      },
      condition = "TUPLE_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map(
        "function" -> "`test_function`",
        "reason" -> "Possible corruption: Invalid Family: COMPACT"))
  }

  test("heapifyStringTupleSketch: successfully deserializes valid tuple sketch bytes") {
    import org.apache.datasketches.tuple.strings.{ArrayOfStringsSummary, ArrayOfStringsSummaryFactory}
    // Create a valid string tuple sketch and get its bytes
    val summaryFactory = new ArrayOfStringsSummaryFactory()
    val updateSketch =
      new UpdatableSketchBuilder[Array[String], ArrayOfStringsSummary](summaryFactory)
        .build()

    updateSketch.update("test1", Array("a"))
    updateSketch.update("test2", Array("b"))
    updateSketch.update("test3", Array("c"))

    val compactSketch = updateSketch.compact()
    val validBytes = compactSketch.toByteArray

    // Test that heapifyStringTupleSketch can successfully deserialize the valid bytes
    val heapifiedSketch = ThetaSketchUtils.heapifyStringTupleSketch(validBytes, "test_function")

    assert(heapifiedSketch != null)
    assert(heapifiedSketch.getEstimate == compactSketch.getEstimate)
    assert(heapifiedSketch.getRetainedEntries == compactSketch.getRetainedEntries)
  }

  test("heapifyStringTupleSketch: throws exception for invalid bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    checkError(
      exception = intercept[SparkRuntimeException] {
        ThetaSketchUtils.heapifyStringTupleSketch(invalidBytes, "test_function")
      },
      condition = "TUPLE_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map(
        "function" -> "`test_function`",
        "reason" -> "Possible corruption: Invalid Family: COMPACT"))
  }

  test("aggregateNumericSummaries: sum mode aggregates correctly for Double") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 1.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 3.0)

    val compactSketch = updateSketch.compact()
    val result = ThetaSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      "sum",
      it => it.getSummary.getValue)

    assert(result == 6.0)
  }

  test("aggregateNumericSummaries: min mode finds minimum for Double") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 5.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 8.0)

    val compactSketch = updateSketch.compact()
    val result = ThetaSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      "min",
      it => it.getSummary.getValue)

    assert(result == 2.0)
  }

  test("aggregateNumericSummaries: max mode finds maximum for Double") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 5.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 8.0)

    val compactSketch = updateSketch.compact()
    val result = ThetaSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      "max",
      it => it.getSummary.getValue)

    assert(result == 8.0)
  }

  test("aggregateNumericSummaries: alwaysone mode counts entries for Double") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 5.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 8.0)

    val compactSketch = updateSketch.compact()
    val result = ThetaSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      "alwaysone",
      it => it.getSummary.getValue)

    assert(result == 3.0)
  }

  test("aggregateNumericSummaries: sum mode aggregates correctly for Long") {
    import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryFactory}
    val summaryFactory = new IntegerSummaryFactory(IntegerSummary.Mode.Sum)
    val updateSketch =
      new UpdatableSketchBuilder[java.lang.Integer, IntegerSummary](summaryFactory)
        .build()

    updateSketch.update("test1", 10)
    updateSketch.update("test2", 20)
    updateSketch.update("test3", 30)

    val compactSketch = updateSketch.compact()
    val result = ThetaSketchUtils.aggregateNumericSummaries[IntegerSummary, Long](
      compactSketch.iterator(),
      "sum",
      it => it.getSummary.getValue.toLong)

    assert(result == 60L)
  }
}
