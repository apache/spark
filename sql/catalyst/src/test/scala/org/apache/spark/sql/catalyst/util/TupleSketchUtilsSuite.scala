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

import org.apache.datasketches.tuple.UpdatableSketchBuilder
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryFactory}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryFactory}

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper

class TupleSketchUtilsSuite extends SparkFunSuite with SQLHelper {

  test("TupleSummaryMode.fromString: accepts valid modes") {
    val validModes = Seq("sum", "min", "max", "alwaysone")
    validModes.foreach { mode =>
      // Should not throw any exception
      val result = TupleSummaryMode.fromString(mode, "test_function")
      assert(result != null)
      assert(result.toString == mode)
    }
  }

  test("TupleSummaryMode.fromString: case insensitive") {
    assert(TupleSummaryMode.fromString("SUM", "test_function") == TupleSummaryMode.Sum)
    assert(TupleSummaryMode.fromString("Min", "test_function") == TupleSummaryMode.Min)
    assert(TupleSummaryMode.fromString("MAX", "test_function") == TupleSummaryMode.Max)
    assert(TupleSummaryMode.fromString("AlwaysOne", "test_function") == TupleSummaryMode.AlwaysOne)
  }

  test("TupleSummaryMode.fromString: throws exception for invalid modes") {
    val invalidModes = Seq("invalid", "average", "count", "multiply", "")
    invalidModes.foreach { mode =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          TupleSummaryMode.fromString(mode, "test_function")
        },
        condition = "TUPLE_INVALID_SKETCH_MODE",
        parameters = Map(
          "function" -> "`test_function`",
          "mode" -> mode,
          "validModes" -> TupleSummaryMode.validModeStrings.mkString(", ")))
    }
  }

  test("TupleSummaryMode: converts to DoubleSummary.Mode correctly") {
    assert(TupleSummaryMode.Sum.toDoubleSummaryMode == DoubleSummary.Mode.Sum)
    assert(TupleSummaryMode.Min.toDoubleSummaryMode == DoubleSummary.Mode.Min)
    assert(TupleSummaryMode.Max.toDoubleSummaryMode == DoubleSummary.Mode.Max)
    assert(TupleSummaryMode.AlwaysOne.toDoubleSummaryMode == DoubleSummary.Mode.AlwaysOne)
  }

  test("TupleSummaryMode: converts to IntegerSummary.Mode correctly") {
    assert(TupleSummaryMode.Sum.toIntegerSummaryMode == IntegerSummary.Mode.Sum)
    assert(TupleSummaryMode.Min.toIntegerSummaryMode == IntegerSummary.Mode.Min)
    assert(TupleSummaryMode.Max.toIntegerSummaryMode == IntegerSummary.Mode.Max)
    assert(TupleSummaryMode.AlwaysOne.toIntegerSummaryMode == IntegerSummary.Mode.AlwaysOne)
  }

  test("heapifyDoubleSketch: successfully deserializes valid tuple sketch bytes") {
    // Create a valid tuple sketch and get its bytes
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 1.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 3.0)

    val compactSketch = updateSketch.compact()
    val validBytes = compactSketch.toByteArray

    // Test that heapifyDoubleSketch can successfully deserialize the valid bytes
    val heapifiedSketch = TupleSketchUtils.heapifyDoubleSketch(validBytes, "test_function")

    assert(heapifiedSketch != null)
    assert(heapifiedSketch.getEstimate == compactSketch.getEstimate)
    assert(heapifiedSketch.getRetainedEntries == compactSketch.getRetainedEntries)
  }

  test("heapifyDoubleSketch: throws exception for invalid bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    checkError(
      exception = intercept[SparkRuntimeException] {
        TupleSketchUtils.heapifyDoubleSketch(invalidBytes, "test_function")
      },
      condition = "TUPLE_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map("function" -> "`test_function`"))
  }

  test("heapifyIntegerSketch: successfully deserializes valid tuple sketch bytes") {
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

    // Test that heapifyIntegerSketch can successfully deserialize the valid bytes
    val heapifiedSketch = TupleSketchUtils.heapifyIntegerSketch(validBytes, "test_function")

    assert(heapifiedSketch != null)
    assert(heapifiedSketch.getEstimate == compactSketch.getEstimate)
    assert(heapifiedSketch.getRetainedEntries == compactSketch.getRetainedEntries)
  }

  test("heapifyIntegerSketch: throws exception for invalid bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    checkError(
      exception = intercept[SparkRuntimeException] {
        TupleSketchUtils.heapifyIntegerSketch(invalidBytes, "test_function")
      },
      condition = "TUPLE_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map("function" -> "`test_function`"))
  }

  test("aggregateNumericSummaries: sum mode aggregates correctly for Double") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 1.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 3.0)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Sum,
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
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Min,
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
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Max,
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
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.AlwaysOne,
      it => it.getSummary.getValue)

    assert(result == 3.0)
  }

  test("aggregateNumericSummaries: sum mode aggregates correctly for Long") {
    val summaryFactory = new IntegerSummaryFactory(IntegerSummary.Mode.Sum)
    val updateSketch =
      new UpdatableSketchBuilder[java.lang.Integer, IntegerSummary](summaryFactory)
        .build()

    updateSketch.update("test1", 10)
    updateSketch.update("test2", 20)
    updateSketch.update("test3", 30)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[IntegerSummary, Long](
      compactSketch.iterator(),
      TupleSummaryMode.Sum,
      it => it.getSummary.getValue.toLong)

    assert(result == 60L)
  }

  test("aggregateNumericSummaries: min mode finds minimum for Long") {
    val summaryFactory = new IntegerSummaryFactory(IntegerSummary.Mode.Sum)
    val updateSketch =
      new UpdatableSketchBuilder[java.lang.Integer, IntegerSummary](summaryFactory)
        .build()

    updateSketch.update("test1", 50)
    updateSketch.update("test2", 20)
    updateSketch.update("test3", 80)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[IntegerSummary, Long](
      compactSketch.iterator(),
      TupleSummaryMode.Min,
      it => it.getSummary.getValue.toLong)

    assert(result == 20L)
  }

  test("aggregateNumericSummaries: max mode finds maximum for Long") {
    val summaryFactory = new IntegerSummaryFactory(IntegerSummary.Mode.Sum)
    val updateSketch =
      new UpdatableSketchBuilder[java.lang.Integer, IntegerSummary](summaryFactory)
        .build()

    updateSketch.update("test1", 50)
    updateSketch.update("test2", 20)
    updateSketch.update("test3", 80)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[IntegerSummary, Long](
      compactSketch.iterator(),
      TupleSummaryMode.Max,
      it => it.getSummary.getValue.toLong)

    assert(result == 80L)
  }

  test("aggregateNumericSummaries: alwaysone mode counts entries for Long") {
    val summaryFactory = new IntegerSummaryFactory(IntegerSummary.Mode.Sum)
    val updateSketch =
      new UpdatableSketchBuilder[java.lang.Integer, IntegerSummary](summaryFactory)
        .build()

    updateSketch.update("test1", 50)
    updateSketch.update("test2", 20)
    updateSketch.update("test3", 80)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[IntegerSummary, Long](
      compactSketch.iterator(),
      TupleSummaryMode.AlwaysOne,
      it => it.getSummary.getValue.toLong)

    assert(result == 3L)
  }

  test("aggregateNumericSummaries: empty sketch returns zero for sum mode") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Sum,
      it => it.getSummary.getValue)

    assert(result == 0.0)
  }

  test("aggregateNumericSummaries: empty sketch returns zero for min mode") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Min,
      it => it.getSummary.getValue)

    assert(result == 0.0)
  }

  test("aggregateNumericSummaries: empty sketch returns zero for max mode") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Max,
      it => it.getSummary.getValue)

    assert(result == 0.0)
  }

  test("aggregateNumericSummaries: empty sketch returns zero for alwaysone mode") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.AlwaysOne,
      it => it.getSummary.getValue)

    assert(result == 0.0)
  }

  test("aggregateNumericSummaries: single entry sketch") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 42.0)

    val compactSketch = updateSketch.compact()

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Sum, it => it.getSummary.getValue) == 42.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Min, it => it.getSummary.getValue) == 42.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Max, it => it.getSummary.getValue) == 42.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.AlwaysOne, it => it.getSummary.getValue) == 1.0)
  }

  test("aggregateNumericSummaries: negative values for sum mode") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", -5.0)
    updateSketch.update("test2", -2.0)
    updateSketch.update("test3", -8.0)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Sum,
      it => it.getSummary.getValue)

    assert(result == -15.0)
  }

  test("aggregateNumericSummaries: negative values for min mode") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", -5.0)
    updateSketch.update("test2", -2.0)
    updateSketch.update("test3", -8.0)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Min,
      it => it.getSummary.getValue)

    assert(result == -8.0)
  }

  test("aggregateNumericSummaries: negative values for max mode") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", -5.0)
    updateSketch.update("test2", -2.0)
    updateSketch.update("test3", -8.0)

    val compactSketch = updateSketch.compact()
    val result = TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Max,
      it => it.getSummary.getValue)

    assert(result == -2.0)
  }

  test("aggregateNumericSummaries: mixed positive and negative values") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", -5.0)
    updateSketch.update("test2", 10.0)
    updateSketch.update("test3", -3.0)
    updateSketch.update("test4", 7.0)

    val compactSketch = updateSketch.compact()

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Sum, it => it.getSummary.getValue) == 9.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Min, it => it.getSummary.getValue) == -5.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Max, it => it.getSummary.getValue) == 10.0)
  }

  test("aggregateNumericSummaries: zero values") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", 0.0)
    updateSketch.update("test2", 0.0)
    updateSketch.update("test3", 0.0)

    val compactSketch = updateSketch.compact()

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Sum, it => it.getSummary.getValue) == 0.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Min, it => it.getSummary.getValue) == 0.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Max, it => it.getSummary.getValue) == 0.0)

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.AlwaysOne, it => it.getSummary.getValue) == 3.0)
  }

  test("aggregateNumericSummaries: special Double values - Infinity") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", Double.PositiveInfinity)
    updateSketch.update("test2", 10.0)
    updateSketch.update("test3", 5.0)

    val compactSketch = updateSketch.compact()

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Sum, it => it.getSummary.getValue) == Double.PositiveInfinity
    )

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Max, it => it.getSummary.getValue) == Double.PositiveInfinity
    )

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Min, it => it.getSummary.getValue) == 5.0)
  }

  test("aggregateNumericSummaries: special Double values - NegativeInfinity") {
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()

    updateSketch.update("test1", Double.NegativeInfinity)
    updateSketch.update("test2", 10.0)
    updateSketch.update("test3", 5.0)

    val compactSketch = updateSketch.compact()

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Sum, it => it.getSummary.getValue) == Double.NegativeInfinity
    )

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(),
      TupleSummaryMode.Min, it => it.getSummary.getValue) == Double.NegativeInfinity
    )

    assert(TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      compactSketch.iterator(), TupleSummaryMode.Max, it => it.getSummary.getValue) == 10.0)
  }
}
