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
        condition = "THETA_INVALID_LG_NOM_ENTRIES",
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
        condition = "THETA_INVALID_LG_NOM_ENTRIES",
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

  test("checkSummaryType: accepts valid summary types") {
    val validTypes = Seq(
      ThetaSketchUtils.SUMMARY_TYPE_DOUBLE,
      ThetaSketchUtils.SUMMARY_TYPE_INTEGER,
      ThetaSketchUtils.SUMMARY_TYPE_STRING)
    validTypes.foreach { summaryType =>
      // Should not throw any exception
      ThetaSketchUtils.checkSummaryType(summaryType, "test_function")
    }
  }

  test("checkSummaryType: throws exception for invalid summary types") {
    val invalidTypes = Seq("invalid", "float", "long", "boolean", "")
    invalidTypes.foreach { summaryType =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          ThetaSketchUtils.checkSummaryType(summaryType, "test_function")
        },
        condition = "TUPLE_INVALID_SKETCH_SUMMARY_TYPE",
        parameters = Map(
          "function" -> "`test_function`",
          "summaryType" -> summaryType,
          "validTypes" -> ThetaSketchUtils.VALID_SUMMARY_TYPES.mkString(", ")))
    }
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

  test("heapifyTupleSketch: successfully deserializes valid tuple sketch bytes") {
    // Create a valid tuple sketch and get its bytes
    val summaryFactory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum)
    val updateSketch = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](summaryFactory)
      .build()
    
    updateSketch.update("test1", 1.0)
    updateSketch.update("test2", 2.0)
    updateSketch.update("test3", 3.0)
    
    val compactSketch = updateSketch.compact()
    val validBytes = compactSketch.toByteArray

    // Test that heapifyTupleSketch can successfully deserialize the valid bytes
    val heapifiedSketch = ThetaSketchUtils.heapifyTupleSketch(
      validBytes,
      ThetaSketchUtils.SUMMARY_TYPE_DOUBLE,
      "test_function")

    assert(heapifiedSketch != null)
    assert(heapifiedSketch.getEstimate == compactSketch.getEstimate)
    assert(heapifiedSketch.getRetainedEntries == compactSketch.getRetainedEntries)
  }

  test("heapifyTupleSketch: throws exception for invalid bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    checkError(
      exception = intercept[SparkRuntimeException] {
        ThetaSketchUtils.heapifyTupleSketch(
          invalidBytes,
          ThetaSketchUtils.SUMMARY_TYPE_DOUBLE,
          "test_function")
      },
      condition = "TUPLE_INVALID_INPUT_SKETCH_BUFFER",
      parameters = Map("function" -> "`test_function`"))
  }

  test("getDoubleSummaryMode: converts mode strings to DoubleSummary.Mode enum") {
    assert(ThetaSketchUtils.getDoubleSummaryMode(ThetaSketchUtils.MODE_SUM) ==
      DoubleSummary.Mode.Sum)
    assert(ThetaSketchUtils.getDoubleSummaryMode(ThetaSketchUtils.MODE_MIN) ==
      DoubleSummary.Mode.Min)
    assert(ThetaSketchUtils.getDoubleSummaryMode(ThetaSketchUtils.MODE_MAX) ==
      DoubleSummary.Mode.Max)
    assert(ThetaSketchUtils.getDoubleSummaryMode(ThetaSketchUtils.MODE_ALWAYSONE) ==
      DoubleSummary.Mode.AlwaysOne)
  }

  test("getSummaryFactory: creates appropriate factory for each summary type") {
    // Test double summary factory
    val doubleFactory = ThetaSketchUtils.getSummaryFactory(
      ThetaSketchUtils.SUMMARY_TYPE_DOUBLE,
      ThetaSketchUtils.MODE_SUM)
    assert(doubleFactory != null)

    // Test integer summary factory
    val integerFactory = ThetaSketchUtils.getSummaryFactory(
      ThetaSketchUtils.SUMMARY_TYPE_INTEGER,
      ThetaSketchUtils.MODE_SUM)
    assert(integerFactory != null)

    // Test string summary factory
    val stringFactory = ThetaSketchUtils.getSummaryFactory(
      ThetaSketchUtils.SUMMARY_TYPE_STRING,
      ThetaSketchUtils.MODE_SUM)
    assert(stringFactory != null)
  }

  test("getSummarySetOperations: creates appropriate operations for each summary type") {
    // Test double summary set operations
    val doubleOps = ThetaSketchUtils.getSummarySetOperations(
      ThetaSketchUtils.SUMMARY_TYPE_DOUBLE,
      ThetaSketchUtils.MODE_SUM)
    assert(doubleOps != null)

    // Test integer summary set operations
    val integerOps = ThetaSketchUtils.getSummarySetOperations(
      ThetaSketchUtils.SUMMARY_TYPE_INTEGER,
      ThetaSketchUtils.MODE_SUM)
    assert(integerOps != null)

    // Test string summary set operations
    val stringOps = ThetaSketchUtils.getSummarySetOperations(
      ThetaSketchUtils.SUMMARY_TYPE_STRING,
      ThetaSketchUtils.MODE_SUM)
    assert(stringOps != null)
  }

  test("getSummaryDeserializer: creates appropriate deserializer for each summary type") {
    // Test double summary deserializer
    val doubleDeserializer = ThetaSketchUtils.getSummaryDeserializer(
      ThetaSketchUtils.SUMMARY_TYPE_DOUBLE)
    assert(doubleDeserializer != null)

    // Test integer summary deserializer
    val integerDeserializer = ThetaSketchUtils.getSummaryDeserializer(
      ThetaSketchUtils.SUMMARY_TYPE_INTEGER)
    assert(integerDeserializer != null)

    // Test string summary deserializer
    val stringDeserializer = ThetaSketchUtils.getSummaryDeserializer(
      ThetaSketchUtils.SUMMARY_TYPE_STRING)
    assert(stringDeserializer != null)
  }
}
