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

package org.apache.spark.sql.catalyst.fwf

import org.apache.spark.{
  SparkException, SparkFunSuite, SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.util.{DropMalformedMode, FailFastMode, PermissiveMode}

class FixedWidthOptionsSuite extends SparkFunSuite {

  private def options(parameters: (String, String)*): FixedWidthOptions =
    new FixedWidthOptions(Map(parameters: _*), "UTC")

  test("neither colspecs nor widths given defaults to inference") {
    assert(options().colSpecs === InferFwfColSpecs)
  }

  test("colspecs=\"infer\" (case-insensitive) resolves to inference") {
    assert(options("colspecs" -> "INFER").colSpecs === InferFwfColSpecs)
  }

  test("widths are converted to contiguous colspecs") {
    assert(options("widths" -> "5,5,10").colSpecs ===
      ExplicitFwfColSpecs(Seq((0, 5), (5, 10), (10, 20))))
  }

  test("explicit colspecs are parsed as half-open intervals") {
    assert(options("colspecs" -> "0-5,5-10,10-20").colSpecs ===
      ExplicitFwfColSpecs(Seq((0, 5), (5, 10), (10, 20))))
  }

  test("giving both explicit colspecs and widths is rejected") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("colspecs" -> "0-5,5-10", "widths" -> "5,5")
      },
      condition = "INVALID_FIXED_WIDTH_COLSPECS.CONFLICTING_OPTIONS",
      parameters = Map.empty)
  }

  test("widths alongside colspecs=\"infer\" is allowed -- widths simply wins") {
    assert(options("colspecs" -> "infer", "widths" -> "5,5").colSpecs ===
      ExplicitFwfColSpecs(Seq((0, 5), (5, 10))))
  }

  test("malformed widths raise a clear error") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("widths" -> "5,abc")
      },
      condition = "INVALID_FIXED_WIDTH_COLSPECS.MALFORMED_WIDTHS",
      parameters = Map("value" -> "5,abc"))
  }

  test("malformed colspecs raise a clear error") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("colspecs" -> "0-5,notapair")
      },
      condition = "INVALID_FIXED_WIDTH_COLSPECS.MALFORMED_COLSPECS",
      parameters = Map("value" -> "0-5,notapair"))
  }

  test("a non-integer inferNrows raises a clear error") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        options("inferNrows" -> "abc")
      },
      condition = "_LEGACY_ERROR_TEMP_2146",
      parameters = Map("paramName" -> "inferNrows", "value" -> "abc"))
  }

  test("a non-boolean header raises a clear error") {
    checkError(
      exception = intercept[SparkException] {
        options("header" -> "yes")
      },
      condition = "_LEGACY_ERROR_TEMP_2147",
      parameters = Map("paramName" -> "header"))
  }

  test("default option values") {
    val opts = options()
    assert(opts.inferNrows === 100)
    assert(!opts.header)
    assert(opts.comment === None)
    assert(opts.delimiter === None)
    assert(opts.nullValue === "")
    assert(opts.parseMode === PermissiveMode)
    assert(opts.skipRows === Set.empty)
  }

  test("skipRows as a single integer skips that many rows from the start") {
    assert(options("skipRows" -> "3").skipRows === Set(0, 1, 2))
    assert(options("skipRows" -> "0").skipRows === Set.empty)
  }

  test("skipRows as a comma-separated list skips those specific row numbers") {
    assert(options("skipRows" -> "0,2,5").skipRows === Set(0, 2, 5))
  }

  test("a negative skipRows integer is rejected") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("skipRows" -> "-1")
      },
      condition = "INVALID_FIXED_WIDTH_SKIP_ROWS.NEGATIVE",
      parameters = Map("value" -> "-1"))
  }

  test("a malformed skipRows value is rejected") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("skipRows" -> "a,b")
      },
      condition = "INVALID_FIXED_WIDTH_SKIP_ROWS.MALFORMED",
      parameters = Map("value" -> "a,b"))
  }

  test("mode is parsed") {
    assert(options("mode" -> "DROPMALFORMED").parseMode === DropMalformedMode)
    assert(options("mode" -> "FAILFAST").parseMode === FailFastMode)
  }

  test("toCSVOptionsShim forwards shared fields and forces schema inference on") {
    val shim = options("nullValue" -> "NA", "dateFormat" -> "yyyy/MM/dd").toCSVOptionsShim
    assert(shim.nullValue === "NA")
    assert(shim.dateFormatInRead.contains("yyyy/MM/dd"))
    assert(shim.inferSchemaFlag)
  }
}
