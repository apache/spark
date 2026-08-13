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

  test("default is infer") {
    assert(options().colSpecs === InferFwfColSpecs)
  }

  test("colspecs infer is case-insensitive") {
    assert(options("colspecs" -> "INFER").colSpecs === InferFwfColSpecs)
  }

  test("widths to colspecs") {
    assert(options("widths" -> "5,5,10").colSpecs ===
      ExplicitFwfColSpecs(Seq((0, 5), (5, 10), (10, 20))))
  }

  test("parse colspecs") {
    assert(options("colspecs" -> "0-5,5-10,10-20").colSpecs ===
      ExplicitFwfColSpecs(Seq((0, 5), (5, 10), (10, 20))))
  }

  test("conflicting colspecs and widths") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("colspecs" -> "0-5,5-10", "widths" -> "5,5")
      },
      condition = "INVALID_FIXED_WIDTH_COLSPECS.CONFLICTING_OPTIONS",
      parameters = Map.empty)
  }

  test("widths wins over colspecs infer") {
    assert(options("colspecs" -> "infer", "widths" -> "5,5").colSpecs ===
      ExplicitFwfColSpecs(Seq((0, 5), (5, 10))))
  }

  test("malformed widths") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("widths" -> "5,abc")
      },
      condition = "INVALID_FIXED_WIDTH_COLSPECS.MALFORMED_WIDTHS",
      parameters = Map("value" -> "5,abc"))
  }

  test("malformed colspecs") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("colspecs" -> "0-5,notapair")
      },
      condition = "INVALID_FIXED_WIDTH_COLSPECS.MALFORMED_COLSPECS",
      parameters = Map("value" -> "0-5,notapair"))
  }

  test("non-integer inferNrows") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        options("inferNrows" -> "abc")
      },
      condition = "_LEGACY_ERROR_TEMP_2146",
      parameters = Map("paramName" -> "inferNrows", "value" -> "abc"))
  }

  test("non-boolean header") {
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

  test("skipRows as an integer") {
    assert(options("skipRows" -> "3").skipRows === Set(0, 1, 2))
    assert(options("skipRows" -> "0").skipRows === Set.empty)
  }

  test("skipRows as a list") {
    assert(options("skipRows" -> "0,2,5").skipRows === Set(0, 2, 5))
  }

  test("negative skipRows") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options("skipRows" -> "-1")
      },
      condition = "INVALID_FIXED_WIDTH_SKIP_ROWS.NEGATIVE",
      parameters = Map("value" -> "-1"))
  }

  test("malformed skipRows") {
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

  test("toCSVOptionsShim") {
    val shim = options("nullValue" -> "NA", "dateFormat" -> "yyyy/MM/dd").toCSVOptionsShim
    assert(shim.nullValue === "NA")
    assert(shim.dateFormatInRead.contains("yyyy/MM/dd"))
    assert(shim.inferSchemaFlag)
  }
}
