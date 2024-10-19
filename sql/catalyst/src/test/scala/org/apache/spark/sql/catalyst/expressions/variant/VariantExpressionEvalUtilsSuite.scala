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

package org.apache.spark.sql.catalyst.expressions.variant

import org.apache.spark.{SparkFunSuite, SparkThrowable}
import org.apache.spark.types.variant.VariantUtil._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class VariantExpressionEvalUtilsSuite extends SparkFunSuite {

  test("parseJson type coercion") {
    def check(json: String, expectedValue: Array[Byte], expectedMetadata: Array[Byte],
              allowDuplicateKeys: Boolean = false): Unit = {
      // parse_json
      val actual = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json),
        allowDuplicateKeys)
      // try_parse_json
      val tryActual = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json),
        allowDuplicateKeys, failOnError = false)
      val expected = new VariantVal(expectedValue, expectedMetadata)
      assert(actual === expected && tryActual === expected)
    }

    // Dictionary size is `0` for value 0. An empty dictionary contains one offset `0` for the
    // one-past-the-end position (i.e. the sum of all string lengths).
    val emptyMetadata = Array[Byte](VERSION, 0, 0)
    check("null", Array(primitiveHeader(NULL)), emptyMetadata)
    check("true", Array(primitiveHeader(TRUE)), emptyMetadata)
    check("false", Array(primitiveHeader(FALSE)), emptyMetadata)
    check("1", Array(primitiveHeader(INT1), 1), emptyMetadata)
    check("-1", Array(primitiveHeader(INT1), -1), emptyMetadata)
    check("127", Array(primitiveHeader(INT1), 127), emptyMetadata)
    check("128", Array(primitiveHeader(INT2), -128, 0), emptyMetadata)
    check("-32768", Array(primitiveHeader(INT2), 0, -128), emptyMetadata)
    check("-32769", Array(primitiveHeader(INT4), -1, 127, -1, -1), emptyMetadata)
    check("2147483647", Array(primitiveHeader(INT4), -1, -1, -1, 127), emptyMetadata)
    check("2147483648", Array(primitiveHeader(INT8), 0, 0, 0, -128, 0, 0, 0, 0), emptyMetadata)
    check("9223372036854775807",
      Array(primitiveHeader(INT8), -1, -1, -1, -1, -1, -1, -1, 127), emptyMetadata)
    check("-9223372036854775808",
      Array(primitiveHeader(INT8), 0, 0, 0, 0, 0, 0, 0, -128), emptyMetadata)
    check("9223372036854775808",
      Array(primitiveHeader(DECIMAL16), 0, 0, 0, 0, 0, 0, 0, 0, -128, 0, 0, 0, 0, 0, 0, 0, 0),
      emptyMetadata)
    check("1.0", Array(primitiveHeader(DECIMAL4), 1, 10, 0, 0, 0), emptyMetadata)
    check("1.01", Array(primitiveHeader(DECIMAL4), 2, 101, 0, 0, 0), emptyMetadata)
    check("99999.9999", Array(primitiveHeader(DECIMAL4), 4, -1, -55, -102, 59), emptyMetadata)
    check("99999.99999",
      Array(primitiveHeader(DECIMAL8), 5, -1, -29, 11, 84, 2, 0, 0, 0), emptyMetadata)
    check("0.000000001", Array(primitiveHeader(DECIMAL4), 9, 1, 0, 0, 0), emptyMetadata)
    check("0.0000000001",
      Array(primitiveHeader(DECIMAL8), 10, 1, 0, 0, 0, 0, 0, 0, 0), emptyMetadata)
    check("9" * 38,
      Array[Byte](primitiveHeader(DECIMAL16), 0) ++ BigInt("9" * 38).toByteArray.reverse,
      emptyMetadata)
    check("1" + "0" * 38,
      Array(primitiveHeader(DOUBLE)) ++
        BigInt(java.lang.Double.doubleToLongBits(1E38)).toByteArray.reverse,
      emptyMetadata)
    check("\"\"", Array(shortStrHeader(0)), emptyMetadata)
    check("\"abcd\"", Array(shortStrHeader(4), 'a', 'b', 'c', 'd'), emptyMetadata)
    check("\"" + ("x" * 63) + "\"",
      Array(shortStrHeader(63)) ++ Array.fill(63)('x'.toByte), emptyMetadata)
    check("\"" + ("y" * 64) + "\"",
      Array[Byte](primitiveHeader(LONG_STR), 64, 0, 0, 0) ++ Array.fill(64)('y'.toByte),
      emptyMetadata)
    check("{}", Array(objectHeader(false, 1, 1),
      /* size */ 0,
      /* offset list */ 0), emptyMetadata)
    check("[]", Array(arrayHeader(false, 1),
      /* size */ 0,
      /* offset list */ 0), emptyMetadata)
    check("""{"a": 1, "b": 2, "c": "3"}""", Array(objectHeader(false, 1, 1),
      /* size */ 3,
      /* id list */ 0, 1, 2,
      /* offset list */ 0, 2, 4, 6,
      /* field data */ primitiveHeader(INT1), 1, primitiveHeader(INT1), 2, shortStrHeader(1), '3'),
      Array(VERSION, 3, 0, 1, 2, 3, 'a', 'b', 'c'))
    check("""{"a": 1, "b": 2, "c": "3", "a": 4}""", Array(objectHeader(false, 1, 1),
      /* size */ 3,
      /* id list */ 0, 1, 2,
      /* offset list */ 4, 0, 2, 6,
      /* field data */ primitiveHeader(INT1), 2, shortStrHeader(1), '3', primitiveHeader(INT1), 4),
      Array(VERSION, 3, 0, 1, 2, 3, 'a', 'b', 'c'),
      allowDuplicateKeys = true)
    check("""{"z": 1, "y": 2, "x": "3"}""", Array(objectHeader(false, 1, 1),
      /* size */ 3,
      /* id list */ 2, 1, 0,
      /* offset list */ 4, 2, 0, 6,
      /* field data */ primitiveHeader(INT1), 1, primitiveHeader(INT1), 2, shortStrHeader(1), '3'),
      Array(VERSION, 3, 0, 1, 2, 3, 'z', 'y', 'x'))
    check("""[null, true, {"false" : 0}]""", Array(arrayHeader(false, 1),
      /* size */ 3,
      /* offset list */ 0, 1, 2, 9,
      /* element data */ primitiveHeader(NULL), primitiveHeader(TRUE), objectHeader(false, 1, 1),
      /* size */ 1,
      /* id list */ 0,
      /* offset list */ 0, 2,
      /* field data */ primitiveHeader(INT1), 0),
      Array(VERSION, 1, 0, 5, 'f', 'a', 'l', 's', 'e'))
  }

  test("parseJson negative") {
    def checkException(json: String, condition: String, parameters: Map[String, String]): Unit = {
      val try_parse_json_output = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json),
        allowDuplicateKeys = false, failOnError = false)
      checkError(
        exception = intercept[SparkThrowable] {
          VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json),
            allowDuplicateKeys = false)
        },
        condition = condition,
        parameters = parameters
      )
      assert(try_parse_json_output === null)
    }
    for (json <- Seq("", "[", "+1", "1a", """{"a": 1, "b": 2, "a": "3"}""")) {
      checkException(json, "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
        Map("badRecord" -> json, "failFastMode" -> "FAILFAST"))
    }
    for (json <- Seq("\"" + "a" * (16 * 1024 * 1024) + "\"",
      (0 to 4 * 1024 * 1024).mkString("[", ",", "]"))) {
      checkException(json, "VARIANT_SIZE_LIMIT",
        Map("sizeLimit" -> "16.0 MiB", "functionName" -> "`parse_json`"))
    }
  }

  test("isVariantNull") {
    def check(json: String, expected: Boolean): Unit = {
      if (json != null) {
        val parsedVariant = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json))
        val actual = VariantExpressionEvalUtils.isVariantNull(parsedVariant)
        assert(actual == expected)
      } else {
        val actual = VariantExpressionEvalUtils.isVariantNull(null)
        assert(actual == expected)
      }
    }

    // Primitive types
    check("null", expected = true)
    check(null, expected = false)
    check("0", expected = false)
    check("13", expected = false)
    check("-54", expected = false)
    check("2147483647", expected = false)
    check("2147483648", expected = false)
    check("238457328534848", expected = false)
    check("342.769", expected = false)
    check("true", expected = false)
    check("false", expected = false)
    check("false", expected = false)
    check("65.43", expected = false)
    check("\"" + "spark" * 100 + "\"", expected = false)
    // Short String
    check("\"\"", expected = false)
    check("\"null\"", expected = false)
    // Array
    check("[]", expected = false)
    check("[null, null]", expected = false)
    check("[{\"a\" : 13}, \"spark\"]", expected = false)
    // Object
    check("[{\"a\" : 13, \"b\" : null}]", expected = false)
  }
}
