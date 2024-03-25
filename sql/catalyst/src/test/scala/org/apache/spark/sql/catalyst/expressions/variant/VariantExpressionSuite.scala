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

import org.apache.spark.{SparkException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.types.variant.VariantUtil._
import org.apache.spark.unsafe.types.VariantVal

class VariantExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  // Zero-extend each byte in the array with the appropriate number of bytes.
  // Used to manually construct variant binary values with a given offset size.
  // E.g. padded(Array(1,2,3), 3) will produce Array(1,0,0,2,0,0,3,0,0).
  private def padded(a: Array[Byte], size: Int): Array[Byte] = {
    a.flatMap { b =>
      val padding = List.fill(size - 1)(0.toByte)
      b :: padding
    }
  }

  test("parse_json") {
    def check(json: String, expectedValue: Array[Byte], expectedMetadata: Array[Byte]): Unit = {
      checkEvaluation(ParseJson(Literal(json)), new VariantVal(expectedValue, expectedMetadata))
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

  test("parse_json negative") {
    for (json <- Seq("", "[", "+1", "1a", """{"a": 1, "b": 2, "a": "3"}""")) {
      checkExceptionInExpression[SparkException](ParseJson(Literal(json)),
        "Malformed records are detected in record parsing")
    }
    for (json <- Seq("\"" + "a" * (16 * 1024 * 1024) + "\"",
      (0 to 4 * 1024 * 1024).mkString("[", ",", "]"))) {
      checkExceptionInExpression[SparkRuntimeException](ParseJson(Literal(json)),
        "Cannot build variant bigger than 16.0 MiB")
    }
  }

  test("round-trip") {
    def check(input: String, output: String = null): Unit = {
      checkEvaluation(
        StructsToJson(Map.empty, ParseJson(Literal(input))),
        if (output != null) output else input
      )
    }

    check("null")
    check("true")
    check("false")
    check("-1")
    check("1.0E10")
    check("\"\"")
    check("\"" + ("a" * 63) + "\"")
    check("\"" + ("b" * 64) + "\"")
    // scalastyle:off nonascii
    check("\"" + ("你好，世界" * 20) + "\"")
    // scalastyle:on nonascii
    check("[]")
    check("{}")
    // scalastyle:off nonascii
    check(
      "[null, true,   false,-1, 1e10, \"\\uD83D\\uDE05\", [ ], { } ]",
      "[null,true,false,-1,1.0E10,\"😅\",[],{}]"
    )
    // scalastyle:on nonascii
    check("[0.0, 1.00, 1.10, 1.23]", "[0,1,1.1,1.23]")
  }

  test("to_json with nested variant") {
    checkEvaluation(
      StructsToJson(Map.empty, CreateArray(Seq(ParseJson(Literal("{}")),
        ParseJson(Literal("\"\"")),
        ParseJson(Literal("[1, 2, 3]"))))),
      "[{},\"\",[1,2,3]]"
    )
    checkEvaluation(
      StructsToJson(Map.empty, CreateNamedStruct(Seq(
        Literal("a"), ParseJson(Literal("""{ "x": 1, "y": null, "z": "str" }""")),
        Literal("b"), ParseJson(Literal("[[]]")),
        Literal("c"), ParseJson(Literal("false"))))),
      """{"a":{"x":1,"y":null,"z":"str"},"b":[[]],"c":false}"""
    )
  }

  test("to_json malformed") {
    def check(value: Array[Byte], metadata: Array[Byte],
              errorClass: String = "MALFORMED_VARIANT"): Unit = {
      checkErrorInExpression[SparkRuntimeException](
        ResolveTimeZone.resolveTimeZones(
          StructsToJson(Map.empty, Literal(new VariantVal(value, metadata)))),
        errorClass
      )
    }

    val emptyMetadata = Array[Byte](VERSION, 0, 0)
    // INT8 only has 7 byte content.
    check(Array(primitiveHeader(INT8), 0, 0, 0, 0, 0, 0, 0), emptyMetadata)
    // DECIMAL16 only has 15 byte content.
    check(Array(primitiveHeader(DECIMAL16)) ++ Array.fill(16)(0.toByte), emptyMetadata)
    // Short string content too short.
    check(Array(shortStrHeader(2), 'x'), emptyMetadata)
    // Long string length too short (requires 4 bytes).
    check(Array(primitiveHeader(LONG_STR), 0, 0, 0), emptyMetadata)
    // Long string content too short.
    check(Array(primitiveHeader(LONG_STR), 1, 0, 0, 0), emptyMetadata)
    // Size is 1 but no content.
    check(Array(arrayHeader(false, 1),
      /* size */ 1,
      /* offset list */ 0), emptyMetadata)
    // Requires 4-byte size is but the actual size only has one byte.
    check(Array(arrayHeader(true, 1),
      /* size */ 0,
      /* offset list */ 0), emptyMetadata)
    // Offset out of bound.
    check(Array(arrayHeader(false, 1),
      /* size */ 1,
      /* offset list */ 1, 1), emptyMetadata)
    // Id out of bound.
    check(Array(objectHeader(false, 1, 1),
      /* size */ 1,
      /* id list */ 0,
      /* offset list */ 0, 2,
      /* field data */ primitiveHeader(INT1), 1), emptyMetadata)
    // Variant version is not 1.
    check(Array(primitiveHeader(INT1), 0), Array[Byte](3, 0, 0))
    check(Array(primitiveHeader(INT1), 0), Array[Byte](2, 0, 0))

    // Construct binary values that are over 1 << 24 bytes, but otherwise valid.
    val bigVersion = Array[Byte]((VERSION | (3 << 6)).toByte)
    val a = Array.fill(1 << 24)('a'.toByte)
    val hugeMetadata = bigVersion ++ Array[Byte](2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1) ++
      a ++ Array[Byte]('b')
    check(Array(primitiveHeader(TRUE)), hugeMetadata, "VARIANT_CONSTRUCTOR_SIZE_LIMIT")

    // The keys are 'aaa....' and 'b'. Values are "yyy..." and 'true'.
    val y = Array.fill(1 << 24)('y'.toByte)
    val hugeObject = Array[Byte](objectHeader(true, 4, 4)) ++
      /* size */ padded(Array(2), 4) ++
      /* id list */ padded(Array(0, 1), 4) ++
      // Second value starts at offset 5 + (1 << 24), which is `5001` little-endian. The last value
      // is 1 byte, so the one-past-the-end value is `6001`
      /* offset list */ Array[Byte](0, 0, 0, 0, 5, 0, 0, 1, 6, 0, 0, 1) ++
      /* field data */ Array[Byte](primitiveHeader(LONG_STR), 0, 0, 0, 1) ++ y ++ Array[Byte](
        primitiveHeader(TRUE)
      )

    val smallMetadata = bigVersion ++ Array[Byte](2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0) ++
      Array[Byte]('a', 'b')
    check(hugeObject, smallMetadata, "VARIANT_CONSTRUCTOR_SIZE_LIMIT")
    check(hugeObject, hugeMetadata, "VARIANT_CONSTRUCTOR_SIZE_LIMIT")
  }

  // Test valid forms of Variant that our writer would never produce.
  test("to_json valid input") {
    def check(expectedJson: String, value: Array[Byte], metadata: Array[Byte]): Unit = {
      checkEvaluation(
        StructsToJson(Map.empty, Literal(new VariantVal(value, metadata))),
        expectedJson
      )
    }
    // Some valid metadata formats. Check that they aren't rejected.
    // Sorted string bit is set, and can be ignored.
    val emptyMetadata2 = Array[Byte](VERSION | 1 << 4, 0, 0)
    // Bit 5 is not defined in the spec, and can be ignored.
    val emptyMetadata3 = Array[Byte](VERSION | 1 << 5, 0, 0)
    // Can specify 3 bytes per size/offset, even if they aren't needed.
    val header = (VERSION | (2 << 6)).toByte
    val emptyMetadata4 = Array[Byte](header, 0, 0, 0, 0, 0, 0)
    check("true", Array(primitiveHeader(TRUE)), emptyMetadata2)
    check("true", Array(primitiveHeader(TRUE)), emptyMetadata3)
    check("true", Array(primitiveHeader(TRUE)), emptyMetadata4)
  }

  // Test StructsToJson with manually constructed input that uses up to 4 bytes for offsets and
  // sizes.  We never produce 4-byte offsets, since they're only needed for >16 MiB values, which we
  // error out on, but the reader should be able to handle them if some other writer decides to use
  // them for smaller values.
  test("to_json with large offsets and sizes") {
    def check(expectedJson: String, value: Array[Byte], metadata: Array[Byte]): Unit = {
      checkEvaluation(
        StructsToJson(Map.empty, Literal(new VariantVal(value, metadata))),
        expectedJson
      )
    }

    for {
      offsetSize <- 1 to 4
      idSize <- 1 to 4
      metadataSize <- 1 to 4
      largeSize <- Seq(false, true)
    } {
      // Test array
      val version = Array[Byte]((VERSION | ((metadataSize - 1) << 6)).toByte)
      val emptyMetadata = version ++ padded(Array(0, 0), metadataSize)
      // Construct a binary with the given sizes. Regardless, to_json should produce the same
      // result.
      val arrayValue = Array[Byte](arrayHeader(largeSize, offsetSize)) ++
        /* size */ padded(Array(3), if (largeSize) 4 else 1) ++
        /* offset list */ padded(Array(0, 1, 4, 5), offsetSize) ++
        Array[Byte](/* values */ primitiveHeader(FALSE),
            primitiveHeader(INT2), 2, 1, primitiveHeader(NULL))
      check("[false,258,null]", arrayValue, emptyMetadata)

      // Test object
      val metadata = version ++
                     padded(Array(3, 0, 1, 2, 3), metadataSize) ++
                     Array[Byte]('a', 'b', 'c')
      val objectValue = Array[Byte](objectHeader(largeSize, idSize, offsetSize)) ++
        /* size */ padded(Array(3), if (largeSize) 4 else 1) ++
        /* id list */ padded(Array(0, 1, 2), idSize) ++
        /* offset list */ padded(Array(0, 2, 4, 6), offsetSize) ++
        /* field data */ Array[Byte](primitiveHeader(INT1), 1,
            primitiveHeader(INT1), 2, shortStrHeader(1), '3')

      check("""{"a":1,"b":2,"c":"3"}""", objectValue, metadata)
    }
  }

  test("to_json large binary") {
    def check(expectedJson: String, value: Array[Byte], metadata: Array[Byte]): Unit = {
      checkEvaluation(
        StructsToJson(Map.empty, Literal(new VariantVal(value, metadata))),
        expectedJson
      )
    }

    // Create a binary that uses the max 1 << 24 bytes for both metadata and value.
    val bigVersion = Array[Byte]((VERSION | (2 << 6)).toByte)
    // Create a single huge value, followed by a one-byte string. We'll have 1 header byte, plus 12
    // bytes for size and offsets, plus 1 byte for the final value, so the large value is 1 << 24 -
    // 14 bytes, or (-14, -1, -1) as a signed little-endian value.
    val aSize = (1 << 24) - 14
    val a = Array.fill(aSize)('a'.toByte)
    val hugeMetadata = bigVersion ++ Array[Byte](2, 0, 0, 0, 0, 0, -14, -1, -1, -13, -1, -1) ++
      a ++ Array[Byte]('b')
    // Validate metadata in isolation.
    check("true", Array(primitiveHeader(TRUE)), hugeMetadata)

    // The object will contain a large string, and the following bytes:
    // - object header and size: 1+4 bytes
    // - ID list: 6 bytes
    // - offset list: 9 bytes
    // - field headers and string length: 6 bytes
    // In order to get the full binary to 1 << 24, the large string is (1 << 24) - 26 bytes. As a
    // signed little-endian value, this is (-26, -1, -1).
    val ySize = (1 << 24) - 26
    val y = Array.fill(ySize)('y'.toByte)
    val hugeObject = Array[Byte](objectHeader(true, 3, 3)) ++
      /* size */ padded(Array(2), 4) ++
      /* id list */ padded(Array(0, 1), 3) ++
      // Second offset is (-26,-1,-1), plus 5 bytes for string header, so (-21,-1,-1)
      /* offset list */ Array[Byte](0, 0, 0, -21, -1, -1, -20, -1, -1) ++
      /* field data */ Array[Byte](primitiveHeader(LONG_STR), -26, -1, -1, 0) ++ y ++ Array[Byte](
        primitiveHeader(TRUE)
      )
    // Same as hugeObject, but with a short string.
    val smallObject = Array[Byte](objectHeader(false, 1, 1)) ++
      /* size */ Array[Byte](2) ++
      /* id list */ Array[Byte](0, 1) ++
      /* offset list */ Array[Byte](0, 6, 7) ++
      /* field data */ Array[Byte](primitiveHeader(LONG_STR), 1, 0, 0, 0, 'y',
          primitiveHeader(TRUE))
    val smallMetadata = bigVersion ++ Array[Byte](2, 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0) ++
      Array[Byte]('a', 'b')

    // Check all combinations of large/small value and metadata.
    val expectedResult1 =
      s"""{"${a.map(_.toChar).mkString}":"${y.map(_.toChar).mkString}","b":true}"""
    check(expectedResult1, hugeObject, hugeMetadata)
    val expectedResult2 =
      s"""{"${a.map(_.toChar).mkString}":"y","b":true}"""
    check(expectedResult2, smallObject, hugeMetadata)
    val expectedResult3 =
      s"""{"a":"${y.map(_.toChar).mkString}","b":true}"""
    check(expectedResult3, hugeObject, smallMetadata)
    val expectedResult4 =
      s"""{"a":"y","b":true}"""
    check(expectedResult4, smallObject, smallMetadata)
  }
}
