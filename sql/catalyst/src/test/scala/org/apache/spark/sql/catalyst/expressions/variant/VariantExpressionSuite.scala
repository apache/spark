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

import java.time.{Duration, LocalDateTime, Period, ZoneId, ZoneOffset}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.VariantUtil._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

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

  test("to_json malformed") {
    def check(
        value: Array[Byte],
        metadata: Array[Byte],
        condition: String = "MALFORMED_VARIANT"): Unit = {
      checkErrorInExpression[SparkRuntimeException](
        ResolveTimeZone.resolveTimeZones(
          StructsToJson(Map.empty, Literal(new VariantVal(value, metadata)))),
        condition
      )
    }

    val emptyMetadata = Array[Byte](VERSION, 0, 0)
    // INT8 only has 7 byte content.
    check(Array(primitiveHeader(INT8), 0, 0, 0, 0, 0, 0, 0), emptyMetadata)
    // DECIMAL16 only has 15 byte content.
    check(Array(primitiveHeader(DECIMAL16)) ++ Array.fill(16)(0.toByte), emptyMetadata)
    // 1e38 has a precision of 39. Even if it still fits into 16 bytes, it is not a valid decimal.
    check(Array[Byte](primitiveHeader(DECIMAL16), 0) ++
      BigDecimal(1e38).toBigInt.toByteArray.reverse, emptyMetadata)
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

  test("is_variant_null invalid input") {
    checkErrorInExpression[SparkRuntimeException](
      IsVariantNull(Literal(new VariantVal(Array(), Array(1, 2, 3)))),
      "MALFORMED_VARIANT"
    )
  }

  private def parseJson(input: String): VariantVal =
    VariantExpressionEvalUtils.parseJson(UTF8String.fromString(input))

  private def variantGet(input: String, path: String, dataType: DataType): VariantGet =
    VariantGet(Literal(parseJson(input)), Literal(path), dataType, failOnError = true)

  private def tryVariantGet(input: String, path: String, dataType: DataType): VariantGet =
    VariantGet(Literal(parseJson(input)), Literal(path), dataType, failOnError = false)

  private def testVariantGet(input: String, path: String, dataType: DataType, output: Any): Unit = {
    checkEvaluation(variantGet(input, path, dataType), output)
    checkEvaluation(
      VariantGet(variantGet(input, path, VariantType), Literal("$"), dataType, failOnError = true),
      output
    )
    checkEvaluation(tryVariantGet(input, path, dataType), output)
  }

  // If an individual element cannot be cast to the target type, `variant_get` will return an error
  // and `try_variant_get` will only set that element to be null.
  private def testInvalidVariantGet(
      input: String,
      path: String,
      dataType: DataType,
      parameters: Map[String, String] = null,
      tryOutput: Any = null): Unit = {
    checkErrorInExpression[SparkRuntimeException](
      variantGet(input, path, dataType),
      "INVALID_VARIANT_CAST",
      Option(parameters).getOrElse(
        Map("value" -> input, "dataType" -> ("\"" + dataType.sql + "\"")))
    )
    checkEvaluation(tryVariantGet(input, path, dataType), tryOutput)
  }

  test("variant_get cast") {
    // Source type is string.
    testVariantGet("\"true\"", "$", BooleanType, true)
    testVariantGet("\"false\"", "$", BooleanType, false)
    testVariantGet("\" t \"", "$", BooleanType, true)
    testInvalidVariantGet("\"true\"", "$", IntegerType)
    testVariantGet("\"1\"", "$", IntegerType, 1)
    testVariantGet("\"9223372036854775807\"", "$", LongType, 9223372036854775807L)
    testVariantGet("\"-0.0\"", "$", DoubleType, -0.0)
    testVariantGet("\"inf\"", "$", DoubleType, Double.PositiveInfinity)
    testVariantGet("\"-inf\"", "$", DoubleType, Double.NegativeInfinity)
    testVariantGet("\"nan\"", "$", DoubleType, Double.NaN)
    testVariantGet("\"12.34\"", "$", FloatType, 12.34f)
    testVariantGet("\"12.34\"", "$", DecimalType(9, 4), Decimal(12.34))
    testVariantGet("\"1970-01-01\"", "$", DateType, 0)
    testVariantGet("\"1970-03-01\"", "$", DateType, 59)

    // Source type is boolean.
    testVariantGet("true", "$", BooleanType, true)
    testVariantGet("false", "$", BooleanType, false)
    testVariantGet("true", "$", ByteType, 1.toByte)
    testVariantGet("true", "$", DoubleType, 1.0)
    testVariantGet("true", "$", DecimalType(18, 17), Decimal(1))
    testInvalidVariantGet("true", "$", DecimalType(18, 18))
    testVariantGet("false", "$", DecimalType(18, 18), Decimal(0))

    // Source type is integer.
    testVariantGet("1", "$", BooleanType, true)
    testVariantGet("0", "$", BooleanType, false)
    testInvalidVariantGet("1", "$", BinaryType)
    testVariantGet("127", "$", ByteType, 127.toByte)
    testInvalidVariantGet("128", "$", ByteType)
    testVariantGet("-32768", "$", ShortType, (-32768).toShort)
    testInvalidVariantGet("-32769", "$", ShortType)
    testVariantGet("2147483647", "$", IntegerType, 2147483647)
    testInvalidVariantGet("2147483648", "$", IntegerType)
    testVariantGet("9223372036854775807", "$", LongType, 9223372036854775807L)
    testVariantGet("-9223372036854775808", "$", LongType, -9223372036854775808L)
    testVariantGet("2147483647", "$", FloatType, 2147483647.0f)
    testVariantGet("2147483647", "$", DoubleType, 2147483647.0d)
    testVariantGet("1", "$", DecimalType(9, 4), Decimal(1))
    testVariantGet("99999999", "$", DecimalType(38, 30), Decimal(99999999))
    testInvalidVariantGet("100000000", "$", DecimalType(38, 30))
    testInvalidVariantGet("12345", "$", DecimalType(6, 3))
    testVariantGet("-1", "$", TimestampType, -1000000L)
    testVariantGet("9223372036854", "$", TimestampType, 9223372036854000000L)
    testInvalidVariantGet("9223372036855", "$", TimestampType)
    testInvalidVariantGet("0", "$", TimestampNTZType)

    // year-month interval corner + random cases
    Seq(0, 2147483647, -2147483648, 4398201, -213494932).foreach(input => {
      for (startField <- YearMonthIntervalType.YEAR to YearMonthIntervalType.MONTH) {
        for (endField <- startField to YearMonthIntervalType.MONTH) {
          val cleanInput = if (endField == 0) input / 12 else input
          // numeric source
          testVariantGet(
            cleanInput.toString,
            "$",
            YearMonthIntervalType(startField.toByte, endField.toByte),
            Cast(
              Literal(cleanInput, IntegerType),
              YearMonthIntervalType(startField.toByte, endField.toByte)
            ).eval()
          )
          // String source
          testVariantGet(
            "\"" + Cast(Cast(
              Literal(cleanInput, IntegerType),
              YearMonthIntervalType(startField.toByte, endField.toByte)
            ), StringType).eval().toString + "\"",
            "$",
            YearMonthIntervalType(startField.toByte, endField.toByte),
            Cast(
              Literal(cleanInput, IntegerType),
              YearMonthIntervalType(startField.toByte, endField.toByte)
            ).eval()
          )
        }
      }
    })

    // When a variant is being cast to interval, the none of the newly written code is used
    // (the cast to interval was simply enabled) and therefore, not all of the corner cases need
    // to be tested.

    // day-time interval corner cases. In the string source examples, the corner cases are cast to
    // interval followed by another cast to string followed by another cast to interval since
    // the cast from string to interval loses information.
    testVariantGet("9223372036854.775807", "$", DayTimeIntervalType(0, 3),
      Cast(Literal(Decimal("9223372036854.775807")), DayTimeIntervalType(0, 3)).eval()
    )
    testVariantGet(
      "\"" + Cast(Cast(Literal(Decimal("9223372036854.775807")), DayTimeIntervalType(0, 3)),
        StringType).eval().toString + "\"", "$", DayTimeIntervalType(0, 3),
      Cast(
        Cast(
          Cast(Literal(Decimal("9223372036854.775807")), DayTimeIntervalType(0, 3)),
          StringType
        ),
        DayTimeIntervalType(0, 3)
      ).eval()
    )
    testVariantGet("-153722867280.912930", "$", DayTimeIntervalType(1, 2),
      Cast(Literal(Decimal("-153722867280.912930")), DayTimeIntervalType(1, 2)).eval()
    )
    testVariantGet(
      "\"" + Cast(Cast(Literal(Decimal("-153722867280.912930")), DayTimeIntervalType(1, 2)),
        StringType).eval().toString + "\"", "$", DayTimeIntervalType(1, 2),
      Cast(
        Cast(
          Cast(Literal(Decimal("-153722867280.912930")), DayTimeIntervalType(1, 2)),
          StringType
        ),
        DayTimeIntervalType(1, 2)
      ).eval()
    )
    testVariantGet("-2562047788.015215", "$", DayTimeIntervalType(0, 1),
      Cast(Literal(Decimal("-2562047788.015215")), DayTimeIntervalType(0, 1)).eval()
    )
    testVariantGet(
      "\"" + Cast(Cast(Literal(Decimal("-2562047788.015215")), DayTimeIntervalType(0, 1)),
        StringType).eval().toString + "\"", "$", DayTimeIntervalType(0, 1),
      Cast(
        Cast(Cast(Literal(Decimal("-2562047788.015215")), DayTimeIntervalType(0, 1)), StringType),
        DayTimeIntervalType(0, 1)
      ).eval()
    )
    testVariantGet("-106751991.167300", "$", DayTimeIntervalType(0, 0),
      Cast(Literal(Decimal("-106751991.167300")), DayTimeIntervalType(0, 0)).eval()
    )
    testVariantGet(
      "\"" + Cast(Cast(Literal(Decimal("-106751991.167300")), DayTimeIntervalType(0, 0)),
        StringType).eval().toString + "\"", "$", DayTimeIntervalType(0, 0),
      Cast(
        Cast(Cast(Literal(Decimal("-106751991.167300")), DayTimeIntervalType(0, 0)), StringType),
        DayTimeIntervalType(0, 0)
      ).eval()
    )

    // Source type is double. Always use scientific notation to avoid decimal.
    testVariantGet("1E0", "$", BooleanType, true)
    testVariantGet("0E0", "$", BooleanType, false)
    testVariantGet("-0E0", "$", BooleanType, false)
    testVariantGet("127E0", "$", ByteType, 127.toByte)
    testInvalidVariantGet(
      "128E0",
      "$",
      ByteType,
      Map("value" -> "128.0", "dataType" -> "\"TINYINT\"")
    )
    testVariantGet("-9.223372036854776E18", "$", LongType, Long.MinValue)
    testInvalidVariantGet("-9.223372036854778E18", "$", LongType)
    testVariantGet("1E308", "$", FloatType, Float.PositiveInfinity)
    testVariantGet("12345E-4", "$", DecimalType(5, 2), Decimal(1.23))
    testVariantGet("9999999999E-2", "$", DecimalType(38, 30), Decimal(99999999.99))
    testInvalidVariantGet(
      "100000000E0",
      "$",
      DecimalType(38, 30),
      Map("value" -> "1.0E8", "dataType" -> "\"DECIMAL(38,30)\"")
    )
    testVariantGet("9223372036854.5E0", "$", TimestampType, 9223372036854500352L)
    testInvalidVariantGet(
      "9223372036855E0",
      "$",
      TimestampType,
      Map("value" -> "9.223372036855E12", "dataType" -> "\"TIMESTAMP\"")
    )

    // Source type is decimal.
    testVariantGet("1.0", "$", BooleanType, true)
    testVariantGet("0.0", "$", BooleanType, false)
    testVariantGet("-0.0", "$", BooleanType, false)
    testVariantGet("2147483647.999", "$", IntegerType, 2147483647)
    testInvalidVariantGet("9223372036854775808", "$", LongType)
    testVariantGet("-9223372036854775808.0", "$", LongType, -9223372036854775808L)
    testVariantGet("123.0", "$", DecimalType(6, 3), Decimal(123000, 6, 3))
    testVariantGet("1.14", "$", DecimalType(2, 1), Decimal(11, 2, 1))
    testVariantGet("1.15", "$", DecimalType(2, 1), Decimal(12, 2, 1))
    testVariantGet(
      "0.0000000009999999994",
      "$",
      DecimalType(18, 18),
      Decimal("0.000000000999999999")
    )
    testVariantGet("0.0000000009999999995", "$", DecimalType(18, 18), Decimal("0.000000001"))
    testInvalidVariantGet("9.5", "$", DecimalType(1, 0))
    testVariantGet("9999999999999999999.9999999999999999999", "$", FloatType, 1e19f)
    testVariantGet("9999999999999999999.9999999999999999999", "$", DoubleType, 1e19)
    testVariantGet(
      "9999999999999999999.9999999999999999999",
      "$",
      StringType,
      "9999999999999999999.9999999999999999999"
    )
    // Input doesn't fit into decimal, use double instead, which causes a loss of precision.
    testVariantGet("9999999999999999999.99999999999999999999", "$", StringType, "1.0E19")
    // Input fits into `decimal(38, 38)`.
    testVariantGet(
      "0.99999999999999999999999999999999999999",
      "$",
      DecimalType(38, 38),
      Decimal("0.99999999999999999999999999999999999999")
    )
    testVariantGet("1.10", "$", StringType, "1.1")
    testVariantGet("-1.00", "$", StringType, "-1")
    // Test Decimal(N, 0).
    testVariantGet("-100000000000000000000", "$", StringType, "-100000000000000000000")
    testVariantGet(
      "99999999999999999999000000000000000000",
      "$",
      StringType,
      "99999999999999999999000000000000000000"
    )

    // Source type is null.
    testVariantGet("null", "$", BooleanType, null)
    testVariantGet("null", "$", IntegerType, null)
    testVariantGet("null", "$", DoubleType, null)
    testVariantGet("null", "$", DecimalType(18, 9), null)
    testVariantGet("null", "$", TimestampType, null)
    testVariantGet("null", "$", DateType, null)
  }

  test("variant_get path extraction") {
    // Test case adapted from `JsonExpressionsSuite`.
    val json =
      """
        |{"store":{"fruit":[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}],
        |"basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],"book":[{"author":"Nigel Rees",
        |"title":"Sayings of the Century","category":"reference","price":8.95},
        |{"author":"Herman Melville","title":"Moby Dick","category":"fiction","price":8.99,
        |"isbn":"0-553-21311-3"},{"author":"J. R. R. Tolkien","title":"The Lord of the Rings",
        |"category":"fiction","reader":[{"age":25,"name":"bob"},{"age":26,"name":"jack"}],
        |"price":22.99,"isbn":"0-395-19395-8"}],"bicycle":{"price":19.95,"color":"red"}},
        |"email":"amy@only_for_json_udf_test.net","owner":"amy","zip code":"94025",
        |"fb:testid":"1234"}
        |""".stripMargin
    testVariantGet(json, "$.store.bicycle", StringType, """{"color":"red","price":19.95}""")
    checkEvaluation(
      VariantGet(
        tryVariantGet(json, "$.store.bicycle", VariantType),
        Literal("$"),
        StringType,
        failOnError = true
      ),
      """{"color":"red","price":19.95}"""
    )
    testVariantGet(json, "$.store.bicycle.color", StringType, "red")
    testVariantGet(json, "$.store.bicycle.price", DoubleType, 19.95)
    testVariantGet(
      json,
      "$.store.book",
      StringType,
      """[{"author":"Nigel Rees","category":"reference","price":8.95,"title":
        |"Sayings of the Century"},{"author":"Herman Melville","category":"fiction","isbn":
        |"0-553-21311-3","price":8.99,"title":"Moby Dick"},{"author":"J. R. R. Tolkien","category":
        |"fiction","isbn":"0-395-19395-8","price":22.99,"reader":[{"age":25,"name":"bob"},{"age":26,
        |"name":"jack"}],"title":"The Lord of the Rings"}]""".stripMargin.replace("\n", "")
    )
    testVariantGet(
      json,
      "$.store.book[0]",
      StringType,
      """{"author":"Nigel Rees","category":"reference","price":8.95,"title":
        |"Sayings of the Century"}""".stripMargin.replace("\n", "")
    )
    testVariantGet(json, "$.store.book[0].category", StringType, "reference")
    testVariantGet(json, "$.store.book[1].price", DoubleType, 8.99)
    testVariantGet(json, "$.store.book[2].reader[0].name", StringType, "bob")
    testVariantGet(json, "$.store.book[2].reader[1].age", IntegerType, 26)
    testVariantGet(json, "$.store.basket[0][1]", IntegerType, 2)
    testVariantGet(json, "$.store.basket[0][2]", StringType, """{"a":"x","b":"y"}""")
    testVariantGet(json, "$.zip code", IntegerType, 94025)
    testVariantGet(json, "$.fb:testid", IntegerType, 1234)
    testVariantGet(
      json,
      "$.store.fruit",
      DataType.fromDDL("array<struct<weight int, type string>>"),
      Array(Row(8, "apple"), Row(9, "pear"))
    )
    testVariantGet(
      json,
      "$.store.book[0]",
      DataType.fromDDL("struct<author string, title string, category string, price decimal(4, 2)>"),
      Row("Nigel Rees", "Sayings of the Century", "reference", Decimal(8.95))
    )
  }

  test("variant_get negative") {
    testVariantGet("""{"a": 1}""", "$[0]", IntegerType, null)
    testVariantGet("""{"a": 1}""", "$.A", IntegerType, null)
    testVariantGet("[1]", "$.a", IntegerType, null)
    testVariantGet("[1]", "$[1]", IntegerType, null)
    testVariantGet("1", "$.a", IntegerType, null)
    testVariantGet("1", "$[0]", IntegerType, null)
    testInvalidVariantGet(
      """{"a": 1}""",
      "$",
      IntegerType,
      Map("value" -> "{\"a\":1}", "dataType" -> "\"INT\"")
    )
    testInvalidVariantGet("[1]", "$", IntegerType)
  }

  test("variant_get large") {
    val numKeys = 256

    var json = (0 until numKeys).map(_.toString).mkString("[", ",", "]")
    for (i <- 0 until numKeys) {
      testVariantGet(json, "$[" + i + "]", IntegerType, i)
    }
    testVariantGet(json, "$[" + numKeys + "]", IntegerType, null)

    json = (0 until numKeys).map(i => s""""$i": $i""").mkString("{", ",", "}")
    for (i <- 0 until numKeys) {
      testVariantGet(json, "$." + i, IntegerType, i)
    }
    testVariantGet(json, "$." + numKeys, IntegerType, null)
  }

  test("variant_get timestamp") {
    DateTimeTestUtils.outstandingZoneIds.foreach { zid =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId) {
        def toMicros(time: LocalDateTime, zoneId: ZoneId): Long = {
          val instant = time.atZone(zoneId).toInstant
          instant.getEpochSecond * 1000000L + instant.getNano / 1000L
        }

        val input = "\"2026-04-05 5:16:07\""
        val expected = LocalDateTime.of(2026, 4, 5, 5, 16, 7, 0)
        testVariantGet(input, "$", TimestampType, toMicros(expected, zid))
        testVariantGet(input, "$", TimestampNTZType, toMicros(expected, ZoneOffset.UTC))
      }
    }
  }

  test("variant_get overflow") {
    for (ansi <- Seq(false, true)) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
        // `variant_get` is not affected by the ANSI flag. It doesn't have the LEGACY mode.
        testInvalidVariantGet(
          """{"a": 2147483648}""",
          "$.a",
          IntegerType,
          Map("value" -> "2147483648", "dataType" -> "\"INT\"")
        )
      }
    }
  }

  test("variant_get nested") {
    testVariantGet("null", "$", DataType.fromDDL("a int"), null)
    testVariantGet("{}", "$", DataType.fromDDL("a int"), Row(null))
    testVariantGet("""{"a": 1}""", "$", DataType.fromDDL("a int"), Row(1))
    testInvalidVariantGet("1", "$", DataType.fromDDL("a int"))
    testVariantGet("""{"a": 1, "b": "2"}""", "$", DataType.fromDDL("a int, b string"), Row(1, "2"))
    testVariantGet("""{"a": 1, "b": "2"}""", "$", DataType.fromDDL("a string, b int"), Row("1", 2))
    testVariantGet("""{"b": "2", "a": 1}""", "$", DataType.fromDDL("a string, b int"), Row("1", 2))
    testVariantGet(
      """{"a": 1, "d": 2, "c": 3}""",
      "$",
      DataType.fromDDL("a int, b int, c int"),
      Row(1, null, 3)
    )
    testInvalidVariantGet(
      """{"a": 1, "b": "2"}""",
      "$",
      DataType.fromDDL("a int, b boolean"),
      Map("value" -> "\"2\"", "dataType" -> "\"BOOLEAN\""),
      Row(1, null)
    )

    testVariantGet("null", "$", DataType.fromDDL("array<int>"), null)
    testVariantGet("[]", "$", DataType.fromDDL("array<int>"), Array())
    testInvalidVariantGet("{}", "$", DataType.fromDDL("array<int>"))
    testVariantGet(
      """[1, 2, 3, null, "4", 5.0]""",
      "$",
      DataType.fromDDL("array<int>"),
      Array(1, 2, 3, null, 4, 5)
    )
    testVariantGet(
      """[1, 2, 3, null, "4", 5.0]""",
      "$",
      DataType.fromDDL("array<string>"),
      Array("1", "2", "3", null, "4", "5")
    )
    testVariantGet(
      """[[1], [2, 3], [4, 5, 6], [7, 8, 9, 10]]""",
      "$",
      DataType.fromDDL("array<array<int>>"),
      Array(Array(1), Array(2, 3), Array(4, 5, 6), Array(7, 8, 9, 10))
    )
    testInvalidVariantGet(
      """[1, 2, 3, "hello"]""",
      "$",
      DataType.fromDDL("array<int>"),
      Map("value" -> "\"hello\"", "dataType" -> "\"INT\""),
      Array(1, 2, 3, null)
    )

    testVariantGet("null", "$", DataType.fromDDL("map<string, int>"), null)
    testVariantGet("{}", "$", DataType.fromDDL("map<string, int>"), Map())
    testInvalidVariantGet("[]", "$", DataType.fromDDL("map<string, int>"))
    testVariantGet(
      """{"a": 1, "b": "2", "c": null}""",
      "$",
      DataType.fromDDL("map<string, int>"),
      Map("a" -> 1, "b" -> 2, "c" -> null)
    )
    testVariantGet(
      """{"a": {}, "b": {"c": "d"}, "e": {"f": "g"}}""",
      "$",
      DataType.fromDDL("map<string, map<string, string>>"),
      Map("a" -> Map(), "b" -> Map("c" -> "d"), "e" -> Map("f" -> "g"))
    )
    testInvalidVariantGet(
      """{"a": 1, "b": "2", "c": {}}""",
      "$",
      DataType.fromDDL("map<string, int>"),
      Map("value" -> "{}", "dataType" -> "\"INT\""),
      Map("a" -> 1, "b" -> 2, "c" -> null)
    )

    testVariantGet(
      """[{"a": 1}, {"b": 2}, null, {}]""",
      "$",
      DataType.fromDDL("array<struct<a int, b int>>"),
      Array(Row(1, null), Row(null, 2), null, Row(null, null))
    )
    testVariantGet(
      """[{"a": 1}, {"b": 2}, null, {}]""",
      "$",
      DataType.fromDDL("array<map<string, int>>"),
      Array(Map("a" -> 1), Map("b" -> 2), null, Map())
    )
  }

  test("variant_get path") {
    def checkInvalidPath(path: String): Unit = {
      checkErrorInExpression[SparkRuntimeException](
        variantGet("0", path, IntegerType),
        "INVALID_VARIANT_GET_PATH",
        Map("path" -> path, "functionName" -> "`variant_get`")
      )
    }

    testVariantGet("""{"1": {"2": {"3": [4]}}}""", "$.1.2.3[0]", IntegerType, 4)
    testVariantGet("""{"1": {"2": {"3": [4]}}}""", "$.1.2.3['0']", IntegerType, null)
    // scalastyle:off nonascii
    testVariantGet("""{"你好": {"世界": "hello"}}""", """$['你好']["世界"]""", StringType, "hello")
    // scalastyle:on nonascii

    checkInvalidPath("")
    checkInvalidPath(".a")
    checkInvalidPath("$1")
    checkInvalidPath("$[-1]")
    checkInvalidPath("""$['"]""")
  }

  test("cast from variant") {
    // We do not test too many type combinations, as the cast implementation is mostly the same as
    // variant_get.

    def checkCast(input: Any, dataType: DataType, output: Any): Unit = {
      for (mode <- Seq(EvalMode.LEGACY, EvalMode.ANSI, EvalMode.TRY)) {
        checkEvaluation(Cast(Literal(input), dataType, evalMode = mode), output)
      }
    }

    def checkInvalidCast(input: Any, dataType: DataType, tryOutput: Any): Unit = {
      // Casting from variant is not affected by the ANSI flag.
      for (mode <- Seq(EvalMode.LEGACY, EvalMode.ANSI)) {
        checkExceptionInExpression[SparkRuntimeException](
          Cast(Literal(input), dataType, evalMode = mode),
          "INVALID_VARIANT_CAST"
        )
      }
      checkEvaluation(Cast(Literal(input), dataType, evalMode = EvalMode.TRY), tryOutput)
    }

    checkCast(parseJson("1"), StringType, "1")
    // Other to-string casts never produce NULL when the input is not NULL, but variant-to-string
    // cast can produce NULL when the input is a variant null (not NULL).
    checkCast(parseJson("null"), StringType, null)
    checkCast(parseJson("\"1\""), IntegerType, 1)

    checkInvalidCast(parseJson("2147483648"), IntegerType, null)
    checkInvalidCast(parseJson("[2147483648, 1]"), ArrayType(IntegerType), Array(null, 1))

    checkCast(Array(null, parseJson("true")), ArrayType(BooleanType), Array(null, true))
    checkCast(
      Array(null, parseJson("false"), parseJson("null")),
      ArrayType(StringType),
      Array(null, "false", null)
    )
    checkCast(Array(parseJson("[1]")), ArrayType(ArrayType(IntegerType)), Array(Array(1)))
    checkInvalidCast(
      Array(parseJson("\"hello\""), null, parseJson("\"1\"")),
      ArrayType(IntegerType),
      Array(null, null, 1)
    )
  }

  test("atomic types that are not produced by parse_json") {
    // Dictionary size is `0` for value 0. An empty dictionary contains one offset `0` for the
    // one-past-the-end position (i.e. the sum of all string lengths).
    val emptyMetadata = Array[Byte](VERSION, 0, 0)

    def checkToJson(value: Array[Byte], expected: String): Unit = {
      val input = Literal(new VariantVal(value, emptyMetadata))
      checkEvaluation(StructsToJson(Map.empty, input), expected)
    }

    def checkToJsonFail(value: Array[Byte], id: Int): Unit = {
      val input = Literal(new VariantVal(value, emptyMetadata))
      checkErrorInExpression[SparkRuntimeException](
        ResolveTimeZone.resolveTimeZones(StructsToJson(Map.empty, input)),
        "UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT", Map("id" -> id.toString)
      )
    }

    def checkCast(value: Array[Byte], dataType: DataType, expected: Any): Unit = {
      val input = Literal(new VariantVal(value, emptyMetadata))
      checkEvaluation(Cast(input, dataType, evalMode = EvalMode.ANSI), expected)
    }

    checkToJson(Array(primitiveHeader(DATE), 0, 0, 0, 0), "\"1970-01-01\"")
    checkToJson(Array(primitiveHeader(DATE), -1, -1, -1, 127), "\"+5881580-07-11\"")
    checkToJson(Array(primitiveHeader(DATE), 0, 0, 0, -128), "\"-5877641-06-23\"")
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      checkCast(Array(primitiveHeader(DATE), 0, 0, 0, 0), TimestampType, 0L)
      checkCast(Array(primitiveHeader(DATE), 1, 0, 0, 0), TimestampType, MICROS_PER_DAY)
    }
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      checkCast(Array(primitiveHeader(DATE), 0, 0, 0, 0), TimestampType, 8 * MICROS_PER_HOUR)
      checkCast(Array(primitiveHeader(DATE), 1, 0, 0, 0), TimestampType,
        MICROS_PER_DAY + 8 * MICROS_PER_HOUR)
    }
    // corner + random cases
    Seq(0, 2147483647, -2147483648, 345344843, -4357342).foreach(input => {
      for (startField <- YearMonthIntervalType.YEAR to YearMonthIntervalType.MONTH) {
        for (endField <- startField to YearMonthIntervalType.MONTH) {
          val headerByte = startField | (endField << 1)
          checkToJson(Array(primitiveHeader(YEAR_MONTH_INTERVAL), headerByte.toByte,
            (input & 0xFF).toByte, ((input >> 8) & 0xFF).toByte,
            ((input >> 16) & 0xFF).toByte, ((input >> 24) & 0xFF).toByte),
            "\"" + Literal(input, YearMonthIntervalType(startField.toByte, endField.toByte)) +
              "\"")
        }
      }
    })

    // corner + random cases
    Seq(0L, 9223372036854775807L, -9223372036854775808L, 2374234381L, -23467681L).foreach(input => {
      for (startField <- DayTimeIntervalType.DAY to DayTimeIntervalType.SECOND) {
        for (endField <- startField to DayTimeIntervalType.SECOND) {
          val headerByte = startField | (endField << 2)
          checkToJson(Array(primitiveHeader(DAY_TIME_INTERVAL), headerByte.toByte,
            (input & 0xFF).toByte, ((input >> 8) & 0xFF).toByte,
            ((input >> 16) & 0xFF).toByte, ((input >> 24) & 0xFF).toByte,
            ((input >> 32) & 0xFF).toByte, ((input >> 40) & 0xFF).toByte,
            ((input >> 48) & 0xFF).toByte, ((input >> 56) & 0xFF).toByte),
            "\"" + Literal(input, DayTimeIntervalType(startField.toByte, endField.toByte)) +
              "\"")
        }
      }
    })

    checkToJsonFail(Array(primitiveHeader(25)), 25)

    def littleEndianLong(value: Long): Array[Byte] =
      BigInt(value).toByteArray.reverse.padTo(8, 0.toByte)

    val time1 = littleEndianLong(0)
    // In America/Los_Angeles timezone, timestamp value `skippedTime` is 2011-03-13 03:00:00.
    // The next second of 2011-03-13 01:59:59 jumps to 2011-03-13 03:00:00.
    val skippedTime = 1300010400000000L
    val time2 = littleEndianLong(skippedTime)
    val time3 = littleEndianLong(skippedTime - 1)
    val time4 = littleEndianLong(Long.MinValue)
    val time5 = littleEndianLong(Long.MaxValue)
    val time6 = littleEndianLong(-62198755200000000L)
    val timestampHeader = Array(primitiveHeader(TIMESTAMP))
    val timestampNtzHeader = Array(primitiveHeader(TIMESTAMP_NTZ))

    for (timeZone <- Seq("UTC", "America/Los_Angeles")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        checkToJson(timestampNtzHeader ++ time1, "\"1970-01-01 00:00:00\"")
        checkToJson(timestampNtzHeader ++ time2, "\"2011-03-13 10:00:00\"")
        checkToJson(timestampNtzHeader ++ time3, "\"2011-03-13 09:59:59.999999\"")
        checkToJson(timestampNtzHeader ++ time4, "\"-290308-12-21 19:59:05.224192\"")
        checkToJson(timestampNtzHeader ++ time5, "\"+294247-01-10 04:00:54.775807\"")
        checkToJson(timestampNtzHeader ++ time6, "\"-0001-01-01 00:00:00\"")

        checkCast(timestampNtzHeader ++ time1, DateType, 0)
        checkCast(timestampNtzHeader ++ time2, DateType, 15046)
        checkCast(timestampNtzHeader ++ time3, DateType, 15046)
        checkCast(timestampNtzHeader ++ time4, DateType, -106751992)
        checkCast(timestampNtzHeader ++ time5, DateType, 106751991)
        checkCast(timestampNtzHeader ++ time6, DateType, -719893)
      }
    }

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      checkToJson(timestampHeader ++ time1, "\"1970-01-01 00:00:00+00:00\"")
      checkToJson(timestampHeader ++ time2, "\"2011-03-13 10:00:00+00:00\"")
      checkToJson(timestampHeader ++ time3, "\"2011-03-13 09:59:59.999999+00:00\"")
      checkToJson(timestampHeader ++ time4, "\"-290308-12-21 19:59:05.224192+00:00\"")
      checkToJson(timestampHeader ++ time5, "\"+294247-01-10 04:00:54.775807+00:00\"")
      checkToJson(timestampHeader ++ time6, "\"-0001-01-01 00:00:00+00:00\"")

      checkCast(timestampHeader ++ time1, DateType, 0)
      checkCast(timestampHeader ++ time2, DateType, 15046)
      checkCast(timestampHeader ++ time3, DateType, 15046)
      checkCast(timestampHeader ++ time4, DateType, -106751992)
      checkCast(timestampHeader ++ time5, DateType, 106751991)
      checkCast(timestampHeader ++ time6, DateType, -719893)
    }

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      checkToJson(timestampHeader ++ time1, "\"1969-12-31 16:00:00-08:00\"")
      checkToJson(timestampHeader ++ time2, "\"2011-03-13 03:00:00-07:00\"")
      checkToJson(timestampHeader ++ time3, "\"2011-03-13 01:59:59.999999-08:00\"")
      checkToJson(timestampHeader ++ time4, "\"-290308-12-21 12:06:07.224192-07:52\"")
      checkToJson(timestampHeader ++ time5, "\"+294247-01-09 20:00:54.775807-08:00\"")
      checkToJson(timestampHeader ++ time6, "\"-0002-12-31 16:07:02-07:52\"")

      checkCast(timestampHeader ++ time1, DateType, -1)
      checkCast(timestampHeader ++ time2, DateType, 15046)
      checkCast(timestampHeader ++ time3, DateType, 15046)
      checkCast(timestampHeader ++ time4, DateType, -106751992)
      checkCast(timestampHeader ++ time5, DateType, 106751990)
      checkCast(timestampHeader ++ time6, DateType, -719894)
    }

    checkToJson(Array(primitiveHeader(FLOAT)) ++
      BigInt(java.lang.Float.floatToIntBits(1.23F)).toByteArray.reverse, "1.23")
    checkToJson(Array(primitiveHeader(FLOAT)) ++
      BigInt(java.lang.Float.floatToIntBits(-0.0F)).toByteArray.reverse, "-0.0")
    // Note: 1.23F.toDouble != 1.23.
    checkCast(Array(primitiveHeader(FLOAT)) ++
      BigInt(java.lang.Float.floatToIntBits(1.23F)).toByteArray.reverse, DoubleType, 1.23F.toDouble)

    checkToJson(Array(primitiveHeader(BINARY), 0, 0, 0, 0), "\"\"")
    checkToJson(Array(primitiveHeader(BINARY), 1, 0, 0, 0, 1), "\"AQ==\"")
    checkToJson(Array(primitiveHeader(BINARY), 2, 0, 0, 0, 1, 2), "\"AQI=\"")
    checkToJson(Array(primitiveHeader(BINARY), 3, 0, 0, 0, 1, 2, 3), "\"AQID\"")
    checkCast(Array(primitiveHeader(BINARY), 3, 0, 0, 0, 1, 2, 3), StringType,
      "\u0001\u0002\u0003")
    checkCast(Array(primitiveHeader(BINARY), 5, 0, 0, 0, 72, 101, 108, 108, 111), StringType,
      "Hello")
  }

  test("SPARK-48150: ParseJson expression nullability") {
    assert(!ParseJson(Literal("["), failOnError = true).nullable)
    assert(ParseJson(Literal("["), failOnError = false).nullable)
    checkEvaluation(
      ParseJson(Literal("["), failOnError = false),
      null
    )
  }

  test("cast to variant/to_variant_object") {
    def check[T : TypeTag](input: T, expectedJson: String,
                           toVariantObject: Boolean = false): Unit = {
      val expr =
        if (toVariantObject) ToVariantObject(Literal.create(input))
        else Cast(Literal.create(input), VariantType, evalMode = EvalMode.ANSI)
      checkEvaluation(StructsToJson(Map.empty, expr), expectedJson)
    }

    def checkFailure[T: TypeTag](input: T, toVariantObject: Boolean = false): Unit = {
      val expr =
        if (toVariantObject) ToVariantObject(Literal.create(input))
        else Cast(Literal.create(input), VariantType, evalMode = EvalMode.ANSI)
      val resolvedExpr = ResolveTimeZone.resolveTimeZones(expr)
      assert(!resolvedExpr.resolved)
    }

    // cast to variant - success cases
    check(null.asInstanceOf[String], null)
    // The following tests cover all allowed scalar types.
    for (input <- Seq[Any](false, true, 0.toByte, 1.toShort, 2, 3L, 4.0F, 5.0D)) {
      check(input, input.toString)
    }
    for (precision <- Seq(9, 18, 38)) {
      val input = BigDecimal("9" * precision)
      check(Literal.create(input, DecimalType(precision, 0)), input.toString)
    }
    check("", "\"\"")
    check("x" * 128, "\"" + ("x" * 128) + "\"")
    check(Array[Byte](1, 2, 3), "\"AQID\"")
    check(Literal(0, DateType), "\"1970-01-01\"")

    // year-month interval corner + random cases
    Seq(0, 2147483647, -2147483648, 753992, -5920283).foreach(input => {
      for (startField <- YearMonthIntervalType.YEAR to YearMonthIntervalType.MONTH) {
        for (endField <- startField to YearMonthIntervalType.MONTH) {
          val lit = Literal(input, YearMonthIntervalType(startField.toByte, endField.toByte))
          check(lit, "\"" + lit.toString + "\"")
        }
      }
    })
    // Size of YMInterval
    assert(Cast(Literal.create(Period.ofMonths(0)), VariantType, evalMode = EvalMode.ANSI)
      .eval().asInstanceOf[VariantVal].getValue.length == 6)

    // Array of year-month intervals
    val ymArrLit = Literal.create(
      Array(Period.ofMonths(0), Period.ofMonths(2147483647), Period.ofMonths(-2147483647)),
      ArrayType(YearMonthIntervalType(1, 1))
    )
    check(ymArrLit, """["INTERVAL '0' MONTH","INTERVAL""" +
      """ '2147483647' MONTH","INTERVAL '-2147483647' MONTH"]""")

    // day-time interval corner + random cases
    Seq(0L, 9223372036854775807L, -9223372036854775808L, 47356878948217L, -23745867989934789L)
      .foreach(input => {
        for (startField <- DayTimeIntervalType.DAY to DayTimeIntervalType.SECOND) {
          for (endField <- startField to DayTimeIntervalType.SECOND) {
            val lit = Literal(input, DayTimeIntervalType(startField.toByte, endField.toByte))
            check(lit, "\"" + lit.toString + "\"")
          }
        }
      })
    // Size of DTInterval
    assert(Cast(Literal.create(Duration.ofSeconds(0)), VariantType, evalMode = EvalMode.ANSI)
      .eval().asInstanceOf[VariantVal].getValue.length == 10)

    // Array of day-time intervals
    val dtArrLit = Literal.create(
      Array(Duration.ofSeconds(0), Duration.ofSeconds(9223372036854L),
        Duration.ofSeconds(-9223372036854L)),
      ArrayType(DayTimeIntervalType(3, 3))
    )
    check(dtArrLit, """["INTERVAL '00' SECOND","INTERVAL""" +
      """ '9223372036854' SECOND","INTERVAL '-9223372036854' SECOND"]""")

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      check(Literal(0L, TimestampType), "\"1970-01-01 00:00:00+00:00\"")
      check(Literal(0L, TimestampNTZType), "\"1970-01-01 00:00:00\"")
    }
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      check(Literal(0L, TimestampType), "\"1969-12-31 16:00:00-08:00\"")
      check(Literal(0L, TimestampNTZType), "\"1970-01-01 00:00:00\"")
    }

    check(Array(null, "a", "b", "c"), """[null,"a","b","c"]""")
    check(Array(parseJson("""{"a": 1,"b": [1, 2, 3]}"""),
      parseJson("""{"c": true,"d": {"e": "str"}}""")),
      """[{"a":1,"b":[1,2,3]},{"c":true,"d":{"e":"str"}}]""")

    // cast to variant - failure cases - struct and map types
    val mp = Map("z" -> 1, "y" -> 2, "x" -> 3)
    val arrayMp = Array(Map("z" -> 1, "y" -> 2, "x" -> 3))
    val arrayArrayMp = Array(Array(Map("z" -> 1, "y" -> 2, "x" -> 3)))
    checkFailure(mp)
    checkFailure(arrayMp)
    checkFailure(arrayArrayMp)
    val struct = Literal.create(create_row(1),
      StructType(Array(StructField("a", IntegerType))))
    checkFailure(struct)
    val arrayStruct = Literal.create(
      Array(create_row(1)),
      ArrayType(StructType(Array(StructField("a", IntegerType)))))
    checkFailure(arrayStruct)

    // to_variant_object - success cases - nested types
    check(Array(1, 2, 3), "[1,2,3]", toVariantObject = true)
    check(mp, """{"x":3,"y":2,"z":1}""", toVariantObject = true)
    check(arrayMp, """[{"x":3,"y":2,"z":1}]""", toVariantObject = true)
    check(arrayArrayMp, """[[{"x":3,"y":2,"z":1}]]""", toVariantObject = true)
    check(struct, """{"a":1}""", toVariantObject = true)
    check(arrayStruct, """[{"a":1}]""", toVariantObject = true)
    val complexStruct = Literal.create(
      Row(
        Seq("123", "true", "f"),
        Map("a" -> "123", "b" -> "true", "c" -> "f"),
        Map("a" -> Row(132)),
        Row(0)),
      StructType.fromDDL("c ARRAY<STRING>,b MAP<STRING, STRING>,d MAP<STRING, STRUCT<i: INT>>," +
        "a STRUCT<i: INT>"))
    check(complexStruct,
      """{"a":{"i":0},"b":{"a":"123","b":"true","c":"f"},"c":["123","true","f"],""" +
      """"d":{"a":{"i":132}}}""",
      toVariantObject = true)
    check(ymArrLit, """["INTERVAL '0' MONTH","INTERVAL""" +
      """ '2147483647' MONTH","INTERVAL '-2147483647' MONTH"]""", toVariantObject = true)

    // to_variant_object - failure cases - non-nested types or map with non-string key
    checkFailure(1, toVariantObject = true)
    checkFailure(true, toVariantObject = true)
    checkFailure(Literal.create(Literal.create(Period.ofMonths(0))), toVariantObject = true)
    checkFailure(Map(1 -> 1), toVariantObject = true)
  }

  test("schema_of_variant - unknown type") {
    val emptyMetadata = Array[Byte](VERSION, 0, 0)

    def checkErrorInSchemaOf(value: Array[Byte], id: Int): Unit = {
      val input = Literal(new VariantVal(value, emptyMetadata))
      checkErrorInExpression[SparkRuntimeException](
        ResolveTimeZone.resolveTimeZones(SchemaOfVariant(input).replacement),
        "UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT", Map("id" -> id.toString)
      )
    }
    checkErrorInSchemaOf(Array(primitiveHeader(25)), 25)
  }

  test("malformed interval type") {
    val emptyMetadata = Array[Byte](VERSION, 0, 0)

    def checkErrorInIntervalVariant(value: Array[Byte], id: Int): Unit = {
      val input = Literal(new VariantVal(value, emptyMetadata))
      checkErrorInExpression[SparkRuntimeException](
        ResolveTimeZone.resolveTimeZones(StructsToJson(Map.empty, input)),
        "MALFORMED_VARIANT")
    }
    checkErrorInIntervalVariant(Array(primitiveHeader(YEAR_MONTH_INTERVAL), 0, 0, 0, 0),
      YEAR_MONTH_INTERVAL)
    checkErrorInIntervalVariant(Array(primitiveHeader(DAY_TIME_INTERVAL), 0, 0, 0, 0, 0, 0, 0, 0),
      DAY_TIME_INTERVAL)
  }

  test("schema_of_variant - schema merge") {
    val nul = Literal(null, StringType)
    val boolean = Literal.default(BooleanType)
    val long = Literal.default(LongType)
    val string = Literal.default(StringType)
    val double = Literal.default(DoubleType)
    val date = Literal.default(DateType)
    val timestamp = Literal.default(TimestampType)
    val timestampNtz = Literal.default(TimestampNTZType)
    val float = Literal.default(FloatType)
    val binary = Literal.default(BinaryType)
    val decimal = Literal(Decimal("123.456"), DecimalType(6, 3))
    val array1 = Literal(Array(0L))
    val array2 = Literal(Array(0.0))
    val struct1 = Literal.default(StructType.fromDDL("a string"))
    val struct2 = Literal.default(StructType.fromDDL("a boolean, b bigint"))
    // TypeCoercion.findTightestCommonType handles interval types in expected ways. It doesn't make
    // sense to add merge intervals with other types
    val dtInterval1 = Literal(0L, DayTimeIntervalType(1, 3))
    val dtInterval2 = Literal(0L, DayTimeIntervalType(0, 2))
    val ymInterval1 = Literal(0, YearMonthIntervalType(0, 0))
    val ymInterval2 = Literal(0, YearMonthIntervalType(1, 1))
    val inputs = Seq(nul, boolean, long, string, double, date, timestamp, timestampNtz, float,
      binary, decimal, array1, array2, struct1, struct2, dtInterval1, dtInterval2, ymInterval1,
      ymInterval2)

    val results = mutable.HashMap.empty[(Literal, Literal), String]
    for (i <- inputs) {
      val inputType = if (i.value == null) "VOID" else SchemaOfVariant.printSchema(i.dataType)
      results.put((nul, i), inputType)
      results.put((i, i), inputType)
    }
    results.put((long, double), "DOUBLE")
    results.put((long, float), "FLOAT")
    results.put((long, decimal), "DECIMAL(23,3)")
    results.put((double, float), "DOUBLE")
    results.put((double, decimal), "DOUBLE")
    results.put((date, timestamp), "TIMESTAMP")
    results.put((date, timestampNtz), "TIMESTAMP_NTZ")
    results.put((timestamp, timestampNtz), "TIMESTAMP")
    results.put((float, decimal), "DOUBLE")
    results.put((array1, array2), "ARRAY<DOUBLE>")
    results.put((struct1, struct2), "OBJECT<a: VARIANT, b: BIGINT>")
    results.put((dtInterval1, dtInterval2), "INTERVAL DAY TO SECOND")
    results.put((ymInterval1, ymInterval2), "INTERVAL YEAR TO MONTH")

    for (i1 <- inputs) {
      for (i2 <- inputs) {
        val expected = results.getOrElse((i1, i2), results.getOrElse((i2, i1), "VARIANT"))
        val elem1 =
          if (i1.dataType.isInstanceOf[ArrayType] || i1.dataType.isInstanceOf[MapType] ||
            i1.dataType.isInstanceOf[StructType]) {
            ToVariantObject(i1)
          } else Cast(i1, VariantType)
        val elem2 =
          if (i2.dataType.isInstanceOf[ArrayType] || i2.dataType.isInstanceOf[MapType] ||
            i2.dataType.isInstanceOf[StructType]) {
            ToVariantObject(i2)
          } else Cast(i2, VariantType)
        val array = CreateArray(Seq(elem1, elem2))
        checkEvaluation(SchemaOfVariant(Cast(array, VariantType)).replacement, s"ARRAY<$expected>")
      }
    }
  }
}
