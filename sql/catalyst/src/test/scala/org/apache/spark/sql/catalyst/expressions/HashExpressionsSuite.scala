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

package org.apache.spark.sql.catalyst.expressions

import java.nio.charset.StandardCharsets
import java.util.TimeZone

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.encoders.{ExamplePointUDT, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, StructType, _}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class HashExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  val random = new scala.util.Random

  test("md5") {
    checkEvaluation(Md5(Literal("ABC".getBytes(StandardCharsets.UTF_8))),
      "902fbdd2b1df0c4f70b4a5d23525e932")
    checkEvaluation(Md5(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "6ac1e56bc78f031059be7be854522c4c")
    checkEvaluation(Md5(Literal.create(null, BinaryType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(Md5, BinaryType)
  }

  test("sha1") {
    checkEvaluation(Sha1(Literal("ABC".getBytes(StandardCharsets.UTF_8))),
      "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8")
    checkEvaluation(Sha1(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "5d211bad8f4ee70e16c7d343a838fc344a1ed961")
    checkEvaluation(Sha1(Literal.create(null, BinaryType)), null)
    checkEvaluation(Sha1(Literal("".getBytes(StandardCharsets.UTF_8))),
      "da39a3ee5e6b4b0d3255bfef95601890afd80709")
    checkConsistencyBetweenInterpretedAndCodegen(Sha1, BinaryType)
  }

  test("sha2") {
    checkEvaluation(Sha2(Literal("ABC".getBytes(StandardCharsets.UTF_8)), Literal(256)),
      DigestUtils.sha256Hex("ABC"))
    checkEvaluation(Sha2(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType), Literal(384)),
      DigestUtils.sha384Hex(Array[Byte](1, 2, 3, 4, 5, 6)))
    // unsupported bit length
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal(1024)), null)
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal(512)), null)
    checkEvaluation(Sha2(Literal("ABC".getBytes(StandardCharsets.UTF_8)),
      Literal.create(null, IntegerType)), null)
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal.create(null, IntegerType)), null)
  }

  test("crc32") {
    checkEvaluation(Crc32(Literal("ABC".getBytes(StandardCharsets.UTF_8))), 2743272264L)
    checkEvaluation(Crc32(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      2180413220L)
    checkEvaluation(Crc32(Literal.create(null, BinaryType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(Crc32, BinaryType)
  }

  def checkHiveHash(input: Any, dataType: DataType, expected: Long): Unit = {
    // Note : All expected hashes need to be computed using Hive 1.2.1
    val actual = HiveHashFunction.hash(input, dataType, seed = 0)

    withClue(s"hash mismatch for input = `$input` of type `$dataType`.") {
      assert(actual == expected)
    }
  }

  def checkHiveHashForIntegralType(dataType: DataType): Unit = {
    // corner cases
    checkHiveHash(null, dataType, 0)
    checkHiveHash(1, dataType, 1)
    checkHiveHash(0, dataType, 0)
    checkHiveHash(-1, dataType, -1)
    checkHiveHash(Int.MaxValue, dataType, Int.MaxValue)
    checkHiveHash(Int.MinValue, dataType, Int.MinValue)

    // random values
    for (_ <- 0 until 10) {
      val input = random.nextInt()
      checkHiveHash(input, dataType, input)
    }
  }

  test("hive-hash for null") {
    checkHiveHash(null, NullType, 0)
  }

  test("hive-hash for boolean") {
    checkHiveHash(true, BooleanType, 1)
    checkHiveHash(false, BooleanType, 0)
  }

  test("hive-hash for byte") {
    checkHiveHashForIntegralType(ByteType)
  }

  test("hive-hash for short") {
    checkHiveHashForIntegralType(ShortType)
  }

  test("hive-hash for int") {
    checkHiveHashForIntegralType(IntegerType)
  }

  test("hive-hash for long") {
    checkHiveHash(1L, LongType, 1L)
    checkHiveHash(0L, LongType, 0L)
    checkHiveHash(-1L, LongType, 0L)
    checkHiveHash(Long.MaxValue, LongType, -2147483648)
    // Hive's fails to parse this.. but the hashing function itself can handle this input
    checkHiveHash(Long.MinValue, LongType, -2147483648)

    for (_ <- 0 until 10) {
      val input = random.nextLong()
      checkHiveHash(input, LongType, ((input >>> 32) ^ input).toInt)
    }
  }

  test("hive-hash for float") {
    checkHiveHash(0F, FloatType, 0)
    checkHiveHash(0.0F, FloatType, 0)
    checkHiveHash(1.1F, FloatType, 1066192077L)
    checkHiveHash(-1.1F, FloatType, -1081291571)
    checkHiveHash(99999999.99999999999F, FloatType, 1287568416L)
    checkHiveHash(Float.MaxValue, FloatType, 2139095039)
    checkHiveHash(Float.MinValue, FloatType, -8388609)
  }

  test("hive-hash for double") {
    checkHiveHash(0, DoubleType, 0)
    checkHiveHash(0.0, DoubleType, 0)
    checkHiveHash(1.1, DoubleType, -1503133693)
    checkHiveHash(-1.1, DoubleType, 644349955)
    checkHiveHash(1000000000.000001, DoubleType, 1104006509)
    checkHiveHash(1000000000.0000000000000000000000001, DoubleType, 1104006501)
    checkHiveHash(9999999999999999999.9999999999999999999, DoubleType, 594568676)
    checkHiveHash(Double.MaxValue, DoubleType, -2146435072)
    checkHiveHash(Double.MinValue, DoubleType, 1048576)
  }

  test("hive-hash for string") {
    checkHiveHash(UTF8String.fromString("apache spark"), StringType, 1142704523L)
    checkHiveHash(UTF8String.fromString("!@#$%^&*()_+=-"), StringType, -613724358L)
    checkHiveHash(UTF8String.fromString("abcdefghijklmnopqrstuvwxyz"), StringType, 958031277L)
    checkHiveHash(UTF8String.fromString("AbCdEfGhIjKlMnOpQrStUvWxYz012"), StringType, -648013852L)
    // scalastyle:off nonascii
    checkHiveHash(UTF8String.fromString("数据砖头"), StringType, -898686242L)
    checkHiveHash(UTF8String.fromString("नमस्ते"), StringType, 2006045948L)
    // scalastyle:on nonascii
  }

  test("hive-hash for date type") {
    def checkHiveHashForDateType(dateString: String, expected: Long): Unit = {
      checkHiveHash(
        DateTimeUtils.stringToDate(UTF8String.fromString(dateString)).get,
        DateType,
        expected)
    }

    // basic case
    checkHiveHashForDateType("2017-01-01", 17167)

    // boundary cases
    checkHiveHashForDateType("0000-01-01", -719530)
    checkHiveHashForDateType("9999-12-31", 2932896)

    // epoch
    checkHiveHashForDateType("1970-01-01", 0)

    // before epoch
    checkHiveHashForDateType("1800-01-01", -62091)

    // Invalid input: bad date string. Hive returns 0 for such cases
    intercept[NoSuchElementException](checkHiveHashForDateType("0-0-0", 0))
    intercept[NoSuchElementException](checkHiveHashForDateType("-1212-01-01", 0))
    intercept[NoSuchElementException](checkHiveHashForDateType("2016-99-99", 0))

    // Invalid input: Empty string. Hive returns 0 for this case
    intercept[NoSuchElementException](checkHiveHashForDateType("", 0))

    // Invalid input: February 30th for a leap year. Hive supports this but Spark doesn't
    intercept[NoSuchElementException](checkHiveHashForDateType("2016-02-30", 16861))
  }

  test("hive-hash for timestamp type") {
    def checkHiveHashForTimestampType(
        timestamp: String,
        expected: Long,
        timeZone: TimeZone = TimeZone.getTimeZone("UTC")): Unit = {
      checkHiveHash(
        DateTimeUtils.stringToTimestamp(UTF8String.fromString(timestamp), timeZone).get,
        TimestampType,
        expected)
    }

    // basic case
    checkHiveHashForTimestampType("2017-02-24 10:56:29", 1445725271)

    // with higher precision
    checkHiveHashForTimestampType("2017-02-24 10:56:29.111111", 1353936655)

    // with different timezone
    checkHiveHashForTimestampType("2017-02-24 10:56:29", 1445732471,
      TimeZone.getTimeZone("US/Pacific"))

    // boundary cases
    checkHiveHashForTimestampType("0001-01-01 00:00:00", 1645926784)
    checkHiveHashForTimestampType("9999-01-01 00:00:00", -1081818240)

    // epoch
    checkHiveHashForTimestampType("1970-01-01 00:00:00", 0)

    // before epoch
    checkHiveHashForTimestampType("1800-01-01 03:12:45", -267420885)

    // Invalid input: bad timestamp string. Hive returns 0 for such cases
    intercept[NoSuchElementException](checkHiveHashForTimestampType("0-0-0 0:0:0", 0))
    intercept[NoSuchElementException](checkHiveHashForTimestampType("-99-99-99 99:99:45", 0))
    intercept[NoSuchElementException](checkHiveHashForTimestampType("555555-55555-5555", 0))

    // Invalid input: Empty string. Hive returns 0 for this case
    intercept[NoSuchElementException](checkHiveHashForTimestampType("", 0))

    // Invalid input: February 30th is a leap year. Hive supports this but Spark doesn't
    intercept[NoSuchElementException](checkHiveHashForTimestampType("2016-02-30 00:00:00", 0))

    // Invalid input: Hive accepts upto 9 decimal place precision but Spark uses upto 6
    intercept[TestFailedException](checkHiveHashForTimestampType("2017-02-24 10:56:29.11111111", 0))
  }

  test("hive-hash for CalendarInterval type") {
    def checkHiveHashForIntervalType(interval: String, expected: Long): Unit = {
      checkHiveHash(CalendarInterval.fromString(interval), CalendarIntervalType, expected)
    }

    // ----- MICROSEC -----

    // basic case
    checkHiveHashForIntervalType("interval 1 microsecond", 24273)

    // negative
    checkHiveHashForIntervalType("interval -1 microsecond", 22273)

    // edge / boundary cases
    checkHiveHashForIntervalType("interval 0 microsecond", 23273)
    checkHiveHashForIntervalType("interval 999 microsecond", 1022273)
    checkHiveHashForIntervalType("interval -999 microsecond", -975727)

    // ----- MILLISEC -----

    // basic case
    checkHiveHashForIntervalType("interval 1 millisecond", 1023273)

    // negative
    checkHiveHashForIntervalType("interval -1 millisecond", -976727)

    // edge / boundary cases
    checkHiveHashForIntervalType("interval 0 millisecond", 23273)
    checkHiveHashForIntervalType("interval 999 millisecond", 999023273)
    checkHiveHashForIntervalType("interval -999 millisecond", -998976727)

    // ----- SECOND -----

    // basic case
    checkHiveHashForIntervalType("interval 1 second", 23310)

    // negative
    checkHiveHashForIntervalType("interval -1 second", 23273)

    // edge / boundary cases
    checkHiveHashForIntervalType("interval 0 second", 23273)
    checkHiveHashForIntervalType("interval 2147483647 second", -2147460412)
    checkHiveHashForIntervalType("interval -2147483648 second", -2147460412)

    // Out of range for both Hive and Spark
    // Hive throws an exception. Spark overflows and returns wrong output
    // checkHiveHashForIntervalType("interval 9999999999 second", 0)

    // ----- MINUTE -----

    // basic cases
    checkHiveHashForIntervalType("interval 1 minute", 25493)

    // negative
    checkHiveHashForIntervalType("interval -1 minute", 25456)

    // edge / boundary cases
    checkHiveHashForIntervalType("interval 0 minute", 23273)
    checkHiveHashForIntervalType("interval 2147483647 minute", 21830)
    checkHiveHashForIntervalType("interval -2147483648 minute", 22163)

    // Out of range for both Hive and Spark
    // Hive throws an exception. Spark overflows and returns wrong output
    // checkHiveHashForIntervalType("interval 9999999999 minute", 0)

    // ----- HOUR -----

    // basic case
    checkHiveHashForIntervalType("interval 1 hour", 156473)

    // negative
    checkHiveHashForIntervalType("interval -1 hour", 156436)

    // edge / boundary cases
    checkHiveHashForIntervalType("interval 0 hour", 23273)
    checkHiveHashForIntervalType("interval 2147483647 hour", -62308)
    checkHiveHashForIntervalType("interval -2147483648 hour", -43327)

    // Out of range for both Hive and Spark
    // Hive throws an exception. Spark overflows and returns wrong output
    // checkHiveHashForIntervalType("interval 9999999999 hour", 0)

    // ----- DAY -----

    // basic cases
    checkHiveHashForIntervalType("interval 1 day", 3220073)

    // negative
    checkHiveHashForIntervalType("interval -1 day", 3220036)

    // edge / boundary cases
    checkHiveHashForIntervalType("interval 0 day", 23273)
    checkHiveHashForIntervalType("interval 106751991 day", -451506760)
    checkHiveHashForIntervalType("interval -106751991 day", -451514123)

    // Hive supports `day` for a longer range but Spark's range is smaller
    // The check for range is done at the parser level so this does not fail in Spark
    // checkHiveHashForIntervalType("interval -2147483648 day", -1575127)
    // checkHiveHashForIntervalType("interval 2147483647 day", -4767228)

    // Out of range for both Hive and Spark
    // Hive throws an exception. Spark overflows and returns wrong output
    // checkHiveHashForIntervalType("interval 9999999999 day", 0)

    // ----- MIX -----

    checkHiveHashForIntervalType("interval 0 day 0 hour", 23273)
    checkHiveHashForIntervalType("interval 0 day 0 hour 0 minute", 23273)
    checkHiveHashForIntervalType("interval 0 day 0 hour 0 minute 0 second", 23273)
    checkHiveHashForIntervalType("interval 0 day 0 hour 0 minute 0 second 0 millisecond", 23273)
    checkHiveHashForIntervalType(
      "interval 0 day 0 hour 0 minute 0 second 0 millisecond 0 microsecond", 23273)

    checkHiveHashForIntervalType("interval 6 day 15 hour", 21202073)
    checkHiveHashForIntervalType("interval 5 day 4 hour 8 minute", 16557833)
    checkHiveHashForIntervalType("interval -23 day 56 hour -1111113 minute 9898989 second",
      -2128468593)
    checkHiveHashForIntervalType("interval 66 day 12 hour 39 minute 23 second 987 millisecond",
      1199697904)
    checkHiveHashForIntervalType(
      "interval 66 day 12 hour 39 minute 23 second 987 millisecond 123 microsecond", 1199820904)
  }

  test("hive-hash for array") {
    // empty array
    checkHiveHash(
      input = new GenericArrayData(Array[Int]()),
      dataType = ArrayType(IntegerType, containsNull = false),
      expected = 0)

    // basic case
    checkHiveHash(
      input = new GenericArrayData(Array(1, 10000, Int.MaxValue)),
      dataType = ArrayType(IntegerType, containsNull = false),
      expected = -2147172688L)

    // with negative values
    checkHiveHash(
      input = new GenericArrayData(Array(-1L, 0L, 999L, Int.MinValue.toLong)),
      dataType = ArrayType(LongType, containsNull = false),
      expected = -2147452680L)

    // with nulls only
    val arrayTypeWithNull = ArrayType(IntegerType, containsNull = true)
    checkHiveHash(
      input = new GenericArrayData(Array(null, null)),
      dataType = arrayTypeWithNull,
      expected = 0)

    // mix with null
    checkHiveHash(
      input = new GenericArrayData(Array(-12221, 89, null, 767)),
      dataType = arrayTypeWithNull,
      expected = -363989515)

    // nested with array
    checkHiveHash(
      input = new GenericArrayData(
        Array(
          new GenericArrayData(Array(1234L, -9L, 67L)),
          new GenericArrayData(Array(null, null)),
          new GenericArrayData(Array(55L, -100L, -2147452680L))
        )),
      dataType = ArrayType(ArrayType(LongType)),
      expected = -1007531064)

    // nested with map
    checkHiveHash(
      input = new GenericArrayData(
        Array(
          new ArrayBasedMapData(
            new GenericArrayData(Array(-99, 1234)),
            new GenericArrayData(Array(UTF8String.fromString("sql"), null))),
          new ArrayBasedMapData(
            new GenericArrayData(Array(67)),
            new GenericArrayData(Array(UTF8String.fromString("apache spark"))))
        )),
      dataType = ArrayType(MapType(IntegerType, StringType)),
      expected = 1139205955)
  }

  test("hive-hash for map") {
    val mapType = MapType(IntegerType, StringType)

    // empty map
    checkHiveHash(
      input = new ArrayBasedMapData(new GenericArrayData(Array()), new GenericArrayData(Array())),
      dataType = mapType,
      expected = 0)

    // basic case
    checkHiveHash(
      input = new ArrayBasedMapData(
        new GenericArrayData(Array(1, 2)),
        new GenericArrayData(Array(UTF8String.fromString("foo"), UTF8String.fromString("bar")))),
      dataType = mapType,
      expected = 198872)

    // with null value
    checkHiveHash(
      input = new ArrayBasedMapData(
        new GenericArrayData(Array(55, -99)),
        new GenericArrayData(Array(UTF8String.fromString("apache spark"), null))),
      dataType = mapType,
      expected = 1142704473)

    // nesting (only values can be nested as keys have to be primitive datatype)
    val nestedMapType = MapType(IntegerType, MapType(IntegerType, StringType))
    checkHiveHash(
      input = new ArrayBasedMapData(
        new GenericArrayData(Array(1, -100)),
        new GenericArrayData(
          Array(
            new ArrayBasedMapData(
              new GenericArrayData(Array(-99, 1234)),
              new GenericArrayData(Array(UTF8String.fromString("sql"), null))),
            new ArrayBasedMapData(
              new GenericArrayData(Array(67)),
              new GenericArrayData(Array(UTF8String.fromString("apache spark"))))
          ))),
      dataType = nestedMapType,
      expected = -1142817416)
  }

  test("hive-hash for struct") {
    // basic
    val row = new GenericInternalRow(Array[Any](1, 2, 3))
    checkHiveHash(
      input = row,
      dataType =
        new StructType()
          .add("col1", IntegerType)
          .add("col2", IntegerType)
          .add("col3", IntegerType),
      expected = 1026)

    // mix of several datatypes
    val structType = new StructType()
      .add("null", NullType)
      .add("boolean", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("arrayOfString", arrayOfString)
      .add("mapOfString", mapOfString)

    val rowValues = new ArrayBuffer[Any]()
    rowValues += null
    rowValues += true
    rowValues += 1
    rowValues += 2
    rowValues += Int.MaxValue
    rowValues += Long.MinValue
    rowValues += new GenericArrayData(Array(
      UTF8String.fromString("apache spark"),
      UTF8String.fromString("hello world")
    ))
    rowValues += new ArrayBasedMapData(
      new GenericArrayData(Array(UTF8String.fromString("project"), UTF8String.fromString("meta"))),
      new GenericArrayData(Array(UTF8String.fromString("apache spark"), null))
    )

    val row2 = new GenericInternalRow(rowValues.toArray)
    checkHiveHash(
      input = row2,
      dataType = structType,
      expected = -2119012447)
  }

  private val structOfString = new StructType().add("str", StringType)
  private val structOfUDT = new StructType().add("udt", new ExamplePointUDT, false)
  private val arrayOfString = ArrayType(StringType)
  private val arrayOfNull = ArrayType(NullType)
  private val mapOfString = MapType(StringType, StringType)
  private val arrayOfUDT = ArrayType(new ExamplePointUDT, false)

  testHash(
    new StructType()
      .add("null", NullType)
      .add("boolean", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("bigDecimal", DecimalType.SYSTEM_DEFAULT)
      .add("smallDecimal", DecimalType.USER_DEFAULT)
      .add("string", StringType)
      .add("binary", BinaryType)
      .add("date", DateType)
      .add("timestamp", TimestampType)
      .add("udt", new ExamplePointUDT))

  testHash(
    new StructType()
      .add("arrayOfNull", arrayOfNull)
      .add("arrayOfString", arrayOfString)
      .add("arrayOfArrayOfString", ArrayType(arrayOfString))
      .add("arrayOfArrayOfInt", ArrayType(ArrayType(IntegerType)))
      .add("arrayOfMap", ArrayType(mapOfString))
      .add("arrayOfStruct", ArrayType(structOfString))
      .add("arrayOfUDT", arrayOfUDT))

  testHash(
    new StructType()
      .add("mapOfIntAndString", MapType(IntegerType, StringType))
      .add("mapOfStringAndArray", MapType(StringType, arrayOfString))
      .add("mapOfArrayAndInt", MapType(arrayOfString, IntegerType))
      .add("mapOfArray", MapType(arrayOfString, arrayOfString))
      .add("mapOfStringAndStruct", MapType(StringType, structOfString))
      .add("mapOfStructAndString", MapType(structOfString, StringType))
      .add("mapOfStruct", MapType(structOfString, structOfString)))

  testHash(
    new StructType()
      .add("structOfString", structOfString)
      .add("structOfStructOfString", new StructType().add("struct", structOfString))
      .add("structOfArray", new StructType().add("array", arrayOfString))
      .add("structOfMap", new StructType().add("map", mapOfString))
      .add("structOfArrayAndMap",
        new StructType().add("array", arrayOfString).add("map", mapOfString))
      .add("structOfUDT", structOfUDT))

  test("hive-hash for decimal") {
    def checkHiveHashForDecimal(
        input: String,
        precision: Int,
        scale: Int,
        expected: Long): Unit = {
      val decimalType = DataTypes.createDecimalType(precision, scale)
      val decimal = {
        val value = Decimal.apply(new java.math.BigDecimal(input))
        if (value.changePrecision(precision, scale)) value else null
      }

      checkHiveHash(decimal, decimalType, expected)
    }

    checkHiveHashForDecimal("18", 38, 0, 558)
    checkHiveHashForDecimal("-18", 38, 0, -558)
    checkHiveHashForDecimal("-18", 38, 12, -558)
    checkHiveHashForDecimal("18446744073709001000", 38, 19, 0)
    checkHiveHashForDecimal("-18446744073709001000", 38, 22, 0)
    checkHiveHashForDecimal("-18446744073709001000", 38, 3, 17070057)
    checkHiveHashForDecimal("18446744073709001000", 38, 4, -17070057)
    checkHiveHashForDecimal("9223372036854775807", 38, 4, 2147482656)
    checkHiveHashForDecimal("-9223372036854775807", 38, 5, -2147482656)
    checkHiveHashForDecimal("00000.00000000000", 38, 34, 0)
    checkHiveHashForDecimal("-00000.00000000000", 38, 11, 0)
    checkHiveHashForDecimal("123456.1234567890", 38, 2, 382713974)
    checkHiveHashForDecimal("123456.1234567890", 38, 20, 1871500252)
    checkHiveHashForDecimal("123456.1234567890", 38, 10, 1871500252)
    checkHiveHashForDecimal("-123456.1234567890", 38, 10, -1871500234)
    checkHiveHashForDecimal("123456.1234567890", 38, 0, 3827136)
    checkHiveHashForDecimal("-123456.1234567890", 38, 0, -3827136)
    checkHiveHashForDecimal("123456.1234567890", 38, 20, 1871500252)
    checkHiveHashForDecimal("-123456.1234567890", 38, 20, -1871500234)
    checkHiveHashForDecimal("123456.123456789012345678901234567890", 38, 0, 3827136)
    checkHiveHashForDecimal("-123456.123456789012345678901234567890", 38, 0, -3827136)
    checkHiveHashForDecimal("123456.123456789012345678901234567890", 38, 10, 1871500252)
    checkHiveHashForDecimal("-123456.123456789012345678901234567890", 38, 10, -1871500234)
    checkHiveHashForDecimal("123456.123456789012345678901234567890", 38, 20, 236317582)
    checkHiveHashForDecimal("-123456.123456789012345678901234567890", 38, 20, -236317544)
    checkHiveHashForDecimal("123456.123456789012345678901234567890", 38, 30, 1728235666)
    checkHiveHashForDecimal("-123456.123456789012345678901234567890", 38, 30, -1728235608)
    checkHiveHashForDecimal("123456.123456789012345678901234567890", 38, 31, 1728235666)
  }

  test("SPARK-18207: Compute hash for a lot of expressions") {
    val N = 1000
    val wideRow = new GenericInternalRow(
      Seq.tabulate(N)(i => UTF8String.fromString(i.toString)).toArray[Any])
    val schema = StructType((1 to N).map(i => StructField("", StringType)))

    val exprs = schema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val murmur3HashExpr = Murmur3Hash(exprs, 42)
    val murmur3HashPlan = GenerateMutableProjection.generate(Seq(murmur3HashExpr))
    val murmursHashEval = Murmur3Hash(exprs, 42).eval(wideRow)
    assert(murmur3HashPlan(wideRow).getInt(0) == murmursHashEval)

    val hiveHashExpr = HiveHash(exprs)
    val hiveHashPlan = GenerateMutableProjection.generate(Seq(hiveHashExpr))
    val hiveHashEval = HiveHash(exprs).eval(wideRow)
    assert(hiveHashPlan(wideRow).getInt(0) == hiveHashEval)
  }

  test("SPARK-22284: Compute hash for nested structs") {
    val M = 80
    val N = 10
    val L = M * N
    val O = 50
    val seed = 42

    val wideRow = new GenericInternalRow(Seq.tabulate(O)(k =>
      new GenericInternalRow(Seq.tabulate(M)(j =>
        new GenericInternalRow(Seq.tabulate(N)(i =>
          new GenericInternalRow(Array[Any](
            UTF8String.fromString((k * L + j * N + i).toString))))
          .toArray[Any])).toArray[Any])).toArray[Any])
    val inner = new StructType(
      (0 until N).map(_ => StructField("structOfString", structOfString)).toArray)
    val outer = new StructType(
      (0 until M).map(_ => StructField("structOfStructOfString", inner)).toArray)
    val schema = new StructType(
      (0 until O).map(_ => StructField("structOfStructOfStructOfString", outer)).toArray)
    val exprs = schema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val murmur3HashExpr = Murmur3Hash(exprs, 42)
    val murmur3HashPlan = GenerateMutableProjection.generate(Seq(murmur3HashExpr))

    val murmursHashEval = Murmur3Hash(exprs, 42).eval(wideRow)
    assert(murmur3HashPlan(wideRow).getInt(0) == murmursHashEval)
  }

  private def testHash(inputSchema: StructType): Unit = {
    val inputGenerator = RandomDataGenerator.forType(inputSchema, nullable = false).get
    val encoder = RowEncoder(inputSchema)
    val seed = scala.util.Random.nextInt()
    test(s"murmur3/xxHash64/hive hash: ${inputSchema.simpleString}") {
      for (_ <- 1 to 10) {
        val input = encoder.toRow(inputGenerator.apply().asInstanceOf[Row]).asInstanceOf[UnsafeRow]
        val literals = input.toSeq(inputSchema).zip(inputSchema.map(_.dataType)).map {
          case (value, dt) => Literal.create(value, dt)
        }
        // Only test the interpreted version has same result with codegen version.
        checkEvaluation(Murmur3Hash(literals, seed), Murmur3Hash(literals, seed).eval())
        checkEvaluation(XxHash64(literals, seed), XxHash64(literals, seed).eval())
        checkEvaluation(HiveHash(literals), HiveHash(literals).eval())
      }
    }
  }
}
