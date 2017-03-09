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

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.encoders.{ExamplePointUDT, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, StructType, _}
import org.apache.spark.unsafe.types.UTF8String

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
