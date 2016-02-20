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

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.encoders.{ExamplePointUDT, RowEncoder}
import org.apache.spark.sql.types._

class MiscFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("md5") {
    checkEvaluation(Md5(Literal("ABC".getBytes)), "902fbdd2b1df0c4f70b4a5d23525e932")
    checkEvaluation(Md5(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "6ac1e56bc78f031059be7be854522c4c")
    checkEvaluation(Md5(Literal.create(null, BinaryType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(Md5, BinaryType)
  }

  test("sha1") {
    checkEvaluation(Sha1(Literal("ABC".getBytes)), "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8")
    checkEvaluation(Sha1(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "5d211bad8f4ee70e16c7d343a838fc344a1ed961")
    checkEvaluation(Sha1(Literal.create(null, BinaryType)), null)
    checkEvaluation(Sha1(Literal("".getBytes)), "da39a3ee5e6b4b0d3255bfef95601890afd80709")
    checkConsistencyBetweenInterpretedAndCodegen(Sha1, BinaryType)
  }

  test("sha2") {
    checkEvaluation(Sha2(Literal("ABC".getBytes), Literal(256)), DigestUtils.sha256Hex("ABC"))
    checkEvaluation(Sha2(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType), Literal(384)),
      DigestUtils.sha384Hex(Array[Byte](1, 2, 3, 4, 5, 6)))
    // unsupported bit length
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal(1024)), null)
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal(512)), null)
    checkEvaluation(Sha2(Literal("ABC".getBytes), Literal.create(null, IntegerType)), null)
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal.create(null, IntegerType)), null)
  }

  test("crc32") {
    checkEvaluation(Crc32(Literal("ABC".getBytes)), 2743272264L)
    checkEvaluation(Crc32(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      2180413220L)
    checkEvaluation(Crc32(Literal.create(null, BinaryType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(Crc32, BinaryType)
  }

  private val structOfString = new StructType().add("str", StringType)
  private val structOfUDT = new StructType().add("udt", new ExamplePointUDT, false)
  private val arrayOfString = ArrayType(StringType)
  private val arrayOfNull = ArrayType(NullType)
  private val mapOfString = MapType(StringType, StringType)
  private val arrayOfUDT = ArrayType(new ExamplePointUDT, false)

  testMurmur3Hash(
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

  testMurmur3Hash(
    new StructType()
      .add("arrayOfNull", arrayOfNull)
      .add("arrayOfString", arrayOfString)
      .add("arrayOfArrayOfString", ArrayType(arrayOfString))
      .add("arrayOfArrayOfInt", ArrayType(ArrayType(IntegerType)))
      .add("arrayOfMap", ArrayType(mapOfString))
      .add("arrayOfStruct", ArrayType(structOfString))
      .add("arrayOfUDT", arrayOfUDT))

  testMurmur3Hash(
    new StructType()
      .add("mapOfIntAndString", MapType(IntegerType, StringType))
      .add("mapOfStringAndArray", MapType(StringType, arrayOfString))
      .add("mapOfArrayAndInt", MapType(arrayOfString, IntegerType))
      .add("mapOfArray", MapType(arrayOfString, arrayOfString))
      .add("mapOfStringAndStruct", MapType(StringType, structOfString))
      .add("mapOfStructAndString", MapType(structOfString, StringType))
      .add("mapOfStruct", MapType(structOfString, structOfString)))

  testMurmur3Hash(
    new StructType()
      .add("structOfString", structOfString)
      .add("structOfStructOfString", new StructType().add("struct", structOfString))
      .add("structOfArray", new StructType().add("array", arrayOfString))
      .add("structOfMap", new StructType().add("map", mapOfString))
      .add("structOfArrayAndMap",
        new StructType().add("array", arrayOfString).add("map", mapOfString))
      .add("structOfUDT", structOfUDT))

  private def testMurmur3Hash(inputSchema: StructType): Unit = {
    val inputGenerator = RandomDataGenerator.forType(inputSchema, nullable = false).get
    val encoder = RowEncoder(inputSchema)
    val seed = scala.util.Random.nextInt()
    test(s"murmur3 hash: ${inputSchema.simpleString}") {
      for (_ <- 1 to 10) {
        val input = encoder.toRow(inputGenerator.apply().asInstanceOf[Row]).asInstanceOf[UnsafeRow]
        val literals = input.toSeq(inputSchema).zip(inputSchema.map(_.dataType)).map {
          case (value, dt) => Literal.create(value, dt)
        }
        // Only test the interpreted version has same result with codegen version.
        checkEvaluation(Murmur3Hash(literals, seed), Murmur3Hash(literals, seed).eval())
      }
    }
  }

  test("aesEncrypt") {
    val expr1 = AesEncrypt(Literal("ABC".getBytes), Literal("1234567890123456".getBytes))
    val expr2 = AesEncrypt(Literal("".getBytes), Literal("1234567890123456".getBytes))

    checkEvaluation(Base64(expr1), "y6Ss+zCYObpCbgfWfyNWTw==")
    checkEvaluation(Base64(expr2), "BQGHoM3lqYcsurCRq3PlUw==")

    // input is null
    checkEvaluation(AesEncrypt(Literal.create(null, BinaryType),
      Literal("1234567890123456".getBytes)), null)
    // key is null
    checkEvaluation(AesEncrypt(Literal("ABC".getBytes),
      Literal.create(null, BinaryType)), null)
    // both are null
    checkEvaluation(AesEncrypt(Literal.create(null, BinaryType),
      Literal.create(null, BinaryType)), null)

    val expr3 = AesEncrypt(Literal("ABC".getBytes), Literal("1234567890".getBytes))
    // key length (80 bits) is not one of the permitted values (128, 192 or 256 bits)
    intercept[java.security.InvalidKeyException] {
      evaluate(expr3)
    }
    intercept[java.security.InvalidKeyException] {
      UnsafeProjection.create(expr3 :: Nil).apply(null)
    }
  }

  test("aesDecrypt") {
    val expr1 = AesDecrypt(UnBase64(Literal("y6Ss+zCYObpCbgfWfyNWTw==")),
      Literal("1234567890123456".getBytes))
    val expr2 = AesDecrypt(UnBase64(Literal("BQGHoM3lqYcsurCRq3PlUw==")),
      Literal("1234567890123456".getBytes))

    checkEvaluation(expr1, "ABC")
    checkEvaluation(expr2, "")

    // input is null
    checkEvaluation(AesDecrypt(UnBase64(Literal.create(null, StringType)),
      Literal("1234567890123456".getBytes)), null)
    // key is null
    checkEvaluation(AesDecrypt(UnBase64(Literal("y6Ss+zCYObpCbgfWfyNWTw==")),
      Literal.create(null, BinaryType)), null)
    // both are null
    checkEvaluation(AesDecrypt(UnBase64(Literal.create(null, StringType)),
      Literal.create(null, BinaryType)), null)

    val expr3 = AesDecrypt(UnBase64(Literal("y6Ss+zCYObpCbgfWfyNWTw==")),
      Literal("1234567890".getBytes))
    val expr4 = AesDecrypt(UnBase64(Literal("y6Ss+zCsdYObpCbgfWfyNW3Twewr")),
      Literal("1234567890123456".getBytes))
    val expr5 = AesDecrypt(UnBase64(Literal("t6Ss+zCYObpCbgfWfyNWTw==")),
      Literal("1234567890123456".getBytes))

    // key length (80 bits) is not one of the permitted values (128, 192 or 256 bits)
    intercept[java.security.InvalidKeyException] {
      evaluate(expr3)
    }
    intercept[java.security.InvalidKeyException] {
      UnsafeProjection.create(expr3 :: Nil).apply(null)
    }
    // input can not be decrypted
    intercept[javax.crypto.IllegalBlockSizeException] {
      evaluate(expr4)
    }
    intercept[javax.crypto.IllegalBlockSizeException] {
      UnsafeProjection.create(expr4 :: Nil).apply(null)
    }
    // input can not be decrypted
    intercept[javax.crypto.BadPaddingException] {
      evaluate(expr5)
    }
    intercept[javax.crypto.BadPaddingException] {
      UnsafeProjection.create(expr5 :: Nil).apply(null)
    }
  }

  ignore("aesEncryptWith256bitsKey") {
    // Before testing this, installing Java Cryptography Extension (JCE) Unlimited Strength Juris-
    // diction Policy Files first. Otherwise `java.security.InvalidKeyException` will be thrown.
    // Because Oracle JDK does not support 192 and 256 bits key out of box.
    checkEvaluation(Base64(AesEncrypt(Literal("ABC".getBytes),
      Literal("12345678901234561234567890123456".getBytes))), "nYfCuJeRd5eD60yXDw7WEA==")
  }
}
