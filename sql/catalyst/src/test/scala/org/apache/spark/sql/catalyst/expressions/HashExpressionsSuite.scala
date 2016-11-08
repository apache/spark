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

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.encoders.{ExamplePointUDT, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class HashExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

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
