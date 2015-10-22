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

package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class RowEncoderSuite extends SparkFunSuite {

  private val structOfString = new StructType().add("str", StringType)
  private val arrayOfString = ArrayType(StringType)
  private val mapOfString = MapType(StringType, StringType)

  encodeDecodeTest(
    new StructType()
      .add("boolean", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("decimal", DecimalType.SYSTEM_DEFAULT)
      .add("string", StringType)
      .add("binary", BinaryType)
      .add("date", DateType)
      .add("timestamp", TimestampType))

  encodeDecodeTest(
    new StructType()
      .add("arrayOfString", arrayOfString)
      .add("arrayOfArrayOfString", ArrayType(arrayOfString))
      .add("arrayOfArrayOfInt", ArrayType(ArrayType(IntegerType)))
      .add("arrayOfMap", ArrayType(mapOfString))
      .add("arrayOfStruct", ArrayType(structOfString)))

  encodeDecodeTest(
    new StructType()
      .add("mapOfIntAndString", MapType(IntegerType, StringType))
      .add("mapOfStringAndArray", MapType(StringType, arrayOfString))
      .add("mapOfArrayAndInt", MapType(arrayOfString, IntegerType))
      .add("mapOfArray", MapType(arrayOfString, arrayOfString))
      .add("mapOfStringAndStruct", MapType(StringType, structOfString))
      .add("mapOfStructAndString", MapType(structOfString, StringType))
      .add("mapOfStruct", MapType(structOfString, structOfString)))

  encodeDecodeTest(
    new StructType()
      .add("structOfString", structOfString)
      .add("structOfStructOfString", new StructType().add("struct", structOfString))
      .add("structOfArray", new StructType().add("array", arrayOfString))
      .add("structOfMap", new StructType().add("map", mapOfString))
      .add("structOfArrayAndMap",
        new StructType().add("array", arrayOfString).add("map", mapOfString)))

  private def encodeDecodeTest(schema: StructType): Unit = {
    test(s"encode/decode: ${schema.simpleString}") {
      val encoder = RowEncoder(schema)
      val inputGenerator = RandomDataGenerator.forType(schema, nullable = false).get

      var input: Row = null
      try {
        for (_ <- 1 to 5) {
          input = inputGenerator.apply().asInstanceOf[Row]
          val row = encoder.toRow(input)
          val convertedBack = encoder.fromRow(row)
          assert(input == convertedBack)
        }
      } catch {
        case e: Exception =>
          fail(
            s"""
               |schema: ${schema.simpleString}
               |input: ${input}
             """.stripMargin, e)
      }
    }
  }
}
