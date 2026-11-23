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

package org.apache.spark.sql.catalyst.json

import java.io.CharArrayWriter

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.types._

class JacksonGeneratorSuite extends SparkFunSuite {

  val utcId = DateTimeUtils.TimeZoneUTC.getID
  val option = new JSONOptions(Map.empty, utcId)

  test("initial with StructType and write out a row") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = InternalRow(1)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """{"a":1}""")
  }

  test("SPARK-29444: initial with StructType and write out an empty row " +
      "with ignoreNullFields=false") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = InternalRow(null)
    val writer = new CharArrayWriter()
    val allowNullOption =
      new JSONOptions(Map("ignoreNullFields" -> "false"), utcId)
    val gen = new JacksonGenerator(dataType, writer, allowNullOption)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """{"a":null}""")
  }

  test("SPARK-29444: initial with StructType field and write out a row " +
    "with ignoreNullFields=false and struct inner null") {
    val fieldType = StructType(StructField("b", IntegerType) :: Nil)
    val dataType = StructType(StructField("a", fieldType) :: Nil)
    val input = InternalRow(InternalRow(null))
    val writer = new CharArrayWriter()
    val allowNullOption =
      new JSONOptions(Map("ignoreNullFields" -> "false"), utcId)
    val gen = new JacksonGenerator(dataType, writer, allowNullOption)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """{"a":{"b":null}}""")
  }

  test("initial with StructType and write out rows") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = new GenericArrayData(InternalRow(1) :: InternalRow(2) :: Nil)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """[{"a":1},{"a":2}]""")
  }

  test("initial with StructType and write out an array with single empty row") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = new GenericArrayData(InternalRow(null) :: Nil)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """[{}]""")
  }

  test("initial with StructType and write out an empty array") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = new GenericArrayData(Nil)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """[]""")
  }

  test("initial with Map and write out a map data") {
    val dataType = MapType(StringType, IntegerType)
    val input = ArrayBasedMapData(Map("a" -> 1))
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """{"a":1}""")
  }

  test("initial with Map and write out an array of maps") {
    val dataType = MapType(StringType, IntegerType)
    val input = new GenericArrayData(
      ArrayBasedMapData(Map("a" -> 1)) :: ArrayBasedMapData(Map("b" -> 2)) :: Nil)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """[{"a":1},{"b":2}]""")
  }

  test("write BINARY map keys in base64, the encoding of BINARY values") {
    def toJson(dataType: DataType, write: JacksonGenerator => Unit): String = {
      val writer = new CharArrayWriter()
      val gen = new JacksonGenerator(dataType, writer, option)
      write(gen)
      gen.flush()
      writer.toString
    }
    val cafe = Array[Byte](0xCA.toByte, 0xFE.toByte)
    // Long enough that an encoding with line breaks would break the name.
    val long = Array.tabulate[Byte](100)(_.toByte)
    val longBase64 = java.util.Base64.getEncoder.encodeToString(long)
    // BINARY values are written in base64.
    val valueType = StructType(StructField("c", BinaryType) :: StructField("l", BinaryType) :: Nil)
    assert(toJson(valueType, _.write(InternalRow(cafe, long))) ===
      s"""{"c":"yv4=","l":"$longBase64"}""")
    // Map keys use the same encoding. The keys are copies, so equal bytes give equal names.
    val mapType = MapType(BinaryType, IntegerType)
    val map = ArrayBasedMapData(Array[Any](cafe.clone(), long.clone()), Array[Any](1, 2))
    val mapJson = s"""{"yv4=":1,"$longBase64":2}"""
    assert(toJson(mapType, _.write(map)) === mapJson)
    // The same holds for a map in a struct or in an array.
    val structType = StructType(StructField("m", mapType) :: Nil)
    assert(toJson(structType, _.write(InternalRow(map))) === s"""{"m":$mapJson}""")
    assert(toJson(mapType, _.write(new GenericArrayData(Array[Any](map)))) === s"[$mapJson]")
  }

  test("sort BINARY map keys by their base64 names with sortKeys") {
    val sortKeysOption = new JSONOptions(Map("sortKeys" -> "true"), utcId)
    def toJson(map: ArrayBasedMapData, keyType: DataType, options: JSONOptions): String = {
      val writer = new CharArrayWriter()
      val gen = new JacksonGenerator(MapType(keyType, IntegerType), writer, options)
      gen.write(map)
      gen.flush()
      writer.toString
    }
    // X'00', X'7F' and X'FF' are AA==, fw== and /w== in base64.
    val binaryKeys = ArrayBasedMapData(
      Array[Any](Array[Byte](0), Array[Byte](0x7F), Array[Byte](0xFF.toByte)),
      Array[Any](1, 2, 3))
    assert(toJson(binaryKeys, BinaryType, option) === """{"AA==":1,"fw==":2,"/w==":3}""")
    assert(toJson(binaryKeys, BinaryType, sortKeysOption) === """{"/w==":3,"AA==":1,"fw==":2}""")
    // Keys of other types are still written and sorted by their `toString`.
    val intKeys = ArrayBasedMapData(Array[Any](9, 10), Array[Any](1, 2))
    assert(toJson(intKeys, IntegerType, option) === """{"9":1,"10":2}""")
    assert(toJson(intKeys, IntegerType, sortKeysOption) === """{"10":2,"9":1}""")
  }

  test("error handling: initial with StructType but error calling write a map") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = ArrayBasedMapData(Map("a" -> 1))
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    intercept[SparkUnsupportedOperationException] {
      gen.write(input)
    }
  }

  test("error handling: initial with MapType and write out a row") {
    val dataType = MapType(StringType, IntegerType)
    val input = InternalRow(1)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    intercept[SparkUnsupportedOperationException] {
      gen.write(input)
    }
  }

}
