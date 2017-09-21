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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.types._

class JacksonGeneratorSuite extends SparkFunSuite {

  val gmtId = DateTimeUtils.TimeZoneGMT.getID
  val option = new JSONOptions(Map.empty, gmtId)

  test("initial with StructType and write out a row") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = InternalRow(1)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    gen.write(input)
    gen.flush()
    assert(writer.toString === """{"a":1}""")
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

  test("error handling: initial with StructType but error calling write a map") {
    val dataType = StructType(StructField("a", IntegerType) :: Nil)
    val input = ArrayBasedMapData(Map("a" -> 1))
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    intercept[UnsupportedOperationException] {
      gen.write(input)
    }
  }

  test("error handling: initial with MapType and write out a row") {
    val dataType = MapType(StringType, IntegerType)
    val input = InternalRow(1)
    val writer = new CharArrayWriter()
    val gen = new JacksonGenerator(dataType, writer, option)
    intercept[UnsupportedOperationException] {
      gen.write(input)
    }
  }

}
