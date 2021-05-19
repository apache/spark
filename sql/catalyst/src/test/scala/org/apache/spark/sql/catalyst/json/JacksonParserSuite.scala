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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, IntervalUtils}
import org.apache.spark.sql.sources.{EqualTo, Filter, StringStartsWith}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class JacksonParserSuite extends SparkFunSuite {

  def check(input: String = """{"i":1, "s": "a"}""",
      schema: DataType = StructType.fromDDL("i INTEGER"),
      filters: Seq[Filter],
      expected: Seq[InternalRow],
      compare: (Iterable[InternalRow], Iterable[InternalRow]) => Boolean = defaultCompare )
  : Unit = {
    val options = new JSONOptions(Map.empty[String, String], "GMT", "")
    val parser = new JacksonParser(schema, options, false, filters)
    val createParser = CreateJacksonParser.string _
    val actual = parser.parse(input, createParser, UTF8String.fromString)
    assert(compare(actual, expected))
  }

  def defaultCompare: (Iterable[InternalRow], Iterable[InternalRow]) => Boolean = {
    (it1, it2) => it1 === it2
  }

  def compareArrayBasedMapData: (Iterable[InternalRow], Iterable[InternalRow]) => Boolean = {
    (it1, it2) =>
      val mapIt1 = it1.map(_.getMap(0))
      val mapIt2 = it2.map(_.getMap(0))
      mapIt1.map(_.keyArray()) == mapIt2.map(_.keyArray()) &&
        mapIt1.map(_.valueArray()) == mapIt2.map(_.valueArray())
  }

  test("skipping rows using pushdown filters") {
    check(filters = Seq(), expected = Seq(InternalRow(1)))
    check(filters = Seq(EqualTo("i", 1)), expected = Seq(InternalRow(1)))
    check(filters = Seq(EqualTo("i", 2)), expected = Seq.empty)
    check(
      schema = StructType.fromDDL("s STRING"),
      filters = Seq(StringStartsWith("s", "b")),
      expected = Seq.empty)
    check(
      schema = StructType.fromDDL("i INTEGER, s STRING"),
      filters = Seq(StringStartsWith("s", "a")),
      expected = Seq(InternalRow(1, UTF8String.fromString("a"))))
    check(
      input = """{"i":1,"s": "a", "d": 3.14}""",
      schema = StructType.fromDDL("i INTEGER, d DOUBLE"),
      filters = Seq(EqualTo("d", 3.14)),
      expected = Seq(InternalRow(1, 3.14)))
  }

  test("JacksonParser ArrayBasedMapData BooleanType key parser") {
    check(s"""{"True": 1}""",
      MapType(BooleanType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(true), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData ByteType key parser") {
    check(s"""{"1": 1}""",
      MapType(ByteType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(1.toByte), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData ShortType key parser") {
    check(s"""{"1": 1}""",
      MapType(ShortType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(1.toShort), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData IntegerType key parser") {
    check(s"""{"1": 1}""",
      MapType(IntegerType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(1), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData LongType key parser") {
    check(s"""{"1": 1}""",
      MapType(LongType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(1L), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData FloatType key parser") {
    check(s"""{"1": 1}""",
      MapType(FloatType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(1f), Array(1)))),
      compareArrayBasedMapData
    )
    check(s"""{"NaN": 1}""",
      MapType(FloatType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(Float.NaN), Array(1)))),
      compareArrayBasedMapData
    )
    check(s"""{"Infinity": 1}""",
      MapType(FloatType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(Float.PositiveInfinity), Array(1)))),
      compareArrayBasedMapData
    )
    check(s"""{"-Infinity": 1}""",
      MapType(FloatType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(Float.NegativeInfinity), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData DoubleType key parser") {
    check(s"""{"1": 1}""",
      MapType(DoubleType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(1d), Array(1)))),
      compareArrayBasedMapData
    )
    check(s"""{"NaN": 1}""",
      MapType(DoubleType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(Double.NaN), Array(1)))),
      compareArrayBasedMapData
    )
    check(s"""{"Infinity": 1}""",
      MapType(DoubleType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(Double.PositiveInfinity), Array(1)))),
      compareArrayBasedMapData
    )
    check(s"""{"-Infinity": 1}""",
      MapType(DoubleType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(Double.NegativeInfinity), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData TimestampType key parser") {
    check(s"""{"1970-01-01T00:00:00": 1}""",
      MapType(TimestampType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(0L), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData DateType key parser") {
    check(s"""{"1970-01-01": 1}""",
      MapType(DateType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(0), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData DecimalType key parser") {
    val decimal = Decimal(20)
    check(s"""{"$decimal": 1}""",
      MapType(DecimalType.IntDecimal, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(decimal), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData CalendarIntervalType key parser") {
    val interval =
      IntervalUtils.makeInterval(2, 1, 1, 0, 0, 0, Decimal(0, Decimal.MAX_LONG_DIGITS, 6))
    check(s"""{"$interval": 1}""",
      MapType(CalendarIntervalType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(interval), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData UDT key parser") {
    object TestJsonKeyUDT extends UserDefinedType[String] {
      override def sqlType: DataType = StringType
      override def serialize(obj: String): Any = obj
      override def deserialize(datum: Any): String = datum.toString
      override def userClass: Class[String] = classOf[String]
    }

    val valueString = "test"
    check(s"""{"$valueString": 1}""",
      MapType(TestJsonKeyUDT, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(UTF8String.fromString(valueString)), Array(1)))),
      compareArrayBasedMapData
    )
  }

  test("JacksonParser ArrayBasedMapData StringType key parser") {
    val valueString = "test"
    check(s"""{"$valueString": 1}""",
      MapType(StringType, IntegerType), Seq(),
      Seq(InternalRow(ArrayBasedMapData(Array(UTF8String.fromString(valueString)), Array(1)))),
      compareArrayBasedMapData
    )
  }
}
