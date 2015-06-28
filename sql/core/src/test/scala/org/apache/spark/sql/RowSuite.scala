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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.SparkSqlSerializer

import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, SpecificMutableRow}
import org.apache.spark.sql.types._

class RowSuite extends SparkFunSuite {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("create row") {
    val expected = new GenericMutableRow(4)
    expected.update(0, 2147483647)
    expected.setString(1, "this is a string")
    expected.update(2, false)
    expected.update(3, null)
    val actual1 = Row(2147483647, "this is a string", false, null)
    assert(expected.size === actual1.size)
    assert(expected.getInt(0) === actual1.getInt(0))
    assert(expected.getString(1) === actual1.getString(1))
    assert(expected.getBoolean(2) === actual1.getBoolean(2))
    assert(expected(3) === actual1(3))

    val actual2 = Row.fromSeq(Seq(2147483647, "this is a string", false, null))
    assert(expected.size === actual2.size)
    assert(expected.getInt(0) === actual2.getInt(0))
    assert(expected.getString(1) === actual2.getString(1))
    assert(expected.getBoolean(2) === actual2.getBoolean(2))
    assert(expected(3) === actual2(3))
  }

  test("SpecificMutableRow.update with null") {
    val row = new SpecificMutableRow(Seq(IntegerType))
    row(0) = null
    assert(row.isNullAt(0))
  }

  test("serialize w/ kryo") {
    val row = Seq((1, Seq(1), Map(1 -> 1), BigDecimal(1))).toDF().first()
    val serializer = new SparkSqlSerializer(ctx.sparkContext.getConf)
    val instance = serializer.newInstance()
    val ser = instance.serialize(row)
    val de = instance.deserialize(ser).asInstanceOf[Row]
    assert(de === row)
  }

  test("get values by field name on Row created via .toDF") {
    val row = Seq((1, Seq(1))).toDF("a", "b").first()
    assert(row.getAs[Int]("a") === 1)
    assert(row.getAs[Seq[Int]]("b") === Seq(1))

    intercept[IllegalArgumentException]{
      row.getAs[Int]("c")
    }
  }

  test("compare field values between by name and by index") {
    val row = Seq((
      "string1", // Any
      true, // Boolean
      127, // Byte
      255, // Short
      2147483647, // Int
      9223372036854775807L, // Long
      2.3F, // Float
      0.0000001, // Double
      "string2", // String
      new java.math.BigDecimal("9223372036854775808"), // Decimal
      new java.sql.Date(1435475728001L), // Date
      new java.sql.Timestamp(1435475728001L), // Timestamp
      (1 to 100).toArray, // Seq
      (1 to 200).toArray, // List
      Map(1 -> "Scala", 2 -> "Java", 3 -> "Python", 4 -> "SQL", 5 -> "R"), // Map
      Map("Tokyo" -> "Shinjuku", "Okinawa" -> "Okinawa", "Hokkaido" -> "Hokkaido"), // Java Map
      null: String // null
    )).toDF(
      "anyCol",
      "booleanCol",
      "byteCol",
      "shortCol",
      "intCol",
      "longCol",
      "floatCol",
      "doubleCol",
      "stringCol",
      "decimalCol",
      "dateCol",
      "timestampCol",
      "seqCol",
      "listCol",
      "mapCol",
      "jmapCol",
      "nullCol"
    ).first()

    assert(row.get(0) === row.get("anyCol"))
    assert(row.get(1) === row.get("booleanCol"))
    assert(row.get(2) === row.get("byteCol"))
    assert(row.get(3) === row.get("shortCol"))
    assert(row.get(4) === row.get("intCol"))
    assert(row.get(5) === row.get("longCol"))
    assert(row.get(6) === row.get("floatCol"))
    assert(row.get(7) === row.get("doubleCol"))
    assert(row.get(8) === row.get("stringCol"))
    assert(row.get(9) === row.get("decimalCol"))
    assert(row.get(10) === row.get("dateCol"))
    assert(row.get(11) === row.get("timestampCol"))
    assert(row.get(12) === row.get("seqCol"))
    assert(row.get(13) === row.get("listCol"))
    assert(row.get(14) === row.get("mapCol"))
    assert(row.get(15) === row.get("jmapCol"))
    assert(row.isNullAt(16) === true)
    assert(row.isNullAt("nullCol") === true)
  }
}
