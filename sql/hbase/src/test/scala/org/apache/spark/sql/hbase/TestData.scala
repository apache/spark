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

package org.apache.spark.sql.hbase

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.plans.logical

case class TestData(k: Int, v: String)

object TestData {
  import TestHbase._
  
  val testData = TestHbase.sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i.toString))).toSchemaRDD
  testData.registerTempTable("testData")

  val negativeData = TestHbase.sparkContext.parallelize(
    (1 to 100).map(i => TestData(-i, (-i).toString))).toSchemaRDD
  negativeData.registerTempTable("negativeData")

  case class LargeAndSmallInts(a: Int, b: Int)

  val largeAndSmallInts =
    TestHbase.sparkContext.parallelize(
      LargeAndSmallInts(2147483644, 1) ::
        LargeAndSmallInts(1, 2) ::
        LargeAndSmallInts(2147483645, 1) ::
        LargeAndSmallInts(2, 2) ::
        LargeAndSmallInts(2147483646, 1) ::
        LargeAndSmallInts(3, 2) :: Nil).toSchemaRDD
  largeAndSmallInts.registerTempTable("largeAndSmallInts")

  case class TestData2(a: Int, b: Int)

  val testData2 =
    TestHbase.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 1) ::
        TestData2(2, 2) ::
        TestData2(3, 1) ::
        TestData2(3, 2) :: Nil).toSchemaRDD
  testData2.registerTempTable("testData2")

  case class DecimalData(a: BigDecimal, b: BigDecimal)

  val decimalData =
    TestHbase.sparkContext.parallelize(
      DecimalData(1, 1) ::
        DecimalData(1, 2) ::
        DecimalData(2, 1) ::
        DecimalData(2, 2) ::
        DecimalData(3, 1) ::
        DecimalData(3, 2) :: Nil).toSchemaRDD
  decimalData.registerTempTable("decimalData")

  case class BinaryData(a: Array[Byte], b: Int)

  val binaryData =
    TestHbase.sparkContext.parallelize(
      BinaryData("12".getBytes, 1) ::
        BinaryData("22".getBytes, 5) ::
        BinaryData("122".getBytes, 3) ::
        BinaryData("121".getBytes, 2) ::
        BinaryData("123".getBytes, 4) :: Nil).toSchemaRDD
  binaryData.registerTempTable("binaryData")

  case class TestData3(a: Int, b: Option[Int])

  val testData3 =
    TestHbase.sparkContext.parallelize(
      TestData3(1, None) ::
        TestData3(2, Some(2)) :: Nil).toSchemaRDD
  testData3.registerTempTable("testData3")

  val emptyTableData = logical.LocalRelation('a.int, 'b.int)

  case class UpperCaseData(N: Int, L: String)

  val upperCaseData =
    TestHbase.sparkContext.parallelize(
      UpperCaseData(1, "A") ::
        UpperCaseData(2, "B") ::
        UpperCaseData(3, "C") ::
        UpperCaseData(4, "D") ::
        UpperCaseData(5, "E") ::
        UpperCaseData(6, "F") :: Nil).toSchemaRDD
  upperCaseData.registerTempTable("upperCaseData")

  case class LowerCaseData(n: Int, l: String)

  val lowerCaseData =
    TestHbase.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
        LowerCaseData(2, "b") ::
        LowerCaseData(3, "c") ::
        LowerCaseData(4, "d") :: Nil).toSchemaRDD
  lowerCaseData.registerTempTable("lowerCaseData")

  case class ArrayData(dt: Seq[Int], nestedData: Seq[Seq[Int]])

  val arrayData =
    TestHbase.sparkContext.parallelize(
      ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3))) ::
        ArrayData(Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
  arrayData.registerTempTable("arrayData")

  case class MapData(data: scala.collection.Map[Int, String])

  val mapData =
    TestHbase.sparkContext.parallelize(
      MapData(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
        MapData(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
        MapData(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
        MapData(Map(1 -> "a4", 2 -> "b4")) ::
        MapData(Map(1 -> "a5")) :: Nil)
  mapData.registerTempTable("mapData")

  case class StringData(s: String)

  val repeatedData =
    TestHbase.sparkContext.parallelize(List.fill(2)(StringData("test")))
  repeatedData.registerTempTable("repeatedData")

  val nullableRepeatedData =
    TestHbase.sparkContext.parallelize(
      List.fill(2)(StringData(null)) ++
        List.fill(2)(StringData("test")))
  nullableRepeatedData.registerTempTable("nullableRepeatedData")

  case class NullInts(a: Integer)

  val nullInts =
    TestHbase.sparkContext.parallelize(
      NullInts(1) ::
        NullInts(2) ::
        NullInts(3) ::
        NullInts(null) :: Nil
    )
  nullInts.registerTempTable("nullInts")

  val allNulls =
    TestHbase.sparkContext.parallelize(
      NullInts(null) ::
        NullInts(null) ::
        NullInts(null) ::
        NullInts(null) :: Nil)
  allNulls.registerTempTable("allNulls")

  case class NullStrings(n: Int, s: String)

  val nullStrings =
    TestHbase.sparkContext.parallelize(
      NullStrings(1, "abc") ::
        NullStrings(2, "ABC") ::
        NullStrings(3, null) :: Nil)
  nullStrings.registerTempTable("nullStrings")

  case class TableName(tableName: String)

  TestHbase.sparkContext.parallelize(TableName("test") :: Nil).registerTempTable("tableName")

  val unparsedStrings =
    TestHbase.sparkContext.parallelize(
      "1, A1, true, null" ::
        "2, B2, false, null" ::
        "3, C3, true, null" ::
        "4, D4, true, 2147483644" :: Nil)

  case class TimestampField(time: Timestamp)

  val timestamps = TestHbase.sparkContext.parallelize((1 to 3).map { i =>
    TimestampField(new Timestamp(i))
  })
  timestamps.registerTempTable("timestamps")

  case class IntField(i: Int)

  // An RDD with 4 elements and 8 partitions
  val withEmptyParts = TestHbase.sparkContext.parallelize((1 to 4).map(IntField), 8)
  withEmptyParts.registerTempTable("withEmptyParts")

  case class Person(id: Int, name: String, age: Int)

  case class Salary(personId: Int, salary: Double)

  val person = TestHbase.sparkContext.parallelize(
    Person(0, "mike", 30) ::
      Person(1, "jim", 20) :: Nil)
  person.registerTempTable("person")
  val salary = TestHbase.sparkContext.parallelize(
    Salary(0, 2000.0) ::
      Salary(1, 1000.0) :: Nil)
  salary.registerTempTable("salary")

  case class ComplexData(m: Map[Int, String], s: TestData, a: Seq[Int], b: Boolean)

  val complexData =
    TestHbase.sparkContext.parallelize(
      ComplexData(Map(1 -> "1"), TestData(1, "1"), Seq(1), b = true)
        :: ComplexData(Map(2 -> "2"), TestData(2, "2"), Seq(2), b = false)
        :: Nil).toSchemaRDD
  complexData.registerTempTable("complexData")
}
