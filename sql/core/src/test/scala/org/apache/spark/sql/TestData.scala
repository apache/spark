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

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.test._

/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

case class TestData(key: Int, value: String)

object TestData {
  val testData: SchemaRDD = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i.toString)))
  testData.registerTempTable("testData")

  case class LargeAndSmallInts(a: Int, b: Int)
  val largeAndSmallInts: SchemaRDD =
    TestSQLContext.sparkContext.parallelize(
      LargeAndSmallInts(2147483644, 1) ::
      LargeAndSmallInts(1, 2) ::
      LargeAndSmallInts(2147483645, 1) ::
      LargeAndSmallInts(2, 2) ::
      LargeAndSmallInts(2147483646, 1) ::
      LargeAndSmallInts(3, 2) :: Nil)
  largeAndSmallInts.registerTempTable("largeAndSmallInts")

  case class TestData2(a: Int, b: Int)
  val testData2: SchemaRDD =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
      TestData2(1, 2) ::
      TestData2(2, 1) ::
      TestData2(2, 2) ::
      TestData2(3, 1) ::
      TestData2(3, 2) :: Nil)
  testData2.registerTempTable("testData2")

  // TODO: There is no way to express null primitives as case classes currently...
  val testData3 =
    logical.LocalRelation('a.int, 'b.int).loadData(
      (1, null) ::
      (2, 2) :: Nil)

  val emptyTableData = logical.LocalRelation('a.int, 'b.int)

  case class UpperCaseData(N: Int, L: String)
  val upperCaseData =
    TestSQLContext.sparkContext.parallelize(
      UpperCaseData(1, "A") ::
      UpperCaseData(2, "B") ::
      UpperCaseData(3, "C") ::
      UpperCaseData(4, "D") ::
      UpperCaseData(5, "E") ::
      UpperCaseData(6, "F") :: Nil)
  upperCaseData.registerTempTable("upperCaseData")

  case class LowerCaseData(n: Int, l: String)
  val lowerCaseData =
    TestSQLContext.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
      LowerCaseData(2, "b") ::
      LowerCaseData(3, "c") ::
      LowerCaseData(4, "d") :: Nil)
  lowerCaseData.registerTempTable("lowerCaseData")

  case class ArrayData(data: Seq[Int], nestedData: Seq[Seq[Int]])
  val arrayData =
    TestSQLContext.sparkContext.parallelize(
      ArrayData(Seq(1,2,3), Seq(Seq(1,2,3))) ::
      ArrayData(Seq(2,3,4), Seq(Seq(2,3,4))) :: Nil)
  arrayData.registerTempTable("arrayData")

  case class MapData(data: Map[Int, String])
  val mapData =
    TestSQLContext.sparkContext.parallelize(
      MapData(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      MapData(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      MapData(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      MapData(Map(1 -> "a4", 2 -> "b4")) ::
      MapData(Map(1 -> "a5")) :: Nil)
  mapData.registerTempTable("mapData")

  case class StringData(s: String)
  val repeatedData =
    TestSQLContext.sparkContext.parallelize(List.fill(2)(StringData("test")))
  repeatedData.registerTempTable("repeatedData")

  val nullableRepeatedData =
    TestSQLContext.sparkContext.parallelize(
      List.fill(2)(StringData(null)) ++
      List.fill(2)(StringData("test")))
  nullableRepeatedData.registerTempTable("nullableRepeatedData")

  case class NullInts(a: Integer)
  val nullInts =
    TestSQLContext.sparkContext.parallelize(
      NullInts(1) ::
      NullInts(2) ::
      NullInts(3) ::
      NullInts(null) :: Nil
    )
  nullInts.registerTempTable("nullInts")

  val allNulls =
    TestSQLContext.sparkContext.parallelize(
      NullInts(null) ::
      NullInts(null) ::
      NullInts(null) ::
      NullInts(null) :: Nil)
  allNulls.registerTempTable("allNulls")

  case class NullStrings(n: Int, s: String)
  val nullStrings =
    TestSQLContext.sparkContext.parallelize(
      NullStrings(1, "abc") ::
      NullStrings(2, "ABC") ::
      NullStrings(3, null) :: Nil)
  nullStrings.registerTempTable("nullStrings")

  case class TableName(tableName: String)
  TestSQLContext.sparkContext.parallelize(TableName("test") :: Nil).registerTempTable("tableName")

  val unparsedStrings =
    TestSQLContext.sparkContext.parallelize(
      "1, A1, true, null" ::
      "2, B2, false, null" ::
      "3, C3, true, null" ::
      "4, D4, true, 2147483644" :: Nil)

  case class TimestampField(time: Timestamp)
  val timestamps = TestSQLContext.sparkContext.parallelize((1 to 3).map { i =>
    TimestampField(new Timestamp(i))
  })
  timestamps.registerTempTable("timestamps")

  case class IntField(i: Int)
  // An RDD with 4 elements and 8 partitions
  val withEmptyParts = TestSQLContext.sparkContext.parallelize((1 to 4).map(IntField), 8)
  withEmptyParts.registerTempTable("withEmptyParts")
}
