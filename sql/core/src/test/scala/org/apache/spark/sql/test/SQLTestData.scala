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

package org.apache.spark.sql.test

import java.nio.charset.StandardCharsets
import java.time.{Duration, Period}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, SQLImplicits}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * A collection of sample data used in SQL tests.
 */
private[sql] trait SQLTestData { self =>
  protected def spark: SparkSession

  // Helper object to import SQL implicits without a concrete SparkSession
  private object internalImplicits extends SQLImplicits {
    override protected def session: SparkSession = self.spark
  }

  import internalImplicits._
  import SQLTestData._

  // Note: all test data should be lazy because the SparkSession is not set up yet.

  protected lazy val emptyTestData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      Seq.empty[Int].map(i => TestData(i, i.toString))).toDF()
    df.createOrReplaceTempView("emptyTestData")
    df
  }

  protected lazy val testData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString))).toDF()
    df.createOrReplaceTempView("testData")
    df
  }

  protected lazy val testData2: DataFrame = {
    val df = spark.sparkContext.parallelize(
      TestData2(1, 1) ::
      TestData2(1, 2) ::
      TestData2(2, 1) ::
      TestData2(2, 2) ::
      TestData2(3, 1) ::
      TestData2(3, 2) :: Nil, 2).toDF()
    df.createOrReplaceTempView("testData2")
    df
  }

  protected lazy val testData3: DataFrame = {
    val df = spark.sparkContext.parallelize(
      TestData3(1, None) ::
      TestData3(2, Some(2)) :: Nil).toDF()
    df.createOrReplaceTempView("testData3")
    df
  }

  protected lazy val negativeData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(-i, (-i).toString))).toDF()
    df.createOrReplaceTempView("negativeData")
    df
  }

  protected lazy val largeAndSmallInts: DataFrame = {
    val df = spark.sparkContext.parallelize(
      LargeAndSmallInts(2147483644, 1) ::
      LargeAndSmallInts(1, 2) ::
      LargeAndSmallInts(2147483645, 1) ::
      LargeAndSmallInts(2, 2) ::
      LargeAndSmallInts(2147483646, 1) ::
      LargeAndSmallInts(3, 2) :: Nil).toDF()
    df.createOrReplaceTempView("largeAndSmallInts")
    df
  }

  protected lazy val decimalData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      DecimalData(1, 1) ::
      DecimalData(1, 2) ::
      DecimalData(2, 1) ::
      DecimalData(2, 2) ::
      DecimalData(3, 1) ::
      DecimalData(3, 2) :: Nil).toDF()
    df.createOrReplaceTempView("decimalData")
    df
  }

  protected lazy val binaryData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      BinaryData("12".getBytes(StandardCharsets.UTF_8), 1) ::
      BinaryData("22".getBytes(StandardCharsets.UTF_8), 5) ::
      BinaryData("122".getBytes(StandardCharsets.UTF_8), 3) ::
      BinaryData("121".getBytes(StandardCharsets.UTF_8), 2) ::
      BinaryData("123".getBytes(StandardCharsets.UTF_8), 4) :: Nil).toDF()
    df.createOrReplaceTempView("binaryData")
    df
  }

  protected lazy val upperCaseData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      UpperCaseData(1, "A") ::
      UpperCaseData(2, "B") ::
      UpperCaseData(3, "C") ::
      UpperCaseData(4, "D") ::
      UpperCaseData(5, "E") ::
      UpperCaseData(6, "F") :: Nil).toDF()
    df.createOrReplaceTempView("upperCaseData")
    df
  }

  protected lazy val lowerCaseData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
      LowerCaseData(2, "b") ::
      LowerCaseData(3, "c") ::
      LowerCaseData(4, "d") :: Nil).toDF()
    df.createOrReplaceTempView("lowerCaseData")
    df
  }

  protected lazy val lowerCaseDataWithDuplicates: DataFrame = {
    val df = spark.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
      LowerCaseData(2, "b") ::
      LowerCaseData(2, "b") ::
      LowerCaseData(3, "c") ::
      LowerCaseData(3, "c") ::
      LowerCaseData(3, "c") ::
      LowerCaseData(4, "d") :: Nil).toDF()
    df.createOrReplaceTempView("lowerCaseData")
    df
  }

  protected lazy val arrayData: RDD[ArrayData] = {
    val rdd = spark.sparkContext.parallelize(
      ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3))) ::
      ArrayData(Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
    rdd.toDF().createOrReplaceTempView("arrayData")
    rdd
  }

  protected lazy val mapData: RDD[MapData] = {
    val rdd = spark.sparkContext.parallelize(
      MapData(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      MapData(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      MapData(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      MapData(Map(1 -> "a4", 2 -> "b4")) ::
      MapData(Map(1 -> "a5")) :: Nil)
    rdd.toDF().createOrReplaceTempView("mapData")
    rdd
  }

  protected lazy val calendarIntervalData: RDD[IntervalData] = {
    val rdd = spark.sparkContext.parallelize(
      IntervalData(new CalendarInterval(1, 1, 1)) :: Nil)
    rdd.toDF().createOrReplaceTempView("calendarIntervalData")
    rdd
  }

  protected lazy val repeatedData: RDD[StringData] = {
    val rdd = spark.sparkContext.parallelize(List.fill(2)(StringData("test")))
    rdd.toDF().createOrReplaceTempView("repeatedData")
    rdd
  }

  protected lazy val nullableRepeatedData: RDD[StringData] = {
    val rdd = spark.sparkContext.parallelize(
      List.fill(2)(StringData(null)) ++
      List.fill(2)(StringData("test")))
    rdd.toDF().createOrReplaceTempView("nullableRepeatedData")
    rdd
  }

  protected lazy val nullInts: DataFrame = {
    val df = spark.sparkContext.parallelize(
      NullInts(1) ::
      NullInts(2) ::
      NullInts(3) ::
      NullInts(null) :: Nil).toDF()
    df.createOrReplaceTempView("nullInts")
    df
  }

  protected lazy val allNulls: DataFrame = {
    val df = spark.sparkContext.parallelize(
      NullInts(null) ::
      NullInts(null) ::
      NullInts(null) ::
      NullInts(null) :: Nil).toDF()
    df.createOrReplaceTempView("allNulls")
    df
  }

  protected lazy val nullStrings: DataFrame = {
    val df = spark.sparkContext.parallelize(
      NullStrings(1, "abc") ::
      NullStrings(2, "ABC") ::
      NullStrings(3, null) :: Nil).toDF()
    df.createOrReplaceTempView("nullStrings")
    df
  }

  protected lazy val tableName: DataFrame = {
    val df = spark.sparkContext.parallelize(TableName("test") :: Nil).toDF()
    df.createOrReplaceTempView("tableName")
    df
  }

  protected lazy val unparsedStrings: RDD[String] = {
    spark.sparkContext.parallelize(
      "1, A1, true, null" ::
      "2, B2, false, null" ::
      "3, C3, true, null" ::
      "4, D4, true, 2147483644" :: Nil)
  }

  // An RDD with 4 elements and 8 partitions
  protected lazy val withEmptyParts: RDD[IntField] = {
    val rdd = spark.sparkContext.parallelize((1 to 4).map(IntField), 8)
    rdd.toDF().createOrReplaceTempView("withEmptyParts")
    rdd
  }

  protected lazy val person: DataFrame = {
    val df = spark.sparkContext.parallelize(
      Person(0, "mike", 30) ::
      Person(1, "jim", 20) :: Nil).toDF()
    df.createOrReplaceTempView("person")
    df
  }

  protected lazy val salary: DataFrame = {
    val df = spark.sparkContext.parallelize(
      Salary(0, 2000.0) ::
      Salary(1, 1000.0) :: Nil).toDF()
    df.createOrReplaceTempView("salary")
    df
  }

  protected lazy val complexData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      ComplexData(Map("1" -> 1), TestData(1, "1"), Seq(1, 1, 1), true) ::
      ComplexData(Map("2" -> 2), TestData(2, "2"), Seq(2, 2, 2), false) ::
      Nil).toDF()
    df.createOrReplaceTempView("complexData")
    df
  }

  protected lazy val courseSales: DataFrame = {
    val df = spark.sparkContext.parallelize(
      CourseSales("dotNET", 2012, 10000) ::
        CourseSales("Java", 2012, 20000) ::
        CourseSales("dotNET", 2012, 5000) ::
        CourseSales("dotNET", 2013, 48000) ::
        CourseSales("Java", 2013, 30000) :: Nil).toDF()
    df.createOrReplaceTempView("courseSales")
    df
  }

  protected lazy val trainingSales: DataFrame = {
    val df = spark.sparkContext.parallelize(
      TrainingSales("Experts", CourseSales("dotNET", 2012, 10000)) ::
        TrainingSales("Experts", CourseSales("JAVA", 2012, 20000)) ::
        TrainingSales("Dummies", CourseSales("dotNet", 2012, 5000)) ::
        TrainingSales("Experts", CourseSales("dotNET", 2013, 48000)) ::
        TrainingSales("Dummies", CourseSales("Java", 2013, 30000)) :: Nil).toDF()
    df.createOrReplaceTempView("trainingSales")
    df
  }

  protected lazy val intervalData: DataFrame = Seq(
    (1,
      Period.ofMonths(10),
      Period.ofYears(8),
      Period.ofMonths(10),
      Duration.ofDays(7).plusHours(13).plusMinutes(3).plusSeconds(18),
      Duration.ofDays(5).plusHours(21).plusMinutes(12),
      Duration.ofDays(1).plusHours(8),
      Duration.ofDays(10),
      Duration.ofHours(20).plusMinutes(11).plusSeconds(33),
      Duration.ofHours(3).plusMinutes(18),
      Duration.ofHours(13),
      Duration.ofMinutes(2).plusSeconds(59),
      Duration.ofMinutes(38),
      Duration.ofSeconds(5)),
    (2,
      Period.ofMonths(1),
      Period.ofYears(1),
      Period.ofMonths(1),
      Duration.ofSeconds(1),
      Duration.ofMinutes(1),
      Duration.ofHours(1),
      Duration.ofDays(1),
      Duration.ofSeconds(1),
      Duration.ofMinutes(1),
      Duration.ofHours(1),
      Duration.ofSeconds(1),
      Duration.ofMinutes(1),
      Duration.ofSeconds(1)),
    (2, null, null, null, null, null, null, null, null, null, null, null, null, null),
    (3,
      Period.ofMonths(-3),
      Period.ofYears(-12),
      Period.ofMonths(-3),
      Duration.ofDays(-8).plusHours(-21).plusMinutes(-10).plusSeconds(-32),
      Duration.ofDays(-2).plusHours(-1).plusMinutes(-12),
      Duration.ofDays(-11).plusHours(-7),
      Duration.ofDays(-6),
      Duration.ofHours(-6).plusMinutes(-17).plusSeconds(-38),
      Duration.ofHours(-12).plusMinutes(-53),
      Duration.ofHours(-8),
      Duration.ofMinutes(-30).plusSeconds(-2),
      Duration.ofMinutes(-15),
      Duration.ofSeconds(-36)),
    (3,
      Period.ofMonths(21),
      Period.ofYears(30),
      Period.ofMonths(5),
      Duration.ofDays(11).plusHours(7).plusMinutes(36).plusSeconds(17),
      Duration.ofDays(19).plusHours(12).plusMinutes(25),
      Duration.ofDays(1).plusHours(14),
      Duration.ofDays(-5),
      Duration.ofHours(22).plusMinutes(8).plusSeconds(37),
      Duration.ofHours(10).plusMinutes(16),
      Duration.ofHours(5),
      Duration.ofMinutes(45).plusSeconds(5),
      Duration.ofMinutes(27),
      Duration.ofSeconds(50)),
    (3,
      null,
      Period.ofYears(1),
      null,
      null,
      Duration.ofMinutes(1),
      Duration.ofHours(1),
      Duration.ofDays(1),
      null,
      Duration.ofMinutes(1),
      Duration.ofHours(1),
      null,
      Duration.ofMinutes(1),
      null))
    .toDF("class",
      "year-month",
      "year",
      "month",
      "day-second",
      "day-minute",
      "day-hour",
      "day",
      "hour-second",
      "hour-minute",
      "hour",
      "minute-second",
      "minute",
      "second")
    .select(
      $"class",
      $"year-month",
      $"year" cast YearMonthIntervalType(YEAR) as "year",
      $"month" cast YearMonthIntervalType(MONTH) as "month",
      $"day-second",
      $"day-minute" cast DayTimeIntervalType(DAY, MINUTE) as "day-minute",
      $"day-hour" cast DayTimeIntervalType(DAY, HOUR) as "day-hour",
      $"day" cast DayTimeIntervalType(DAY) as "day",
      $"hour-second" cast DayTimeIntervalType(HOUR, SECOND) as "hour-second",
      $"hour-minute" cast DayTimeIntervalType(HOUR, MINUTE) as "hour-minute",
      $"hour" cast DayTimeIntervalType(HOUR) as "hour",
      $"minute-second" cast DayTimeIntervalType(MINUTE, SECOND) as "minute-second",
      $"minute" cast DayTimeIntervalType(MINUTE) as "minute",
      $"second" cast DayTimeIntervalType(SECOND) as "second")

  /**
   * Initialize all test data such that all temp tables are properly registered.
   */
  def loadTestData(): Unit = {
    assert(spark != null, "attempted to initialize test data before SparkSession.")
    emptyTestData
    testData
    testData2
    testData3
    negativeData
    largeAndSmallInts
    decimalData
    binaryData
    upperCaseData
    lowerCaseData
    arrayData
    mapData
    repeatedData
    nullableRepeatedData
    nullInts
    allNulls
    nullStrings
    tableName
    unparsedStrings
    withEmptyParts
    person
    salary
    complexData
    courseSales
  }
}

/**
 * Case classes used in test data.
 */
private[sql] object SQLTestData {
  case class TestData(key: Int, value: String)
  case class TestData2(a: Int, b: Int)
  case class TestData3(a: Int, b: Option[Int])
  case class LargeAndSmallInts(a: Int, b: Int)
  case class DecimalData(a: BigDecimal, b: BigDecimal)
  case class BinaryData(a: Array[Byte], b: Int)
  case class UpperCaseData(N: Int, L: String)
  case class LowerCaseData(n: Int, l: String)
  case class ArrayData(data: Seq[Int], nestedData: Seq[Seq[Int]])
  case class MapData(data: scala.collection.Map[Int, String])
  case class StringData(s: String)
  case class IntField(i: Int)
  case class NullInts(a: Integer)
  case class NullStrings(n: Int, s: String)
  case class TableName(tableName: String)
  case class Person(id: Int, name: String, age: Int)
  case class Salary(personId: Int, salary: Double)
  case class ComplexData(m: Map[String, Int], s: TestData, a: Seq[Int], b: Boolean)
  case class CourseSales(course: String, year: Int, earnings: Double)
  case class TrainingSales(training: String, sales: CourseSales)
  case class IntervalData(data: CalendarInterval)
  case class StringWrapper(s: String) extends AnyVal
  case class ArrayStringWrapper(wrappers: Seq[StringWrapper])
  case class ContainerStringWrapper(wrapper: StringWrapper)
}
