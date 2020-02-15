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

package org.apache.spark.sql.execution.datasources.parquet

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators}
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.Operators.{Column => _, _}

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2}

/**
 * A test suite that tests Parquet filter2 API based filter pushdown optimization.
 *
 * NOTE:
 *
 * 1. `!(a cmp b)` is always transformed to its negated form `a cmp' b` by the
 *    `BooleanSimplification` optimization rule whenever possible. As a result, predicate `!(a < 1)`
 *    results in a `GtEq` filter predicate rather than a `Not`.
 *
 * 2. `Tuple1(Option(x))` is used together with `AnyVal` types like `Int` to ensure the inferred
 *    data type is nullable.
 *
 * NOTE:
 *
 * This file intendedly enables record-level filtering explicitly. If new test cases are
 * dependent on this configuration, don't forget you better explicitly set this configuration
 * within the test.
 */
class ParquetFilterSuite extends QueryTest with ParquetTest with SharedSQLContext {

  private lazy val parquetFilters =
    new ParquetFilters(conf.parquetFilterPushDownDate, conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal, conf.parquetFilterPushDownStringStartWith,
      conf.parquetFilterPushDownInFilterThreshold, conf.caseSensitiveAnalysis)

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Note that there are many tests here that require record-level filtering set to be true.
    spark.conf.set(SQLConf.PARQUET_RECORD_FILTER_ENABLED.key, "true")
  }

  override def afterEach(): Unit = {
    try {
      spark.conf.unset(SQLConf.PARQUET_RECORD_FILTER_ENABLED.key)
    } finally {
      super.afterEach()
    }
  }

  private def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (DataFrame, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    withSQLConf(
      SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val query = df
          .select(output.map(e => Column(e)): _*)
          .where(Column(predicate))

        var maybeRelation: Option[HadoopFsRelation] = None
        val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
          case PhysicalOperation(_, filters,
                                 LogicalRelation(relation: HadoopFsRelation, _, _, _)) =>
            maybeRelation = Some(relation)
            filters
        }.flatten.reduceLeftOption(_ && _)
        assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

        val (_, selectedFilters, _) =
          DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
        assert(selectedFilters.nonEmpty, "No filter is pushed down")

        selectedFilters.foreach { pred =>
          val maybeFilter = parquetFilters.createFilter(
            new SparkToParquetSchemaConverter(conf).convert(df.schema), pred)
          assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          maybeFilter.exists(_.getClass === filterClass)
        }
        checker(stripSparkFilter(query), expected)
    }
  }

  private def checkFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
      (implicit df: DataFrame): Unit = {
    checkFilterPredicate(df, predicate, filterClass, checkAnswer(_, _: Seq[Row]), expected)
  }

  private def checkFilterPredicate[T]
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: T)
      (implicit df: DataFrame): Unit = {
    checkFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }

  private def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
      (implicit df: DataFrame): Unit = {
    def checkBinaryAnswer(df: DataFrame, expected: Seq[Row]) = {
      assertResult(expected.map(_.getAs[Array[Byte]](0).mkString(",")).sorted) {
        df.rdd.map(_.getAs[Array[Byte]](0).mkString(",")).collect().toSeq.sorted
      }
    }

    checkFilterPredicate(df, predicate, filterClass, checkBinaryAnswer _, expected)
  }

  private def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Array[Byte])
      (implicit df: DataFrame): Unit = {
    checkBinaryFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }

  private def testTimestampPushdown(data: Seq[Timestamp]): Unit = {
    assert(data.size === 4)
    val ts1 = data.head
    val ts2 = data(1)
    val ts3 = data(2)
    val ts4 = data(3)

    withParquetDataFrame(data.map(i => Tuple1(i))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], data.map(i => Row.apply(i)))

      checkFilterPredicate('_1 === ts1, classOf[Eq[_]], ts1)
      checkFilterPredicate('_1 <=> ts1, classOf[Eq[_]], ts1)
      checkFilterPredicate('_1 =!= ts1, classOf[NotEq[_]],
        Seq(ts2, ts3, ts4).map(i => Row.apply(i)))

      checkFilterPredicate('_1 < ts2, classOf[Lt[_]], ts1)
      checkFilterPredicate('_1 > ts1, classOf[Gt[_]], Seq(ts2, ts3, ts4).map(i => Row.apply(i)))
      checkFilterPredicate('_1 <= ts1, classOf[LtEq[_]], ts1)
      checkFilterPredicate('_1 >= ts4, classOf[GtEq[_]], ts4)

      checkFilterPredicate(Literal(ts1) === '_1, classOf[Eq[_]], ts1)
      checkFilterPredicate(Literal(ts1) <=> '_1, classOf[Eq[_]], ts1)
      checkFilterPredicate(Literal(ts2) > '_1, classOf[Lt[_]], ts1)
      checkFilterPredicate(Literal(ts3) < '_1, classOf[Gt[_]], ts4)
      checkFilterPredicate(Literal(ts1) >= '_1, classOf[LtEq[_]], ts1)
      checkFilterPredicate(Literal(ts4) <= '_1, classOf[GtEq[_]], ts4)

      checkFilterPredicate(!('_1 < ts4), classOf[GtEq[_]], ts4)
      checkFilterPredicate('_1 < ts2 || '_1 > ts3, classOf[Operators.Or], Seq(Row(ts1), Row(ts4)))
    }
  }

  private def testDecimalPushDown(data: DataFrame)(f: DataFrame => Unit): Unit = {
    withTempPath { file =>
      data.write.parquet(file.getCanonicalPath)
      readParquetFile(file.toString)(f)
    }
  }

  // This function tests that exactly go through the `canDrop` and `inverseCanDrop`.
  private def testStringStartsWith(dataFrame: DataFrame, filter: String): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      dataFrame.write.option("parquet.block.size", 512).parquet(path)
      Seq(true, false).foreach { pushDown =>
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED.key -> pushDown.toString) {
          val accu = new NumRowGroupsAcc
          sparkContext.register(accu)

          val df = spark.read.parquet(path).filter(filter)
          df.foreachPartition((it: Iterator[Row]) => it.foreach(v => accu.add(0)))
          if (pushDown) {
            assert(accu.value == 0)
          } else {
            assert(accu.value > 0)
          }

          AccumulatorContext.remove(accu.id)
        }
      }
    }
  }

  test("filter pushdown - boolean") {
    withParquetDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], Seq(Row(true), Row(false)))

      checkFilterPredicate('_1 === true, classOf[Eq[_]], true)
      checkFilterPredicate('_1 <=> true, classOf[Eq[_]], true)
      checkFilterPredicate('_1 =!= true, classOf[NotEq[_]], false)
    }
  }

  test("filter pushdown - tinyint") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toByte)))) { implicit df =>
      assert(df.schema.head.dataType === ByteType)
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1.toByte, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1.toByte, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1.toByte, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2.toByte, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3.toByte, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1.toByte, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4.toByte, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1.toByte) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1.toByte) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2.toByte) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3.toByte) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1.toByte) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4.toByte) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4.toByte), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2.toByte || '_1 > 3.toByte,
        classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - smallint") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toShort)))) { implicit df =>
      assert(df.schema.head.dataType === ShortType)
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1.toShort, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1.toShort, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1.toShort, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2.toShort, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3.toShort, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1.toShort, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4.toShort, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1.toShort) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1.toShort) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2.toShort) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3.toShort) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1.toShort) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4.toShort) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4.toShort), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2.toShort || '_1 > 3.toShort,
        classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - integer") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - long") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - float") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - double") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - string") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(i.toString))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 === "1", classOf[Eq[_]], "1")
      checkFilterPredicate('_1 <=> "1", classOf[Eq[_]], "1")
      checkFilterPredicate(
        '_1 =!= "1", classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 < "2", classOf[Lt[_]], "1")
      checkFilterPredicate('_1 > "3", classOf[Gt[_]], "4")
      checkFilterPredicate('_1 <= "1", classOf[LtEq[_]], "1")
      checkFilterPredicate('_1 >= "4", classOf[GtEq[_]], "4")

      checkFilterPredicate(Literal("1") === '_1, classOf[Eq[_]], "1")
      checkFilterPredicate(Literal("1") <=> '_1, classOf[Eq[_]], "1")
      checkFilterPredicate(Literal("2") > '_1, classOf[Lt[_]], "1")
      checkFilterPredicate(Literal("3") < '_1, classOf[Gt[_]], "4")
      checkFilterPredicate(Literal("1") >= '_1, classOf[LtEq[_]], "1")
      checkFilterPredicate(Literal("4") <= '_1, classOf[GtEq[_]], "4")

      checkFilterPredicate(!('_1 < "4"), classOf[GtEq[_]], "4")
      checkFilterPredicate('_1 < "2" || '_1 > "3", classOf[Operators.Or], Seq(Row("1"), Row("4")))
    }
  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }

    withParquetDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkBinaryFilterPredicate('_1 === 1.b, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate('_1 <=> 1.b, classOf[Eq[_]], 1.b)

      checkBinaryFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkBinaryFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate(
        '_1 =!= 1.b, classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate('_1 < 2.b, classOf[Lt[_]], 1.b)
      checkBinaryFilterPredicate('_1 > 3.b, classOf[Gt[_]], 4.b)
      checkBinaryFilterPredicate('_1 <= 1.b, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate('_1 >= 4.b, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(Literal(1.b) === '_1, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(1.b) <=> '_1, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(2.b) > '_1, classOf[Lt[_]], 1.b)
      checkBinaryFilterPredicate(Literal(3.b) < '_1, classOf[Gt[_]], 4.b)
      checkBinaryFilterPredicate(Literal(1.b) >= '_1, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(4.b) <= '_1, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(!('_1 < 4.b), classOf[GtEq[_]], 4.b)
      checkBinaryFilterPredicate(
        '_1 < 2.b || '_1 > 3.b, classOf[Operators.Or], Seq(Row(1.b), Row(4.b)))
    }
  }

  test("filter pushdown - date") {
    implicit class StringToDate(s: String) {
      def date: Date = Date.valueOf(s)
    }

    val data = Seq("2018-03-18", "2018-03-19", "2018-03-20", "2018-03-21")

    withParquetDataFrame(data.map(i => Tuple1(i.date))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], data.map(i => Row.apply(i.date)))

      checkFilterPredicate('_1 === "2018-03-18".date, classOf[Eq[_]], "2018-03-18".date)
      checkFilterPredicate('_1 <=> "2018-03-18".date, classOf[Eq[_]], "2018-03-18".date)
      checkFilterPredicate('_1 =!= "2018-03-18".date, classOf[NotEq[_]],
        Seq("2018-03-19", "2018-03-20", "2018-03-21").map(i => Row.apply(i.date)))

      checkFilterPredicate('_1 < "2018-03-19".date, classOf[Lt[_]], "2018-03-18".date)
      checkFilterPredicate('_1 > "2018-03-20".date, classOf[Gt[_]], "2018-03-21".date)
      checkFilterPredicate('_1 <= "2018-03-18".date, classOf[LtEq[_]], "2018-03-18".date)
      checkFilterPredicate('_1 >= "2018-03-21".date, classOf[GtEq[_]], "2018-03-21".date)

      checkFilterPredicate(
        Literal("2018-03-18".date) === '_1, classOf[Eq[_]], "2018-03-18".date)
      checkFilterPredicate(
        Literal("2018-03-18".date) <=> '_1, classOf[Eq[_]], "2018-03-18".date)
      checkFilterPredicate(
        Literal("2018-03-19".date) > '_1, classOf[Lt[_]], "2018-03-18".date)
      checkFilterPredicate(
        Literal("2018-03-20".date) < '_1, classOf[Gt[_]], "2018-03-21".date)
      checkFilterPredicate(
        Literal("2018-03-18".date) >= '_1, classOf[LtEq[_]], "2018-03-18".date)
      checkFilterPredicate(
        Literal("2018-03-21".date) <= '_1, classOf[GtEq[_]], "2018-03-21".date)

      checkFilterPredicate(!('_1 < "2018-03-21".date), classOf[GtEq[_]], "2018-03-21".date)
      checkFilterPredicate(
        '_1 < "2018-03-19".date || '_1 > "2018-03-20".date,
        classOf[Operators.Or],
        Seq(Row("2018-03-18".date), Row("2018-03-21".date)))
    }
  }

  test("filter pushdown - timestamp") {
    // spark.sql.parquet.outputTimestampType = TIMESTAMP_MILLIS
    val millisData = Seq(Timestamp.valueOf("2018-06-14 08:28:53.123"),
      Timestamp.valueOf("2018-06-15 08:28:53.123"),
      Timestamp.valueOf("2018-06-16 08:28:53.123"),
      Timestamp.valueOf("2018-06-17 08:28:53.123"))
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key ->
      ParquetOutputTimestampType.TIMESTAMP_MILLIS.toString) {
      testTimestampPushdown(millisData)
    }

    // spark.sql.parquet.outputTimestampType = TIMESTAMP_MICROS
    val microsData = Seq(Timestamp.valueOf("2018-06-14 08:28:53.123456"),
      Timestamp.valueOf("2018-06-15 08:28:53.123456"),
      Timestamp.valueOf("2018-06-16 08:28:53.123456"),
      Timestamp.valueOf("2018-06-17 08:28:53.123456"))
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key ->
      ParquetOutputTimestampType.TIMESTAMP_MICROS.toString) {
      testTimestampPushdown(microsData)
    }

    // spark.sql.parquet.outputTimestampType = INT96 doesn't support pushdown
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key ->
      ParquetOutputTimestampType.INT96.toString) {
      withParquetDataFrame(millisData.map(i => Tuple1(i))) { implicit df =>
        assertResult(None) {
          parquetFilters.createFilter(
            new SparkToParquetSchemaConverter(conf).convert(df.schema), sources.IsNull("_1"))
        }
      }
    }
  }

  test("filter pushdown - decimal") {
    Seq(true, false).foreach { legacyFormat =>
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> legacyFormat.toString) {
        Seq(
          s"a decimal(${Decimal.MAX_INT_DIGITS}, 2)",  // 32BitDecimalType
          s"a decimal(${Decimal.MAX_LONG_DIGITS}, 2)", // 64BitDecimalType
          "a decimal(38, 18)"                          // ByteArrayDecimalType
        ).foreach { schemaDDL =>
          val schema = StructType.fromDDL(schemaDDL)
          val rdd =
            spark.sparkContext.parallelize((1 to 4).map(i => Row(new java.math.BigDecimal(i))))
          val dataFrame = spark.createDataFrame(rdd, schema)
          testDecimalPushDown(dataFrame) { implicit df =>
            assert(df.schema === schema)
            checkFilterPredicate('a.isNull, classOf[Eq[_]], Seq.empty[Row])
            checkFilterPredicate('a.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

            checkFilterPredicate('a === 1, classOf[Eq[_]], 1)
            checkFilterPredicate('a <=> 1, classOf[Eq[_]], 1)
            checkFilterPredicate('a =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

            checkFilterPredicate('a < 2, classOf[Lt[_]], 1)
            checkFilterPredicate('a > 3, classOf[Gt[_]], 4)
            checkFilterPredicate('a <= 1, classOf[LtEq[_]], 1)
            checkFilterPredicate('a >= 4, classOf[GtEq[_]], 4)

            checkFilterPredicate(Literal(1) === 'a, classOf[Eq[_]], 1)
            checkFilterPredicate(Literal(1) <=> 'a, classOf[Eq[_]], 1)
            checkFilterPredicate(Literal(2) > 'a, classOf[Lt[_]], 1)
            checkFilterPredicate(Literal(3) < 'a, classOf[Gt[_]], 4)
            checkFilterPredicate(Literal(1) >= 'a, classOf[LtEq[_]], 1)
            checkFilterPredicate(Literal(4) <= 'a, classOf[GtEq[_]], 4)

            checkFilterPredicate(!('a < 4), classOf[GtEq[_]], 4)
            checkFilterPredicate('a < 2 || 'a > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
          }
        }
      }
    }
  }

  test("Ensure that filter value matched the parquet file schema") {
    val scale = 2
    val schema = StructType(Seq(
      StructField("cint", IntegerType),
      StructField("cdecimal1", DecimalType(Decimal.MAX_INT_DIGITS, scale)),
      StructField("cdecimal2", DecimalType(Decimal.MAX_LONG_DIGITS, scale)),
      StructField("cdecimal3", DecimalType(DecimalType.MAX_PRECISION, scale))
    ))

    val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)

    val decimal = new JBigDecimal(10).setScale(scale)
    val decimal1 = new JBigDecimal(10).setScale(scale + 1)
    assert(decimal.scale() === scale)
    assert(decimal1.scale() === scale + 1)

    assertResult(Some(lt(intColumn("cdecimal1"), 1000: Integer))) {
      parquetFilters.createFilter(parquetSchema, sources.LessThan("cdecimal1", decimal))
    }
    assertResult(None) {
      parquetFilters.createFilter(parquetSchema, sources.LessThan("cdecimal1", decimal1))
    }

    assertResult(Some(lt(longColumn("cdecimal2"), 1000L: java.lang.Long))) {
      parquetFilters.createFilter(parquetSchema, sources.LessThan("cdecimal2", decimal))
    }
    assertResult(None) {
      parquetFilters.createFilter(parquetSchema, sources.LessThan("cdecimal2", decimal1))
    }

    assert(parquetFilters.createFilter(
      parquetSchema, sources.LessThan("cdecimal3", decimal)).isDefined)
    assertResult(None) {
      parquetFilters.createFilter(parquetSchema, sources.LessThan("cdecimal3", decimal1))
    }
  }

  test("SPARK-6554: don't push down predicates which reference partition columns") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/part=1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)

        // If the "part = 1" filter gets pushed down, this query will throw an exception since
        // "part" is not a valid column in the actual Parquet file
        checkAnswer(
          spark.read.parquet(dir.getCanonicalPath).filter("part = 1"),
          (1 to 3).map(i => Row(i, i.toString, 1)))
      }
    }
  }

  test("SPARK-10829: Filter combine partition key and attribute doesn't work in DataSource scan") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/part=1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)

        // If the "part = 1" filter gets pushed down, this query will throw an exception since
        // "part" is not a valid column in the actual Parquet file
        checkAnswer(
          spark.read.parquet(dir.getCanonicalPath).filter("a > 0 and (part = 0 or a > 1)"),
          (2 to 3).map(i => Row(i, i.toString, 1)))
      }
    }
  }

  test("SPARK-12231: test the filter and empty project in partitioned DataSource scan") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}"
        (1 to 3).map(i => (i, i + 1, i + 2, i + 3)).toDF("a", "b", "c", "d").
          write.partitionBy("a").parquet(path)

        // The filter "a > 1 or b < 2" will not get pushed down, and the projection is empty,
        // this query will throw an exception since the project from combinedFilter expect
        // two projection while the
        val df1 = spark.read.parquet(dir.getCanonicalPath)

        assert(df1.filter("a > 1 or b < 2").count() == 2)
      }
    }
  }

  test("SPARK-12231: test the new projection in partitioned DataSource scan") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}"
        (1 to 3).map(i => (i, i + 1, i + 2, i + 3)).toDF("a", "b", "c", "d").
          write.partitionBy("a").parquet(path)

        // test the generate new projection case
        // when projects != partitionAndNormalColumnProjs

        val df1 = spark.read.parquet(dir.getCanonicalPath)

        checkAnswer(
          df1.filter("a > 1 or b > 2").orderBy("a").selectExpr("a", "b", "c", "d"),
          (2 to 3).map(i => Row(i, i + 1, i + 2, i + 3)))
      }
    }
  }


  test("Filter applied on merged Parquet schema with new column should work") {
    import testImplicits._
    Seq("true", "false").foreach { vectorized =>
      withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized) {
        withTempPath { dir =>
          val path1 = s"${dir.getCanonicalPath}/table1"
          (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path1)
          val path2 = s"${dir.getCanonicalPath}/table2"
          (1 to 3).map(i => (i, i.toString)).toDF("c", "b").write.parquet(path2)

          // No matter "c = 1" gets pushed down or not, this query should work without exception.
          val df = spark.read.parquet(path1, path2).filter("c = 1").selectExpr("c", "b", "a")
          checkAnswer(
            df,
            Row(1, "1", null))

          val path3 = s"${dir.getCanonicalPath}/table3"
          val dfStruct = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
          dfStruct.select(struct("a").as("s")).write.parquet(path3)

          val path4 = s"${dir.getCanonicalPath}/table4"
          val dfStruct2 = sparkContext.parallelize(Seq((1, 1))).toDF("c", "b")
          dfStruct2.select(struct("c").as("s")).write.parquet(path4)

          // No matter "s.c = 1" gets pushed down or not, this query should work without exception.
          val dfStruct3 = spark.read.parquet(path3, path4).filter("s.c = 1")
            .selectExpr("s")
          checkAnswer(dfStruct3, Row(Row(null, 1)))
        }
      }
    }
  }

  // The unsafe row RecordReader does not support row by row filtering so run it with it disabled.
  test("SPARK-11661 Still pushdown filters returned by unhandledFilters") {
    import testImplicits._
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        withTempPath { dir =>
          val path = s"${dir.getCanonicalPath}/part=1"
          (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)
          val df = spark.read.parquet(path).filter("a = 2")

          // The result should be single row.
          // When a filter is pushed to Parquet, Parquet can apply it to every row.
          // So, we can check the number of rows returned from the Parquet
          // to make sure our filter pushdown work.
          assert(stripSparkFilter(df).count == 1)
        }
      }
    }
  }

  test("SPARK-12218: 'Not' is included in Parquet filter pushdown") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/table1"
        (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b").write.parquet(path)

        checkAnswer(
          spark.read.parquet(path).where("not (a = 2) or not(b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))

        checkAnswer(
          spark.read.parquet(path).where("not (a = 2 and b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))
      }
    }
  }

  test("SPARK-12218 Converting conjunctions into Parquet filter predicates") {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType, nullable = true),
      StructField("c", DoubleType, nullable = true)
    ))

    val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)

    assertResult(Some(and(
      lt(intColumn("a"), 10: Integer),
      gt(doubleColumn("c"), 1.5: java.lang.Double)))
    ) {
      parquetFilters.createFilter(
        parquetSchema,
        sources.And(
          sources.LessThan("a", 10),
          sources.GreaterThan("c", 1.5D)))
    }

    assertResult(None) {
      parquetFilters.createFilter(
        parquetSchema,
        sources.And(
          sources.LessThan("a", 10),
          sources.StringContains("b", "prefix")))
    }

    assertResult(None) {
      parquetFilters.createFilter(
        parquetSchema,
        sources.Not(
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix"))))
    }
  }

  test("SPARK-16371 Do not push down filters when inner name and outer name are the same") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Tuple1(i)))) { implicit df =>
      // Here the schema becomes as below:
      //
      // root
      //  |-- _1: struct (nullable = true)
      //  |    |-- _1: integer (nullable = true)
      //
      // The inner column name, `_1` and outer column name `_1` are the same.
      // Obviously this should not push down filters because the outer column is struct.
      assert(df.filter("_1 IS NOT NULL").count() === 4)
    }
  }

  test("Filters should be pushed down for vectorized Parquet reader at row group level") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/table"
        (1 to 1024).map(i => (101, i)).toDF("a", "b").write.parquet(path)

        Seq(true, false).foreach { enablePushDown =>
          withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> enablePushDown.toString) {
            val accu = new NumRowGroupsAcc
            sparkContext.register(accu)

            val df = spark.read.parquet(path).filter("a < 100")
            df.foreachPartition((it: Iterator[Row]) => it.foreach(v => accu.add(0)))

            if (enablePushDown) {
              assert(accu.value == 0)
            } else {
              assert(accu.value > 0)
            }
            AccumulatorContext.remove(accu.id)
          }
        }
      }
    }
  }

  test("SPARK-17213: Broken Parquet filter push-down for string columns") {
    Seq(true, false).foreach { vectorizedEnabled =>
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorizedEnabled.toString) {
        withTempPath { dir =>
          import testImplicits._

          val path = dir.getCanonicalPath
          // scalastyle:off nonascii
          Seq("a", "é").toDF("name").write.parquet(path)
          // scalastyle:on nonascii

          assert(spark.read.parquet(path).where("name > 'a'").count() == 1)
          assert(spark.read.parquet(path).where("name >= 'a'").count() == 2)

          // scalastyle:off nonascii
          assert(spark.read.parquet(path).where("name < 'é'").count() == 1)
          assert(spark.read.parquet(path).where("name <= 'é'").count() == 2)
          // scalastyle:on nonascii
        }
      }
    }
  }

  test("SPARK-20364: Disable Parquet predicate pushdown for fields having dots in the names") {
    import testImplicits._

    Seq(true, false).foreach { vectorized =>
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString,
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString,
          SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
        withTempPath { path =>
          Seq(Some(1), None).toDF("col.dots").write.parquet(path.getAbsolutePath)
          val readBack = spark.read.parquet(path.getAbsolutePath).where("`col.dots` IS NOT NULL")
          assert(readBack.count() == 1)
        }
      }
    }
  }

  test("Filters should be pushed down for Parquet readers at row group level") {
    import testImplicits._

    withSQLConf(
      // Makes sure disabling 'spark.sql.parquet.recordFilter' still enables
      // row group level filtering.
      SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
      SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
      withTempPath { path =>
        val data = (1 to 1024)
        data.toDF("a").coalesce(1)
          .write.option("parquet.block.size", 512)
          .parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath).filter("a == 500")
        // Here, we strip the Spark side filter and check the actual results from Parquet.
        val actual = stripSparkFilter(df).collect().length
        // Since those are filtered at row group level, the result count should be less
        // than the total length but should not be a single record.
        // Note that, if record level filtering is enabled, it should be a single record.
        // If no filter is pushed down to Parquet, it should be the total length of data.
        assert(actual > 1 && actual < data.length)
      }
    }
  }

  test("SPARK-23852: Broken Parquet push-down for partially-written stats") {
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      // parquet-1217.parquet contains a single column with values -1, 0, 1, 2 and null.
      // The row-group statistics include null counts, but not min and max values, which
      // triggers PARQUET-1217.
      val df = readResourceParquetFile("test-data/parquet-1217.parquet")

      // Will return 0 rows if PARQUET-1217 is not fixed.
      assert(df.where("col > 0").count() === 2)
    }
  }

  test("filter pushdown - StringStartsWith") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(i + "str" + i))) { implicit df =>
      checkFilterPredicate(
        '_1.startsWith("").asInstanceOf[Predicate],
        classOf[UserDefinedByInstance[_, _]],
        Seq("1str1", "2str2", "3str3", "4str4").map(Row(_)))

      Seq("2", "2s", "2st", "2str", "2str2").foreach { prefix =>
        checkFilterPredicate(
          '_1.startsWith(prefix).asInstanceOf[Predicate],
          classOf[UserDefinedByInstance[_, _]],
          "2str2")
      }

      Seq("2S", "null", "2str22").foreach { prefix =>
        checkFilterPredicate(
          '_1.startsWith(prefix).asInstanceOf[Predicate],
          classOf[UserDefinedByInstance[_, _]],
          Seq.empty[Row])
      }

      checkFilterPredicate(
        !'_1.startsWith("").asInstanceOf[Predicate],
        classOf[UserDefinedByInstance[_, _]],
        Seq().map(Row(_)))

      Seq("2", "2s", "2st", "2str", "2str2").foreach { prefix =>
        checkFilterPredicate(
          !'_1.startsWith(prefix).asInstanceOf[Predicate],
          classOf[UserDefinedByInstance[_, _]],
          Seq("1str1", "3str3", "4str4").map(Row(_)))
      }

      Seq("2S", "null", "2str22").foreach { prefix =>
        checkFilterPredicate(
          !'_1.startsWith(prefix).asInstanceOf[Predicate],
          classOf[UserDefinedByInstance[_, _]],
          Seq("1str1", "2str2", "3str3", "4str4").map(Row(_)))
      }

      assertResult(None) {
        parquetFilters.createFilter(
          new SparkToParquetSchemaConverter(conf).convert(df.schema),
          sources.StringStartsWith("_1", null))
      }
    }

    // SPARK-28371: make sure filter is null-safe.
    withParquetDataFrame(Seq(Tuple1[String](null))) { implicit df =>
      checkFilterPredicate(
        '_1.startsWith("blah").asInstanceOf[Predicate],
        classOf[UserDefinedByInstance[_, _]],
        Seq.empty[Row])
    }

    import testImplicits._
    // Test canDrop() has taken effect
    testStringStartsWith(spark.range(1024).map(_.toString).toDF(), "value like 'a%'")
    // Test inverseCanDrop() has taken effect
    testStringStartsWith(spark.range(1024).map(c => "100").toDF(), "value not like '10%'")
  }

  test("SPARK-17091: Convert IN predicate to Parquet filter push-down") {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false)
    ))

    val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)

    assertResult(Some(FilterApi.eq(intColumn("a"), null: Integer))) {
      parquetFilters.createFilter(parquetSchema, sources.In("a", Array(null)))
    }

    assertResult(Some(FilterApi.eq(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(parquetSchema, sources.In("a", Array(10)))
    }

    // Remove duplicates
    assertResult(Some(FilterApi.eq(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(parquetSchema, sources.In("a", Array(10, 10)))
    }

    assertResult(Some(or(or(
      FilterApi.eq(intColumn("a"), 10: Integer),
      FilterApi.eq(intColumn("a"), 20: Integer)),
      FilterApi.eq(intColumn("a"), 30: Integer)))
    ) {
      parquetFilters.createFilter(parquetSchema, sources.In("a", Array(10, 20, 30)))
    }

    assert(parquetFilters.createFilter(parquetSchema, sources.In("a",
      Range(0, conf.parquetFilterPushDownInFilterThreshold).toArray)).isDefined)
    assert(parquetFilters.createFilter(parquetSchema, sources.In("a",
      Range(0, conf.parquetFilterPushDownInFilterThreshold + 1).toArray)).isEmpty)

    import testImplicits._
    withTempPath { path =>
      val data = 0 to 1024
      data.toDF("a").selectExpr("if (a = 1024, null, a) AS a") // convert 1024 to null
        .coalesce(1).write.option("parquet.block.size", 512)
        .parquet(path.getAbsolutePath)
      val df = spark.read.parquet(path.getAbsolutePath)
      Seq(true, false).foreach { pushEnabled =>
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> pushEnabled.toString) {
          Seq(1, 5, 10, 11).foreach { count =>
            val filter = s"a in(${Range(0, count).mkString(",")})"
            assert(df.where(filter).count() === count)
            val actual = stripSparkFilter(df.where(filter)).collect().length
            if (pushEnabled && count <= conf.parquetFilterPushDownInFilterThreshold) {
              assert(actual > 1 && actual < data.length)
            } else {
              assert(actual === data.length)
            }
          }
          assert(df.where("a in(null)").count() === 0)
          assert(df.where("a = null").count() === 0)
          assert(df.where("a is null").count() === 1)
        }
      }
    }
  }

  test("SPARK-25207: Case-insensitive field resolution for pushdown when reading parquet") {
    def createParquetFilter(caseSensitive: Boolean): ParquetFilters = {
      new ParquetFilters(conf.parquetFilterPushDownDate, conf.parquetFilterPushDownTimestamp,
        conf.parquetFilterPushDownDecimal, conf.parquetFilterPushDownStringStartWith,
        conf.parquetFilterPushDownInFilterThreshold, caseSensitive)
    }
    val caseSensitiveParquetFilters = createParquetFilter(caseSensitive = true)
    val caseInsensitiveParquetFilters = createParquetFilter(caseSensitive = false)

    def testCaseInsensitiveResolution(
        schema: StructType,
        expected: FilterPredicate,
        filter: sources.Filter): Unit = {
      val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)

      assertResult(Some(expected)) {
        caseInsensitiveParquetFilters.createFilter(parquetSchema, filter)
      }
      assertResult(None) {
        caseSensitiveParquetFilters.createFilter(parquetSchema, filter)
      }
    }

    val schema = StructType(Seq(StructField("cint", IntegerType)))

    testCaseInsensitiveResolution(
      schema, FilterApi.eq(intColumn("cint"), null.asInstanceOf[Integer]), sources.IsNull("CINT"))

    testCaseInsensitiveResolution(
      schema,
      FilterApi.notEq(intColumn("cint"), null.asInstanceOf[Integer]),
      sources.IsNotNull("CINT"))

    testCaseInsensitiveResolution(
      schema, FilterApi.eq(intColumn("cint"), 1000: Integer), sources.EqualTo("CINT", 1000))

    testCaseInsensitiveResolution(
      schema,
      FilterApi.notEq(intColumn("cint"), 1000: Integer),
      sources.Not(sources.EqualTo("CINT", 1000)))

    testCaseInsensitiveResolution(
      schema, FilterApi.eq(intColumn("cint"), 1000: Integer), sources.EqualNullSafe("CINT", 1000))

    testCaseInsensitiveResolution(
      schema,
      FilterApi.notEq(intColumn("cint"), 1000: Integer),
      sources.Not(sources.EqualNullSafe("CINT", 1000)))

    testCaseInsensitiveResolution(
      schema,
      FilterApi.lt(intColumn("cint"), 1000: Integer), sources.LessThan("CINT", 1000))

    testCaseInsensitiveResolution(
      schema,
      FilterApi.ltEq(intColumn("cint"), 1000: Integer),
      sources.LessThanOrEqual("CINT", 1000))

    testCaseInsensitiveResolution(
      schema, FilterApi.gt(intColumn("cint"), 1000: Integer), sources.GreaterThan("CINT", 1000))

    testCaseInsensitiveResolution(
      schema,
      FilterApi.gtEq(intColumn("cint"), 1000: Integer),
      sources.GreaterThanOrEqual("CINT", 1000))

    testCaseInsensitiveResolution(
      schema,
      FilterApi.or(
        FilterApi.eq(intColumn("cint"), 10: Integer),
        FilterApi.eq(intColumn("cint"), 20: Integer)),
      sources.In("CINT", Array(10, 20)))

    val dupFieldSchema = StructType(
      Seq(StructField("cint", IntegerType), StructField("cINT", IntegerType)))
    val dupParquetSchema = new SparkToParquetSchemaConverter(conf).convert(dupFieldSchema)
    assertResult(None) {
      caseInsensitiveParquetFilters.createFilter(
        dupParquetSchema, sources.EqualTo("CINT", 1000))
    }
  }

  test("SPARK-25207: exception when duplicate fields in case-insensitive mode") {
    withTempPath { dir =>
      val count = 10
      val tableName = "spark_25207"
      val tableDir = dir.getAbsoluteFile + "/table"
      withTable(tableName) {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          spark.range(count).selectExpr("id as A", "id as B", "id as b")
            .write.mode("overwrite").parquet(tableDir)
        }
        sql(
          s"""
             |CREATE TABLE $tableName (A LONG, B LONG) USING PARQUET LOCATION '$tableDir'
           """.stripMargin)

        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
          val e = intercept[SparkException] {
            sql(s"select a from $tableName where b > 0").collect()
          }
          assert(e.getCause.isInstanceOf[RuntimeException] && e.getCause.getMessage.contains(
            """Found duplicate field(s) "B": [B, b] in case-insensitive mode"""))
        }

        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          checkAnswer(sql(s"select A from $tableName where B > 0"), (1 until count).map(Row(_)))
        }
      }
    }
  }

  test("SPARK-30826: case insensitivity of StringStartsWith attribute") {
    import testImplicits._
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTable("t1") {
        withTempPath { dir =>
          val path = dir.toURI.toString
          Seq("42").toDF("COL").write.parquet(path)
          spark.sql(
            s"""
               |CREATE TABLE t1 (col STRING)
               |USING parquet
               |OPTIONS (path '$path')
           """.stripMargin)
          checkAnswer(
            spark.sql("SELECT * FROM t1 WHERE col LIKE '4%'"),
            Row("42"))
        }
      }
    }
  }
}

class NumRowGroupsAcc extends AccumulatorV2[Integer, Integer] {
  private var _sum = 0

  override def isZero: Boolean = _sum == 0

  override def copy(): AccumulatorV2[Integer, Integer] = {
    val acc = new NumRowGroupsAcc()
    acc._sum = _sum
    acc
  }

  override def reset(): Unit = _sum = 0

  override def add(v: Integer): Unit = _sum += v

  override def merge(other: AccumulatorV2[Integer, Integer]): Unit = other match {
    case a: NumRowGroupsAcc => _sum += a._sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Integer = _sum
}
