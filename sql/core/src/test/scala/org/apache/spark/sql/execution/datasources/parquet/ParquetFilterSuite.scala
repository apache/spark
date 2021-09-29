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
import java.time.{Duration, LocalDate, LocalDateTime, Period, ZoneId}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators}
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.Operators.{Column => _, _}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetOutputFormat}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation, LogicalRelation, PushableColumnAndNestedColumn}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.{CORRECTED, LEGACY}
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType.{INT96, TIMESTAMP_MICROS, TIMESTAMP_MILLIS}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, Utils}

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
abstract class ParquetFilterSuite extends QueryTest with ParquetTest with SharedSparkSession {

  protected def createParquetFilters(
      schema: MessageType,
      caseSensitive: Option[Boolean] = None,
      datetimeRebaseMode: LegacyBehaviorPolicy.Value = LegacyBehaviorPolicy.CORRECTED
    ): ParquetFilters =
    new ParquetFilters(schema, conf.parquetFilterPushDownDate, conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal, conf.parquetFilterPushDownStringStartWith,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      datetimeRebaseMode)

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

  def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (DataFrame, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit

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

  /**
   * Takes a sequence of products `data` to generate multi-level nested
   * dataframes as new test data. It tests both non-nested and nested dataframes
   * which are written and read back with Parquet datasource.
   *
   * This is different from [[ParquetTest.withParquetDataFrame]] which does not
   * test nested cases.
   */
  private def withNestedParquetDataFrame[T <: Product: ClassTag: TypeTag](data: Seq[T])
      (runTest: (DataFrame, String, Any => Any) => Unit): Unit =
    withNestedParquetDataFrame(spark.createDataFrame(data))(runTest)

  private def withNestedParquetDataFrame(inputDF: DataFrame)
      (runTest: (DataFrame, String, Any => Any) => Unit): Unit = {
    withNestedDataFrame(inputDF).foreach { case (newDF, colName, resultFun) =>
      withTempPath { file =>
        newDF.write.format(dataSourceName).save(file.getCanonicalPath)
        readParquetFile(file.getCanonicalPath) { df => runTest(df, colName, resultFun) }
      }
    }
  }

  private def testTimestampPushdown(data: Seq[String], java8Api: Boolean): Unit = {
    implicit class StringToTs(s: String) {
      def ts: Timestamp = Timestamp.valueOf(s)
    }
    assert(data.size === 4)
    val ts1 = data.head
    val ts2 = data(1)
    val ts3 = data(2)
    val ts4 = data(3)

    import testImplicits._
    val df = data.map(i => Tuple1(Timestamp.valueOf(i))).toDF()
    withNestedParquetDataFrame(df) { case (parquetDF, colName, fun) =>
      implicit val df: DataFrame = parquetDF

      def resultFun(tsStr: String): Any = {
        val parsed = if (java8Api) {
          LocalDateTime.parse(tsStr.replace(" ", "T"))
            .atZone(ZoneId.systemDefault())
            .toInstant
        } else {
          Timestamp.valueOf(tsStr)
        }
        fun(parsed)
      }

      val tsAttr = df(colName).expr
      assert(df(colName).expr.dataType === TimestampType)

      checkFilterPredicate(tsAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(tsAttr.isNotNull, classOf[NotEq[_]],
        data.map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tsAttr === ts1.ts, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr <=> ts1.ts, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr =!= ts1.ts, classOf[NotEq[_]],
        Seq(ts2, ts3, ts4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tsAttr < ts2.ts, classOf[Lt[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr > ts1.ts, classOf[Gt[_]],
        Seq(ts2, ts3, ts4).map(i => Row.apply(resultFun(i))))
      checkFilterPredicate(tsAttr <= ts1.ts, classOf[LtEq[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr >= ts4.ts, classOf[GtEq[_]], resultFun(ts4))

      checkFilterPredicate(Literal(ts1.ts) === tsAttr, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts1.ts) <=> tsAttr, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts2.ts) > tsAttr, classOf[Lt[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts3.ts) < tsAttr, classOf[Gt[_]], resultFun(ts4))
      checkFilterPredicate(Literal(ts1.ts) >= tsAttr, classOf[LtEq[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts4.ts) <= tsAttr, classOf[GtEq[_]], resultFun(ts4))

      checkFilterPredicate(!(tsAttr < ts4.ts), classOf[GtEq[_]], resultFun(ts4))
      checkFilterPredicate(tsAttr < ts2.ts || tsAttr > ts3.ts, classOf[Operators.Or],
        Seq(Row(resultFun(ts1)), Row(resultFun(ts4))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(tsAttr, Array(ts2.ts, ts3.ts, ts4.ts, "2021-05-01 00:01:02".ts).map(Literal.apply)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(ts2)), Row(resultFun(ts3)), Row(resultFun(ts4))))
        }
      }
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
    val data = (true :: false :: Nil).map(b => Tuple1.apply(Option(b)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val booleanAttr = df(colName).expr
      assert(df(colName).expr.dataType === BooleanType)

      checkFilterPredicate(booleanAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(booleanAttr.isNotNull, classOf[NotEq[_]],
        Seq(Row(resultFun(true)), Row(resultFun(false))))

      checkFilterPredicate(booleanAttr === true, classOf[Eq[_]], resultFun(true))
      checkFilterPredicate(booleanAttr <=> true, classOf[Eq[_]], resultFun(true))
      checkFilterPredicate(booleanAttr =!= true, classOf[NotEq[_]], resultFun(false))
    }
  }

  test("filter pushdown - tinyint") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toByte)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val tinyIntAttr = df(colName).expr
      assert(df(colName).expr.dataType === ByteType)

      checkFilterPredicate(tinyIntAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(tinyIntAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tinyIntAttr === 1.toByte, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr <=> 1.toByte, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr =!= 1.toByte, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tinyIntAttr < 2.toByte, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr > 3.toByte, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(tinyIntAttr <= 1.toByte, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr >= 4.toByte, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1.toByte) === tinyIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1.toByte) <=> tinyIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2.toByte) > tinyIntAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3.toByte) < tinyIntAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1.toByte) >= tinyIntAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4.toByte) <= tinyIntAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(tinyIntAttr < 4.toByte), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(tinyIntAttr < 2.toByte || tinyIntAttr > 3.toByte,
        classOf[Operators.Or], Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - smallint") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toShort)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val smallIntAttr = df(colName).expr
      assert(df(colName).expr.dataType === ShortType)

      checkFilterPredicate(smallIntAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(smallIntAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(smallIntAttr === 1.toShort, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr <=> 1.toShort, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr =!= 1.toShort, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(smallIntAttr < 2.toShort, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr > 3.toShort, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(smallIntAttr <= 1.toShort, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr >= 4.toShort, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1.toShort) === smallIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1.toShort) <=> smallIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2.toShort) > smallIntAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3.toShort) < smallIntAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1.toShort) >= smallIntAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4.toShort) <= smallIntAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(smallIntAttr < 4.toShort), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(smallIntAttr < 2.toShort || smallIntAttr > 3.toShort,
        classOf[Operators.Or], Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - integer") {
    val data = (1 to 4).map(i => Tuple1(Option(i)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val intAttr = df(colName).expr
      assert(df(colName).expr.dataType === IntegerType)

      checkFilterPredicate(intAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(intAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(intAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(intAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(intAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(intAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(intAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(intAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(intAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === intAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> intAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > intAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < intAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= intAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= intAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(intAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(intAttr < 2 || intAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(intAttr, Array(2, 3, 4, 5, 6, 7).map(Literal.apply)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(2)), Row(resultFun(3)), Row(resultFun(4))))
        }
      }
    }
  }

  test("filter pushdown - long") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toLong)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val longAttr = df(colName).expr
      assert(df(colName).expr.dataType === LongType)

      checkFilterPredicate(longAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(longAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(longAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(longAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(longAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(longAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(longAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(longAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(longAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === longAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> longAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > longAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < longAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= longAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= longAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(longAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(longAttr < 2 || longAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(longAttr, Array(2L, 3L, 4L, 5L, 6L, 7L).map(Literal.apply)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(2L)), Row(resultFun(3L)), Row(resultFun(4L))))
        }
      }
    }
  }

  test("filter pushdown - float") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toFloat)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val floatAttr = df(colName).expr
      assert(df(colName).expr.dataType === FloatType)

      checkFilterPredicate(floatAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(floatAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(floatAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(floatAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(floatAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(floatAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(floatAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(floatAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(floatAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === floatAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> floatAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > floatAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < floatAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= floatAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= floatAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(floatAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(floatAttr < 2 || floatAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(floatAttr, Array(2F, 3F, 4F, 5F, 6F, 7F).map(Literal.apply)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(2F)), Row(resultFun(3F)), Row(resultFun(4F))))
        }
      }
    }
  }

  test("filter pushdown - double") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toDouble)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val doubleAttr = df(colName).expr
      assert(df(colName).expr.dataType === DoubleType)

      checkFilterPredicate(doubleAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(doubleAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(doubleAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(doubleAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(doubleAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(doubleAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(doubleAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(doubleAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(doubleAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === doubleAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> doubleAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > doubleAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < doubleAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= doubleAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= doubleAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(doubleAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(doubleAttr < 2 || doubleAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(doubleAttr, Array(2.0D, 3.0D, 4.0D, 5.0D, 6.0D, 7.0D).map(Literal.apply)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(2D)), Row(resultFun(3D)), Row(resultFun(4F))))
        }
      }
    }
  }

  test("filter pushdown - string") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toString)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val stringAttr = df(colName).expr
      assert(df(colName).expr.dataType === StringType)

      checkFilterPredicate(stringAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(stringAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i.toString))))

      checkFilterPredicate(stringAttr === "1", classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(stringAttr <=> "1", classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(stringAttr =!= "1", classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i.toString))))

      checkFilterPredicate(stringAttr < "2", classOf[Lt[_]], resultFun("1"))
      checkFilterPredicate(stringAttr > "3", classOf[Gt[_]], resultFun("4"))
      checkFilterPredicate(stringAttr <= "1", classOf[LtEq[_]], resultFun("1"))
      checkFilterPredicate(stringAttr >= "4", classOf[GtEq[_]], resultFun("4"))

      checkFilterPredicate(Literal("1") === stringAttr, classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(Literal("1") <=> stringAttr, classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(Literal("2") > stringAttr, classOf[Lt[_]], resultFun("1"))
      checkFilterPredicate(Literal("3") < stringAttr, classOf[Gt[_]], resultFun("4"))
      checkFilterPredicate(Literal("1") >= stringAttr, classOf[LtEq[_]], resultFun("1"))
      checkFilterPredicate(Literal("4") <= stringAttr, classOf[GtEq[_]], resultFun("4"))

      checkFilterPredicate(!(stringAttr < "4"), classOf[GtEq[_]], resultFun("4"))
      checkFilterPredicate(stringAttr < "2" || stringAttr > "3", classOf[Operators.Or],
        Seq(Row(resultFun("1")), Row(resultFun("4"))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(stringAttr, Array("2", "3", "4", "5", "6", "7").map(Literal.apply)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun("2")), Row(resultFun("3")), Row(resultFun("4"))))
        }
      }
    }
  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }

    val data = (1 to 4).map(i => Tuple1(Option(i.b)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val binaryAttr: Expression = df(colName).expr
      assert(df(colName).expr.dataType === BinaryType)

      checkFilterPredicate(binaryAttr === 1.b, classOf[Eq[_]], resultFun(1.b))
      checkFilterPredicate(binaryAttr <=> 1.b, classOf[Eq[_]], resultFun(1.b))

      checkFilterPredicate(binaryAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(binaryAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i.b))))

      checkFilterPredicate(binaryAttr =!= 1.b, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i.b))))

      checkFilterPredicate(binaryAttr < 2.b, classOf[Lt[_]], resultFun(1.b))
      checkFilterPredicate(binaryAttr > 3.b, classOf[Gt[_]], resultFun(4.b))
      checkFilterPredicate(binaryAttr <= 1.b, classOf[LtEq[_]], resultFun(1.b))
      checkFilterPredicate(binaryAttr >= 4.b, classOf[GtEq[_]], resultFun(4.b))

      checkFilterPredicate(Literal(1.b) === binaryAttr, classOf[Eq[_]], resultFun(1.b))
      checkFilterPredicate(Literal(1.b) <=> binaryAttr, classOf[Eq[_]], resultFun(1.b))
      checkFilterPredicate(Literal(2.b) > binaryAttr, classOf[Lt[_]], resultFun(1.b))
      checkFilterPredicate(Literal(3.b) < binaryAttr, classOf[Gt[_]], resultFun(4.b))
      checkFilterPredicate(Literal(1.b) >= binaryAttr, classOf[LtEq[_]], resultFun(1.b))
      checkFilterPredicate(Literal(4.b) <= binaryAttr, classOf[GtEq[_]], resultFun(4.b))

      checkFilterPredicate(!(binaryAttr < 4.b), classOf[GtEq[_]], resultFun(4.b))
      checkFilterPredicate(binaryAttr < 2.b || binaryAttr > 3.b, classOf[Operators.Or],
        Seq(Row(resultFun(1.b)), Row(resultFun(4.b))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(binaryAttr, Array(2.b, 3.b, 4.b, 5.b, 6.b, 7.b).map(Literal.apply)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(2.b)), Row(resultFun(3.b)), Row(resultFun(4.b))))
        }
      }
    }
  }

  test("filter pushdown - date") {
    implicit class StringToDate(s: String) {
      def date: Date = Date.valueOf(s)
    }

    val data = Seq("1000-01-01", "2018-03-19", "2018-03-20", "2018-03-21")
    import testImplicits._

    Seq(false, true).foreach { java8Api =>
      Seq(CORRECTED, LEGACY).foreach { rebaseMode =>
        withSQLConf(
          SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
          SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> rebaseMode.toString) {
          val dates = data.map(i => Tuple1(Date.valueOf(i))).toDF()
          withNestedParquetDataFrame(dates) { case (inputDF, colName, fun) =>
            implicit val df: DataFrame = inputDF

            def resultFun(dateStr: String): Any = {
              val parsed = if (java8Api) LocalDate.parse(dateStr) else Date.valueOf(dateStr)
              fun(parsed)
            }

            val dateAttr: Expression = df(colName).expr
            assert(df(colName).expr.dataType === DateType)

            checkFilterPredicate(dateAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
            checkFilterPredicate(dateAttr.isNotNull, classOf[NotEq[_]],
              data.map(i => Row.apply(resultFun(i))))

            checkFilterPredicate(dateAttr === "1000-01-01".date, classOf[Eq[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(dateAttr <=> "1000-01-01".date, classOf[Eq[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(dateAttr =!= "1000-01-01".date, classOf[NotEq[_]],
              Seq("2018-03-19", "2018-03-20", "2018-03-21").map(i => Row.apply(resultFun(i))))

            checkFilterPredicate(dateAttr < "2018-03-19".date, classOf[Lt[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(dateAttr > "2018-03-20".date, classOf[Gt[_]],
              resultFun("2018-03-21"))
            checkFilterPredicate(dateAttr <= "1000-01-01".date, classOf[LtEq[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(dateAttr >= "2018-03-21".date, classOf[GtEq[_]],
              resultFun("2018-03-21"))

            checkFilterPredicate(Literal("1000-01-01".date) === dateAttr, classOf[Eq[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(Literal("1000-01-01".date) <=> dateAttr, classOf[Eq[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(Literal("2018-03-19".date) > dateAttr, classOf[Lt[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(Literal("2018-03-20".date) < dateAttr, classOf[Gt[_]],
              resultFun("2018-03-21"))
            checkFilterPredicate(Literal("1000-01-01".date) >= dateAttr, classOf[LtEq[_]],
              resultFun("1000-01-01"))
            checkFilterPredicate(Literal("2018-03-21".date) <= dateAttr, classOf[GtEq[_]],
              resultFun("2018-03-21"))

            checkFilterPredicate(!(dateAttr < "2018-03-21".date), classOf[GtEq[_]],
              resultFun("2018-03-21"))
            checkFilterPredicate(
              dateAttr < "2018-03-19".date || dateAttr > "2018-03-20".date,
              classOf[Operators.Or],
              Seq(Row(resultFun("1000-01-01")), Row(resultFun("2018-03-21"))))

            Seq(3, 20).foreach { threshold =>
              withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
                checkFilterPredicate(
                  In(dateAttr, Array("2018-03-19".date, "2018-03-20".date, "2018-03-21".date,
                    "2018-03-22".date).map(Literal.apply)),
                  if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
                  Seq(Row(resultFun("2018-03-19")), Row(resultFun("2018-03-20")),
                    Row(resultFun("2018-03-21"))))
              }
            }
          }
        }
      }
    }
  }

  test("filter pushdown - timestamp") {
    Seq(true, false).foreach { java8Api =>
      Seq(CORRECTED, LEGACY).foreach { rebaseMode =>
        val millisData = Seq(
          "1000-06-14 08:28:53.123",
          "1582-06-15 08:28:53.001",
          "1900-06-16 08:28:53.0",
          "2018-06-17 08:28:53.999")
        withSQLConf(
          SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
          SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> rebaseMode.toString,
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> TIMESTAMP_MILLIS.toString) {
          testTimestampPushdown(millisData, java8Api)
        }

        val microsData = Seq(
          "1000-06-14 08:28:53.123456",
          "1582-06-15 08:28:53.123456",
          "1900-06-16 08:28:53.123456",
          "2018-06-17 08:28:53.123456")
        withSQLConf(
          SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
          SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> rebaseMode.toString,
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> TIMESTAMP_MICROS.toString) {
          testTimestampPushdown(microsData, java8Api)
        }

        // INT96 doesn't support pushdown
        withSQLConf(
          SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
          SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> rebaseMode.toString,
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> INT96.toString) {
          import testImplicits._
          withTempPath { file =>
            millisData.map(i => Tuple1(Timestamp.valueOf(i))).toDF
              .write.format(dataSourceName).save(file.getCanonicalPath)
            readParquetFile(file.getCanonicalPath) { df =>
              val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
              assertResult(None) {
                createParquetFilters(schema).createFilter(sources.IsNull("_1"))
              }
            }
          }
        }
      }
    }
  }

  test("filter pushdown - decimal") {
    Seq(
      (false, Decimal.MAX_INT_DIGITS), // int32Writer
      (false, Decimal.MAX_LONG_DIGITS), // int64Writer
      (true, Decimal.MAX_LONG_DIGITS), // binaryWriterUsingUnscaledLong
      (false, DecimalType.MAX_PRECISION) // binaryWriterUsingUnscaledBytes
    ).foreach { case (legacyFormat, precision) =>
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> legacyFormat.toString) {
        val rdd =
          spark.sparkContext.parallelize((1 to 4).map(i => Row(new java.math.BigDecimal(i))))
        val dataFrame = spark.createDataFrame(rdd, StructType.fromDDL(s"a decimal($precision, 2)"))
        withNestedParquetDataFrame(dataFrame) { case (inputDF, colName, resultFun) =>
          implicit val df: DataFrame = inputDF

          val decimalAttr: Expression = df(colName).expr
          assert(df(colName).expr.dataType === DecimalType(precision, 2))

          checkFilterPredicate(decimalAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
          checkFilterPredicate(decimalAttr.isNotNull, classOf[NotEq[_]],
            (1 to 4).map(i => Row.apply(resultFun(i))))

          checkFilterPredicate(decimalAttr === 1, classOf[Eq[_]], resultFun(1))
          checkFilterPredicate(decimalAttr <=> 1, classOf[Eq[_]], resultFun(1))
          checkFilterPredicate(decimalAttr =!= 1, classOf[NotEq[_]],
            (2 to 4).map(i => Row.apply(resultFun(i))))

          checkFilterPredicate(decimalAttr < 2, classOf[Lt[_]], resultFun(1))
          checkFilterPredicate(decimalAttr > 3, classOf[Gt[_]], resultFun(4))
          checkFilterPredicate(decimalAttr <= 1, classOf[LtEq[_]], resultFun(1))
          checkFilterPredicate(decimalAttr >= 4, classOf[GtEq[_]], resultFun(4))

          checkFilterPredicate(Literal(1) === decimalAttr, classOf[Eq[_]], resultFun(1))
          checkFilterPredicate(Literal(1) <=> decimalAttr, classOf[Eq[_]], resultFun(1))
          checkFilterPredicate(Literal(2) > decimalAttr, classOf[Lt[_]], resultFun(1))
          checkFilterPredicate(Literal(3) < decimalAttr, classOf[Gt[_]], resultFun(4))
          checkFilterPredicate(Literal(1) >= decimalAttr, classOf[LtEq[_]], resultFun(1))
          checkFilterPredicate(Literal(4) <= decimalAttr, classOf[GtEq[_]], resultFun(4))

          checkFilterPredicate(!(decimalAttr < 4), classOf[GtEq[_]], resultFun(4))
          checkFilterPredicate(decimalAttr < 2 || decimalAttr > 3, classOf[Operators.Or],
            Seq(Row(resultFun(1)), Row(resultFun(4))))

          Array(1, 2, 3, 4).map(JBigDecimal.valueOf(_).setScale(2))
            .map(Literal.create(_, DecimalType(precision, 2)))

          Seq(3, 20).foreach { threshold =>
            withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
              checkFilterPredicate(
                In(decimalAttr, Array(2, 3, 4, 5).map(Literal.apply)
                  .map(_.cast(DecimalType(precision, 2)))),
                if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
                Seq(Row(resultFun(2)), Row(resultFun(3)), Row(resultFun(4))))
            }
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

    val parquetFilters = createParquetFilters(parquetSchema)
    assertResult(Some(lt(intColumn("cdecimal1"), 1000: Integer))) {
      parquetFilters.createFilter(sources.LessThan("cdecimal1", decimal))
    }
    assertResult(None) {
      parquetFilters.createFilter(sources.LessThan("cdecimal1", decimal1))
    }

    assertResult(Some(lt(longColumn("cdecimal2"), 1000L: java.lang.Long))) {
      parquetFilters.createFilter(sources.LessThan("cdecimal2", decimal))
    }
    assertResult(None) {
      parquetFilters.createFilter(sources.LessThan("cdecimal2", decimal1))
    }

    assert(parquetFilters.createFilter(sources.LessThan("cdecimal3", decimal)).isDefined)
    assertResult(None) {
      parquetFilters.createFilter(sources.LessThan("cdecimal3", decimal1))
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
    withAllParquetReaders {
      withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true") {
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

  test("SPARK-12218 and SPARK-25559 Converting conjunctions into Parquet filter predicates") {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType, nullable = true),
      StructField("c", DoubleType, nullable = true)
    ))

    val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)
    val parquetFilters = createParquetFilters(parquetSchema)
    assertResult(Some(and(
      lt(intColumn("a"), 10: Integer),
      gt(doubleColumn("c"), 1.5: java.lang.Double)))
    ) {
      parquetFilters.createFilter(
        sources.And(
          sources.LessThan("a", 10),
          sources.GreaterThan("c", 1.5D)))
    }

    // Testing when `canRemoveOneSideInAnd == true`
    // case sources.And(lhs, rhs) =>
    //   ...
    //     case (Some(lhsFilter), None) if canRemoveOneSideInAnd => Some(lhsFilter)
    assertResult(Some(lt(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(
        sources.And(
          sources.LessThan("a", 10),
          sources.StringContains("b", "prefix")))
    }

    // Testing when `canRemoveOneSideInAnd == true`
    // case sources.And(lhs, rhs) =>
    //   ...
    //     case (None, Some(rhsFilter)) if canRemoveOneSideInAnd => Some(rhsFilter)
    assertResult(Some(lt(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(
        sources.And(
          sources.StringContains("b", "prefix"),
          sources.LessThan("a", 10)))
    }

    // Testing complex And conditions
    assertResult(Some(
      FilterApi.and(lt(intColumn("a"), 10: Integer), gt(intColumn("a"), 5: Integer)))) {
      parquetFilters.createFilter(
        sources.And(
          sources.And(
            sources.LessThan("a", 10),
            sources.StringContains("b", "prefix")
          ),
          sources.GreaterThan("a", 5)))
    }

    // Testing complex And conditions
    assertResult(Some(
      FilterApi.and(gt(intColumn("a"), 5: Integer), lt(intColumn("a"), 10: Integer)))) {
      parquetFilters.createFilter(
        sources.And(
          sources.GreaterThan("a", 5),
          sources.And(
            sources.StringContains("b", "prefix"),
            sources.LessThan("a", 10)
          )))
    }

    // Testing
    // case sources.Not(pred) =>
    //   createFilterHelper(nameToParquetField, pred, canRemoveOneSideInAnd = false)
    //     .map(FilterApi.not)
    //
    // and
    //
    // Testing when `canRemoveOneSideInAnd == false`
    // case sources.And(lhs, rhs) =>
    //   ...
    //     case (Some(lhsFilter), None) if canRemoveOneSideInAnd => Some(lhsFilter)
    assertResult(None) {
      parquetFilters.createFilter(
        sources.Not(
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix"))))
    }

    // Testing
    // case sources.Not(pred) =>
    //   createFilterHelper(nameToParquetField, pred, canRemoveOneSideInAnd = false)
    //     .map(FilterApi.not)
    //
    // and
    //
    // Testing when `canRemoveOneSideInAnd == false`
    // case sources.And(lhs, rhs) =>
    //   ...
    //     case (None, Some(rhsFilter)) if canRemoveOneSideInAnd => Some(rhsFilter)
    assertResult(None) {
      parquetFilters.createFilter(
        sources.Not(
          sources.And(
            sources.StringContains("b", "prefix"),
            sources.GreaterThan("a", 1))))
    }

    // Testing
    // case sources.Not(pred) =>
    //   createFilterHelper(nameToParquetField, pred, canRemoveOneSideInAnd = false)
    //     .map(FilterApi.not)
    //
    // and
    //
    // Testing passing `canRemoveOneSideInAnd = false` into
    // case sources.And(lhs, rhs) =>
    //   val lhsFilterOption = createFilterHelper(nameToParquetField, lhs, canRemoveOneSideInAnd)
    assertResult(None) {
      parquetFilters.createFilter(
        sources.Not(
          sources.And(
            sources.And(
              sources.GreaterThan("a", 1),
              sources.StringContains("b", "prefix")),
            sources.GreaterThan("a", 2))))
    }

    // Testing
    // case sources.Not(pred) =>
    //   createFilterHelper(nameToParquetField, pred, canRemoveOneSideInAnd = false)
    //     .map(FilterApi.not)
    //
    // and
    //
    // Testing passing `canRemoveOneSideInAnd = false` into
    // case sources.And(lhs, rhs) =>
    //   val rhsFilterOption = createFilterHelper(nameToParquetField, rhs, canRemoveOneSideInAnd)
    assertResult(None) {
      parquetFilters.createFilter(
        sources.Not(
          sources.And(
            sources.GreaterThan("a", 2),
            sources.And(
              sources.GreaterThan("a", 1),
              sources.StringContains("b", "prefix")))))
    }
  }

  test("SPARK-27699 Converting disjunctions into Parquet filter predicates") {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType, nullable = true),
      StructField("c", DoubleType, nullable = true)
    ))

    val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)
    val parquetFilters = createParquetFilters(parquetSchema)
    // Testing
    // case sources.Or(lhs, rhs) =>
    //   ...
    //     lhsFilter <- createFilterHelper(nameToParquetField, lhs, canRemoveOneSideInAnd = true)
    assertResult(Some(
      FilterApi.or(gt(intColumn("a"), 1: Integer), gt(intColumn("a"), 2: Integer)))) {
      parquetFilters.createFilter(
        sources.Or(
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix")),
          sources.GreaterThan("a", 2)))
    }

    // Testing
    // case sources.Or(lhs, rhs) =>
    //   ...
    //     rhsFilter <- createFilterHelper(nameToParquetField, rhs, canRemoveOneSideInAnd = true)
    assertResult(Some(
      FilterApi.or(gt(intColumn("a"), 2: Integer), gt(intColumn("a"), 1: Integer)))) {
      parquetFilters.createFilter(
        sources.Or(
          sources.GreaterThan("a", 2),
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix"))))
    }

    // Testing
    // case sources.Or(lhs, rhs) =>
    //   ...
    //     lhsFilter <- createFilterHelper(nameToParquetField, lhs, canRemoveOneSideInAnd = true)
    //     rhsFilter <- createFilterHelper(nameToParquetField, rhs, canRemoveOneSideInAnd = true)
    assertResult(Some(
      FilterApi.or(gt(intColumn("a"), 1: Integer), lt(intColumn("a"), 0: Integer)))) {
      parquetFilters.createFilter(
        sources.Or(
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix")),
          sources.And(
            sources.LessThan("a", 0),
            sources.StringContains("b", "foobar"))))
    }
  }

  test("SPARK-27698 Convertible Parquet filter predicates") {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType, nullable = true),
      StructField("c", DoubleType, nullable = true)
    ))

    val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)
    val parquetFilters = createParquetFilters(parquetSchema)
    assertResult(Seq(sources.And(sources.LessThan("a", 10), sources.GreaterThan("c", 1.5D)))) {
      parquetFilters.convertibleFilters(
        Seq(sources.And(
          sources.LessThan("a", 10),
          sources.GreaterThan("c", 1.5D))))
    }

    assertResult(Seq(sources.LessThan("a", 10))) {
      parquetFilters.convertibleFilters(
        Seq(sources.And(
          sources.LessThan("a", 10),
          sources.StringContains("b", "prefix"))))
    }

    assertResult(Seq(sources.LessThan("a", 10))) {
      parquetFilters.convertibleFilters(
        Seq(sources.And(
          sources.StringContains("b", "prefix"),
          sources.LessThan("a", 10))))
    }

    // Testing complex And conditions
    assertResult(Seq(sources.And(sources.LessThan("a", 10), sources.GreaterThan("a", 5)))) {
      parquetFilters.convertibleFilters(
        Seq(sources.And(
          sources.And(
            sources.LessThan("a", 10),
            sources.StringContains("b", "prefix")
          ),
          sources.GreaterThan("a", 5))))
    }

    // Testing complex And conditions
    assertResult(Seq(sources.And(sources.GreaterThan("a", 5), sources.LessThan("a", 10)))) {
      parquetFilters.convertibleFilters(
        Seq(sources.And(
          sources.GreaterThan("a", 5),
          sources.And(
            sources.StringContains("b", "prefix"),
            sources.LessThan("a", 10)
          ))))
    }

    // Testing complex And conditions
    assertResult(Seq(sources.Or(sources.GreaterThan("a", 1), sources.GreaterThan("a", 2)))) {
      parquetFilters.convertibleFilters(
        Seq(sources.Or(
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix")),
          sources.GreaterThan("a", 2))))
    }

    // Testing complex And/Or conditions, the And condition under Or condition can't be pushed down.
    assertResult(Seq(sources.And(sources.LessThan("a", 10),
      sources.Or(sources.GreaterThan("a", 1), sources.GreaterThan("a", 2))))) {
      parquetFilters.convertibleFilters(
        Seq(sources.And(
          sources.LessThan("a", 10),
          sources.Or(
            sources.And(
              sources.GreaterThan("a", 1),
              sources.StringContains("b", "prefix")),
            sources.GreaterThan("a", 2)))))
    }

    assertResult(Seq(sources.Or(sources.GreaterThan("a", 2), sources.GreaterThan("c", 1.1)))) {
      parquetFilters.convertibleFilters(
        Seq(sources.Or(
          sources.GreaterThan("a", 2),
          sources.And(
            sources.GreaterThan("c", 1.1),
            sources.StringContains("b", "prefix")))))
    }

    // Testing complex Not conditions.
    assertResult(Seq.empty) {
      parquetFilters.convertibleFilters(
        Seq(sources.Not(
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix")))))
    }

    assertResult(Seq.empty) {
      parquetFilters.convertibleFilters(
        Seq(sources.Not(
          sources.And(
            sources.StringContains("b", "prefix"),
            sources.GreaterThan("a", 1)))))
    }

    assertResult(Seq.empty) {
      parquetFilters.convertibleFilters(
        Seq(sources.Not(
          sources.And(
            sources.And(
              sources.GreaterThan("a", 1),
              sources.StringContains("b", "prefix")),
            sources.GreaterThan("a", 2)))))
    }

    assertResult(Seq.empty) {
      parquetFilters.convertibleFilters(
        Seq(sources.Not(
          sources.And(
            sources.GreaterThan("a", 2),
            sources.And(
              sources.GreaterThan("a", 1),
              sources.StringContains("b", "prefix"))))))
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
    withAllParquetReaders {
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

  test("SPARK-31026: Parquet predicate pushdown for fields having dots in the names") {
    import testImplicits._

    withAllParquetReaders {
      withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString,
          SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
        withTempPath { path =>
          Seq(Some(1), None).toDF("col.dots").write.parquet(path.getAbsolutePath)
          val readBack = spark.read.parquet(path.getAbsolutePath).where("`col.dots` IS NOT NULL")
          assert(readBack.count() == 1)
        }
      }

      withSQLConf(
          // Makes sure disabling 'spark.sql.parquet.recordFilter' still enables
          // row group level filtering.
          SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {

        withTempPath { path =>
          val data = (1 to 1024)
          data.toDF("col.dots").coalesce(1)
            .write.option("parquet.block.size", 512)
            .parquet(path.getAbsolutePath)
          val df = spark.read.parquet(path.getAbsolutePath).filter("`col.dots` == 500")
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
        classOf[Operators.Not],
        Seq().map(Row(_)))

      Seq("2", "2s", "2st", "2str", "2str2").foreach { prefix =>
        checkFilterPredicate(
          !'_1.startsWith(prefix).asInstanceOf[Predicate],
          classOf[Operators.Not],
          Seq("1str1", "3str3", "4str4").map(Row(_)))
      }

      Seq("2S", "null", "2str22").foreach { prefix =>
        checkFilterPredicate(
          !'_1.startsWith(prefix).asInstanceOf[Predicate],
          classOf[Operators.Not],
          Seq("1str1", "2str2", "3str3", "4str4").map(Row(_)))
      }

      val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
      assertResult(None) {
        createParquetFilters(schema).createFilter(sources.StringStartsWith("_1", null))
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
    val parquetFilters = createParquetFilters(parquetSchema)
    assertResult(Some(FilterApi.eq(intColumn("a"), null: Integer))) {
      parquetFilters.createFilter(sources.In("a", Array(null)))
    }

    assertResult(Some(FilterApi.eq(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(sources.In("a", Array(10)))
    }

    // Remove duplicates
    assertResult(Some(FilterApi.eq(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(sources.In("a", Array(10, 10)))
    }

    assertResult(Some(or(or(
      FilterApi.eq(intColumn("a"), 10: Integer),
      FilterApi.eq(intColumn("a"), 20: Integer)),
      FilterApi.eq(intColumn("a"), 30: Integer)))
    ) {
      parquetFilters.createFilter(sources.In("a", Array(10, 20, 30)))
    }

    Seq(0, 10).foreach { threshold =>
      withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> threshold.toString) {
        assert(createParquetFilters(parquetSchema)
          .createFilter(sources.In("a", Array(10, 20, 30))).nonEmpty === threshold > 0)
      }
    }

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
          Seq(1, 5, 10, 11, 100).foreach { count =>
            val filter = s"a in(${Range(0, count).mkString(",")})"
            assert(df.where(filter).count() === count)
            val actual = stripSparkFilter(df.where(filter)).collect().length
            if (pushEnabled) {
              // We support push down In predicate if its value exceeds threshold since SPARK-32792.
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

  test("SPARK-32792: Pushdown IN predicate to min-max filter") {
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> "2") {
      val parquetFilters = createParquetFilters(
        new SparkToParquetSchemaConverter(conf).convert(StructType.fromDDL("a int")))

      assertResult(Some(and(
        FilterApi.gtEq(intColumn("a"), 1: Integer),
        FilterApi.ltEq(intColumn("a"), 20: Integer)))
      ) {
        parquetFilters.createFilter(sources.In("a", (1 to 20).toArray))
      }

      assertResult(Some(and(
        FilterApi.gtEq(intColumn("a"), -200: Integer),
        FilterApi.ltEq(intColumn("a"), 40: Integer)))
      ) {
        parquetFilters.createFilter(sources.In("A", Array(-100, 10, -200, 40)))
      }

      assertResult(Some(or(
        FilterApi.eq(intColumn("a"), null: Integer),
        and(
          FilterApi.gtEq(intColumn("a"), 2: Integer),
          FilterApi.ltEq(intColumn("a"), 7: Integer))))
      ) {
        parquetFilters.createFilter(sources.In("a", Array(2, 3, 7, null, 6)))
      }

      assertResult(
        Some(FilterApi.not(or(
          FilterApi.eq(intColumn("a"), 2: Integer),
          FilterApi.eq(intColumn("a"), 3: Integer))))
      ) {
        parquetFilters.createFilter(sources.Not(sources.In("a", Array(2, 3))))
      }

      assertResult(
        None
      ) {
        parquetFilters.createFilter(sources.Not(sources.In("a", Array(2, 3, 7))))
      }
    }
  }

  test("SPARK-25207: Case-insensitive field resolution for pushdown when reading parquet") {
    def testCaseInsensitiveResolution(
        schema: StructType,
        expected: FilterPredicate,
        filter: sources.Filter): Unit = {
      val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)
      val caseSensitiveParquetFilters =
        createParquetFilters(parquetSchema, caseSensitive = Some(true))
      val caseInsensitiveParquetFilters =
        createParquetFilters(parquetSchema, caseSensitive = Some(false))
      assertResult(Some(expected)) {
        caseInsensitiveParquetFilters.createFilter(filter)
      }
      assertResult(None) {
        caseSensitiveParquetFilters.createFilter(filter)
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
    val dupCaseInsensitiveParquetFilters =
      createParquetFilters(dupParquetSchema, caseSensitive = Some(false))
    assertResult(None) {
      dupCaseInsensitiveParquetFilters.createFilter(sources.EqualTo("CINT", 1000))
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

  test("Support Parquet column index") {
    // block 1:
    //                      null count  min                                       max
    // page-0                         0  0                                         99
    // page-1                         0  100                                       199
    // page-2                         0  200                                       299
    // page-3                         0  300                                       399
    // page-4                         0  400                                       449
    //
    // block 2:
    //                      null count  min                                       max
    // page-0                         0  450                                       549
    // page-1                         0  550                                       649
    // page-2                         0  650                                       749
    // page-3                         0  750                                       849
    // page-4                         0  850                                       899
    withTempPath { path =>
      spark.range(900)
        .repartition(1)
        .write
        .option(ParquetOutputFormat.PAGE_SIZE, "500")
        .option(ParquetOutputFormat.BLOCK_SIZE, "2000")
        .parquet(path.getCanonicalPath)

      val parquetFile = path.listFiles().filter(_.getName.startsWith("part")).last
      val in = HadoopInputFile.fromPath(
        new Path(parquetFile.getCanonicalPath),
        spark.sessionState.newHadoopConf())

      Utils.tryWithResource(ParquetFileReader.open(in)) { reader =>
        val blocks = reader.getFooter.getBlocks
        assert(blocks.size() > 1)
        val columns = blocks.get(0).getColumns
        assert(columns.size() === 1)
        val columnIndex = reader.readColumnIndex(columns.get(0))
        assert(columnIndex.getMinValues.size() > 1)

        val rowGroupCnt = blocks.get(0).getRowCount
        // Page count = Second page min value - first page min value
        val pageCnt = columnIndex.getMinValues.get(1).asLongBuffer().get() -
          columnIndex.getMinValues.get(0).asLongBuffer().get()
        assert(pageCnt < rowGroupCnt)
        Seq(true, false).foreach { columnIndex =>
          withSQLConf(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED -> s"$columnIndex") {
            val df = spark.read.parquet(parquetFile.getCanonicalPath).where("id = 1")
            df.collect()
            val plan = df.queryExecution.executedPlan
            val metrics = plan.collectLeaves().head.metrics
            val numOutputRows = metrics("numOutputRows").value
            if (columnIndex) {
              assert(numOutputRows === pageCnt)
            } else {
              assert(numOutputRows === rowGroupCnt)
            }
          }
        }
      }
    }
  }

  test("SPARK-34562: Bloom filter push down") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(100).selectExpr("id * 2 AS id")
        .write
        .option(ParquetOutputFormat.BLOOM_FILTER_ENABLED + "#id", true)
        // Disable dictionary because the distinct values less than 40000.
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, false)
        .parquet(path)

      Seq(true, false).foreach { bloomFilterEnabled =>
        withSQLConf(ParquetInputFormat.BLOOM_FILTERING_ENABLED -> bloomFilterEnabled.toString) {
          val accu = new NumRowGroupsAcc
          sparkContext.register(accu)

          val df = spark.read.parquet(path).filter("id = 19")
          df.foreachPartition((it: Iterator[Row]) => it.foreach(_ => accu.add(0)))
          if (bloomFilterEnabled) {
            assert(accu.value === 0)
          } else {
            assert(accu.value > 0)
          }

          AccumulatorContext.remove(accu.id)
        }
      }
    }
  }

  test("SPARK-36866: filter pushdown - year-month interval") {
    def months(m: Int): Period = Period.ofMonths(m)
    def monthsLit(m: Int): Literal = Literal(months(m))
    val data = (1 to 4).map(i => Tuple1(Option(months(i))))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val iAttr = df(colName).expr
      assert(df(colName).expr.dataType === YearMonthIntervalType())

      checkFilterPredicate(iAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(iAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(months(i)))))

      checkFilterPredicate(iAttr === monthsLit(1), classOf[Eq[_]], resultFun(months(1)))
      checkFilterPredicate(iAttr <=> monthsLit(1), classOf[Eq[_]], resultFun(months(1)))
      checkFilterPredicate(iAttr =!= monthsLit(1), classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(months(i)))))

      checkFilterPredicate(iAttr < monthsLit(2), classOf[Lt[_]], resultFun(months(1)))
      checkFilterPredicate(iAttr > monthsLit(3), classOf[Gt[_]], resultFun(months(4)))
      checkFilterPredicate(iAttr <= monthsLit(1), classOf[LtEq[_]], resultFun(months(1)))
      checkFilterPredicate(iAttr >= monthsLit(4), classOf[GtEq[_]], resultFun(months(4)))

      checkFilterPredicate(monthsLit(1) === iAttr, classOf[Eq[_]], resultFun(months(1)))
      checkFilterPredicate(monthsLit(1) <=> iAttr, classOf[Eq[_]], resultFun(months(1)))
      checkFilterPredicate(monthsLit(2) > iAttr, classOf[Lt[_]], resultFun(months(1)))
      checkFilterPredicate(monthsLit(3) < iAttr, classOf[Gt[_]], resultFun(months(4)))
      checkFilterPredicate(monthsLit(1) >= iAttr, classOf[LtEq[_]], resultFun(months(1)))
      checkFilterPredicate(monthsLit(4) <= iAttr, classOf[GtEq[_]], resultFun(months(4)))

      checkFilterPredicate(!(iAttr < monthsLit(4)), classOf[GtEq[_]], resultFun(months(4)))
      checkFilterPredicate(iAttr < monthsLit(2) || iAttr > monthsLit(3), classOf[Operators.Or],
        Seq(Row(resultFun(months(1))), Row(resultFun(months(4)))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(iAttr, Array(2, 3, 4, 5, 6, 7).map(monthsLit)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(months(2))), Row(resultFun(months(3))), Row(resultFun(months(4)))))
        }
      }
    }
  }

  test("SPARK-36866: filter pushdown - day-time interval") {
    def secs(m: Int): Duration = Duration.ofSeconds(m)
    def secsLit(m: Int): Literal = Literal(secs(m))
    val data = (1 to 4).map(i => Tuple1(Option(secs(i))))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val iAttr = df(colName).expr
      assert(df(colName).expr.dataType === DayTimeIntervalType())

      checkFilterPredicate(iAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(iAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(secs(i)))))

      checkFilterPredicate(iAttr === secsLit(1), classOf[Eq[_]], resultFun(secs(1)))
      checkFilterPredicate(iAttr <=> secsLit(1), classOf[Eq[_]], resultFun(secs(1)))
      checkFilterPredicate(iAttr =!= secsLit(1), classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(secs(i)))))

      checkFilterPredicate(iAttr < secsLit(2), classOf[Lt[_]], resultFun(secs(1)))
      checkFilterPredicate(iAttr > secsLit(3), classOf[Gt[_]], resultFun(secs(4)))
      checkFilterPredicate(iAttr <= secsLit(1), classOf[LtEq[_]], resultFun(secs(1)))
      checkFilterPredicate(iAttr >= secsLit(4), classOf[GtEq[_]], resultFun(secs(4)))

      checkFilterPredicate(secsLit(1) === iAttr, classOf[Eq[_]], resultFun(secs(1)))
      checkFilterPredicate(secsLit(1) <=> iAttr, classOf[Eq[_]], resultFun(secs(1)))
      checkFilterPredicate(secsLit(2) > iAttr, classOf[Lt[_]], resultFun(secs(1)))
      checkFilterPredicate(secsLit(3) < iAttr, classOf[Gt[_]], resultFun(secs(4)))
      checkFilterPredicate(secsLit(1) >= iAttr, classOf[LtEq[_]], resultFun(secs(1)))
      checkFilterPredicate(secsLit(4) <= iAttr, classOf[GtEq[_]], resultFun(secs(4)))

      checkFilterPredicate(!(iAttr < secsLit(4)), classOf[GtEq[_]], resultFun(secs(4)))
      checkFilterPredicate(iAttr < secsLit(2) || iAttr > secsLit(3), classOf[Operators.Or],
        Seq(Row(resultFun(secs(1))), Row(resultFun(secs(4)))))

      Seq(3, 20).foreach { threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
          checkFilterPredicate(
            In(iAttr, Array(2, 3, 4, 5, 6, 7).map(secsLit)),
            if (threshold == 3) classOf[Operators.And] else classOf[Operators.Or],
            Seq(Row(resultFun(secs(2))), Row(resultFun(secs(3))), Row(resultFun(secs(4)))))
        }
      }
    }
  }
}

@ExtendedSQLTest
class ParquetV1FilterSuite extends ParquetFilterSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")

  override def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (DataFrame, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    Seq(("parquet", true), ("", false)).foreach { case (pushdownDsList, nestedPredicatePushdown) =>
      withSQLConf(
        SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
        SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
        SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
        SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED.key -> "true",
        // Disable adding filters from constraints because it adds, for instance,
        // is-not-null to pushed filters, which makes it hard to test if the pushed
        // filter is expected or not (this had to be fixed with SPARK-13495).
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName,
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
        SQLConf.NESTED_PREDICATE_PUSHDOWN_FILE_SOURCE_LIST.key -> pushdownDsList) {
        val query = df
          .select(output.map(e => Column(e)): _*)
          .where(Column(predicate))

        val nestedOrAttributes = predicate.collectFirst {
          case g: GetStructField => g
          case a: Attribute => a
        }
        assert(nestedOrAttributes.isDefined, "No GetStructField nor Attribute is detected.")

        val parsed = parseColumnPath(
          PushableColumnAndNestedColumn.unapply(nestedOrAttributes.get).get)

        val containsNestedColumnOrDot = parsed.length > 1 || parsed(0).contains(".")

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
        // If predicates contains nested column or dot, we push down the predicates only if
        // "parquet" is in `NESTED_PREDICATE_PUSHDOWN_V1_SOURCE_LIST`.
        if (nestedPredicatePushdown || !containsNestedColumnOrDot) {
          assert(selectedFilters.nonEmpty, "No filter is pushed down")
          val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
          val parquetFilters = createParquetFilters(schema)
          // In this test suite, all the simple predicates are convertible here.
          assert(parquetFilters.convertibleFilters(selectedFilters) === selectedFilters)
          val pushedParquetFilters = selectedFilters.map { pred =>
            val maybeFilter = parquetFilters.createFilter(pred)
            assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
            maybeFilter.get
          }
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          assert(pushedParquetFilters.exists(_.getClass === filterClass),
            s"${pushedParquetFilters.map(_.getClass).toList} did not contain ${filterClass}.")

          checker(stripSparkFilter(query), expected)
        } else {
          assert(selectedFilters.isEmpty, "There is filter pushed down")
        }
      }
    }
  }
}

@ExtendedSQLTest
class ParquetV2FilterSuite extends ParquetFilterSuite {
  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  override def checkFilterPredicate(
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
      // Disable adding filters from constraints because it adds, for instance,
      // is-not-null to pushed filters, which makes it hard to test if the pushed
      // filter is expected or not (this had to be fixed with SPARK-13495).
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName,
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
      val query = df
        .select(output.map(e => Column(e)): _*)
        .where(Column(predicate))

      query.queryExecution.optimizedPlan.collectFirst {
        case PhysicalOperation(_, filters,
            DataSourceV2ScanRelation(_, scan: ParquetScan, _)) =>
          assert(filters.nonEmpty, "No filter is analyzed from the given query")
          val sourceFilters = filters.flatMap(DataSourceStrategy.translateFilter(_, true)).toArray
          val pushedFilters = scan.pushedFilters
          assert(pushedFilters.nonEmpty, "No filter is pushed down")
          val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
          val parquetFilters = createParquetFilters(schema)
          // In this test suite, all the simple predicates are convertible here.
          assert(parquetFilters.convertibleFilters(sourceFilters) === pushedFilters)
          val pushedParquetFilters = pushedFilters.map { pred =>
            val maybeFilter = parquetFilters.createFilter(pred)
            assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
            maybeFilter.get
          }
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          assert(pushedParquetFilters.exists(_.getClass === filterClass),
            s"${pushedParquetFilters.map(_.getClass).toList} did not contain ${filterClass}.")

          checker(stripSparkFilter(query), expected)

        case _ =>
          throw new AnalysisException("Can not match ParquetTable in the query.")
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
