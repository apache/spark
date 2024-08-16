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

package org.apache.spark.sql.execution.datasources.orc

import java.math.MathContext
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.{Duration, LocalDateTime, Period}

import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.ql.io.sarg.{PredicateLeaf, SearchArgument, SearchArgumentImpl}
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory.newBuilder

import org.apache.spark.{SparkConf, SparkException, SparkRuntimeException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.ExpressionUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ArrayImplicits._

/**
 * A test suite that tests Apache ORC filter API based filter pushdown optimization.
 */
@ExtendedSQLTest
class OrcFilterSuite extends OrcTest with SharedSparkSession {
  import testImplicits.toRichColumn

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  protected def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      checker: (SearchArgument) => Unit): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct
    val query = df
      .select(output.map(e => ExpressionUtils.column(e)): _*)
      .where(ExpressionUtils.column(predicate))

    query.queryExecution.optimizedPlan match {
      case PhysicalOperation(_, filters, DataSourceV2ScanRelation(_, o: OrcScan, _, _, _)) =>
        assert(filters.nonEmpty, "No filter is analyzed from the given query")
        assert(o.pushedFilters.nonEmpty, "No filter is pushed down")
        val maybeFilter = OrcFilters.createFilter(query.schema, o.pushedFilters.toImmutableArraySeq)
        assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for " +
          s"${o.pushedFilters.mkString("pushedFilters(", ", ", ")")}")
        checker(maybeFilter.get)

      case _ => assert(false, "Can not match OrcTable in the query.")
    }
  }

  protected def checkFilterPredicate
      (predicate: Predicate, filterOperator: PredicateLeaf.Operator)
      (implicit df: DataFrame): Unit = {
    def checkComparisonOperator(filter: SearchArgument) = {
      val operator = filter.getLeaves.asScala
      assert(operator.map(_.getOperator).contains(filterOperator))
    }
    checkFilterPredicate(df, predicate, checkComparisonOperator)
  }

  protected def checkFilterPredicate
      (predicate: Predicate, stringExpr: String)
      (implicit df: DataFrame): Unit = {
    def checkLogicalOperator(filter: SearchArgument) = {
      // HIVE-24458 changes toString output and provides `toOldString` for old style.
      assert(filter.asInstanceOf[SearchArgumentImpl].toOldString == stringExpr)
    }
    checkFilterPredicate(df, predicate, checkLogicalOperator)
  }

  test("filter pushdown - integer") {
    withNestedOrcDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { case (inputDF, colName, _) =>
      implicit val df: DataFrame = inputDF

      val intAttr = df(colName).expr
      assert(df(colName).expr.dataType === IntegerType)

      checkFilterPredicate(intAttr.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate(intAttr === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(intAttr <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate(intAttr < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(intAttr > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(intAttr <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(intAttr >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === intAttr, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> intAttr, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > intAttr, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < intAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= intAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= intAttr, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - long") {
    withNestedOrcDataFrame(
        (1 to 4).map(i => Tuple1(Option(i.toLong)))) { case (inputDF, colName, _) =>
      implicit val df: DataFrame = inputDF

      val longAttr = df(colName).expr
      assert(df(colName).expr.dataType === LongType)

      checkFilterPredicate(longAttr.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate(longAttr === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(longAttr <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate(longAttr < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(longAttr > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(longAttr <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(longAttr >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === longAttr, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> longAttr, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > longAttr, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < longAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= longAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= longAttr, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - float") {
    withNestedOrcDataFrame(
        (1 to 4).map(i => Tuple1(Option(i.toFloat)))) { case (inputDF, colName, _) =>
      implicit val df: DataFrame = inputDF

      val floatAttr = df(colName).expr
      assert(df(colName).expr.dataType === FloatType)

      checkFilterPredicate(floatAttr.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate(floatAttr === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(floatAttr <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate(floatAttr < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(floatAttr > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(floatAttr <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(floatAttr >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === floatAttr, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> floatAttr, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > floatAttr, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < floatAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= floatAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= floatAttr, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - double") {
    withNestedOrcDataFrame(
        (1 to 4).map(i => Tuple1(Option(i.toDouble)))) { case (inputDF, colName, _) =>
      implicit val df: DataFrame = inputDF

      val doubleAttr = df(colName).expr
      assert(df(colName).expr.dataType === DoubleType)

      checkFilterPredicate(doubleAttr.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate(doubleAttr === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(doubleAttr <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate(doubleAttr < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(doubleAttr > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(doubleAttr <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(doubleAttr >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === doubleAttr, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> doubleAttr, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > doubleAttr, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < doubleAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= doubleAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= doubleAttr, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - string") {
    withNestedOrcDataFrame(
        (1 to 4).map(i => Tuple1(i.toString))) { case (inputDF, colName, _) =>
      implicit val df: DataFrame = inputDF

      val strAttr = df(colName).expr
      assert(df(colName).expr.dataType === StringType)

      checkFilterPredicate(strAttr.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate(strAttr === "1", PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(strAttr <=> "1", PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate(strAttr < "2", PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(strAttr > "3", PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(strAttr <= "1", PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(strAttr >= "4", PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal("1") === strAttr, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal("1") <=> strAttr, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal("2") > strAttr, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal("3") < strAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal("1") >= strAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal("4") <= strAttr, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - boolean") {
    withNestedOrcDataFrame(
        (true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { case (inputDF, colName, _) =>
      implicit val df: DataFrame = inputDF

      val booleanAttr = df(colName).expr
      assert(df(colName).expr.dataType === BooleanType)

      checkFilterPredicate(booleanAttr.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate(booleanAttr === true, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(booleanAttr <=> true, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate(booleanAttr < true, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(booleanAttr > false, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(booleanAttr <= false, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(booleanAttr >= false, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(false) === booleanAttr, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(false) <=> booleanAttr,
        PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(false) > booleanAttr, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(true) < booleanAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(true) >= booleanAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(true) <= booleanAttr, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - decimal") {
    withNestedOrcDataFrame(
        (1 to 4).map(i => Tuple1.apply(BigDecimal.valueOf(i)))) { case (inputDF, colName, _) =>
      implicit val df: DataFrame = inputDF

      val decimalAttr = df(colName).expr
      assert(df(colName).expr.dataType === DecimalType(38, 18))

      checkFilterPredicate(decimalAttr.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate(decimalAttr === BigDecimal.valueOf(1), PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(decimalAttr <=> BigDecimal.valueOf(1),
        PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate(decimalAttr < BigDecimal.valueOf(2), PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(decimalAttr > BigDecimal.valueOf(3),
        PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(decimalAttr <= BigDecimal.valueOf(1),
        PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(decimalAttr >= BigDecimal.valueOf(4), PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(
        Literal(BigDecimal.valueOf(1)) === decimalAttr, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(1)) <=> decimalAttr, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(2)) > decimalAttr, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(3)) < decimalAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(1)) >= decimalAttr, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(
        Literal(BigDecimal.valueOf(4)) <= decimalAttr, PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - timestamp") {
    val input = Seq(
      "1000-01-01 01:02:03",
      "1582-10-01 00:11:22",
      "1900-01-01 23:59:59",
      "2020-05-25 10:11:12").map(Timestamp.valueOf)

    withOrcFile(input.map(Tuple1(_))) { path =>
      Seq(false, true).foreach { java8Api =>
        withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString) {
          readFile(path) { implicit df =>
            val timestamps = input.map(Literal(_))
            checkFilterPredicate($"_1".isNull, PredicateLeaf.Operator.IS_NULL)

            checkFilterPredicate($"_1" === timestamps(0), PredicateLeaf.Operator.EQUALS)
            checkFilterPredicate($"_1" <=> timestamps(0), PredicateLeaf.Operator.NULL_SAFE_EQUALS)

            checkFilterPredicate($"_1" < timestamps(1), PredicateLeaf.Operator.LESS_THAN)
            checkFilterPredicate($"_1" > timestamps(2), PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate($"_1" <= timestamps(0), PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate($"_1" >= timestamps(3), PredicateLeaf.Operator.LESS_THAN)

            checkFilterPredicate(Literal(timestamps(0)) === $"_1", PredicateLeaf.Operator.EQUALS)
            checkFilterPredicate(
              Literal(timestamps(0)) <=> $"_1", PredicateLeaf.Operator.NULL_SAFE_EQUALS)
            checkFilterPredicate(Literal(timestamps(1)) > $"_1", PredicateLeaf.Operator.LESS_THAN)
            checkFilterPredicate(
              Literal(timestamps(2)) < $"_1",
              PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate(
              Literal(timestamps(0)) >= $"_1",
              PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate(Literal(timestamps(3)) <= $"_1", PredicateLeaf.Operator.LESS_THAN)
          }
        }
      }
    }
  }

  test("SPARK-36357: filter pushdown - timestamp_ntz") {
    val localDateTimes = Seq(
      LocalDateTime.of(1000, 1, 1, 1, 2, 3, 456000000),
      LocalDateTime.of(1582, 10, 1, 0, 11, 22, 456000000),
      LocalDateTime.of(1900, 1, 1, 23, 59, 59, 456000000),
      LocalDateTime.of(2020, 5, 25, 10, 11, 12, 456000000))
    withOrcFile(localDateTimes.map(Tuple1(_))) { path =>
      readFile(path) { implicit df =>
        checkFilterPredicate($"_1".isNull, PredicateLeaf.Operator.IS_NULL)

        checkFilterPredicate($"_1" === localDateTimes(0), PredicateLeaf.Operator.EQUALS)
        checkFilterPredicate($"_1" <=> localDateTimes(0), PredicateLeaf.Operator.NULL_SAFE_EQUALS)

        checkFilterPredicate($"_1" < localDateTimes(1), PredicateLeaf.Operator.LESS_THAN)
        checkFilterPredicate($"_1" > localDateTimes(2), PredicateLeaf.Operator.LESS_THAN_EQUALS)
        checkFilterPredicate($"_1" <= localDateTimes(0), PredicateLeaf.Operator.LESS_THAN_EQUALS)
        checkFilterPredicate($"_1" >= localDateTimes(3), PredicateLeaf.Operator.LESS_THAN)

        checkFilterPredicate(Literal(localDateTimes(0)) === $"_1", PredicateLeaf.Operator.EQUALS)
        checkFilterPredicate(
          Literal(localDateTimes(0)) <=> $"_1", PredicateLeaf.Operator.NULL_SAFE_EQUALS)
        checkFilterPredicate(Literal(localDateTimes(1)) > $"_1", PredicateLeaf.Operator.LESS_THAN)
        checkFilterPredicate(
          Literal(localDateTimes(2)) < $"_1",
          PredicateLeaf.Operator.LESS_THAN_EQUALS)
        checkFilterPredicate(
          Literal(localDateTimes(0)) >= $"_1",
          PredicateLeaf.Operator.LESS_THAN_EQUALS)
        checkFilterPredicate(Literal(localDateTimes(3)) <= $"_1", PredicateLeaf.Operator.LESS_THAN)
      }
    }
  }

  test("filter pushdown - combinations with logical operators") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate(
        $"_1".isNotNull,
        "leaf-0 = (IS_NULL _1), expr = (not leaf-0)"
      )
      checkFilterPredicate(
        $"_1" =!= 1,
        "leaf-0 = (IS_NULL _1), leaf-1 = (EQUALS _1 1), expr = (and (not leaf-0) (not leaf-1))"
      )
      checkFilterPredicate(
        !($"_1" < 4),
        "leaf-0 = (IS_NULL _1), leaf-1 = (LESS_THAN _1 4), expr = (and (not leaf-0) (not leaf-1))"
      )
      checkFilterPredicate(
        $"_1" < 2 || $"_1" > 3,
        "leaf-0 = (LESS_THAN _1 2), leaf-1 = (LESS_THAN_EQUALS _1 3), " +
          "expr = (or leaf-0 (not leaf-1))"
      )
      checkFilterPredicate(
        $"_1" < 2 && $"_1" > 3,
        "leaf-0 = (IS_NULL _1), leaf-1 = (LESS_THAN _1 2), leaf-2 = (LESS_THAN_EQUALS _1 3), " +
          "expr = (and (not leaf-0) leaf-1 (not leaf-2))"
      )
    }
  }

  test("filter pushdown - date") {
    val input = Seq("2017-08-18", "2017-08-19", "2017-08-20", "2017-08-21").map { day =>
      Date.valueOf(day)
    }
    withOrcFile(input.map(Tuple1(_))) { path =>
      Seq(false, true).foreach { java8Api =>
        withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString) {
          readFile(path) { implicit df =>
            val dates = input.map(Literal(_))
            checkFilterPredicate($"_1".isNull, PredicateLeaf.Operator.IS_NULL)

            checkFilterPredicate($"_1" === dates(0), PredicateLeaf.Operator.EQUALS)
            checkFilterPredicate($"_1" <=> dates(0), PredicateLeaf.Operator.NULL_SAFE_EQUALS)

            checkFilterPredicate($"_1" < dates(1), PredicateLeaf.Operator.LESS_THAN)
            checkFilterPredicate($"_1" > dates(2), PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate($"_1" <= dates(0), PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate($"_1" >= dates(3), PredicateLeaf.Operator.LESS_THAN)

            checkFilterPredicate(dates(0) === $"_1", PredicateLeaf.Operator.EQUALS)
            checkFilterPredicate(dates(0) <=> $"_1", PredicateLeaf.Operator.NULL_SAFE_EQUALS)
            checkFilterPredicate(dates(1) > $"_1", PredicateLeaf.Operator.LESS_THAN)
            checkFilterPredicate(dates(2) < $"_1", PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate(dates(0) >= $"_1", PredicateLeaf.Operator.LESS_THAN_EQUALS)
            checkFilterPredicate(dates(3) <= $"_1", PredicateLeaf.Operator.LESS_THAN)
          }
        }
      }
    }
  }

  test("SPARK-36960: filter pushdown - year-month interval") {
    DataTypeTestUtils.yearMonthIntervalTypes.foreach { ymIntervalType =>

      def periods(i: Int): Expression = Literal(Period.of(i, i, 0)).cast(ymIntervalType)

      val baseDF = spark.createDataFrame((1 to 4).map { i =>
        Tuple1.apply(Period.of(i, i, 0))
      }).select(col("_1").cast(ymIntervalType))

      withNestedOrcDataFrame(baseDF) {
        case (inputDF, colName, _) =>
          implicit val df: DataFrame = inputDF

          val ymIntervalAttr = df(colName).expr
          assert(df(colName).expr.dataType === ymIntervalType)

         checkFilterPredicate(ymIntervalAttr.isNull, PredicateLeaf.Operator.IS_NULL)

          checkFilterPredicate(ymIntervalAttr === periods(1),
            PredicateLeaf.Operator.EQUALS)
          checkFilterPredicate(ymIntervalAttr <=> periods(1),
            PredicateLeaf.Operator.NULL_SAFE_EQUALS)
          checkFilterPredicate(ymIntervalAttr < periods(2),
            PredicateLeaf.Operator.LESS_THAN)
          checkFilterPredicate(ymIntervalAttr > periods(3),
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(ymIntervalAttr <= periods(1),
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(ymIntervalAttr >= periods(4),
            PredicateLeaf.Operator.LESS_THAN)

          checkFilterPredicate(periods(1) === ymIntervalAttr,
            PredicateLeaf.Operator.EQUALS)
          checkFilterPredicate(periods(1) <=> ymIntervalAttr,
            PredicateLeaf.Operator.NULL_SAFE_EQUALS)
          checkFilterPredicate(periods(2) > ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN)
          checkFilterPredicate(periods(3) < ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(periods(1) >= ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(periods(4) <= ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN)
      }
    }
  }

  test("SPARK-36960: filter pushdown - day-time interval") {
    DataTypeTestUtils.dayTimeIntervalTypes.foreach { dtIntervalType =>

      def durations(i: Int): Expression =
        Literal(Duration.ofDays(i).plusHours(i).plusMinutes(i).plusSeconds(i)).cast(dtIntervalType)

      val baseDF = spark.createDataFrame((1 to 4).map { i =>
        Tuple1.apply(Duration.ofDays(i).plusHours(i).plusMinutes(i).plusSeconds(i))
      }).select(col("_1").cast(dtIntervalType))

      withNestedOrcDataFrame(baseDF) {
        case (inputDF, colName, _) =>
          implicit val df: DataFrame = inputDF

          val ymIntervalAttr = df(colName).expr
          assert(df(colName).expr.dataType === dtIntervalType)

          checkFilterPredicate(ymIntervalAttr.isNull, PredicateLeaf.Operator.IS_NULL)

          checkFilterPredicate(ymIntervalAttr === durations(1),
            PredicateLeaf.Operator.EQUALS)
          checkFilterPredicate(ymIntervalAttr <=> durations(1),
            PredicateLeaf.Operator.NULL_SAFE_EQUALS)
          checkFilterPredicate(ymIntervalAttr < durations(2),
            PredicateLeaf.Operator.LESS_THAN)
          checkFilterPredicate(ymIntervalAttr > durations(3),
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(ymIntervalAttr <= durations(1),
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(ymIntervalAttr >= durations(4),
            PredicateLeaf.Operator.LESS_THAN)

          checkFilterPredicate(durations(1) === ymIntervalAttr,
            PredicateLeaf.Operator.EQUALS)
          checkFilterPredicate(durations(1) <=> ymIntervalAttr,
            PredicateLeaf.Operator.NULL_SAFE_EQUALS)
          checkFilterPredicate(durations(2) > ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN)
          checkFilterPredicate(durations(3) < ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(durations(1) >= ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN_EQUALS)
          checkFilterPredicate(durations(4) <= ymIntervalAttr,
            PredicateLeaf.Operator.LESS_THAN)
      }
    }
  }

  test("no filter pushdown - non-supported types") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }
    // ArrayType
    withOrcDataFrame((1 to 4).map(i => Tuple1(Array(i)))) { implicit df =>
      checkNoFilterPredicate($"_1".isNull, noneSupported = true)
    }
    // BinaryType
    withOrcDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkNoFilterPredicate($"_1" <=> 1.b, noneSupported = true)
    }
    // MapType
    withOrcDataFrame((1 to 4).map(i => Tuple1(Map(i -> i)))) { implicit df =>
      checkNoFilterPredicate($"_1".isNotNull, noneSupported = true)
    }
  }

  test("SPARK-12218 and SPARK-25699 Converting conjunctions into ORC SearchArguments") {
    import org.apache.spark.sql.sources._
    // The `LessThan` should be converted while the `StringContains` shouldn't
    val schema = new StructType(
      Array(
        StructField("a", IntegerType, nullable = true),
        StructField("b", StringType, nullable = true)))
    assertResult("leaf-0 = (LESS_THAN a 10), expr = leaf-0") {
      OrcFilters.createFilter(schema, Array(
        LessThan("a", 10),
        StringContains("b", "prefix")
      ).toImmutableArraySeq).get.asInstanceOf[SearchArgumentImpl].toOldString
    }

    // The `LessThan` should be converted while the whole inner `And` shouldn't
    assertResult("leaf-0 = (LESS_THAN a 10), expr = leaf-0") {
      OrcFilters.createFilter(schema, Array(
        LessThan("a", 10),
        Not(And(
          GreaterThan("a", 1),
          StringContains("b", "prefix")
        ))
      ).toImmutableArraySeq).get.asInstanceOf[SearchArgumentImpl].toOldString
    }

    // Safely remove unsupported `StringContains` predicate and push down `LessThan`
    assertResult("leaf-0 = (LESS_THAN a 10), expr = leaf-0") {
      OrcFilters.createFilter(schema, Array(
        And(
          LessThan("a", 10),
          StringContains("b", "prefix")
        )
      ).toImmutableArraySeq).get.asInstanceOf[SearchArgumentImpl].toOldString
    }

    // Safely remove unsupported `StringContains` predicate, push down `LessThan` and `GreaterThan`.
    assertResult("leaf-0 = (LESS_THAN a 10), leaf-1 = (LESS_THAN_EQUALS a 1)," +
      " expr = (and leaf-0 (not leaf-1))") {
      OrcFilters.createFilter(schema, Array(
        And(
          And(
            LessThan("a", 10),
            StringContains("b", "prefix")
          ),
          GreaterThan("a", 1)
        )
      ).toImmutableArraySeq).get.asInstanceOf[SearchArgumentImpl].toOldString
    }
  }

  test("SPARK-27699 Converting disjunctions into ORC SearchArguments") {
    import org.apache.spark.sql.sources._
    // The `LessThan` should be converted while the `StringContains` shouldn't
    val schema = new StructType(
      Array(
        StructField("a", IntegerType, nullable = true),
        StructField("b", StringType, nullable = true)))

    // The predicate `StringContains` predicate is not able to be pushed down.
    assertResult("leaf-0 = (LESS_THAN_EQUALS a 10), leaf-1 = (LESS_THAN a 1)," +
      " expr = (or (not leaf-0) leaf-1)") {
      OrcFilters.createFilter(schema, Array(
        Or(
          GreaterThan("a", 10),
          And(
            StringContains("b", "prefix"),
            LessThan("a", 1)
          )
        )
      ).toImmutableArraySeq).get.asInstanceOf[SearchArgumentImpl].toOldString
    }

    assertResult("leaf-0 = (LESS_THAN_EQUALS a 10), leaf-1 = (LESS_THAN a 1)," +
      " expr = (or (not leaf-0) leaf-1)") {
      OrcFilters.createFilter(schema, Array(
        Or(
          And(
            GreaterThan("a", 10),
            StringContains("b", "foobar")
          ),
          And(
            StringContains("b", "prefix"),
            LessThan("a", 1)
          )
        )
      ).toImmutableArraySeq).get.asInstanceOf[SearchArgumentImpl].toOldString
    }

    assert(OrcFilters.createFilter(schema, Array(
      Or(
        StringContains("b", "foobar"),
        And(
          StringContains("b", "prefix"),
          LessThan("a", 1)
        )
      )
    ).toImmutableArraySeq).isEmpty)
  }

  test("SPARK-27160: Fix casting of the DecimalType literal") {
    import org.apache.spark.sql.sources._
    val schema = StructType(Array(StructField("a", DecimalType(3, 2))))
    assertResult("leaf-0 = (LESS_THAN a 3.14), expr = leaf-0") {
      OrcFilters.createFilter(schema, Array(
        LessThan(
          "a",
          new java.math.BigDecimal(3.14, MathContext.DECIMAL64).setScale(2))).toImmutableArraySeq
      ).get.asInstanceOf[SearchArgumentImpl].toOldString
    }
  }

  test("SPARK-32622: case sensitivity in predicate pushdown") {
    withTempPath { dir =>
      val count = 10
      val tableName = "spark_32622"
      val tableDir1 = s"${dir.getAbsoluteFile}/table1"

      // Physical ORC files have both `A` and `a` fields.
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        spark.range(count).repartition(count).selectExpr("id - 1 as A", "id as a")
          .write.mode("overwrite").orc(tableDir1)
      }

      // Metastore table has both `A` and `a` fields too.
      withTable(tableName) {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          sql(
            s"""
               |CREATE TABLE $tableName (A LONG, a LONG) USING ORC LOCATION '$tableDir1'
             """.stripMargin)

          checkAnswer(sql(s"select a, A from $tableName"), (0 until count).map(c => Row(c, c - 1)))

          val actual1 = stripSparkFilter(sql(s"select A from $tableName where A < 0"))
          assert(actual1.count() == 1)

          val actual2 = stripSparkFilter(sql(s"select A from $tableName where a < 0"))
          assert(actual2.count() == 0)
        }

        // Exception thrown for ambiguous case.
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"select a from $tableName where a < 0").collect()
            },
            errorClass = "AMBIGUOUS_REFERENCE",
            parameters = Map(
              "name" -> "`a`",
              "referenceNames" -> ("[`spark_catalog`.`default`.`spark_32622`.`a`, " +
                "`spark_catalog`.`default`.`spark_32622`.`a`]")),
            context = ExpectedContext(
              fragment = "a",
              start = 32,
              stop = 32
            )
          )
        }
      }

      // Metastore table has only `A` field.
      withTable(tableName) {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
          sql(
            s"""
               |CREATE TABLE $tableName (A LONG) USING ORC LOCATION '$tableDir1'
             """.stripMargin)

          val ex = intercept[SparkException] {
            sql(s"select A from $tableName where A < 0").collect()
          }
          assert(ex.getErrorClass.startsWith("FAILED_READ_FILE"))
          assert(ex.getCause.isInstanceOf[SparkRuntimeException])
          assert(ex.getCause.getMessage.contains(
            """Found duplicate field(s) "A": [A, a] in case-insensitive mode"""))
        }
      }

      // Physical ORC files have only `A` field.
      val tableDir2 = s"${dir.getAbsoluteFile}/table2"
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        spark.range(count).repartition(count).selectExpr("id - 1 as A")
          .write.mode("overwrite").orc(tableDir2)
      }

      withTable(tableName) {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
          sql(
            s"""
               |CREATE TABLE $tableName (a LONG) USING ORC LOCATION '$tableDir2'
             """.stripMargin)

          checkAnswer(sql(s"select a from $tableName"), (0 until count).map(c => Row(c - 1)))

          val actual = stripSparkFilter(sql(s"select a from $tableName where a < 0"))
          assert(actual.count() == 1)
        }
      }

      withTable(tableName) {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          sql(
            s"""
               |CREATE TABLE $tableName (A LONG) USING ORC LOCATION '$tableDir2'
             """.stripMargin)

          checkAnswer(sql(s"select A from $tableName"), (0 until count).map(c => Row(c - 1)))

          val actual = stripSparkFilter(sql(s"select A from $tableName where A < 0"))
          assert(actual.count() == 1)
        }
      }
    }
  }

  test("SPARK-32646: Case-insensitive field resolution for pushdown when reading ORC") {
    import org.apache.spark.sql.sources._

    def getOrcFilter(
        schema: StructType,
        filters: Seq[Filter],
        caseSensitive: String): Option[SearchArgument] = {
      var orcFilter: Option[SearchArgument] = None
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        orcFilter =
          OrcFilters.createFilter(schema, filters)
      }
      orcFilter
    }

    def testFilter(
        schema: StructType,
        filters: Seq[Filter],
        expected: SearchArgument): Unit = {
      val caseSensitiveFilters = getOrcFilter(schema, filters, "true")
      val caseInsensitiveFilters = getOrcFilter(schema, filters, "false")

      assert(caseSensitiveFilters.isEmpty)
      assert(caseInsensitiveFilters.isDefined)

      assert(caseInsensitiveFilters.get.getLeaves().size() > 0)
      assert(caseInsensitiveFilters.get.getLeaves().size() == expected.getLeaves().size())
      (0 until expected.getLeaves().size()).foreach { index =>
        assert(caseInsensitiveFilters.get.getLeaves().get(index) == expected.getLeaves().get(index))
      }
    }

    val schema1 = StructType(Seq(StructField("cint", IntegerType)))
    testFilter(schema1, Seq(GreaterThan("CINT", 1)),
      newBuilder.startNot()
        .lessThanEquals("cint", OrcFilters.getPredicateLeafType(IntegerType), 1L).`end`().build())
    testFilter(schema1, Seq(
      And(GreaterThan("CINT", 1), EqualTo("Cint", 2))),
      newBuilder.startAnd()
        .startNot()
        .lessThanEquals("cint", OrcFilters.getPredicateLeafType(IntegerType), 1L).`end`()
        .equals("cint", OrcFilters.getPredicateLeafType(IntegerType), 2L)
        .`end`().build())

    // Nested column case
    val schema2 = StructType(Seq(StructField("a",
      StructType(Seq(StructField("cint", IntegerType))))))

    testFilter(schema2, Seq(GreaterThan("A.CINT", 1)),
      newBuilder.startNot()
        .lessThanEquals("a.cint", OrcFilters.getPredicateLeafType(IntegerType), 1L).`end`().build())
    testFilter(schema2, Seq(GreaterThan("a.CINT", 1)),
      newBuilder.startNot()
        .lessThanEquals("a.cint", OrcFilters.getPredicateLeafType(IntegerType), 1L).`end`().build())
    testFilter(schema2, Seq(GreaterThan("A.cint", 1)),
      newBuilder.startNot()
        .lessThanEquals("a.cint", OrcFilters.getPredicateLeafType(IntegerType), 1L).`end`().build())
    testFilter(schema2, Seq(
      And(GreaterThan("a.CINT", 1), EqualTo("a.Cint", 2))),
      newBuilder.startAnd()
        .startNot()
        .lessThanEquals("a.cint", OrcFilters.getPredicateLeafType(IntegerType), 1L).`end`()
        .equals("a.cint", OrcFilters.getPredicateLeafType(IntegerType), 2L)
        .`end`().build())
  }
}

