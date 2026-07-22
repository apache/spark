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

package org.apache.spark.sql.execution.datasources.v2

import java.util

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Project}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.fromAttributes
import org.apache.spark.sql.catalyst.util.GeneratedColumn
import org.apache.spark.sql.connector.catalog.{Column, InMemoryTable}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, Metadata, MetadataBuilder, StringType, StructType, TimestampType}

/**
 * Tests for [[GeneratedColumnPartitionFilters]]. For each supported generation expression, verifies
 * that the generated partition column is recognized and that data filters on the base column derive
 * the expected partition filters.
 */
class GeneratedColumnPartitionFilterSuite
  extends QueryTest with SharedSparkSession with PredicateHelper {

  private def emptyProperties = new util.HashMap[String, String]()

  private def generationMetadata(expr: String): Metadata =
    new MetadataBuilder()
      .putString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY, expr)
      .build()

  // Nullability guard emitted by truncating expressions: "(expr) OR ((expr) IS NULL)".
  private def orNull(expr: String): String = s"(($expr) OR (($expr) IS NULL))"

  // Guard emitted by Substring/Identity expressions: "(col IS NULL) OR (expr)".
  private def nullFirst(col: String, expr: String): String = s"(($col IS NULL) OR ($expr))"

  /**
   * Builds a table with the given data columns and generated partition columns, asserts the base
   * column is recognized as the expected [[OptimizablePartitionExpression]], then checks each data
   * filter derives the expected partition filter SQL.
   */
  private def testOptimizablePartitionExpression(
      testName: String,
      dataColumns: Seq[(String, DataType)],
      generatedPartitionColumns: Seq[(String, DataType, String)],
      baseColumn: String,
      expectedPartitionExpr: OptimizablePartitionExpression,
      filterCases: (String, Seq[String])*): Unit = {
    test(testName) {
      val relation = relationWith(dataColumns, generatedPartitionColumns)
      val recognized = GeneratedColumnPartitionFilters.getOptimizablePartitionExpressions(
        spark,
        fromAttributes(relation.output),
        GeneratedColumnPartitionFilters.identityPartitionSchema(relation))
      assert(
        recognized.find(_._1.equalsIgnoreCase(baseColumn)).map(_._2)
          .contains(Seq(expectedPartitionExpr)),
        s"expected $expectedPartitionExpr for '$baseColumn' but got $recognized")
      checkDerivedFilters(relation, filterCases: _*)
    }
  }

  private def relationWith(
      dataColumns: Seq[(String, DataType)],
      generatedPartitionColumns: Seq[(String, DataType, String)]): DataSourceV2Relation = {
    val columns =
      (dataColumns.map { case (name, dt) => Column.create(name, dt) } ++
        generatedPartitionColumns.map { case (name, dt, expr) =>
          Column.create(name, dt, true, null, expr, null)
        }).toArray
    val partitions = generatedPartitionColumns.map(gc => Expressions.identity(gc._1)).toArray
    val table = new InMemoryTable("t", columns, partitions, emptyProperties)
    DataSourceV2Relation.create(table, None, None)
  }

  /**
   * For each (dataFilter, expectedDerivedFilters) case, resolves the data filter SQL against the
   * relation, runs [[GeneratedColumnPartitionFilters.generatePartitionFilters]], and asserts the
   * derived partition filters render to the expected SQL.
   */
  private def checkDerivedFilters(
      relation: DataSourceV2Relation,
      cases: (String, Seq[String])*): Unit = {
    val output = relation.output
    // Constant-folds expressions against the relation output, mirroring what the optimizer does
    // before/after V2ScanRelationPushDown runs (so literals are folded on input and the derived
    // filters render in their final form).
    def fold(exprs: Seq[Expression]): Seq[Expression] =
      ConstantFolding(Project(exprs.map(e => Alias(e, "c")()), LocalRelation(output)))
        .asInstanceOf[Project].projectList.map { case a: Alias => a.child }

    val actual = cases.map { case (dataFilter, _) =>
      val parsed = spark.sessionState.sqlParser.parseExpression(dataFilter)
      val condition = spark.sessionState.analyzer
        .execute(Filter(parsed, LocalRelation(output)))
        .asInstanceOf[Filter].condition
      val dataFilters = fold(splitConjunctivePredicates(condition))
      val derived = GeneratedColumnPartitionFilters.generatePartitionFilters(
        spark, relation, dataFilters)
      dataFilter -> fold(derived).map(_.sql)
    }
    assert(actual == cases,
      s"\nexpected:\n${cases.mkString("\n")}\nactual:\n${actual.mkString("\n")}")
  }

  testOptimizablePartitionExpression(
    "DatePartitionExpr",
    Seq("eventTime" -> TimestampType),
    Seq(("date", DateType, "CAST(eventTime AS DATE)")),
    "eventTime",
    DatePartitionExpr("date"),
    "eventTime < '2021-01-01 18:00:00'" -> Seq(orNull("date <= DATE '2021-01-01'")),
    "eventTime <= '2021-01-01 18:00:00'" -> Seq(orNull("date <= DATE '2021-01-01'")),
    "eventTime = '2021-01-01 18:00:00'" -> Seq(orNull("date = DATE '2021-01-01'")),
    "eventTime > '2021-01-01 18:00:00'" -> Seq(orNull("date >= DATE '2021-01-01'")),
    "eventTime >= '2021-01-01 18:00:00'" -> Seq(orNull("date >= DATE '2021-01-01'")),
    "eventTime is null" -> Seq("(date IS NULL)"),
    // Reversed operand order (literal on the left) should be normalized.
    "'2021-01-01 18:00:00' > eventTime" -> Seq(orNull("date <= DATE '2021-01-01'")),
    "'2021-01-01 18:00:00' >= eventTime" -> Seq(orNull("date <= DATE '2021-01-01'")),
    "'2021-01-01 18:00:00' = eventTime" -> Seq(orNull("date = DATE '2021-01-01'")),
    "'2021-01-01 18:00:00' < eventTime" -> Seq(orNull("date >= DATE '2021-01-01'")),
    "'2021-01-01 18:00:00' <= eventTime" -> Seq(orNull("date >= DATE '2021-01-01'")),
    // A DATE-typed literal is analyzed to a timestamp comparison, so it behaves the same.
    "eventTime < '2021-01-01'" -> Seq(orNull("date <= DATE '2021-01-01'")))

  testOptimizablePartitionExpression(
    "YearPartitionExpr",
    Seq("eventTime" -> TimestampType),
    Seq(("year", IntegerType, "YEAR(eventTime)")),
    "eventTime",
    YearPartitionExpr("year"),
    "eventTime < '2021-06-15 18:30:00'" -> Seq(orNull("year <= 2021")),
    "eventTime = '2021-06-15 18:30:00'" -> Seq(orNull("year = 2021")),
    "eventTime > '2021-06-15 18:30:00'" -> Seq(orNull("year >= 2021")),
    "eventTime is null" -> Seq("(year IS NULL)"))

  testOptimizablePartitionExpression(
    "YearMonthPartitionExpr",
    Seq("eventTime" -> TimestampType),
    Seq(
      ("year", IntegerType, "YEAR(eventTime)"),
      ("month", IntegerType, "MONTH(eventTime)")),
    "eventTime",
    YearMonthPartitionExpr("year", "month"),
    "eventTime < '2021-06-15 18:30:00'" ->
      Seq("((year < 2021) OR ((year = 2021) AND (month <= 6)))"),
    "eventTime = '2021-06-15 18:30:00'" ->
      Seq("((year = 2021) AND (month = 6))"),
    "eventTime > '2021-06-15 18:30:00'" ->
      Seq("((year > 2021) OR ((year = 2021) AND (month >= 6)))"),
    "eventTime is null" ->
      Seq("((year IS NULL) AND (month IS NULL))"))

  testOptimizablePartitionExpression(
    "YearMonthDayPartitionExpr",
    Seq("eventTime" -> TimestampType),
    Seq(
      ("year", IntegerType, "YEAR(eventTime)"),
      ("month", IntegerType, "MONTH(eventTime)"),
      ("day", IntegerType, "DAY(eventTime)")),
    "eventTime",
    YearMonthDayPartitionExpr("year", "month", "day"),
    "eventTime < '2021-06-15 18:30:00'" ->
      Seq("(((year < 2021) OR ((year = 2021) AND (month < 6))) OR " +
        "(((year = 2021) AND (month = 6)) AND (day <= 15)))"),
    "eventTime = '2021-06-15 18:30:00'" ->
      Seq("(((year = 2021) AND (month = 6)) AND (day = 15))"),
    "eventTime > '2021-06-15 18:30:00'" ->
      Seq("(((year > 2021) OR ((year = 2021) AND (month > 6))) OR " +
        "(((year = 2021) AND (month = 6)) AND (day >= 15)))"),
    "eventTime is null" ->
      Seq("(((year IS NULL) AND (month IS NULL)) AND (day IS NULL))"))

  testOptimizablePartitionExpression(
    "YearMonthDayHourPartitionExpr",
    Seq("eventTime" -> TimestampType),
    Seq(
      ("year", IntegerType, "YEAR(eventTime)"),
      ("month", IntegerType, "MONTH(eventTime)"),
      ("day", IntegerType, "DAY(eventTime)"),
      ("hour", IntegerType, "HOUR(eventTime)")),
    "eventTime",
    YearMonthDayHourPartitionExpr("year", "month", "day", "hour"),
    "eventTime < '2021-06-15 18:30:00'" ->
      Seq("((((year < 2021) OR ((year = 2021) AND (month < 6))) OR " +
        "(((year = 2021) AND (month = 6)) AND (day < 15))) OR " +
        "((((year = 2021) AND (month = 6)) AND (day = 15)) AND (hour <= 18)))"),
    "eventTime = '2021-06-15 18:30:00'" ->
      Seq("((((year = 2021) AND (month = 6)) AND (day = 15)) AND (hour = 18))"),
    "eventTime > '2021-06-15 18:30:00'" ->
      Seq("((((year > 2021) OR ((year = 2021) AND (month > 6))) OR " +
        "(((year = 2021) AND (month = 6)) AND (day > 15))) OR " +
        "((((year = 2021) AND (month = 6)) AND (day = 15)) AND (hour >= 18)))"),
    "eventTime is null" ->
      Seq("((((year IS NULL) AND (month IS NULL)) AND (day IS NULL)) AND (hour IS NULL))"))

  testOptimizablePartitionExpression(
    "DateFormatPartitionExpr",
    Seq("eventTime" -> TimestampType),
    Seq(("month", StringType, "DATE_FORMAT(eventTime, 'yyyy-MM')")),
    "eventTime",
    DateFormatPartitionExpr("month", "yyyy-MM"),
    "eventTime < '2021-06-15 18:30:00'" ->
      Seq(orNull("unix_timestamp(month, 'yyyy-MM') <= 1622530800L")),
    "eventTime = '2021-06-15 18:30:00'" ->
      Seq(orNull("unix_timestamp(month, 'yyyy-MM') = 1622530800L")),
    "eventTime > '2021-06-15 18:30:00'" ->
      Seq(orNull("unix_timestamp(month, 'yyyy-MM') >= 1622530800L")),
    "eventTime is null" -> Seq("(month IS NULL)"))

  testOptimizablePartitionExpression(
    "TimestampTruncPartitionExpr",
    Seq("eventTime" -> TimestampType),
    Seq(("hour", TimestampType, "DATE_TRUNC('HOUR', eventTime)")),
    "eventTime",
    TimestampTruncPartitionExpr("HOUR", "hour"),
    "eventTime < '2021-06-15 18:30:00'" ->
      Seq(orNull("hour <= TIMESTAMP '2021-06-15 18:00:00'")),
    "eventTime = '2021-06-15 18:30:00'" ->
      Seq(orNull("hour = TIMESTAMP '2021-06-15 18:00:00'")),
    "eventTime > '2021-06-15 18:30:00'" ->
      Seq(orNull("hour >= TIMESTAMP '2021-06-15 18:00:00'")),
    "eventTime is null" -> Seq("(hour IS NULL)"))

  testOptimizablePartitionExpression(
    "TruncDatePartitionExpr",
    Seq("eventDate" -> DateType),
    Seq(("trunc", DateType, "TRUNC(eventDate, 'YEAR')")),
    "eventDate",
    TruncDatePartitionExpr("trunc", "YEAR"),
    "eventDate < '2021-06-15'" -> Seq(orNull("trunc <= DATE '2021-01-01'")),
    "eventDate = '2021-06-15'" -> Seq(orNull("trunc = DATE '2021-01-01'")),
    "eventDate > '2021-06-15'" -> Seq(orNull("trunc >= DATE '2021-01-01'")),
    "eventDate is null" -> Seq("(trunc IS NULL)"))

  testOptimizablePartitionExpression(
    "SubstringPartitionExpr",
    Seq("eventString" -> StringType),
    Seq(("prefix", StringType, "SUBSTRING(eventString, 1, 4)")),
    "eventString",
    SubstringPartitionExpr("prefix", 1, 4),
    "eventString < 'abcdefgh'" -> Seq(nullFirst("prefix", "prefix <= 'abcd'")),
    "eventString = 'abcdefgh'" -> Seq(nullFirst("prefix", "prefix = 'abcd'")),
    "eventString > 'abcdefgh'" -> Seq(nullFirst("prefix", "prefix >= 'abcd'")),
    "eventString is null" -> Seq("(prefix IS NULL)"))

  testOptimizablePartitionExpression(
    "IdentityPartitionExpr",
    Seq("region" -> StringType),
    Seq(("regionPart", StringType, "region")),
    "region",
    IdentityPartitionExpr("regionPart"),
    "region < 'us'" -> Seq(nullFirst("regionPart", "regionPart < 'us'")),
    "region = 'us'" -> Seq(nullFirst("regionPart", "regionPart = 'us'")),
    "region > 'us'" -> Seq(nullFirst("regionPart", "regionPart > 'us'")),
    "region is null" -> Seq("(regionPart IS NULL)"))

  testOptimizablePartitionExpression(
    "DatePartitionExpr from a date base column",
    Seq("eventDate" -> DateType),
    Seq(("date", DateType, "CAST(eventDate AS DATE)")),
    "eventDate",
    DatePartitionExpr("date"),
    "eventDate < '2021-01-01'" -> Seq(orNull("date <= DATE '2021-01-01'")),
    "eventDate = '2021-01-01'" -> Seq(orNull("date = DATE '2021-01-01'")),
    "eventDate > '2021-01-01'" -> Seq(orNull("date >= DATE '2021-01-01'")),
    "eventDate is null" -> Seq("(date IS NULL)"))

  testOptimizablePartitionExpression(
    "YearPartitionExpr from a date base column",
    Seq("eventDate" -> DateType),
    Seq(("year", IntegerType, "YEAR(eventDate)")),
    "eventDate",
    YearPartitionExpr("year"),
    "eventDate < '2021-01-01'" -> Seq(orNull("year <= 2021")),
    "eventDate = '2021-01-01'" -> Seq(orNull("year = 2021")),
    "eventDate > '2021-01-01'" -> Seq(orNull("year >= 2021")),
    "eventDate is null" -> Seq("(year IS NULL)"))

  testOptimizablePartitionExpression(
    "DateFormatPartitionExpr from a date base column",
    Seq("eventDate" -> DateType),
    Seq(("month", StringType, "DATE_FORMAT(eventDate, 'yyyy-MM')")),
    "eventDate",
    DateFormatPartitionExpr("month", "yyyy-MM"),
    "eventDate < '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(month, 'yyyy-MM') <= 1622530800L")),
    "eventDate = '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(month, 'yyyy-MM') = 1622530800L")),
    "eventDate > '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(month, 'yyyy-MM') >= 1622530800L")),
    "eventDate is null" -> Seq("(month IS NULL)"))

  testOptimizablePartitionExpression(
    "DateFormatPartitionExpr with yyyy-MM-dd",
    Seq("eventTime" -> TimestampType),
    Seq(("day", StringType, "DATE_FORMAT(eventTime, 'yyyy-MM-dd')")),
    "eventTime",
    DateFormatPartitionExpr("day", "yyyy-MM-dd"),
    "eventTime < '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(day, 'yyyy-MM-dd') <= 1624863600L")),
    "eventTime = '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(day, 'yyyy-MM-dd') = 1624863600L")),
    "eventTime > '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(day, 'yyyy-MM-dd') >= 1624863600L")),
    "eventTime is null" -> Seq("(day IS NULL)"))

  testOptimizablePartitionExpression(
    "DateFormatPartitionExpr with yyyy-MM-dd-HH",
    Seq("eventTime" -> TimestampType),
    Seq(("hour", StringType, "DATE_FORMAT(eventTime, 'yyyy-MM-dd-HH')")),
    "eventTime",
    DateFormatPartitionExpr("hour", "yyyy-MM-dd-HH"),
    "eventTime < '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(hour, 'yyyy-MM-dd-HH') <= 1624928400L")),
    "eventTime = '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(hour, 'yyyy-MM-dd-HH') = 1624928400L")),
    "eventTime > '2021-06-28 18:00:00'" ->
      Seq(orNull("unix_timestamp(hour, 'yyyy-MM-dd-HH') >= 1624928400L")),
    "eventTime is null" -> Seq("(hour IS NULL)"))

  testOptimizablePartitionExpression(
    "TimestampTruncPartitionExpr from a date base column",
    Seq("eventDate" -> DateType),
    Seq(("eventTimeTrunc", TimestampType, "date_trunc('DD', eventDate)")),
    "eventDate",
    TimestampTruncPartitionExpr("DD", "eventTimeTrunc"),
    "eventDate < '2021-01-01'" ->
      Seq(orNull("eventTimeTrunc <= TIMESTAMP '2021-01-01 00:00:00'")),
    "eventDate = '2021-01-01'" ->
      Seq(orNull("eventTimeTrunc = TIMESTAMP '2021-01-01 00:00:00'")),
    "eventDate > '2021-01-01'" ->
      Seq(orNull("eventTimeTrunc >= TIMESTAMP '2021-01-01 00:00:00'")),
    "eventDate is null" -> Seq("(eventTimeTrunc IS NULL)"))

  testOptimizablePartitionExpression(
    "TruncDatePartitionExpr from a string base column",
    Seq("eventDateStr" -> StringType),
    Seq(("date", DateType, "TRUNC(eventDateStr, 'quarter')")),
    "eventDateStr",
    TruncDatePartitionExpr("date", "quarter"),
    "eventDateStr < '2022-04-01'" -> Seq(orNull("date <= DATE '2022-04-01'")),
    "eventDateStr = '2022-04-01'" -> Seq(orNull("date = DATE '2022-04-01'")),
    "eventDateStr > '2022-04-01'" -> Seq(orNull("date >= DATE '2022-04-01'")),
    "eventDateStr is null" -> Seq("(date IS NULL)"))

  testOptimizablePartitionExpression(
    "SubstringPartitionExpr with pos 0",
    Seq("value" -> StringType),
    Seq(("substr", StringType, "SUBSTRING(value, 0, 3)")),
    "value",
    SubstringPartitionExpr("substr", 0, 3),
    "value < 'foo'" -> Seq(nullFirst("substr", "substr <= 'foo'")),
    "value = 'foo'" -> Seq(nullFirst("substr", "substr = 'foo'")),
    "value > 'foo'" -> Seq(nullFirst("substr", "substr >= 'foo'")),
    "value is null" -> Seq("(substr IS NULL)"))

  testOptimizablePartitionExpression(
    "SubstringPartitionExpr with pos 2 supports only equality",
    Seq("value" -> StringType),
    Seq(("substr", StringType, "SUBSTRING(value, 2, 3)")),
    "value",
    SubstringPartitionExpr("substr", 2, 3),
    "value < 'foo'" -> Nil,
    "value = 'foo'" -> Seq(nullFirst("substr", "substr = 'oo'")),
    "value > 'foo'" -> Nil,
    "value is null" -> Seq("(substr IS NULL)"))

  testOptimizablePartitionExpression(
    "SubstringPartitionExpr on a deeply nested base column",
    Seq("outer" -> new StructType()
      .add("inner", new StructType()
        .add("nested", new StructType().add("value", StringType)))),
    Seq(("substr", StringType, "SUBSTRING(outer.inner.nested.value, 1, 3)")),
    "outer.inner.nested.value",
    SubstringPartitionExpr("substr", 1, 3),
    "outer.inner.nested.value < 'foo'" -> Seq(nullFirst("substr", "substr <= 'foo'")),
    "outer.inner.nested.value = 'foo'" -> Seq(nullFirst("substr", "substr = 'foo'")),
    "outer.inner.nested.value > 'foo'" -> Seq(nullFirst("substr", "substr >= 'foo'")),
    "outer.inner.nested.value is null" -> Seq("(substr IS NULL)"))

  testOptimizablePartitionExpression(
    "DatePartitionExpr on a nested timestamp base column",
    Seq("nested" -> new StructType().add("eventTime", TimestampType)),
    Seq(("date", DateType, "CAST(nested.eventTime AS DATE)")),
    "nested.eventTime",
    DatePartitionExpr("date"),
    "nested.eventTime < '2021-01-01 18:00:00'" -> Seq(orNull("date <= DATE '2021-01-01'")),
    "nested.eventTime = '2021-01-01 18:00:00'" -> Seq(orNull("date = DATE '2021-01-01'")),
    "nested.eventTime > '2021-01-01 18:00:00'" -> Seq(orNull("date >= DATE '2021-01-01'")),
    "nested.eventTime is null" -> Seq("(date IS NULL)"))

  testOptimizablePartitionExpression(
    "YearMonthPartitionExpr with differently-cased generation expressions",
    Seq("eventTime" -> TimestampType),
    // Generation expressions may spell the base column with a different case; recognition must
    // still merge YEAR/MONTH into a composite expression under case-insensitive analysis.
    Seq(
      ("year", IntegerType, "YEAR(EVENTTIME)"),
      ("month", IntegerType, "MONTH(eventTime)")),
    "eventTime",
    YearMonthPartitionExpr("year", "month"),
    "eventTime = '2021-06-15 18:30:00'" ->
      Seq("((year = 2021) AND (month = 6))"),
    "eventTime is null" ->
      Seq("((year IS NULL) AND (month IS NULL))"))

  testOptimizablePartitionExpression(
    "TruncDatePartitionExpr from a timestamp base column",
    Seq("eventTime" -> TimestampType),
    Seq(("trunc", DateType, "TRUNC(eventTime, 'YEAR')")),
    "eventTime",
    TruncDatePartitionExpr("trunc", "YEAR"),
    "eventTime < '2021-06-15 18:30:00'" -> Seq(orNull("trunc <= DATE '2021-01-01'")),
    "eventTime = '2021-06-15 18:30:00'" -> Seq(orNull("trunc = DATE '2021-01-01'")),
    "eventTime > '2021-06-15 18:30:00'" -> Seq(orNull("trunc >= DATE '2021-01-01'")),
    "eventTime is null" -> Seq("(trunc IS NULL)"))

  test("month-only and incomplete year/day partitions do not derive filters") {
    // MonthPartitionExpr / DayPartitionExpr / HourPartitionExpr are placeholders used only for
    // merging with Year. Alone, or without the contiguous year->month->day->hour chain, they
    // must not produce partition filters.
    val monthOnly = relationWith(
      Seq("eventTime" -> TimestampType),
      Seq(("month", IntegerType, "MONTH(eventTime)")))
    checkDerivedFilters(
      monthOnly,
      "eventTime = '2021-06-15 18:30:00'" -> Nil,
      "eventTime < '2021-06-15 18:30:00'" -> Nil)

    val yearAndDay = relationWith(
      Seq("eventTime" -> TimestampType),
      Seq(
        ("year", IntegerType, "YEAR(eventTime)"),
        ("day", IntegerType, "DAY(eventTime)")))
    // YEAR alone is still optimizable; DAY alone is not merged without MONTH.
    val recognized = GeneratedColumnPartitionFilters.getOptimizablePartitionExpressions(
      spark,
      fromAttributes(yearAndDay.output),
      GeneratedColumnPartitionFilters.identityPartitionSchema(yearAndDay))
    assert(
      recognized.find(_._1.equalsIgnoreCase("eventTime")).map(_._2)
        .contains(Seq(YearPartitionExpr("year"), DayPartitionExpr("day"))),
      s"expected unmerged year+day expressions, got $recognized")
    checkDerivedFilters(
      yearAndDay,
      // Only YEAR contributes a derived filter; DAY remains a no-op placeholder.
      "eventTime = '2021-06-15 18:30:00'" -> Seq(orNull("year = 2021")),
      "eventTime is null" -> Seq("(year IS NULL)"))
  }

  test("a partition column whose name contains a dot is quoted in the derived filter") {
    val schema = new StructType()
      .add("value", StringType)
      .add("my.substr", StringType, nullable = true, generationMetadata("SUBSTRING(value, 1, 3)"))
    val partitionSchema = new StructType().add(schema("my.substr"))
    val result = GeneratedColumnPartitionFilters.getOptimizablePartitionExpressions(
      spark, schema, partitionSchema)
    assert(result.get("value").contains(Seq(SubstringPartitionExpr("my.substr", 1, 3))),
      s"got $result")
    // The dotted partition column name must be back-tick quoted when rendered in a derived filter.
    val derived = SubstringPartitionExpr("my.substr", 1, 3).equalTo(Literal("foo")).get
    assert(derived.sql.contains("`my.substr`"), s"got ${derived.sql}")
  }

  test("a dotted column name and a nested field resolve to distinct base columns") {
    // The schema has both a top level column literally named `nested.value` and a struct `nested`
    // with a field `value`; escaping must keep the two base column paths distinct.
    val schema = new StructType()
      .add("nested.value", StringType)
      .add("nested", new StructType().add("value", StringType))
      .add("part1", StringType, nullable = true, generationMetadata("`nested.value`"))
      .add("part2", StringType, nullable = true, generationMetadata("nested.value"))
    val partitionSchema = new StructType().add(schema("part1")).add(schema("part2"))
    val result = GeneratedColumnPartitionFilters.getOptimizablePartitionExpressions(
      spark, schema, partitionSchema)
    assert(result.get("`nested.value`").contains(Seq(IdentityPartitionExpr("part1"))),
      s"got $result")
    assert(result.get("nested.value").contains(Seq(IdentityPartitionExpr("part2"))),
      s"got $result")
  }
}
