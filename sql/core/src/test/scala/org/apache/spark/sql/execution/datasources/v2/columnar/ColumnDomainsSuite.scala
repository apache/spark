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
package org.apache.spark.sql.execution.datasources.v2.columnar

import java.sql.Date

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Tests how [[ColumnDomains]] derives the domains of columns from the predicates of a query. */
class ColumnDomainsSuite extends QueryTest with SharedSparkSession with PredicateHelper {

  private val schema = StructType.fromDDL("id BIGINT, i INT, d DOUBLE, f FLOAT, s STRING, " +
    "b BINARY, flag BOOLEAN, dt DATE, ts TIMESTAMP, dec DECIMAL(10, 2), " +
    "st STRUCT<x: INT, y: STRING>, arr ARRAY<INT>, c STRING COLLATE UTF8_LCASE")

  private def char(codePoint: Int): String = new String(Character.toChars(codePoint))

  /**
   * The predicates that Data Source V2 pushes for the filter, as in the analyzed plan with the
   * constants folded: the optimizer would rewrite some of them, such as NOT (x > 1).
   */
  private def predicates(filter: String, schema: StructType): Seq[Predicate] = {
    val df = spark.createDataFrame(Seq.empty[Row].asJava, schema)
    val condition = df.where(filter).queryExecution.analyzed.collectFirst {
      case Filter(condition, _) => condition
    }.get
    splitConjunctivePredicates(condition.transformUp {
      case e if e.foldable && !e.isInstanceOf[Literal] => Literal.create(e.eval(), e.dataType)
    }).flatMap(DataSourceV2Strategy.translateFilterV2)
  }

  private def domains(filter: String): Seq[String] = {
    ColumnDomains.derive(predicates(filter, schema), schema).domains.map(_.toString)
  }

  test("comparisons, IN and null checks") {
    assert(domains("id > 5") == Seq("id: (5L, +inf)"))
    assert(domains("5 < id") == Seq("id: (5L, +inf)"))
    assert(domains("id >= 5 AND id < 10") == Seq("id: [5L, 10L)"))
    assert(domains("id <= 5") == Seq("id: (-inf, 5L]"))
    assert(domains("id = 5") == Seq("id: 5L"))
    assert(domains("id <> 5") == Seq("id: (-inf, 5L), (5L, +inf)"))
    assert(domains("id <=> 5") == Seq("id: 5L"))
    // x <=> v is true for null when false, and so is NOT (x <=> v).
    assert(domains("NOT (id <=> 5)") == Seq("id: NULL, (-inf, 5L), (5L, +inf)"))
    assert(domains("id <=> NULL") == Seq("id: NULL"))
    assert(domains("NOT (id <=> NULL)") == Seq("id: ANY"))
    assert(domains("id IS NULL") == Seq("id: NULL"))
    assert(domains("id IS NOT NULL") == Seq("id: ANY"))
    assert(domains("id IN (3, 1, 2, 3)") == Seq("id: 1L, 2L, 3L"))
    assert(domains("id NOT IN (1, 3)") == Seq("id: (-inf, 1L), (1L, 3L), (3L, +inf)"))
    // Comparisons with null are null: so is NOT (x IN (..., NULL)), which matches no row.
    assert(domains("id = NULL") == Seq("id: NONE"))
    assert(domains("NOT (id = NULL)") == Seq("id: NONE"))
    assert(domains("id IN (1, NULL)") == Seq("id: 1L"))
    assert(domains("NOT (id IN (1, NULL))") == Seq("id: NONE"))
    assert(domains("id = 1 AND id = 2") == Seq("id: NONE"))
    // The domains of all the predicates, which are combined with AND, at most one per column.
    assert(domains("id > 5 AND i = 1 AND id < 8") == Seq("id: (5L, 8L)", "i: 1"))
    assert(domains("true").isEmpty)
    assert(ColumnDomains.derive(predicates("false", schema), schema).domains.isEmpty)
  }

  test("AND, OR and NOT") {
    assert(domains("id > 5 OR id < 0") == Seq("id: (-inf, 0L), (5L, +inf)"))
    assert(domains("id > 5 OR id > 3") == Seq("id: (3L, +inf)"))
    assert(domains("id >= 3 OR id = 3 OR id < 1") == Seq("id: (-inf, 1L), [3L, +inf)"))
    // [1, 2) and [2, 3] touch, and so do (-inf, 5) and [5, +inf), which then constrain id to not
    // be null only.
    assert(domains("(id >= 1 AND id < 2) OR (id >= 2 AND id <= 3)") == Seq("id: [1L, 3L]"))
    assert(domains("id < 5 OR id >= 5") == Seq("id: ANY"))
    assert(domains("id < 5 OR id >= 5 OR id IS NULL").isEmpty)
    // A column that only one side of OR constrains can have any value in the rows of the other.
    assert(domains("id > 5 OR i > 5").isEmpty)
    assert(domains("id > 5 AND (i > 1 OR id < 3)") == Seq("id: (5L, +inf)"))
    assert(domains("(id > 5 AND i = 1) OR (id < 0 AND i = 2)") ==
      Seq("id: (-inf, 0L), (5L, +inf)", "i: 1, 2"))
    // A side that matches no row does not widen the other.
    assert(domains("id = NULL OR i > 5") == Seq("i: (5, +inf)"))
    assert(domains("NOT (id > 5 OR i IS NULL)") == Seq("id: (-inf, 5L]", "i: ANY"))
    assert(domains("NOT (id > 5 AND i < 2)").isEmpty)
    assert(domains("NOT NOT (id > 5)") == Seq("id: (5L, +inf)"))
    // Predicates on other expressions constrain no column.
    assert(domains("id + 1 > 5").isEmpty)
    assert(domains("id = i").isEmpty)
    assert(domains("id + 1 > 5 AND id < 3") == Seq("id: (-inf, 3L)"))
    assert(domains("NOT (id + 1 > 5) AND id < 3") == Seq("id: (-inf, 3L)"))
  }

  test("NaN and -0.0") {
    // NaN is equal to NaN and greater than any other value, including positive infinity.
    assert(domains("d > 1.0") == Seq("d: NaN, (1.0D, +inf)"))
    assert(domains("d < 1.0") == Seq("d: (-inf, 1.0D)"))
    assert(domains("d <> 1.0") == Seq("d: NaN, (-inf, 1.0D), (1.0D, +inf)"))
    assert(domains("NOT (d < 1.0)") == Seq("d: NaN, [1.0D, +inf)"))
    assert(domains("d > CAST('Infinity' AS DOUBLE)") ==
      Seq("d: NaN, (CAST('Infinity' AS DOUBLE), +inf)"))
    assert(domains("d = CAST('NaN' AS DOUBLE)") == Seq("d: NaN"))
    assert(domains("d <> CAST('NaN' AS DOUBLE)") == Seq("d: ANY"))
    assert(domains("d > CAST('NaN' AS DOUBLE)") == Seq("d: NONE"))
    assert(domains("d >= CAST('NaN' AS DOUBLE)") == Seq("d: NaN"))
    assert(domains("d < CAST('NaN' AS DOUBLE)") == Seq("d: ANY"))
    assert(domains("d <= CAST('NaN' AS DOUBLE)") == Seq("d: NaN, ANY"))
    assert(domains("d IN (1.0, CAST('NaN' AS DOUBLE))") == Seq("d: NaN, 1.0D"))
    assert(domains("d NOT IN (1.0)") == Seq("d: NaN, (-inf, 1.0D), (1.0D, +inf)"))
    assert(domains("d IS NOT NULL") == Seq("d: NaN, ANY"))
    // -0.0 is equal to 0.0, which a data source may order after -0.0.
    assert(domains("d = -0.0") == Seq("d: 0.0D"))
    assert(domains("f > CAST(-0.0 AS FLOAT)") == Seq("f: NaN, (CAST('0.0' AS FLOAT), +inf)"))
    assert(domains("f = CAST('NaN' AS FLOAT)") == Seq("f: NaN"))
  }

  test("strings and binary values") {
    assert(domains("s > 'a'") == Seq("s: ('a', +inf)"))
    assert(domains("s IN ('b', 'a')") == Seq("s: 'a', 'b'"))
    assert(domains("startswith(s, 'ab')") == Seq("s: ['ab', 'ac')"))
    assert(domains("startswith(s, '')") == Seq("s: ANY"))
    // The strings that do not start with a prefix are not a range.
    assert(domains("NOT startswith(s, 'ab')") == Seq("s: ANY"))
    assert(domains("endswith(s, 'x') OR contains(s, 'x')").isEmpty)
    // A data source compares strings by their bytes: not those of a non-binary collation.
    assert(domains("c = 'a'").isEmpty)
    assert(domains("c IS NULL").isEmpty)
    // A string that is not valid UTF-8 cannot be a bound.
    assert(domains("s = CAST(X'FF' AS STRING)").isEmpty)
    assert(domains("b > X'7F'") == Seq("b: (X'7F', +inf)"))
    assert(domains("b IN (X'', X'0102')") == Seq("b: X'', X'0102'"))
  }

  test("the next prefix of the strings that start with a prefix") {
    def next(prefix: String): Option[String] =
      ColumnDomains.nextPrefix(UTF8String.fromString(prefix)).map(_.toString)
    assert(next("ab").contains("ac"))
    assert(next("a" + char(0x7F)).contains("a" + char(0x80)))
    // After the last code point before the surrogates comes the first one after them.
    assert(next("a" + char(0xD7FF)).contains("a" + char(0xE000)))
    assert(next("a" + char(0xFFFF)).contains("a" + char(0x10000)))
    // U+10FFFF is the largest code point.
    assert(next("a" + char(0x10FFFF) + char(0x10FFFF)).contains("b"))
    assert(next(char(0x10FFFF)).isEmpty)
    assert(domains(s"startswith(s, '${char(0x10FFFF)}')") == Seq(s"s: ['${char(0x10FFFF)}', +inf)"))
  }

  test("other types, and fields of structs") {
    assert(domains("flag") == Seq("flag: true"))
    assert(domains("dt >= DATE '2020-01-01'") == Seq("dt: [DATE '2020-01-01', +inf)"))
    assert(domains("ts < TIMESTAMP '2020-01-01 00:00:00'") ==
      Seq("ts: (-inf, TIMESTAMP '2020-01-01 00:00:00')"))
    assert(domains("dec = 1.5") == Seq("dec: 1.50BD"))
    assert(domains("st.x > 1 AND st.y IS NULL") == Seq("st.x: (1, +inf)", "st.y: NULL"))
    assert(ColumnDomains.derive(predicates("st.x > 1", schema), schema).domains.map(_.path) ==
      Seq(Seq(10, 0)))
    // Arrays, maps and structs have other rules, such as nulls equal to each other inside them.
    assert(domains("arr IS NULL").isEmpty)
    assert(domains("st IS NOT NULL").isEmpty)
    assert(domains("size(arr) > 1").isEmpty)
  }

  test("the pushed predicates are those whose domains are used") {
    val pushed = predicates("id > 5", schema) ++ predicates("i < 3", schema) ++
      predicates("id + 1 > 0", schema) ++ predicates("id < 9 OR i = 1", schema)
    val derived = ColumnDomains.derive(pushed, schema)
    assert(derived.domains.map(_.toString) == Seq("id: (5L, +inf)", "i: (-inf, 3)"))
    assert(derived.pushedPredicates(pushed.toArray, Seq(true, false)).toSeq == pushed.take(1))
    assert(derived.pushedPredicates(pushed.toArray, Seq(true, true)).toSeq == pushed.take(2))
  }

  /**
   * Checks that the domains have the values of all the rows for which Spark finds the filters
   * true: these filters, and random combinations of comparisons with AND, OR and NOT.
   */
  test("the domains have the values of the matching rows") {
    val testSchema = StructType.fromDDL("id INT, i INT, d DOUBLE, s STRING, f FLOAT, b BINARY, " +
      "dec DECIMAL(5, 2), dt DATE")
    val (emoji, eAcute, replacement, max) = (char(0x1F600), char(0xE9), char(0xFFFD),
      char(0x10FFFF))
    def bytes(values: Int*): Array[Byte] = values.map(_.toByte).toArray
    def decimal(value: String): java.math.BigDecimal = new java.math.BigDecimal(value)
    val rows = Seq(
      Row(0, 1, 0.0, "a", 0.1f, bytes(0x00), decimal("1.00"), Date.valueOf("2020-01-01")),
      Row(1, 2, -0.0, "", 1.5f, bytes(0x7F), decimal("-1.50"), Date.valueOf("1970-01-01")),
      Row(2, null, Double.NaN, eAcute, null, bytes(0x80), null, null),
      Row(3, 3, Double.PositiveInfinity, emoji, 0.1f, bytes(0xFF), decimal("999.99"),
        Date.valueOf("2020-01-02")),
      Row(4, 1, Double.NegativeInfinity, null, 0.5f, bytes(0x7F, 0x00), decimal("0.00"),
        Date.valueOf("1969-12-31")),
      Row(5, null, 1.5, "ab", 2.0f, bytes(), decimal("-999.99"), Date.valueOf("2020-01-01")),
      Row(6, 2, null, "b" + eAcute, Float.NaN, null, decimal("1.01"), Date.valueOf("2000-02-29")),
      Row(7, 4, 2.5, replacement, 3.0f, bytes(0x01, 0x02), decimal("2.50"),
        Date.valueOf("2020-01-01")),
      Row(8, Int.MinValue, -1.0, "a" + max, -0.0f, bytes(0x61), decimal("0.01"),
        Date.valueOf("2021-06-15")),
      Row(9, Int.MaxValue, 0.5, "abc", Float.PositiveInfinity, bytes(0x61, 0x62),
        decimal("10.00"), Date.valueOf("1999-12-31")))

    val fixed = Seq(
      "d = 0.0D", "d = -0.0D", "d < 0.0D", "d > 1.0D", "d >= 2.5D", "d <> 0.0D", "d <=> 0.0D",
      "d IN (0.0D, 1.5D)", "NOT (d > 1.0D)", "NOT (d < 1.0D)", "d IS NULL", "d IS NOT NULL",
      "d > CAST('Infinity' AS DOUBLE)", "d >= CAST('NaN' AS DOUBLE)", "f = CAST(0.1 AS FLOAT)",
      "f > CAST(0.1 AS FLOAT)", "f = CAST(-0.0 AS FLOAT)", "f < CAST('NaN' AS FLOAT)",
      "i = 1", "i = NULL", "i <=> NULL", "NOT (i <=> NULL)", "i IN (1, NULL)",
      "NOT (i IN (1, NULL))", "NOT (i IN (1, 3))", "i IS NULL OR d > 1.0D",
      "i > 1 AND d IS NOT NULL", "i < -2147483647 OR i > 2147483646",
      s"s = '$emoji'", s"s > '$replacement'", "s < 'b'", "startswith(s, 'a')",
      s"startswith(s, 'a$max')", "NOT startswith(s, 'ab')", s"s IN ('', '$eAcute')",
      "b > X'7F'", "b < X'80'", "b IN (X'', X'7F00')", "dec > 0", "dec <= -1.5",
      "dt < DATE '2000-01-01'", "dt IN (DATE '2020-01-01', NULL)", "(id > 3) = (id < 3)")

    // Random combinations of comparisons, with the values of the rows and edge values.
    val random = new Random(42)
    def pick[T](values: Seq[T]): T = values(random.nextInt(values.length))
    def sqlString(value: String): String = {
      "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    }
    val literals = Map(
      "id" -> Seq("-1", "0", "3", "5", "9", "10", "NULL"),
      "i" -> Seq("-1", "0", "1", "2", "3", "NULL", "2147483647", "-2147483648"),
      "d" -> Seq("0.0D", "-0.0D", "1.0D", "1.5D", "-1.0D", "CAST('NaN' AS DOUBLE)",
        "CAST('Infinity' AS DOUBLE)", "CAST('-Infinity' AS DOUBLE)", "NULL"),
      "f" -> Seq("CAST(0.1 AS FLOAT)", "CAST(-0.0 AS FLOAT)", "CAST(1.5 AS FLOAT)",
        "CAST('NaN' AS FLOAT)", "CAST('Infinity' AS FLOAT)", "NULL"),
      "s" -> (Seq("", "a", "ab", "abc", "b", eAcute, emoji, replacement, "a" + max)
        .map(sqlString) :+ "NULL"),
      "b" -> Seq("X''", "X'00'", "X'7F'", "X'80'", "X'FF'", "X'7F00'", "X'61'", "NULL"),
      "dec" -> Seq("CAST(0 AS DECIMAL(5, 2))", "CAST(1.5 AS DECIMAL(5, 2))",
        "CAST(-999.99 AS DECIMAL(5, 2))", "CAST(1.01 AS DECIMAL(5, 2))", "NULL"),
      "dt" -> Seq("DATE '2020-01-01'", "DATE '1970-01-01'", "DATE '2021-06-15'", "NULL"))
    val ops = Seq("=", "<>", "<=>", "<", "<=", ">", ">=")
    def leaf(): String = {
      val column = pick(literals.keys.toSeq.sorted)
      def value(): String = pick(literals(column))
      random.nextInt(8) match {
        case 0 => s"$column IS NULL"
        case 1 => s"$column IS NOT NULL"
        case 2 => s"$column IN (${Seq.fill(1 + random.nextInt(3))(value()).mkString(", ")})"
        case 3 if column == "s" => s"startswith(s, ${pick(literals("s").init)})"
        case 4 if column == "s" => s"endswith(s, ${pick(literals("s").init)})"
        case 5 => s"${value()} ${pick(ops)} $column"
        case _ => s"$column ${pick(ops)} ${value()}"
      }
    }
    def filter(depth: Int): String = {
      if (depth == 0 || random.nextInt(3) == 0) {
        leaf()
      } else {
        random.nextInt(3) match {
          case 0 => s"(${filter(depth - 1)}) AND (${filter(depth - 1)})"
          case 1 => s"(${filter(depth - 1)}) OR (${filter(depth - 1)})"
          case _ => s"NOT (${filter(depth - 1)})"
        }
      }
    }
    val filters = fixed ++ Seq.fill(400)(filter(3))

    // Evaluates all the filters in one job, with the semantics of Spark SQL.
    val df = spark.createDataFrame(rows.asJava, testSchema)
    val results = df.select(filters.zipWithIndex.map { case (f, i) => expr(f).as(s"f$i") }: _*)
      .collect()
    val converters = testSchema.fields.map(f => CatalystTypeConverters.createToCatalystConverter(
      f.dataType))
    var constrained = 0
    filters.zipWithIndex.foreach { case (filter, i) =>
      val domains = ColumnDomains.derive(predicates(filter, testSchema), testSchema).domains
      if (domains.nonEmpty) constrained += 1
      rows.zip(results).foreach { case (row, result) =>
        if (!result.isNullAt(i) && result.getBoolean(i)) {
          domains.foreach { domain =>
            val column = domain.path.head
            val value = converters(column)(row.get(column))
            assert(domain.contains(value),
              s"The row $row matches $filter, but $domain does not have its value.")
          }
        }
      }
    }
    // Most filters constrain a column.
    assert(constrained > filters.length / 2, s"$constrained of ${filters.length} filters")
  }
}
