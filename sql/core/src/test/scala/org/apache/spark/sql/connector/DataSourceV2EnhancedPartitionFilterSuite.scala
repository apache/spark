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

package org.apache.spark.sql.connector

import java.util.Locale

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{Expression, In, PredicateHelper, ScalaUDF}
import org.apache.spark.sql.connector.catalog.BufferedRows
import org.apache.spark.sql.connector.catalog.InMemoryEnhancedPartitionFilterTable
import org.apache.spark.sql.connector.catalog.InMemoryTableEnhancedPartitionFilterCatalog
import org.apache.spark.sql.connector.expressions.PartitionFieldReference
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for enhanced partition filter pushdown with tables whose scan builder handles
 * PartitionPredicates in a second pass of partition filter pushdown, for those
 * Catalyst Expression filters that are not translatable to DSV2, or are returned by DSV2
 * in the first pushdown.
 *
 * Pushdown cases (Translated/Untranslatable, Partition Filter/Data Filter, 1st/2nd Pass):
 * 1. Translated, Data Filter, 1st Pass Returned -> Post-Scan Filters
 * 2. Translated, Data Filter, 1st Pass Accepted -> Pushed Down
 * 3. Translated, Partition Filter, 1st Pass Returned, 2nd Pass Returned -> Post-Scan Filters
 * 4. Translated, Partition Filter, 1st Pass Returned, 2nd Pass Accepted -> Pushed Down
 * 5. Translated, Partition Filter, 1st Pass Accepted -> Pushed Down
 * 6. Untranslatable, Data Filter -> Post-Scan Filters
 * 7. Untranslatable, Partition Filter, 2nd Pass Returned -> Post-Scan Filters
 * 8. Untranslatable, Partition Filter, 2nd Pass Accepted -> Pushed Down
 */
class DataSourceV2EnhancedPartitionFilterSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter with PredicateHelper {

  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  protected val partFilterTableName = "testpartfilter.t"

  protected def registerCatalog(name: String, clazz: Class[_]): Unit = {
    spark.conf.set(s"spark.sql.catalog.$name", clazz.getName)
  }

  before {
    registerCatalog("testpartfilter", classOf[InMemoryTableEnhancedPartitionFilterCatalog])
  }

  after {
    spark.sessionState.catalogManager.reset()
  }

  test("case 1: translated data filter returned in first pass is in post-scan") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'keep'), ('a', 'drop'), ('b', 'other')")

      // Translated, Data Filter; 1st Pass Returned -> Post-Scan Filters.
      // Returned because it is a data filter (on data column), not a partition filter.
      // No filter pushdown
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE data = 'keep'")
      checkAnswer(df, Seq(Row("a", "keep")))
      assertPushedPartitionPredicates(df, 0)
      assertScanReturnsPartitionKeys(df, Set("a", "b"))
    }
  }

  test("case 2: translated data filter accepted in first pass is pushed down") {
    // Translated, Data Filter; 1st Pass Accepted -> Pushed Down (not in post-scan).
    withTable(partFilterTableName) {
      // We mock a data source that can evaluate and reject
      // this data predicate (accept-data-predicates);
      // the test uses data IS NOT NULL, which always evaluates to true here.
      // No partition filter pushdown
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) TBLPROPERTIES('accept-data-predicates' = 'true')")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE data IS NOT NULL")
      checkAnswer(df, Seq(Row("a", "x"), Row("b", "y"), Row("c", "z")))
      assertPushedPartitionPredicates(df, 0)
      assertScanReturnsPartitionKeys(df, Set("a", "b", "c"))
      assert(!df.queryExecution.executedPlan.exists(_.isInstanceOf[FilterExec]),
        "Data filter accepted in first pass should not appear as post-scan Filter")
    }
  }

  test("case 3: filter returned in both first and second pass") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) " +
        "TBLPROPERTIES('accept-partition-predicates' = 'false')")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // Translated, Partition Filter; 1st Pass Returned, 2nd Pass Returned
      // No partition filter pushdown
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col IN ('a')")
      checkAnswer(df, Seq(Row("a", "x")))
      assertPushedPartitionPredicates(df, 0)
      assertScanReturnsPartitionKeys(df, Set("a", "b", "c"))
    }
  }

  test("case 4: first pass partition predicate returned by source applied in second pass") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // Translated, Partition Filter; 1st Pass Returned, 2nd Pass Accepted
      // Partition Filter pushdown
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col IN ('a', 'b')")
      checkAnswer(df, Seq(Row("a", "x"), Row("b", "y")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "b"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part_col"))
    }
  }

  test("case 5: first pass partition filter still works") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // Translated, Partition Filter; 1st Pass Accepted
      // Partition Filter pushdown
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col = 'b'")
      checkAnswer(df, Seq(Row("b", "y")))
      assertPushedPartitionPredicates(df, 0)
      assertScanReturnsPartitionKeys(df, Set("b"))
    }
  }

  test("case 6: untranslatable data filters are applied after scan") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'keep'), ('a', 'drop'), ('b', 'keep'), ('b', 'other')")

      // Untranslatable, Data Filter
      // No filter pushdown
      spark.udf.register("is_keep", (s: String) => s != null && s == "keep")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE is_keep(data)")
      checkAnswer(df, Seq(Row("a", "keep"), Row("b", "keep")))
      assertPushedPartitionPredicates(df, 0)
      assertScanReturnsPartitionKeys(df, Set("a", "b"))
    }
  }

  test("case 7: untranslatable partition filter returned in second pass is in post-scan") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) " +
        "TBLPROPERTIES('accept-partition-predicates' = 'false')")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('bc', 'z')")

      // Untranslatable, Partition Filter; 2nd Pass Returned
      // No partition filter pushdown
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col LIKE 'b%'")
      checkAnswer(df, Seq(Row("b", "y"), Row("bc", "z")))
      assertPushedPartitionPredicates(df, 0)
      assertScanReturnsPartitionKeys(df, Set("a", "b", "bc"))
    }
  }

  test("case 8: untranslatable partition-only expression handled by second pass") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('bc', 'z')")

      // Untranslatable, Partition Filter; 2nd Pass Accepted
      // Partition Filter push down
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col LIKE 'b%'")
      checkAnswer(df, Seq(Row("b", "y"), Row("bc", "z")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("b", "bc"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part_col"))
    }
  }

  test("case 8: Second-pass PartitionPredicate filter works for UDF filter on partition field") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('A', 'y'), ('b', 'z')")

      spark.udf.register("my_upper", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      // Untranslatable, Partition Filter; 2nd Pass Accepted
      // Partition Filter push down
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE my_upper(part_col) = 'A'")
      checkAnswer(df, Seq(Row("a", "x"), Row("A", "y")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "A"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part_col"))
    }
  }

  test("nested identity partition: second-pass PartitionPredicate with UDF on nested key") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName " +
        s"(s struct<tz: string, x: int>, data string) USING $v2Source PARTITIONED BY (s.tz)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "(named_struct('tz', 'LA', 'x', 1), 'a'), (named_struct('tz', 'NY', 'x', 2), 'b')")

      spark.udf.register("my_upper_nested", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql(
        s"SELECT * FROM $partFilterTableName WHERE my_upper_nested(s.tz) = 'LA'")
      checkAnswer(df, Seq(Row(Row("LA", 1), "a")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("LA"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("s.tz"))
    }
  }

  test("nested identity partition: second-pass rejection returns filter to post-scan") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName " +
        s"(s struct<tz: string, x: int>, data string) USING $v2Source " +
        "PARTITIONED BY (s.tz) " +
        "TBLPROPERTIES('accept-partition-predicates' = 'false')")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "(named_struct('tz', 'LA', 'x', 1), 'a'), " +
        "(named_struct('tz', 'NY', 'x', 2), 'b')")

      val df = sql(
        s"SELECT * FROM $partFilterTableName WHERE s.tz = 'LA'")
      checkAnswer(df, Seq(Row(Row("LA", 1), "a")))
      assertPushedPartitionPredicates(df, 0)
      assertScanReturnsPartitionKeys(df, Set("LA", "NY"))
    }
  }

  test("nested identity partition: field name containing a dot") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName " +
        s"(s struct<`a.b`: string, x: int>, data string) USING $v2Source " +
        "PARTITIONED BY (s.`a.b`)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "(named_struct('a.b', 'LA', 'x', 1), 'v1'), " +
        "(named_struct('a.b', 'NY', 'x', 2), 'v2')")

      spark.udf.register("my_upper_dot", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql(
        s"SELECT * FROM $partFilterTableName WHERE my_upper_dot(s.`a.b`) = 'LA'")
      checkAnswer(df, Seq(Row(Row("LA", 1), "v1")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("LA"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("s.a.b"))
    }
  }

  test("partition filter with data array col filter") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName " +
        s"(part_col string, arr array<string>, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', array('x','y'), 'd1'), ('b', array('z'), 'd2')")

      val df = sql(
        s"SELECT * FROM $partFilterTableName " +
        "WHERE part_col LIKE 'a%' AND size(arr) > 1")
      checkAnswer(df, Seq(Row("a", Seq("x", "y"), "d1")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a"))
    }
  }

  test("referenced partition field ordinals: partition predicate same field twice " +
    "has de-duped ordinals") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('bc', 'z')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col LIKE 'b%' " +
        "OR part_col = 'a'")
      checkAnswer(df, Seq(Row("a", "x"), Row("b", "y"), Row("bc", "z")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "b", "bc"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part_col"))
    }
  }

  test("referenced partition field ordinals: one non-first partition field in second-pass") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (p0 string, p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', '1', 'd1'), ('a', 'y', '1', 'd2'), " +
        "('a', 'x', '2', 'd3'), ('b', 'x', '1', 'd4')")

      // Untranslatable, Partition Filter; 2nd Pass Accepted
      // Partition filter push down
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE p1 LIKE 'x%'")
      checkAnswer(df, Seq(
        Row("a", "x", "1", "d1"), Row("a", "x", "2", "d3"), Row("b", "x", "1", "d4")))
      assertPushedPartitionPredicates(df, 1)
      assertReferencedPartitionFieldOrdinals(df, Array(1), Array("p0", "p1", "p2"))
    }
  }

  test("referenced partition field ordinals: two non-first partition fields in second-pass") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (p0 string, p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', '1', 'd1'), ('a', 'y', '1', 'd2'), " +
        "('a', 'x', '2', 'd3'), ('b', 'x', '1', 'd4')")

      spark.udf.register("concat2", (a: String, b: String) =>
        if (a == null || b == null) null else a + b)

      // Untranslatable, Partition Filter; 2nd Pass Accepted
      // Partition filter pushdown
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE concat2(p1, p2) = 'x1'")
      checkAnswer(df, Seq(Row("a", "x", "1", "d1"), Row("b", "x", "1", "d4")))
      assertPushedPartitionPredicates(df, 1)
      assertReferencedPartitionFieldOrdinals(df, Array(1, 2), Array("p0", "p1", "p2"))
    }
  }

  test("non-deterministic partition filter not pushed as PartitionPredicate") {
    // Same checks as FileSourceStrategy/PruneFileSourcePartitions: non-deterministic
    // partition filters must not be pushed as PartitionPredicate; they are applied after scan.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      spark.udf.register("nondet_identity", udf((s: String) => s).asNondeterministic())

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE nondet_identity(part_col) = 'a'")
      checkAnswer(df, Seq(Row("a", "x")))
      assertPushedPartitionPredicates(df, 0)
    }
  }

  test("partition filter with subquery is not pushed as PartitionPredicate") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      withView("subq") {
        sql("CREATE TEMP VIEW subq AS SELECT 'a' AS c")
        val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col IN (SELECT c FROM subq)")
        checkAnswer(df, Seq(Row("a", "x")))
        assertPushedPartitionPredicates(df, 0)
      }
    }
  }

   test("all eight pushdown cases: translatable filters before untranslatable " +
     "in post-scan Filter") {
    // Cases 1, 3, 6, 7 end in post-scan (accept-partition-predicates=false).
    // Post-scan filters are ordered with translatable first.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) TBLPROPERTIES('accept-partition-predicates' = 'false')")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'keep'), ('a', 'drop'), ('A', 'keep'), ('b', 'keep'), ('b', 'other')")

      spark.udf.register("is_keep", (s: String) => s != null && s == "keep")
      spark.udf.register("my_upper", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "part_col IN ('a', 'A') AND data = 'keep' AND is_keep(data) AND my_upper(part_col) = 'A'")
      checkAnswer(df, Seq(Row("a", "keep"), Row("A", "keep")))
      assertScanReturnsPartitionKeys(df, Set("a", "A", "b"))
      assertTranslatableBeforeUntranslatableInPostScan(df)

      // Reversed filter order in the query; post-scan order still translatable first.
      val dfReversed = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "my_upper(part_col) = 'A' AND is_keep(data) AND data = 'keep' AND part_col IN ('a', 'A')")
      checkAnswer(dfReversed, Seq(Row("a", "keep"), Row("A", "keep")))
      assertScanReturnsPartitionKeys(dfReversed, Set("a", "A", "b"))
      assertTranslatableBeforeUntranslatableInPostScan(dfReversed)
    }
  }

  test("extract partition filter from translated OR with mixed partition and data references") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x'), ('a', 'other'), ('b', 'y'), ('c', 'z')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "(part_col = 'a' AND data = 'x') OR (part_col = 'b' AND data = 'y')")
      checkAnswer(df, Seq(Row("a", "x"), Row("b", "y")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "b"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part_col"))
    }
  }

  test("extract partition filter from untranslatable OR with mixed partition and data references") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x'), ('b', 'y'), ('c', 'z')")

      spark.udf.register("my_upper_extract", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "(my_upper_extract(part_col) = 'A' AND data = 'x') OR " +
        "(my_upper_extract(part_col) = 'B' AND data = 'y')")
      checkAnswer(df, Seq(Row("a", "x"), Row("b", "y")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "b"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part_col"))
    }
  }

  test("extract partition filter from OR with one partition-only and one mixed filter") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x'), ('a', 'other'), ('b', 'y'), ('b', 'other'), ('c', 'z')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "part_col = 'a' OR (part_col = 'b' AND data = 'y')")
      checkAnswer(df, Seq(Row("a", "x"), Row("a", "other"), Row("b", "y")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "b"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part_col"))
    }
  }

  test("extract multi-column partition filter from OR with mixed partition and data references") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', 'd1'), ('a', 'y', 'd2'), ('b', 'x', 'd3'), ('b', 'y', 'd4')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "(p1 = 'a' AND p2 = 'x' AND data = 'd1') OR (p1 = 'b' AND p2 = 'y' AND data = 'd4')")
      checkAnswer(df, Seq(Row("a", "x", "d1"), Row("b", "y", "d4")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a/x", "b/y"))
      assertReferencedPartitionFieldOrdinals(df, Array(0, 1), Array("p1", "p2"))
    }
  }

  test("two partition predicates pushed: UDF on p1 and " +
    "extracted filter on p2 from mixed data and partition references") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', 'd1'), " +
        "('a', 'y', 'd4'), " +
        "('b', 'x', 'd3'), " +
        "('b', 'y', 'd4'), " +
        "('c', 'z', 'd5')")

      spark.udf.register("my_upper_multi", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      // my_upper_multi(p1) = 'A' is untranslatable and partition-only, so it is a partition filter.
      // The OR mixes p2 and data; we infer (p2 = 'x' OR p2 = 'y') as a partition filter.
      // Both are pushed as separate PartitionPredicates.
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "my_upper_multi(p1) = 'A' AND " +
        "((p2 = 'x' AND data = 'd1') OR (p2 = 'y' AND data = 'd4'))")
      checkAnswer(df, Seq(Row("a", "x", "d1"), Row("a", "y", "d4")))
      assertPushedPartitionPredicates(df, 2)
      assertScanReturnsPartitionKeys(df, Set("a/x", "a/y"))
    }
  }

  test("nested partition: extract partition filter from " +
    "OR with mixed data and partition references") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName " +
        s"(s struct<tz: string, x: int>, data string) USING $v2Source " +
        "PARTITIONED BY (s.tz)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "(named_struct('tz', 'LA', 'x', 1), 'a'), " +
        "(named_struct('tz', 'NY', 'x', 2), 'b'), " +
        "(named_struct('tz', 'SF', 'x', 3), 'c')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "(s.tz = 'LA' AND data = 'a') OR (s.tz = 'NY' AND data = 'b')")
      checkAnswer(df, Seq(Row(Row("LA", 1), "a"), Row(Row("NY", 2), "b")))
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("LA", "NY"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("s.tz"))
    }
  }

  test("nested partition: two partition predicates from " +
    "UDF and extracted mixed data and partition references") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName " +
        s"(s struct<tz: string, x: int>, data string) USING $v2Source " +
        "PARTITIONED BY (s.tz)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "(named_struct('tz', 'LA', 'x', 1), 'a'), " +
        "(named_struct('tz', 'la', 'x', 2), 'b'), " +
        "(named_struct('tz', 'NY', 'x', 3), 'c'), " +
        "(named_struct('tz', 'SF', 'x', 4), 'd')")

      spark.udf.register("my_upper_nested2", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      // my_upper_nested2(s.tz) = 'LA' is untranslatable and partition-only,
      // it is a partition filter.
      // The OR mixes s.tz and data; we infer (s.tz = 'LA' OR s.tz = 'la') as an partition filter.
      // Both are pushed as separate PartitionPredicates.
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "my_upper_nested2(s.tz) = 'LA' AND " +
        "((s.tz = 'LA' AND data = 'a') OR (s.tz = 'la' AND data = 'b'))")
      checkAnswer(df, Seq(Row(Row("LA", 1), "a"), Row(Row("la", 2), "b")))
      assertPushedPartitionPredicates(df, 2)
      assertScanReturnsPartitionKeys(df, Set("LA", "la"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("s.tz"))
    }
  }

  private def assertTranslatableBeforeUntranslatableInPostScan(df: DataFrame): Unit = {
    val postScanFilterExec = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(_, _) if f.exists(_.isInstanceOf[BatchScanExec]) => f
    }.headOption.getOrElse(fail("Expected a post-scan FilterExec above BatchScanExec"))

    val predicates = splitConjunctivePredicates(postScanFilterExec.condition)
    // Untranslatable: UDFs and predicates that may be rejected by the scan (e.g. IN)
    def isUntranslatable(pred: Expression): Boolean =
      pred.exists(_.isInstanceOf[ScalaUDF]) || pred.exists(_.isInstanceOf[In])
    val untranslatableIndices = predicates.indices.filter(i => isUntranslatable(predicates(i)))
    val translatableIndices = predicates.indices.filter(i => !isUntranslatable(predicates(i)))

    assert(
      untranslatableIndices.isEmpty || translatableIndices.isEmpty ||
        translatableIndices.max < untranslatableIndices.min,
      s"Translatable filters must appear before untranslatable filters in post-scan " +
        s"condition; predicates: ${predicates.mkString(", ")}; " +
        s"untranslatable indices: $untranslatableIndices, translatable indices: " +
        s"$translatableIndices")
  }

  /**
   * Collects pushed partition predicates from the plan when the scan is our
   * test in-memory scan.
   */
  private def getPushedPartitionPredicates(
      df: DataFrame): Seq[PartitionPredicate] = {
    val batchScan = stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
    batchScan.batch match {
      case s: InMemoryEnhancedPartitionFilterTable#InMemoryEnhancedPartitionFilterBatchScan =>
        s.getPushedPartitionPredicates
      case _ => Seq.empty
    }
  }

  /**
   * Asserts that the number of pushed partition predicates (second pass) in the plan
   * matches the expected count. Use for tests that run a query against the in-memory
   * enhanced partition filter table.
   */
  private def assertPushedPartitionPredicates(
      df: DataFrame,
      expectedCount: Int): Unit = {
    val predicates = getPushedPartitionPredicates(df)
    assert(predicates.size === expectedCount,
      s"Expected $expectedCount pushed partition predicate(s), got ${predicates.size}: $predicates")
  }

  /**
   * Asserts that each pushed partition predicate's references() (PartitionFieldReference,
   * each with ordinal()) match the expected ordinals and partition field names.
   *
   * @param df the query result
   * @param expectedOrdinals expected 0-based ordinals from Table.partitioning()
   * @param expectedPartitionFieldNames partition field names by ordinal
   *        (names(ordinal) is the name for that partition field)
   */
  private def assertReferencedPartitionFieldOrdinals(
      df: DataFrame,
      expectedOrdinals: Array[Int],
      expectedPartitionFieldNames: Array[String]): Unit = {
    val predicates = getPushedPartitionPredicates(df)
    val names = expectedPartitionFieldNames
    predicates.foreach { p =>
      val refs = p.references()
      val ordinals = refs.map(_.asInstanceOf[PartitionFieldReference].ordinal()).sorted
      assert(ordinals.sameElements(expectedOrdinals.sorted),
        s"Expected references().map(_.ordinal()) " +
          s"${expectedOrdinals.sorted.mkString("[", ", ", "]")}, " +
          s"got ${ordinals.mkString("[", ", ", "]")}")

      refs.foreach { ref =>
        assert(ref.isInstanceOf[PartitionFieldReference],
          s"Expected PartitionFieldReference, got ${ref.getClass.getName}")
        val partRef = ref.asInstanceOf[PartitionFieldReference]
        assert(partRef.fieldNames().nonEmpty,
          s"PartitionFieldReference.ordinal=${partRef.ordinal()} has empty fieldNames")
        assert(partRef.ordinal() < names.length,
          s"PartitionFieldReference.ordinal=${partRef.ordinal()} " +
            s"out of range for names length ${names.length}")
        val expectedName = names(partRef.ordinal())
        val actualName = partRef.fieldNames().mkString(".")
        assert(actualName === expectedName,
          s"PartitionFieldReference.ordinal=${partRef.ordinal()}: " +
            s"expected fieldNames '${expectedName}', got '${actualName}'")
      }
    }
  }

  /**
   * Asserts that the scan reads exactly the given set of partition keys (single-partition
   * field tables use keyString() which is the partition value).
   */
  private def assertScanReturnsPartitionKeys(
      df: DataFrame,
      expectedPartitionKeys: Set[String]): Unit = {
    val batchScan = df.queryExecution.executedPlan.collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
    val partitions = batchScan.batch.planInputPartitions()
    assert(partitions.length === expectedPartitionKeys.size,
      s"Expected ${expectedPartitionKeys.size} partition(s), got ${partitions.length}")
    val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
    assert(partKeys === expectedPartitionKeys,
      s"Partition keys should be $expectedPartitionKeys, got $partKeys")
  }
}
