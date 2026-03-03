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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.BufferedRows
import org.apache.spark.sql.connector.catalog.InMemoryTableEnhancedPartitionFilterCatalog
import org.apache.spark.sql.connector.catalog.TestPartitionPredicateScan
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for enhanced partition filter pushdown with tables whose scan builder handles
 * PartitionPredicates in a second pass of partition filter pushdown, for those
 * Catalyst Expression filters that are not translatable to DSV2, or are returned by DSV2
 * in the first pushdown.
 */
class DataSourceV2EnhancedPartitionFilterSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter {

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

  /**
   * Collects pushed partition predicates from the plan when the scan is our
   * test in-memory scan.
   */
  private def getPushedPartitionPredicates(
      df: org.apache.spark.sql.DataFrame): Seq[PartitionPredicate] = {
    val batchScan = df.queryExecution.executedPlan.collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
    batchScan.batch match {
      case s: TestPartitionPredicateScan => s.getPushedPartitionPredicates
      case _ => Seq.empty
    }
  }

  test("first pass partition filter still works (e.g. part_col = value)") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // Simple partition equality is pushed in the first pass and used to prune partitions
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col = 'b'")
      checkAnswer(df, Seq(Row("b", "y")))

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()

      assert(partitions.length === 1,
        "First-pass pushed predicate (part_col = 'b') should prune to one partition")
      assert(partitions.head.asInstanceOf[BufferedRows].keyString() === "b")
    }
  }

  test("untranslatable partition-only expression handled by second pass (e.g. LIKE)") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('bc', 'z')")

      // LIKE is not translated to V2 Predicate by V2ExpressionBuilder; it stays in
      // untranslatableExprs and is pushed as PartitionPredicate in the second pass.
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col LIKE 'b%'")
      checkAnswer(df, Seq(Row("b", "y"), Row("bc", "z")))

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()

      assert(partitions.length === 2,
        "Second-pass PartitionPredicate (part_col LIKE 'b%') should prune to matching partitions")
      val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
      assert(partKeys === Set("b", "bc"))
    }
  }

  test("first pass partition predicate rejected by source (e.g. IN) applied in second pass") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // IN is translated to a V2 Predicate in the first pass but rejected by
      // InMemoryTableWithV2Filter.supportsPredicates (which only accepts =, <=>, IS NULL,
      // IS NOT NULL, ALWAYS_TRUE). So it is pushed as PartitionPredicate in the second pass.
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col IN ('a', 'b')")
      checkAnswer(df, Seq(Row("a", "x"), Row("b", "y")))

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()

      assert(partitions.length === 2,
        "Rejected first-pass predicate (IN) should be applied in second pass via " +
          "PartitionPredicate and prune to matching partitions")
      val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
      assert(partKeys === Set("a", "b"))
    }
  }

  test("Second-pass PartitionPredicate filter works for UDF filter on partition column") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('A', 'y'), ('b', 'z')")

      spark.udf.register("my_upper", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE my_upper(part_col) = 'A'")
      checkAnswer(df, Seq(Row("a", "x"), Row("A", "y")))

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val scan = batchScan.batch
      val partitions = scan.planInputPartitions()

      assert(partitions.length === 2,
        "Only partitions satisfying all pushed PartitionPredicates should be read")
      val expectedPartitionKeys = Set("a", "A")
      partitions.foreach { p =>
        val keyStr = p.asInstanceOf[BufferedRows].keyString()
        assert(expectedPartitionKeys.contains(keyStr),
          s"Partition $keyStr (InternalRow) must be among partitions accepted by " +
            "all pushed PartitionPredicates")
      }
      val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
      assert(partKeys === expectedPartitionKeys)
    }
  }

  test("untranslatable data filters are applied after scan") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'keep'), ('a', 'drop'), ('b', 'other')")

      // UDF on data column does not translate to V2 Predicate; it must be returned as
      // post-scan filter (dataFiltersFromSecondPass) so Spark applies it.
      spark.udf.register("is_keep", (s: String) => s != null && s == "keep")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col = 'a' AND is_keep(data)")
      checkAnswer(df, Seq(Row("a", "keep")))

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()
      assert(partitions.length === 1,
        "Partition predicate part_col = 'a' should prune to one partition")
      assert(partitions.head.asInstanceOf[BufferedRows].keyString() === "a")
    }
  }

  test("referencedPartitionColumnOrdinals: one non-first partition column in second-pass") {
    withTable(partFilterTableName) {
      // Partitioning (p0, p1, p2); filter only on p1 (ordinal 1) via LIKE in second pass.
      sql(s"CREATE TABLE $partFilterTableName (p0 string, p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', '1', 'd1'), ('a', 'y', '1', 'd2'), " +
        "('a', 'x', '2', 'd3'), ('b', 'x', '1', 'd4')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE p1 LIKE 'x%'")
      checkAnswer(df, Seq(
        Row("a", "x", "1", "d1"), Row("a", "x", "2", "d3"), Row("b", "x", "1", "d4")))

      val predicates = getPushedPartitionPredicates(df)
      assert(predicates.nonEmpty, "Expected at least one pushed PartitionPredicate")
      predicates.foreach { p =>
        val ordinals = p.referencedPartitionColumnOrdinals()
        assert(ordinals.sameElements(Array(1)),
          s"Predicate on p1 only (ordinal 1) should have ordinals [1], " +
            s"got ${ordinals.mkString("[", ", ", "]")}")
      }
    }
  }

  test("referencedPartitionColumnOrdinals: two non-first partition columns in second-pass") {
    withTable(partFilterTableName) {
      // Partitioning (p0, p1, p2); filter on p1 and p2 (ordinals 1, 2) via UDF.
      sql(s"CREATE TABLE $partFilterTableName (p0 string, p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', '1', 'd1'), ('a', 'y', '1', 'd2'), " +
        "('a', 'x', '2', 'd3'), ('b', 'x', '1', 'd4')")

      spark.udf.register("concat2", (a: String, b: String) =>
        if (a == null || b == null) null else a + b)

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE concat2(p1, p2) = 'x1'")
      checkAnswer(df, Seq(Row("a", "x", "1", "d1"), Row("b", "x", "1", "d4")))

      val predicates = getPushedPartitionPredicates(df)
      assert(predicates.nonEmpty, "Expected at least one pushed PartitionPredicate")
      predicates.foreach { p =>
        val ordinals = p.referencedPartitionColumnOrdinals()
        assert(ordinals.sameElements(Array(1, 2)),
          s"Predicate on p1 and p2 should have ordinals [1, 2], " +
            s"got ${ordinals.mkString("[", ", ", "]")}")
      }
    }
  }
}
