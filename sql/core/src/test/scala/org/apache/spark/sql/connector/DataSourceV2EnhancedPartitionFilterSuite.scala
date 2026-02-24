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
import org.apache.spark.sql.connector.catalog.{BufferedRows, InMemoryTableEnhancedPartitionFilterCatalog}
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

  protected def registerCatalog(name: String, clazz: Class[_]): Unit = {
    spark.conf.set(s"spark.sql.catalog.$name", clazz.getName)
  }

  before {
    registerCatalog("testpartfilter", classOf[InMemoryTableEnhancedPartitionFilterCatalog])
  }

  after {
    spark.sessionState.catalogManager.reset()
  }

  test("first pass partition filter still works (e.g. part_col = value)") {
    val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
    val t = "testpartfilter.t"
    withTable(t) {
      sql(s"CREATE TABLE $t (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $t VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // Simple partition equality is pushed in the first pass and used to prune partitions
      val df = sql("SELECT * FROM testpartfilter.t WHERE part_col = 'b'")
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
    val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
    val t = "testpartfilter.t"
    withTable(t) {
      sql(s"CREATE TABLE $t (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $t VALUES ('a', 'x'), ('b', 'y'), ('bc', 'z')")

      // LIKE is not translated to V2 Predicate by V2ExpressionBuilder; it stays in
      // untranslatableExprs and is pushed as PartitionPredicate in the second pass.
      val df = sql("SELECT * FROM testpartfilter.t WHERE part_col LIKE 'b%'")
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
    val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
    val t = "testpartfilter.t"
    withTable(t) {
      sql(s"CREATE TABLE $t (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $t VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // IN is translated to a V2 Predicate in the first pass but rejected by
      // InMemoryTableWithV2Filter.supportsPredicates (which only accepts =, <=>, IS NULL,
      // IS NOT NULL, ALWAYS_TRUE). So it is pushed as PartitionPredicate in the second pass.
      val df = sql("SELECT * FROM testpartfilter.t WHERE part_col IN ('a', 'b')")
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
    val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
    val t = "testpartfilter.t"
    withTable(t) {
      sql(s"CREATE TABLE $t (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $t VALUES ('a', 'x'), ('A', 'y'), ('b', 'z')")

      spark.udf.register("my_upper", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql("SELECT * FROM testpartfilter.t WHERE my_upper(part_col) = 'A'")
      checkAnswer(df, Seq(Row("a", "x"), Row("A", "y")))

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val scan = batchScan.batch
      val partitions = scan.planInputPartitions()

      // planInputPartitions must return only InputPartitions whose partition values
      // (as InternalRow) return true for all pushed PartitionPredicate.accept().
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
}
