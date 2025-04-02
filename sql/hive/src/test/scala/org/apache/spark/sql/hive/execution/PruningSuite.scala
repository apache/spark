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

package org.apache.spark.sql.hive.execution

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.hive.test.{TestHive, TestHiveQueryExecution}
import org.apache.spark.tags.SlowHiveTest

/**
 * A set of test cases that validate partition and column pruning.
 */
@SlowHiveTest
class PruningSuite extends HiveComparisonTest with BeforeAndAfter {

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestHive.setCacheTables(false)
    // Column/partition pruning is not implemented for `InMemoryColumnarTableScan` yet,
    // need to reset the environment to ensure all referenced tables in this suites are
    // not cached in-memory. Refer to https://issues.apache.org/jira/browse/SPARK-2283
    // for details.
    TestHive.reset()
  }

  // Column pruning tests

  createPruningTest("Column pruning - with partitioned table",
    "SELECT key FROM srcpart WHERE ds = '2008-04-08' LIMIT 3",
    Seq("key"),
    Seq("key"),
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-08", "12")))

  createPruningTest("Column pruning - with non-partitioned table",
    "SELECT key FROM src WHERE key > 10 LIMIT 3",
    Seq("key"),
    Seq("key"),
    Seq.empty)

  createPruningTest("Column pruning - with multiple projects",
    "SELECT c1 FROM (SELECT key AS c1 FROM src WHERE key > 10) t1 LIMIT 3",
    Seq("c1"),
    Seq("key"),
    Seq.empty)

  createPruningTest("Column pruning - projects alias substituting",
    "SELECT c1 AS c2 FROM (SELECT key AS c1 FROM src WHERE key > 10) t1 LIMIT 3",
    Seq("c2"),
    Seq("key"),
    Seq.empty)

  createPruningTest("Column pruning - filter alias in-lining",
    "SELECT c1 FROM (SELECT key AS c1 FROM src WHERE key > 10) t1 WHERE c1 < 100 LIMIT 3",
    Seq("c1"),
    Seq("key"),
    Seq.empty)

  createPruningTest("Column pruning - without filters",
    "SELECT c1 FROM (SELECT key AS c1 FROM src) t1 LIMIT 3",
    Seq("c1"),
    Seq("key"),
    Seq.empty)

  createPruningTest("Column pruning - simple top project without aliases",
    "SELECT key FROM (SELECT key FROM src WHERE key > 10) t1 WHERE key < 100 LIMIT 3",
    Seq("key"),
    Seq("key"),
    Seq.empty)

  createPruningTest("Column pruning - non-trivial top project with aliases",
    "SELECT c1 * 2 AS dbl FROM (SELECT key AS c1 FROM src WHERE key > 10) t1 LIMIT 3",
    Seq("dbl"),
    Seq("key"),
    Seq.empty)

  // Partition pruning tests

  createPruningTest("Partition pruning - non-partitioned, non-trivial project",
    "SELECT key * 2 AS dbl FROM src WHERE value IS NOT NULL",
    Seq("dbl"),
    Seq("key", "value"),
    Seq.empty)

  createPruningTest("Partition pruning - non-partitioned table",
    "SELECT value FROM src WHERE key IS NOT NULL",
    Seq("value"),
    Seq("value", "key"),
    Seq.empty)

  createPruningTest("Partition pruning - with filter on string partition key",
    "SELECT value, hr FROM srcpart1 WHERE ds = '2008-04-08'",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-08", "12")))

  createPruningTest("Partition pruning - with filter on int partition key",
    "SELECT value, hr FROM srcpart1 WHERE hr < 12",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-09", "11")))

  createPruningTest("Partition pruning - left only 1 partition",
    "SELECT value, hr FROM srcpart1 WHERE ds = '2008-04-08' AND hr < 12",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq(
      Seq("2008-04-08", "11")))

  createPruningTest("Partition pruning - all partitions pruned",
    "SELECT value, hr FROM srcpart1 WHERE ds = '2014-01-27' AND hr = 11",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq.empty)

  createPruningTest("Partition pruning - pruning with both column key and partition key",
    "SELECT value, hr FROM srcpart1 WHERE value IS NOT NULL AND hr < 12",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-09", "11")))

  createPruningTest("Partition pruning - with filter containing non-deterministic condition",
    "SELECT value, hr FROM srcpart1 WHERE ds = '2008-04-08' AND hr < 12 AND rand() < 1",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq(
      Seq("2008-04-08", "11")))

  def createPruningTest(
      testCaseName: String,
      sql: String,
      expectedOutputColumns: Seq[String],
      expectedScannedColumns: Seq[String],
      expectedPartValues: Seq[Seq[String]]): Unit = {
    test(s"$testCaseName - pruning test") {
      val plan = new TestHiveQueryExecution(sql).sparkPlan
      val actualOutputColumns = plan.output.map(_.name)
      val (actualScannedColumns, actualPartValues) = plan.collect {
        case p @ HiveTableScanExec(columns, relation, _) =>
          val columnNames = columns.map(_.name)
          val partValues = if (relation.isPartitioned) {
            p.prunedPartitions.map(_.getValues)
          } else {
            Seq.empty
          }
          (columnNames, partValues)
      }.head

      assert(actualOutputColumns === expectedOutputColumns, "Output columns mismatch")

      // Scanned columns in `HiveTableScanExec` are generated by the `pruneFilterProject` method
      // in `SparkPlanner`. This method internally uses `AttributeSet.toSeq`, in which
      // the returned output columns are sorted by the names and expression ids.
      assert(actualScannedColumns.sorted === expectedScannedColumns.sorted,
        "Scanned columns mismatch")

      val actualPartitions = actualPartValues.map(_.asScala.mkString(",")).sorted
      val expectedPartitions = expectedPartValues.map(_.mkString(",")).sorted

      assert(actualPartitions === expectedPartitions, "Partitions selected do not match")
    }

    // Creates a query test to compare query results generated by Hive and Catalyst.
    createQueryTest(s"$testCaseName - query test", sql)
  }
}
