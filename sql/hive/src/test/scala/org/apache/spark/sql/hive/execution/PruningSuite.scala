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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.hive.test.TestHive

/* Implicit conversions */
import scala.collection.JavaConversions._

/**
 * A set of test cases that validate partition and column pruning.
 */
class PruningSuite extends HiveComparisonTest with BeforeAndAfter {
  TestHive.cacheTables = false

  // Column/partition pruning is not implemented for `InMemoryColumnarTableScan` yet, need to reset
  // the environment to ensure all referenced tables in this suites are not cached in-memory.
  // Refer to https://issues.apache.org/jira/browse/SPARK-2283 for details.
  TestHive.reset()

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
    "SELECT c1 * 2 AS double FROM (SELECT key AS c1 FROM src WHERE key > 10) t1 LIMIT 3",
    Seq("double"),
    Seq("key"),
    Seq.empty)

  // Partition pruning tests

  createPruningTest("Partition pruning - non-partitioned, non-trivial project",
    "SELECT key * 2 AS double FROM src WHERE value IS NOT NULL",
    Seq("double"),
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

  def createPruningTest(
      testCaseName: String,
      sql: String,
      expectedOutputColumns: Seq[String],
      expectedScannedColumns: Seq[String],
      expectedPartValues: Seq[Seq[String]]) = {
    test(s"$testCaseName - pruning test") {
      val plan = new TestHive.HiveQLQueryExecution(sql).executedPlan
      val actualOutputColumns = plan.output.map(_.name)
      val (actualScannedColumns, actualPartValues) = plan.collect {
        case p @ HiveTableScan(columns, relation, _) =>
          val columnNames = columns.map(_.name)
          val partValues = p.prunePartitions(relation.hiveQlPartitions).map(_.getValues)
          (columnNames, partValues)
      }.head

      assert(actualOutputColumns === expectedOutputColumns, "Output columns mismatch")
      assert(actualScannedColumns === expectedScannedColumns, "Scanned columns mismatch")

      assert(
        actualPartValues.length === expectedPartValues.length,
        "Partition value count mismatches")

      for ((actual, expected) <- actualPartValues.zip(expectedPartValues)) {
        assert(actual sameElements expected, "Partition values mismatch")
      }
    }

    // Creates a query test to compare query results generated by Hive and Catalyst.
    createQueryTest(s"$testCaseName - query test", sql)
  }
}
