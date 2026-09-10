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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{InMemoryPartitionPredicateDeleteCatalog, InMemoryPartitionPredicateDeleteTable}
import org.apache.spark.sql.connector.expressions.PartitionFieldReference
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DeleteFromTableExec, ReplaceDataExec, WriteDeltaExec}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Tests for metadata-only delete optimization using second-pass
 * PartitionPredicate (see SPARK-55596), and for the second pass in the scan of a
 * group-based UPDATE or MERGE (GroupBasedRowLevelOperationScanPlanning).
 */
class DataSourceV2EnhancedDeleteFilterSuite extends SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  private val catalogName = "ppd_cat"
  private val deleteTableName = s"$catalogName.t"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalogName",
      classOf[InMemoryPartitionPredicateDeleteCatalog].getName)

  // '=' on a partition column is accepted by canDeleteWhere in the first pass.
  test("first pass accepted: partition column filter") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep)")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'hr', 200), (3, 'software', 300)")

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName WHERE dep = 'hr'",
        expectedNumConditions = 1)

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Row(3, "software", 300) :: Nil)
    }
  }

  // '=' on a data column is accepted by canDeleteWhere in the first pass
  // when accept-data-predicates is enabled; row-level filtering applies.
  test("first pass accepted: data column filter") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep) " +
        "TBLPROPERTIES('accept-data-predicates' = 'true')")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'hr', 200), (3, 'software', 300)")

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName WHERE salary = 100",
        expectedNumConditions = 1)

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row(2, "hr", 200), Row(3, "software", 300)))
    }
  }

  // IN is rejected by canDeleteWhere; second pass converts it
  // to a PartitionPredicate which is accepted.
  test("first pass rejected, second pass accepted: partition predicate") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep)")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'software', 200), (3, 'marketing', 300)")

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName WHERE dep IN ('hr', 'software')",
        expectedNumConditions = 1,
        expectedNumPartitionPredicates = 1,
        expectedOrdinalsPerPredicate = Seq(Array(0)),
        expectedPartitionFieldNames = Array("dep"))

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Row(3, "marketing", 300) :: Nil)
    }
  }

  // STARTS_WITH on partition column rejected in first pass; second pass
  // creates a PartitionPredicate for it. '=' on data column translates
  // to V2. Table accepts both (accept-data-predicates=true).
  test("first pass rejected, second pass accepted: mixed partition + data") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep) " +
        "TBLPROPERTIES('accept-data-predicates' = 'true')")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'hr', 200), (3, 'software', 300)")

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName " +
          "WHERE dep LIKE 'h%' AND salary = 100",
        expectedNumConditions = 2,
        expectedNumPartitionPredicates = 1,
        expectedOrdinalsPerPredicate = Seq(Array(0)),
        expectedPartitionFieldNames = Array("dep"))

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row(2, "hr", 200), Row(3, "software", 300)))
    }
  }

  // UDF referencing non-contiguous partition columns (p0, p2);
  // untranslatable to V2, wrapped as PartitionPredicate in second pass.
  test("second pass accepted: UDF on non-contiguous partition columns") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName " +
        "(data STRING, p0 STRING, p1 STRING, p2 STRING) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "('d1', 'a', 'y', 'x'), ('d2', 'b', 'y', 'z'), " +
        "('d3', 'a', 'z', 'z')")

      spark.udf.register("my_concat",
        udf((s1: String, s2: String) => s1 + s2))

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName " +
          "WHERE my_concat(p0, p2) = 'ax'",
        expectedNumConditions = 1,
        expectedNumPartitionPredicates = 1,
        expectedOrdinalsPerPredicate = Seq(Array(0, 2)),
        expectedPartitionFieldNames = Array("p0", "p1", "p2"))

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row("d2", "b", "y", "z"),
          Row("d3", "a", "z", "z")))
    }
  }

  // Two separate partition filters on different columns (p0, p2) each
  // become their own PartitionPredicate; ordinals [0] and [2].
  test("second pass accepted: multiple PartitionPredicates") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName " +
        "(data STRING, p0 STRING, p1 STRING, p2 STRING) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "('d1', 'a', 'y', 'x'), ('d2', 'b', 'y', 'z'), " +
        "('d3', 'a', 'z', 'z')")

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName " +
          "WHERE p0 IN ('a', 'b') AND p2 LIKE 'x%'",
        expectedNumConditions = 2,
        expectedNumPartitionPredicates = 2,
        expectedOrdinalsPerPredicate =
          Seq(Array(0), Array(2)),
        expectedPartitionFieldNames =
          Array("p0", "p1", "p2"))

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row("d2", "b", "y", "z"),
          Row("d3", "a", "z", "z")))
    }
  }

  // Mixed partitioning: the bucket field keeps its ordinal but is never referenced, so the
  // IN on the identity column still becomes a PartitionPredicate over the full partition key.
  test("second pass accepted: identity column next to a bucket transform") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep, bucket(4, pk))")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'software', 200), (3, 'marketing', 300)")

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName WHERE dep IN ('hr', 'software')",
        expectedNumConditions = 1,
        expectedNumPartitionPredicates = 1,
        expectedOrdinalsPerPredicate = Seq(Array(0)),
        expectedPartitionFieldNames = Array("dep", "bucket(4, pk)"))

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Row(3, "marketing", 300) :: Nil)
    }
  }

  // `dt = DATE'...'` is analyzed as `cast(dt AS DATE) = DATE'...'`, which the table cannot
  // evaluate in the first pass; the second pass turns it into a PartitionPredicate.
  test("second pass accepted: cast on a string identity column next to a bucket transform") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dt STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dt, bucket(4, pk))")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, '2026-09-01', 100), (2, '2026-09-02', 200), (3, '2026-09-03', 300)")

      assertDeleteWithFilters(
        s"DELETE FROM $deleteTableName WHERE dt = DATE'2026-09-02'",
        expectedNumConditions = 1,
        expectedNumPartitionPredicates = 1,
        expectedOrdinalsPerPredicate = Seq(Array(0)),
        expectedPartitionFieldNames = Array("dt", "bucket(4, pk)"))

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row(1, "2026-09-01", 100), Row(3, "2026-09-03", 300)))
    }
  }

  // Table property disables PartitionPredicate acceptance;
  // both passes rejected, falls back to row-level operation.
  test("first and second pass rejected: table rejects all") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep) " +
        "TBLPROPERTIES('accept-partition-predicates' = 'false')")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'software', 200), (3, 'marketing', 300)")

      assertDeleteWithRowLevel(
        s"DELETE FROM $deleteTableName WHERE dep IN ('hr', 'software')")

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Row(3, "marketing", 300) :: Nil)
    }
  }

  // First pass fails because table configured to reject data-column predicate.
  // Second pass skipped because no partition predicates can be created from a data filter
  // and the table configured to reject data filters.
  test("second pass skipped: data-only filter") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep)")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'hr', 200), (3, 'software', 300)")

      assertDeleteWithRowLevel(
        s"DELETE FROM $deleteTableName WHERE salary = 100")

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row(2, "hr", 200), Row(3, "software", 300)))
    }
  }

  // Partition predicate created for IN, but remaining data filter
  // causes canDeleteWhere to reject the combined predicates;
  // falls back to row-level operation.
  test("second pass rejected: successful partition predicate but rejected data filter") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep)")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'hr', 200), (3, 'software', 300)")

      assertDeleteWithRowLevel(
        s"DELETE FROM $deleteTableName WHERE dep IN ('hr') AND salary = 100")

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row(2, "hr", 200), Row(3, "software", 300)))
    }
  }

  // A group-based UPDATE reads the table through RowLevelOperationTable. The wrapper reports
  // the table's partitioning, so the row-level scan, which pushes V2 predicates iteratively,
  // receives the IN on the partition column as a second-pass PartitionPredicate, and only the
  // matching partitions are read and replaced.
  test("SPARK-59457: group-based UPDATE receives a second-pass PartitionPredicate") {
    withTable(deleteTableName) {
      sql(s"CREATE TABLE $deleteTableName (pk INT, dep STRING, salary INT) " +
        s"USING $v2Source PARTITIONED BY (dep)")
      sql(s"INSERT INTO $deleteTableName VALUES " +
        "(1, 'hr', 100), (2, 'software', 200), (3, 'marketing', 300)")

      val plan = executeAndKeepPlan {
        sql(s"UPDATE $deleteTableName SET salary = salary + 1 WHERE dep IN ('hr', 'software')")
      }
      assertRowLevelScanPrunedByPartitionPredicate(plan,
        expectedOrdinals = Array(0),
        expectedPartitionFieldNames = Array("dep"),
        expectedReplacedDeps = Set("hr", "software"))

      checkAnswer(
        sql(s"SELECT * FROM $deleteTableName"),
        Seq(Row(1, "hr", 101), Row(2, "software", 201), Row(3, "marketing", 300)))
    }
  }

  private def executeAndKeepPlan(func: => Unit): SparkPlan = {
    var executedPlan: SparkPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(
          funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        executedPlan = qe.executedPlan
      }
      override def onFailure(
          funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)

    try {
      func
      sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.listenerManager.unregister(listener)
    }

    assert(executedPlan != null,
      "QueryExecutionListener did not capture the executed plan")
    executedPlan
  }

  /**
   * @param expectedOrdinalsPerPredicate one entry per expected
   *   PartitionPredicate, each listing the ordinals that predicate
   *   should reference, in the order they appear in the plan.
   */
  private def assertDeleteWithFilters(
      query: String,
      expectedNumConditions: Int,
      expectedNumPartitionPredicates: Int = 0,
      expectedOrdinalsPerPredicate: Seq[Array[Int]] = Seq.empty,
      expectedPartitionFieldNames: Array[String] = Array.empty
  ): Unit = {
    val plan = executeAndKeepPlan { sql(query) }
    val exec = plan match {
      case d: DeleteFromTableExec => d
      case other =>
        fail("Expected DeleteFromTableExec but got: " +
          other.getClass.getSimpleName)
    }
    val conds = exec.condition
    assert(conds.length === expectedNumConditions,
      s"Expected $expectedNumConditions condition(s), " +
        s"got ${conds.length}: " +
        conds.mkString("[", ", ", "]"))

    val partPreds = conds.collect {
      case p: PartitionPredicate => p
    }
    assert(partPreds.length === expectedNumPartitionPredicates,
      s"Expected $expectedNumPartitionPredicates " +
        s"PartitionPredicate(s), got ${partPreds.length}: " +
        conds.mkString("[", ", ", "]"))

    if (partPreds.nonEmpty && partPreds.length < conds.length) {
      val lastPartIdx = conds.lastIndexWhere(
        _.isInstanceOf[PartitionPredicate])
      val firstNonPartIdx = conds.indexWhere(
        c => !c.isInstanceOf[PartitionPredicate])
      assert(lastPartIdx < firstNonPartIdx,
        "PartitionPredicates should precede non-partition " +
          "predicates: " + conds.mkString("[", ", ", "]"))
    }

    assertPartitionFieldReferences(
      partPreds, expectedOrdinalsPerPredicate,
      expectedPartitionFieldNames)
  }

  private def assertPartitionFieldReferences(
      partPreds: Array[PartitionPredicate],
      expectedOrdinalsPerPredicate: Seq[Array[Int]],
      expectedPartitionFieldNames: Array[String]): Unit = {
    if (expectedOrdinalsPerPredicate.isEmpty) return
    val names = expectedPartitionFieldNames

    assert(partPreds.length === expectedOrdinalsPerPredicate.size,
      s"Predicate count ${partPreds.length} != " +
        s"expected ${expectedOrdinalsPerPredicate.size}")

    partPreds.zip(expectedOrdinalsPerPredicate).foreach {
      case (p, expectedOrdinals) =>
        val refs = p.references()
        val ordinals = refs.map(
          _.asInstanceOf[PartitionFieldReference].ordinal()).sorted
        assert(
          ordinals.sameElements(expectedOrdinals.sorted),
          s"Expected ordinals " +
            expectedOrdinals.sorted.mkString("[", ", ", "]") +
            s", got ${ordinals.mkString("[", ", ", "]")}")

        refs.foreach { ref =>
          assert(ref.isInstanceOf[PartitionFieldReference],
            "Expected PartitionFieldReference, " +
              s"got ${ref.getClass.getName}")
          val partRef =
            ref.asInstanceOf[PartitionFieldReference]
          assert(partRef.fieldNames().nonEmpty,
            s"ordinal=${partRef.ordinal()} " +
              "has empty fieldNames")
          assert(partRef.ordinal() < names.length,
            s"ordinal=${partRef.ordinal()} out of range " +
              s"for ${names.length} field name(s)")
          val expected = names(partRef.ordinal())
          val actual = partRef.fieldNames().mkString(".")
          assert(actual === expected,
            s"ordinal=${partRef.ordinal()}: expected " +
              s"'$expected', got '$actual'")
        }
    }
  }

  /**
   * Asserts that the group-based plan's row-level scan was pruned by one PartitionPredicate with
   * the given references, and that only the partitions with the given `dep` values, the first
   * partition field, were replaced.
   */
  private def assertRowLevelScanPrunedByPartitionPredicate(
      plan: SparkPlan,
      expectedOrdinals: Array[Int],
      expectedPartitionFieldNames: Array[String],
      expectedReplacedDeps: Set[String]): Unit = {
    assert(plan.isInstanceOf[ReplaceDataExec],
      s"Expected ReplaceDataExec but got: ${plan.getClass.getSimpleName}")
    val scans = collect(plan) { case s: BatchScanExec => s }
    val scan = scans.map(_.scan).collectFirst {
      case s: InMemoryPartitionPredicateDeleteTable#PartitionPredicateRowLevelBatchScan => s
    }.getOrElse(fail("Expected the row-level scan of the in-memory table"))
    assertPartitionFieldReferences(
      scan.pushedPartitionPredicates.toArray, Seq(expectedOrdinals), expectedPartitionFieldNames)

    val table = scans.map(_.table).collectFirst {
      case RowLevelOperationTable(t: InMemoryPartitionPredicateDeleteTable, _) => t
    }.getOrElse(fail("Expected the row-level operation table"))
    val replacedDeps = table.replacedPartitions.map(_.head.toString)
    assert(
      replacedDeps.toSet === expectedReplacedDeps &&
        replacedDeps.size === expectedReplacedDeps.size,
      s"Expected replaced partitions for $expectedReplacedDeps, got ${table.replacedPartitions}")
  }

  private def assertDeleteWithRowLevel(query: String): Unit = {
    val plan = executeAndKeepPlan { sql(query) }
    plan match {
      case _: ReplaceDataExec => // expected
      case _: WriteDeltaExec => // expected
      case other =>
        fail("Expected ReplaceDataExec or WriteDeltaExec " +
          s"but got: ${other.getClass.getSimpleName}")
    }
  }
}
