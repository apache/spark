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

package org.apache.spark.sql.execution.planmerging

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.connector.FakeV2ProviderWithCustomSchema
import org.apache.spark.sql.connector.catalog.InMemoryScanMergingPartitionFilterCatalog
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end test for the DSv2 scan-merge gap this change closes: a source whose (equal) filter is
 * strict only via the iterative PartitionPredicate second pass. When [[PlanMerger]] rebuilds the
 * merged scan it drives the real
 * [[org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown]] (Approach 2), so the
 * second pass runs and the filter comes back strict -- the merge proceeds. A hand-rolled single
 * push-predicates call would only run the first pass, see the filter come back post-scan,
 * mis-classify it as non-strict and decline the merge (leaving two scans).
 */
class DSv2PlanMergingSuite extends QueryTest with SharedSparkSession
  with BeforeAndAfter {

  private val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  private val tbl = "scanmerge.t"

  before {
    spark.conf.set("spark.sql.catalog.scanmerge",
      classOf[InMemoryScanMergingPartitionFilterCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.conf.unset("spark.sql.catalog.scanmerge")
  }

  private def v2Scans(df: DataFrame): Seq[DataSourceV2ScanRelation] =
    df.queryExecution.optimizedPlan.collectWithSubqueries {
      case s: DataSourceV2ScanRelation => s
    }

  test("SPARK-40259: merge two DSv2 scans whose filter is strict only via the second pass") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (part_col string, c1 int, c2 int) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $tbl VALUES ('a', 1, 10), ('a', 2, 20), ('b', 3, 30)")

      // `part_col IN ('a')` is untranslatable/returned in the first pass and only accepted (strict)
      // in the iterative PartitionPredicate second pass. The two scalar subqueries differ only in
      // their projected data column, so PlanMerger fuses them into a single scan reading {c1, c2}.
      val df = sql(
        s"""
           |SELECT
           |  (SELECT max(c1) FROM $tbl WHERE part_col IN ('a')) AS m1,
           |  (SELECT max(c2) FROM $tbl WHERE part_col IN ('a')) AS m2
           |""".stripMargin)

      // Correctness: the merged scan must still enforce the partition filter. Reading all
      // partitions would give (3, 30) instead of the filtered (2, 20).
      checkAnswer(df, Row(2, 20))

      // The merged subquery is referenced once per scalar subquery, so the logical plan duplicates
      // it (physical planning reuses it). Dedupe by canonical form: a successful merge leaves a
      // single distinct scan reading the union of both columns; declining would leave two distinct
      // scans, one per column.
      val scans = v2Scans(df)
      assert(scans.nonEmpty, s"expected a DSv2 scan:\n${df.queryExecution.optimizedPlan}")
      assert(scans.map(_.canonicalized).distinct.length == 1,
        s"the two scans should be fused into one:\n${df.queryExecution.optimizedPlan}")
      val scan = scans.head
      assert(scan.output.map(_.name).toSet == Set("c1", "c2"),
        s"the merged scan should read the union of both columns; got ${scan.output}")
      // The filter must be re-enforced strictly on the merged scan (present in pushedFilters),
      // which only happens because the rebuild runs the iterative second pass.
      assert(scan.pushedFilters.exists(_.references.exists(_.name == "part_col")),
        s"the part_col filter should be re-pushed strict onto the merged scan; " +
          s"got pushedFilters=${scan.pushedFilters.mkString("[", ", ", "]")}")
    }
  }

  test("SPARK-40259: do not merge DSv2 scans with different strict partition filters") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (part_col string, c1 int, c2 int) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $tbl VALUES ('a', 1, 10), ('a', 2, 20), ('b', 3, 30)")

      // Same table and shape, but different (strict, fully-enforced) partition filters. The strict
      // filters are unequal, so the two scans must NOT fuse -- otherwise one side's partition would
      // be read for both, giving a wrong answer.
      val df = sql(
        s"""
           |SELECT
           |  (SELECT max(c1) FROM $tbl WHERE part_col IN ('a')) AS m1,
           |  (SELECT max(c2) FROM $tbl WHERE part_col IN ('b')) AS m2
           |""".stripMargin)

      checkAnswer(df, Row(2, 30))

      val scans = v2Scans(df)
      assert(scans.map(_.canonicalized).distinct.length == 2,
        s"scans with different partition filters must not be fused:\n" +
          df.queryExecution.optimizedPlan)
    }
  }

  test("SPARK-40259: merge two DSv2 scans with differing best-effort filters (OR-widen)") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (part_col string, c1 int, c2 int) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $tbl VALUES ('a', 1, 10), ('a', 2, 20), ('b', 3, 30)")

      // c1 > 1 / c2 > 1 are data-column filters this source does not enforce (best-effort,
      // post-scan), so the two scans carry no strict filter and fuse into one reading {c1, c2}.
      // The differing post-scan filters are OR-widened above the merged scan (Phase 2) and each
      // aggregate keeps its own filter. This runs the differing-filter path end to end, which the
      // plan-shape tests in MergeSubplansSuite cover only structurally. (A rand() on one side is
      // not added here: it would make the scalar subquery non-deterministic, so MergeSubplans would
      // not extract it at all; the non-deterministic pruning drop is unit-covered in
      // MergeSubplansSuite.)
      withSQLConf(
          SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
          SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
        val df = sql(
          s"""
             |SELECT
             |  (SELECT max(c1) FROM $tbl WHERE c1 > 1) AS m1,
             |  (SELECT max(c2) FROM $tbl WHERE c2 > 1) AS m2
             |""".stripMargin)

        // c1 > 1 -> {2, 3} -> max 3; c2 > 1 -> {10, 20, 30} -> max 30.
        checkAnswer(df, Row(3, 30))

        val scans = v2Scans(df)
        assert(scans.map(_.canonicalized).distinct.length == 1,
          s"the two scans should be fused into one:\n${df.queryExecution.optimizedPlan}")
        assert(scans.head.output.map(_.name).toSet == Set("c1", "c2"),
          s"the merged scan should read the union of both columns; got ${scans.head.output}")
      }
    }
  }
}
