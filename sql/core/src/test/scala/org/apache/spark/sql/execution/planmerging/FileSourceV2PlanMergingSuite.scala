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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.execution.{ReusedSubqueryExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, FileTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Scan merging for the built-in file sources on their DSv2 read path (SPARK-57205).
 *
 * [[FileTable]] declares the `SCAN_MERGING` table capability, so [[PlanMerger]] may fuse two file
 * scans of the same table that differ only in their projected columns and/or pushed filters. For a
 * file source the strictly enforced filters are the partition filters and the best-effort ones are
 * the data filters.
 *
 * SQL-on-file and catalog tables resolve to the V1 `FileFormat` regardless of
 * `spark.sql.sources.useV1SourceList`, so every test goes through `DataFrameReader` and asserts the
 * plan is V2 before asserting anything about merging.
 */
class FileSourceV2PlanMergingSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  // The multi-column formats in sql/core; text is single-column and Avro lives in connector/avro.
  private val multiColumnFormats = Seq("parquet", "orc", "json", "csv")

  private val flatSchema = "a long, b long, c long, d long"

  private def writeFlat(format: String, path: String): Unit =
    spark.range(0, 20)
      .selectExpr("id AS a", "id * 2 AS b", "id % 3 AS c", "id * 3 AS d")
      .write.format(format).save(path)

  private def writePartitioned(path: String): Unit =
    spark.range(0, 20)
      .selectExpr("id AS a", "id * 2 AS b", "id % 3 AS c", "id % 4 AS p")
      .write.partitionBy("p").format("parquet").save(path)

  /**
   * Registers `path` as a temp view read through the V2 path, or the V1 path if `useV1`. Not named
   * `withView`: that name is taken by a varargs helper in `QueryCleanupHelper`, which a call with
   * only positional String arguments would silently bind to instead.
   */
  private def withFileView[T](
      format: String,
      path: String,
      useV1: Boolean = false,
      schema: Option[String] = None,
      options: Map[String, String] = Map.empty,
      viewName: String = "t")(f: => T): T = {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> (if (useV1) format else "")) {
      val base = spark.read.format(format).options(options)
      val reader = schema.map(s => base.schema(s)).getOrElse(base)
      reader.load(path).createOrReplaceTempView(viewName)
      try f finally spark.catalog.dropTempView(viewName)
    }
  }

  private def v2Scans(df: DataFrame): Seq[DataSourceV2ScanRelation] =
    df.queryExecution.optimizedPlan.collectWithSubqueries {
      case s: DataSourceV2ScanRelation => s
    }

  /**
   * A merged subquery is referenced once per original subquery, so the logical plan duplicates it
   * (physical planning reuses it). Dedupe by canonical form: one distinct scan means the merge
   * happened, two means it was declined.
   */
  private def distinctScans(df: DataFrame): Int = v2Scans(df).map(_.canonicalized).distinct.length

  private def assertUsesFileSourceV2(df: DataFrame): Unit = {
    val plan = df.queryExecution.optimizedPlan
    assert(plan.collectWithSubqueries { case r: LogicalRelation => r }.isEmpty,
      s"expected the V2 file source path, but the plan has a V1 relation:\n$plan")
    val scans = v2Scans(df)
    assert(scans.nonEmpty, s"expected a DSv2 file scan:\n$plan")
    scans.foreach { s =>
      assert(s.relation.table.isInstanceOf[FileTable],
        s"expected a FileTable, got ${s.relation.table.getClass.getSimpleName}")
    }
  }

  // A successful merge builds the scan and leaves no bare DataSourceV2Relation behind; a leaked
  // deferred scan would show up as an unbuilt placeholder the read path cannot plan.
  private def assertNoPlaceholderRelation(df: DataFrame): Unit =
    assert(
      df.queryExecution.optimizedPlan.collectWithSubqueries {
        case r: DataSourceV2Relation => r
      }.isEmpty,
      s"unbuilt placeholder DataSourceV2Relation left in plan:\n${df.queryExecution.optimizedPlan}")

  /** `(SubqueryExec, ReusedSubqueryExec)` counts, the same measure `PlanMergingSuite` uses. */
  private def subqueryCounts(df: DataFrame): (Int, Int) = {
    val plan = df.queryExecution.executedPlan
    val subqueries = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
    val reused = collectWithSubqueries(plan) { case rs: ReusedSubqueryExec => rs.child.id }
    (subqueries.size, reused.size)
  }

  /**
   * Runs `query` over the parquet data at `path` on the V1 or V2 read path with both symmetric
   * filter propagation configurations on, checks the rows and returns the subquery counts.
   */
  private def mergedCounts(
      path: String,
      query: String,
      expected: Row,
      useV1: Boolean,
      enableAQE: Boolean): (Int, Int) = {
    withFileView("parquet", path, useV1 = useV1) {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
          SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
          SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
        val df = sql(query)
        checkAnswer(df, expected)
        subqueryCounts(df)
      }
    }
  }

  test("SPARK-57205: every built-in file table declares SCAN_MERGING") {
    Seq("parquet", "orc", "json", "csv", "text").foreach { format =>
      withClue(s"format=$format: ") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.range(0, 5).selectExpr("cast(id AS string) AS value")
            .write.format(format).save(path)
          withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
            val relations = spark.read.format(format).load(path)
              .queryExecution.analyzed.collect { case r: DataSourceV2Relation => r }
            assert(relations.size == 1, s"expected a single DSv2 relation, got $relations")
            val table = relations.head.table
            assert(table.isInstanceOf[FileTable], s"expected a FileTable, got $table")
            assert(table.capabilities().contains(TableCapability.SCAN_MERGING),
              s"${table.getClass.getSimpleName} should declare SCAN_MERGING")
          }
        }
      }
    }
  }

  test("SPARK-57205: merge two file scans that differ only in their projected columns") {
    multiColumnFormats.foreach { format =>
      withClue(s"format=$format: ") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          writeFlat(format, path)
          withFileView(format, path, schema = Some(flatSchema)) {
            val df = sql(
              """
                |SELECT
                |  (SELECT sum(a) FROM t WHERE c = 1),
                |  (SELECT sum(b) FROM t WHERE c = 1)
                |""".stripMargin)

            // c is id % 3, so c = 1 selects ids 1, 4, 7, 10, 13, 16 and 19.
            checkAnswer(df, Row(70, 140))
            assertUsesFileSourceV2(df)
            assert(distinctScans(df) == 1,
              s"the two scans should be fused into one:\n${df.queryExecution.optimizedPlan}")
            // Both sides carry the same data filter, so no widening is needed and this merges
            // under the default configuration. c is read because the filter stays above the scan.
            assert(v2Scans(df).head.output.map(_.name).toSet == Set("a", "b", "c"),
              s"the merged scan should read the union of both columns; " +
                s"got ${v2Scans(df).head.output}")
            assertNoPlaceholderRelation(df)
          }
        }
      }
    }
  }

  test("SPARK-57205: merge two file scans over the same partition filter") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writePartitioned(path)
      withFileView("parquet", path) {
        val df = sql(
          """
            |SELECT
            |  (SELECT sum(a) FROM t WHERE p = 1),
            |  (SELECT sum(b) FROM t WHERE p = 1)
            |""".stripMargin)

        // p is id % 4, so p = 1 selects ids 1, 5, 9, 13 and 17.
        checkAnswer(df, Row(45, 90))
        assertUsesFileSourceV2(df)
        assert(distinctScans(df) == 1,
          s"the two scans should be fused into one:\n${df.queryExecution.optimizedPlan}")
        val scan = v2Scans(df).head
        // A partition filter is fully enforced by the scan and nothing above it re-checks, so p is
        // not read; the rebuilt scan has to push the filter again or it would read all partitions.
        assert(scan.output.map(_.name).toSet == Set("a", "b"),
          s"the merged scan should read the union of both columns; got ${scan.output}")
        assert(scan.pushedFilters.exists(_.references.exists(_.name == "p")),
          s"the partition filter should be re-pushed strict onto the merged scan; " +
            s"got pushedFilters=${scan.pushedFilters.mkString("[", ", ", "]")}")
        assertNoPlaceholderRelation(df)
      }
    }
  }

  test("SPARK-57205: merge three file scans into one") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeFlat("parquet", path)
      withFileView("parquet", path) {
        val df = sql(
          """
            |SELECT
            |  (SELECT sum(a) FROM t WHERE c = 1),
            |  (SELECT sum(b) FROM t WHERE c = 1),
            |  (SELECT sum(d) FROM t WHERE c = 1)
            |""".stripMargin)

        checkAnswer(df, Row(70, 140, 210))
        assertUsesFileSourceV2(df)
        assert(distinctScans(df) == 1,
          s"the three scans should be fused into one:\n${df.queryExecution.optimizedPlan}")
        assert(v2Scans(df).head.output.map(_.name).toSet == Set("a", "b", "c", "d"),
          s"the merged scan should read the union of all three; got ${v2Scans(df).head.output}")
        assertNoPlaceholderRelation(df)
      }
    }
  }

  test("SPARK-57205: merge file scans with differing data filters only when dsv2 symmetric " +
    "filter propagation is on") {
    Seq(true, false).foreach { dsv2Symmetric =>
      withClue(s"dsv2SymmetricFilterPropagation=$dsv2Symmetric: ") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          writeFlat("parquet", path)
          withFileView("parquet", path) {
            withSQLConf(SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key ->
                dsv2Symmetric.toString) {
              val df = sql(
                """
                  |SELECT
                  |  (SELECT sum(a) FROM t WHERE a > 10),
                  |  (SELECT sum(b) FROM t WHERE b > 10)
                  |""".stripMargin)

              // a > 10 selects ids 11 to 19; b is 2 * a, so b > 10 selects ids 6 to 19.
              checkAnswer(df, Row(135, 350))
              assertUsesFileSourceV2(df)
              // a and b are data columns, so neither scan pushes a strict filter: the strict sets
              // are equal and only the OR-widening of the differing best-effort filters gates the
              // merge. The enclosing Filter keeps each aggregate exact either way.
              assert(distinctScans(df) == (if (dsv2Symmetric) 1 else 2),
                s"unexpected scan count:\n${df.queryExecution.optimizedPlan}")
              assertNoPlaceholderRelation(df)
            }
          }
        }
      }
    }
  }

  test("SPARK-57205: do not merge file scans with different partition filters") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writePartitioned(path)
      withFileView("parquet", path) {
        // Known gap against V1, which merges this shape: a partition filter is strictly enforced
        // by the scan, so widening it to OR would make the merged scan return rows nothing above
        // it filters out. Both propagation configs are on to show the merge is declined regardless.
        withSQLConf(
            SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
            SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
          val df = sql(
            """
              |SELECT
              |  (SELECT sum(a) FROM t WHERE p = 1),
              |  (SELECT sum(b) FROM t WHERE p = 2)
              |""".stripMargin)

          // p = 1 selects ids 1, 5, 9, 13, 17; p = 2 selects ids 2, 6, 10, 14, 18.
          checkAnswer(df, Row(45, 100))
          assertUsesFileSourceV2(df)
          assert(distinctScans(df) == 2,
            s"scans with different partition filters must not be fused:\n" +
              df.queryExecution.optimizedPlan)
        }
      }
    }
  }

  test("SPARK-57205: do not merge file scans that read different nested fields") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 20).selectExpr("id AS a", "named_struct('x', id, 'y', id * 2) AS s")
        .write.format("parquet").save(path)
      Seq(true, false).foreach { nestedPruning =>
        withClue(s"nestedSchemaPruning=$nestedPruning: ") {
          withFileView("parquet", path) {
            withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> nestedPruning.toString) {
              val df = sql(
                """
                  |SELECT
                  |  (SELECT sum(s.x) FROM t),
                  |  (SELECT sum(s.y) FROM t)
                  |""".stripMargin)

              checkAnswer(df, Row(190, 380))
              assertUsesFileSourceV2(df)
              // Nested pruning narrows s to the one field each side reads, so the read column is
              // no longer a same-type subset of the relation's s and the merge is declined -- the
              // field ordinals in the extractors above the scan are resolved against the narrowed
              // type. Without pruning both scans read the whole struct and are canonically equal,
              // so they merge on PlanMerger's identical-plan path, which needs no capability.
              assert(distinctScans(df) == (if (nestedPruning) 2 else 1),
                s"unexpected scan count:\n${df.queryExecution.optimizedPlan}")
            }
          }
        }
      }
    }
  }

  test("SPARK-57205: do not merge file scans that carry a pushed aggregate") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeFlat("parquet", path)
      withFileView("parquet", path) {
        withSQLConf(SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key -> "true") {
          val df = sql(
            """
              |SELECT
              |  (SELECT max(a) FROM t),
              |  (SELECT max(b) FROM t)
              |""".stripMargin)

          checkAnswer(df, Row(19, 38))
          assertUsesFileSourceV2(df)
          // A pushed aggregate is built on a branch of V2ScanRelationPushDown that never marks the
          // scan mergeable, so the merge is declined before the capability is consulted.
          assert(distinctScans(df) == 2,
            s"scans with a pushed aggregate must not be fused:\n" +
              df.queryExecution.optimizedPlan)
        }
      }
    }
  }

  test("SPARK-57205: do not merge file scans of different tables") {
    withTempPath { dir1 =>
      withTempPath { dir2 =>
        writeFlat("parquet", dir1.getCanonicalPath)
        writeFlat("parquet", dir2.getCanonicalPath)
        withFileView("parquet", dir1.getCanonicalPath, viewName = "t1") {
          withFileView("parquet", dir2.getCanonicalPath, viewName = "t2") {
            val df = sql(
              """
                |SELECT
                |  (SELECT sum(a) FROM t1 WHERE c = 1),
                |  (SELECT sum(b) FROM t2 WHERE c = 1)
                |""".stripMargin)

            checkAnswer(df, Row(70, 140))
            assertUsesFileSourceV2(df)
            assert(distinctScans(df) == 2,
              s"scans of different tables must remain separate:\n" +
                df.queryExecution.optimizedPlan)
          }
        }
      }
    }
  }

  test("SPARK-57205: V1 and V2 file sources merge the same subquery shapes") {
    val shapes = Seq(
      ("differing projected columns",
        """
          |SELECT
          |  (SELECT sum(a) FROM t WHERE c = 1),
          |  (SELECT sum(b) FROM t WHERE c = 1)
          |""".stripMargin,
        Row(70, 140)),
      ("differing data filters",
        """
          |SELECT
          |  (SELECT sum(a) FROM t WHERE a > 10),
          |  (SELECT sum(b) FROM t WHERE b > 10)
          |""".stripMargin,
        Row(135, 350)),
      ("same partition filter, differing data filters",
        """
          |SELECT
          |  (SELECT sum(a) FROM t WHERE p = 1 AND a > 4),
          |  (SELECT sum(b) FROM t WHERE p = 1 AND b > 20)
          |""".stripMargin,
        Row(44, 60)))

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writePartitioned(path)
      shapes.foreach { case (shape, query, expected) =>
        Seq(false, true).foreach { enableAQE =>
          withClue(s"$shape, AQE=$enableAQE: ") {
            val v1 = mergedCounts(path, query, expected, useV1 = true, enableAQE)
            val v2 = mergedCounts(path, query, expected, useV1 = false, enableAQE)
            assert(v1 == v2, s"V1 and V2 should merge alike; V1 got $v1, V2 got $v2")
            assert(v1 == ((1, 1)), s"both paths should merge into a single subquery; got $v1")
          }
        }
      }
    }
  }

  test("SPARK-57205: V1 merges differing partition filters, V2 does not") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writePartitioned(path)
      val query =
        """
          |SELECT
          |  (SELECT sum(a) FROM t WHERE p = 1),
          |  (SELECT sum(b) FROM t WHERE p = 2)
          |""".stripMargin
      Seq(false, true).foreach { enableAQE =>
        withClue(s"AQE=$enableAQE: ") {
          // V1 keeps the partition filter in a Filter node until physical planning, so symmetric
          // propagation can widen it; on the V2 path V2ScanRelationPushDown has already pushed it
          // into the scan as a strict filter by the time MergeSubplans runs, and strict filters
          // have to be equal to merge. Both paths return the same rows.
          assert(mergedCounts(path, query, Row(45, 100), useV1 = true, enableAQE) == ((1, 1)))
          assert(mergedCounts(path, query, Row(45, 100), useV1 = false, enableAQE) == ((2, 0)))
        }
      }
    }
  }

  test("SPARK-57205: merging widens the columns a text-based scan parses, as it does on V1") {
    // A record malformed only in the column the other scan reads. The parsers are handed just the
    // requested columns (spark.sql.csv.parser.columnPruning.enabled for CSV), so which columns a
    // scan reads decides which malformed values it notices.
    val cases = Seq(
      "csv" -> Seq("0,0", "1,10", "2,BAD", "3,30", "4,40"),
      "json" -> Seq(
        """{"a":0,"b":0}""",
        """{"a":1,"b":10}""",
        """{"a":2,"b":"BAD"}""",
        """{"a":3,"b":30}""",
        """{"a":4,"b":40}"""))
    val query =
      """
        |SELECT
        |  (SELECT sum(a) FROM t),
        |  (SELECT sum(b) FROM t)
        |""".stripMargin

    cases.foreach { case (format, lines) =>
      withClue(s"format=$format: ") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          lines.toDS().write.text(path)

          def rows(useV1: Boolean): Seq[Row] =
            withFileView(format, path, useV1 = useV1, schema = Some("a long, b long"),
              options = Map("mode" -> "DROPMALFORMED")) {
              sql(query).collect().toSeq
            }

          // Merging makes one scan parse both a and b, so the record malformed in b is dropped for
          // both aggregates and sum(a) is 8, not the 10 two separate scans produce. The merged scan
          // therefore reads fewer rows than the a-only scan did, which SCAN_MERGING's "superset of
          // their rows" premise does not allow. V1 already read the union after merging, so the V2
          // path now matches it.
          assert(rows(useV1 = false) == Seq(Row(8, 80)))
          assert(rows(useV1 = true) == rows(useV1 = false))
        }
      }
    }
  }
}
