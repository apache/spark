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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.execution.{ReusedSubqueryExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileScan, FileTable}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Scan merging for the built-in file sources on their DSv2 read path (SPARK-57205).
 *
 * Parquet, ORC, text and Avro override [[FileTable.supportsScanMerging]], so [[PlanMerger]] may
 * fuse two of their scans of the same table. Scans that differ only in their projected columns
 * merge under the default configuration; scans whose data filters differ need one of the symmetric
 * filter propagation configurations, and scans whose partition filters differ never merge, because
 * for a file source the partition filters are the strictly enforced ones and the data filters are
 * best-effort. The capability is also withheld from a table whose reads are not strict.
 *
 * CSV and JSON do not override it. Their parsers are handed the columns the scan asked for, at
 * least while `spark.sql.csv.parser.columnPruning.enabled` is on for CSV, and decide from that set
 * what counts as a malformed record.
 *
 * SQL-on-file and catalog tables resolve to the V1 `FileFormat` regardless of
 * `spark.sql.sources.useV1SourceList`, so every test goes through `DataFrameReader` and asserts
 * which read path the plan took before asserting anything about merging.
 */
class FileSourceV2PlanMergingSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper with V2ScanMergingTestHelper {
  import testImplicits._

  // Pin what the merge decision now depends on, and what the subquery-count measure depends on, so
  // a changed default fails in one legible place rather than inverting every assertion below. Tests
  // that vary one of these set it themselves.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.IGNORE_CORRUPT_FILES, false)
    .set(SQLConf.IGNORE_MISSING_FILES, false)
    .set(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED, true)
    .set(SQLConf.SUBQUERY_REUSE_ENABLED, true)

  // The formats in sql/core that declare SCAN_MERGING and have more than one column. Avro also
  // declares it but lives in connector/avro; text declares it but has only `value`.
  private val mergingFormats = Seq("parquet", "orc")

  // These do not declare it: the parser is handed the columns the scan asked for, so widening the
  // column set changes which records it treats as malformed.
  private val projectionSensitiveFormats = Seq("csv", "json")

  private val flatSchema = "a long, b long, c long, d long"

  private def writeFlat(format: String, path: String, start: Long = 0): Unit =
    spark.range(start, start + 20)
      .selectExpr("id AS a", "id * 2 AS b", "id % 3 AS c", "id * 3 AS d")
      .write.format(format).save(path)

  private def writePartitioned(path: String): Unit =
    spark.range(0, 20)
      .selectExpr("id AS a", "id * 2 AS b", "id % 3 AS c", "id % 4 AS p")
      .write.partitionBy("p").format("parquet").save(path)

  /**
   * Registers `path` as a temp view read through the V2 path, or the V1 path if `useV1`. The view
   * is created inside the `USE_V1_SOURCE_LIST` scope on purpose: a temp view stores its analyzed
   * plan, so which read path it takes is fixed when the view is created, not when it is queried.
   *
   * Not named `withView`: that name is taken by a varargs helper in `QueryCleanupHelper`, which a
   * call with only positional String arguments would silently bind to instead.
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

  private def assertUsesFileSourceV1(df: DataFrame): Unit = {
    val plan = df.queryExecution.optimizedPlan
    assert(plan.collectWithSubqueries { case r: LogicalRelation => r }.nonEmpty,
      s"expected the V1 file source path, but the plan has no V1 relation:\n$plan")
    assert(v2Scans(df).isEmpty, s"expected no DSv2 file scan on the V1 path:\n$plan")
  }

  /** `(SubqueryExec, ReusedSubqueryExec)` counts, the same measure `PlanMergingSuite` uses. */
  private def subqueryCounts(df: DataFrame): (Int, Int) = {
    val plan = df.queryExecution.executedPlan
    val subqueries = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
    val reused = collectWithSubqueries(plan) { case rs: ReusedSubqueryExec => rs.child.id }
    (subqueries.size, reused.size)
  }

  /**
   * Runs `query` over the parquet data at `path` on the V1 or V2 read path, with AQE as given and
   * both symmetric filter propagation configurations on, checks the rows and returns the subquery
   * counts.
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
        if (useV1) assertUsesFileSourceV1(df) else assertUsesFileSourceV2(df)
        subqueryCounts(df)
      }
    }
  }

  test("SPARK-57205: which built-in file tables declare SCAN_MERGING") {
    // Avro is covered in AvroV2Suite, the module that has AvroTable on the classpath.
    Seq("parquet" -> true, "orc" -> true, "text" -> true, "csv" -> false, "json" -> false)
      .foreach { case (format, declares) =>
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
              assert(table.capabilities().contains(TableCapability.SCAN_MERGING) == declares,
                s"${table.getClass.getSimpleName}.capabilities() should " +
                  s"${if (declares) "declare" else "not declare"} SCAN_MERGING")
            }
          }
        }
      }
  }

  test("SPARK-57205: withhold SCAN_MERGING from a table whose reads are not strict") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 10).selectExpr("id AS a", "cast(id AS string) AS b").write.parquet(path)
      // The table is built outside the strictness scope on purpose: the gate is evaluated per call,
      // so it has to answer for the read that is running rather than for the read that built it.
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val relations = spark.read.schema("a long, b long").parquet(path)
          .queryExecution.analyzed.collect { case r: DataSourceV2Relation => r }
        assert(relations.size == 1, s"expected a single DSv2 relation, got $relations")
        val table = relations.head.table
        assert(table.capabilities().contains(TableCapability.SCAN_MERGING),
          "a strict read should declare SCAN_MERGING")
        // Both halves of the strictness predicate, on the table that was built while both were off.
        Seq(SQLConf.IGNORE_CORRUPT_FILES.key, SQLConf.IGNORE_MISSING_FILES.key).foreach { conf =>
          withClue(s"$conf=true: ") {
            withSQLConf(conf -> "true") {
              assert(!table.capabilities().contains(TableCapability.SCAN_MERGING),
                s"$conf should withhold SCAN_MERGING")
            }
          }
        }
      }
    }
  }

  // Separate from the capability test above so that a change breaking the gate names which half it
  // broke: an assertion that aborts on the capability never reports on the rows.
  test("SPARK-57205: a non-strict read keeps its scans separate") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // b is written as a string and read as a long, so the reader fails only once it reads b.
      spark.range(0, 10).selectExpr("id AS a", "cast(id AS string) AS b").write.parquet(path)
      // The scans stay separate, so the a-only scan never reads b and sum(a) is still exact. If
      // they merged, reading b would fail, ignoreCorruptFiles would swallow it and drop the rest of
      // the file, and sum(a) would come back null over rows nothing above the scan removed. The
      // view is registered before the conf is set, so a cached gate would answer from the strict
      // read.
      withFileView("parquet", path, schema = Some("a long, b long")) {
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
          val df = sql("SELECT (SELECT sum(a) FROM t), (SELECT count(b) FROM t)")
          checkAnswer(df, Row(45, 0))
          assertUsesFileSourceV2(df)
          assert(distinctScans(df) == 2,
            s"the two scans should stay separate:\n${df.queryExecution.optimizedPlan}")
        }
      }
    }
  }

  test("SPARK-57205: merge two file scans that differ only in their projected columns") {
    mergingFormats.foreach { format =>
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
            val mergedOutput = v2Scans(df).head.output
            assert(mergedOutput.map(_.name).toSet == Set("a", "b", "c"),
              s"the merged scan should read the union of both columns; got $mergedOutput")
          }
        }
      }
    }
  }

  test("SPARK-57205: merge two text scans that differ only in their projected columns") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq("a", "bb", "ccc").toDS().write.text(path)
      withFileView("text", path) {
        // A text table has the single column `value`, so the only projection difference reachable
        // is an empty read set against `[value]`. Both aggregates have to be hash-aggregatable or
        // PlanMerger's supportedAggregateMerge declines above the scans, before the capability is
        // reached: max(value) over a string is neither hash nor object-hash, sum(length(value)) is.
        val df = sql("SELECT (SELECT count(*) FROM t), (SELECT sum(length(value)) FROM t)")
        checkAnswer(df, Row(3, 6))
        assertUsesFileSourceV2(df)
        assert(distinctScans(df) == 1,
          s"the two scans should be fused into one:\n${df.queryExecution.optimizedPlan}")
        val mergedOutput = v2Scans(df).head.output
        assert(mergedOutput.map(_.name) == Seq("value"),
          s"the merged scan should read value; got $mergedOutput")
      }
    }
  }

  test("SPARK-57205: do not merge CSV or JSON scans that differ in their projected columns") {
    projectionSensitiveFormats.foreach { format =>
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

            checkAnswer(df, Row(70, 140))
            assertUsesFileSourceV2(df)
            // Same shape as the test above, which merges for parquet and orc. These two decline
            // because neither table declares SCAN_MERGING, which is what keeps the union of the
            // columns out of the parser. Both measures are meaningful here: the two scans read
            // different columns, so they do not canonicalize equal either.
            assert(distinctScans(df) == 2,
              s"the two scans should stay separate:\n${df.queryExecution.optimizedPlan}")
            assert(subqueryCounts(df) == ((2, 0)),
              s"unexpected subquery counts:\n${df.queryExecution.executedPlan}")
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
        // not read.
        assert(scan.output.map(_.name).toSet == Set("a", "b"),
          s"the merged scan should read the union of both columns; got ${scan.output}")
        // The rebuilt scan has to carry the filter or it would read all four partitions. Read it
        // off the FileScan rather than off `pushedFilters`, which each unmerged scan records too.
        val partitionFilters = scan.scan match {
          case f: FileScan => f.partitionFilters
          case other => fail(s"expected a FileScan, got ${other.getClass.getSimpleName}")
        }
        assert(partitionFilters.exists(_.references.exists(_.name == "p")),
          s"the merged scan should still enforce the partition filter; got $partitionFilters")
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
        val mergedOutput = v2Scans(df).head.output
        assert(mergedOutput.map(_.name).toSet == Set("a", "b", "c", "d"),
          s"the merged scan should read the union of all three; got $mergedOutput")
      }
    }
  }

  test("SPARK-57205: merge file scans with differing data filters only when dsv2 symmetric " +
    "filter propagation is on") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeFlat("parquet", path)
      withFileView("parquet", path) {
        Seq(true, false).foreach { dsv2Symmetric =>
          withClue(s"dsv2SymmetricFilterPropagation=$dsv2Symmetric: ") {
            // The generic symmetric propagation would enable this merge on its own, so pin it off:
            // the point of the test is that the dsv2 configuration alone decides.
            withSQLConf(
                SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false",
                SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key ->
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
              if (dsv2Symmetric) {
                // The widened predicate has to reach the rebuilt scan, or the merge would keep the
                // answer right through the enclosing Filter while losing the row-group pruning that
                // is the whole point. checkAnswer and the scan count both stay green in that case.
                val dataFilters = v2Scans(df).head.scan match {
                  case f: FileScan => f.dataFilters
                  case other => fail(s"expected a FileScan, got ${other.getClass.getSimpleName}")
                }
                val referenced = dataFilters.flatMap(_.references.map(_.name)).toSet
                assert(referenced == Set("a", "b"),
                  s"the merged scan should carry the widened predicate; got $dataFilters")
              }
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
      withFileView("parquet", path) {
        Seq(true, false).foreach { nestedPruning =>
          withClue(s"nestedSchemaPruning=$nestedPruning: ") {
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
              // field ordinals in the extractors above the scan resolve against the narrowed type.
              // Without pruning both scans read the whole struct and merge on PlanMerger's
              // identical-plan path, which needs no capability. Two whole-struct scans canonicalize
              // equal, so distinctScans cannot tell that merge from a decline; subqueryCounts can.
              val expectedCounts = if (nestedPruning) (2, 0) else (1, 1)
              assert(subqueryCounts(df) == expectedCounts,
                s"unexpected subquery counts:\n${df.queryExecution.executedPlan}")
              if (nestedPruning) {
                assert(distinctScans(df) == 2,
                  s"the pruned scans should stay separate:\n${df.queryExecution.optimizedPlan}")
              }
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
          // Observe the aggregate itself, not just `!mergeableScan`, which several other pushdowns
          // also clear: a decline for one of those reasons must not pass as this one.
          assert(v2Scans(df).forall(_.scan match {
              case p: ParquetScan => p.pushedAggregate.isDefined
              case _ => false
            }),
            s"the aggregate should have been pushed into both scans:\n" +
              df.queryExecution.optimizedPlan)
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
        // Different rows in the second table, so a merge across the two would change the answer and
        // not just the plan shape.
        writeFlat("parquet", dir2.getCanonicalPath, start = 100)
        withFileView("parquet", dir1.getCanonicalPath, viewName = "t1") {
          withFileView("parquet", dir2.getCanonicalPath, viewName = "t2") {
            val df = sql(
              """
                |SELECT
                |  (SELECT sum(a) FROM t1 WHERE c = 1),
                |  (SELECT sum(b) FROM t2 WHERE c = 1)
                |""".stripMargin)

            // c = 1 selects ids 1, 4, ..., 19 in t1 and 100, 103, ..., 118 in t2.
            checkAnswer(df, Row(70, 1526))
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
          assert(mergedCounts(path, query, Row(45, 100), useV1 = true, enableAQE) == ((1, 1)),
            "V1 should merge the two partition filters into one subquery")
          assert(mergedCounts(path, query, Row(45, 100), useV1 = false, enableAQE) == ((2, 0)),
            "V2 should leave the two differing partition filters unmerged")
        }
      }
    }
  }

  test("SPARK-57205: CSV and JSON decline to merge, so their parsing stays per subquery") {
    // The parsers are handed just the columns the scan asked for (for CSV under
    // spark.sql.csv.parser.columnPruning.enabled), so which columns a scan reads decides which
    // records it treats as malformed. Neither table declares SCAN_MERGING, so each subquery keeps
    // its own scan. Every shape below is one where a merged scan would return something else, so
    // adding the capability back to either table fails this test. The V1 path does merge them
    // today, and does return something else, which is SPARK-59107.
    val typeErrorCsv = Seq("0,0", "1,10", "2,BAD", "3,30", "4,40")
    val typeErrorJson = Seq(
      """{"a":0,"b":0}""",
      """{"a":1,"b":10}""",
      """{"a":2,"b":"BAD"}""",
      """{"a":3,"b":30}""",
      """{"a":4,"b":40}""")
    // One token where the schema has two columns. Neither narrow scan is malformed: with column
    // pruning the parsed schema is the projection, so a one-column scan matches a one-token row.
    val shortRowCsv = Seq("0,0", "1,10", "2", "3,30", "4,40")
    val sumQuery =
      """
        |SELECT
        |  (SELECT sum(a) FROM t),
        |  (SELECT sum(b) FROM t)
        |""".stripMargin
    val corruptQuery =
      """
        |SELECT
        |  (SELECT count(_corrupt_record) FROM t WHERE a >= 0),
        |  (SELECT sum(b) FROM t WHERE a >= 0)
        |""".stripMargin

    def withData(lines: Seq[String])(f: String => Unit): Unit =
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        lines.toDS().write.text(path)
        f(path)
      }

    def rows(
        format: String,
        path: String,
        schema: String,
        mode: String,
        query: String): Seq[Row] =
      // Pin what the expectations below depend on rather than rely on the defaults: with CSV column
      // pruning off the parser is handed the full data schema, and with JSON partial results off
      // the malformed record yields an all-null row, which the WHERE then drops.
      withSQLConf(
          SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> "true",
          SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
        withFileView(format, path, schema = Some(schema),
          options = Map("mode" -> mode, "columnNameOfCorruptRecord" -> "_corrupt_record")) {
          val df = sql(query)
          assertUsesFileSourceV2(df)
          val result = df.collect().toSeq
          // Asserted after collect() so that AQE has finalized and a merged subquery's reuse would
          // be visible in the plan. Pinning this alongside the rows attributes them to the merge
          // decision itself.
          assert(subqueryCounts(df) == ((2, 0)),
            s"the two subqueries should keep their own scans:\n${df.queryExecution.executedPlan}")
          result
        }
      }

    Seq("csv" -> typeErrorCsv, "json" -> typeErrorJson).foreach { case (format, lines) =>
      withClue(s"format=$format: ") {
        withData(lines) { path =>
          // DROPMALFORMED. The a-only scan never parses b, so the record survives for sum(a). A
          // merged scan would parse the union and drop it for both, giving 8.
          assert(rows(format, path, "a long, b long", "DROPMALFORMED", sumQuery) ==
            Seq(Row(10, 80)))

          // PERMISSIVE, the default mode, with the corrupt-record column in the schema. The
          // subquery that reads a and the corrupt column does not flag the record; a merged scan
          // would parse b and populate the column for a row that subquery counted as clean.
          assert(rows(format, path, "a long, b long, _corrupt_record string", "PERMISSIVE",
            corruptQuery) == Seq(Row(0, 80)))
        }
      }
    }

    // FAILFAST, CSV only: JSON has no arity check, so a missing field is null, not malformed. Each
    // narrow scan matches the short row, so the query returns rows; a merged scan would parse two
    // columns against a one-token row and throw, turning a working query into an error.
    withData(shortRowCsv) { path =>
      assert(rows("csv", path, "a long, b long", "FAILFAST", sumQuery) == Seq(Row(10, 80)))
    }
  }
}
