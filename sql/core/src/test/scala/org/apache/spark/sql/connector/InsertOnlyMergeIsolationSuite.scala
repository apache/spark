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

import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.expressions.{EqualTo => CatalystEqualTo, GenericInternalRow, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, InsertOnlyMerge, LocalRelation}
import org.apache.spark.sql.connector.catalog.{
  BufferedRows,
  CatalogV2Util,
  Identifier,
  InMemoryRowLevelOperationTable,
  InMemoryTableCatalog,
  Table,
  TableCapability,
  TableInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{
  FieldReference,
  LogicalExpressions,
  SortDirection,
  SortOrder,
  Transform}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  LogicalWriteInfo,
  RequiresDistributionAndOrdering,
  RowLevelOperation,
  RowLevelOperationBuilder,
  RowLevelOperationInfo,
  RowLevelOperationTable,
  V1Write,
  Write,
  WriteBuilder,
  WriterCommitMessage}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{
  AppendDataExecV1,
  BatchScanExec,
  DataSourceV2Relation,
  DataSourceV2ScanRelation,
  DataSourceV2Strategy,
  InsertOnlyMergeExec,
  ReplaceDataExec,
  V2Writes,
  WriteDeltaExec}
import org.apache.spark.sql.sources.{EqualTo, Filter, InsertableRelation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DataSourceV2Test
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests verifying that insert-only MERGE statements preserve the [[RowLevelOperation]]
 * scan-to-write handoff for connectors opting in via
 * [[SupportsRowLevelOperations#newInsertOnlyMergeOperationBuilder]], allowing DSv2 connectors to
 * enforce SERIALIZABLE isolation without sacrificing the LeftAnti append fast path.
 */
@DataSourceV2Test
class InsertOnlyMergeIsolationSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  import testImplicits._

  private val catalogName = "serializable_cat"
  private val plainCatalogName = "plain_cat"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[SerializableInsertOnlyMergeCatalog].getName)
    spark.conf.set(
      s"spark.sql.catalog.$plainCatalogName",
      classOf[InMemoryTableCatalog].getName)
  }

  private def loadTable(name: String): SerializableInsertOnlyMergeTable = {
    val catalog = spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[SerializableInsertOnlyMergeCatalog]
    catalog.loadTable(Identifier.of(Array("ns"), name))
      .asInstanceOf[SerializableInsertOnlyMergeTable]
  }

  Seq(false, true).foreach { supportsDeltas =>
    val mode = if (supportsDeltas) "delta-based" else "group-based"
    val tblProps = if (supportsDeltas) "TBLPROPERTIES ('supports-deltas' = 'true')" else ""

    test(s"$mode: scan-to-write handoff, options, and pushed filters (single & multi)") {
      Seq(
        "single" -> "WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)",
        "multi" ->
          """WHEN NOT MATCHED AND s.id = 3 THEN INSERT (id, dep) VALUES (s.id, 'special')
            |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
            |""".stripMargin
      ).foreach { case (variant, clauses) =>
        val tblName = s"${mode.replace('-', '_')}_$variant"
        val fullName = s"$catalogName.ns.$tblName"
        withTable(fullName) {
          withTempView("src") {
            sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING) PARTITIONED BY (dep) " +
              tblProps)
            sql(s"INSERT INTO $fullName VALUES (1, 'eng'), (2, 'sales')")
            val table = loadTable(tblName)
            table.resetCounters()
            val snapshotBeforeMerge = table.snapshotVersion

            Seq((1, "eng"), (2, "eng"), (3, "eng"), (4, "eng"))
              .toDF("id", "dep")
              .createOrReplaceTempView("src")

            val Seq(qe) = withQueryExecutionsCaptured(spark) {
              sql(
                s"""MERGE INTO $fullName t WITH (`write-opt` = 'v1')
                   |USING src s
                   |ON t.id = s.id AND t.dep = 'eng'
                   |$clauses
                   |""".stripMargin)
            }

            val plan = stripAQEPlan(qe.executedPlan)
            assert(plan.collectFirst { case e: InsertOnlyMergeExec => e }.isDefined)
            assert(plan.collectFirst { case e: ReplaceDataExec => e }.isEmpty)
            assert(plan.collectFirst { case e: WriteDeltaExec => e }.isEmpty)

            assert(table.insertOnlyOpBuilderCalls === 1)
            assert(table.rowLevelOpBuilderCalls === 0)
            assert(table.insertOnlyWriteBuilderCalls === 1)
            assert(table.tableWriteBuilderFallbackCalls === 0)
            assert(table.lastOperationCommand === Command.MERGE)
            assert(table.lastOperationOptions.get("write-opt") === "v1")
            assert(table.lastScanSnapshotVersion === Some(snapshotBeforeMerge))
            assert(table.lastPushedFilters.exists {
              case EqualTo("dep", "eng") => true
              case _ => false
            }, s"expected dep = 'eng' pushed filter, got ${table.lastPushedFilters.mkString(", ")}")
            assert(table.lastInsertOnlyWriteInfo.schema().fieldNames === Array("id", "dep"))
            assert(table.lastInsertOnlyWriteInfo.options().get("write-opt") === "v1")
            assert(!table.lastInsertOnlyWriteInfo.rowIdSchema().isPresent)
            assert(!table.lastInsertOnlyWriteInfo.metadataSchema().isPresent)
            // Untouched partitions (e.g. 'sales') must not be rewritten.
            assert(table.replacedPartitions.isEmpty)

            val expectedRow3 = if (variant == "multi") Row(3, "special") else Row(3, "eng")
            checkAnswer(
              sql(s"SELECT * FROM $fullName"),
              Seq(
                Row(1, "eng"),
                Row(2, "sales"),
                Row(2, "eng"),
                expectedRow3,
                Row(4, "eng")))
          }
        }
      }
    }

    test(s"$mode: concurrent INSERT causes SERIALIZABLE conflict (prevents duplicate keys)") {
      val tblName = s"${mode.replace('-', '_')}_phantom_insert"
      val fullName = s"$catalogName.ns.$tblName"
      withTable(fullName) {
        withTempView("src") {
          sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING) $tblProps")
          sql(s"INSERT INTO $fullName VALUES (1, 'hr')")
          val table = loadTable(tblName)

          // Simulate a concurrent INSERT INTO t VALUES (2, 'sales') committing after the MERGE
          // scan starts and before the MERGE commits.
          table.onBeforeInsertOnlyCommit = Some(() => {
            table.applyConcurrentInsert(2, "sales")
          })

          Seq((2, "eng")).toDF("id", "dep").createOrReplaceTempView("src")

          val ex = intercept[SerializableConflictException] {
            sql(
              s"""MERGE INTO $fullName t
                 |USING src s
                 |ON t.id = s.id
                 |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
                 |""".stripMargin)
          }
          assert(ex.getMessage.contains("conflicting concurrent commit"))
          assert(table.insertOnlyWriteBuilderCalls === 1)
          assert(table.rowLevelOpBuilderCalls === 0)

          // Only the concurrent insert survived; no duplicate id = 2 was written.
          checkAnswer(
            sql(s"SELECT * FROM $fullName"),
            Seq(Row(1, "hr"), Row(2, "sales")))
        }
      }
    }

    test(s"$mode: concurrent DELETE causes SERIALIZABLE conflict (prevents write skew)") {
      val tblName = s"${mode.replace('-', '_')}_write_skew"
      val fullName = s"$catalogName.ns.$tblName"
      withTable(fullName) {
        withTempView("src") {
          sql(s"CREATE TABLE $fullName (id INT NOT NULL, tier STRING) $tblProps")
          sql(s"INSERT INTO $fullName VALUES (1, 'platinum'), (2, 'gold')")
          val table = loadTable(tblName)

          // Simulate a concurrent DELETE FROM t WHERE id IN (2, 9) committing after the MERGE
          // scan starts and before the MERGE commits.
          table.onBeforeInsertOnlyCommit = Some(() => {
            table.applyConcurrentDelete(Set(2, 9))
          })

          Seq((2, "bronze"), (9, "bronze")).toDF("id", "tier").createOrReplaceTempView("src")

          val ex = intercept[SerializableConflictException] {
            sql(
              s"""MERGE INTO $fullName t
                 |USING src s
                 |ON t.id = s.id
                 |WHEN NOT MATCHED THEN INSERT (id, tier) VALUES (s.id, s.tier)
                 |""".stripMargin)
          }
          assert(ex.getMessage.contains("conflicting concurrent commit"))

          // Must NOT produce the non-serializable [(1, 'platinum'), (9, 'bronze')] state.
          checkAnswer(
            sql(s"SELECT * FROM $fullName"),
            Seq(Row(1, "platinum")))
        }
      }
    }

    test(s"$mode: non-conflicting concurrent INSERT outside pushed filter succeeds") {
      val tblName = s"${mode.replace('-', '_')}_no_conflict"
      val fullName = s"$catalogName.ns.$tblName"
      withTable(fullName) {
        withTempView("src") {
          sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING) PARTITIONED BY (dep) " +
            tblProps)
          sql(s"INSERT INTO $fullName VALUES (1, 'eng')")
          val table = loadTable(tblName)

          // Concurrent insert writes to dep = 'sales', disjoint from pushed filter dep = 'eng'.
          table.onBeforeInsertOnlyCommit = Some(() => {
            table.applyConcurrentInsert(10, "sales")
          })

          Seq((1, "eng"), (2, "eng")).toDF("id", "dep").createOrReplaceTempView("src")

          sql(
            s"""MERGE INTO $fullName t
               |USING src s
               |ON t.id = s.id AND t.dep = 'eng'
               |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
               |""".stripMargin)

          checkAnswer(
            sql(s"SELECT * FROM $fullName"),
            Seq(Row(1, "eng"), Row(2, "eng"), Row(10, "sales")))
        }
      }
    }

    test(s"$mode: Optional.empty from newInsertOnlyMergeOperationBuilder falls back to table") {
      val tblName = s"${mode.replace('-', '_')}_fallback"
      val fullName = s"$catalogName.ns.$tblName"
      withTable(fullName) {
        withTempView("src") {
          sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING) $tblProps")
          sql(s"INSERT INTO $fullName VALUES (1, 'hr')")
          val table = loadTable(tblName)
          table.resetCounters()

          Seq((1, "hr"), (2, "eng")).toDF("id", "dep").createOrReplaceTempView("src")

          // Per-query WITH option allows connector to opt out dynamically (e.g. snapshot mode).
          val Seq(qe) = withQueryExecutionsCaptured(spark) {
            sql(
              s"""MERGE INTO $fullName t WITH (`isolation-level` = 'snapshot')
                 |USING src s
                 |ON t.id = s.id
                 |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
                 |""".stripMargin)
          }

          val insertOnly = qe.optimizedPlan.collectFirst {
            case m: InsertOnlyMerge => m
          }.getOrElse(fail("expected InsertOnlyMerge in optimized plan"))
          assert(!insertOnly.table.asInstanceOf[DataSourceV2Relation].table
            .isInstanceOf[RowLevelOperationTable])
          assert(table.insertOnlyOpBuilderCalls === 1)
          assert(table.rowLevelOpBuilderCalls === 0)
          assert(table.insertOnlyWriteBuilderCalls === 0)
          assert(table.tableWriteBuilderFallbackCalls === 1)

          checkAnswer(
            sql(s"SELECT * FROM $fullName"),
            Seq(Row(1, "hr"), Row(2, "eng")))
        }
      }
    }
  }

  test("RequiresDistributionAndOrdering on insert-only RowLevelOperation write is honored") {
    val tblName = "ordered_insert_only"
    val fullName = s"$catalogName.ns.$tblName"
    withTable(fullName) {
      withTempView("src") {
        sql(
          s"""CREATE TABLE $fullName (id INT NOT NULL, dep STRING)
             |PARTITIONED BY (dep)
             |TBLPROPERTIES ('require-distribution-and-ordering' = 'true')
             |""".stripMargin)
        sql(s"INSERT INTO $fullName VALUES (1, 'eng')")
        val table = loadTable(tblName)
        table.resetCounters()

        Seq((1, "eng"), (2, "sales"), (3, "eng")).toDF("id", "dep").createOrReplaceTempView("src")

        val Seq(qe) = withQueryExecutionsCaptured(spark) {
          sql(
            s"""MERGE INTO $fullName t
               |USING src s
               |ON t.id = s.id
               |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
               |""".stripMargin)
        }

        assert(collect(qe.executedPlan) { case s: SortExec => s }.nonEmpty,
          "expected SortExec from RequiresDistributionAndOrdering")
        assert(table.insertOnlyWriteBuilderCalls === 1)
        assert(table.tableWriteBuilderFallbackCalls === 0)

        checkAnswer(
          sql(s"SELECT * FROM $fullName"),
          Seq(Row(1, "eng"), Row(2, "sales"), Row(3, "eng")))
      }
    }
  }

  test("pruned target scan on ON false or empty source falls back to table.newWriteBuilder") {
    val tblName = "pruned_scan_tbl"
    val fullName = s"$catalogName.ns.$tblName"
    withTable(fullName) {
      withTempView("src", "empty_src") {
        sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING)")
        sql(s"INSERT INTO $fullName VALUES (1, 'hr')")
        val table = loadTable(tblName)

        // 1. ON false with non-empty source: PropagateEmptyRelation drops target scan,
        //    so V2Writes falls back to table.newWriteBuilder instead of calling
        //    operation.newWriteBuilder without a configured scan.
        table.resetCounters()
        Seq((2, "eng")).toDF("id", "dep").createOrReplaceTempView("src")
        sql(
          s"""MERGE INTO $fullName t
             |USING src s
             |ON false
             |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
             |""".stripMargin)
        assert(table.insertOnlyWriteBuilderCalls === 0)
        assert(table.tableWriteBuilderFallbackCalls === 1)
        checkAnswer(sql(s"SELECT * FROM $fullName"), Seq(Row(1, "hr"), Row(2, "eng")))

        // 2. Empty LocalRelation source: PropagateEmptyRelation folds LeftAnti join to empty,
        //    so V2Writes falls back cleanly without calling operation.newWriteBuilder.
        table.resetCounters()
        Seq.empty[(Int, String)].toDF("id", "dep").createOrReplaceTempView("empty_src")
        sql(
          s"""MERGE INTO $fullName t
             |USING empty_src s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
             |""".stripMargin)
        assert(table.insertOnlyWriteBuilderCalls === 0)
        assert(table.tableWriteBuilderFallbackCalls === 1)
        checkAnswer(sql(s"SELECT * FROM $fullName"), Seq(Row(1, "hr"), Row(2, "eng")))

        // 3. WHEN NOT MATCHED AND false: Filter(false, src) is pruned in Operator Optimizations,
        //    while PropagateEmptyRelation runs again after Early Filter Push-Down, so
        //    newScanBuilder still runs before operation.newWriteBuilder and the stateful write
        //    succeeds without violating the scan-before-write contract.
        table.resetCounters()
        sql(
          s"""MERGE INTO $fullName t
             |USING src s
             |ON t.id = s.id
             |WHEN NOT MATCHED AND false THEN INSERT (id, dep) VALUES (s.id, s.dep)
             |""".stripMargin)
        assert(table.insertOnlyWriteBuilderCalls === 1)
        assert(table.tableWriteBuilderFallbackCalls === 0)
        checkAnswer(sql(s"SELECT * FROM $fullName"), Seq(Row(1, "hr"), Row(2, "eng")))
      }
    }
  }

  test("DELETE/UPDATE-only connector (default Optional.empty) skips newRowLevelOperationBuilder") {
    val tblName = "delete_update_only"
    val fullName = s"$catalogName.ns.$tblName"
    withTable(fullName) {
      withTempView("src") {
        sql(
          s"""CREATE TABLE $fullName (id INT NOT NULL, dep STRING)
             |TBLPROPERTIES (
             |  'supports-insert-only-merge' = 'false',
             |  'reject-merge-operation' = 'true'
             |)
             |""".stripMargin)
        sql(s"INSERT INTO $fullName VALUES (1, 'hr')")
        val table = loadTable(tblName)
        table.resetCounters()

        Seq((1, "hr"), (2, "eng")).toDF("id", "dep").createOrReplaceTempView("src")

        sql(
          s"""MERGE INTO $fullName t
             |USING src s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
             |""".stripMargin)

        assert(table.rowLevelOpBuilderCalls === 0)
        assert(table.insertOnlyWriteBuilderCalls === 0)
        assert(table.tableWriteBuilderFallbackCalls === 1)

        checkAnswer(
          sql(s"SELECT * FROM $fullName"),
          Seq(Row(1, "hr"), Row(2, "eng")))
      }
    }
  }

  test("V1_BATCH_WRITE + SupportsRowLevelOperations skips RowLevelOperationTable wrapping") {
    val tblName = "v1_batch_write_rlo"
    val fullName = s"$catalogName.ns.$tblName"
    withTable(fullName) {
      withTempView("src") {
        sql(
          s"""CREATE TABLE $fullName (id INT NOT NULL, dep STRING)
             |TBLPROPERTIES ('v1-batch-write' = 'true')
             |""".stripMargin)
        sql(s"INSERT INTO $fullName VALUES (1, 'hr')")
        val table = loadTable(tblName)
        table.resetCounters()

        Seq((1, "hr"), (2, "eng")).toDF("id", "dep").createOrReplaceTempView("src")

        val qes = withQueryExecutionsCaptured(spark) {
          sql(
            s"""MERGE INTO $fullName t
               |USING src s
               |ON t.id = s.id
               |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
               |""".stripMargin)
        }

        assert(qes.exists(qe =>
          stripAQEPlan(qe.executedPlan).collectFirst { case e: AppendDataExecV1 => e }.isDefined))
        assert(table.insertOnlyOpBuilderCalls === 0)
        assert(table.rowLevelOpBuilderCalls === 0)
        assert(table.insertOnlyWriteBuilderCalls === 0)
        assert(table.tableWriteBuilderFallbackCalls === 1)

        checkAnswer(
          sql(s"SELECT * FROM $fullName"),
          Seq(Row(1, "hr"), Row(2, "eng")))
      }
    }
  }

  test("insert-only merge with metadata column in ON strips metadata from writeRelation output") {
    val tblName = "meta_on_cond"
    val fullName = s"$catalogName.ns.$tblName"
    withTable(fullName) {
      withTempView("src") {
        sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING) PARTITIONED BY (dep)")
        sql(s"INSERT INTO $fullName VALUES (1, 'eng'), (2, 'sales')")
        val table = loadTable(tblName)
        table.resetCounters()

        Seq((1, "eng"), (2, "eng"), (3, "eng"))
          .toDF("id", "dep")
          .createOrReplaceTempView("src")

        val Seq(qe) = withQueryExecutionsCaptured(spark) {
          sql(
            s"""MERGE INTO $fullName t
               |USING src s
               |ON t.id = s.id AND t._partition = 'eng'
               |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
               |""".stripMargin)
        }

        val insertOnly = qe.optimizedPlan.collectFirst {
          case m: InsertOnlyMerge => m
        }.getOrElse(fail("expected InsertOnlyMerge in optimized plan"))
        assert(insertOnly.table.output.map(_.name) === Seq("id", "dep"))
        assert(table.insertOnlyWriteBuilderCalls === 1)
        assert(table.rowLevelOpBuilderCalls === 0)
        assert(table.tableWriteBuilderFallbackCalls === 0)
        assert(table.lastInsertOnlyWriteInfo.schema().fieldNames === Array("id", "dep"))

        checkAnswer(
          sql(s"SELECT * FROM $fullName"),
          Seq(Row(1, "eng"), Row(2, "sales"), Row(2, "eng"), Row(3, "eng")))
      }
    }
  }

  test("group-based insert-only RowLevelOperationTable allows runtime filtering") {
    val tblName = "group_based_runtime_filter"
    val fullName = s"$catalogName.ns.$tblName"
    withTable(fullName) {
      sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING) PARTITIONED BY (dep)")
      sql(s"INSERT INTO $fullName VALUES (1, 'eng'), (2, 'sales')")

      val scanRel = sql(s"SELECT * FROM $fullName").queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r
      }.getOrElse(fail("expected DataSourceV2ScanRelation"))
      assert(scanRel.runtimeFilterAttrs.nonEmpty)

      val table = loadTable(tblName)
      val op = table.newInsertOnlyMergeOperationBuilder(
        new RowLevelOperationInfo {
          override def options(): CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()
          override def command(): Command = Command.MERGE
        }).orElseThrow(() => new IllegalStateException("expected insert-only builder")).build()
      val depAttr = scanRel.output.find(_.name == "dep")
        .getOrElse(fail("expected dep attribute in scan output"))
      val scalarSubquery = ScalarSubquery(
        sql(s"SELECT max(dep) FROM $fullName").queryExecution.optimizedPlan)
      val strategy = new DataSourceV2Strategy(spark)

      val regularOpTable = RowLevelOperationTable(table, op, isInsertOnly = false)
      val regularFilter = LogicalFilter(
        CatalystEqualTo(depAttr, scalarSubquery),
        scanRel.copy(relation = scanRel.relation.copy(table = regularOpTable)))
      val regularScan = strategy.apply(regularFilter).head.collectFirst {
        case b: BatchScanExec => b
      }.getOrElse(fail("expected BatchScanExec"))
      assert(regularScan.runtimeFilters.isEmpty,
        "regular group-based RowLevelOperationTable must suppress scalar subquery runtime filters")

      val insertOnlyOpTable = RowLevelOperationTable(table, op, isInsertOnly = true)
      val insertOnlyFilter = LogicalFilter(
        CatalystEqualTo(depAttr, scalarSubquery),
        scanRel.copy(relation = scanRel.relation.copy(table = insertOnlyOpTable)))
      val insertOnlyScan = strategy.apply(insertOnlyFilter).head.collectFirst {
        case b: BatchScanExec => b
      }.getOrElse(fail("expected BatchScanExec"))
      assert(insertOnlyScan.runtimeFilters.nonEmpty,
        "insert-only group-based RowLevelOperationTable must allow scalar subquery runtime filters")
    }
  }

  test("V2Writes builds InsertOnlyMerge write for plain V2 table without RowLevelOperationTable") {
    val fullName = s"$plainCatalogName.ns.plain_tbl"
    withTable(fullName) {
      sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING)")
      val plainCatalog = spark.sessionState.catalogManager
        .catalog(plainCatalogName)
        .asInstanceOf[InMemoryTableCatalog]
      val plainTable = plainCatalog.loadTable(Identifier.of(Array("ns"), "plain_tbl"))
      val relation = DataSourceV2Relation.create(plainTable, None, None)
      val query = LocalRelation(relation.output)
      val plan = InsertOnlyMerge(relation, query, relation)
      val written = V2Writes(plan).asInstanceOf[InsertOnlyMerge]
      assert(written.write.isDefined)
    }
  }

  test("insert-only merge with UNION ALL, DISTINCT, and GROUP BY sources keeps single scan") {
    val tblName = "union_distinct_src"
    val fullName = s"$catalogName.ns.$tblName"
    withTable(fullName) {
      withTempView("s1", "s2") {
        sql(s"CREATE TABLE $fullName (id INT NOT NULL, dep STRING) PARTITIONED BY (dep)")
        sql(s"INSERT INTO $fullName VALUES (1, 'eng'), (2, 'sales')")
        val table = loadTable(tblName)

        Seq((1, "eng"), (3, "eng"), (3, "eng")).toDF("id", "dep").createOrReplaceTempView("s1")
        Seq((2, "sales"), (4, "sales")).toDF("id", "dep").createOrReplaceTempView("s2")

        // (a) UNION ALL source must not clone RowLevelOperationTable across branches or bypass
        //     conflict detection on branch 1 ('eng').
        table.resetCounters()
        table.onBeforeInsertOnlyCommit = Some(() => table.applyConcurrentInsert(10, "eng"))
        intercept[SerializableConflictException] {
          sql(
            s"""MERGE INTO $fullName t
               |USING (
               |  SELECT id, dep FROM s1 WHERE dep = 'eng'
               |  UNION ALL
               |  SELECT id, dep FROM s2 WHERE dep = 'sales'
               |) s
               |ON t.id = s.id AND t.dep = s.dep
               |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
               |""".stripMargin)
        }
        assert(table.scanBuilderCalls === 1)
        assert(table.insertOnlyWriteBuilderCalls === 1)

        // (b) UNION ALL source without concurrent conflicts succeeds with a single target scan.
        table.resetCounters()
        table.onBeforeInsertOnlyCommit = None
        sql(
          s"""MERGE INTO $fullName t
             |USING (
             |  SELECT id, dep FROM s1 WHERE dep = 'eng' AND id = 1
             |  UNION ALL
             |  SELECT id, dep FROM s2 WHERE dep = 'sales'
             |) s
             |ON t.id = s.id AND t.dep = s.dep
             |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
             |""".stripMargin)
        assert(table.scanBuilderCalls === 1)
        assert(table.insertOnlyWriteBuilderCalls === 1)

        // (c) DISTINCT source must not call DataSourceV2Relation.computeStats() before pushdown.
        table.resetCounters()
        sql(
          s"""MERGE INTO $fullName t
             |USING (SELECT DISTINCT id, dep FROM s1) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
             |""".stripMargin)
        assert(table.scanBuilderCalls === 1)
        assert(table.insertOnlyWriteBuilderCalls === 1)

        // (d) GROUP BY source must not call DataSourceV2Relation.computeStats() before pushdown.
        table.resetCounters()
        sql(
          s"""MERGE INTO $fullName t
             |USING (SELECT id + 100 AS id, dep FROM s2 GROUP BY id, dep) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.id, s.dep)
             |""".stripMargin)
        assert(table.scanBuilderCalls === 1)
        assert(table.insertOnlyWriteBuilderCalls === 1)

        checkAnswer(
          sql(s"SELECT * FROM $fullName"),
          Seq(
            Row(1, "eng"),
            Row(2, "sales"),
            Row(3, "eng"),
            Row(4, "sales"),
            Row(10, "eng"),
            Row(102, "sales"),
            Row(104, "sales")))
      }
    }
  }
}

private class SerializableConflictException(message: String) extends RuntimeException(message)

private case class RecordedCommit(version: Long, affectedDeps: Set[String])

class SerializableInsertOnlyMergeCatalog extends InMemoryTableCatalog {
  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    val tableName = s"$name.${ident.quoted}"
    val table = new SerializableInsertOnlyMergeTable(
      tableName,
      CatalogV2Util.v2ColumnsToStructType(tableInfo.columns),
      tableInfo.partitions,
      tableInfo.properties,
      tableInfo.constraints)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}

class SerializableInsertOnlyMergeTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    constraints: Array[Constraint])
  extends InMemoryRowLevelOperationTable(name, schema, partitioning, properties, constraints) {

  private val versionCounter = new AtomicLong(0L)
  private val commitHistory = ArrayBuffer.empty[RecordedCommit]

  private val optInInsertOnly =
    properties.getOrDefault("supports-insert-only-merge", "true") == "true"
  private val rejectMergeOperation =
    properties.getOrDefault("reject-merge-operation", "false") == "true"
  private val requireDistAndOrder =
    properties.getOrDefault("require-distribution-and-ordering", "false") == "true"
  private val useV1BatchWrite =
    properties.getOrDefault("v1-batch-write", "false") == "true"

  var onBeforeInsertOnlyCommit: Option[() => Unit] = None
  var rowLevelOpBuilderCalls: Int = 0
  var insertOnlyOpBuilderCalls: Int = 0
  var scanBuilderCalls: Int = 0
  var insertOnlyWriteBuilderCalls: Int = 0
  var tableWriteBuilderFallbackCalls: Int = 0
  var lastOperationCommand: Command = _
  var lastOperationOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()
  var lastScanSnapshotVersion: Option[Long] = None
  var lastPushedFilters: Array[Filter] = Array.empty
  var lastInsertOnlyWriteInfo: LogicalWriteInfo = _

  def snapshotVersion: Long = versionCounter.get()

  def resetCounters(): Unit = {
    rowLevelOpBuilderCalls = 0
    insertOnlyOpBuilderCalls = 0
    scanBuilderCalls = 0
    insertOnlyWriteBuilderCalls = 0
    tableWriteBuilderFallbackCalls = 0
  }

  override def capabilities(): util.Set[TableCapability] = {
    if (useV1BatchWrite) {
      util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.V1_BATCH_WRITE)
    } else {
      super.capabilities()
    }
  }

  private def recordCommit(affectedDeps: Set[String]): Unit = dataMap.synchronized {
    val v = versionCounter.incrementAndGet()
    commitHistory += RecordedCommit(v, affectedDeps)
  }

  def applyConcurrentInsert(id: Int, secondCol: String): Unit = dataMap.synchronized {
    val row = new GenericInternalRow(Array[Any](id, UTF8String.fromString(secondCol)))
    val key = getKey(row)
    val buffer = new BufferedRows(key, schema)
    buffer.rows.append(row)
    withData(Array(buffer))
    recordCommit(Set(secondCol))
  }

  def applyConcurrentDelete(idsToDelete: Set[Int]): Unit = dataMap.synchronized {
    val affected = ArrayBuffer.empty[String]
    val updated = dataMap.toSeq.map { case (key, splits) =>
      val newSplits = splits.map { split =>
        val kept = new BufferedRows(split.key, split.schema)
        split.rows.foreach { row =>
          if (idsToDelete.contains(row.getInt(0))) {
            affected += row.getUTF8String(1).toString
          } else {
            kept.rows.append(row.copy())
          }
        }
        kept
      }.filter(_.rows.nonEmpty)
      key -> newSplits
    }.filter(_._2.nonEmpty)
    dataMap.clear()
    dataMap ++= updated
    recordCommit(affected.toSet)
  }

  private def validateSerializable(
      startVersion: Long,
      pushedFilters: Array[Filter]): Unit = dataMap.synchronized {
    val concurrentCommits = commitHistory.filter(_.version > startVersion)
    if (concurrentCommits.nonEmpty) {
      val depFilters = pushedFilters.collect {
        case EqualTo("dep", value: String) => value
      }.toSet

      val hasConflict = if (depFilters.isEmpty) {
        true
      } else {
        concurrentCommits.exists(c => c.affectedDeps.exists(depFilters.contains))
      }

      if (hasConflict) {
        throw new SerializableConflictException(
          s"Found conflicting concurrent commit(s) after snapshot $startVersion " +
            s"(current: ${versionCounter.get()})")
      }
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    tableWriteBuilderFallbackCalls += 1
    if (useV1BatchWrite) {
      new WriteBuilder {
        override def build(): V1Write = new V1Write {
          override def toInsertableRelation: InsertableRelation = {
            (data: DataFrame, _: Boolean) => {
              data.collect().foreach { row =>
                applyConcurrentInsert(row.getInt(0), row.getString(1))
              }
            }
          }
        }
      }
    } else {
      val delegate = super.newWriteBuilder(info)
      new WriteBuilder {
        override def build(): Write = {
          val w = delegate.build()
          new Write {
            override def toBatch: BatchWrite = new Append(info) {
              override protected def doCommit(messages: Array[WriterCommitMessage]): Unit = {
                val buffers = messages.map(_.asInstanceOf[BufferedRows])
                val deps = buffers.flatMap(_.rows.map(_.getUTF8String(1).toString)).toSet
                super.doCommit(messages)
                recordCommit(deps)
              }
            }
            override def description(): String = w.description()
          }
        }
      }
    }
  }

  private case class CapturedScanState(snapshotVersion: Long, pushedFilters: Array[Filter])

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    rowLevelOpBuilderCalls += 1
    if (rejectMergeOperation && info.command() == Command.MERGE) {
      throw new UnsupportedOperationException("MERGE is not supported")
    }
    super.newRowLevelOperationBuilder(info)
  }

  override def newInsertOnlyMergeOperationBuilder(
      info: RowLevelOperationInfo): Optional[RowLevelOperationBuilder] = {
    insertOnlyOpBuilderCalls += 1
    if (!optInInsertOnly || info.options().get("isolation-level") == "snapshot") {
      Optional.empty()
    } else {
      lastOperationCommand = info.command()
      lastOperationOptions = info.options()
      Optional.of(() => new SerializableInsertOnlyMergeOperation(info.command()))
    }
  }

  private class SerializableInsertOnlyMergeOperation(val command: Command)
    extends RowLevelOperation {

    private var capturedScan: Option[CapturedScanState] = None

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      scanBuilderCalls += 1
      val startVersion = snapshotVersion
      new InMemoryScanBuilder(schema, options) {
        override def build(): Scan = {
          val scan = super.build()
          capturedScan = Some(CapturedScanState(startVersion, pushedFilters()))
          scan
        }
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      insertOnlyWriteBuilderCalls += 1
      lastInsertOnlyWriteInfo = info
      val scanState = capturedScan.getOrElse(
        throw new IllegalStateException(
          "Scan must be configured via newScanBuilder before newWriteBuilder"))
      lastScanSnapshotVersion = Some(scanState.snapshotVersion)
      lastPushedFilters = scanState.pushedFilters

      val batchWrite = new Append(info) {
        override protected def doCommit(messages: Array[WriterCommitMessage]): Unit = {
          onBeforeInsertOnlyCommit.foreach { hook =>
            onBeforeInsertOnlyCommit = None
            hook()
          }
          validateSerializable(scanState.snapshotVersion, scanState.pushedFilters)
          val buffers = messages.map(_.asInstanceOf[BufferedRows])
          val deps = buffers.flatMap(_.rows.map(_.getUTF8String(1).toString)).toSet
          super.doCommit(messages)
          recordCommit(deps)
        }
      }

      new WriteBuilder {
        override def build(): Write = if (requireDistAndOrder) {
          new Write with RequiresDistributionAndOrdering {
            override def requiredDistribution(): Distribution =
              Distributions.clustered(Array(FieldReference("dep")))

            override def requiredOrdering(): Array[SortOrder] = Array(
              LogicalExpressions.sort(
                FieldReference("id"),
                SortDirection.ASCENDING,
                SortDirection.ASCENDING.defaultNullOrdering()))

            override def toBatch: BatchWrite = batchWrite
            override def description(): String = "OrderedSerializableInsertOnlyMergeWrite"
          }
        } else {
          new Write {
            override def toBatch: BatchWrite = batchWrite
            override def description(): String = "SerializableInsertOnlyMergeWrite"
          }
        }
      }
    }
  }
}
