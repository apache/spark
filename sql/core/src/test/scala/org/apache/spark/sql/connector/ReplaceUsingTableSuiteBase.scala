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

import org.apache.spark.internal.config
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.connector.catalog.{InMemoryRowLevelOperationTable, InMemoryTable}
import org.apache.spark.sql.connector.write.{BatchWrite, ReplaceSummary, ReplaceSummaryImpl, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{ReplaceDataExec, WriteDeltaExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Shared end-to-end coverage for `INSERT INTO ... REPLACE USING` execution.
 *
 * Scoped replace deletes every target row whose scope-column tuple appears in the source and
 * appends all source rows. Concrete subclasses fix the table layout to a group-based
 * (copy-on-write) or delta-based (merge-on-read) row-level operation. Assertions are written
 * against final table contents so they hold for both backends.
 */
abstract class ReplaceUsingTableSuiteBase extends RowLevelOperationSuiteBase {

  import testImplicits._

  private val schemaString = "pk INT NOT NULL, id INT, dep STRING"

  protected def isDeltaBasedReplace: Boolean = false

  // Select the in-memory table subclass that implements SupportsRowLevelReplace via props.
  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put(InMemoryRowLevelOperationTable.SUPPORTS_ROW_LEVEL_REPLACE, "true")
    props
  }

  test("replace using deletes matching scopes and inserts all source rows") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
         |SELECT * FROM VALUES (10, 10, 'hr'), (11, 11, 'hr') AS t(pk, id, dep)
         |""".stripMargin)

    // Both `hr` target rows are removed, the unrelated `software` row is untouched, and every
    // source row (including two sharing the `hr` scope) is appended.
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "software") :: Row(10, 10, "hr") :: Row(11, 11, "hr") :: Nil)
  }

  test("replace using still works after an ALTER TABLE that preserves the opt-in") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    // Altering an unrelated property must not rebuild the table as the unmarked base class, which
    // would cause the REPLACE USING below to fail the analysis gate.
    sql(s"ALTER TABLE $tableNameAsString SET TBLPROPERTIES ('comment' = 'updated')")

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
         |SELECT * FROM VALUES (10, 10, 'hr') AS t(pk, id, dep)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "software") :: Row(10, 10, "hr") :: Nil)
  }

  test("replace using leaves scopes absent from the source untouched") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
         |SELECT * FROM VALUES (20, 20, 'finance') AS t(pk, id, dep)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 1, "hr") :: Row(2, 2, "software") :: Row(20, 20, "finance") :: Nil)
  }

  test("replace using scope can differ from the data grouping columns") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 1, "dep": "finance" }
        |""".stripMargin)

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (id)
         |SELECT * FROM VALUES (10, 1, 'legal') AS t(pk, id, dep)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "software") :: Row(10, 1, "legal") :: Nil)
  }

  test("replace using matches a multi-column scope as a full tuple") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 1, "dep": "software" }
        |{ "pk": 3, "id": 2, "dep": "hr" }
        |{ "pk": 4, "id": 2, "dep": "finance" }
        |""".stripMargin)

    // Scope is the (id, dep) tuple. Only the row whose full tuple equals the source tuple
    // (1, 'hr') is replaced; rows that match just one of the two scope columns are preserved,
    // confirming the scope predicate is an AND of all columns rather than a per-column match.
    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (id, dep)
         |SELECT * FROM VALUES (10, 1, 'hr') AS t(pk, id, dep)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 1, "software") :: Row(3, 2, "hr") :: Row(4, 2, "finance") :: Row(10, 1, "hr") :: Nil)
  }

  test("replace using matches null scope values with EqualNullSafe") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": null }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
         |SELECT * FROM VALUES (30, 30, CAST(NULL AS STRING)) AS t(pk, id, dep)
         |""".stripMargin)

    // The existing null-scope row is replaced by the null-scope source row.
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "software") :: Row(30, 30, null) :: Nil)
  }

  test("replace using with an empty source deletes nothing and inserts nothing") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
         |SELECT * FROM VALUES (0, 0, 'x') AS t(pk, id, dep) WHERE 1 = 0
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 1, "hr") :: Row(2, 2, "software") :: Nil)
  }

  test("replace using aligns the source by position") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
         |SELECT * FROM VALUES (40, 40, 'hr') AS t(a, b, c)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(40, 40, "hr") :: Nil)
  }

  test("replace using exposes expected connector write schema and rows") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "hr" }
        |{ "pk": 3, "id": 3, "dep": "software" }
        |""".stripMargin)

    sql(
      s"""INSERT INTO $tableNameAsString REPLACE USING (id)
         |SELECT * FROM VALUES (10, 1, 'legal'), (11, 4, 'finance') AS t(pk, id, dep)
         |""".stripMargin)

    if (isDeltaBasedReplace) {
      checkLastWriteInfo(
        expectedRowSchema = StructType(table.schema.map(_.copy(nullable = false))),
        expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
        expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

      checkLastWriteLog(
        deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
        insertWriteLogEntry(data = Row(10, 1, "legal")),
        insertWriteLogEntry(data = Row(11, 4, "finance")))
    } else {
      checkLastWriteInfo(
        expectedRowSchema = table.schema,
        expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD))))

      checkLastWriteLog(
        writeWithMetadataLogEntry(metadata = Row("hr", 1), data = Row(2, 2, "hr")),
        writeLogEntry(data = Row(10, 1, "legal")),
        writeLogEntry(data = Row(11, 4, "finance")))
    }
  }

  test("replace using aligns the source by name") {
    withTempView("source") {
      createAndInitTable(schemaString,
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |""".stripMargin)

      // BY NAME reorders the source columns to the target layout: (pk, id, dep).
      sql("SELECT 'hr' AS dep, 50 AS id, 41 AS pk").createOrReplaceTempView("source")

      sql(
        s"""INSERT INTO $tableNameAsString BY NAME REPLACE USING (dep)
           |SELECT * FROM source
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(41, 50, "hr") :: Nil)
    }
  }

  test("replace using rejects a non-deterministic source") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
             |SELECT * FROM VALUES (1, 1, 'hr') AS t(pk, id, dep) WHERE rand() > 0.5
             |""".stripMargin)
      },
      condition = "INSERT_REPLACE_USING_NON_DETERMINISTIC_SOURCE",
      parameters = Map.empty[String, String])
  }

  test("replace using rejects an unknown scope column") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (missing)
             |SELECT * FROM VALUES (1, 1, 'hr') AS t(pk, id, dep)
             |""".stripMargin)
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`missing`",
        "proposal" -> "`pk`, `id`, `dep`"))
  }

  test("replace using rejects a duplicate scope column") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep, dep)
             |SELECT * FROM VALUES (1, 1, 'hr') AS t(pk, id, dep)
             |""".stripMargin)
      },
      condition = "INSERT_REPLACE_USING_DUPLICATE_SCOPE_COLUMN",
      parameters = Map("columnName" -> "`dep`"))
  }

  test("replace using rejects a case-insensitive duplicate scope column") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      createAndInitTable(schemaString,
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |""".stripMargin)

      // Under case-insensitive resolution `dep` and `DEP` resolve to the same target column, so
      // the pair must be flagged as a duplicate scope column rather than silently deduplicated.
      checkError(
        exception = intercept[AnalysisException] {
          sql(
            s"""INSERT INTO $tableNameAsString REPLACE USING (dep, DEP)
               |SELECT * FROM VALUES (1, 1, 'hr') AS t(pk, id, dep)
               |""".stripMargin)
        },
        condition = "INSERT_REPLACE_USING_DUPLICATE_SCOPE_COLUMN",
        parameters = Map("columnName" -> "`DEP`"))
    }
  }

  test("replace using resolves duplicate scope columns with the active resolver") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val caseSensitiveSchema = "pk INT NOT NULL, id INT, dep STRING, Dep STRING"
      createAndInitTable(caseSensitiveSchema,
        """{ "pk": 1, "id": 1, "dep": "lower", "Dep": "upper-a" }
          |{ "pk": 2, "id": 2, "dep": "lower", "Dep": "upper-b" }
          |""".stripMargin)

      sql(
        s"""INSERT INTO $tableNameAsString REPLACE USING (Dep, dep)
           |SELECT * FROM VALUES (10, 10, 'lower', 'upper-a') AS t(pk, id, dep, Dep)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, "lower", "upper-b") :: Row(10, 10, "lower", "upper-a") :: Nil)
    }
  }

  test("replace using populates a runtime group filter only when enabled") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true") {
      val (_, groupFilterCond) = executeAndKeepConditions {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
             |SELECT * FROM VALUES (3, 3, 'hr') AS t(pk, id, dep)
             |""".stripMargin)
      }
      assert(groupFilterCond.isDefined, "scoped replace must populate a runtime group filter")
    }

    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "false") {
      val (_, groupFilterCond) = executeAndKeepConditions {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
             |SELECT * FROM VALUES (4, 4, 'hr') AS t(pk, id, dep)
             |""".stripMargin)
      }
      assert(groupFilterCond.isEmpty, "no group filter when runtime filtering is disabled")
    }
  }

  private def getReplaceSummary(): ReplaceSummary = {
    val t = catalog.loadTable(ident).asInstanceOf[InMemoryTable]
    t.commits.last.writeSummary.get.asInstanceOf[ReplaceSummary]
  }

  // numCopiedRows reports rows physically re-emitted by copy-on-write replacement. Merge-on-read
  // replacement records deletes and inserts without copying target rows.
  private def checkReplaceSummary(
      numInsertedRows: Long,
      numDeletedRows: Long,
      numCopiedRows: Long): Unit = {
    val summary = getReplaceSummary()
    assert(summary.numInsertedRows() === numInsertedRows,
      s"Expected numInsertedRows=$numInsertedRows, got ${summary.numInsertedRows()}")
    assert(summary.numDeletedRows() === numDeletedRows,
      s"Expected numDeletedRows=$numDeletedRows, got ${summary.numDeletedRows()}")
    val expectedCopied = if (isDeltaBasedReplace) 0L else numCopiedRows
    assert(summary.numCopiedRows() === expectedCopied,
      s"Expected numCopiedRows=$expectedCopied, got ${summary.numCopiedRows()}")
  }

  private def replaceWriteMetrics(plan: SparkPlan): Map[String, SQLMetric] = {
    collectFirst(plan) {
      case e: ReplaceDataExec => e.metrics
      case e: WriteDeltaExec => e.metrics
    }.getOrElse(fail("expected a ReplaceDataExec or WriteDeltaExec in the executed plan"))
  }

  private def assertWriteMetric(
      metrics: Map[String, SQLMetric],
      name: String,
      expected: Long): Unit = {
    val metric = metrics.getOrElse(name, fail(s"missing SQL UI metric: $name"))
    assert(metric.value === expected, s"Expected $name=$expected, got ${metric.value}")
  }

  test("replace using reports summary and SQL UI metrics for matching scopes") {
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true") {
      createAndInitTable(schemaString,
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "software" }
          |{ "pk": 3, "id": 3, "dep": "hr" }
          |""".stripMargin)

      val plan = executeAndKeepPlan {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
             |SELECT * FROM VALUES (10, 10, 'hr'), (11, 11, 'hr') AS t(pk, id, dep)
             |""".stripMargin)
      }

      // Runtime group filtering limits the COW scan to the matched group, so no rows are copied.
      checkReplaceSummary(numInsertedRows = 2, numDeletedRows = 2, numCopiedRows = 0)
      val metrics = replaceWriteMetrics(plan)
      assertWriteMetric(metrics, "numInsertedRows", 2)
      assertWriteMetric(metrics, "numDeletedRows", 2)
      assertWriteMetric(metrics, "numCopiedRows", 0)
    }
  }

  test("replace using reports copied rows for copy-on-write carryover") {
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "false") {
      createAndInitTable(schemaString,
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "software" }
          |{ "pk": 3, "id": 3, "dep": "hr" }
          |""".stripMargin)

      val plan = executeAndKeepPlan {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
             |SELECT * FROM VALUES (10, 10, 'hr'), (11, 11, 'hr') AS t(pk, id, dep)
             |""".stripMargin)
      }

      checkReplaceSummary(numInsertedRows = 2, numDeletedRows = 2, numCopiedRows = 1)
      val metrics = replaceWriteMetrics(plan)
      assertWriteMetric(metrics, "numInsertedRows", 2)
      assertWriteMetric(metrics, "numDeletedRows", 2)
      assertWriteMetric(metrics, "numCopiedRows", if (isDeltaBasedReplace) 0 else 1)
    }
  }

  test("replace using reports inserts only when no scope matches") {
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true") {
      createAndInitTable(schemaString,
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "software" }
          |""".stripMargin)

      val plan = executeAndKeepPlan {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
             |SELECT * FROM VALUES (20, 20, 'finance') AS t(pk, id, dep)
             |""".stripMargin)
      }

      checkReplaceSummary(numInsertedRows = 1, numDeletedRows = 0, numCopiedRows = 0)
      val metrics = replaceWriteMetrics(plan)
      assertWriteMetric(metrics, "numInsertedRows", 1)
      assertWriteMetric(metrics, "numDeletedRows", 0)
      assertWriteMetric(metrics, "numCopiedRows", 0)
    }
  }

  test("replace using reports all-zero metrics for an empty source") {
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true") {
      createAndInitTable(schemaString,
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "software" }
          |""".stripMargin)

      val plan = executeAndKeepPlan {
        sql(
          s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
             |SELECT * FROM VALUES (0, 0, 'x') AS t(pk, id, dep) WHERE 1 = 0
             |""".stripMargin)
      }

      checkReplaceSummary(numInsertedRows = 0, numDeletedRows = 0, numCopiedRows = 0)
      val metrics = replaceWriteMetrics(plan)
      assertWriteMetric(metrics, "numInsertedRows", 0)
      assertWriteMetric(metrics, "numDeletedRows", 0)
      assertWriteMetric(metrics, "numCopiedRows", 0)
    }
  }

  test("replace metric values are stable across scan-stage retries") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("source") {
        createAndInitTable(schemaString,
          """{ "pk": 1, "id": 1, "dep": "hr" }
            |{ "pk": 2, "id": 2, "dep": "software" }
            |{ "pk": 3, "id": 3, "dep": "hr" }
            |{ "pk": 4, "id": 4, "dep": "software" }
            |""".stripMargin)

        Seq((10, 1, "legal"), (20, 2, "finance")).toDF("pk", "id", "dep")
          .createOrReplaceTempView("source")

        withSparkContextConf(config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> "true") {
          sql(
            s"""INSERT INTO $tableNameAsString REPLACE USING (id)
               |SELECT * FROM source
               |""".stripMargin)
        }

        checkReplaceSummary(numInsertedRows = 2, numDeletedRows = 2, numCopiedRows = 2)
      }
    }
  }

  test("replace summary commit falls back to the no-summary commit for legacy connectors") {
    // Connectors that do not override commit(messages, WriteSummary) use BatchWrite's default
    // delegation.
    val committedMessages =
      new java.util.concurrent.atomic.AtomicReference[Array[WriterCommitMessage]]

    val legacyWrite = new BatchWrite {
      override def createBatchWriterFactory(
          info: org.apache.spark.sql.connector.write.PhysicalWriteInfo)
        : org.apache.spark.sql.connector.write.DataWriterFactory = {
        throw new UnsupportedOperationException("not needed for this test")
      }

      override def commit(messages: Array[WriterCommitMessage]): Unit = {
        committedMessages.set(messages)
      }

      override def abort(messages: Array[WriterCommitMessage]): Unit = {}
    }

    val summary: WriteSummary = ReplaceSummaryImpl(
      numInsertedRows = 5L, numDeletedRows = 2L, numCopiedRows = 0L)

    val messages = Array.empty[WriterCommitMessage]
    legacyWrite.commit(messages, summary)

    assert(committedMessages.get() != null, "default commit(messages, summary) must delegate")
    assert(committedMessages.get() eq messages, "the original commit messages must be forwarded")
  }
}
