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

import org.apache.spark.{SparkException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.ORIGINAL_ROW_ID_VALUE_PREFIX
import org.apache.spark.sql.connector.catalog.{Delete, InMemoryBaseTable, InMemoryRowLevelOperationTable, Reinsert}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperationTable}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2Writes}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for UPDATE statements targeting connectors that mix in
 * [[org.apache.spark.sql.connector.write.SupportsColumnUpdates]].
 *
 * When a connector supports column updates, updated rows contain only the declared columns
 * (LogicalWriteInfo.columnUpdateSchema()) rather than the full table row.
 */
class DeltaBasedColumnUpdateTableSuite extends DeltaBasedUpdateTableSuiteBase
  with ColumnUpdateTableSharedTests {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update", "true")
    props
  }

  override protected def reqAttrsTableProps(reqAttrs: String): java.util.Map[String, String] = {
    java.util.Map.of("column-update-req-attrs", reqAttrs)
  }

  override protected def fullWidthTableProps: java.util.Map[String, String] = {
    java.util.Map.of("supports-deltas", "true")
  }

  override protected def scanMetadataColumns: String = ", _partition STRING"

  private val reqAttrsOperationClass =
    classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateOperationWithReqAttrs].getName

  private val splitOperationClass =
    classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateSplitOperation].getName

  private val splitReqAttrsOperationClass =
    classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateSplitOperationWithReqAttrs]
      .getName

  private val splitWithRowIdOperationClass =
    classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateSplitOperationWithRowId].getName

  test("column-update: column update schema contains a single assigned column") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        DEP_FIELD,
        StructField("id", IntegerType, nullable = false)
      ))))
  }

  test("column-update: column update schema contains multiple assigned columns") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1, dep = 'engineering' WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("dep", StringType, nullable = false),
        StructField("id", IntegerType, nullable = false)
      ))))
  }

  test("column-update: column update schema is pk and dep for a full identity update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = dep WHERE pk = 1")

    // All assignments are identity, so updatedColumns is empty; pk and dep remain because the
    // connector declares them as base columns.
    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Array(PK_FIELD, DEP_FIELD))))
  }

  test("column-update: row filter condition is orthogonal to column narrowing") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET dep = 'engineering' WHERE pk IN (1, 3)")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("dep", StringType, nullable = false)
      ))))
  }

  test("column-update: update all rows (no WHERE clause)") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        DEP_FIELD,
        StructField("salary", IntegerType, nullable = true)
      ))))
  }

  test("column-update: column update schema excludes identity assignments in a mixed UPDATE") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("dep", StringType, nullable = false)
      ))))
  }

  test("column-update: CHAR and VARCHAR self-assignments are reported as updated") {
    createTable("pk INT NOT NULL, id INT, c CHAR(3), v VARCHAR(10), dep STRING")
    sql(s"INSERT INTO $tableNameAsString VALUES (1, 1, 'a', 'x', 'hr')")

    sql(s"UPDATE $tableNameAsString SET id = id, c = c, v = v WHERE pk = 1")

    checkLastUpdatedColumns("c", "v")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 1, "a  ", "x", "hr") :: Nil)
  }

  test("column-update: a LogicalWriteInfo that does not implement columnUpdateSchema throws") {
    val info = new LogicalWriteInfo {
      override def options(): CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()
      override def queryId(): String = "query"
      override def schema(): StructType = new StructType()
    }

    checkError(
      exception = intercept[SparkUnsupportedOperationException](info.columnUpdateSchema()),
      condition = "DATA_SOURCE_COLUMN_UPDATE_SCHEMA_NOT_IMPLEMENTED",
      parameters = Map("class" -> info.getClass.getName))
  }

  test("column-update: updatedColumns is empty for DELETE") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE dep = 'hr'")

    checkLastUpdatedColumns()
  }

  test("column-update split: row-level DELETE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-split", "true"))
    checkDeleteFallsBackToFullWidthRows(Row(2, 200, "hr"))
  }

  test("column-update split: row-level DELETE by a metadata row ID falls back to full-width " +
    "rows") {
    // Each partition holds one row, so `_partition` identifies it.
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      java.util.Map.of("column-update-split-row-id", "_partition"))
    checkDeleteFallsBackToFullWidthRows(Row(2, 200, "software"))
  }

  test("column-update split: MERGE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      java.util.Map.of("column-update-split", "true"))
    checkMergeFallsBackToFullWidthRows()
  }

  test("column-update: data correctness -- single column update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)
  }

  test("column-update: data correctness -- update all rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 200, "hr") :: Row(2, 400, "software") :: Row(3, 600, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 3, numCopiedRows = 0)
  }

  test("column-update: data correctness -- mixed identity and real assignments") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "engineering") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)
  }

  private def createAndInitTableWithReqAttrs(
      reqAttrs: String,
      schemaString: String,
      jsonData: String): Unit = {
    createAndInitTable(schemaString, jsonData,
      java.util.Map.of("column-update-req-attrs", reqAttrs))
  }

  test("column-update: requiredDataAttributes - data correctness") {
    createAndInitTableWithReqAttrs("dep,id", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: undeclared distribution column is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-req-attrs", "pk,salary",
        "column-update-cluster-by", "dep"))

    val excludeColumnPruning = SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ColumnPruning.ruleName
    Seq(Nil, Seq(excludeColumnPruning)).foreach { confs =>
      withSQLConf(confs: _*) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
          },
          condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
          parameters = Map("connector" -> reqAttrsOperationClass, "columns" -> "[`dep`]"))
      }
    }
  }

  test("column-update split: undeclared distribution column is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-split-req-attrs", "pk,salary",
        "column-update-cluster-by", "dep"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
      parameters = Map("connector" -> splitReqAttrsOperationClass, "columns" -> "[`dep`]"))
  }

  test("column-update: undeclared write requirement columns are listed once each, sorted") {
    createAndInitTable("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 1, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-req-attrs", "pk,salary",
        "column-update-cluster-by", "id,dep"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "columns" -> "[`dep`, `id`]"))
  }

  test("column-update: a write requirement on a missing nested field reports why") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1 }, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-req-attrs", "pk,s",
        "column-update-cluster-by", "s.missing"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
      parameters = Map(
        "connector" -> reqAttrsOperationClass,
        "columns" -> "[`s`.`missing` (FIELD_NOT_FOUND)]"))
  }

  test("column-update: an empty write requirement column name is reported as <empty>") {
    checkError(
      exception = QueryCompilationErrors.undeclaredWriteRequirementColumnsError(
        reqAttrsOperationClass, Seq(Seq.empty[String] -> None)).asInstanceOf[AnalysisException],
      condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "columns" -> "[<empty>]"))
  }

  test("column-update: undeclared column inside nested transforms is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-req-attrs", "pk,salary",
        "column-update-cluster-by", "dep",
        "column-update-cluster-by-nested-transform", "true"))

    val excludeColumnPruning = SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ColumnPruning.ruleName
    Seq(Nil, Seq(excludeColumnPruning)).foreach { confs =>
      withSQLConf(confs: _*) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
          },
          condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
          parameters = Map("connector" -> reqAttrsOperationClass, "columns" -> "[`dep`]"))
      }
    }
  }

  test("column-update: distribution by an undeclared row ID column is allowed") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-req-attrs", "salary,dep",
        "column-update-cluster-by", "pk"))

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 200, "hr") :: Nil)
  }

  test("column-update: distribution by an undeclared row ID column assigned to itself is " +
    "allowed") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-req-attrs", "salary,dep",
        "column-update-cluster-by", "pk"))

    sql(s"UPDATE $tableNameAsString SET salary = -1, pk = pk WHERE pk = 1")

    checkLastUpdatedColumns("salary")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 200, "hr") :: Nil)
  }

  test("column-update: partition source column not in requiredDataAttributes") {
    createAndInitTableWithReqAttrs("pk,id", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    executeAndCheckScanExcludes(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1", "dep")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: requiredDataAttributes resolves case-insensitively on the row-ID " +
    "column without adopting the declared spelling") {
    createAndInitTableWithReqAttrs("PK,salary,dep", "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary + 1 WHERE pk = 1")

    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq ==
      Seq("pk", "salary", "dep"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 101, "hr") :: Row(2, 200, "software") :: Nil)
  }

  test("column-update: undeclared, differently-cased columns in the RHS and WHERE clause " +
    "are read with the table's own spelling") {
    createAndInitTableWithReqAttrs("pk,dep,salary",
      "pk INT NOT NULL, salary INT, bonus INT, extra INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "extra": 5, "dep": "hr" }
        |""".stripMargin)

    executeAndCheckScan(
      s"UPDATE $tableNameAsString SET salary = salary + BONUS WHERE pk = 1 AND EXTRA > 3",
      "pk INT, salary INT, bonus INT, extra INT, dep STRING, _partition STRING")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 110, 10, 5, "hr") :: Nil)
  }

  test("column-update: DELETE on a scan that returns unrequested columns is unaffected") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update", "true",
        InMemoryBaseTable.SIMULATE_PARTIAL_COLUMN_PRUNING, "true"))

    sql(s"DELETE FROM $tableNameAsString WHERE pk < 2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 200, 20, "software") :: Nil)
  }

  test("column-update: requiredDataAttributes resolves case-insensitively on an assigned " +
    "column without adopting the declared spelling") {
    createAndInitTableWithReqAttrs("pk,SALARY,dep", "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary + 1 WHERE pk = 1")

    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq ==
      Seq("pk", "salary", "dep"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 101, "hr") :: Row(2, 200, "software") :: Nil)
  }

  test("column-update: case-sensitive analysis accepts columns that differ only in case") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      createAndInitTableWithReqAttrs("pk,salary,SALARY,dep",
        "pk INT NOT NULL, salary INT, SALARY INT, dep STRING",
        """{ "pk": 1, "salary": 100, "SALARY": 200, "dep": "hr" }
          |""".stripMargin)

      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

      assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq ==
        Seq("pk", "salary", "SALARY", "dep"))
      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, -1, 200, "hr") :: Nil)
    }
  }

  test("column-update: empty requiredDataAttributes throws AnalysisException") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }""".stripMargin,
      java.util.Map.of("column-update-empty-req-attrs", "true"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_EMPTY_REQUIRED_DATA_ATTRIBUTES",
      parameters = Map("connector" -> reqAttrsOperationClass))
  }

  test("column-update: nested requiredDataAttributes throws AnalysisException") {
    createAndInitTableWithReqAttrs("pk,s.c1,dep",
      "pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_NESTED_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map("connector" -> reqAttrsOperationClass, "nestedAttributes" -> "[`s`.`c1`]"))
  }

  test("column-update: duplicate requiredDataAttributes throws AnalysisException") {
    createAndInitTableWithReqAttrs("pk,pk,salary", "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = salary + 1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_DUPLICATE_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map("connector" -> reqAttrsOperationClass, "duplicateAttributes" -> "[`pk`]"))
  }

  test("column-update: case-only duplicate requiredDataAttributes throws AnalysisException") {
    createAndInitTableWithReqAttrs("PK,pk,salary", "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = salary + 1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_DUPLICATE_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map("connector" -> reqAttrsOperationClass, "duplicateAttributes" -> "[`PK`]"))
  }

  test("column-update: duplicate requiredDataAttributes are reported in declared order") {
    createAndInitTableWithReqAttrs("salary,pk,SALARY,PK", "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = salary + 1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_DUPLICATE_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map(
        "connector" -> reqAttrsOperationClass, "duplicateAttributes" -> "[`salary`, `pk`]"))
  }

  test("column-update: metadata column in requiredDataAttributes throws AnalysisException") {
    createAndInitTableWithReqAttrs("pk,_partition,dep", "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET dep = 'x' WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_METADATA_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map(
        "connector" -> reqAttrsOperationClass,
        "metadataAttributes" -> "[`_partition`]"))
  }

  test("column-update split: metadata column in requiredDataAttributes throws " +
    "AnalysisException") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-split-req-attrs", "pk,_partition,dep"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET dep = 'x' WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_METADATA_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map(
        "connector" -> splitReqAttrsOperationClass,
        "metadataAttributes" -> "[`_partition`]"))
  }

  test("column-update: requiredDataAttributes rejects a column that does not exist") {
    createAndInitTableWithReqAttrs("pk,nonexistent_col,id", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_UNKNOWN_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map(
        "connector" -> reqAttrsOperationClass,
        "unknownAttributes" -> "[`nonexistent_col`]"))
  }

  test("column-update: column update schema excludes undeclared columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq ==
      Seq("pk", "dep", "salary"))
  }

  test("column-update: data correctness") {
    createAndInitTable("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "id": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
  }

  private def createAndInitTableSplit(schemaString: String, jsonData: String): Unit = {
    createAndInitTable(schemaString, jsonData,
      java.util.Map.of("column-update-split", "true"))
  }

  test("column-update split: column update schema has the declared columns") {
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        DEP_FIELD,
        StructField("id", IntegerType, nullable = false)
      ))))
  }

  test("column-update split: data correctness") {
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, -1, "hr") :: Nil)
  }

  test("column-update: split-updates sends a narrow UPDATE as delete and reinsert") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update", "true", "split-updates", "true"))

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastColumnUpdateWriteLog(
      deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
      reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(1, "hr", -1)))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "hr") :: Nil)
  }

  test("column-update split: delete and reinsert carry the row ID and preserved metadata") {
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastColumnUpdateWriteLog(
      deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
      reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(1, "hr", -1)))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "hr") :: Nil)
  }

  test("column-update split: row-ID reassignment on narrow write is rejected") {
    // `extra` is neither declared, updated, nor referenced, so requiredDataAttributes()
    // ([pk, dep, salary]) doesn't cover every table column -- the write stays genuinely narrow
    // and reassigning the row ID must be rejected.
    createAndInitTableSplit("pk INT NOT NULL, salary INT, dep STRING, extra STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr", "extra": "x" }
        |{ "pk": 2, "salary": 200, "dep": "software", "extra": "y" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")
      },
      condition = "COLUMN_UPDATE_SPLIT_ROW_ID_REASSIGNMENT",
      parameters = Map("connector" -> splitOperationClass, "rowIds" -> "[`pk`]"))
  }

  test("column-update split: row-ID reassignment is allowed when requiredDataAttributes " +
    "covers every table column") {
    createAndInitTableSplitWithReqAttrs("pk,salary,dep",
      "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(2, 200, "software") :: Row(11, -1, "hr") :: Nil)
  }

  test("column-update split: row-ID reassignment is allowed when requiredDataAttributes " +
    "covers every data column of a relation with metadata columns") {
    createAndInitTableSplitWithReqAttrs("pk,salary,dep",
      "pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    // `_partition` and the coerced `salary > 0.5` make the analyzer add the metadata columns to
    // the target relation in a later iteration. Only analyzes the plan.
    val parsed = spark.sessionState.sqlParser.parsePlan(
      s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 " +
        s"WHERE dep = 'hr' AND _partition = 'hr' AND salary > 0.5")
    val write = spark.sessionState.executePlan(parsed).analyzed.collectFirst {
      case w: WriteDelta => w
    }.getOrElse(fail("couldn't find WriteDelta in analyzed plan"))

    assert(write.table.output.map(_.name) == Seq("pk", "salary", "dep"))
  }

  private def createAndInitTableSplitWithReqAttrs(
      reqAttrs: String,
      schemaString: String,
      jsonData: String): Unit = {
    createAndInitTable(schemaString, jsonData,
      java.util.Map.of("column-update-split-req-attrs", reqAttrs))
  }

  test("column-update split: undeclared row-ID column on narrow write is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      java.util.Map.of("column-update-split-row-id", "pk"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")
      },
      condition = "COLUMN_UPDATE_SPLIT_ROW_ID_NOT_DECLARED",
      parameters = Map("connector" -> splitWithRowIdOperationClass, "rowIds" -> "[`pk`]"))
  }

  test("column-update split: metadata row ID preserved on reinsert need not be declared") {
    // `_partition` is a required metadata attribute with PRESERVE_ON_REINSERT, so the REINSERT
    // row carries it through the metadata projection.
    // Each partition holds one row, so `_partition` identifies it.
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      java.util.Map.of("column-update-split-row-id", "_partition"))

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq == Seq("salary"))
    checkLastColumnUpdateWriteLog(
      WriteLogEntry(operation = Delete, metadata = Some(Row("hr", null))),
      reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(-1)))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 200, "software") :: Nil)
  }

  test("column-update split: metadata row ID not preserved on reinsert must be declared") {
    // `index` is a required metadata attribute, but its REINSERT value is null.
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-split-row-id", "index"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")
      },
      condition = "COLUMN_UPDATE_SPLIT_ROW_ID_NOT_DECLARED",
      parameters = Map("connector" -> splitWithRowIdOperationClass, "rowIds" -> "[`index`]"))
  }

  test("column-update split: metadata row ID outside requiredMetadataAttributes must be " +
    "declared") {
    // Without required metadata attributes, nothing projects `_partition` into the REINSERT row.
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-split-row-id", "_partition", "no-metadata", "true"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")
      },
      condition = "COLUMN_UPDATE_SPLIT_ROW_ID_NOT_DECLARED",
      parameters = Map("connector" -> splitWithRowIdOperationClass, "rowIds" -> "[`_partition`]"))
  }

  // ---------------------------------------------------------------------------
  // Write relation tests: verify that only the write relation is narrowed to the
  // declared columns, for in-place and delete+insert deltas.
  // ---------------------------------------------------------------------------

  test("column-update: write relation has the declared columns in declared order") {
    createAndInitTableWithProps(java.util.Map.of("column-update-req-attrs", "salary,pk"))

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")
    }

    checkColumnUpdateWrite(write, "salary", "pk")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update split: write relation has the declared columns") {
    createAndInitTableWithProps(java.util.Map.of("column-update-split-req-attrs", "pk,salary"))

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")
    }

    checkColumnUpdateWrite(write, "pk", "salary")
    assert(table.lastWriteLog.map(_.getUTF8String(0).toString).toSet ==
      Set(Delete.toString, Reinsert.toString))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update: scan reads only the referenced columns") {
    createAndInitTableWithProps(java.util.Map.of("column-update-req-attrs", "pk,salary"))

    executeAndCheckScan(
      s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'",
      "pk INT, dep STRING, salary INT, bonus INT, extra INT, _partition STRING")
  }

  test("column-update split: scan reads only the referenced columns") {
    createAndInitTableWithProps(java.util.Map.of("column-update-split-req-attrs", "pk,salary"))

    executeAndCheckScan(
      s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'",
      "pk INT, dep STRING, salary INT, bonus INT, extra INT, _partition STRING")
  }

  test("column-update: an operation replaced after the rewrite is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
    }
    val relation = write.table.asInstanceOf[DataSourceV2Relation]
    val operationTable = relation.table.asInstanceOf[RowLevelOperationTable]
    val t = table
    Seq(
      t.DeltaBasedOperation(UPDATE, CaseInsensitiveStringMap.empty()) -> "[pk, salary, dep]",
      new t.DeltaBasedColumnUpdateOperation(UPDATE) -> "[pk, dep]"
    ).foreach { case (operation, expectedNames) =>
      val replaced = write.withNewTable(
        relation.copy(table = operationTable.copy(operation = operation)(
          operationTable.operationInfo)))
      checkError(
        exception = intercept[SparkException](V2Writes(replaced)),
        condition = "INTERNAL_ERROR",
        parameters = Map("message" ->
          (s"The write relation of operation ${operation.getClass.getName} has columns " +
            s"[pk, dep, salary], but the operation expects $expectedNames.")))
    }
  }

  test("column-update: analyzed plan shows the declared columns as the write target") {
    createAndInitTableWithProps(java.util.Map.of("column-update-req-attrs", "pk,salary"))

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")
    }

    val header = write.simpleString(Int.MaxValue)
    assert(header.matches("""WriteDelta RelationV2\[pk#\d+, salary#\d+\] .*"""), header)
  }

  test("column-update: empty requiredDataAttributes is rejected for an all-identity UPDATE") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }""".stripMargin,
      java.util.Map.of("column-update-empty-req-attrs", "true"))

    // Every assignment is identity, so only the non-empty check can reject the declaration.
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET id = id WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_EMPTY_REQUIRED_DATA_ATTRIBUTES",
      parameters = Map("connector" -> reqAttrsOperationClass))
  }

  test("column-update: a dotted top-level column name is quoted as one identifier") {
    createAndInitTableWithReqAttrs("pk,salary",
      "pk INT NOT NULL, salary INT, `a.b` INT, dep STRING",
      """{ "pk": 1, "salary": 100, "a.b": 1, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET `a.b` = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "missingColumns" -> "[`a.b`]"))
  }

  test("column-update: MERGE carries the original row ID only for a reassigned row ID") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)

    withTempView("source") {
      import testImplicits._
      Seq((1, 150)).toDF("pk", "salary").createOrReplaceTempView("source")

      def originalRowIdColumns(updates: String): Seq[String] = {
        val write = executeAndKeepAnalyzedWrite {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN UPDATE SET $updates
               |""".stripMargin)
        }
        write.query.output.map(_.name).filter(_.startsWith(ORIGINAL_ROW_ID_VALUE_PREFIX))
      }

      assert(originalRowIdColumns("t.pk = t.pk, t.salary = s.salary").isEmpty)
      assert(originalRowIdColumns("t.pk = s.pk + 10, t.salary = s.salary") ==
        Seq(ORIGINAL_ROW_ID_VALUE_PREFIX + "pk"))
    }
  }

  test("column-update: analysis fails when assignment key is outside requiredDataAttributes") {
    createAndInitTableWithReqAttrs("pk", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "missingColumns" -> "[`id`]"))
  }

  test("column-update: a column assigned from another column must be declared") {
    createAndInitTableWithReqAttrs("pk,dep", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 10, "dep": "hr" }""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET id = pk WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "missingColumns" -> "[`id`]"))
  }

  test("column-update: CHECK constraint on an undeclared column referenced by the condition") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "extra": 5 }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_extra CHECK (extra > 0)")

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE extra > 3")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr", 5) :: Nil)
  }

  test("column-update: CHECK constraint on an undeclared column referenced by the assignment") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "extra": 5 }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_extra CHECK (extra > 0)")

    sql(s"UPDATE $tableNameAsString SET id = extra + 1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 6, "hr", 5) :: Nil)
  }

  test("column-update: reassigning a row-ID column that has a CHECK constraint") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_pk CHECK (pk > 0)")

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(2, 200, "software") :: Row(11, -1, "hr") :: Nil)
  }

  test("column-update: CHECK constraint on a reassigned row-ID column is enforced") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_pk CHECK (pk > 0)")

    val ex = intercept[SparkRuntimeException] {
      sql(s"UPDATE $tableNameAsString SET pk = -pk WHERE dep = 'hr'")
    }
    assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION",
      s"expected CHECK_CONSTRAINT_VIOLATION but got: ${ex.getCondition}")
  }

  test("column-update: CHECK constraint column survives when a row-ID column is reassigned") {
    createAndInitTable("pk INT NOT NULL, salary INT, extra INT, dep STRING",
      """{ "pk": 1, "salary": 100, "extra": 5, "dep": "hr" }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_extra CHECK (extra > 0)")

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(11, -1, 5, "hr") :: Nil)
  }

}
