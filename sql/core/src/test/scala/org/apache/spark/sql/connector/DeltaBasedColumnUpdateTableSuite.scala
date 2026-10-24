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

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.RewriteRowLevelCommand
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.ORIGINAL_ROW_ID_VALUE_PREFIX
import org.apache.spark.sql.connector.catalog.{Delete, InMemoryBaseTable, InMemoryRowLevelOperationTable, Reinsert}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
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

  private val splitOperationClass =
    classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateSplitOperation].getName

  private val splitWithRowIdOperationClass =
    classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateSplitOperationWithRowId].getName

  private def splitReqAttrsTableProps(reqAttrs: String): java.util.Map[String, String] = {
    java.util.Map.of("column-update-req-attrs", reqAttrs, "split-updates", "true")
  }

  private def createAndInitTableWithReqAttrs(
      reqAttrs: String,
      schemaString: String,
      jsonData: String): Unit = {
    createAndInitTable(schemaString, jsonData, reqAttrsTableProps(reqAttrs))
  }

  private def createAndInitTableSplit(schemaString: String, jsonData: String): Unit = {
    createAndInitTable(schemaString, jsonData,
      java.util.Map.of("column-update", "true", "split-updates", "true"))
  }

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

  test("column-update: updatedColumns and the column update schema follow the non-identity " +
    "assignments") {
    Seq(
      ("id = -1", Seq("id"), Seq("pk", "dep", "id"), Row(1, -1, "hr")),
      ("id = -1, dep = 'eng'", Seq("id", "dep"), Seq("pk", "dep", "id"), Row(1, -1, "eng")),
      ("id = -1, dep = dep", Seq("id"), Seq("pk", "dep", "id"), Row(1, -1, "hr")),
      ("id = id, dep = 'eng'", Seq("dep"), Seq("pk", "dep"), Row(1, 10, "eng")),
      ("id = id, dep = dep", Seq.empty, Seq("pk", "dep"), Row(1, 10, "hr")),
      ("id = pk", Seq("id"), Seq("pk", "dep", "id"), Row(1, 1, "hr"))
    ).foreach { case (assignments, updatedColumns, columnUpdateNames, updatedRow) =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
          """{ "pk": 1, "id": 10, "dep": "hr" }
            |""".stripMargin)

        sql(s"UPDATE $tableNameAsString SET $assignments WHERE pk = 1")

        checkLastUpdatedColumns(updatedColumns: _*)
        assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq == columnUpdateNames)
        checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), updatedRow :: Nil)
      }
    }
  }

  test("column-update split: row-level DELETE falls back to full-width rows") {
    createAndInitTableSplit("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin)
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
    createAndInitTableSplit("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)
    checkMergeFallsBackToFullWidthRows()
  }

  test("column-update: undeclared write requirement columns are rejected") {
    val excludeColumnPruning = SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ColumnPruning.ruleName
    val splitReqAttrsOperationClass =
      classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateSplitOperationWithReqAttrs]
        .getName
    Seq(
      ("pk,salary", "dep", Map.empty[String, String], reqAttrsOperationClass, "[`dep`]"),
      ("pk,salary", "dep", Map("split-updates" -> "true"), splitReqAttrsOperationClass,
        "[`dep`]"),
      ("pk,salary", "dep", Map("column-update-cluster-by-nested-transform" -> "true"),
        reqAttrsOperationClass, "[`dep`]"),
      ("pk,salary", "id,dep", Map.empty[String, String], reqAttrsOperationClass,
        "[`dep`, `id`]"),
      ("pk,salary,s", "s.missing", Map.empty[String, String], reqAttrsOperationClass,
        "[`s`.`missing` (FIELD_NOT_FOUND)]")
    ).foreach { case (reqAttrs, clusterBy, props, operationClass, columns) =>
      withTable(tableNameAsString) {
        val tableProps = props ++ Map(
          "column-update-req-attrs" -> reqAttrs, "column-update-cluster-by" -> clusterBy)
        createAndInitTable("pk INT NOT NULL, salary INT, id INT, s STRUCT<c1: INT>, dep STRING",
          """{ "pk": 1, "salary": 100, "id": 1, "s": { "c1": 1 }, "dep": "hr" }
            |""".stripMargin,
          tableProps.asJava)

        Seq(Nil, Seq(excludeColumnPruning)).foreach { confs =>
          withSQLConf(confs: _*) {
            checkError(
              exception = intercept[AnalysisException] {
                sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
              },
              condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
              parameters = Map("connector" -> operationClass, "columns" -> columns))
          }
        }
      }
    }
  }

  test("column-update: an empty write requirement column name is reported as <empty>") {
    checkError(
      exception = QueryCompilationErrors.undeclaredWriteRequirementColumnsError(
        reqAttrsOperationClass, Seq(Seq.empty[String] -> None)).asInstanceOf[AnalysisException],
      condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "columns" -> "[<empty>]"))
  }

  test("column-update: distribution by an undeclared row ID column is allowed") {
    Seq("salary = -1", "salary = -1, pk = pk").foreach { assignments =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "hr" }
            |""".stripMargin,
          java.util.Map.of(
            "column-update-req-attrs", "salary,dep",
            "column-update-cluster-by", "pk"))

        sql(s"UPDATE $tableNameAsString SET $assignments WHERE pk = 1")

        checkLastUpdatedColumns("salary")
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Row(1, -1, "hr") :: Row(2, 200, "hr") :: Nil)
      }
    }
  }

  test("column-update: an undeclared, unreferenced partition column is not read") {
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

  test("column-update: requiredDataAttributes resolves case-insensitively without adopting the " +
    "declared spelling") {
    createAndInitTableWithReqAttrs("PK,SALARY,dep", "pk INT NOT NULL, salary INT, dep STRING",
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

    // `id = id` assigns nothing, so only the non-empty check can reject the declaration
    Seq("id = -1", "id = id").foreach { assignment =>
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"UPDATE $tableNameAsString SET $assignment WHERE pk = 1")
        },
        condition = "COLUMN_UPDATE_EMPTY_REQUIRED_DATA_ATTRIBUTES",
        parameters = Map("connector" -> reqAttrsOperationClass))
    }
  }

  test("column-update: invalid requiredDataAttributes are rejected") {
    Seq(
      ("pk,s.c1,dep", "COLUMN_UPDATE_NESTED_REQUIRED_DATA_ATTRIBUTE",
        "nestedAttributes" -> "[`s`.`c1`]"),
      ("pk,pk,salary", "COLUMN_UPDATE_DUPLICATE_REQUIRED_DATA_ATTRIBUTE",
        "duplicateAttributes" -> "[`pk`]"),
      ("salary,PK,SALARY,pk", "COLUMN_UPDATE_DUPLICATE_REQUIRED_DATA_ATTRIBUTE",
        "duplicateAttributes" -> "[`salary`, `PK`]"),
      ("pk,_partition,salary", "COLUMN_UPDATE_METADATA_REQUIRED_DATA_ATTRIBUTE",
        "metadataAttributes" -> "[`_partition`]"),
      ("pk,nonexistent_col,salary", "COLUMN_UPDATE_UNKNOWN_REQUIRED_DATA_ATTRIBUTE",
        "unknownAttributes" -> "[`nonexistent_col`]"),
      ("pk,salary", "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
        "missingColumns" -> "[`a.b`]")
    ).foreach { case (reqAttrs, condition, columns) =>
      withTable(tableNameAsString) {
        createAndInitTableWithReqAttrs(reqAttrs,
          "pk INT NOT NULL, salary INT, s STRUCT<c1: INT, c2: INT>, `a.b` INT, dep STRING",
          """{ "pk": 1, "salary": 100, "s": { "c1": 1, "c2": 2 }, "a.b": 1, "dep": "hr" }
            |""".stripMargin)

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"UPDATE $tableNameAsString SET salary = -1, `a.b` = -1 WHERE pk = 1")
          },
          condition = condition,
          parameters = Map("connector" -> reqAttrsOperationClass, columns))
      }
    }
  }

  test("column-update: a declared column with no name parts is reported as <empty>") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)
    val t = table
    val operation = new t.DeltaBasedColumnUpdateOperation(UPDATE) {
      override def requiredDataAttributes(): Array[NamedReference] =
        Array(FieldReference(Seq.empty[String]), FieldReference("pk"),
          FieldReference(Seq.empty[String]))
    }

    checkError(
      exception = intercept[AnalysisException] {
        RewriteRowLevelCommand.resolveRequiredDataAttrs(
          spark.table(tableNameAsString).queryExecution.analyzed, operation)
      },
      condition = "COLUMN_UPDATE_UNKNOWN_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map(
        "connector" -> operation.getClass.getName, "unknownAttributes" -> "[<empty>, <empty>]"))
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
    // the declared [pk, dep, salary] leaves out `extra`, so the write is narrow
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
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      splitReqAttrsTableProps("pk,salary,dep"))

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(2, 200, "software") :: Row(11, -1, "hr") :: Nil)
  }

  test("column-update split: row-ID reassignment is allowed when requiredDataAttributes " +
    "covers every data column of a relation with metadata columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      splitReqAttrsTableProps("pk,salary,dep"))

    // `_partition` and the coerced `salary > 0.5` make the analyzer add the metadata columns to
    // the target relation in a later iteration.
    val parsed = spark.sessionState.sqlParser.parsePlan(
      s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 " +
        s"WHERE dep = 'hr' AND _partition = 'hr' AND salary > 0.5")
    val write = spark.sessionState.executePlan(parsed).analyzed.collectFirst {
      case w: WriteDelta => w
    }.getOrElse(fail("couldn't find WriteDelta in analyzed plan"))

    assert(write.table.output.map(_.name) == Seq("pk", "salary", "dep"))
  }

  test("column-update split: a row-ID column that does not reach the REINSERT row must be " +
    "declared") {
    Seq(
      Map("column-update-split-row-id" -> "pk"),
      // `index` is a required metadata attribute, but its REINSERT value is null
      Map("column-update-split-row-id" -> "index"),
      // without required metadata attributes, nothing projects `_partition` into the REINSERT row
      Map("column-update-split-row-id" -> "_partition", "no-metadata" -> "true")
    ).foreach { props =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |""".stripMargin,
          props.asJava)

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")
          },
          condition = "COLUMN_UPDATE_SPLIT_ROW_ID_NOT_DECLARED",
          parameters = Map(
            "connector" -> splitWithRowIdOperationClass,
            "rowIds" -> s"[`${props("column-update-split-row-id")}`]"))
      }
    }
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

  test("column-update split: the fixture rejects a DELETE it cannot locate by row ID") {
    Seq(
      Map("column-update-split-row-id" -> "_partition", "no-metadata" -> "true") -> "_partition",
      Map("column-update-split-row-id" -> "id") -> "id"
    ).foreach { case (props, rowIdName) =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, id INT NOT NULL, dep STRING",
          """{ "pk": 1, "id": 2, "dep": "hr" }
            |{ "pk": 2, "id": 1, "dep": "hr" }
            |""".stripMargin,
          props.asJava)

        val e = intercept[IllegalStateException] {
          sql(s"DELETE FROM $tableNameAsString WHERE pk < 2")
        }
        assert(e.getMessage == "The split fixture can only locate a row by pk or by metadata " +
          s"row-ID columns, but row-ID column $rowIdName is in neither")
      }
    }
  }

  test("column-update split: the write relation and the scan have only the declared and " +
    "referenced columns") {
    createAndInitTableWithProps(splitReqAttrsTableProps("pk,salary"))

    val write = executeAndKeepAnalyzedWrite {
      executeAndCheckScan(
        s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'",
        "pk INT, dep STRING, salary INT, bonus INT, extra INT, _partition STRING")
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

  test("column-update: an undeclared CHECK constraint column that the UPDATE reads is kept") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "extra": 5 }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_extra CHECK (extra > 0)")

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE extra > 3")
    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, -1, "hr", 5) :: Nil)

    sql(s"UPDATE $tableNameAsString SET id = extra + 1 WHERE pk = 1")
    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, 6, "hr", 5) :: Nil)
  }

  test("column-update: CHECK constraint on a reassigned row-ID column is enforced") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_pk CHECK (pk > 0)")

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(2, 200, "software") :: Row(11, -1, "hr") :: Nil)

    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"UPDATE $tableNameAsString SET pk = -pk WHERE dep = 'hr'")
      },
      condition = "CHECK_CONSTRAINT_VIOLATION",
      sqlState = "23001",
      parameters = Map(
        "constraintName" -> "positive_pk", "expression" -> "pk > 0", "values" -> " - pk : -11"))
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
