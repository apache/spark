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

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.RewriteRowLevelCommand
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.ORIGINAL_ROW_ID_VALUE_PREFIX
import org.apache.spark.sql.connector.catalog.{Delete, InMemoryRowLevelOperationTable, Reinsert}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.CommandExecutionMode
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

  private def splitReqAttrsTableProps(reqAttrs: String): java.util.Map[String, String] = {
    tablePropsWith(reqAttrsTableProps(reqAttrs), "split-updates" -> "true")
  }

  private val splitTableProps = tablePropsWith(extraTableProps, "split-updates" -> "true")

  private val splitPartitionRowIdTableProps =
    java.util.Map.of("column-update-split-row-id", "_partition")

  test("column-update: column update schema contains a single assigned column") {
    Seq(extraTableProps, splitTableProps).foreach { props =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
          """{ "pk": 1, "id": 1, "dep": "hr" }
            |{ "pk": 2, "id": 2, "dep": "software" }
            |{ "pk": 3, "id": 3, "dep": "hr" }
            |""".stripMargin,
          props)

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
    }
  }

  test("column-update: update handles metadata columns correctly") {
    Seq(
      "id IN (1, 100)",
      "id IN (1, 20) AND dep IN (SELECT * FROM updated_dep)"
    ).foreach { condition =>
      withTable(tableNameAsString) {
        withTempView("updated_dep") {
          createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
            """{ "pk": 1, "id": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "id": 3, "salary": 300, "dep": "hr" }
              |""".stripMargin)
          import testImplicits._
          Seq(Some("hr"), Some("it")).toDF().createOrReplaceTempView("updated_dep")

          sql(s"UPDATE $tableNameAsString SET id = -1 WHERE $condition")

          checkLastColumnUpdateWriteLog(
            updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(1, "hr", -1)))
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Row(1, -1, 100, "hr") :: Row(2, 2, 200, "software") :: Row(3, 3, 300, "hr") :: Nil)
        }
      }
    }
  }

  test("column-update: a CHAR or VARCHAR self-assignment is reported when Spark pads or checks " +
    "its value") {
    Seq(
      Seq.empty[(String, String)] -> Seq("id", "c", "v", "a", "m", "s"),
      // with preserved types, only the read-side padding of CHAR changes the assigned value
      Seq(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") -> Seq("id", "c", "a", "m", "s"),
      Seq(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true",
        SQLConf.READ_SIDE_CHAR_PADDING.key -> "false") -> Seq("id")
    ).foreach { case (confs, updated) =>
      withTable(tableNameAsString) {
        withSQLConf(confs: _*) {
          createTable("pk INT NOT NULL, id INT, c CHAR(3), v VARCHAR(10), " +
            "a ARRAY<CHAR(3)>, m MAP<STRING, CHAR(3)>, s STRUCT<f: CHAR(3)>, dep STRING")
          sql(s"INSERT INTO $tableNameAsString VALUES " +
            "(1, 1, 'a', 'x', array('b'), map('k', 'c'), named_struct('f', 'd'), 'hr')")

          sql(s"UPDATE $tableNameAsString SET id = -1, c = c, v = v, a = a, m = m, s = s " +
            "WHERE pk = 1")

          checkLastUpdatedColumns(updated: _*)
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Row(1, -1, "a  ", "x", Seq("b  "), Map("k" -> "c  "), Row("d  "), "hr") :: Nil)
        }
      }
    }
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
      ("ID = -1", Seq("id"), Seq("pk", "dep", "id"), Row(1, -1, "hr")),
      // assigns every column in table order, so the assignments are aligned as written
      ("PK = PK, ID = -1, DEP = 'eng'", Seq("id", "dep"), Seq("pk", "dep", "id"),
        Row(1, -1, "eng")),
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
        checkLastColumnUpdateSchema(columnUpdateNames: _*)
        checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), updatedRow :: Nil)
      }
    }
  }

  test("column-update split: row-level DELETE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin,
      splitTableProps)
    checkDeleteFallsBackToFullWidthRows(Row(2, 200, "hr"))
  }

  test("column-update split: row-level DELETE by a metadata row ID falls back to full-width " +
    "rows") {
    // Each partition holds one row, so `_partition` identifies it.
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      splitPartitionRowIdTableProps)
    checkDeleteFallsBackToFullWidthRows(Row(2, 200, "software"))
  }

  test("column-update split: MERGE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      splitTableProps)
    checkMergeFallsBackToFullWidthRows()
  }

  test("column-update: undeclared write requirement columns are rejected") {
    val excludeColumnPruning = SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ColumnPruning.ruleName
    Seq(
      (reqAttrsTableProps("pk,salary"), "dep", columnUpdateOperationClass, "[`dep`]"),
      (splitReqAttrsTableProps("pk,salary"), "dep", splitOperationClass, "[`dep`]"),
      (tablePropsWith(reqAttrsTableProps("pk,salary"), "column-update-order-by" -> "pk"), "dep",
        columnUpdateOperationClass, "[`dep`]"),
      (tablePropsWith(reqAttrsTableProps("pk,salary"), "column-update-order-by" -> "dep"), "pk",
        columnUpdateOperationClass, "[`dep`]"),
      (tablePropsWith(reqAttrsTableProps("pk,salary"),
        "column-update-cluster-by-nested-transform" -> "true"), "dep", columnUpdateOperationClass,
        "[`dep`]"),
      (reqAttrsTableProps("pk,salary"), "id,dep", columnUpdateOperationClass, "[`id`, `dep`]"),
      (reqAttrsTableProps("pk,salary,s"), "s.missing", columnUpdateOperationClass,
        "[`s`.`missing` (FIELD_NOT_FOUND)]")
    ).foreach { case (props, clusterBy, operationClass, columns) =>
      withTable(tableNameAsString) {
        val tableProps = tablePropsWith(props, "column-update-cluster-by" -> clusterBy)
        createAndInitTable("pk INT NOT NULL, salary INT, id INT, s STRUCT<c1: INT>, dep STRING",
          """{ "pk": 1, "salary": 100, "id": 1, "s": { "c1": 1 }, "dep": "hr" }
            |""".stripMargin,
          tableProps)

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
        columnUpdateOperationClass, Seq(Seq.empty[String] -> None)).asInstanceOf[AnalysisException],
      condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
      parameters = Map("connector" -> columnUpdateOperationClass, "columns" -> "[<empty>]"))
  }

  test("column-update: distribution by an undeclared row ID column is allowed") {
    Seq("salary = -1", "salary = -1, pk = pk").foreach { assignments =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "hr" }
            |""".stripMargin,
          tablePropsWith(reqAttrsTableProps("salary,dep"), "column-update-cluster-by" -> "pk"))

        sql(s"UPDATE $tableNameAsString SET $assignments WHERE pk = 1")

        checkLastUpdatedColumns("salary")
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Row(1, -1, "hr") :: Row(2, 200, "hr") :: Nil)
      }
    }
  }

  test("column-update: an undeclared, unreferenced partition column is not read") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin,
      reqAttrsTableProps("pk,id"))

    executeAndCheckScan(
      s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1", "pk INT, _partition STRING")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: requiredDataAttributes resolves case-insensitively without adopting the " +
    "declared spelling") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      reqAttrsTableProps("PK,SALARY,dep"))

    sql(s"UPDATE $tableNameAsString SET salary = salary + 1 WHERE pk = 1")

    checkLastColumnUpdateSchema("pk", "salary", "dep")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 101, "hr") :: Row(2, 200, "software") :: Nil)
  }

  test("column-update: case-sensitive analysis accepts columns that differ only in case") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      createAndInitTable("pk INT NOT NULL, salary INT, SALARY INT, dep STRING",
        """{ "pk": 1, "salary": 100, "SALARY": 200, "dep": "hr" }
          |""".stripMargin,
        reqAttrsTableProps("pk,salary,SALARY,dep"))

      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

      checkLastColumnUpdateSchema("pk", "salary", "SALARY", "dep")
      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, -1, 200, "hr") :: Nil)
    }
  }

  test("column-update: a declared name matches the same spelling, or else the one column that " +
    "matches case-insensitively") {
    Seq(
      "pk,salary,SALARY,dep" -> Some(Seq("pk", "salary", "SALARY", "dep")),
      "pk,SALARY,dep" -> Some(Seq("pk", "SALARY", "dep")),
      "PK,SALARY,dep" -> Some(Seq("pk", "SALARY", "dep")),
      "pk,Salary,dep" -> None
    ).foreach { case (reqAttrs, writeColumns) =>
      withTable(tableNameAsString) {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          createAndInitTable("pk INT NOT NULL, salary INT, SALARY INT, dep STRING",
            """{ "pk": 1, "salary": 100, "SALARY": 200, "dep": "hr" }
              |""".stripMargin,
            reqAttrsTableProps(reqAttrs))
        }

        val update = s"UPDATE $tableNameAsString SET pk = 3 WHERE pk = 1"
        writeColumns match {
          case Some(columns) =>
            sql(update)
            checkLastColumnUpdateSchema(columns: _*)
            withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
              checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(3, 100, 200, "hr") :: Nil)
            }
          case None =>
            checkError(
              exception = intercept[AnalysisException](sql(update)),
              condition = "COLUMN_UPDATE_UNKNOWN_REQUIRED_DATA_ATTRIBUTE",
              parameters = Map(
                "connector" -> columnUpdateOperationClass, "unknownAttributes" -> "[`Salary`]"))
        }
      }
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
        createAndInitTable(
          "pk INT NOT NULL, salary INT, s STRUCT<c1: INT, c2: INT>, `a.b` INT, dep STRING",
          """{ "pk": 1, "salary": 100, "s": { "c1": 1, "c2": 2 }, "a.b": 1, "dep": "hr" }
            |""".stripMargin,
          reqAttrsTableProps(reqAttrs))

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"UPDATE $tableNameAsString SET salary = -1, `a.b` = -1 WHERE pk = 1")
          },
          condition = condition,
          parameters = Map("connector" -> columnUpdateOperationClass, columns))
      }
    }
  }

  test("column-update: a declared metadata column in any case is rejected as a metadata column " +
    "when the relation already has the metadata columns") {
    // on a table that accepts any schema, an UPDATE that lists the metadata columns adds them to
    // the relation's output
    Seq("_partition", "_PARTITION").foreach { name =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |""".stripMargin,
          tablePropsWith(reqAttrsTableProps(s"pk,salary,dep,$name"), "accept-any-schema" -> "true"))

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"UPDATE $tableNameAsString SET pk = pk, salary = -1, dep = dep, " +
              "index = index, _partition = _partition")
          },
          condition = "COLUMN_UPDATE_METADATA_REQUIRED_DATA_ATTRIBUTE",
          parameters = Map(
            "connector" -> columnUpdateOperationClass, "metadataAttributes" -> s"[`$name`]"))
      }
    }
  }

  test("column-update: a declared column with no name parts is reported as <empty>") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)
    val t = table
    val nameless = FieldReference(Seq.empty[String])
    val operation = new t.DeltaBasedColumnUpdateOperation(
      UPDATE, Seq(nameless, FieldReference("pk"), nameless))

    checkError(
      exception = intercept[AnalysisException] {
        RewriteRowLevelCommand.resolveRequiredDataAttrs(
          spark.table(tableNameAsString).queryExecution.analyzed, operation)
      },
      condition = "COLUMN_UPDATE_UNKNOWN_REQUIRED_DATA_ATTRIBUTE",
      parameters = Map(
        "connector" -> operation.getClass.getName, "unknownAttributes" -> "[<empty>, <empty>]"))
  }

  test("column-update split: delete and reinsert carry the row ID and preserved metadata") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "hr" }
        |""".stripMargin,
      splitTableProps)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastColumnUpdateWriteLog(
      deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
      reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(1, "hr", -1)))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "hr") :: Nil)
  }

  test("column-update split: a reinserted row is found by a row-ID column declared in another " +
    "case") {
    createAndInitTable("PK INT NOT NULL, id INT, salary INT, dep STRING",
      """{ "PK": 1, "id": 1, "salary": 100, "dep": "hr" }
        |{ "PK": 2, "id": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin,
      splitTableProps)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE PK = 1")

    checkLastColumnUpdateSchema("PK", "dep", "salary")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY PK"),
      Row(1, 1, -1, "hr") :: Row(2, 2, 200, "hr") :: Nil)
  }

  test("column-update split: a row-ID reassignment is written when every column is declared") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      splitReqAttrsTableProps("pk,salary,dep"))

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")

    checkLastColumnUpdateSchema("pk", "salary", "dep")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(2, 200, "software") :: Row(11, -1, "hr") :: Nil)
  }

  test("column-update split: reassigning a declared row-ID column keeps the row's other columns") {
    // the split fixtures complete a reinserted row by its `pk`, so they fall back to full width
    Seq(splitReqAttrsTableProps("pk,salary"), java.util.Map.of("column-update-split-row-id", "pk"))
      .foreach(props => checkRowIdReassignmentKeepsOtherColumns(props, narrow = false))
  }

  test("column-update split: assigning an undeclared row-ID column is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      splitReqAttrsTableProps("salary,dep"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET pk = 5 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      parameters = Map("connector" -> splitOperationClass, "missingColumns" -> "[`pk`]"))
  }

  test("column-update split: Spark narrows the write when the row ID does not reach the " +
    "reinserted row") {
    // the row ID does not reach the reinserted row, so the fixture cannot commit it, and this
    // checks the optimized plan
    Seq(
      // the declaration leaves out the row ID `pk`
      java.util.Map.of("column-update-split-row-id", "pk"),
      // without required metadata attributes, nothing projects `_partition` into the REINSERT row
      java.util.Map.of("column-update-split-row-id", "_partition", "no-metadata", "true")
    ).foreach { props =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }""", props)

        val optimized = spark.sessionState.executePlan(
          spark.sessionState.sqlParser.parsePlan(s"UPDATE $tableNameAsString SET salary = -1"),
          CommandExecutionMode.SKIP).optimizedPlan

        val write = optimized.collectFirst { case w: WriteDelta => w }
          .getOrElse(fail("couldn't find WriteDelta in the optimized plan"))
        assert(write.table.output.map(_.name) == Seq("salary"))
      }
    }
  }

  test("column-update split: an empty declaration keeps the write full width") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      splitReqAttrsTableProps(""))

    sql(s"UPDATE $tableNameAsString SET salary = salary WHERE pk = 1")

    checkLastUpdatedColumns()
    checkLastWriteIsFullWidth()
    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, 100, "hr") :: Nil)
  }

  test("column-update split: an UPDATE with a subquery condition") {
    withTempView("updated_pk") {
      createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
        """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
          |""".stripMargin,
        splitTableProps)
      import testImplicits._
      Seq(2).toDF("pk").createOrReplaceTempView("updated_pk")

      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk IN (SELECT pk FROM updated_pk)")

      checkLastColumnUpdateSchema("pk", "dep", "salary")
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 100, 10, "hr") :: Row(2, -1, 20, "software") :: Nil)
    }
  }

  test("column-update: runtime group filtering on a narrow delta write") {
    withTempView("matched_ids") {
      createAndInitTableWithProps(reqAttrsTableProps("pk,dep,salary"))
      import testImplicits._
      Seq(1, 100).toDF().createOrReplaceTempView("matched_ids")

      executeAndCheckScans(
        s"UPDATE $tableNameAsString SET salary = -1 WHERE id IN (SELECT * FROM matched_ids)",
        primaryScanSchema = "pk INT, id INT, dep STRING, extra INT, _partition STRING",
        groupFilterScanSchema = Some("id INT, dep STRING"))

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 1, "hr", -1, 10, 1) ::
        Row(2, 2, "hr", 200, 20, 1) ::
        Row(3, 3, "software", 300, 30, 1) :: Nil)
    }
  }

  test("column-update: a full-width write keeps a condition column that its distribution uses") {
    // the declaration is empty, so the write is full width, and without metadata attributes only
    // the condition reads `_partition`
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      tablePropsWith(reqAttrsTableProps(""), "no-metadata" -> "true",
        "column-update-cluster-by" -> "_partition"))

    sql(s"UPDATE $tableNameAsString SET salary = salary WHERE _partition = 'hr'")

    checkLastWriteIsFullWidth()
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 100, "hr") :: Row(2, 200, "software") :: Nil)
  }

  test("column-update split: a reinserted row carries a metadata row ID preserved on reinsert") {
    // `_partition` is a required metadata attribute with PRESERVE_ON_REINSERT, so the REINSERT
    // row carries it through the metadata projection.
    // Each partition holds one row, so `_partition` identifies it.
    Seq(splitPartitionRowIdTableProps,
        java.util.Map.of("column-update-split-row-id", "_PARTITION")).foreach { props =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin,
          props)

        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

        checkLastColumnUpdateSchema("salary")
        checkLastColumnUpdateWriteLog(
          WriteLogEntry(operation = Delete, metadata = Some(Row("hr", null))),
          reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(-1)))
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Row(1, -1, "hr") :: Row(2, 200, "software") :: Nil)
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

  test("column-update: an undeclared CHECK constraint column that the UPDATE reads is kept and " +
    "enforced") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "extra": 5 }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT extra_above_id CHECK (extra > id)")

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE extra > 3")
    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, -1, "hr", 5) :: Nil)

    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"UPDATE $tableNameAsString SET id = extra + 1 WHERE pk = 1")
      },
      condition = "CHECK_CONSTRAINT_VIOLATION",
      sqlState = "23001",
      parameters = Map(
        "constraintName" -> "extra_above_id",
        "expression" -> "extra > id",
        "values" -> " - extra : 5\n - id : 6"))
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

}
