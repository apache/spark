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

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.connector.catalog.{InMemoryBaseTable, InMemoryRowLevelOperationTable}
import org.apache.spark.sql.connector.write.{RowLevelOperation, RowLevelOperationTable}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2Writes}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// Column-update tests that hold for both delta-based and group-based operations; each suite
// supplies its column-update table properties through `extraTableProps`.
trait ColumnUpdateTableSharedTests extends UpdateTableSuiteBase {

  // the properties of a column-update table whose connector declares exactly `reqAttrs`
  protected def reqAttrsTableProps(reqAttrs: String): java.util.Map[String, String]

  // the properties of a table of the same kind whose operation does not mix in column updates
  protected def fullWidthTableProps: java.util.Map[String, String]

  // the metadata columns the scan of a column-update table reads, in DDL form
  protected def scanMetadataColumns: String = ", _partition STRING, index INT"

  // the class name of the operation that `reqAttrsTableProps` builds
  protected def reqAttrsOperationClass: String = if (deltaUpdate) {
    classOf[InMemoryRowLevelOperationTable#DeltaBasedColumnUpdateOperationWithReqAttrs].getName
  } else {
    classOf[InMemoryRowLevelOperationTable#PartitionBasedColumnUpdateOperationWithReqAttrs].getName
  }

  // `base` with the given table properties added; a given key replaces the same key in `base`
  protected def tablePropsWith(
      base: java.util.Map[String, String],
      props: (String, String)*): java.util.Map[String, String] = {
    (base.asScala ++ props).asJava
  }

  // t(pk, id, dep, salary, bonus, extra) with CHECK (extra > 0), partitioned by dep
  protected def createAndInitTableWithProps(props: java.util.Map[String, String]): Unit = {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, salary INT, bonus INT, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "salary": 100, "bonus": 10, "extra": 1 }
        |{ "pk": 2, "id": 2, "dep": "hr", "salary": 200, "bonus": 20, "extra": 1 }
        |{ "pk": 3, "id": 3, "dep": "software", "salary": 300, "bonus": 30, "extra": 1 }
        |""".stripMargin,
      props)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_extra CHECK (extra > 0)")
  }

  test("column-update: undeclared columns are not written and keep their values") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkLastColumnUpdateSchema("pk", "dep", "salary")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("column-update: write relation has the declared columns in declared order") {
    createAndInitTableWithProps(reqAttrsTableProps("salary,pk"))

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")
    }

    assert(!write.query.exists(_.isInstanceOf[Union]))
    checkColumnUpdateWrite(write, "salary", "pk")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update: the plan writes the declared columns and scans the referenced ones") {
    createAndInitTableWithProps(reqAttrsTableProps("pk,salary"))

    // `id` is neither declared nor referenced; `dep`, `bonus` and `extra` are read for the
    // condition, the assignment and the CHECK constraint
    val write = executeAndKeepAnalyzedWrite {
      executeAndCheckScan(
        s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'",
        s"pk INT, dep STRING, salary INT, bonus INT, extra INT$scanMetadataColumns")
    }

    val writeNodeName = if (deltaUpdate) "WriteDelta" else "ReplaceData"
    val header = write.simpleString(Int.MaxValue)
    assert(header.matches(s"""$writeNodeName RelationV2\\[pk#\\d+, salary#\\d+\\] .*"""), header)
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update: undeclared, differently-cased columns in the RHS and WHERE clause are " +
    "read with the table's own spelling") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, extra INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "extra": 5, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "extra": 5, "dep": "hr" }
        |""".stripMargin)

    executeAndCheckScan(
      s"UPDATE $tableNameAsString SET salary = salary + BONUS WHERE pk = 1 AND EXTRA > 3",
      s"pk INT, salary INT, bonus INT, extra INT, dep STRING$scanMetadataColumns")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 110, 10, 5, "hr") :: Row(2, 200, 20, 5, "hr") :: Nil)
  }

  test("column-update: assigned column outside requiredDataAttributes is rejected") {
    createAndInitTableWithProps(reqAttrsTableProps("pk,salary"))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET bonus = 0 WHERE dep = 'hr'")
      },
      condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "missingColumns" -> "[`bonus`]"))
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
    val t = operationTable.table.asInstanceOf[InMemoryRowLevelOperationTable]
    val replacements: Seq[(RowLevelOperation, String)] = if (deltaUpdate) {
      Seq(
        t.DeltaBasedOperation(UPDATE, CaseInsensitiveStringMap.empty()) -> "[pk, salary, dep]",
        new t.DeltaBasedColumnUpdateOperation(UPDATE) -> "[pk, dep]")
    } else {
      Seq(
        t.PartitionBasedOperation(UPDATE, CaseInsensitiveStringMap.empty()) -> "[pk, salary, dep]",
        new t.PartitionBasedColumnUpdateOperation(UPDATE) -> "[pk, dep]")
    }
    replacements.foreach { case (operation, expectedNames) =>
      assert(operation != operationTable.operation)
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

  test("column-update: nested struct field update narrows to the root struct column") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |{ "pk": 2, "s": { "c1": 3, "c2": 4 }, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")

    checkLastUpdatedColumns("s")
    checkLastColumnUpdateSchema("pk", "dep", "s")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, Row(-1, 2), "hr") :: Row(2, Row(3, 4), "hr") :: Nil)
  }

  test("column-update: excluding ColumnPruning keeps the write narrow") {
    createAndInitTableWithProps(reqAttrsTableProps("pk,salary"))

    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ColumnPruning.ruleName) {
      executeAndCheckScan(
        s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'",
        s"pk INT, id INT, dep STRING, salary INT, bonus INT, extra INT$scanMetadataColumns")
    }

    checkLastColumnUpdateSchema("pk", "salary")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update: reassigning a declared row-ID column keeps the row's other columns") {
    createAndInitTableWithProps(reqAttrsTableProps("pk,salary"))

    sql(s"UPDATE $tableNameAsString SET pk = 2, salary = 0 WHERE pk = 1")

    // the copy-on-write fixture finds original rows by `pk`, so it falls back to full-width rows
    if (deltaUpdate) checkLastColumnUpdateSchema("pk", "salary") else checkLastWriteIsFullWidth()
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk, id"),
      Row(2, 1, "hr", 0, 10, 1) ::
      Row(2, 2, "hr", 200, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update: an UPDATE whose assignments are all identity assignments follows the " +
    "declaration") {
    Seq(
      extraTableProps -> Some(Seq("pk", "dep")),
      reqAttrsTableProps("pk,salary") -> Some(Seq("pk", "salary")),
      reqAttrsTableProps("") -> None
    ).foreach { case (props, columnUpdateNames) =>
      withTable(tableNameAsString) {
        createAndInitTableWithProps(props)

        sql(s"UPDATE $tableNameAsString SET salary = salary, dep = dep WHERE pk = 1")

        checkLastUpdatedColumns()
        columnUpdateNames match {
          case Some(names) => checkLastColumnUpdateSchema(names: _*)
          case None => checkLastWriteIsFullWidth()
        }
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Row(1, 1, "hr", 100, 10, 1) ::
          Row(2, 2, "hr", 200, 20, 1) ::
          Row(3, 3, "software", 300, 30, 1) :: Nil)
      }
    }
  }

  test("column-update: a narrow UPDATE keeps the other columns after a schema change") {
    Seq(
      ("ALTER COLUMN salary AFTER bonus",
        Row(1, 10, -1, "hr") :: Row(2, 20, 200, "software") :: Row(3, 30, 300, "hr") :: Nil),
      ("ALTER COLUMN bonus TYPE DOUBLE",
        Row(1, -1, 10.0, "hr") :: Row(2, 200, 20.0, "software") :: Row(3, 300, 30.0, "hr") :: Nil)
    ).foreach { case (alter, expectedRows) =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
          """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
            |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
            |""".stripMargin)
        sql(s"ALTER TABLE $tableNameAsString $alter")

        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

        checkAnswer(sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"), expectedRows)
      }
    }
  }

  test("column-update: a top-level column with a dot in its name is one updated column") {
    createAndInitTable("pk INT NOT NULL, `a.b` INT, dep STRING",
      """{ "pk": 1, "a.b": 100, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET `a.b` = -1 WHERE pk = 1")

    checkLastUpdatedColumns("`a.b`")
    checkLastColumnUpdateSchema("pk", "dep", "a.b")
    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, -1, "hr") :: Nil)
  }

  test("column-update: an empty requiredDataAttributes does not cover an updated column") {
    createAndInitTableWithProps(reqAttrsTableProps(""))

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET bonus = 0 WHERE pk = 1")
      },
      condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      parameters = Map("connector" -> reqAttrsOperationClass, "missingColumns" -> "[`bonus`]"))
  }

  test("column-update: other operations keep a full-width write relation") {
    createAndInitTableWithProps(fullWidthTableProps)

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
    }

    assert(write.table.output == write.originalTable.output)
  }

  test("column-update: row-level DELETE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin)
    checkDeleteFallsBackToFullWidthRows(Row(2, 200, "hr"))
  }

  // deletes the rows with pk < 2 from a table with columns (pk, salary, dep)
  protected def checkDeleteFallsBackToFullWidthRows(remainingRows: Row*): Unit = {
    sql(s"DELETE FROM $tableNameAsString WHERE pk < 2")

    checkLastWriteIsFullWidth()
    checkLastUpdatedColumns()
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      remainingRows)
  }

  test("column-update: MERGE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)
    checkMergeFallsBackToFullWidthRows()
  }

  protected def checkMergeFallsBackToFullWidthRows(): Unit = {
    withTempView("source") {
      import testImplicits._
      Seq((1, 150, "hr"), (3, 300, "software")).toDF("pk", "salary", "dep")
        .createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET salary = s.salary
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin)

      checkLastWriteIsFullWidth()
      checkLastUpdatedColumns()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 150, "hr") :: Row(2, 200, "software") :: Row(3, 300, "software") :: Nil)
    }
  }

  test("column-update: nested field identity update reports root struct as updated") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)

    // The aligned assignment rebuilds the whole struct, named_struct("c1", s.c1, "c2", s.c2),
    // which is not the column itself, so `s` is reported although its values don't change.
    sql(s"UPDATE $tableNameAsString SET s.c1 = s.c1 WHERE pk = 1")

    checkLastUpdatedColumns("s")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(1, 2), "hr") :: Nil)
  }

  test("column-update: CHECK constraint is still enforced on a narrow write") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "extra": 5 }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_id CHECK (id > 0)")

    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
      },
      condition = "CHECK_CONSTRAINT_VIOLATION",
      sqlState = "23001",
      parameters = Map(
        "constraintName" -> "positive_id", "expression" -> "id > 0", "values" -> " - id : -1"))
  }

  test("column-update: a CHECK constraint on a nested field is read and enforced") {
    createAndInitTable("pk INT NOT NULL, id INT, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "id": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_c1 CHECK (s.c1 > 0)")

    executeAndCheckScanIncludes(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1", "s")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, Row(1, 2), "hr") :: Nil)

    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")
      },
      condition = "CHECK_CONSTRAINT_VIOLATION",
      sqlState = "23001",
      parameters = Map(
        "constraintName" -> "positive_c1", "expression" -> "s.c1 > 0", "values" -> " - s.c1 : -1"))
  }

  test("column-update: scan that returns unrequested columns still writes narrow rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin,
      tablePropsWith(extraTableProps, InMemoryBaseTable.SIMULATE_PARTIAL_COLUMN_PRUNING -> "true"))

    executeAndCheckScanIncludes(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1", "bonus")
    checkLastColumnUpdateSchema("pk", "dep", "salary")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, 10, "hr") :: Nil)
  }
}
