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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.connector.catalog.InMemoryBaseTable
import org.apache.spark.sql.internal.SQLConf

// Column-update tests that hold for both delta-based and group-based operations; each suite
// supplies its column-update table properties through `extraTableProps`.
trait ColumnUpdateTableSharedTests extends RowLevelOperationSuiteBase {

  // the properties of a column-update table whose connector declares exactly `reqAttrs`
  protected def reqAttrsTableProps(reqAttrs: String): java.util.Map[String, String]

  // the properties of a table of the same kind whose operation does not mix in column updates
  protected def fullWidthTableProps: java.util.Map[String, String]

  // the metadata columns the scan of a column-update table reads, in DDL form
  protected def scanMetadataColumns: String = ", _partition STRING, index INT"

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

  test("column-update: nested struct field update narrows to the root struct column") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |{ "pk": 2, "s": { "c1": 3, "c2": 4 }, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")

    checkLastUpdatedColumns("s")
    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq == Seq("pk", "dep", "s"))
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

    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq == Seq("pk", "salary"))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
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

  // deletes the row with pk = 1 from a table with columns (pk, salary, dep)
  protected def checkDeleteFallsBackToFullWidthRows(remainingRows: Row*): Unit = {
    sql(s"DELETE FROM $tableNameAsString WHERE pk < 2")

    assert(!table.lastWriteInfo.columnUpdateSchema().isPresent,
      s"DELETE must not report a column update schema: " +
        s"${table.lastWriteInfo.columnUpdateSchema()}")
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

      assert(!table.lastWriteInfo.columnUpdateSchema().isPresent,
        s"MERGE must not report a column update schema: " +
          s"${table.lastWriteInfo.columnUpdateSchema()}")
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

    val ex = intercept[SparkRuntimeException] {
      sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
    }
    assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION",
      s"expected CHECK_CONSTRAINT_VIOLATION but got: ${ex.getCondition}")
  }

  test("column-update: CHECK constraint on a nested field narrows correctly") {
    createAndInitTable("pk INT NOT NULL, id INT, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "id": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_c1 CHECK (s.c1 > 0)")

    executeAndCheckScanIncludes(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1", "s")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, Row(1, 2), "hr") :: Nil)

    val ex = intercept[SparkRuntimeException] {
      sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")
    }
    assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION",
      s"expected CHECK_CONSTRAINT_VIOLATION but got: ${ex.getCondition}")
  }

  test("column-update: scan that returns unrequested columns still writes narrow rows") {
    val props = new java.util.HashMap[String, String](extraTableProps)
    props.put(InMemoryBaseTable.SIMULATE_PARTIAL_COLUMN_PRUNING, "true")
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin,
      props)

    executeAndCheckScanIncludes(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1", "bonus")
    val columnUpdateSchema = table.lastWriteInfo.columnUpdateSchema().get()
    assert(!columnUpdateSchema.fieldNames.contains("bonus"),
      s"bonus must not be in column update schema: $columnUpdateSchema")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, 10, "hr") :: Nil)
  }
}
