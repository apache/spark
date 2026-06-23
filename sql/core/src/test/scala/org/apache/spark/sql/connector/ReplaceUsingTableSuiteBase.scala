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

import org.apache.spark.sql.{AnalysisException, Row}
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

  private val schemaString = "pk INT NOT NULL, id INT, dep STRING"

  protected def isDeltaBasedReplace: Boolean = false

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
}
