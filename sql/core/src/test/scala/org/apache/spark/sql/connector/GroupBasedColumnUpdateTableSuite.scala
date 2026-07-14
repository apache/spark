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

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableInfo, WriteUpdate}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

class GroupBasedColumnUpdateTableSuite extends RowLevelOperationSuiteBase {

  private def createAndInitTableReplaceData(schemaString: String, jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-cow", "true")
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schemaString, jsonData)
  }

  test("column-update ReplaceData: write schema contains only declared + assigned columns") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val updateSchema = table.lastWriteInfo.updateSchema().get()
    assert(updateSchema.fieldNames.contains("pk"), s"pk must be in update schema: $updateSchema")
    assert(updateSchema.fieldNames.contains("dep"), s"dep must be in update schema: $updateSchema")
    assert(updateSchema.fieldNames.contains("salary"),
      s"salary must be in update schema: $updateSchema")
    assert(!updateSchema.fieldNames.contains("bonus"),
      s"bonus must not be in update schema: $updateSchema")
  }

  test("column-update ReplaceData: data correctness -- bonus preserved, salary updated") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
    // CoW: pk=1 and pk=3 match (2 updates); the 'hr' partition has no other rows so no copies.
    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("column-update ReplaceData: subquery WHERE condition data correctness") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    import testImplicits._
    val subqueryDF = Seq("hr").toDF()
    subqueryDF.createOrReplaceTempView("target_deps")

    sql(
      s"""UPDATE $tableNameAsString
         |SET salary = -1
         |WHERE dep IN (SELECT * FROM target_deps)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("column-update ReplaceData: runtime group filtering data correctness") {
    Seq(true, false).foreach { dppEnabled =>
      Seq(true, false).foreach { aqeEnabled =>
        withSQLConf(
            org.apache.spark.sql.internal.SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key ->
              dppEnabled.toString,
            org.apache.spark.sql.internal.SQLConf.ADAPTIVE_EXECUTION_ENABLED.key ->
              aqeEnabled.toString) {
          withTable(tableNameAsString) {
            withTempView("matched_pk") {
              createAndInitTableReplaceData("pk INT, salary INT, bonus INT, dep STRING",
                """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
                  |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
                  |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
                  |""".stripMargin)

              import testImplicits._
              val matchedPkDF = Seq(Some(1), None).toDF()
              matchedPkDF.createOrReplaceTempView("matched_pk")

              sql(s"UPDATE $tableNameAsString SET salary = -1 " +
                s"WHERE pk IN (SELECT * FROM matched_pk)")

              checkAnswer(
                sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
                Row(1, -1, 10, "hr") ::
                Row(2, 200, 20, "software") ::
                Row(3, 300, 30, "hr") :: Nil)
            }
          }
        }
      }
    }
  }

  test("column-update ReplaceData: dispatch verification -- UPDATE/COPY rows use writeUpdate") {
    // Validates the runtime contract: when SupportsColumnUpdates is in play, UPDATE and COPY
    // rows flow through DataWriter.writeUpdate(...) rather than DataWriter.write(...). The
    // test connector tags log entries by which writer method was called, so we can assert
    // every row in the write log went through the narrow path.
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    // The matched 'hr' partition rewrites two rows: pk=1 (UPDATE) and pk=2 (COPY). Both
    // must be tagged as writeUpdate; no plain write entries.
    val ops = table.lastWriteLog.map(_.getUTF8String(0).toString).toSet
    assert(ops == Set(WriteUpdate.toString),
      s"all CoW column-update log entries must use writeUpdate, got: $ops")
  }

  // ---------------------------------------------------------------------------
  // Scan-narrowing tests: verify that the physical scan reads only the columns
  // required by the connector, the assignment RHS, and the operation condition.
  // ---------------------------------------------------------------------------

  test("column-update ReplaceData: scan excludes columns outside required + cond") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |""".stripMargin)

    // Connector-declared requiredDataAttributes = [pk, dep, salary]; cond refs `dep` (already
    // declared). `bonus` is neither declared nor referenced -- it must not appear in the scan.
    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkLastScanExcludes("bonus")
  }

  test("column-update ReplaceData: scan transparently widens for RHS references") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin)

    // Connector-declared = [pk, dep, salary]; RHS `salary + bonus` references `bonus`.
    // Narrow scan must include `bonus` even though it's not declared as required.
    sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")

    checkLastScanIncludes("bonus", "salary")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 110, 10, "hr") :: Row(2, 220, 20, "hr") :: Nil)
  }

  test("column-update ReplaceData: nested struct field update narrows to root struct column") {
    createAndInitTableReplaceData("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |{ "pk": 2, "s": { "c1": 3, "c2": 4 }, "dep": "hr" }
        |""".stripMargin)

    // `SET s.c1 = -1` is aligned into a whole-struct assignment, so updatedColumns is [s]
    // at root granularity. The narrow write / scan schema covers the root struct column.
    sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("s"),
      s"expected [s] in updatedColumns (root struct) but got: $updatedNames")

    val updateSchema = table.lastWriteInfo.updateSchema().get()
    assert(updateSchema.fieldNames.contains("s"),
      s"s must be in update schema: $updateSchema")

    // Data correctness: matched row's c1 updated, c2 preserved; unmatched row untouched.
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, Row(-1, 2), "hr") :: Row(2, Row(3, 4), "hr") :: Nil)
  }

  test("column-update ReplaceData: nested field identity update reports root struct as updated") {
    createAndInitTableReplaceData("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)

    // `SET s.c1 = s.c1` is semantically a no-op on the nested field, but the analyzer's
    // AssignmentUtils rewrites nested field assignments into whole-struct rebuilds:
    //     Assignment(s, named_struct("c1", s.c1, "c2", s.c2))
    // `isIdentityAssignment` operates at root-column granularity and does NOT recognize the
    // rebuilt struct as identity (its value is a `named_struct(...)`, not a plain Attribute
    // matching the key). So `s` is reported in updatedColumns even though the values don't
    // change. This documents the root-column granularity of `RowLevelOperationInfo` for the
    // CoW path.
    sql(s"UPDATE $tableNameAsString SET s.c1 = s.c1 WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("s"),
      s"nested identity is reported as an update at root-column granularity: $updatedNames")

    // Data correctness: the struct is rewritten but with equal values, so rows are unchanged.
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(1, 2), "hr") :: Nil)
  }
}
