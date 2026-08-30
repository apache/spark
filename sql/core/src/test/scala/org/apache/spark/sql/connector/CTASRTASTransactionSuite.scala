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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Aborted, Committed, Identifier, InMemoryRowLevelOperationTable}
import org.apache.spark.sql.sources

class CTASRTASTransactionSuite extends RowLevelOperationSuiteBase {

  private val newTableNameAsString = "cat.ns1.new_table"

  private def newTable: InMemoryRowLevelOperationTable =
    catalog.loadTable(Identifier.of(Array("ns1"), "new_table"))
      .asInstanceOf[InMemoryRowLevelOperationTable]

  test("CTAS with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    val (txn, txnTables) = executeTransaction {
      sql(s"""CREATE TABLE $newTableNameAsString
             |AS SELECT * FROM $tableNameAsString WHERE dep = 'hr'
             |""".stripMargin)
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(txnTables.size === 1)
    assert(table.version() === "1")    // source table: read-only, version unchanged
    assert(newTable.version() === "1") // target table: newly created and written

    // the source table was scanned once through the transaction catalog with a dep='hr' filter
    val sourceTxnTable = txnTables(tableNameAsString)
    assert(sourceTxnTable.scanEvents.size === 1)
    assert(sourceTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    checkAnswer(
      sql(s"SELECT * FROM $newTableNameAsString"),
      Seq(Row(1, 100, "hr")))
  }

  test("CTAS from literal source with transactional checks") {
    // no source catalog table involved — the query is a pure literal SELECT
    val (txn, txnTables) = executeTransaction {
      sql(s"CREATE TABLE $newTableNameAsString AS SELECT 1 AS pk, 100 AS salary, 'hr' AS dep")
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)

    // literal SELECT - no catalog tables were scanned
    assert(txnTables.isEmpty)
    assert(newTable.version() === "1") // target table: newly created and written

    checkAnswer(
      sql(s"SELECT * FROM $newTableNameAsString"),
      Seq(Row(1, 100, "hr")))
  }

  test("CTAS with analysis failure and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    val e = intercept[AnalysisException] {
      sql(s"CREATE TABLE $newTableNameAsString AS SELECT nonexistent_col FROM $tableNameAsString")
    }

    assert(e.getMessage.contains("nonexistent_col"))
    assert(catalog.lastTransaction.currentState === Aborted)
    assert(catalog.lastTransaction.isClosed)
  }

  test("RTAS with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // pre-create the target so REPLACE TABLE (not CREATE OR REPLACE) is valid
    sql(s"CREATE TABLE $newTableNameAsString (pk INT NOT NULL, salary INT, dep STRING)")

    val (txn, txnTables) = executeTransaction {
      sql(s"""REPLACE TABLE $newTableNameAsString
             |AS SELECT * FROM $tableNameAsString WHERE dep = 'hr'
             |""".stripMargin)
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(txnTables.size === 1)
    assert(table.version() === "1")    // source table: read-only, version unchanged
    assert(newTable.version() === "1") // target table: replaced and written

    // the source table was scanned once through the transaction catalog with a dep='hr' filter
    val sourceTxnTable = txnTables(tableNameAsString)
    assert(sourceTxnTable.scanEvents.size === 1)
    assert(sourceTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    checkAnswer(
      sql(s"SELECT * FROM $newTableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(3, 300, "hr")))
  }

  test("RTAS self-reference with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // source and target are the same table: reads the old snapshot via TxnTable,
    // replaces the table with a filtered version
    val (txn, txnTables) = executeTransaction {
      sql(s"""CREATE OR REPLACE TABLE $tableNameAsString
             |AS SELECT * FROM $tableNameAsString WHERE dep = 'hr'
             |""".stripMargin)
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(txnTables.size === 1)
    assert(table.version() === "1") // source/target table: replaced in place, version reset to 1

    // the source/target table was scanned once with a dep='hr' filter
    val sourceTxnTable = txnTables(tableNameAsString)
    assert(sourceTxnTable.scanEvents.size === 1)
    assert(sourceTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(3, 300, "hr")))
  }

  test("RTAS with analysis failure and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    val e = intercept[AnalysisException] {
      sql(s"""CREATE OR REPLACE TABLE $tableNameAsString
             |AS SELECT nonexistent_col FROM $tableNameAsString
             |""".stripMargin)
    }

    assert(e.getMessage.contains("nonexistent_col"))
    assert(catalog.lastTransaction.currentState === Aborted)
    assert(catalog.lastTransaction.isClosed)
  }

  test("simple CREATE TABLE and DROP TABLE do not create transactions") {
    sql(s"CREATE TABLE $newTableNameAsString (pk INT NOT NULL, salary INT, dep STRING)")
    assert(catalog.transaction === null)
    assert(catalog.lastTransaction === null)

    sql(s"DROP TABLE $newTableNameAsString")
    assert(catalog.transaction === null)
    assert(catalog.lastTransaction === null)
  }

  test("EXPLAIN CTAS with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"""EXPLAIN CREATE TABLE $newTableNameAsString
           |AS SELECT * FROM $tableNameAsString WHERE dep = 'hr'
           |""".stripMargin)

    // EXPLAIN should not start a transaction
    assert(catalog.transaction === null)
  }
}
