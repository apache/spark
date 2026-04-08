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
import org.apache.spark.sql.connector.catalog.Committed

class CTASRTASTransactionSuite extends RowLevelOperationSuiteBase {

  private val newTableNameAsString = "cat.ns1.new_table"

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

    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(txnTables.size == 1)
    assert(table.version() == "2")

    val sourceTxnTable = txnTables(tableNameAsString)
    assert(sourceTxnTable.scanEvents.size >= 1)

    checkAnswer(
      sql(s"SELECT * FROM $newTableNameAsString"),
      Seq(Row(1, 100, "hr")))
  }

  test("CTAS with cached source and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    // cache the source table before running CTAS
    spark.catalog.cacheTable(tableNameAsString)

    try {
      val (txn, txnTables) = executeTransaction {
        sql(s"""CREATE TABLE $newTableNameAsString
               |AS SELECT * FROM $tableNameAsString WHERE dep = 'hr'
               |""".stripMargin)
      }

      assert(txn.currentState == Committed)
      assert(txn.isClosed)
      assert(table.version() == "2")

      // cache miss: TxnTable-based relation is not structurally equal to the cached one,
      // so the scan goes through the transaction catalog and scan events are captured
      val sourceTxnTable = txnTables(tableNameAsString)
      assert(sourceTxnTable.scanEvents.size >= 1)

      checkAnswer(
        sql(s"SELECT * FROM $newTableNameAsString"),
        Seq(Row(1, 100, "hr")))
    } finally {
      spark.catalog.uncacheTable(tableNameAsString)
    }
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

    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(txnTables.size == 1)
    assert(table.version() == "2")

    val sourceTxnTable = txnTables(tableNameAsString)
    assert(sourceTxnTable.scanEvents.size >= 1)

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

    assert(txn.currentState == Committed)
    assert(txn.isClosed)

    val sourceTxnTable = txnTables(tableNameAsString)
    assert(sourceTxnTable.scanEvents.size >= 1)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(3, 300, "hr")))
  }
}
