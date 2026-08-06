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
import org.apache.spark.sql.connector.catalog.{Aborted, Committed}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.sources

class AppendDataTransactionSuite extends RowLevelOperationSuiteBase {

  test("writeTo append with transactional checks") {
    // create table with initial data
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    // create a source on top of itself that will be fully resolved and analyzed
    val sourceDF = spark.table(tableNameAsString)
      .where("pk == 1")
      .select(col("pk") + 10 as "pk", col("salary"), col("dep"))
    sourceDF.queryExecution.assertAnalyzed()

    // append data using the DataFrame API
    val (txn, txnTables) = executeTransaction {
      sourceDF.writeTo(tableNameAsString).append()
    }

    // check txn was properly committed and closed
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(txnTables.size === 1)
    assert(table.version() === "2")

    // check the source scan was tracked via the transaction catalog
    val targetTxnTable = txnTables(tableNameAsString)
    assert(targetTxnTable.scanEvents.size === 1)
    assert(targetTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("pk", 1) => true
      case _ => false
    })

    // check data was appended correctly
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software"),
        Row(11, 100, "hr"))) // appended
  }

  test("SQL INSERT INTO with transactional checks") {
    // create table with initial data
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    // SQL INSERT INTO using VALUES
    val (txn, txnTables) = executeTransaction {
      sql(s"INSERT INTO $tableNameAsString VALUES (3, 300, 'hr'), (4, 400, 'finance')")
    }

    // check txn was properly committed and closed
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(table.version() === "2")

    // VALUES literal - No catalog tables were scanned
    assert(txnTables.isEmpty)

    // check data was inserted correctly
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software"),
        Row(3, 300, "hr"),
        Row(4, 400, "finance")))
  }

  for (isDynamic <- Seq(false, true))
  test(s"SQL INSERT OVERWRITE with transactional checks - isDynamic: $isDynamic") {
    // create table with initial data; table is partitioned by dep
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    val insertOverwrite = if (isDynamic) {
      // OverwritePartitionsDynamic
      s"""INSERT OVERWRITE $tableNameAsString
         |SELECT pk + 10, salary, dep FROM $tableNameAsString WHERE dep = 'hr'
         |""".stripMargin
    } else {
      // OverwriteByExpression
      s"""INSERT OVERWRITE $tableNameAsString
         |PARTITION (dep = 'hr')
         |SELECT pk + 10, salary FROM $tableNameAsString WHERE dep = 'hr'
         |""".stripMargin
    }

    val confValue = if (isDynamic) PartitionOverwriteMode.DYNAMIC else PartitionOverwriteMode.STATIC
    val (txn, txnTables) = withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> confValue.toString) {
      executeTransaction { sql(insertOverwrite) }
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(table.version() === "2")

    // the SELECT reads from the target table once with a dep='hr' filter
    assert(txnTables.size == 1)
    val targetTxnTable = txnTables(tableNameAsString)
    assert(targetTxnTable.scanEvents.size == 1)
    assert(targetTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(2, 200, "software"),  // unchanged
        Row(11, 100, "hr"),       // overwritten
        Row(13, 300, "hr")))      // overwritten
  }

  test("writeTo overwrite with transactional checks") {
    // create table with initial data; table is partitioned by dep
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // overwrite using a condition that covers the hr partition -> OverwriteByExpression
    val sourceDF = spark.createDataFrame(Seq((11, 999, "hr"), (12, 888, "hr"))).
      toDF("pk", "salary", "dep")

    val (txn, txnTables) = executeTransaction {
      sourceDF.writeTo(tableNameAsString).overwrite(col("dep") === "hr")
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(table.version() === "2")

    // literal DataFrame source - no catalog tables were scanned
    assert(txnTables.isEmpty)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(2, 200, "software"),  // unchanged
        Row(11, 999, "hr"),       // overwrote hr partition
        Row(12, 888, "hr")))      // overwrote hr partition
  }

  test("writeTo overwritePartitions with transactional checks") {
    // create table with initial data; table is partitioned by dep
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // overwrite partitions dynamically -> OverwritePartitionsDynamic
    val sourceDF = spark.createDataFrame(Seq((11, 999, "hr"), (12, 888, "hr"))).
      toDF("pk", "salary", "dep")

    val (txn, txnTables) = executeTransaction {
      sourceDF.writeTo(tableNameAsString).overwritePartitions()
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(table.version() === "2")

    // literal DataFrame source - no catalog tables were scanned
    assert(txnTables.isEmpty)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(2, 200, "software"),  // unchanged
        Row(11, 999, "hr"),       // overwrote hr partition
        Row(12, 888, "hr")))      // overwrote hr partition
  }

  test("SQL INSERT INTO SELECT with transactional checks") {
    // create table with initial data
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // SQL INSERT INTO using SELECT from the same table (self-insert)
    val (txn, txnTables) = executeTransaction {
      sql(s"""INSERT INTO $tableNameAsString
             |SELECT pk + 10, salary, dep FROM $tableNameAsString WHERE dep = 'hr'
             |""".stripMargin)
    }

    // check txn was properly committed and closed
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(table.version() === "2")

    // the SELECT reads from the target table once with a dep='hr' filter
    assert(txnTables.size === 1)
    val targetTxnTable = txnTables(tableNameAsString)
    assert(targetTxnTable.scanEvents.size === 1)
    assert(targetTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    // check data was inserted correctly
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software"),
        Row(3, 300, "hr"),
        Row(11, 100, "hr"), // inserted from pk=1
        Row(13, 300, "hr"))) // inserted from pk=3
  }

  test("SQL INSERT INTO SELECT with subquery on source table and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"CREATE TABLE $sourceNameAsString (pk INT NOT NULL, salary INT, dep STRING)")
    sql(s"INSERT INTO $sourceNameAsString VALUES (1, 500, 'hr'), (3, 600, 'software')")

    // INSERT using a subquery that reads from the target to filter source rows
    // both tables are scanned through the transaction catalog
    val (txn, txnTables) = executeTransaction {
      sql(
        s"""INSERT INTO $tableNameAsString
           |SELECT pk + 10, salary, dep FROM $sourceNameAsString
           |WHERE pk IN (SELECT pk FROM $tableNameAsString WHERE dep = 'hr')
           |""".stripMargin)
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(txnTables.size === 2)
    assert(table.version() === "2")

    // target was scanned via the transaction catalog (IN subquery) once with dep='hr' filter
    val targetTxnTable = txnTables(tableNameAsString)
    assert(targetTxnTable.scanEvents.size === 1)
    assert(targetTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    // source was scanned via the transaction catalog exactly once (no filter)
    val sourceTxnTable = txnTables(sourceNameAsString)
    assert(sourceTxnTable.scanEvents.size === 1)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software"),
        Row(11, 500, "hr"))) // inserted: source pk=1 matched target hr row
  }

  test("SQL INSERT INTO SELECT with CTE and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"CREATE TABLE $sourceNameAsString (pk INT NOT NULL, salary INT, dep STRING)")
    sql(s"INSERT INTO $sourceNameAsString VALUES (1, 500, 'hr'), (3, 600, 'software')")

    // CTE reads from target; INSERT selects from source filtered by the CTE result
    // both tables are scanned through the transaction catalog
    val (txn, txnTables) = executeTransaction {
      sql(
        s"""WITH hr_pks AS (SELECT pk FROM $tableNameAsString WHERE dep = 'hr')
           |INSERT INTO $tableNameAsString
           |SELECT pk + 10, salary, dep FROM $sourceNameAsString
           |WHERE pk IN (SELECT pk FROM hr_pks)
           |""".stripMargin)
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(txnTables.size === 2)
    assert(table.version() === "2")

    // target was scanned via the transaction catalog (CTE) once with dep='hr' filter
    val targetTxnTable = txnTables(tableNameAsString)
    assert(targetTxnTable.scanEvents.size === 1)
    assert(targetTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    // source was scanned via the transaction catalog exactly once (no filter)
    val sourceTxnTable = txnTables(sourceNameAsString)
    assert(sourceTxnTable.scanEvents.size === 1)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software"),
        Row(11, 500, "hr"))) // inserted: source pk=1 matched target hr row via CTE
  }

  test("SQL INSERT with analysis failure and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    val e = intercept[AnalysisException] {
      sql(s"INSERT INTO $tableNameAsString SELECT nonexistent_col FROM $tableNameAsString")
    }

    assert(e.getMessage.contains("nonexistent_col"))
    assert(catalog.lastTransaction.currentState === Aborted)
    assert(catalog.lastTransaction.isClosed)
  }

  for (isDynamic <- Seq(false, true))
  test(s"SQL INSERT OVERWRITE with analysis failure and transactional checks" +
      s"isDynamic: $isDynamic") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    val insertOverwrite = if (isDynamic) {
      s"""INSERT OVERWRITE $tableNameAsString
         |SELECT nonexistent_col, salary, dep FROM $tableNameAsString WHERE dep = 'hr'
         |""".stripMargin
    } else {
      s"""INSERT OVERWRITE $tableNameAsString
         |PARTITION (dep = 'hr')
         |SELECT nonexistent_col FROM $tableNameAsString WHERE dep = 'hr'
         |""".stripMargin
    }

    val confValue = if (isDynamic) PartitionOverwriteMode.DYNAMIC else PartitionOverwriteMode.STATIC
    val e = withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> confValue.toString) {
      intercept[AnalysisException] { sql(insertOverwrite) }
    }

    assert(e.getMessage.contains("nonexistent_col"))
    assert(catalog.lastTransaction.currentState === Aborted)
    assert(catalog.lastTransaction.isClosed)
  }

  test("EXPLAIN INSERT SQL with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"EXPLAIN INSERT INTO $tableNameAsString VALUES (3, 300, 'hr')")

    // EXPLAIN should not start a transaction
    assert(catalog.transaction === null)

    // INSERT was not executed; data is unchanged
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software")))
  }

  test("SQL INSERT WITH SCHEMA EVOLUTION adds new column with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(
      s"""CREATE TABLE $sourceNameAsString
         |(pk INT NOT NULL, salary INT, dep STRING, active BOOLEAN)""".stripMargin)
    sql(s"INSERT INTO $sourceNameAsString VALUES (3, 300, 'hr', true), (4, 400, 'software', false)")

    val (txn, txnTables) = executeTransaction {
      sql(s"INSERT WITH SCHEMA EVOLUTION INTO $tableNameAsString SELECT * FROM $sourceNameAsString")
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)

    // the new column must be visible in the committed delegate's schema
    assert(table.schema.fieldNames.toSeq === Seq("pk", "salary", "dep", "active"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr", null),      // pre-existing rows: active is null
        Row(2, 200, "software", null),
        Row(3, 300, "hr", true),      // inserted with active
        Row(4, 400, "software", false)))
  }

  for (isDynamic <- Seq(false, true))
  test(s"SQL INSERT OVERWRITE WITH SCHEMA EVOLUTION adds new column with transactional checks " +
      s"isDynamic: $isDynamic") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(
      s"""CREATE TABLE $sourceNameAsString
         |(pk INT NOT NULL, salary INT, dep STRING, active BOOLEAN)""".stripMargin)
    sql(s"INSERT INTO $sourceNameAsString VALUES (11, 999, 'hr', true), (12, 888, 'hr', false)")

    val insertOverwrite = if (isDynamic) {
      s"""INSERT WITH SCHEMA EVOLUTION OVERWRITE TABLE $tableNameAsString
         |SELECT * FROM $sourceNameAsString
         |""".stripMargin
    } else {
      s"""INSERT WITH SCHEMA EVOLUTION OVERWRITE TABLE $tableNameAsString
         |PARTITION (dep = 'hr')
         |SELECT pk, salary, active FROM $sourceNameAsString
         |""".stripMargin
    }

    val confValue = if (isDynamic) PartitionOverwriteMode.DYNAMIC else PartitionOverwriteMode.STATIC
    val (txn, _) = withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> confValue.toString) {
      executeTransaction { sql(insertOverwrite) }
    }

    assert(txn.currentState === Committed)
    assert(txn.isClosed)

    // the new column must be visible in the committed delegate's schema
    assert(table.schema.fieldNames.contains("active"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(2, 200, "software", null), // unchanged (different partition)
        Row(11, 999, "hr", true),      // overwrote hr partition
        Row(12, 888, "hr", false)))
  }

  test("SQL INSERT WITH SCHEMA EVOLUTION analysis failure aborts transaction") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(
      s"""CREATE TABLE $sourceNameAsString
         |(pk INT NOT NULL, salary INT, dep STRING, active BOOLEAN)""".stripMargin)

    val e = intercept[AnalysisException] {
      sql(
        s"""INSERT WITH SCHEMA EVOLUTION INTO $tableNameAsString
           |SELECT nonexistent_col FROM $sourceNameAsString
           |""".stripMargin)
    }

    assert(e.getMessage.contains("nonexistent_col"))
    assert(catalog.lastTransaction.currentState === Aborted)
    assert(catalog.lastTransaction.isClosed)
    // schema must be unchanged after the aborted transaction
    assert(table.schema.fieldNames.toSeq === Seq("pk", "salary", "dep"))
  }
}
