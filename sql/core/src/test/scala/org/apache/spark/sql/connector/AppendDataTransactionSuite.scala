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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

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
    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(txnTables.size == 1)
    assert(table.version() == "2")

    // check the source scan was tracked via the transaction catalog
    val targetTxnTable = txnTables(tableNameAsString)
    assert(targetTxnTable.scanEvents.size >= 1)

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
    val (txn, _) = executeTransaction {
      sql(s"INSERT INTO $tableNameAsString VALUES (3, 300, 'hr'), (4, 400, 'finance')")
    }

    // check txn was properly committed and closed
    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(table.version() == "2")

    // check data was inserted correctly
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software"),
        Row(3, 300, "hr"),
        Row(4, 400, "finance")))
  }

  test("SQL INSERT OVERWRITE with transactional checks") {
    // create table with initial data; table is partitioned by dep
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // INSERT OVERWRITE with static partition predicate -> OverwriteByExpression
    val (txn, _) = executeTransaction {
      sql(s"""INSERT OVERWRITE $tableNameAsString
             |PARTITION (dep = 'hr')
             |SELECT pk + 10, salary FROM $tableNameAsString WHERE dep = 'hr'
             |""".stripMargin)
    }

    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(table.version() == "2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(2, 200, "software"),  // unchanged
        Row(11, 100, "hr"),       // overwritten
        Row(13, 300, "hr")))      // overwritten
  }

  test("SQL INSERT OVERWRITE dynamic partition with transactional checks") {
    // create table with initial data; table is partitioned by dep
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // INSERT OVERWRITE with dynamic partitioning -> OverwritePartitionsDynamic
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
      val (txn, _) = executeTransaction {
        sql(s"""INSERT OVERWRITE $tableNameAsString
               |SELECT pk + 10, salary, dep FROM $tableNameAsString WHERE dep = 'hr'
               |""".stripMargin)
      }

      assert(txn.currentState == Committed)
      assert(txn.isClosed)
      assert(table.version() == "2")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"),  // unchanged (different partition)
          Row(11, 100, "hr"),       // overwrote hr partition
          Row(13, 300, "hr")))      // overwrote hr partition
    }
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

    val (txn, _) = executeTransaction {
      sourceDF.writeTo(tableNameAsString).overwrite(col("dep") === "hr")
    }

    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(table.version() == "2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(2, 200, "software"),  // unchanged (different partition)
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

    val (txn, _) = executeTransaction {
      sourceDF.writeTo(tableNameAsString).overwritePartitions()
    }

    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(table.version() == "2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(2, 200, "software"),  // unchanged (different partition)
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
    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(txnTables.size == 1)
    assert(table.version() == "2")

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
}
