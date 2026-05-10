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

import java.util.Collections

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Aborted, CatalogV2Util, Committed, Identifier, InMemoryBaseTable, InMemoryRowLevelOperationTableCatalog, InMemoryTableCatalog, SharedTablesInMemoryRowLevelOperationTableCatalog, TableInfo}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType

class StreamingTransactionSuite extends RowLevelOperationSuiteBase {

  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(
      "spark.sql.catalog.cat",
      classOf[SharedTablesInMemoryRowLevelOperationTableCatalog].getName)
  }

  override def afterEach(): Unit = {
    SharedTablesInMemoryRowLevelOperationTableCatalog.reset()
    super.afterEach()
  }

  private def createSimpleTable(schemaString: String): Unit = {
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val tableInfo = new TableInfo.Builder().withColumns(columns).build()
    catalog.createTable(ident, tableInfo)
  }

  private def streamCatalog(query: StreamingQuery): InMemoryRowLevelOperationTableCatalog = {
    val session = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sparkSessionForStream
    session.sessionState.catalogManager.catalog("cat")
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
  }

  test("streaming write commits a transaction") {
    createSimpleTable("value INT")

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]

      val query = inputData.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tableNameAsString)

      assert(table.version() === "0")

      inputData.addData(1, 2, 3)
      query.processAllAvailable()
      query.stop()

      val txn = streamCatalog(query).lastTransaction
      assert(txn != null, "expected a transaction to have been committed")
      assert(txn.currentState === Committed)
      assert(txn.isClosed)

      // Pure streaming append: the write target is not read, source is not a TxnTable.
      val targetTxnTable = indexByName(txn.catalog.txnTables.values.toSeq)(tableNameAsString)
      assert(txn.catalog.txnTables.size === 1)
      assert(targetTxnTable.scanEvents.isEmpty)
      assert(table.version() === "1")

      // Transaction must be scoped to the streaming session; main session catalog is untouched.
      assert(catalog.observedTransactions.isEmpty)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("each micro-batch is an independent transaction") {
    createSimpleTable("value INT")

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]

      val query = inputData.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tableNameAsString)

      assert(table.version() === "0")

      inputData.addData(1, 2, 3)
      query.processAllAvailable()

      inputData.addData(4, 5, 6)
      query.processAllAvailable()

      query.stop()

      val sc = streamCatalog(query)
      assert(sc.observedTransactions.size === 2)
      assert(sc.observedTransactions.forall(_.currentState === Committed))
      // Pure streaming append: write target is not read in any micro-batch.
      assert(sc.observedTransactions.forall { t =>
        indexByName(t.catalog.txnTables.values.toSeq)(tableNameAsString).scanEvents.isEmpty
      })
      // Each committed micro-batch increments the delegate version exactly once.
      assert(table.version() === "2")

      // Transaction must be scoped to the streaming session; main session catalog is untouched.
      assert(catalog.observedTransactions.isEmpty)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)))
    }
  }

  test("batch read from catalog-backed table inside streaming query is tracked as a scan event") {
    // Target table for the stream.
    createSimpleTable("value INT")

    // Catalog-backed static table used as a batch (non-streaming) source.
    val sourceIdent = Identifier.of(namespace, "source_table")
    val srcColumns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL("value INT"))
    catalog.createTable(sourceIdent, new TableInfo.Builder().withColumns(srcColumns).build())
    sql(s"INSERT INTO $sourceNameAsString VALUES (1), (2), (3)")
    // The INSERT above runs a transaction on the main session catalog; capture the count now
    // so we can assert the streaming query does not add more.
    val mainTxnsBefore = catalog.observedTransactions.size

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]

      // spark.read produces a DataSourceV2Relation (batch), not a streaming source.
      // UnresolveRelationsInTransaction converts it to V2TableReference each micro-batch so
      // the transaction-aware catalog can record the scan event.
      val staticData = spark.read.table(sourceNameAsString)

      val query = inputData.toDF()
        .join(staticData, "value")
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tableNameAsString)

      inputData.addData(1, 2, 3)
      query.processAllAvailable()
      query.stop()

      val txn = streamCatalog(query).lastTransaction
      assert(txn != null, "expected a transaction to have been committed")
      assert(txn.currentState === Committed)
      assert(txn.isClosed)

      // Both the write target and the batch source participate in the transaction.
      assert(txn.catalog.txnTables.size === 2)

      val targetTxnTable = indexByName(txn.catalog.txnTables.values.toSeq)(tableNameAsString)
      assert(targetTxnTable.scanEvents.isEmpty)

      // The static source was read exactly once and its scan event was captured.
      val sourceTxnTable = indexByName(txn.catalog.txnTables.values.toSeq)(sourceNameAsString)
      assert(sourceTxnTable.scanEvents.size === 1)

      // Streaming must not add transactions to the main session catalog beyond the pre-existing
      // INSERT transaction.
      assert(catalog.observedTransactions.size === mainTxnsBefore)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("transaction is aborted when micro-batch write fails and no data is written") {
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL("value INT"))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withProperties(Collections.singletonMap(
        InMemoryBaseTable.SIMULATE_FAILED_WRITE_OPTION, "true"))
      .build()
    catalog.createTable(ident, tableInfo)

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val query = inputData.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tableNameAsString)

      inputData.addData(1, 2, 3)
      intercept[Exception] { query.processAllAvailable() }
      query.stop()

      val txn = streamCatalog(query).lastTransaction
      assert(txn != null, "expected a transaction to have been recorded")
      assert(txn.currentState === Aborted)
      assert(txn.isClosed)
      // Aborted transaction must not advance the delegate version.
      assert(table.version() === "0")

      // Transaction must be scoped to the streaming session; main session catalog is untouched.
      assert(catalog.observedTransactions.isEmpty)

      // Writes must not be visible after an aborted transaction.
      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Seq.empty)
    }
  }

  test("streaming write to non-transactional catalog does not start a transaction") {
    withSQLConf("spark.sql.catalog.nonTxnCat" -> classOf[InMemoryTableCatalog].getName) {
      val nonTxnCat = spark
        .sessionState
        .catalogManager
        .catalog("nonTxnCat")
        .asInstanceOf[InMemoryTableCatalog]
      val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL("value INT"))
      nonTxnCat.createTable(
        Identifier.of(Array("ns"), "tbl"),
        new TableInfo.Builder().withColumns(columns).build())

      withTempDir { checkpointDir =>
        val inputData = MemoryStream[Int]
        val query = inputData.toDF()
          .writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .toTable("nonTxnCat.ns.tbl")

        inputData.addData(1, 2, 3)
        query.processAllAvailable()
        query.stop()

        assert(catalog.observedTransactions.isEmpty,
          "no transaction expected for non-transactional catalog")
        checkAnswer(spark.table("nonTxnCat.ns.tbl"), Seq(Row(1), Row(2), Row(3)))
      }
    }
  }
}
