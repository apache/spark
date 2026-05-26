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

  for (isSelfScan <- Seq(true, false))
  test("batch read from catalog-backed table inside streaming query is tracked as a " +
      s"scan event (isSelfScan=$isSelfScan)") {
    // Target table for the stream.
    createSimpleTable("value INT")

    // Pick the static (non-streaming) source table. When isSelfScan is true, the stream's
    // write target is also used as the batch source.
    val staticSourceName = if (isSelfScan) {
      tableNameAsString
    } else {
      val sourceIdent = Identifier.of(namespace, "source_table")
      val srcColumns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL("value INT"))
      catalog.createTable(sourceIdent, new TableInfo.Builder().withColumns(srcColumns).build())
      sourceNameAsString
    }
    sql(s"INSERT INTO $staticSourceName VALUES (1), (2), (3)")
    // The INSERT above runs a transaction on the main session catalog; capture the count now
    // so we can assert the streaming query does not add more.
    val mainTxnsBefore = catalog.observedTransactions.size

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]

      // spark.read produces a DataSourceV2Relation (batch), not a streaming source.
      // UnresolveRelationsInTransaction converts it to V2TableReference each micro-batch so
      // the transaction-aware catalog can record the scan event.
      val staticData = spark.read.table(staticSourceName)

      val query = inputData.toDF()
        .join(staticData, "value")
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tableNameAsString)

      // There should be no transaction yet in the cloned session.
      assert(streamCatalog(query).lastTransaction === null)

      inputData.addData(1, 2, 3)
      query.processAllAvailable()
      query.stop()

      val txn = streamCatalog(query).lastTransaction
      assert(txn != null, "expected a transaction to have been committed")
      assert(txn.currentState === Committed)
      assert(txn.isClosed)

      if (isSelfScan) {
        // Target acts as both write target and batch source.
        assert(txn.catalog.txnTables.size === 1)
        val targetTxnTable = indexByName(txn.catalog.txnTables.values.toSeq)(tableNameAsString)
        assert(targetTxnTable.scanEvents.size === 1)
      } else {
        // Both the write target and the batch source participate in the transaction.
        assert(txn.catalog.txnTables.size === 2)
        val targetTxnTable = indexByName(txn.catalog.txnTables.values.toSeq)(tableNameAsString)
        assert(targetTxnTable.scanEvents.isEmpty)
        // The static source was read exactly once and its scan event was captured.
        val sourceTxnTable = indexByName(txn.catalog.txnTables.values.toSeq)(sourceNameAsString)
        assert(sourceTxnTable.scanEvents.size === 1)
      }

      // Streaming must not add transactions to the main session catalog beyond pre-existing
      // setup transactions.
      assert(catalog.observedTransactions.size === mainTxnsBefore)

      // In the self-scan case the target was pre-populated with 1,2,3 and the streaming append
      // adds another 1,2,3 from the join, so the table ends with two copies of each value.
      val expectedRows = if (isSelfScan) {
        Seq(Row(1), Row(2), Row(3), Row(1), Row(2), Row(3))
      } else {
        Seq(Row(1), Row(2), Row(3))
      }
      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), expectedRows)
    }
  }

  test("micro-batch fails when target table schema changes between batches") {
    createSimpleTable("value INT")

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val query = inputData.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tableNameAsString)

      // Batch 1 succeeds against the original schema captured at query start.
      inputData.addData(1, 2, 3)
      query.processAllAvailable()

      val firstTxn = streamCatalog(query).lastTransaction
      assert(firstTxn != null)
      assert(firstTxn.currentState === Committed)

      // Mutate the target schema between micro-batches via the main session catalog. The
      // shared in-memory backing store makes the change visible to the streaming session.
      sql(s"ALTER TABLE $tableNameAsString ADD COLUMNS (extra STRING)")

      // Batch 2: re-resolution of the WriteTargetContext reference loads the altered table
      // and validateNoChanges rejects the added column.
      inputData.addData(4, 5, 6)
      val ex = intercept[Exception] { query.processAllAvailable() }
      assert(ex.getMessage.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS") ||
        Option(ex.getCause).exists(
          _.getMessage.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS")))
      query.stop()

      // Only batch 1's rows should be visible; batch 2 never wrote anything.
      checkAnswer(
        sql(s"SELECT value FROM $tableNameAsString"),
        Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("micro-batch fails when batch source schema changes after capture") {
    createSimpleTable("value INT")

    val sourceIdent = Identifier.of(namespace, "source_table")
    val srcColumns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL("value INT"))
    catalog.createTable(sourceIdent, new TableInfo.Builder().withColumns(srcColumns).build())
    sql(s"INSERT INTO $sourceNameAsString VALUES (1), (2), (3)")

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]

      // Capture the static source against its original schema.
      val staticData = spark.read.table(sourceNameAsString)

      // Mutate the source schema after the static reference was captured.
      sql(s"ALTER TABLE $sourceNameAsString ADD COLUMNS (extra STRING)")

      val query = inputData.toDF()
        .join(staticData, "value")
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tableNameAsString)

      inputData.addData(1, 2, 3)
      val ex = intercept[Exception] { query.processAllAvailable() }
      assert(ex.getMessage.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS") ||
        Option(ex.getCause).exists(
          _.getMessage.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS")))
      query.stop()

      // Analysis failed before any commit. The target must remain empty.
      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Seq.empty)
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
