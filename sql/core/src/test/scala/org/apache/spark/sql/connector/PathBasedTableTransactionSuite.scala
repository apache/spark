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
import org.apache.spark.sql.connector.catalog.{Aborted, Committed, InMemoryRowLevelOperationTableCatalog, InMemoryTableCatalog, SharedTablesInMemoryRowLevelOperationTableCatalog, Txn, TxnTable}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Tests for transactional writes to path-based tables, where the table is identified by a
 * bare path with no catalog prefix (e.g. `/path/to/t`), or a connector-prefixed path
 * (e.g. `pathformat.`/path/to/t``). Bare paths resolve to the session catalog
 * (`spark_catalog`, set to a transactional impl); connector-prefixed paths route through
 * the SCO seam to a dedicated transactional catalog (`pathformat_cat`).
 */
class PathBasedTableTransactionSuite extends RowLevelOperationSuiteBase {

  import testImplicits._

  private val tablePath = "`/path/to/t`"
  private val tablePathWithFormat = "pathformat.`/path/to/t`"

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[SharedTablesInMemoryRowLevelOperationTableCatalog].getName)
    spark.conf.set(
      s"spark.sql.catalog.${FakePathBasedSource.CATALOG_NAME}",
      classOf[SharedTablesInMemoryRowLevelOperationTableCatalog].getName)
  }

  override def afterEach(): Unit = {
    SharedTablesInMemoryRowLevelOperationTableCatalog.reset()
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    spark.conf.unset(s"spark.sql.catalog.${FakePathBasedSource.CATALOG_NAME}")
    super.afterEach()
  }

  override protected def catalog: InMemoryRowLevelOperationTableCatalog = {
    spark.sessionState.catalogManager.v2SessionCatalog
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
  }

  // The SCO-targeted catalog that connector-prefixed paths route to. Distinct instance
  // from `catalog` (session); both share table state via the SharedTables static map.
  protected def pathformatCat: InMemoryRowLevelOperationTableCatalog = {
    spark.sessionState.catalogManager.catalog(FakePathBasedSource.CATALOG_NAME)
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
  }

  private def streamPathformatCat(
      query: StreamingQuery): InMemoryRowLevelOperationTableCatalog = {
    val session = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sparkSessionForStream
    session.sessionState.catalogManager.catalog(FakePathBasedSource.CATALOG_NAME)
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
  }

  // Bare-path tests resolve to the session catalog; connector-prefixed tests resolve via
  // SCO to `pathformat_cat`. Return whichever catalog actually ran a transaction so test
  // bodies can keep using `executeTransaction { ... }` without caring which one fired.
  override protected def executeTransaction(func: => Unit): (Txn, Map[String, TxnTable]) = {
    val (sessionTxn, tables) = super.executeTransaction(func)
    val txn = Option(sessionTxn).orElse(Option(pathformatCat.lastTransaction)).orNull
    (txn, tables)
  }

  private def createPathTable(name: String): Unit = {
    sql(s"CREATE TABLE $name (id INT, data STRING)")
  }

  test("SQL insert into bare path-based table participates in transaction") {
    createPathTable(tablePath)
    val (txn, _) = executeTransaction {
      sql(s"INSERT INTO $tablePath VALUES (1, 'a'), (2, 'b')")
    }
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    checkAnswer(spark.table(tablePath), Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("SQL insert with connector-prefixed path participates in transaction") {
    createPathTable(tablePathWithFormat)
    val (txn, _) = executeTransaction {
      sql(s"INSERT INTO $tablePathWithFormat VALUES (1, 'a'), (2, 'b')")
    }
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    // The transaction must have gone through the SCO seam to pathformat_cat, not the
    // (also-transactional) session catalog. Without these checks the assertions above
    // would silently pass when the session catalog absorbs the write.
    assert(pathformatCat.lastTransaction eq txn)
    assert(catalog.lastTransaction == null)
    checkAnswer(spark.table(tablePathWithFormat), Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("SQL insert with CTE into connector-prefixed path participates in transaction") {
    createPathTable(tablePathWithFormat)
    val (txn, _) = executeTransaction {
      sql(s"""
        |WITH cte AS (SELECT 1 AS id, 'a' AS data)
        |INSERT INTO $tablePathWithFormat SELECT * FROM cte
        |""".stripMargin)
    }
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    assert(pathformatCat.lastTransaction eq txn)
    assert(catalog.lastTransaction == null)
    checkAnswer(spark.table(tablePathWithFormat), Row(1, "a") :: Nil)
  }

  test("session-config catalog controls which catalog is enrolled in transaction") {
    withSQLConf(
        "spark.sql.catalog.txncat" -> classOf[InMemoryRowLevelOperationTableCatalog].getName,
        "spark.sql.catalog.nontxncat" -> classOf[InMemoryTableCatalog].getName) {
      val txnCat = spark.sessionState.catalogManager.catalog("txncat")
        .asInstanceOf[InMemoryRowLevelOperationTableCatalog]

      // Non-transactional catalog configured.
      withSQLConf("spark.datasource.pathformat2.catalog" -> "nontxncat") {
        createPathTable("pathformat2.`/path/to/t1`")
        sql("INSERT INTO pathformat2.`/path/to/t1` VALUES (1, 'a')")
        // The transaction was not routed to any of the transactional catalogs.
        assert(catalog.lastTransaction == null)
        assert(txnCat.lastTransaction == null)
      }

      txnCat.lastTransaction = null  // Reset to distinguish from block 1.

      // Transactional catalog configured: pathBased resolves txncat as a
      // TransactionalCatalogPlugin and opens the transaction there instead.
      withSQLConf("spark.datasource.pathformat2.catalog" -> "txncat") {
        createPathTable("pathformat2.`/path/to/t2`")
        sql("INSERT INTO pathformat2.`/path/to/t2` VALUES (1, 'a')")
        assert(txnCat.lastTransaction.currentState === Committed)
        assert(txnCat.lastTransaction.isClosed)
      }
    }
  }

  test("streaming write to path-based table participates in transaction") {
    sql(s"CREATE TABLE $tablePathWithFormat (value INT)")

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val query = inputData.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tablePathWithFormat)

      inputData.addData(1, 2, 3)
      query.processAllAvailable()
      query.stop()

      val streamCat = streamPathformatCat(query)
      val txn = streamCat.lastTransaction
      assert(txn != null, "expected a transaction to have been committed")
      assert(txn.currentState === Committed)
      assert(txn.isClosed)
      // Streaming must not add transactions to the main session's catalogs.
      assert(catalog.observedTransactions.isEmpty)
      assert(pathformatCat.observedTransactions.isEmpty)
      checkAnswer(spark.table(tablePathWithFormat), Row(1) :: Row(2) :: Row(3) :: Nil)
    }
  }

  test("streaming self-join on path-based table is tracked as a scan event") {
    sql(s"CREATE TABLE $tablePathWithFormat (value INT)")
    sql(s"INSERT INTO $tablePathWithFormat VALUES (1), (2), (3)")

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val staticData = spark.read.table(tablePathWithFormat)

      val query = inputData.toDF()
        .join(staticData, "value")
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .toTable(tablePathWithFormat)

      inputData.addData(1, 2, 3)
      query.processAllAvailable()
      query.stop()

      val streamCat = streamPathformatCat(query)
      val txn = streamCat.lastTransaction
      assert(txn != null, "expected a transaction to have been committed")
      assert(txn.currentState === Committed)
      assert(txn.isClosed)
      // The path-based table is both the write target and a batch source in the same transaction.
      assert(txn.catalog.txnTables.size === 1)
      val txnTable = txn.catalog.txnTables.values.head
      assert(txnTable.scanEvents.size === 1)
      // Streaming must not add transactions to the main session's catalogs beyond the
      // pre-existing INSERT transaction (which lives on pathformatCat, not the session).
      assert(catalog.observedTransactions.isEmpty)
      assert(pathformatCat.observedTransactions.size === 1)
    }
  }

  test("SQL insert with unregistered format produces analysis error and aborts transaction") {
    createPathTable(tablePathWithFormat)
    // "Unregistered" is not a known catalog and not registered data source.
    // So Spark falls back to treating it as a namespace in spark_catalog. The table
    // does not exist, causing an AnalysisException. The transaction is started (because
    // spark_catalog IS a TransactionalCatalogPlugin) and then aborted on failure.
    checkError(
      exception = intercept[AnalysisException] {
        sql("INSERT INTO unregistered.`/path/to/t` VALUES (1, 'a'), (2, 'b')")
      },
      condition = "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map("relationName" -> "`unregistered`.`/path/to/t`"),
      context = ExpectedContext(
        fragment = "unregistered.`/path/to/t`",
        start = -1,
        stop = -1))
    val txn = catalog.lastTransaction
    assert(txn.currentState === Aborted)
    assert(txn.isClosed)
  }

  test("path-based write with same-catalog source succeeds") {
    createPathTable(tablePathWithFormat)
    // Source must live in pathformat_cat (same as the SCO-routed write target) for the
    // source scan to be tracked in the same transaction.
    val sourceName = s"${FakePathBasedSource.CATALOG_NAME}.ns1.source"
    sql(s"CREATE TABLE $sourceName (id INT, data STRING)")
    sql(s"INSERT INTO $sourceName VALUES (1, 'a'), (2, 'b')")

    val (txn, txnTables) = executeTransaction {
      sql(s"INSERT INTO $tablePathWithFormat SELECT * FROM $sourceName WHERE id = 1")
    }
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    // Source scan with predicate was tracked via the transaction catalog.
    val sourceTxnTable = txnTables(sourceName)
    assert(sourceTxnTable.scanEvents.size === 1)
    assert(sourceTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("id", 1) => true
      case _ => false
    })
    checkAnswer(spark.table(tablePathWithFormat), Row(1, "a") :: Nil)
  }

}
