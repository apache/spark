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
import org.apache.spark.sql.connector.catalog.{Aborted, Committed, Identifier, InMemoryRowLevelOperationTableCatalog, InMemoryTableCatalog, SessionConfigSupport, SupportsCatalogOptions}
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for transactional writes to path-based tables, where the table is identified by a
 * bare path with no catalog prefix (e.g. `/path/to/t`), or a connector-prefixed path
 * (e.g. `pathformat.`/path/to/t``). The transactional catalog is registered as the session
 * catalog (`spark_catalog`).
 */
class PathBasedTableTransactionSuite extends RowLevelOperationSuiteBase {

  private val tablePath = "`/path/to/t`"
  private val tablePathWithFormat = "pathformat.`/path/to/t`"
  private val tablePathWithFormat2 = "pathformat2.`/path/to/t`"

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[InMemoryRowLevelOperationTableCatalog].getName)
  }

  override def afterEach(): Unit = {
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    super.afterEach()
  }

  override protected def catalog: InMemoryRowLevelOperationTableCatalog = {
    spark.sessionState.catalogManager.v2SessionCatalog
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
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
    // ns1.source is resolved via the current catalog (spark_catalog), same as the write target.
    sql("CREATE TABLE ns1.source (id INT, data STRING)")
    sql("INSERT INTO ns1.source VALUES (1, 'a'), (2, 'b')")

    val (txn, txnTables) = executeTransaction {
      sql(s"INSERT INTO $tablePathWithFormat SELECT * FROM ns1.source WHERE id = 1")
    }
    assert(txn.currentState === Committed)
    assert(txn.isClosed)
    // Source scan with predicate was tracked via the transaction catalog.
    val sourceTxnTable = txnTables("spark_catalog.ns1.source")
    assert(sourceTxnTable.scanEvents.size === 1)
    assert(sourceTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("id", 1) => true
      case _ => false
    })
    checkAnswer(spark.table(tablePathWithFormat), Row(1, "a") :: Nil)
  }

  test("path-based write with source from different catalog is rejected") {
    createPathTable(tablePathWithFormat)
    // cat is a different catalog from spark_catalog (the path-based catalog).
    sql("CREATE TABLE cat.ns1.source (id INT, data STRING)")

    val e = intercept[AnalysisException] {
      sql(s"INSERT INTO $tablePathWithFormat SELECT * FROM cat.ns1.source")
    }
    checkError(e, "TRANSACTION_MULTI_CATALOG_NOT_SUPPORTED",
      parameters = Map("txnCatalog" -> "spark_catalog", "foreignCatalogs" -> "cat"))
    assert(catalog.lastTransaction.currentState === Aborted)
    assert(catalog.lastTransaction.isClosed)
  }

  test("path-based write with source from session-config-routed catalog is rejected") {
    withSQLConf(
        "spark.sql.catalog.txncat" -> classOf[InMemoryRowLevelOperationTableCatalog].getName,
        "spark.datasource.pathformat2.catalog" -> "txncat") {
      // pathformat2 routes to txncat; create the source there.
      createPathTable(tablePathWithFormat2)
      sql(s"INSERT INTO $tablePathWithFormat2 VALUES (1, 'a')")

      // pathformat falls back to the session catalog (extractCatalog returns null).
      createPathTable(tablePathWithFormat)

      val e = intercept[AnalysisException] {
        sql(s"INSERT INTO $tablePathWithFormat SELECT * FROM $tablePathWithFormat2")
      }
      checkError(e, "TRANSACTION_MULTI_CATALOG_NOT_SUPPORTED",
        parameters = Map("txnCatalog" -> "spark_catalog", "foreignCatalogs" -> "txncat"))
      assert(catalog.lastTransaction.currentState === Aborted)
      assert(catalog.lastTransaction.isClosed)
    }
  }
}

/**
 * Simulates a path-based connector (e.g. Delta) that implements [[SupportsCatalogOptions]]
 * to route `pathformat.\`/path/to/t\`` SQL identifiers to the session catalog. Returning
 * null from [[extractCatalog]] signals that the session catalog (`spark_catalog`) owns the
 * table, matching Delta's behavior where DeltaCatalog is registered as spark_catalog.
 */
class FakePathBasedSource
    extends FakeV2ProviderWithCustomSchema
    with SupportsCatalogOptions
    with DataSourceRegister {

  override def shortName(): String = "pathformat"

  // Use the session catalog.
  override def extractCatalog(options: CaseInsensitiveStringMap): String = null

  // Not used in the transactional path.
  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = null
}

/**
 * Like [[FakePathBasedSource]] but resolves the owning catalog from the session config
 * `spark.datasource.pathformat2.catalog` instead of always returning null. This simulates
 * a connector that lets users configure the target catalog.
 */
class FakePathBasedSourceWithSessionConfig
    extends FakeV2ProviderWithCustomSchema
    with SupportsCatalogOptions
    with SessionConfigSupport
    with DataSourceRegister {

  override def shortName(): String = "pathformat2"

  override def keyPrefix: String = "pathformat2"

  override def extractCatalog(options: CaseInsensitiveStringMap): String = options.get("catalog")

  // Not used in the transactional path.
  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = null
}
