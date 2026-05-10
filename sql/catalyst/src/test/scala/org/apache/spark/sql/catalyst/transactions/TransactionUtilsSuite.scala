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

package org.apache.spark.sql.catalyst.transactions

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, TransactionalCatalogPlugin}
import org.apache.spark.sql.connector.catalog.transactions.{Transaction, TransactionInfo}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TransactionUtilsSuite extends SparkFunSuite {
  val testCatalogName = "test_catalog"

  // --- Helpers ---------------------------------------------------------------
  private def mockCatalog(catalogName: String): CatalogPlugin = new CatalogPlugin {
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
    override def name(): String = catalogName
  }

  private val emptyFunction = () => ()
  private class TestTransaction(
      catalogName: String,
      onCommit: () => Unit = emptyFunction,
      onAbort: () => Unit = emptyFunction,
      onClose: () => Unit = emptyFunction) extends Transaction {
    var committed = false
    var aborted = false
    var closed = false

    override def catalog(): CatalogPlugin = mockCatalog(catalogName)
    override def commit(): Unit = { committed = true; onCommit() }
    override def abort(): Unit = { aborted = true; onAbort() }
    override def close(): Unit = { closed = true; onClose() }
  }

  private def mockTransactionalCatalog(
      catalogName: String,
      txnCatalogName: String = null): TransactionalCatalogPlugin = {
    val resolvedTxnCatalogName = Option(txnCatalogName).getOrElse(catalogName)
    new TransactionalCatalogPlugin {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = catalogName
      override def beginTransaction(info: TransactionInfo): Transaction =
        new TestTransaction(resolvedTxnCatalogName)
    }
  }

  // --- Commit ----------------------------------------------------------------
  test("commit: calls commit then close") {
    val txn = new TestTransaction(testCatalogName)
    TransactionUtils.commit(txn)
    assert(txn.committed)
    assert(txn.closed)
  }

  test("commit: close is called even if commit fails") {
    val txn = new TestTransaction(
      testCatalogName, onCommit = () => throw new RuntimeException("commit failed"))
    intercept[RuntimeException] { TransactionUtils.commit(txn) }
    assert(txn.closed)
  }

  // --- Abort -----------------------------------------------------------------
  test("abort: calls abort then close") {
    val txn = new TestTransaction(testCatalogName)
    TransactionUtils.abort(txn)
    assert(txn.aborted)
    assert(txn.closed)
  }

  test("abort: close is called even if abort fails") {
    val txn = new TestTransaction(testCatalogName,
      onAbort = () => throw new RuntimeException("abort failed"))
    intercept[RuntimeException] { TransactionUtils.abort(txn) }
    assert(txn.closed)
  }

  // --- Begin Transaction -----------------------------------------------------
  test("beginTransaction: returns transaction when catalog names match") {
    val catalog = mockTransactionalCatalog(testCatalogName)
    val txn = TransactionUtils.beginTransaction(catalog)
    assert(txn.catalog().name() == testCatalogName)
  }

  test("beginTransaction: fails when transaction catalog name does not match") {
    val catalog = mockTransactionalCatalog(catalogName = testCatalogName, txnCatalogName = "other")
    val e = intercept[SparkException] {
      TransactionUtils.beginTransaction(catalog)
    }
    assert(e.getMessage.contains("other"))
    assert(e.getMessage.contains(testCatalogName))
  }

  test("beginTransaction: aborts and closes transaction on catalog name mismatch") {
    var aborted = false
    var closed = false
    val catalog = new TransactionalCatalogPlugin {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = testCatalogName
      override def beginTransaction(info: TransactionInfo): Transaction =
        new TestTransaction(
          "other",
          onAbort = () => { aborted = true },
          onClose = () => { closed = true })
    }
    intercept[SparkException] { TransactionUtils.beginTransaction(catalog) }
    assert(aborted)
    assert(closed)
  }
}
