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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.TempVariableManager
import org.apache.spark.sql.connector.catalog.transactions.Transaction

/**
 * A [[CatalogManager]] decorator that redirects catalog lookups to the transaction's catalog
 * instance when names match, ensuring table loads during analysis are scoped to the transaction.
 * All mutable state (current catalog, current namespace, loaded catalogs) is delegated to the
 * wrapped [[CatalogManager]].
 */
// TODO: Extracting a CatalogManager trait (so this class can implement it instead of extending
//  CatalogManager) would eliminate the inherited mutable state that this decorator doesn't use.
private[sql] class TransactionAwareCatalogManager(
    delegate: CatalogManager,
    txn: Transaction)
  extends CatalogManager(delegate.defaultSessionCatalog, delegate.v1SessionCatalog) {

  override val tempVariableManager: TempVariableManager = delegate.tempVariableManager

  override def transaction: Option[Transaction] = Some(txn)

  override def withTransaction(newTxn: Transaction): CatalogManager =
    throw SparkException.internalError("Cannot nest transactions: a transaction is already active.")

  override def catalog(name: String): CatalogPlugin = {
    val resolved = delegate.catalog(name)
    if (txn.catalog.name() == resolved.name()) txn.catalog else resolved
  }

  override def currentCatalog: CatalogPlugin = {
    val c = delegate.currentCatalog
    if (txn.catalog.name() == c.name()) txn.catalog else c
  }

  override def currentNamespace: Array[String] = delegate.currentNamespace

  override def setCurrentNamespace(namespace: Array[String]): Unit =
    delegate.setCurrentNamespace(namespace)

  override def setCurrentCatalog(catalogName: String): Unit =
    delegate.setCurrentCatalog(catalogName)

  override def listCatalogs(pattern: Option[String]): Seq[String] =
    delegate.listCatalogs(pattern)
}
