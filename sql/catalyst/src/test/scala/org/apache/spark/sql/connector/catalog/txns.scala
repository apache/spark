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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

sealed trait TransactionState
case object Active extends TransactionState
case object Committed extends TransactionState
case object Aborted extends TransactionState

class Txn(override val catalog: TxnTableCatalog) extends Transaction {

  private[this] var state: TransactionState = Active
  private[this] var closed: Boolean = false

  def currentState: TransactionState = state

  def isClosed: Boolean = closed

  override def commit(): Unit = {
    if (closed) throw new IllegalStateException("Can't commit, already closed")
    if (state == Aborted) throw new IllegalStateException("Can't commit, already aborted")
    catalog.commit()
    this.state = Committed
  }

  // This is idempotent since nested QEs can cause multiple aborts.
  override def abort(): Unit = {
    if (state == Committed || state == Aborted) return
    this.state = Aborted
  }

  // This is idempotent since nested QEs can cause multiple aborts.
  override def close(): Unit = {
    if (!closed) {
      catalog.clearActiveTransaction()
      this.closed = true
    }
  }
}

// A special table used in row-level operation transactions. It inherits data
// from the base table upon construction and propagates staged transaction state
// back after an explicit commit.
// Note, the in-memory data store does not handle concurrency at the moment. The assumes that the
// underlying delegate table cannot change from concurrent transactions. Data sources need to
// implement isolation semantics and make sure they are enforced.
class TxnTable(val delegate: InMemoryRowLevelOperationTable, schema: StructType)
  extends InMemoryRowLevelOperationTable(
    delegate.name,
    schema,
    delegate.partitioning,
    delegate.properties,
    delegate.constraints) {

  alterTableWithData(delegate.data, schema)

  // Keep initial version to detect any changes during the transaction.
  private val initialVersion: String = version()

  // A tracker of filters used in each scan.
  val scanEvents = new ArrayBuffer[Array[Filter]]()

  // Record scan events. This is invoked when building a scan for the particular table.
  override protected def recordScanEvent(filters: Array[Filter]): Unit = {
    scanEvents += filters
  }

  // Perform commit if there are any changes. This push metadata and data changes to the
  // delegate table.
  def commit(): Unit = {
    if (version() != initialVersion) {
      delegate.dataMap.clear()
      delegate.updateColumns(columns()) // Evolve schema if needed.
      delegate.alterTableWithData(data, schema)
      delegate.replacedPartitions = replacedPartitions
      delegate.lastWriteInfo = lastWriteInfo
      delegate.lastWriteLog = lastWriteLog
      delegate.commits ++= commits
      delegate.increaseVersion()
    }
  }
}

// A special table catalog used in row-level operation transactions. The lifecycle of this catalog
// is tied to the transaction. A new catalog instance is created at the beginning of a transaction
// and discarded at the end. The catalog is responsible for pinning all tables involved in the
// transaction. Table changes are initially staged in memory and propagated only after an explicit
// commit.
class TxnTableCatalog(delegate: InMemoryRowLevelOperationTableCatalog) extends TableCatalog {

  private val tables: util.Map[Identifier, TxnTable] = new ConcurrentHashMap[Identifier, TxnTable]()

  override def name: String = delegate.name

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    throw new UnsupportedOperationException()
  }

  // This is where the table pinning logic should occur. In this implementation, a tables is loaded
  // (pinned) the first time is accessed. All subsequent accesses should return the same pinned
  // table.
  override def loadTable(ident: Identifier): Table = {
    tables.computeIfAbsent(ident, _ => {
      val table = delegate.loadTable(ident).asInstanceOf[InMemoryRowLevelOperationTable]
      new TxnTable(table, table.schema())
    })
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // AlterTable may be called by ResolveSchemaEvolution when schema evolution is enabled. Thus,
    // it needs to be transactional. The schema changes are only propagated to the delegate at
    // commit time.
    //
    // We delegate schema computation to the underlying catalog so that catalogs with special
    // handling (e.g. PartialSchemaEvolutionCatalog) have the same behaviour inside a
    // transaction.
    val txnTable = tables.get(ident)
    val schema = delegate.computeAlterTableSchema(txnTable.schema, changes.toSeq)

    if (schema.fields.isEmpty) {
      throw new IllegalArgumentException(s"Cannot drop all fields")
    }

    // TODO: We need to pass all tracked predicates to the new TXN table.
    val newTxnTable = new TxnTable(txnTable.delegate, schema)
    tables.put(ident, newTxnTable)
    newTxnTable
  }

  // TODO: Currently not transactional. Should be revised when Atomic CTAS/RTAS is implemented.
  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    delegate.createTable(ident, tableInfo)
    loadTable(ident)
  }

  // TODO: Currently not transactional. Should be revised when Atomic CTAS/RTAS is implemented.
  override def dropTable(ident: Identifier): Boolean = {
    tables.remove(ident)
    delegate.dropTable(ident)
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException()
  }

  // Invoke commit for all tables participated in the transaction. If a table is read-only
  // this is a no-op.
  def commit(): Unit = {
    tables.values.forEach(table => table.commit())
  }

  // Clear transaction context.
  def clearActiveTransaction(): Unit = {
    delegate.lastTransaction = delegate.transaction
    delegate.transaction = null
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: CatalogPlugin => this.name == that.name
      case _ => false
    }
  }

  override def hashCode(): Int = name.hashCode()
}
