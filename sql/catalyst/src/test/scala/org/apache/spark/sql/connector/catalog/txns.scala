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
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperationBuilder, RowLevelOperationInfo, WriteBuilder}
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
class TxnTable(
    val delegate: InMemoryRowLevelOperationTable,
    schema: StructType,
    catalog: TxnTableCatalog)
  extends InMemoryRowLevelOperationTable(
    delegate.name,
    schema,
    delegate.partitioning,
    delegate.properties,
    delegate.constraints) {

  // Expose the same id as the delegate so that identity checks during transaction re-resolution
  // don't false-positive on the TxnTable wrapper having a different UUID.
  override val id: String = delegate.id

  alterTableWithData(delegate.data, schema)

  // A tracker of filters used in each scan.
  val scanEvents = new ArrayBuffer[Array[Filter]]()

  // Record scan events. This is invoked when building a scan for the particular table.
  override protected def recordScanEvent(filters: Array[Filter]): Unit = {
    scanEvents += filters
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    catalog.writeTarget = this
    super.newWriteBuilder(info)
  }

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    catalog.writeTarget = this
    super.newRowLevelOperationBuilder(info)
  }

  override def deleteWhere(filters: Array[Filter]): Unit = {
    catalog.writeTarget = this
    super.deleteWhere(filters)
  }

  // Propagates staged data and metadata changes to the delegate table.
  def commit(): Unit = {
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

// A special table catalog used in row-level operation transactions. The lifecycle of this catalog
// is tied to the transaction. A new catalog instance is created at the beginning of a transaction
// and discarded at the end. The catalog is responsible for pinning all tables involved in the
// transaction. Table changes are initially staged in memory and propagated only after an explicit
// commit.
class TxnTableCatalog(delegate: InMemoryRowLevelOperationTableCatalog) extends TableCatalog {

  private val tables: util.Map[Identifier, TxnTable] = new ConcurrentHashMap[Identifier, TxnTable]()

  var writeTarget: TxnTable = _

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
      new TxnTable(table, table.schema(), this)
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
    val newTxnTable = new TxnTable(txnTable.delegate, schema, this)
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

  // Returns all tables that participated in this transaction, keyed by identifier.
  def txnTables: scala.collection.Map[Identifier, TxnTable] = tables.asScala

  // Commit the write target table, propagating staged changes to the delegate.
  def commit(): Unit = {
    if (writeTarget != null) writeTarget.commit()
  }

  // Clear transaction context.
  def clearActiveTransaction(): Unit = {
    val txn = delegate.transaction
    delegate.lastTransaction = txn
    delegate.observedTransactions += txn
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

/**
 * An InMemoryRowLevelOperationTableCatalog that utilizes tables backed by a shared map. This
 * simulates the behavior of real catalogs (Delta, Iceberg, etc.) where multiple instances
 * of the catalog share the same underlying persistent storage, thus, they see the same tables.
 *
 * This is needed for testing execution that spans multiple Spark sessions. In particular,
 * streaming queries execute micro-batches in cloned Spark sessions. Without this, the cloned
 * spark session catalog will not see any tables created in the original session.
 *
 * Tests that use this catalog must call
 * [[SharedTablesInMemoryRowLevelOperationTableCatalog.reset()]] in `afterEach` to clear the
 * shared state between test cases.
 */
class SharedTablesInMemoryRowLevelOperationTableCatalog
    extends InMemoryRowLevelOperationTableCatalog {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    super.initialize(name, options)
    tables = SharedTablesInMemoryRowLevelOperationTableCatalog.sharedTables
  }
}

object SharedTablesInMemoryRowLevelOperationTableCatalog {
  private[catalog] val sharedTables: ConcurrentHashMap[Identifier, Table] =
    new ConcurrentHashMap[Identifier, Table]()

  def reset(): Unit = sharedTables.clear()
}
