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
    catalog.commit()
    this.state = Committed
  }

  override def abort(): Unit = {
    if (state == Committed || state == Aborted) return
    // if (closed) throw new IllegalStateException("Can't abort, already closed")
    this.state = Aborted
  }

  override def close(): Unit = {
    catalog.clearActiveTransaction()
    this.closed = true
  }
}

// a special table used in row-level operation transactions
// it inherits data from the base table upon construction and
// propagates staged transaction state back after an explicit commit
class TxnTable(val delegate: InMemoryRowLevelOperationTable)
  extends InMemoryRowLevelOperationTable(
    delegate.name,
    delegate.schema,
    delegate.partitioning,
    delegate.properties,
    delegate.constraints) {

  // TODO(achatzis): Rethink how schema evolution works on top of transactions.
  alterTableWithData(delegate.data, schema)

  // a tracker of filters used in each scan
  // achatzis: Non-deterministic filters?
  val scanEvents = new ArrayBuffer[Array[Filter]]()

  override protected def recordScanEvent(filters: Array[Filter]): Unit = {
    scanEvents += filters
  }

  def commit(): Unit = {
    delegate.dataMap.clear()
    // TODO(achatzis): Rethink how schema evolution works on top of transactions.
    delegate.alterTableWithData(data, delegate.schema)
    delegate.replacedPartitions = replacedPartitions
    delegate.lastWriteInfo = lastWriteInfo
    delegate.lastWriteLog = lastWriteLog
    delegate.commits ++= commits
    delegate.increaseVersion()
  }
}

// a special table catalog used in row-level operation transactions
// table changes are initially staged in memory and propagated only after an explicit commit
class TxnTableCatalog(delegate: InMemoryRowLevelOperationTableCatalog) extends TableCatalog {

  private val tables: util.Map[Identifier, TxnTable] = new ConcurrentHashMap[Identifier, TxnTable]()

  override def name: String = delegate.name

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    throw new UnsupportedOperationException()
  }

  override def loadTable(ident: Identifier): Table = {
    tables.computeIfAbsent(ident, _ => {
      val table = delegate.loadTableAs[InMemoryRowLevelOperationTable](ident)
      new TxnTable(table)
    })
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val newDelegateTable = delegate.alterTable(ident, changes: _*)
    // Compute again if absent.
    tables.remove(ident)
    newDelegateTable
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException()
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException()
  }

  def commit(): Unit = {
    tables.values.forEach(table => table.commit())
  }

  def clearActiveTransaction(): Unit = {
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
