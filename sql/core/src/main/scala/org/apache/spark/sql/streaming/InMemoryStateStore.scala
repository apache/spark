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
package org.apache.spark.sql.streaming

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType

private[sql] class InMemoryStateStore(
    val version: Long,
    provider: InMemoryStateStoreProvider) extends StateStore {
  private val updates = mutable.Map[String, mutable.Map[UnsafeRow, Option[UnsafeRow]]]()
  @volatile private var committed = false

  override def id: StateStoreId = provider.stateStoreId

  override def createColFamilyIfAbsent(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean = false,
      isInternal: Boolean = false): Unit = {
    provider.getOrCreateMap(colFamilyName)
  }

  override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    provider.valueStores.remove(colFamilyName).isDefined
  }

  override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
    val updateMap = updates.getOrElse(colFamilyName, mutable.Map.empty)
    updateMap.get(key) match {
      case Some(Some(value)) => value
      case Some(None) => null  // Key was removed
      case None => provider.getOrCreateMap(colFamilyName).getOrElse(key, null)
    }
  }

  override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    require(!committed, "Cannot put after commit")
    val updateMap = updates.getOrElseUpdate(colFamilyName, mutable.Map.empty)
    updateMap(key.copy()) = Some(value.copy())
  }

  override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
    require(!committed, "Cannot remove after commit")
    val updateMap = updates.getOrElseUpdate(colFamilyName, mutable.Map.empty)
    updateMap(key.copy()) = None
  }

  override def commit(): Long = {
    require(!committed, "Cannot commit twice")
    committed = true

    // Apply updates to the backing store
    updates.foreach { case (colFamily, updateMap) =>
      val storeMap = provider.getOrCreateMap(colFamily)
      updateMap.foreach {
        case (key, Some(value)) => storeMap(key) = value
        case (key, None) => storeMap.remove(key)
      }
    }

    version + 1
  }

  override def abort(): Unit = {
    updates.clear()
  }

  override def release(): Unit = {}

  override def iterator(colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
    val storeMap = provider.getOrCreateMap(colFamilyName)
    val updateMap = updates.getOrElse(colFamilyName, mutable.Map.empty)

    // Merge store data with updates
    val merged = mutable.Map.empty[UnsafeRow, UnsafeRow]
    storeMap.foreach { case (k, v) => merged(k) = v }
    updateMap.foreach {
      case (k, Some(v)) => merged(k) = v
      case (k, None) => merged.remove(k)
    }

    val iter = merged.iterator.map { case (k, v) => new UnsafeRowPair(k, v) }
    new StateStoreIterator(iter)
  }

  override def prefixScan(prefixKey: UnsafeRow, colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
    throw new UnsupportedOperationException("prefixScan not supported in InMemoryStateStore")
  }

  override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
    // Check listStores first (for ListState)
    val listIter = provider.listStores.get(colFamilyName).flatMap(_.get(key)).map(_.iterator)
    if (listIter.isDefined) {
      return listIter.get
    }

    Iterator.empty
  }

  override def putList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    require(!committed, "Cannot putList after commit")
    // Store the list in listStores
    val listStore = provider.listStores.getOrElseUpdate(colFamilyName, mutable.Map.empty)
    listStore(key.copy()) = values.map(_.copy())
  }

  override def merge(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    require(!committed, "Cannot merge after commit")
    throw new UnsupportedOperationException("merge not supported in InMemoryStateStore")
  }

  override def mergeList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    throw new UnsupportedOperationException("mergeList not supported in InMemoryStateStore")
  }

  override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
    StateStoreCheckpointInfo(
      partitionId = provider.stateStoreId.partitionId,
      batchVersion = version,
      stateStoreCkptId = None,
      baseStateStoreCkptId = None
    )
  }

  override def metrics: StateStoreMetrics = {
    new StateStoreMetrics(0L, 0L, Map.empty)
  }

  override def hasCommitted: Boolean = committed
}
