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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType

/**
 * Companion object to track the single InMemoryStateStoreProvider instance for testing.
 */
private[sql] object InMemoryStateStoreProvider {
  private var instance: InMemoryStateStoreProvider = null
  
  private[streaming] def registerProvider(provider: InMemoryStateStoreProvider): Unit = {
    if (provider.stateStoreId != null) {
      instance = provider
    }
  }
  
  private[streaming] def getProvider(): Option[InMemoryStateStoreProvider] = {
    Option(instance)
  }
}

/**
 * Minimal in-memory state store provider for testing purposes.
 * Backed by simple HashMaps with no persistence.
 */
private[sql] class InMemoryStateStoreProvider extends StateStoreProvider {
  @volatile private var stateStoreId_ : StateStoreId = _
  private val stores = mutable.Map[String, mutable.Map[UnsafeRow, UnsafeRow]]()
  private val listStores = mutable.Map[String, mutable.Map[UnsafeRow, Array[UnsafeRow]]]()
  private val mapStores = mutable.Map[String, mutable.Map[UnsafeRow, mutable.Map[UnsafeRow, UnsafeRow]]]()
  
  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None): Unit = {
    this.stateStoreId_ = stateStoreId
    // Register this provider instance after stateStoreId is set
    InMemoryStateStoreProvider.registerProvider(this)
  }
  
  /** Access stores for testing - allows TwsTester2 to peek/set state */
  private[streaming] def getStores(): mutable.Map[String, mutable.Map[UnsafeRow, UnsafeRow]] = stores
  
  /** Access list stores for testing - allows TwsTester2 to peek/set list state */
  private[streaming] def getListStores(): mutable.Map[String, mutable.Map[UnsafeRow, Array[UnsafeRow]]] = listStores
  
  /** Access map stores for testing - allows TwsTester2 to peek/set map state */
  private[streaming] def getMapStores(): mutable.Map[String, mutable.Map[UnsafeRow, mutable.Map[UnsafeRow, UnsafeRow]]] = mapStores
  
  override def stateStoreId: StateStoreId = stateStoreId_
  
  override def close(): Unit = {
    stores.clear()
    listStores.clear()
    mapStores.clear()
  }
  
  override def getStore(version: Long, stateStoreCkptId: Option[String] = None): StateStore = {
    new InMemoryStateStore(version)
  }
  
  override def doMaintenance(): Unit = {}
  
  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = Nil
  
  override def toString(): String = s"InMemoryStateStoreProvider[id = $stateStoreId_]"
  
  private def getOrCreateMap(colFamily: String): mutable.Map[UnsafeRow, UnsafeRow] = {
    stores.getOrElseUpdate(colFamily, mutable.Map.empty[UnsafeRow, UnsafeRow])
  }
  
  class InMemoryStateStore(val version: Long) extends StateStore {
    private val updates = mutable.Map[String, mutable.Map[UnsafeRow, Option[UnsafeRow]]]()
    @volatile private var committed = false
    
    override def id: StateStoreId = stateStoreId_
    
    override def createColFamilyIfAbsent(
        colFamilyName: String,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec,
        useMultipleValuesPerKey: Boolean = false,
        isInternal: Boolean = false): Unit = {
      getOrCreateMap(colFamilyName)
    }
    
    override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
      stores.remove(colFamilyName).isDefined
    }
    
    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      val updateMap = updates.getOrElse(colFamilyName, mutable.Map.empty)
      updateMap.get(key) match {
        case Some(Some(value)) => value
        case Some(None) => null  // Key was removed
        case None => getOrCreateMap(colFamilyName).getOrElse(key, null)
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
        val storeMap = getOrCreateMap(colFamily)
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
      val storeMap = getOrCreateMap(colFamilyName)
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
      import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
      
      // Check listStores first (for ListState)
      val listIter = listStores.get(colFamilyName).flatMap(_.get(key)).map(_.iterator)
      if (listIter.isDefined) {
        return listIter.get
      }
      
      // Check mapStores (for MapState) - return composite (mapKey, mapValue) values
      val mapIter = mapStores.get(colFamilyName).flatMap(_.get(key)).map { innerMap =>
        innerMap.iterator.map { case (mapKeyRow, mapValueRow) =>
          // Create a composite row with 2 fields: mapKey and mapValue
          val compositeRow = new GenericInternalRow(Array[Any](mapKeyRow, mapValueRow))
          compositeRow.asInstanceOf[UnsafeRow]
        }
      }
      if (mapIter.isDefined) {
        return mapIter.get
      }
      
      Iterator.empty
    }
    
    override def putList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
      require(!committed, "Cannot putList after commit")
      // Store the list in listStores
      val listStore = listStores.getOrElseUpdate(colFamilyName, mutable.Map.empty)
      listStore(key.copy()) = values.map(_.copy())
    }
    
    override def merge(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      require(!committed, "Cannot merge after commit")
      // For MapState, key is grouping key, value is composite (mapKey, mapValue)
      // The composite has 2 fields: field 0 = mapKey, field 1 = mapValue
      val mapStore = mapStores.getOrElseUpdate(colFamilyName, mutable.Map.empty)
      val innerMap = mapStore.getOrElseUpdate(key.copy(), mutable.Map.empty)
      
      // Extract both mapKey (field 0) and mapValue (field 1) from the composite
      val mapKeyRow = value.getStruct(0, 1).copy()  // Get first field
      val mapValueRow = value.getStruct(1, 1).copy()  // Get second field
      innerMap(mapKeyRow) = mapValueRow
    }
    
    override def mergeList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
      throw new UnsupportedOperationException("mergeList not supported in InMemoryStateStore")
    }
    
    override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
      StateStoreCheckpointInfo(
        partitionId = stateStoreId_.partitionId,
        batchVersion = version,
        stateStoreCkptId = None,
        baseStateStoreCkptId = None
      )
    }
    
    override def metrics: StateStoreMetrics = {
      val numKeys = stores.values.map(_.size).sum.toLong
      StateStoreMetrics(numKeys, 0L, Map.empty)
    }
    
    override def hasCommitted: Boolean = committed
  }
}

