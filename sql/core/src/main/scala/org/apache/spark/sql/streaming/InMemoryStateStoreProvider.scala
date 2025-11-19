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

private[sql] class InMemoryStateStore(provider: InMemoryStateStoreProvider, val version: Long)
    extends StateStore {

  override def id: StateStoreId = provider.stateStoreId

  private def store: mutable.Map[String, mutable.Map[UnsafeRow, List[UnsafeRow]]] = {
    provider.stores(version.toInt + 1)
  }

  override def createColFamilyIfAbsent(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean = false,
      isInternal: Boolean = false): Unit = {
    if (!store.contains(colFamilyName)) {
      store.put(colFamilyName, mutable.Map[UnsafeRow, List[UnsafeRow]]())
      provider.keySchemas_.put(colFamilyName, keySchema)
    }
  }

  override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    require(!hasCommitted)
    store.remove(colFamilyName).isDefined
  }

  override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
    val list: List[UnsafeRow] = store(colFamilyName).getOrElse(key, List())
    list.headOption.orNull
  }

  override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    require(!hasCommitted)
    store(colFamilyName).put(key.copy(), List(value.copy()))
  }

  override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
    require(!hasCommitted)
    store(colFamilyName).remove(key)
  }

  override def commit(): Long = {
    require(!hasCommitted)
    provider.stores += mutable.Map.from(provider.stores.last)
    version + 1L
  }

  override def abort(): Unit = {
    require(!hasCommitted)
    require(version == provider.uncommittedVersion)
    val n = provider.stores.size
    provider.stores(n - 1) = mutable.Map.from(provider.stores(n - 2))
  }

  override def release(): Unit = {
    // TODO: maybe delete this inside provider?
  }

  override def iterator(colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
    val m = store(colFamilyName)
    val iter: Iterator[UnsafeRowPair] = m.iterator.flatMap {
      case (k, vs) =>
        vs.iterator.map { v =>
          new UnsafeRowPair(k, v)
        }
    }
    return new StateStoreIterator(iter)
  }

  override def prefixScan(
      prefixKey: UnsafeRow,
      colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
    val numPrefixCols = prefixKey.numFields
    val keySchema = provider.keySchemas_.getOrElse(colFamilyName, null)

    if (keySchema == null) {
      throw new IllegalStateException(s"Key schema not found for column family: $colFamilyName")
    }

    // Filter keys by comparing the first numPrefixCols fields
    def isPrefixOf(prefixKey: UnsafeRow, key: UnsafeRow): Boolean = {
      val numPrefixCols = prefixKey.numFields
      if (key.numFields < numPrefixCols) {
        false
      } else {
        var matches = true
        var i = 0
        while (i < numPrefixCols && matches) {
          if (prefixKey.isNullAt(i) && key.isNullAt(i)) {
            // Both null, continue
          } else if (prefixKey.isNullAt(i) || key.isNullAt(i)) {
            matches = false
          } else {
            val dataType = keySchema(i).dataType
            val prefixValue = prefixKey.get(i, dataType)
            val keyValue = key.get(i, dataType)
            if (prefixValue != keyValue) {
              matches = false
            }
          }
          i += 1
        }
        matches
      }
    }

    val iter = iterator(colFamilyName).filter { pair =>
      isPrefixOf(prefixKey, pair.key)
    }
    new StateStoreIterator(iter)
  }

  override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
    val m = store(colFamilyName)
    if (m.contains(key)) {
      return m.get(key).get.iterator
    }

    Iterator.empty
  }

  override def putList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    require(!hasCommitted)
    val m = store(colFamilyName)
    m.put(key.copy(), values.map(v => v.copy()).toList)
  }

  override def merge(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    require(!hasCommitted)
    val m = store(colFamilyName)
    m.put(key, m.getOrElse(key, List()) ++ List(value.copy()))
  }

  override def mergeList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    require(!hasCommitted)
    val m = store(colFamilyName)
    m.put(key, m.getOrElse(key, List()) ++ values.map(v => v.copy()).toList)
  }

  override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
    throw new UnsupportedOperationException("getStateStoreCheckpointInfo not supported")
  }

  override def metrics: StateStoreMetrics = {
    new StateStoreMetrics(0L, 0L, Map.empty)
  }

  override def hasCommitted: Boolean = version < provider.uncommittedVersion
}

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
  @volatile private[streaming] var keySchemas_ : mutable.Map[String, StructType] = mutable.Map.empty
  @volatile private var numColsPrefixKey_ : Int = 0

  // By definition, last one is uncommited, everything else is commited.
  // State Store (version v) points to stores(v+1).
  // Therefore:
  //   - latest commited version is stores.size-3.
  //   - current working (uncommitted) version is stores.size -2.
  private[streaming] val stores =
    mutable.ArrayBuffer[mutable.Map[String, mutable.Map[UnsafeRow, List[UnsafeRow]]]]()
  private[streaming] def uncommittedVersion: Int = stores.size - 2

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
    this.keySchemas_ = mutable.Map(StateStore.DEFAULT_COL_FAMILY_NAME -> keySchema)

    // Extract numColsPrefixKey from keyStateEncoderSpec
    keyStateEncoderSpec match {
      case PrefixKeyScanStateEncoderSpec(_, numCols) => this.numColsPrefixKey_ = numCols
      case _ => this.numColsPrefixKey_ = 0
    }

    // Register this provider instance after stateStoreId is set
    InMemoryStateStoreProvider.registerProvider(this)

    // Creates snapshot version 0, which is empty and committed.
    stores += mutable.Map[String, mutable.Map[UnsafeRow, List[UnsafeRow]]]()
    // Creates snapshot version 1, which is empty and uncommitted.
    stores += mutable.Map[String, mutable.Map[UnsafeRow, List[UnsafeRow]]]()
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def close(): Unit = {
    stores.clear()
  }

  override def getStore(version: Long, stateStoreCkptId: Option[String] = None): StateStore = {
    require(0L <= version)
    require(
      version <= stores.size - 2,
      s"Requested version $version, while stores.size=${stores.size}"
    )
    return new InMemoryStateStore(this, version)
  }

  def getLatestStore(): StateStore = {
    return new InMemoryStateStore(this, uncommittedVersion)
  }

  override def doMaintenance(): Unit = {}

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = Nil

  override def toString(): String = s"InMemoryStateStoreProvider[id = $stateStoreId_]"

}
