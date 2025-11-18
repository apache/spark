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
  private[streaming] val valueStores = mutable.Map[String, mutable.Map[UnsafeRow, UnsafeRow]]()
  private[streaming] val listStores = mutable.Map[String, mutable.Map[UnsafeRow, Array[UnsafeRow]]]()
  
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
  private[streaming] def getStores(): mutable.Map[String, mutable.Map[UnsafeRow, UnsafeRow]] = valueStores
  
  /** Access list stores for testing - allows TwsTester2 to peek/set list state */
  private[streaming] def getListStores(): mutable.Map[String, mutable.Map[UnsafeRow, Array[UnsafeRow]]] = listStores

  
  override def stateStoreId: StateStoreId = stateStoreId_
  
  override def close(): Unit = {
    valueStores.clear()
    listStores.clear()
  }
  
  override def getStore(version: Long, stateStoreCkptId: Option[String] = None): StateStore = {
    new InMemoryStateStore(version, this)
  }
  
  override def doMaintenance(): Unit = {}
  
  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = Nil
  
  override def toString(): String = s"InMemoryStateStoreProvider[id = $stateStoreId_]"

  private[streaming] def getOrCreateMap(colFamily: String): mutable.Map[UnsafeRow, UnsafeRow] = {
    valueStores.getOrElseUpdate(colFamily, mutable.Map.empty[UnsafeRow, UnsafeRow])
  }
}

