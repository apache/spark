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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.internal.SQLConf

/** A class that contains configuration parameters for [[StateStore]]s. */
class StateStoreConf(
    @transient private val sqlConf: SQLConf,
    val extraOptions: Map[String, String] = Map.empty)
  extends Serializable {

  def this() = this(new SQLConf)

  /**
   * Size of MaintenanceThreadPool to perform maintenance tasks for StateStore
   */
  val numStateStoreMaintenanceThreads: Int = sqlConf.numStateStoreMaintenanceThreads

  /**
   * Minimum number of delta files in a chain after which HDFSBackedStateStore will
   * consider generating a snapshot.
   */
  val minDeltasForSnapshot: Int = sqlConf.stateStoreMinDeltasForSnapshot

  /** Minimum versions a State Store implementation should retain to allow rollbacks */
  val minVersionsToRetain: Int = sqlConf.minBatchesToRetain

  /**
   * Minimum number of stale checkpoint versions that need to be present in the DFS
   * checkpoint directory for old state checkpoint version deletion to be invoked.
   * This is to amortize the cost of discovering and deleting old checkpoint versions.
   */
  val minVersionsToDelete: Long =
    Math.round(sqlConf.ratioExtraSpaceAllowedInCheckpoint * sqlConf.minBatchesToRetain)

  /** Maximum count of versions a State Store implementation should retain in memory */
  val maxVersionsToRetainInMemory: Int = sqlConf.maxBatchesToRetainInMemory

  /**
   * Optional fully qualified name of the subclass of [[StateStoreProvider]]
   * managing state data. That is, the implementation of the State Store to use.
   */
  val providerClass: String = sqlConf.stateStoreProviderClass

  /** Whether validate the underlying format or not. */
  val formatValidationEnabled: Boolean = sqlConf.stateStoreFormatValidationEnabled

  /**
   * Whether to validate the value side. This config is applied to both validators as below:
   *
   * - whether to validate the value format when the format validation is enabled.
   * - whether to validate the value schema when the state schema check is enabled.
   */
  val formatValidationCheckValue: Boolean =
    extraOptions.getOrElse(StateStoreConf.FORMAT_VALIDATION_CHECK_VALUE_CONFIG, "true") == "true"

  /** Whether to skip null values for hash based stream-stream joins. */
  val skipNullsForStreamStreamJoins: Boolean = sqlConf.stateStoreSkipNullsForStreamStreamJoins

  /** The compression codec used to compress delta and snapshot files. */
  val compressionCodec: String = sqlConf.stateStoreCompressionCodec

  /** whether to validate state schema during query run. */
  val stateSchemaCheckEnabled = sqlConf.isStateSchemaCheckEnabled

  /** The interval of maintenance tasks. */
  val maintenanceInterval = sqlConf.streamingMaintenanceInterval

  /**
   * When creating new state store checkpoint, which format version to use.
   */
  val ifEnableCheckpointId = StatefulOperatorStateInfo.ifEnableCheckpointId(sqlConf)

  /**
   * Additional configurations related to state store. This will capture all configs in
   * SQLConf that start with `spark.sql.streaming.stateStore.`
   */
  val sqlConfs: Map[String, String] =
    sqlConf.getAllConfs.filter(_._1.startsWith("spark.sql.streaming.stateStore."))
}

object StateStoreConf {
  val FORMAT_VALIDATION_CHECK_VALUE_CONFIG = "formatValidationCheckValue"

  val empty = new StateStoreConf()

  def apply(conf: SQLConf): StateStoreConf = new StateStoreConf(conf)
}
