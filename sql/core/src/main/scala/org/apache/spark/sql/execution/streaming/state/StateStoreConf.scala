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

import org.apache.spark.sql.internal.SQLConf

/** A class that contains configuration parameters for [[StateStore]]s. */
class StateStoreConf(@transient private val sqlConf: SQLConf)
  extends Serializable {

  def this() = this(new SQLConf)

  /**
   * Minimum number of delta files in a chain after which HDFSBackedStateStore will
   * consider generating a snapshot.
   */
  val minDeltasForSnapshot: Int = sqlConf.stateStoreMinDeltasForSnapshot

  /** Minimum versions a State Store implementation should retain to allow rollbacks */
  val minVersionsToRetain: Int = sqlConf.minBatchesToRetain

  /** Maximum count of versions a State Store implementation should retain in memory */
  val maxVersionsToRetainInMemory: Int = sqlConf.maxBatchesToRetainInMemory

  /**
   * Optional fully qualified name of the subclass of [[StateStoreProvider]]
   * managing state data. That is, the implementation of the State Store to use.
   */
  val providerClass: String = sqlConf.stateStoreProviderClass

  /**
   * Additional configurations related to state store. This will capture all configs in
   * SQLConf that start with `spark.sql.streaming.stateStore.` */
  val confs: Map[String, String] =
    sqlConf.getAllConfs.filter(_._1.startsWith("spark.sql.streaming.stateStore."))
}

object StateStoreConf {
  val empty = new StateStoreConf()

  def apply(conf: SQLConf): StateStoreConf = new StateStoreConf(conf)
}
