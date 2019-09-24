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
import org.apache.spark.util.Utils

class RocksDbStateStoreConf(@transient private val stateStoreConf: StateStoreConf)
  extends Serializable {

  private val DEFAULT_BLOCKSIZE_IN_KB = 32
  private val DEFAULT_MEMTABLE_BUDGET_IN_MB = 1024
  private val DEFAULT_CACHE_SIZE_IN_MB = 512

  def this() = this(StateStoreConf.empty)

  val blockSizeInKB: Int = stateStoreConf.sqlConf
    .getConf(SQLConf.ROCKSDB_STATE_STORE_DATA_BLOCK_SIZE)
    .getOrElse(DEFAULT_BLOCKSIZE_IN_KB)

  val memtableBudgetInMB: Int = stateStoreConf.sqlConf
    .getConf(SQLConf.ROCKSDB_STATE_STORE_MEMTABLE_BUDGET)
    .getOrElse(DEFAULT_MEMTABLE_BUDGET_IN_MB)

  val cacheSizeInMB: Int = stateStoreConf.sqlConf
    .getConf(SQLConf.ROCKSDB_STATE_STORE_CACHE_SIZE)
    .getOrElse(DEFAULT_CACHE_SIZE_IN_MB)

  val enableStats: Boolean = stateStoreConf.sqlConf
    .getConf(SQLConf.ROCKSDB_STATE_STORE_ENABLE_STATS)
    .getOrElse(false)

  val localDir: String = stateStoreConf.sqlConf
    .getConf(SQLConf.ROCKSDB_STATE_STORE_DATA_LOCAL_DIR)
    .getOrElse(Utils.createTempDir().getAbsolutePath)
}
