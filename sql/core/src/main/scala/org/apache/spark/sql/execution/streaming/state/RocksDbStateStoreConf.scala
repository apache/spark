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

import org.apache.spark.util.Utils

class RocksDbStateStoreConf(@transient private val stateStoreConf: StateStoreConf)
  extends Serializable {

  import RocksDbStateStoreConf._

  def this() = this(StateStoreConf.empty)

  val blockSizeInKB: Int = stateStoreConf.confs
    .getOrElse(BLOCK_SIZE_KEY, DEFAULT_BLOCKSIZE_IN_KB.toString).toInt

  val memtableBudgetInMB: Int = stateStoreConf.confs
    .getOrElse(MEMTABLE_BUDGET_KEY, DEFAULT_MEMTABLE_BUDGET_IN_MB.toString).toInt

  val enableStats: Boolean = stateStoreConf.confs
    .getOrElse(ENABLE_STATS_KEY, "false").toBoolean

  val localDir: String = stateStoreConf.confs
    .getOrElse(LOCAL_DIR_KEY, Utils.createTempDir().getAbsolutePath)
}

object RocksDbStateStoreConf {
  val DEFAULT_BLOCKSIZE_IN_KB = 32
  val DEFAULT_MEMTABLE_BUDGET_IN_MB = 1024
  val DEFAULT_CACHE_SIZE_IN_MB = 512

  val BLOCK_SIZE_KEY = "spark.sql.streaming.stateStore.rocksDb.blockSizeInKB"
  val MEMTABLE_BUDGET_KEY = "spark.sql.streaming.stateStore.rocksDb.memtableBudgetInMB"
  val CACHE_SIZE_KEY = "spark.sql.streaming.stateStore.rocksDb.cacheSizeInMB"
  val ENABLE_STATS_KEY = "spark.sql.streaming.stateStore.rocksDb.enableDbStats"
  val LOCAL_DIR_KEY = "spark.sql.streaming.stateStore.rocksDb.localDir"
}
