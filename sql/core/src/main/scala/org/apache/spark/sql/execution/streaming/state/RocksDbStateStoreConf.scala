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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config.ConfigBuilder

object RocksDbStateStoreConf {

  private[spark] val ROCKSDB_STATE_STORE_DATA_BLOCK_SIZE =
    ConfigBuilder("spark.sql.streaming.stateStore.rocksDb.blockSizeInKB")
      .doc(
        "The maximum size (in KB) of packed data in a block of a table file. " +
          "When reading from a table, an entire block is loaded into memory")
      .intConf
      .createWithDefault(32)

  private[spark] val ROCKSDB_STATE_STORE_MEMTABLE_BUDGET =
    ConfigBuilder("spark.sql.streaming.stateStore.rocksDb.memtableBudgetInMB")
      .doc("The maximum size (in MB) of memory to be used to optimize level style compaction")
      .intConf
      .createWithDefault(1024)

  private[spark] val ROCKSDB_STATE_STORE_CACHE_SIZE =
    ConfigBuilder("spark.sql.streaming.stateStore.rocksDb.cacheSizeInMB")
      .doc("The maximum size (in MB) of in-memory LRU cache for RocksDB operations")
      .intConf
      .createWithDefault(512)

  private[spark] val ROCKSDB_STATE_STORE_ENABLE_STATS =
    ConfigBuilder("spark.sql.streaming.stateStore.rocksDb.enableDbStats")
      .doc("Enable statistics for rocksdb for debugging and reporting")
      .booleanConf
      .createWithDefault(false)

  val blockSizeInKB: Int = Option(SparkEnv.get)
    .map(_.conf.get(ROCKSDB_STATE_STORE_DATA_BLOCK_SIZE))
    .getOrElse(32)

  val memtableBudgetInMB: Int = Option(SparkEnv.get)
    .map(_.conf.get(ROCKSDB_STATE_STORE_MEMTABLE_BUDGET))
    .getOrElse(1024)

  val cacheSize: Int = Option(SparkEnv.get)
    .map(_.conf.get(RocksDbStateStoreConf.ROCKSDB_STATE_STORE_CACHE_SIZE))
    .getOrElse(512)

  val enableStats: Boolean = Option(SparkEnv.get)
    .map(_.conf.get(ROCKSDB_STATE_STORE_ENABLE_STATS))
    .getOrElse(false)

}
