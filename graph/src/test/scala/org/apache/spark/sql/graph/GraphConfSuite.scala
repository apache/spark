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

package org.apache.spark.sql.graph

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.graph.GraphConf.{CheckpointMode, StaticDataMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.StorageLevel

class GraphConfSuite extends QueryTest with SharedSparkSession {

  test("defaults") {
    assert(GraphConf.workDir(spark).isEmpty)
    assert(GraphConf.checkpointMode(spark) == CheckpointMode.LOCAL)
    assert(GraphConf.staticDataMode(spark) == StaticDataMode.DISK)
    assert(GraphConf.checkpointInterval(spark) == 2)
    assert(GraphConf.storageLevel(spark) == StorageLevel.MEMORY_AND_DISK)
  }

  test("storage level falls back to the default cache storage level") {
    withSQLConf(SQLConf.DEFAULT_CACHE_STORAGE_LEVEL.key -> "MEMORY_ONLY") {
      assert(GraphConf.storageLevel(spark) == StorageLevel.MEMORY_ONLY)
      withSQLConf(GraphConf.STORAGE_LEVEL.key -> "DISK_ONLY") {
        assert(GraphConf.storageLevel(spark) == StorageLevel.DISK_ONLY)
      }
    }
  }

  test("invalid values") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        spark.conf.set(GraphConf.CHECKPOINT_MODE.key, "localCheckpoint")
      },
      condition = "INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS",
      parameters = Map(
        "confName" -> GraphConf.CHECKPOINT_MODE.key,
        "confValue" -> "localCheckpoint",
        "confOptions" -> "LOCAL, RELIABLE"))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        spark.conf.set(GraphConf.STATIC_DATA_MODE.key, "cache")
      },
      condition = "INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS",
      parameters = Map(
        "confName" -> GraphConf.STATIC_DATA_MODE.key,
        "confValue" -> "cache",
        "confOptions" -> "MEMORY, DISK"))
  }
}
