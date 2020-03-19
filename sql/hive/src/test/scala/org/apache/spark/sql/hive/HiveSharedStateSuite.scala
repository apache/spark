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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.internal.SharedState
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.Utils

class HiveSharedStateSuite extends SparkFunSuite {

  test("initial configs should be passed to SharedState but not SparkContext") {
    val conf = new SparkConf().setMaster("local").setAppName("SharedState Test")
    val sc = SparkContext.getOrCreate(conf)
    val invalidPath = "invalid/path"
    val metastorePath = Utils.createTempDir()
    val tmpDb = "tmp_db"

    // The initial configs used to generate SharedState, none of these should affect the global
    // shared SparkContext's configurations. Especially, all these configs are passed to the cloned
    // confs inside SharedState except metastore warehouse dir.
    val initialConfigs = Map("spark.foo" -> "bar",
      WAREHOUSE_PATH.key -> invalidPath,
      ConfVars.METASTOREWAREHOUSE.varname -> invalidPath,
      CATALOG_IMPLEMENTATION.key -> "hive",
      ConfVars.METASTORECONNECTURLKEY.varname ->
        s"jdbc:derby:;databaseName=$metastorePath/metastore_db;create=true",
      GLOBAL_TEMP_DATABASE.key -> tmpDb)

    val state = new SharedState(sc, initialConfigs)
    assert(state.warehousePath !== invalidPath, "warehouse path can't determine by session options")
    assert(sc.conf.get(WAREHOUSE_PATH.key) !== invalidPath,
      "warehouse conf in session options can't affect application wide spark conf")
    assert(sc.hadoopConfiguration.get(ConfVars.METASTOREWAREHOUSE.varname) !== invalidPath,
      "warehouse conf in session options can't affect application wide hadoop conf")

    assert(!state.sparkContext.conf.contains("spark.foo"),
      "static spark conf should not be affected by session")
    assert(state.externalCatalog.unwrapped.isInstanceOf[HiveExternalCatalog],
      "Initial SparkSession options can determine the catalog")
    val client = state.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
    assert(client.getConf("spark.foo", "") === "bar",
      "session level conf should be passed to catalog")
    assert(client.getConf(ConfVars.METASTOREWAREHOUSE.varname, invalidPath) !== invalidPath,
      "session level conf should be passed to catalog except warehouse dir")

    assert(state.globalTempViewManager.database === tmpDb)
  }
}
