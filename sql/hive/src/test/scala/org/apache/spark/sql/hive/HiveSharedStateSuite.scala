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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.FileUtils

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.UI
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.Utils

class HiveSharedStateSuite extends SparkFunSuite {

  override def beforeEach(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    super.beforeEach()
  }

  test("initial configs should be passed to SharedState but not SparkContext") {
    val conf = new SparkConf().setMaster("local").setAppName("SharedState Test")
    val sc = SparkContext.getOrCreate(conf)
    val warehousePath = Utils.createTempDir().toString
    val invalidPath = "invalid/path"
    val metastorePath = Utils.createTempDir()
    val tmpDb = "tmp_db"

    // The initial configs used to generate SharedState, none of these should affect the global
    // shared SparkContext's configurations, except spark.sql.warehouse.dir.
    // Especially, all these configs are passed to the cloned confs inside SharedState for sharing
    // cross sessions.
    val initialConfigs = Map("spark.foo" -> "bar",
      WAREHOUSE_PATH.key -> warehousePath,
      "hive.metastore.warehouse.dir" -> warehousePath,
      CATALOG_IMPLEMENTATION.key -> "hive",
      "javax.jdo.option.ConnectionURL" ->
        s"jdbc:derby:;databaseName=$metastorePath/metastore_db;create=true",
      GLOBAL_TEMP_DATABASE.key -> tmpDb)

    val builder = SparkSession.builder()
    initialConfigs.foreach { case (k, v) => builder.config(k, v) }
    val ss = builder.getOrCreate()
    val state = ss.sharedState
    val qualifiedWHPath =
      FileUtils.makeQualified(new Path(warehousePath), sc.hadoopConfiguration).toString
    assert(sc.conf.get(WAREHOUSE_PATH.key) === qualifiedWHPath,
      "initial warehouse conf in session options can affect application wide spark conf")
    assert(sc.hadoopConfiguration.get("hive.metastore.warehouse.dir") === qualifiedWHPath,
      "initial warehouse conf in session options can affect application wide hadoop conf")

    assert(!state.sparkContext.conf.contains("spark.foo"),
      "static spark conf should not be affected by session")
    assert(state.externalCatalog.unwrapped.isInstanceOf[HiveExternalCatalog],
      "Initial SparkSession options can determine the catalog")
    val client = state.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
    assert(client.getConf("spark.foo", "") === "bar",
      "session level conf should be passed to catalog")
    assert(client.getConf("hive.metastore.warehouse.dir", "") === qualifiedWHPath,
      "session level conf should be passed to catalog")

    assert(state.globalTempViewManager.database === tmpDb)

   val ss2 =
     builder.config("spark.foo", "bar2222").config(WAREHOUSE_PATH.key, invalidPath).getOrCreate()

    assert(!ss2.sparkContext.conf.get(WAREHOUSE_PATH.key).contains(invalidPath),
      "warehouse conf in session options can't affect application wide spark conf")
    assert(ss2.sparkContext.hadoopConfiguration.get("hive.metastore.warehouse.dir") !==
      invalidPath, "warehouse conf in session options can't affect application wide hadoop conf")
    assert(ss.conf.get("spark.foo") === "bar2222", "session level conf should be passed to catalog")
    assert(!ss.conf.get(WAREHOUSE_PATH).contains(invalidPath),
      "session level conf should be passed to catalog")
  }

  test("SPARK-34568: When SparkContext's conf not enable hive, " +
    "we should respect `enableHiveSupport()` when build SparkSession too") {
    val conf = new SparkConf().setMaster("local").setAppName("SPARK-34568")
      .set(UI.UI_ENABLED, false)
    val sc = SparkContext.getOrCreate(conf)
    val catalog = sc.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION)
    try {
      sc.conf.set(StaticSQLConf.CATALOG_IMPLEMENTATION, "in-memory")
      val sparkSession = SparkSession.builder().enableHiveSupport().sparkContext(sc).getOrCreate()
      assert(
        sparkSession.sparkContext.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION) === "in-memory")
      assert(sparkSession.sharedState.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION) === "hive")
      assert(sparkSession.sessionState.catalog.getClass
        .getCanonicalName.contains("HiveSessionCatalog"))
    } finally {
      sc.conf.set(StaticSQLConf.CATALOG_IMPLEMENTATION, catalog)
    }
  }
}
