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

package org.apache.spark.sql.internal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, InMemoryCatalog}
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.ui.{SQLListener, SQLTab}
import org.apache.spark.util.{MutableURLClassLoader, Utils}


/**
 * A class that holds all state shared across sessions in a given [[SQLContext]].
 */
private[sql] class SharedState(val sparkContext: SparkContext) extends Logging {

  /**
   * Class for caching query results reused in future executions.
   */
  val cacheManager: CacheManager = new CacheManager

  /**
   * A listener for SQL-specific [[org.apache.spark.scheduler.SparkListenerEvent]]s.
   */
  val listener: SQLListener = createListenerAndUI(sparkContext)

  {
    val configFile = Utils.getContextOrSparkClassLoader.getResource("hive-site.xml")
    if (configFile != null) {
      sparkContext.hadoopConfiguration.addResource(configFile)
    }
  }

  /**
   * A catalog that interacts with external systems.
   */
  lazy val externalCatalog: ExternalCatalog = new InMemoryCatalog(sparkContext.hadoopConfiguration)

  /**
   * A classloader used to load all user-added jar.
   */
  val jarClassLoader = new NonClosableMutableURLClassLoader(
    org.apache.spark.util.Utils.getContextOrSparkClassLoader)

  {
    // Set the Hive metastore warehouse path to the one we use
    val tempConf = new SQLConf
    sparkContext.conf.getAll.foreach { case (k, v) => tempConf.setConfString(k, v) }
    val hiveWarehouseDir = sparkContext.hadoopConfiguration.get("hive.metastore.warehouse.dir")
    if (hiveWarehouseDir != null && !tempConf.contains(SQLConf.WAREHOUSE_PATH.key)) {
      // If hive.metastore.warehouse.dir is set and spark.sql.warehouse.dir is not set,
      // we will respect the value of hive.metastore.warehouse.dir.
      tempConf.setConfString(SQLConf.WAREHOUSE_PATH.key, hiveWarehouseDir)
      sparkContext.conf.set(SQLConf.WAREHOUSE_PATH.key, hiveWarehouseDir)
      logInfo(s"${SQLConf.WAREHOUSE_PATH.key} is not set, but hive.metastore.warehouse.dir " +
        s"is set. Setting ${SQLConf.WAREHOUSE_PATH.key} to the value of " +
        s"hive.metastore.warehouse.dir ('$hiveWarehouseDir').")
    } else {
      // If spark.sql.warehouse.dir is set, we will override hive.metastore.warehouse.dir using
      // the value of spark.sql.warehouse.dir.
      // When neither spark.sql.warehouse.dir nor hive.metastore.warehouse.dir is set,
      // we will set hive.metastore.warehouse.dir to the default value of spark.sql.warehouse.dir.
      sparkContext.conf.set("hive.metastore.warehouse.dir", tempConf.warehousePath)
    }

    logInfo(s"Warehouse path is '${tempConf.warehousePath}'.")
  }

  /**
   * Create a SQLListener then add it into SparkContext, and create a SQLTab if there is SparkUI.
   */
  private def createListenerAndUI(sc: SparkContext): SQLListener = {
    if (SparkSession.sqlListener.get() == null) {
      val listener = new SQLListener(sc.conf)
      if (SparkSession.sqlListener.compareAndSet(null, listener)) {
        sc.addSparkListener(listener)
        sc.ui.foreach(new SQLTab(listener, _))
      }
    }
    SparkSession.sqlListener.get()
  }
}


/**
 * URL class loader that exposes the `addURL` and `getURLs` methods in URLClassLoader.
 * This class loader cannot be closed (its `close` method is a no-op).
 */
private[sql] class NonClosableMutableURLClassLoader(parent: ClassLoader)
  extends MutableURLClassLoader(Array.empty, parent) {

  override def close(): Unit = {}
}
