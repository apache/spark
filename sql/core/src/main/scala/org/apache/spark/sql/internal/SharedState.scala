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

import java.net.URL
import java.util.Locale

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SQLTab}
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.{MutableURLClassLoader, Utils}


/**
 * A class that holds all state shared across sessions in a given [[SQLContext]].
 */
private[sql] class SharedState(val sparkContext: SparkContext) extends Logging {

  // Load hive-site.xml into hadoopConf and determine the warehouse path we want to use, based on
  // the config from both hive and Spark SQL. Finally set the warehouse config value to sparkConf.
  val warehousePath: String = {
    val configFile = Utils.getContextOrSparkClassLoader.getResource("hive-site.xml")
    if (configFile != null) {
      logInfo(s"loading hive config file: $configFile")
      sparkContext.hadoopConfiguration.addResource(configFile)
    }

    // hive.metastore.warehouse.dir only stay in hadoopConf
    sparkContext.conf.remove("hive.metastore.warehouse.dir")
    // Set the Hive metastore warehouse path to the one we use
    val hiveWarehouseDir = sparkContext.hadoopConfiguration.get("hive.metastore.warehouse.dir")
    if (hiveWarehouseDir != null && !sparkContext.conf.contains(WAREHOUSE_PATH.key)) {
      // If hive.metastore.warehouse.dir is set and spark.sql.warehouse.dir is not set,
      // we will respect the value of hive.metastore.warehouse.dir.
      sparkContext.conf.set(WAREHOUSE_PATH.key, hiveWarehouseDir)
      logInfo(s"${WAREHOUSE_PATH.key} is not set, but hive.metastore.warehouse.dir " +
        s"is set. Setting ${WAREHOUSE_PATH.key} to the value of " +
        s"hive.metastore.warehouse.dir ('$hiveWarehouseDir').")
      hiveWarehouseDir
    } else {
      // If spark.sql.warehouse.dir is set, we will override hive.metastore.warehouse.dir using
      // the value of spark.sql.warehouse.dir.
      // When neither spark.sql.warehouse.dir nor hive.metastore.warehouse.dir is set,
      // we will set hive.metastore.warehouse.dir to the default value of spark.sql.warehouse.dir.
      val sparkWarehouseDir = sparkContext.conf.get(WAREHOUSE_PATH)
      logInfo(s"Setting hive.metastore.warehouse.dir ('$hiveWarehouseDir') to the value of " +
        s"${WAREHOUSE_PATH.key} ('$sparkWarehouseDir').")
      sparkContext.hadoopConfiguration.set("hive.metastore.warehouse.dir", sparkWarehouseDir)
      sparkWarehouseDir
    }
  }
  logInfo(s"Warehouse path is '$warehousePath'.")


  /**
   * Class for caching query results reused in future executions.
   */
  val cacheManager: CacheManager = new CacheManager

  /**
   * A status store to query SQL status/metrics of this Spark application, based on SQL-specific
   * [[org.apache.spark.scheduler.SparkListenerEvent]]s.
   */
  val statusStore: SQLAppStatusStore = {
    val kvStore = sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new SQLAppStatusListener(sparkContext.conf, kvStore, live = true)
    sparkContext.listenerBus.addToStatusQueue(listener)
    val statusStore = new SQLAppStatusStore(kvStore, Some(listener))
    sparkContext.ui.foreach(new SQLTab(statusStore, _))
    statusStore
  }

  /**
   * A catalog that interacts with external systems.
   */
  lazy val externalCatalog: ExternalCatalogWithListener = {
    val externalCatalog = SharedState.reflect[ExternalCatalog, SparkConf, Configuration](
      SharedState.externalCatalogClassName(sparkContext.conf),
      sparkContext.conf,
      sparkContext.hadoopConfiguration)

    val defaultDbDefinition = CatalogDatabase(
      SessionCatalog.DEFAULT_DATABASE,
      "default database",
      CatalogUtils.stringToURI(warehousePath),
      Map())
    // Create default database if it doesn't exist
    if (!externalCatalog.databaseExists(SessionCatalog.DEFAULT_DATABASE)) {
      // There may be another Spark application creating default database at the same time, here we
      // set `ignoreIfExists = true` to avoid `DatabaseAlreadyExists` exception.
      externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)
    }

    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener(new ExternalCatalogEventListener {
      override def onEvent(event: ExternalCatalogEvent): Unit = {
        sparkContext.listenerBus.post(event)
      }
    })

    wrapped
  }

  /**
   * A manager for global temporary views.
   */
  lazy val globalTempViewManager: GlobalTempViewManager = {
    // System preserved database should not exists in metastore. However it's hard to guarantee it
    // for every session, because case-sensitivity differs. Here we always lowercase it to make our
    // life easier.
    val globalTempDB = sparkContext.conf.get(GLOBAL_TEMP_DATABASE).toLowerCase(Locale.ROOT)
    if (externalCatalog.databaseExists(globalTempDB)) {
      throw new SparkException(
        s"$globalTempDB is a system preserved database, please rename your existing database " +
          "to resolve the name conflict, or set a different value for " +
          s"${GLOBAL_TEMP_DATABASE.key}, and launch your Spark application again.")
    }
    new GlobalTempViewManager(globalTempDB)
  }

  /**
   * A classloader used to load all user-added jar.
   */
  val jarClassLoader = new NonClosableMutableURLClassLoader(
    org.apache.spark.util.Utils.getContextOrSparkClassLoader)

}

object SharedState extends Logging {
  try {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
  } catch {
    case e: Error =>
      logWarning("URL.setURLStreamHandlerFactory failed to set FsUrlStreamHandlerFactory")
  }

  private val HIVE_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.hive.HiveExternalCatalog"

  private def externalCatalogClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_EXTERNAL_CATALOG_CLASS_NAME
      case "in-memory" => classOf[InMemoryCatalog].getCanonicalName
    }
  }

  /**
   * Helper method to create an instance of [[T]] using a single-arg constructor that
   * accepts an [[Arg1]] and an [[Arg2]].
   */
  private def reflect[T, Arg1 <: AnyRef, Arg2 <: AnyRef](
      className: String,
      ctorArg1: Arg1,
      ctorArg2: Arg2)(
      implicit ctorArgTag1: ClassTag[Arg1],
      ctorArgTag2: ClassTag[Arg2]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag1.runtimeClass, ctorArgTag2.runtimeClass)
      val args = Array[AnyRef](ctorArg1, ctorArg2)
      ctor.newInstance(args: _*).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
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
