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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, InMemoryCatalog}
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.util.MutableURLClassLoader


/**
 * A class that holds all state shared across sessions in a given [[SQLContext]].
 */
private[sql] class SharedState(val sparkContext: SparkContext) {

  /**
   * Class for caching query results reused in future executions.
   */
  val cacheManager: CacheManager = new CacheManager

  /**
   * A listener for SQL-specific [[org.apache.spark.scheduler.SparkListenerEvent]]s.
   */
  val listener: SQLListener = SQLContext.createListenerAndUI(sparkContext)

  /**
   * A catalog that interacts with external systems.
   */
  lazy val externalCatalog: ExternalCatalog = new InMemoryCatalog(sparkContext.hadoopConfiguration)

  /**
   * A classloader used to load all user-added jar.
   */
  val jarClassLoader = new NonClosableMutableURLClassLoader(
    org.apache.spark.util.Utils.getContextOrSparkClassLoader)

}


/**
 * URL class loader that exposes the `addURL` and `getURLs` methods in URLClassLoader.
 * This class loader cannot be closed (its `close` method is a no-op).
 */
private[sql] class NonClosableMutableURLClassLoader(parent: ClassLoader)
  extends MutableURLClassLoader(Array.empty, parent) {

  override def close(): Unit = {}
}
