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

package org.apache.spark.status

import java.util.{ServiceConfigurationError, ServiceLoader}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.NUM_FAILURES
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils

/**
 * An interface for creating history listeners(to replay event logs) defined in other modules like
 * SQL, and setup the UI of the plugin to rebuild the history UI.
 */
private[spark] trait AppHistoryServerPlugin {
  /**
   * Creates listeners to replay the event logs.
   */
  def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener]

  /**
   * Sets up UI of this plugin to rebuild the history UI.
   */
  def setupUI(ui: SparkUI): Unit

  /**
   * Static resources (JS/CSS) to serve from the History Server's own UI, as (classpath resource
   * directory, URL path) pairs. They are registered once at startup. The URL path must be a
   * single segment under /static/ made of letters, digits, `_` or `-` (e.g. /static/sql) that the
   * History Server does not already serve; other paths are rejected so they cannot shadow its
   * own routes or core static files.
   * The directory is resolved with Spark's class loader, not the plugin's, so it must be on
   * Spark's class path. Unlike a handler attached in [[setupUI]], which is torn down when the
   * application cache evicts that application, these live as long as the History Server.
   *
   * This does not replace the registration in [[setupUI]] or in the tabs: the live driver UI
   * never calls this hook, so tabs must still register their own handlers.
   */
  def staticResources: Seq[(String, String)] = Nil

  /**
   * The position of a plugin tab relative to the other plugin tabs in the history UI.
   */
  def displayOrder: Int = Integer.MAX_VALUE
}

private[spark] object AppHistoryServerPlugin extends Logging {

  // Consecutive load failures tolerated before giving up on the remaining plugins
  private[spark] val MAX_LOAD_FAILURES = 10

  /**
   * Loads the available plugins, skipping with a warning any that cannot be loaded, so that a
   * broken plugin does not take down the History Server or the loading of an application UI.
   * Used where nothing is persisted: the History Server's startup, an application UI, and an
   * in-memory store rebuilt from an event log.
   */
  def loadPlugins(): Seq[AppHistoryServerPlugin] = skipUnloadable(loadPluginsOrFail())

  /**
   * Lazily loads the available plugins. A plugin that cannot be loaded makes iteration throw.
   * Use this where a missing plugin must not go unnoticed, e.g. when rebuilding a store that is
   * persisted to disk, which would otherwise keep missing that plugin's data.
   */
  def loadPluginsOrFail(): Iterable[AppHistoryServerPlugin] = {
    ServiceLoader.load(classOf[AppHistoryServerPlugin], Utils.getContextOrSparkClassLoader).asScala
  }

  /**
   * Iterates through the plugins and attempts to load them one by one. If a plugin
   * fails to load, it increments a failure counter. If the number of consecutive failures reaches
   * MAX_LOAD_FAILURES, the loading process is aborted and remaining plugins are skipped.
   *
   * @param plugins an iterable collection of AppHistoryServerPlugin instances to load
   * @return a sequence of successfully loaded AppHistoryServerPlugin instances
   *
   * @note This method will stop loading after MAX_LOAD_FAILURES consecutive failures to prevent
   *       infinite loops or excessive error conditions
   * @note Successfully loading a plugin resets the consecutive failure counter
   * @note Non-fatal exceptions and LinkageErrors are caught and logged as warnings. Fatal
   *       errors such as OutOfMemoryError are not caught.
   */
  private[spark] def skipUnloadable(
      plugins: Iterable[AppHistoryServerPlugin]): Seq[AppHistoryServerPlugin] = {
    val iterator = plugins.iterator
    val loaded = Seq.newBuilder[AppHistoryServerPlugin]
    var keepLoading = true
    var failuresInARow = 0
    while (keepLoading && failuresInARow < MAX_LOAD_FAILURES) {
      try {
        if (iterator.hasNext) {
          loaded += iterator.next()
          failuresInARow = 0
        } else {
          keepLoading = false
        }
      } catch {
        // The ServiceLoader wraps a fatal error from a constructor in a ServiceConfigurationError.
        // Unwrap it so it stops the startup instead of being skipped.
        case e: ServiceConfigurationError if e.getCause.isInstanceOf[VirtualMachineError] =>
          throw e.getCause
        case t if NonFatal(t) || t.isInstanceOf[LinkageError] =>
          failuresInARow += 1
          logWarning("Skipping an AppHistoryServerPlugin that failed to load", t)
      }
    }
    if (keepLoading) {
      logWarning(log"Giving up loading AppHistoryServerPlugins after " +
        log"${MDC(NUM_FAILURES, MAX_LOAD_FAILURES)} consecutive failures; " +
        log"the remaining plugins were skipped")
    }
    loaded.result()
  }
}
