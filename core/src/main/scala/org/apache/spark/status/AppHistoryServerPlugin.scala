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

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
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
   * Static resources (JS/CSS bundled with the plugin) to serve from the History Server's own UI,
   * as (classpath resource directory, URL path) pairs. They are registered once at startup.
   * Unlike a handler attached in [[setupUI]], which is torn down when the application cache
   * evicts that application, these live as long as the History Server.
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

private[spark] object AppHistoryServerPlugin {
  def loadPlugins(): Iterable[AppHistoryServerPlugin] = {
    ServiceLoader.load(classOf[AppHistoryServerPlugin], Utils.getContextOrSparkClassLoader).asScala
  }
}
