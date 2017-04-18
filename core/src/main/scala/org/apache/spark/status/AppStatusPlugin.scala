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

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

/**
 * An interface that defines plugins for collecting and storing application state.
 *
 * The plugin implementations are invoked for both live and replayed applications. For live
 * applications, it's recommended that plugins defer creation of UI tabs until there's actual
 * data to be shown.
 */
private[spark] trait AppStatusPlugin {

  /**
   * Install listeners to collect data about the running application and populate the given
   * store.
   *
   * @param conf The Spark configuration.
   * @param store The KVStore where to keep application data.
   * @param addListenerFn Function to register listeners with a bus.
   * @param live Whether this is a live application (or an application being replayed by the
   *             HistoryServer).
   */
  def setupListeners(
      conf: SparkConf,
      store: ElementTrackingStore,
      addListenerFn: SparkListener => Unit,
      live: Boolean): Unit

  /**
   * Install any needed extensions (tabs, pages, etc) to a Spark UI. The plugin can detect whether
   * the app is live or replayed by looking at the UI's SparkContext field `sc`.
   *
   * @param ui The Spark UI instance for the application.
   */
  def setupUI(ui: SparkUI): Unit

}

private[spark] object AppStatusPlugin {

  def loadPlugins(): Iterable[AppStatusPlugin] = {
    ServiceLoader.load(classOf[AppStatusPlugin], Utils.getContextOrSparkClassLoader).asScala
  }

}
