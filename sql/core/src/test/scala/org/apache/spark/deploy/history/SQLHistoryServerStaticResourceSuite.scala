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

package org.apache.spark.deploy.history

import java.net.{URI, URL}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History.HISTORY_LOG_DIR
import org.apache.spark.util.Utils

/**
 * Verifies that the SQL plugin's static resources are served by the History Server itself, so
 * they are available before any application UI has been loaded, while one is attached, and
 * after it is detached (as happens when it is evicted from the application cache).
 *
 * The application's own SQL tab declares a handler for the same /static/sql path.
 * HistoryServer.attachSparkUI skips it, since the History Server already serves that path, and
 * the application's other pages must keep working.
 */
class SQLHistoryServerStaticResourceSuite extends SparkFunSuite {

  private val staticResource = "/static/sql/spark-sql-viz.js"

  private def withHistoryServer(logDir: String)(
      fn: (HistoryServer, FsHistoryProvider) => Unit): Unit = {
    val conf = new SparkConf().set(HISTORY_LOG_DIR, logDir)
    val provider = new FsHistoryProvider(conf)
    val server = new HistoryServer(conf, provider, new SecurityManager(conf), 0)
    try {
      server.bind()
      provider.checkForLogs()
      fn(server, provider)
    } finally {
      server.stop()
    }
  }

  private def urlOf(server: HistoryServer, path: String): URL = {
    val host = Utils.localHostNameForURI()
    new URI(s"http://$host:${server.boundPort}$path").toURL
  }

  private def assertResourceServed(server: HistoryServer): Unit = {
    val (code, body, _) = HistoryServerSuite.getContentAndCode(urlOf(server, staticResource))
    assert(code === 200)
    assert(body.exists(_.nonEmpty))
  }

  test("/static/sql resources are served before any application is loaded") {
    withTempDir { logDir =>
      withHistoryServer(logDir.getAbsolutePath) { (server, _) =>
        assertResourceServed(server)
      }
    }
  }

  test("/static/sql resources are served while an application UI is attached and after " +
      "it is detached") {
    val appId = "local-1596020211915"
    val logDir = Thread.currentThread().getContextClassLoader.getResource("spark-events").toString
    withHistoryServer(logDir) { (server, provider) =>
      val ui = provider.getAppUI(appId, None).getOrElse(fail(s"Failed to load $appId")).ui
      // The application's own SQL tab declares a handler for the same path. attachSparkUI skips
      // it because the History Server already serves that path, so the History Server's
      // handler answers while the UI is attached and after it is detached.
      assert(ui.getHandlers.exists(_.getContextPath == "/static/sql"))

      server.attachSparkUI(appId, None, ui, completed = true)
      try {
        assertResourceServed(server)
        // The application's own pages must still load after the duplicate handler is skipped.
        // If its page handlers were dropped, the History Server's loader servlet would load
        // the application itself, so this checks for errors and not which handler answered.
        val jobsPage = s"${HistoryServer.UI_PATH_PREFIX}/$appId/jobs/"
        val (code, body, _) = HistoryServerSuite.getContentAndCode(urlOf(server, jobsPage))
        assert(code === 200)
        assert(body.exists(_.nonEmpty))
      } finally {
        server.detachSparkUI(appId, None, ui)
      }
      assertResourceServed(server)
    }
  }
}
