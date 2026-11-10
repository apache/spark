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

import org.apache.spark.SparkFunSuite

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

  test("/static/sql resources are served before any application is loaded") {
    withTempDir { logDir =>
      HistoryServerSuite.withHistoryServer(logDir.getAbsolutePath) { (server, _) =>
        assert(HistoryServerSuite.getUrl(HistoryServerSuite.url(server, staticResource)).nonEmpty)
      }
    }
  }

  test("attachSparkUI skips duplicate handlers and /static/sql stays served after detach") {
    val appId = "local-1596020211915"
    val logDir = Thread.currentThread().getContextClassLoader.getResource("spark-events").toString
    HistoryServerSuite.withHistoryServer(logDir) { (server, provider) =>
      val ui = provider.getAppUI(appId, None).getOrElse(fail(s"Failed to load $appId")).ui
      // The application's own SQL tab declares a handler for the same path. attachSparkUI skips
      // it because the History Server already serves that path, so the History Server's
      // handler answers while the UI is attached and after it is detached.
      assert(ui.getHandlers.exists(_.getContextPath == "/static/sql"))

      server.attachSparkUI(appId, None, ui, completed = true)
      try {
        // ServerInfo.addHandler starts every handler it attaches, and the application's own
        // SparkUI is never bound, so a handler that is not started was skipped.
        Seq("/static", "/static/sql", "/api").foreach { path =>
          ui.getHandlers.filter(_.getContextPath == path).foreach { handler =>
            assert(!handler.isStarted, s"the handler for $path should have been skipped")
          }
        }
        assert(ui.getHandlers.exists { h =>
          h.getContextPath.startsWith(HistoryServer.UI_PATH_PREFIX) && h.isStarted
        }, "the application's own handlers should have been attached")

        assert(HistoryServerSuite.getUrl(HistoryServerSuite.url(server, staticResource)).nonEmpty)
        // The application's own pages must still load after the duplicate handler is skipped.
        // If its page handlers were dropped, the History Server's loader servlet would load
        // the application itself, so this checks for errors and not which handler answered.
        val jobsPage = s"${HistoryServer.UI_PATH_PREFIX}/$appId/jobs/"
        assert(HistoryServerSuite.getUrl(HistoryServerSuite.url(server, jobsPage)).nonEmpty)
      } finally {
        server.detachSparkUI(appId, None, ui)
      }
      assert(HistoryServerSuite.getUrl(HistoryServerSuite.url(server, staticResource)).nonEmpty)
    }
  }
}
