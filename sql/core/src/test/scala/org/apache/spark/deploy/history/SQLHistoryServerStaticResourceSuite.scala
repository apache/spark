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

import java.net.URI

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History.HISTORY_LOG_DIR

/**
 * Verifies that the SQL tab's /static/sql files, which are provided by core's static resource
 * directory, are served by the History Server for a real SQL application: for the files its pages
 * reference, and before and after its UI is detached (as happens when it is evicted from the
 * application cache).
 */
class SQLHistoryServerStaticResourceSuite extends SparkFunSuite {

  private val appId = "local-1596020211915"

  // The History Server binds to SPARK_LOCAL_IP when it is set, so "localhost" may be refused.
  // Fully qualified, because this package has its own Utils object.
  private val localhost: String = org.apache.spark.util.Utils.localHostNameForURI()

  private val staticSqlFiles = Set(
    "allexecutionspage.js",
    "executionpage.js",
    "spark-sql-viz.css",
    "spark-sql-viz.js",
    "sql-table-utils.js").map("/static/sql/" + _)

  private def withHistoryServer(fn: HistoryServer => Unit): Unit = {
    val logDir = Thread.currentThread().getContextClassLoader.getResource("spark-events").toString
    val conf = new SparkConf().set(HISTORY_LOG_DIR, logDir)
    val provider = new FsHistoryProvider(conf)
    provider.checkForLogs()
    val server = new HistoryServer(conf, provider, HistoryServer.createSecurityManager(conf), 0)
    server.bind()
    try fn(server) finally server.stop()
  }

  // Throws if the response is not a 200.
  private def getContent(server: HistoryServer, path: String): String = {
    HistoryServerSuite.getUrl(new URI(s"http://$localhost:${server.boundPort}$path").toURL)
  }

  // A request that misses the static handler can still return a 200 (e.g. the catch-all page),
  // so compare with the bundled file.
  private def assertServed(server: HistoryServer, path: String): Unit = {
    val expected = HistoryServerSuite.bundledStaticFile(path.stripPrefix("/static/"))
    assert(getContent(server, path) === expected, s"$path did not return the bundled file")
  }

  test("SPARK-59613: every /static/sql file referenced by the SQL pages is served") {
    withHistoryServer { server =>
      val pages = Seq(s"/history/$appId/SQL/", s"/history/$appId/SQL/execution/?id=0")
      val referenced = pages.flatMap { page =>
        "/static/sql/[A-Za-z0-9_.-]+".r.findAllIn(getContent(server, page))
      }.toSet
      assert(staticSqlFiles.subsetOf(referenced),
        s"the SQL pages should reference ${staticSqlFiles -- referenced}")
      referenced.foreach { path =>
        assertServed(server, path)
      }
    }
  }

  test("SPARK-59613: /static/sql stays served after the SQL application UI is detached") {
    withHistoryServer { server =>
      val ui = server.getAppUI(appId, None).getOrElse(fail(s"Failed to load $appId")).ui
      // The SQL tab no longer registers its own /static/sql handler.
      assert(!ui.getHandlers.exists(_.getContextPath == "/static/sql"))

      server.attachSparkUI(appId, None, ui, completed = true)
      server.detachSparkUI(appId, None, ui)
      staticSqlFiles.foreach(assertServed(server, _))
    }
  }
}
