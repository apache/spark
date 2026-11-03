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

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History.HISTORY_LOG_DIR
import org.apache.spark.util.Utils

/**
 * Verifies that the SQL plugin's static resources are served by the History Server itself, so
 * they are available before any application UI has been loaded (and stay available after an
 * application UI is evicted from the cache).
 */
class SQLHistoryServerStaticResourceSuite extends SparkFunSuite {

  test("/static/sql resources are served before any application is loaded") {
    withTempDir { logDir =>
      val conf = new SparkConf().set(HISTORY_LOG_DIR, logDir.getAbsolutePath)
      val provider = new FsHistoryProvider(conf)
      val server = new HistoryServer(conf, provider, new SecurityManager(conf), 0)
      try {
        server.bind()
        provider.start()
        val host = Utils.localHostNameForURI()
        val url = new URI(s"http://$host:${server.boundPort}/static/sql/spark-sql-viz.js").toURL
        val (code, body, _) = HistoryServerSuite.getContentAndCode(url)
        assert(code === 200)
        assert(body.exists(_.nonEmpty))
      } finally {
        server.stop()
        provider.stop()
      }
    }
  }
}
