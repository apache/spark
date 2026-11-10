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

import java.util.ServiceConfigurationError

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite, TestUtils}
import org.apache.spark.internal.config.History.HISTORY_LOG_DIR
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}
import org.apache.spark.ui.SparkUI

class HistoryServerPluginSuite extends SparkFunSuite {

  private val staticDir = "org/apache/spark/ui/static"
  private val staticFile = "spark-dag-viz.js"

  /** A plugin that declares the given static resources, evaluated when they are asked for. */
  private class StaticPlugin(resources: => Seq[(String, String)]) extends AppHistoryServerPlugin {
    override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] =
      Nil

    override def setupUI(ui: SparkUI): Unit = {}

    override def staticResources: Seq[(String, String)] = resources
  }

  /** A plugin that declares one static resource at the given path. */
  private def pluginAt(path: String): StaticPlugin = new StaticPlugin(Seq(staticDir -> path))

  private def statusOf(server: HistoryServer, path: String): Int = {
    TestUtils.httpResponseCode(HistoryServerSuite.url(server, s"$path/$staticFile"))
  }

  /**
   * The plugins that load from the given elements, skipping the ones that fail. A Left is
   * thrown when it is reached, like a ServiceLoader iterator does for a provider that fails.
   * Not elements.view.map: the iterator of an indexed view does not move past an element
   * whose function throws.
   */
  private def loadedFrom(
      elements: Either[Throwable, AppHistoryServerPlugin]*): Seq[AppHistoryServerPlugin] =
    AppHistoryServerPlugin.skipUnloadable(new Iterable[AppHistoryServerPlugin] {
      override def iterator: Iterator[AppHistoryServerPlugin] = elements.iterator.map {
        case Left(e) => throw e
        case Right(p) => p
      }
    })

  private def withHistoryServer(fn: HistoryServer => Unit): Unit = {
    withTempDir { logDir =>
      HistoryServerSuite.withHistoryServer(logDir.getAbsolutePath) { (server, _) => fn(server) }
    }
  }

  test("static resources declared by plugins are served") {
    withHistoryServer { server =>
      server.setupPluginStaticResources(
        Seq(pluginAt("/static/test-a"), pluginAt("/static/test-b")))
      assert(statusOf(server, "/static/test-a") === 200)
      assert(statusOf(server, "/static/test-b") === 200)
    }
  }

  test("a plugin that fails to load is skipped and the others are still set up") {
    withHistoryServer { server =>
      server.setupPluginStaticResources(loadedFrom(
        Right(pluginAt("/static/test-before")),
        Left(new ServiceConfigurationError("provider constructor failed")),
        Left(new NoClassDefFoundError("missing class")),
        Right(pluginAt("/static/test-after"))))
      assert(statusOf(server, "/static/test-before") === 200)
      assert(statusOf(server, "/static/test-after") === 200)
    }
  }

  test("a plugin whose staticResources throws does not affect the others") {
    withHistoryServer { server =>
      val before = server.getHandlers.map(_.getContextPath).toSet
      val failing = new StaticPlugin(throw new IllegalStateException("staticResources failed"))
      server.setupPluginStaticResources(Seq(failing, pluginAt("/static/test-healthy")))
      assert(server.getHandlers.map(_.getContextPath).toSet === before + "/static/test-healthy")
      assert(statusOf(server, "/static/test-healthy") === 200)
    }
  }

  test("a failing entry does not skip the later entries of the same plugin") {
    withHistoryServer { server =>
      val plugin = new StaticPlugin(Seq(
        staticDir -> "/static/test-first",
        "org/apache/spark/ui/no-such-dir" -> "/static/test-missing",
        staticDir -> "/static/test-third"))
      server.setupPluginStaticResources(Seq(plugin))
      assert(statusOf(server, "/static/test-first") === 200)
      assert(statusOf(server, "/static/test-third") === 200)
    }
  }

  test("a malformed entry does not skip the later entries of the same plugin") {
    withHistoryServer { server =>
      val plugin = new StaticPlugin(Seq(null, staticDir -> "/static/test-after-null"))
      server.setupPluginStaticResources(Seq(plugin))
      assert(statusOf(server, "/static/test-after-null") === 200)
    }
  }

  test("paths other than a single segment under /static/ are rejected") {
    withHistoryServer { server =>
      val plugin = new StaticPlugin(Seq(
        staticDir -> "/history",
        staticDir -> "/api/v1/x",
        staticDir -> "/static",
        staticDir -> "/static/",
        staticDir -> "/static/../history",
        staticDir -> "/static/%2e%2e/history",
        staticDir -> "/static/webui.js",
        staticDir -> "/static/test-ok"))
      server.setupPluginStaticResources(Seq(plugin))
      assert(statusOf(server, "/static/test-ok") === 200)
      // Check what was registered: a request cannot tell, as the server's own handlers for these
      // paths were attached first (when the server was created) and answer them either way.
      val paths = server.getHandlers.map(_.getContextPath)
      assert(paths.count(_ == "/history") === 1)
      assert(paths.count(_ == "/static") === 1)
      assert(!paths.exists(_.startsWith("/api/v1")))
      assert(!paths.exists(p => p.startsWith("/static/") && p != "/static/test-ok"))
      // The core file is still served
      HistoryServerSuite.getUrl(HistoryServerSuite.url(server, "/static/webui.js"))
    }
  }

  test("a path Jetty cannot normalize does not stop the History Server from binding") {
    withTempDir { logDir =>
      val conf = new SparkConf().set(HISTORY_LOG_DIR, logDir.getAbsolutePath)
      val provider = new FsHistoryProvider(conf)
      val server = new HistoryServer(conf, provider, new SecurityManager(conf), 0)
      try {
        val plugin = new StaticPlugin(Seq(
          staticDir -> "/static/../../x",
          staticDir -> "/static/test-ok"))
        // As in initialize(), the handlers are only created here and are added to Jetty and
        // started in bind(), which exits the process if that fails.
        server.setupPluginStaticResources(Seq(plugin))
        server.bind()
        assert(statusOf(server, "/static/test-ok") === 200)
      } finally {
        server.stop()
      }
    }
  }

  test("a fatal error from a plugin constructor is not skipped") {
    // The ServiceLoader wraps what a provider's constructor throws in a ServiceConfigurationError
    val wrapped = new ServiceConfigurationError("provider constructor failed", new OutOfMemoryError)
    intercept[OutOfMemoryError] {
      loadedFrom(Right(pluginAt("/static/test-before")), Left(wrapped))
    }
  }

  test("loading gives up when the iterator keeps failing without moving on") {
    var calls = 0
    val stuck = new Iterable[AppHistoryServerPlugin] {
      override def iterator: Iterator[AppHistoryServerPlugin] =
        new Iterator[AppHistoryServerPlugin] {
          override def hasNext: Boolean = {
            calls += 1
            // scalastyle:off throwerror
            throw new ServiceConfigurationError("getResources failed")
            // scalastyle:on throwerror
          }
          override def next(): AppHistoryServerPlugin = throw new NoSuchElementException
        }
    }
    assert(AppHistoryServerPlugin.skipUnloadable(stuck).isEmpty)
    assert(calls === AppHistoryServerPlugin.MAX_LOAD_FAILURES)
  }
}
