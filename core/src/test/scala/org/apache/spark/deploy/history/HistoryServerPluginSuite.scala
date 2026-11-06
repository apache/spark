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
import java.util.ServiceConfigurationError

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History.HISTORY_LOG_DIR
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils

class HistoryServerPluginSuite extends SparkFunSuite {

  private val staticDir = "org/apache/spark/ui/static"
  private val staticFile = "spark-dag-viz.js"

  /** A plugin that declares one static resource path, or throws when asked for it. */
  private class StaticPlugin(path: String, fail: Boolean = false) extends AppHistoryServerPlugin {
    override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] =
      Nil

    override def setupUI(ui: SparkUI): Unit = {}

    override def staticResources: Seq[(String, String)] = {
      if (fail) {
        throw new IllegalStateException("staticResources failed")
      }
      Seq(staticDir -> path)
    }
  }

  private def statusOf(server: HistoryServer, path: String): Int = {
    val host = Utils.localHostNameForURI()
    val url = new URI(s"http://$host:${server.boundPort}$path/$staticFile").toURL
    HistoryServerSuite.getContentAndCode(url)._1
  }

  /**
   * An iterable that yields the given elements in order, throwing the Left ones instead.
   * LinkageErrors surface from hasNext and the rest from next(), like a ServiceLoader iterator.
   */
  private def pluginsWithErrors(
      elements: Seq[Either[Throwable, AppHistoryServerPlugin]]): Iterable[AppHistoryServerPlugin] =
    new Iterable[AppHistoryServerPlugin] {
      override def iterator: Iterator[AppHistoryServerPlugin] = new PluginIterator(elements)
    }

  /** The plugins that load from the given elements, skipping the ones that fail. */
  private def loadedFrom(
      elements: Seq[Either[Throwable, AppHistoryServerPlugin]]): Seq[AppHistoryServerPlugin] =
    AppHistoryServerPlugin.skipUnloadable(pluginsWithErrors(elements))

  private class PluginIterator(elements: Seq[Either[Throwable, AppHistoryServerPlugin]])
    extends Iterator[AppHistoryServerPlugin] {

    private val underlying = elements.iterator
    private var pending: Option[Either[Throwable, AppHistoryServerPlugin]] = None

    override def hasNext: Boolean = {
      if (pending.isEmpty && underlying.hasNext) {
        pending = Some(underlying.next())
      }
      pending match {
        case Some(Left(e: LinkageError)) =>
          pending = None
          throw e
        case Some(_) => true
        case None => false
      }
    }

    override def next(): AppHistoryServerPlugin = {
      val element = pending.get
      pending = None
      element match {
        case Left(e) => throw e
        case Right(p) => p
      }
    }
  }

  private def withHistoryServer(fn: HistoryServer => Unit): Unit = {
    withTempDir { logDir =>
      val conf = new SparkConf().set(HISTORY_LOG_DIR, logDir.getAbsolutePath)
      val provider = new FsHistoryProvider(conf)
      val server = new HistoryServer(conf, provider, new SecurityManager(conf), 0)
      try {
        server.bind()
        fn(server)
      } finally {
        server.stop()
      }
    }
  }

  test("static resources declared by plugins are served") {
    withHistoryServer { server =>
      server.setupPluginStaticResources(loadedFrom(Seq(
        Right(new StaticPlugin("/static/test-a")),
        Right(new StaticPlugin("/static/test-b")))))
      assert(statusOf(server, "/static/test-a") === 200)
      assert(statusOf(server, "/static/test-b") === 200)
    }
  }

  test("a plugin that fails to load is skipped and the others are still set up") {
    withHistoryServer { server =>
      server.setupPluginStaticResources(loadedFrom(Seq(
        Right(new StaticPlugin("/static/test-before")),
        Left(new ServiceConfigurationError("provider constructor failed")),
        Left(new NoClassDefFoundError("missing class")),
        Right(new StaticPlugin("/static/test-after")))))
      assert(statusOf(server, "/static/test-before") === 200)
      assert(statusOf(server, "/static/test-after") === 200)
    }
  }

  test("a plugin whose staticResources throws does not affect the others") {
    withHistoryServer { server =>
      server.setupPluginStaticResources(loadedFrom(Seq(
        Right(new StaticPlugin("/static/test-failing", fail = true)),
        Right(new StaticPlugin("/static/test-healthy")))))
      assert(statusOf(server, "/static/test-failing") === 404)
      assert(statusOf(server, "/static/test-healthy") === 200)
    }
  }

  test("a failing entry does not skip the later entries of the same plugin") {
    withHistoryServer { server =>
      val plugin = new StaticPlugin("unused") {
        override def staticResources: Seq[(String, String)] = Seq(
          staticDir -> "/static/test-first",
          "org/apache/spark/ui/no-such-dir" -> "/static/test-missing",
          staticDir -> "/static/test-third")
      }
      server.setupPluginStaticResources(loadedFrom(Seq(Right(plugin))))
      assert(statusOf(server, "/static/test-first") === 200)
      assert(statusOf(server, "/static/test-third") === 200)
    }
  }

  test("paths outside /static/ are rejected and do not take over the server's routes") {
    withHistoryServer { server =>
      val plugin = new StaticPlugin("unused") {
        override def staticResources: Seq[(String, String)] = Seq(
          staticDir -> "/history",
          staticDir -> "/api/v1/x",
          staticDir -> "/static",
          staticDir -> "/static/test-ok")
      }
      server.setupPluginStaticResources(loadedFrom(Seq(Right(plugin))))
      assert(statusOf(server, "/static/test-ok") === 200)
      assert(statusOf(server, "/history") !== 200)
      assert(statusOf(server, "/api/v1/x") !== 200)
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
