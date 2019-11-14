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

package org.apache.spark.internal.plugin

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.codahale.metrics.Gauge
import com.google.common.io.Files
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, spy, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark._
import org.apache.spark.api.plugin._
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.Utils

class PluginContainerSuite extends SparkFunSuite with BeforeAndAfterEach with LocalSparkContext {

  override def afterEach(): Unit = {
    TestSparkPlugin.reset()
    super.afterEach()
  }

  test("plugin initialization and communication") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set(PLUGINS, Seq(classOf[TestSparkPlugin].getName()))

    TestSparkPlugin.extraConf = Map("foo" -> "bar", "bar" -> "baz").asJava

    sc = new SparkContext(conf)

    assert(TestSparkPlugin.driverPlugin != null)
    verify(TestSparkPlugin.driverPlugin).init(meq(sc), any())

    assert(TestSparkPlugin.executorPlugin != null)
    verify(TestSparkPlugin.executorPlugin).init(any(), meq(TestSparkPlugin.extraConf))

    assert(TestSparkPlugin.executorContext != null)

    // One way messages don't block, so need to loop checking whether it arrives.
    TestSparkPlugin.executorContext.send("oneway")
    eventually(timeout(10.seconds), interval(10.millis)) {
      verify(TestSparkPlugin.driverPlugin).receive("oneway")
    }

    assert(TestSparkPlugin.executorContext.ask("ask") === "reply")

    val err = intercept[Exception] {
      TestSparkPlugin.executorContext.ask("unknown message")
    }
    assert(err.getMessage().contains("unknown message"))

    // It should be possible for the driver plugin to send a message to itself, even if that doesn't
    // make a whole lot of sense. It at least allows the same context class to be used on both
    // sides.
    assert(TestSparkPlugin.driverContext != null)
    assert(TestSparkPlugin.driverContext.ask("ask") === "reply")

    val metricSources = sc.env.metricsSystem
      .getSourcesByName(s"plugin.${classOf[TestSparkPlugin].getName()}")
    assert(metricSources.size === 2)

    def findMetric(name: String): Int = {
      val allFound = metricSources.filter(_.metricRegistry.getGauges().containsKey(name))
      assert(allFound.size === 1)
      allFound.head.metricRegistry.getGauges().get(name).asInstanceOf[Gauge[Int]].getValue()
    }

    assert(findMetric("driverMetric") === 42)
    assert(findMetric("executorMetric") === 84)

    sc.stop()
    sc = null

    verify(TestSparkPlugin.driverPlugin).shutdown()
    verify(TestSparkPlugin.executorPlugin).shutdown()
  }

  test("do nothing if plugins are not configured") {
    val conf = new SparkConf()
    val env = mock(classOf[SparkEnv])
    when(env.conf).thenReturn(conf)
    assert(PluginContainer(env) === None)
  }

  test("merging of config options") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set(PLUGINS, Seq(classOf[TestSparkPlugin].getName()))
      .set(DEFAULT_PLUGINS_LIST, classOf[TestSparkPlugin].getName())

    assert(conf.get(PLUGINS).size === 2)

    sc = new SparkContext(conf)
    // Just check plugin is loaded. The plugin code below checks whether a single copy was loaded.
    assert(TestSparkPlugin.driverPlugin != null)
  }

  test("plugin initialization in non-local mode") {
    val path = Utils.createTempDir()

    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local-cluster[2,1,1024]")
      .set(PLUGINS, Seq(classOf[NonLocalModeSparkPlugin].getName()))
      .set(NonLocalModeSparkPlugin.TEST_PATH_CONF, path.getAbsolutePath())

    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 10000)

    eventually(timeout(10.seconds), interval(100.millis)) {
      val children = path.listFiles()
      assert(children != null)
      assert(children.length >= 3)
    }
  }
}

class NonLocalModeSparkPlugin extends SparkPlugin {

  override def driverPlugin(): DriverPlugin = {
    new DriverPlugin() {
      override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
        NonLocalModeSparkPlugin.writeFile(ctx.conf(), ctx.executorID())
        Map.empty.asJava
      }
    }
  }

  override def executorPlugin(): ExecutorPlugin = {
    new ExecutorPlugin() {
      override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
        NonLocalModeSparkPlugin.writeFile(ctx.conf(), ctx.executorID())
      }
    }
  }
}

object NonLocalModeSparkPlugin {
  val TEST_PATH_CONF = "spark.nonLocalPlugin.path"

  def writeFile(conf: SparkConf, id: String): Unit = {
    val path = conf.get(TEST_PATH_CONF)
    Files.write(id, new File(path, id), StandardCharsets.UTF_8)
  }
}

class TestSparkPlugin extends SparkPlugin {

  override def driverPlugin(): DriverPlugin = {
    val p = new TestDriverPlugin()
    require(TestSparkPlugin.driverPlugin == null, "Driver plugin already initialized.")
    TestSparkPlugin.driverPlugin = spy(p)
    TestSparkPlugin.driverPlugin
  }

  override def executorPlugin(): ExecutorPlugin = {
    val p = new TestExecutorPlugin()
    require(TestSparkPlugin.executorPlugin == null, "Executor plugin already initialized.")
    TestSparkPlugin.executorPlugin = spy(p)
    TestSparkPlugin.executorPlugin
  }

}

private class TestDriverPlugin extends DriverPlugin {

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    TestSparkPlugin.driverContext = ctx
    TestSparkPlugin.extraConf
  }

  override def registerMetrics(appId: String, ctx: PluginContext): Unit = {
    ctx.metricRegistry().register("driverMetric", new Gauge[Int] {
      override def getValue(): Int = 42
    })
  }

  override def receive(msg: AnyRef): AnyRef = msg match {
    case "oneway" => null
    case "ask" => "reply"
    case other => throw new IllegalArgumentException(s"unknown: $other")
  }

}

private class TestExecutorPlugin extends ExecutorPlugin {

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    ctx.metricRegistry().register("executorMetric", new Gauge[Int] {
      override def getValue(): Int = 84
    })
    TestSparkPlugin.executorContext = ctx
  }

}

private object TestSparkPlugin {
  var driverPlugin: TestDriverPlugin = _
  var driverContext: PluginContext = _

  var executorPlugin: TestExecutorPlugin = _
  var executorContext: PluginContext = _

  var extraConf: JMap[String, String] = _

  def reset(): Unit = {
    driverPlugin = null
    driverContext = null
    executorPlugin = null
    executorContext = null
    extraConf = null
  }
}
