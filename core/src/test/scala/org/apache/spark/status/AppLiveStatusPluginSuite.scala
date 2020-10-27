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

import scala.collection.JavaConverters._

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.APP_LIVE_STATUS_PLUGINS
import org.apache.spark.scheduler.SparkListener

class AppLiveStatusPluginSuite extends SparkFunSuite with LocalSparkContext {
  test("SPARK-33249: Should be able to add listeners") {
    val plugins = Seq(
      classOf[SomePluginA],
      classOf[SomePluginB]
    )
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(APP_LIVE_STATUS_PLUGINS, plugins.map(_.getName))
    sc = new SparkContext(conf)

    assert(sc.listenerBus.listeners.asScala.count(_.isInstanceOf[SomeListenerA]) == 1)
    assert(sc.listenerBus.listeners.asScala.count(_.isInstanceOf[SomeListenerB]) == 1)
    assert(sc.listenerBus.listeners.asScala.count(_.isInstanceOf[SomeListenerC]) == 1)
  }

  test("SPARK-33249: Only load plugins in conf") {
    val plugins = Seq(
      classOf[SomePluginB],
      classOf[SomePluginC]
    )
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(APP_LIVE_STATUS_PLUGINS, plugins.map(_.getName))
    sc = new SparkContext(conf)

    assert(sc.listenerBus.listeners.asScala.count(_.isInstanceOf[SomeListenerC]) == 1)
    assert(sc.listenerBus.listeners.asScala.count(_.isInstanceOf[SomeListenerD]) == 0)
  }
}

private class SomePluginA extends AppLiveStatusPlugin {
  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new SomeListenerA(), new SomeListenerB())
  }
}

private class SomePluginB extends AppLiveStatusPlugin {
  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new SomeListenerC())
  }
}

private class SomePluginC extends AppLiveStatusPlugin {
  throw new UnsupportedOperationException("do not init")

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new SomeListenerD())
  }
}

private class SomeListenerA extends SparkListener
private class SomeListenerB extends SparkListener
private class SomeListenerC extends SparkListener
private class SomeListenerD extends SparkListener
