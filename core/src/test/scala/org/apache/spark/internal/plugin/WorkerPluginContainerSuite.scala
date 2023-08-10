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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.api.plugin.SparkWorkerPlugin
import org.apache.spark.internal.config.SUBPROCESS_PLUGINS

class WorkerPluginContainerSuite extends SparkFunSuite with LocalSparkContext {

  test("plugin container applies to process builder") {
    val conf = new SparkConf()
      .set(SUBPROCESS_PLUGINS, Seq(classOf[TestWorkerPlugin].getName))
    val pb = new ProcessBuilder()

    WorkerPluginContainer(conf)(pb)

    assert(pb.environment().containsKey("plugin-override"))
  }

  test("plugin container does not fail on empty plugin list") {
    val pb = new ProcessBuilder()

    WorkerPluginContainer(new SparkConf())(pb)
  }
}

private class TestWorkerPlugin extends SparkWorkerPlugin {

  override def apply(pb: ProcessBuilder): Unit =
    pb.environment().put("plugin-override", "plugin-override")
}
