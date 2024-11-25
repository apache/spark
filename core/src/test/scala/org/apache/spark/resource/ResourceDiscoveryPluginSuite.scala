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

package org.apache.spark.resource

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.UUID

import scala.concurrent.duration._

import com.google.common.io.Files
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark._
import org.apache.spark.TestUtils.createTempScriptWithExpectedOutput
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.util.Utils

class ResourceDiscoveryPluginSuite extends SparkFunSuite with LocalSparkContext {

  test("plugin initialization in non-local mode fpga and gpu") {
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val conf = new SparkConf()
        .setAppName(getClass().getName())
        .set(SparkLauncher.SPARK_MASTER, "local-cluster[2,1,1024]")
        .set(RESOURCES_DISCOVERY_PLUGIN, Seq(classOf[TestResourceDiscoveryPluginGPU].getName(),
          classOf[TestResourceDiscoveryPluginFPGA].getName()))
        .set(TestResourceDiscoveryPlugin.TEST_PATH_CONF, dir.getAbsolutePath())
        .set(WORKER_GPU_ID.amountConf, "2")
        .set(TASK_GPU_ID.amountConf, "1")
        .set(EXECUTOR_GPU_ID.amountConf, "1")
        .set(WORKER_FPGA_ID.amountConf, "2")
        .set(TASK_FPGA_ID.amountConf, "1")
        .set(EXECUTOR_FPGA_ID.amountConf, "1")

      sc = new SparkContext(conf)
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

      eventually(timeout(10.seconds), interval(100.millis)) {
        val children = dir.listFiles()
        assert(children != null)
        assert(children.length >= 4)
        val gpuFiles = children.filter(f => f.getName().contains(GPU))
        val fpgaFiles = children.filter(f => f.getName().contains(FPGA))
        assert(gpuFiles.length == 2)
        assert(fpgaFiles.length == 2)
      }
    }
  }

  test("single plugin gpu") {
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val conf = new SparkConf()
        .setAppName(getClass().getName())
        .set(SparkLauncher.SPARK_MASTER, "local-cluster[2,1,1024]")
        .set(RESOURCES_DISCOVERY_PLUGIN, Seq(classOf[TestResourceDiscoveryPluginGPU].getName()))
        .set(TestResourceDiscoveryPlugin.TEST_PATH_CONF, dir.getAbsolutePath())
        .set(WORKER_GPU_ID.amountConf, "2")
        .set(TASK_GPU_ID.amountConf, "1")
        .set(EXECUTOR_GPU_ID.amountConf, "1")

      sc = new SparkContext(conf)
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

      eventually(timeout(10.seconds), interval(100.millis)) {
        val children = dir.listFiles()
        assert(children != null)
        assert(children.length >= 2)
        val gpuFiles = children.filter(f => f.getName().contains(GPU))
        assert(gpuFiles.length == 2)
      }
    }
  }

  test("multiple plugins with one empty") {
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val conf = new SparkConf()
        .setAppName(getClass().getName())
        .set(SparkLauncher.SPARK_MASTER, "local-cluster[2,1,1024]")
        .set(RESOURCES_DISCOVERY_PLUGIN, Seq(classOf[TestResourceDiscoveryPluginEmpty].getName(),
          classOf[TestResourceDiscoveryPluginGPU].getName()))
        .set(TestResourceDiscoveryPlugin.TEST_PATH_CONF, dir.getAbsolutePath())
        .set(WORKER_GPU_ID.amountConf, "2")
        .set(TASK_GPU_ID.amountConf, "1")
        .set(EXECUTOR_GPU_ID.amountConf, "1")

      sc = new SparkContext(conf)
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

      eventually(timeout(10.seconds), interval(100.millis)) {
        val children = dir.listFiles()
        assert(children != null)
        assert(children.length >= 2)
        val gpuFiles = children.filter(f => f.getName().contains(GPU))
        assert(gpuFiles.length == 2)
      }
    }
  }

  test("empty plugin fallback to discovery script") {
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "gpuDiscoveryScript",
        """{"name": "gpu","addresses":["5", "6"]}""")
      val conf = new SparkConf()
        .setAppName(getClass().getName())
        .set(SparkLauncher.SPARK_MASTER, "local-cluster[2,1,1024]")
        .set(RESOURCES_DISCOVERY_PLUGIN, Seq(classOf[TestResourceDiscoveryPluginEmpty].getName()))
        .set(DRIVER_GPU_ID.discoveryScriptConf, scriptPath)
        .set(DRIVER_GPU_ID.amountConf, "2")

      sc = new SparkContext(conf)
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

      assert(sc.resources.size === 1)
      assert(sc.resources.get(GPU).get.addresses === Array("5", "6"))
      assert(sc.resources.get(GPU).get.name === "gpu")
    }
  }
}

object TestResourceDiscoveryPlugin {
  val TEST_PATH_CONF = "spark.nonLocalDiscoveryPlugin.path"

  def writeFile(conf: SparkConf, id: String): Unit = {
    val path = conf.get(TEST_PATH_CONF)
    val fileName = s"$id - ${UUID.randomUUID.toString}"
    Files.asCharSink(new File(path, fileName), StandardCharsets.UTF_8).write(id)
  }
}

private class TestResourceDiscoveryPluginGPU extends ResourceDiscoveryPlugin {

  override def discoverResource(
      request: ResourceRequest,
      conf: SparkConf): Optional[ResourceInformation] = {
    if (request.id.resourceName.equals(GPU)) {
      TestResourceDiscoveryPlugin.writeFile(conf, request.id.resourceName)
      Optional.of(new ResourceInformation(GPU, Array("0", "1", "2", "3")))
    } else {
      Optional.empty()
    }
  }
}

private class TestResourceDiscoveryPluginEmpty extends ResourceDiscoveryPlugin {

  override def discoverResource(
      request: ResourceRequest,
      conf: SparkConf): Optional[ResourceInformation] = {
    Optional.empty()
  }
}

private class TestResourceDiscoveryPluginFPGA extends ResourceDiscoveryPlugin {

  override def discoverResource(
      request: ResourceRequest,
      conf: SparkConf): Optional[ResourceInformation] = {
    if (request.id.resourceName.equals(FPGA)) {
      TestResourceDiscoveryPlugin.writeFile(conf, request.id.resourceName)
      Optional.of(new ResourceInformation(FPGA, Array("0", "1", "2", "3")))
    } else {
      Optional.empty()
    }
  }
}
