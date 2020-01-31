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
import java.util.UUID

import scala.concurrent.duration._

import com.google.common.io.Files
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark._
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.util.Utils

class ResourceDiscoveryPluginSuite extends SparkFunSuite with LocalSparkContext {

  test("plugin initialization in non-local mode") {
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val conf = new SparkConf()
        .setAppName(getClass().getName())
        .set(SparkLauncher.SPARK_MASTER, "local-cluster[2,1,1024]")
        .set(RESOURCES_DISCOVERY_PLUGIN, classOf[TestResourceDiscoveryPlugin].getName())
        .set(TestResourceDiscoveryPlugin.TEST_PATH_CONF, dir.getAbsolutePath())
        .set(WORKER_GPU_ID.amountConf, "2")
        .set(TASK_GPU_ID.amountConf, "1")
        .set(EXECUTOR_GPU_ID.amountConf, "1")
        .set(SPARK_RESOURCES_DIR, dir.getName())
        .set(WORKER_FPGA_ID.amountConf, "2")
        .set(TASK_FPGA_ID.amountConf, "1")
        .set(EXECUTOR_FPGA_ID.amountConf, "1")

      sc = new SparkContext(conf)
      TestUtils.waitUntilExecutorsUp(sc, 2, 10000)

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
}

object TestResourceDiscoveryPlugin {
  val TEST_PATH_CONF = "spark.nonLocalDiscoveryPlugin.path"

  def writeFile(conf: SparkConf, id: String): Unit = {
    val path = conf.get(TEST_PATH_CONF)
    val fileName = s"$id - ${UUID.randomUUID.toString}"
    Files.write(id, new File(path, fileName), StandardCharsets.UTF_8)
  }
}

private class TestResourceDiscoveryPlugin extends ResourceDiscoveryPlugin {

  override def discoverResource(request: ResourceRequest, conf: SparkConf): ResourceInformation = {
    TestResourceDiscoveryPlugin.writeFile(conf, request.id.resourceName)
    if (request.id.resourceName.equals(GPU)) {
      new ResourceInformation(GPU, Array("0", "1", "2", "3"))
    } else {
      new ResourceInformation(FPGA, Array("0", "1", "2", "3"))
    }
  }
}