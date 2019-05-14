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

package org.apache.spark

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JavaFiles}
import java.nio.file.attribute.PosixFilePermission._
import java.util.EnumSet

import com.google.common.io.Files

import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

class ResourceDiscovererSuite extends SparkFunSuite
    with LocalSparkContext {

  def mockDiscoveryScript(file: File, result: String): String = {
    Files.write(s"echo $result", file, StandardCharsets.UTF_8)
    JavaFiles.setPosixFilePermissions(file.toPath(),
      EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
    file.getPath()
  }

  test("Resource discoverer no resources") {
    val sparkconf = new SparkConf
    val resources = ResourceDiscoverer.findResources(sparkconf, isDriver = false)
    assert(resources.size === 0)
    assert(resources.get("gpu").isEmpty,
      "Should have a gpus entry that is empty")
  }

  test("Resource discoverer multiple gpus") {
    val sparkconf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val scriptPath = mockDiscoveryScript(gpuFile,
        """'{"name": "gpu","addresses":["0", "1"]}'""")
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, scriptPath)
      val resources = ResourceDiscoverer.findResources(sparkconf, isDriver = false)
      val gpuValue = resources.get("gpu")
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.deep == Array("0", "1").deep, "should have 0,1 entries")
    }
  }

  test("Resource discoverer no addresses errors") {
    val sparkconf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val scriptPath = mockDiscoveryScript(gpuFile,
        """'{"name": "gpu"}'""")
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, scriptPath)
      val resources = ResourceDiscoverer.findResources(sparkconf, isDriver = false)
      val gpuValue = resources.get("gpu")
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 0, "Should have 0 indexes")
    }
  }

  test("Resource discoverer multiple resource types") {
    val sparkconf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val gpuDiscovery = mockDiscoveryScript(gpuFile,
        """'{"name": "gpu", "addresses": ["0", "1"]}'""")
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, gpuDiscovery)

      val fpgaFile = new File(dir, "fpgaDiscoverScript")
      val fpgaDiscovery = mockDiscoveryScript(fpgaFile,
        """'{"name": "fpga", "addresses": ["f1", "f2", "f3"]}'""")
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, fpgaDiscovery)

      val resources = ResourceDiscoverer.findResources(sparkconf, isDriver = false)
      assert(resources.size === 2)
      val gpuValue = resources.get("gpu")
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.deep == Array("0", "1").deep, "should have 0,1 entries")

      val fpgaValue = resources.get("fpga")
      assert(fpgaValue.nonEmpty, "Should have a gpu entry")
      assert(fpgaValue.get.name == "fpga", "name should be fpga")
      assert(fpgaValue.get.addresses.size == 3, "Should have 3 indexes")
      assert(fpgaValue.get.addresses.deep == Array("f1", "f2", "f3").deep,
        "should have f1,f2,f3 entries")
    }
  }

  test("Resource discoverer multiple gpus on driver") {
    val sparkconf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val gpuDiscovery = mockDiscoveryScript(gpuFile,
        """'{"name": "gpu", "addresses": ["0", "1"]}'""")
      sparkconf.set(SPARK_DRIVER_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, gpuDiscovery)
      sparkconf set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, "boguspath")
      // make sure it reads from correct config, here it should use driver
      val resources = ResourceDiscoverer.findResources(sparkconf, isDriver = true)
      val gpuValue = resources.get("gpu")
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.deep == Array("0", "1").deep, "should have 0,1 entries")
    }
  }

  test("Resource discoverer script returns invalid format") {
    val sparkconf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val gpuDiscovery = mockDiscoveryScript(gpuFile,
        """'{"addresses": ["0", "1"]}'""")
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, gpuDiscovery)
      val error = intercept[SparkException] {
        ResourceDiscoverer.findResources(sparkconf, isDriver = false)
      }.getMessage()

      assert(error.contains("Error running the resource discovery"))
    }
  }

  test("Resource discoverer script doesn't exist") {
    val sparkconf = new SparkConf
    withTempDir { dir =>
      val file1 = new File(dir, "bogusfilepath")
      try {
        sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
          SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, file1.getPath())

        val error = intercept[SparkException] {
          ResourceDiscoverer.findResources(sparkconf, isDriver = false)
        }.getMessage()

        assert(error.contains("doesn't exist"))
      } finally {
        JavaFiles.deleteIfExists(file1.toPath())
      }
    }
  }

  test("gpu's specified but not discovery script") {
    val sparkconf = new SparkConf
    sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
      SPARK_RESOURCE_COUNT_POSTFIX, "2")

    val error = intercept[SparkException] {
      ResourceDiscoverer.findResources(sparkconf, isDriver = false)
    }.getMessage()

    assert(error.contains("User is expecting to use"))
  }

}
