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
import java.nio.file.{Files => JavaFiles}
import java.util.Optional

import org.json4s.{DefaultFormats, Extraction}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.TestUtils._
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.util.Utils

class ResourceUtilsSuite extends SparkFunSuite
    with LocalSparkContext {

  test("ResourceID") {
    val componentName = "spark.test"
    val resourceName = "p100"
    val id = new ResourceID(componentName, resourceName)
    val confPrefix = s"$componentName.resource.$resourceName."
    assert(id.confPrefix === confPrefix)
    assert(id.amountConf === s"${confPrefix}amount")
    assert(id.discoveryScriptConf === s"${confPrefix}discoveryScript")
    assert(id.vendorConf === s"${confPrefix}vendor")
  }

  test("Resource discoverer no addresses errors") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "gpuDiscoverScript",
        """{"name": "gpu"}""")
      conf.set(EXECUTOR_GPU_ID.amountConf, "2")
      conf.set(EXECUTOR_GPU_ID.discoveryScriptConf, scriptPath)

      val error = intercept[IllegalArgumentException] {
        getOrDiscoverAllResources(conf, SPARK_EXECUTOR_PREFIX, None)
      }.getMessage()
      assert(error.contains("Resource: gpu, with " +
        "addresses:  is less than what the user requested: 2"))
    }
  }

  test("Resource discoverer amount 0") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "gpuDiscoverScript",
        """{"name": "gpu"}""")
      conf.set(EXECUTOR_GPU_ID.amountConf, "0")
      conf.set(EXECUTOR_GPU_ID.discoveryScriptConf, scriptPath)

      val res = getOrDiscoverAllResources(conf, SPARK_EXECUTOR_PREFIX, None)
      assert(res.isEmpty)
    }
  }

  test("Resource discoverer multiple resource types") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuDiscovery = createTempScriptWithExpectedOutput(dir, "gpuDiscoveryScript",
        """{"name": "gpu", "addresses": ["0", "1"]}""")
      conf.set(EXECUTOR_GPU_ID.amountConf, "2")
      conf.set(EXECUTOR_GPU_ID.discoveryScriptConf, gpuDiscovery)

      val fpgaDiscovery = createTempScriptWithExpectedOutput(dir, "fpgDiscoverScript",
        """{"name": "fpga", "addresses": ["f1", "f2", "f3"]}""")
      conf.set(EXECUTOR_FPGA_ID.amountConf, "2")
      conf.set(EXECUTOR_FPGA_ID.discoveryScriptConf, fpgaDiscovery)

      // test one with amount 0 to make sure ignored
      val fooDiscovery = createTempScriptWithExpectedOutput(dir, "fooDiscoverScript",
        """{"name": "foo", "addresses": ["f1", "f2", "f3"]}""")
      val fooId = new ResourceID(SPARK_EXECUTOR_PREFIX, "foo")
      conf.set(fooId.amountConf, "0")
      conf.set(fooId.discoveryScriptConf, fooDiscovery)

      val resources = getOrDiscoverAllResources(conf, SPARK_EXECUTOR_PREFIX, None)
      assert(resources.size === 2)
      val gpuValue = resources.get(GPU)
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.sameElements(Array("0", "1")), "should have 0,1 entries")

      val fpgaValue = resources.get(FPGA)
      assert(fpgaValue.nonEmpty, "Should have a gpu entry")
      assert(fpgaValue.get.name == "fpga", "name should be fpga")
      assert(fpgaValue.get.addresses.size == 3, "Should have 3 indexes")
      assert(fpgaValue.get.addresses.sameElements(Array("f1", "f2", "f3")),
        "should have f1,f2,f3 entries")
    }
  }

  test("get from resources file and discover the remaining") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      implicit val formats = DefaultFormats
      val fpgaAddrs = Seq("f1", "f2", "f3")
      val fpgaAllocation = ResourceAllocation(EXECUTOR_FPGA_ID, fpgaAddrs)
      val resourcesFile = createTempJsonFile(
        dir, "resources", Extraction.decompose(Seq(fpgaAllocation)))
      conf.set(EXECUTOR_FPGA_ID.amountConf, "3")
      val resourcesFromFileOnly = getOrDiscoverAllResources(
        conf, SPARK_EXECUTOR_PREFIX, Some(resourcesFile))
      val expectedFpgaInfo = new ResourceInformation(FPGA, fpgaAddrs.toArray)
      assert(resourcesFromFileOnly(FPGA) === expectedFpgaInfo)

      val gpuDiscovery = createTempScriptWithExpectedOutput(
        dir, "gpuDiscoveryScript",
        """{"name": "gpu", "addresses": ["0", "1"]}""")
      conf.set(EXECUTOR_GPU_ID.amountConf, "2")
      conf.set(EXECUTOR_GPU_ID.discoveryScriptConf, gpuDiscovery)
      val resourcesFromBoth = getOrDiscoverAllResources(
        conf, SPARK_EXECUTOR_PREFIX, Some(resourcesFile))
      val expectedGpuInfo = new ResourceInformation(GPU, Array("0", "1"))
      assert(resourcesFromBoth(FPGA) === expectedFpgaInfo)
      assert(resourcesFromBoth(GPU) === expectedGpuInfo)
    }
  }

  test("get from resources file and discover resource profile remaining") {
    val conf = new SparkConf
    val rpId = 1
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      implicit val formats = DefaultFormats
      val fpgaAddrs = Seq("f1", "f2", "f3")
      val fpgaAllocation = ResourceAllocation(EXECUTOR_FPGA_ID, fpgaAddrs)
      val resourcesFile = createTempJsonFile(
        dir, "resources", Extraction.decompose(Seq(fpgaAllocation)))
      val resourcesFromFileOnly = getOrDiscoverAllResourcesForResourceProfile(
        Some(resourcesFile),
        SPARK_EXECUTOR_PREFIX,
        ResourceProfile.getOrCreateDefaultProfile(conf),
        conf)
      val expectedFpgaInfo = new ResourceInformation(FPGA, fpgaAddrs.toArray)
      assert(resourcesFromFileOnly(FPGA) === expectedFpgaInfo)

      val gpuDiscovery = createTempScriptWithExpectedOutput(
        dir, "gpuDiscoveryScript",
        """{"name": "gpu", "addresses": ["0", "1"]}""")
      val rpBuilder = new ResourceProfileBuilder()
      val ereqs = new ExecutorResourceRequests().resource(GPU, 2, gpuDiscovery)
      val treqs = new TaskResourceRequests().resource(GPU, 1)

      val rp = rpBuilder.require(ereqs).require(treqs).build
      val resourcesFromBoth = getOrDiscoverAllResourcesForResourceProfile(
        Some(resourcesFile), SPARK_EXECUTOR_PREFIX, rp, conf)
      val expectedGpuInfo = new ResourceInformation(GPU, Array("0", "1"))
      assert(resourcesFromBoth(FPGA) === expectedFpgaInfo)
      assert(resourcesFromBoth(GPU) === expectedGpuInfo)
    }
  }

  test("list resource ids") {
    val conf = new SparkConf
    conf.set(DRIVER_GPU_ID.amountConf, "2")
    var resources = listResourceIds(conf, SPARK_DRIVER_PREFIX)
    assert(resources.size === 1, "should only have GPU for resource")
    assert(resources(0).resourceName == GPU, "name should be gpu")

    conf.set(DRIVER_FPGA_ID.amountConf, "2")
    val resourcesMap = listResourceIds(conf, SPARK_DRIVER_PREFIX)
      .map { rId => (rId.resourceName, 1) }.toMap
    assert(resourcesMap.size === 2, "should only have GPU for resource")
    assert(resourcesMap.get(GPU).nonEmpty, "should have GPU")
    assert(resourcesMap.get(FPGA).nonEmpty, "should have FPGA")
  }

  test("parse resource request") {
    val conf = new SparkConf
    conf.set(DRIVER_GPU_ID.amountConf, "2")
    var request = parseResourceRequest(conf, DRIVER_GPU_ID)
    assert(request.id.resourceName === GPU, "should only have GPU for resource")
    assert(request.amount === 2, "GPU count should be 2")
    assert(request.discoveryScript === Optional.empty(), "discovery script should be empty")
    assert(request.vendor === Optional.empty(), "vendor should be empty")

    val vendor = "nvidia.com"
    val discoveryScript = "discoveryScriptGPU"
    conf.set(DRIVER_GPU_ID.discoveryScriptConf, discoveryScript)
    conf.set(DRIVER_GPU_ID.vendorConf, vendor)
    request = parseResourceRequest(conf, DRIVER_GPU_ID)
    assert(request.id.resourceName === GPU, "should only have GPU for resource")
    assert(request.amount === 2, "GPU count should be 2")
    assert(request.discoveryScript.get === discoveryScript, "should get discovery script")
    assert(request.vendor.get === vendor, "should get vendor")

    conf.remove(DRIVER_GPU_ID.amountConf)
    val error = intercept[SparkException] {
      request = parseResourceRequest(conf, DRIVER_GPU_ID)
    }.getMessage()

    assert(error.contains("You must specify an amount for gpu"))
  }

  test("Resource discoverer multiple gpus on driver") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuDiscovery = createTempScriptWithExpectedOutput(dir, "gpuDiscoveryScript",
        """{"name": "gpu", "addresses": ["0", "1"]}""")
      conf.set(DRIVER_GPU_ID.amountConf, "2")
      conf.set(DRIVER_GPU_ID.discoveryScriptConf, gpuDiscovery)

      // make sure it reads from correct config, here it should use driver
      val resources = getOrDiscoverAllResources(conf, SPARK_DRIVER_PREFIX, None)
      val gpuValue = resources.get(GPU)
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.sameElements(Array("0", "1")), "should have 0,1 entries")
    }
  }

  test("Resource discoverer script returns mismatched name") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuDiscovery = createTempScriptWithExpectedOutput(dir, "gpuDiscoveryScript",
        """{"name": "fpga", "addresses": ["0", "1"]}""")
      val request =
        new ResourceRequest(
          DRIVER_GPU_ID,
          2,
          Optional.of(gpuDiscovery),
          Optional.empty[String])

      val error = intercept[SparkException] {
        discoverResource(conf, request)
      }.getMessage()

      assert(error.contains(s"Error running the resource discovery script $gpuDiscovery: " +
        "script returned resource name fpga and we were expecting gpu"))
    }
  }

  test("Resource discoverer with invalid class") {
    val conf = new SparkConf()
      .set(RESOURCES_DISCOVERY_PLUGIN, Seq("someinvalidclass"))
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuDiscovery = createTempScriptWithExpectedOutput(dir, "gpuDiscoveryScript",
        """{"name": "fpga", "addresses": ["0", "1"]}""")
      val request =
        new ResourceRequest(
          DRIVER_GPU_ID,
          2,
          Optional.of(gpuDiscovery),
          Optional.empty[String])

      val error = intercept[ClassNotFoundException] {
        discoverResource(conf, request)
      }.getMessage()

      assert(error.contains(s"someinvalidclass"))
    }
  }

  test("Resource discoverer script returns invalid format") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuDiscovery = createTempScriptWithExpectedOutput(dir, "gpuDiscoverScript",
        """{"addresses": ["0", "1"]}""")

      val request =
        new ResourceRequest(
          EXECUTOR_GPU_ID,
          2,
          Optional.of(gpuDiscovery),
          Optional.empty[String])

      val error = intercept[SparkException] {
        discoverResource(conf, request)
      }.getMessage()

      assert(error.contains("Error parsing JSON into ResourceInformation"))
    }
  }

  test("Resource discoverer script doesn't exist") {
    val conf = new SparkConf
    withTempDir { dir =>
      val file1 = new File(dir, "bogusfilepath")
      try {
        val request =
          new ResourceRequest(
            EXECUTOR_GPU_ID,
            2,
            Optional.of(file1.getPath()),
            Optional.empty[String])

        val error = intercept[SparkException] {
          discoverResource(conf, request)
        }.getMessage()

        assert(error.contains("doesn't exist"))
      } finally {
        JavaFiles.deleteIfExists(file1.toPath())
      }
    }
  }

  test("gpu's specified but not a discovery script") {
    val request = new ResourceRequest(EXECUTOR_GPU_ID, 2, Optional.empty[String],
      Optional.empty[String])

    val error = intercept[SparkException] {
      discoverResource(new SparkConf(), request)
    }.getMessage()

    assert(error.contains("User is expecting to use resource: gpu, but " +
      "didn't specify a discovery script!"))
  }
}
