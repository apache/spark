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
import java.nio.file.{Files => JavaFiles}

import org.apache.spark.ResourceUtils._
import org.apache.spark.TestUtils._
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

class ResourceUtilsSuite extends SparkFunSuite
    with LocalSparkContext {

  test("Resource discoverer no addresses errors") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val scriptPath = writeStringToFileAndSetPermissions(gpuFile, """'{"name": "gpu"}'""")
      setExecutorResourceAmountConf(conf, GPU, "2")
      setExecutorResourceDiscoveryConf(conf, GPU, scriptPath)

      val error = intercept[IllegalArgumentException] {
        getAllResources(conf, SPARK_EXECUTOR_PREFIX, None)
      }.getMessage()
      assert(error.contains("Resource: gpu, with " +
        "addresses:  is less than what the user requested: 2"))
    }
  }

  test("Resource discoverer multiple resource types") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val gpuDiscovery = writeStringToFileAndSetPermissions(gpuFile,
        """'{"name": "gpu", "addresses": ["0", "1"]}'""")
      setExecutorResourceAmountConf(conf, GPU, "2")
      setExecutorResourceDiscoveryConf(conf, GPU, gpuDiscovery)

      val fpgaFile = new File(dir, "fpgaDiscoverScript")
      val fpgaDiscovery = writeStringToFileAndSetPermissions(fpgaFile,
        """'{"name": "fpga", "addresses": ["f1", "f2", "f3"]}'""")
      setExecutorResourceAmountConf(conf, FPGA, "2")
      setExecutorResourceDiscoveryConf(conf, FPGA, fpgaDiscovery)

      val resources = getAllResources(conf, SPARK_EXECUTOR_PREFIX, None)
      assert(resources.size === 2)
      val gpuValue = resources.get(GPU)
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.deep == Array("0", "1").deep, "should have 0,1 entries")

      val fpgaValue = resources.get(FPGA)
      assert(fpgaValue.nonEmpty, "Should have a gpu entry")
      assert(fpgaValue.get.name == "fpga", "name should be fpga")
      assert(fpgaValue.get.addresses.size == 3, "Should have 3 indexes")
      assert(fpgaValue.get.addresses.deep == Array("f1", "f2", "f3").deep,
        "should have f1,f2,f3 entries")
    }
  }

  test("list resource ids") {
    val conf = new SparkConf
    setDriverResourceAmountConf(conf, GPU, "2")
    var resources = listResourceIds(conf, SPARK_DRIVER_PREFIX)
    assert(resources.size === 1, "should only have GPU for resource")
    assert(resources(0).resourceName == GPU, "name should be gpu")

    setDriverResourceAmountConf(conf, FPGA, "2")
    val resourcesMap = listResourceIds(conf, SPARK_DRIVER_PREFIX)
      .map{ rId => (rId.resourceName, 1)}.toMap
    assert(resourcesMap.size === 2, "should only have GPU for resource")
    assert(resourcesMap.get(GPU).nonEmpty, "should have GPU")
    assert(resourcesMap.get(FPGA).nonEmpty, "should have FPGA")
  }

  test("parse resource request") {
    val conf = new SparkConf
    setDriverResourceAmountConf(conf, GPU, "2")
    val gpuResourceID = ResourceID(SPARK_DRIVER_PREFIX, GPU)
    var request = parseResourceRequest(conf, gpuResourceID)
    assert(request.id.resourceName === GPU, "should only have GPU for resource")
    assert(request.count === 2, "GPU count should be 2")
    assert(request.discoveryScript === None, "discovery script should be empty")
    assert(request.vendor === None, "vendor should be empty")

    val vendor = "nvidia.com"
    val discoveryScript = "discoveryScriptGPU"
    setDriverResourceDiscoveryConf(conf, GPU, discoveryScript)
    setDriverResourceVendorConf(conf, GPU, vendor)
    request = parseResourceRequest(conf, gpuResourceID)
    assert(request.id.resourceName === GPU, "should only have GPU for resource")
    assert(request.count === 2, "GPU count should be 2")
    assert(request.discoveryScript.get === discoveryScript, "discovery script should be empty")
    assert(request.vendor.get === vendor, "vendor should be empty")

    conf.remove(s"${gpuResourceID.confPrefix}$AMOUNT")
    val error = intercept[SparkException] {
      request = parseResourceRequest(conf, gpuResourceID)
    }.getMessage()

    assert(error.contains("You must specify an amount for gpu"))
  }

  test("Resource discoverer multiple gpus on driver") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val gpuDiscovery = writeStringToFileAndSetPermissions(gpuFile,
        """'{"name": "gpu", "addresses": ["0", "1"]}'""")
      setDriverResourceAmountConf(conf, GPU, "2")
      setDriverResourceDiscoveryConf(conf, GPU, gpuDiscovery)

      // make sure it reads from correct config, here it should use driver
      val resources = getAllResources(conf, SPARK_DRIVER_PREFIX, None)
      val gpuValue = resources.get(GPU)
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.name == "gpu", "name should be gpu")
      assert(gpuValue.get.addresses.size == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.deep == Array("0", "1").deep, "should have 0,1 entries")
    }
  }

  test("Resource discoverer script returns mismatched name") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val gpuDiscovery = writeStringToFileAndSetPermissions(gpuFile,
        """'{"name": "fpga", "addresses": ["0", "1"]}'""")
      val request =
        ResourceRequest(
          ResourceID(SPARK_DRIVER_PREFIX, GPU),
          2,
          Some(gpuDiscovery),
          None)

      val error = intercept[SparkException] {
        discoverResource(request)
      }.getMessage()

      assert(error.contains("Error running the resource discovery script, script " +
        "returned resource name: fpga and we were expecting gpu"))
    }
  }

  test("Resource discoverer script returns invalid format") {
    val conf = new SparkConf
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val gpuFile = new File(dir, "gpuDiscoverScript")
      val gpuDiscovery = writeStringToFileAndSetPermissions(gpuFile,
        """'{"addresses": ["0", "1"]}'""")

      val request =
        ResourceRequest(
          ResourceID(SPARK_EXECUTOR_PREFIX, GPU),
          2,
          Some(gpuDiscovery),
          None)

      val error = intercept[SparkException] {
        discoverResource(request)
      }.getMessage()

      assert(error.contains("Exception parsing the resources in"))
    }
  }

  test("Resource discoverer script doesn't exist") {
    val conf = new SparkConf
    withTempDir { dir =>
      val file1 = new File(dir, "bogusfilepath")
      try {
        val request =
          ResourceRequest(
            ResourceID(SPARK_EXECUTOR_PREFIX, GPU),
            2,
            Some(file1.getPath()),
            None)

        val error = intercept[SparkException] {
          discoverResource(request)
        }.getMessage()

        assert(error.contains("doesn't exist"))
      } finally {
        JavaFiles.deleteIfExists(file1.toPath())
      }
    }
  }

  test("gpu's specified but not a discovery script") {
    val request = ResourceRequest(ResourceID(SPARK_EXECUTOR_PREFIX, GPU), 2, None, None)

    val error = intercept[SparkException] {
      discoverResource(request)
    }.getMessage()

    assert(error.contains("User is expecting to use resource: gpu but " +
      "didn't specify a discovery script!"))
  }
}
