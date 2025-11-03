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
import org.apache.spark.internal.config.Tests.RESOURCES_WARNING_TESTING
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
      assert(gpuValue.get.addresses.length == 2, "Should have 2 indexes")
      assert(gpuValue.get.addresses.sameElements(Array("0", "1")), "should have 0,1 entries")

      val fpgaValue = resources.get(FPGA)
      assert(fpgaValue.nonEmpty, "Should have a gpu entry")
      assert(fpgaValue.get.name == "fpga", "name should be fpga")
      assert(fpgaValue.get.addresses.length == 3, "Should have 3 indexes")
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

      val rp = rpBuilder.require(ereqs).require(treqs).build()
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
    val resources = listResourceIds(conf, SPARK_DRIVER_PREFIX)
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
      assert(gpuValue.get.addresses.length == 2, "Should have 2 indexes")
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

  test("SPARK-45527 warnOnWastedResources for ResourceProfile") {
    val conf = new SparkConf()
    conf.set("spark.executor.cores", "10")
    conf.set("spark.task.cpus", "1")
    conf.set(RESOURCES_WARNING_TESTING, true)

    // cpu limiting task number = 10/1, gpu limiting task number = 1/0.1
    var ereqs = new ExecutorResourceRequests().cores(10).resource("gpu", 1)
    var treqs = new TaskResourceRequests().cpus(1).resource("gpu", 0.1)
    var rp = new ResourceProfileBuilder().require(ereqs).require(treqs).build()
    // no exception,
    warnOnWastedResources(rp, conf)

    ereqs = new ExecutorResourceRequests().cores(10).resource("gpu", 1)
    treqs = new TaskResourceRequests().cpus(2).resource("gpu", 0.2)
    rp = new ResourceProfileBuilder().require(ereqs).require(treqs).build()
    // no exception,
    warnOnWastedResources(rp, conf)

    ereqs = new ExecutorResourceRequests().cores(20).resource("gpu", 2)
    treqs = new TaskResourceRequests().cpus(2).resource("gpu", 0.2)
    rp = new ResourceProfileBuilder().require(ereqs).require(treqs).build()
    // no exception,
    warnOnWastedResources(rp, conf)

    var msg: String = ""

    // Test cpu limiting task number
    // format: (executor.core, task.cpus, executor.gpu, task.gpu, expected runnable tasks)
    Seq(
      (10, 2, 1, 0.1, 5), // cpu limiting task number=10/2=5, gpu limiting task number = 1/0.1 = 10
      (10, 3, 1, 0.1, 3), // cpu limiting task number=10/3=3, gpu limiting task number = 1/0.1 = 10
      (10, 4, 1, 0.1, 2), // cpu limiting task number=10/4=2, gpu limiting task number = 1/0.1 = 10
      (10, 5, 1, 0.1, 2), // cpu limiting task number=10/5=2, gpu limiting task number = 1/0.1 = 10
      (10, 6, 1, 0.1, 1), // cpu limiting task number=10/6=1, gpu limiting task number = 1/0.1 = 10
      (10, 10, 1, 0.1, 1), // cpu limiting task number=10/6=1, gpu limiting task number = 1/0.1 = 10
      (20, 7, 1, 0.1, 2), // cpu limiting task number=20/7=3, gpu limiting task number = 1/0.1 = 10
      (30, 7, 1, 0.1, 4), // cpu limiting task number=30/7=4, gpu limiting task number = 1/0.1 = 10
      (50, 14, 1, 0.1, 3) // cpu limiting task number=50/14=3, gpu limiting task number = 1/0.1=10
    ).foreach { case (executorCores, taskCpus: Int, executorGpus: Int, taskGpus: Double,
    expectedTaskNumber: Int) =>
      ereqs = new ExecutorResourceRequests().cores(executorCores).resource("gpu", executorGpus)
      treqs = new TaskResourceRequests().cpus(taskCpus).resource("gpu", taskGpus)
      rp = new ResourceProfileBuilder().require(ereqs).require(treqs).build()
      msg = intercept[SparkException] {
        warnOnWastedResources(rp, conf)
      }.getMessage
      assert(msg.contains("The configuration of resource: gpu (exec = 1, task = 0.1/10, runnable " +
        "tasks = 10) will result in wasted resources due to resource cpus limiting the number of " +
        s"runnable tasks per executor to: ${expectedTaskNumber}. Please adjust your configuration.")
      )
    }

    // Test gpu limiting task number
    // format: (executor.core, task.cpus, executor.gpu, task.gpu, expected runnable tasks)
    Seq(
      (10, 1, 1, 1.0/9, 9), // cpu limiting task number=10, gpu limiting task number = 9
      (10, 1, 1, 1.0/8, 8), // cpu limiting task number=10, gpu limiting task number = 8
      (10, 1, 1, 1.0/7, 7), // cpu limiting task number=10, gpu limiting task number = 7
      (10, 1, 1, 1.0/6, 6), // cpu limiting task number=10, gpu limiting task number = 6
      (10, 1, 1, 1.0/5, 5), // cpu limiting task number=10, gpu limiting task number = 5
      (10, 1, 1, 1.0/4, 4), // cpu limiting task number=10, gpu limiting task number = 4
      (10, 1, 1, 1.0/3, 3), // cpu limiting task number=10, gpu limiting task number = 3
      (10, 1, 1, 1.0/2, 2), // cpu limiting task number=10, gpu limiting task number = 2
      (10, 1, 1, 1.0, 1), // cpu limiting task number=10, gpu limiting task number = 1
      (30, 1, 2, 1.0/9, 2*9), // cpu limiting task number=30, gpu limiting task number = 2*9
      (30, 1, 2, 1.0/8, 2*8), // cpu limiting task number=30, gpu limiting task number = 2*8
      (30, 1, 2, 1.0/7, 2*7), // cpu limiting task number=30, gpu limiting task number = 2*7
      (30, 1, 2, 1.0/6, 2*6), // cpu limiting task number=30, gpu limiting task number = 2*6
      (30, 1, 2, 1.0/5, 2*5), // cpu limiting task number=30, gpu limiting task number = 2*5
      (30, 1, 2, 1.0/4, 2*4), // cpu limiting task number=30, gpu limiting task number = 2*4
      (30, 1, 2, 1.0/3, 2*3), // cpu limiting task number=30, gpu limiting task number = 2*3
      (30, 1, 2, 1.0/2, 2*2), // cpu limiting task number=30, gpu limiting task number = 2*2
      (30, 1, 2, 1.0, 2*1), // cpu limiting task number=30, gpu limiting task number = 2*1
      (30, 1, 2, 2.0, 1), // cpu limiting task number=30, gpu limiting task number = 1
      (70, 2, 7, 0.5, 7*2), // cpu limiting task number=30, gpu limiting task number = 7*1/0.5=
      (80, 3, 9, 2.0, 9/2) // cpu limiting task number=30, gpu limiting task number = 9/2
    ).foreach { case (executorCores, taskCpus: Int, executorGpus: Int, taskGpus: Double,
    expectedTaskNumber: Int) =>
      ereqs = new ExecutorResourceRequests().cores(executorCores).resource("gpu", executorGpus)
      treqs = new TaskResourceRequests().cpus(taskCpus).resource("gpu", taskGpus)
      rp = new ResourceProfileBuilder().require(ereqs).require(treqs).build()
      msg = intercept[SparkException] {
        warnOnWastedResources(rp, conf)
      }.getMessage
      assert(msg.contains(s"The configuration of cores (exec = ${executorCores} task = " +
        s"${taskCpus}, runnable tasks = ${executorCores/taskCpus}) " +
        "will result in wasted resources due to resource gpu limiting the number of runnable " +
        s"tasks per executor to: ${expectedTaskNumber}. Please adjust your configuration"))
    }
  }

  private class FakedTaskResourceProfile(
      val defaultRp: ResourceProfile,
      override val taskResources: Map[String, TaskResourceRequest])
    extends TaskResourceProfile(taskResources) {
    override protected[spark] def getCustomExecutorResources()
      : Map[String, ExecutorResourceRequest] = defaultRp.getCustomExecutorResources()
  }

  test("SPARK-45527 warnOnWastedResources for TaskResourceProfile when executor number = 1") {
    val conf = new SparkConf()
    conf.set("spark.executor.cores", "10")
    conf.set("spark.task.cpus", "1")
    conf.set(TASK_GPU_ID.amountConf, "0.1")
    conf.set(EXECUTOR_GPU_ID.amountConf, "1")
    conf.set(RESOURCES_WARNING_TESTING, true)

    val defaultDp = ResourceProfile.getOrCreateDefaultProfile(conf)

    // cpu limiting task number = 10/1, gpu limiting task number = 1/0.1
    var treqs = new TaskResourceRequests().cpus(1).resource("gpu", 0.1)
    var rp = new FakedTaskResourceProfile(defaultDp, treqs.requests)
    // no exception,
    warnOnWastedResources(rp, conf)

    var msg: String = ""

    // Test cpu limiting task number
    // format: (task cpu cores, task gpu amount, expected runnable tasks)
    // spark.executor.cores=60, spark.task.cpus=1, EXECUTOR_GPU_ID=6, TASK_GPU_ID=0.1
    Seq(
      (2, 0.1, 5), // cpu limiting task number = 10/2=5, gpu limiting task number = 1/0.1 = 10
      (3, 0.1, 3), // cpu limiting task number = 10/3 = 3, gpu limiting task number = 1/0.1 = 10
      (4, 0.1, 2), // cpu limiting task number = 10/4 = 2, gpu limiting task number = 1/0.1 = 10
      (5, 0.1, 2), // cpu limiting task number = 10/5 = 2, gpu limiting task number = 1/0.1 = 10
      (6, 0.1, 1), // cpu limiting task number = 10/6 = 1, gpu limiting task number = 1/0.1 = 10
      (7, 0.1, 1), // cpu limiting task number = 10/7 = 1, gpu limiting task number = 1/0.1 = 10
      (10, 0.1, 1) // cpu limiting task number = 10/10 = 1, gpu limiting task number = 1/0.1 = 10
    ).foreach { case (cores: Int, gpus: Double, expectedTaskNumber: Int) =>
      treqs = new TaskResourceRequests().cpus(cores).resource("gpu", gpus)
      rp = new FakedTaskResourceProfile(defaultDp, treqs.requests)
      msg = intercept[SparkException] {
        warnOnWastedResources(rp, conf)
      }.getMessage
      assert(msg.contains("The configuration of resource: gpu (exec = 1, task = 0.1/10, runnable " +
        "tasks = 10) will result in wasted resources due to resource cpus limiting the number of " +
        s"runnable tasks per executor to: $expectedTaskNumber. Please adjust your configuration.")
      )
    }

    // Test gpu limiting task number
    // format: (task cpu cores, task gpu amount, expected runnable tasks)
    // spark.executor.cores=60, spark.task.cpus=1, EXECUTOR_GPU_ID=6, TASK_GPU_ID=0.1
    Seq(
      (1, 0.111, 9), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.111=9
      (1, 0.125, 8), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.125=8
      (1, 0.142, 7), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.142=7
      (1, 0.166, 6), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.166=6
      (1, 0.2, 5), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.2=5
      (1, 0.25, 4), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.25=4
      (1, 0.333, 3), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.333=3
      (1, 0.5, 2), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.166=2
      (1, 0.6, 1), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.6=1
      (1, 0.7, 1), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.7=1
      (1, 0.8, 1), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.8=1
      (1, 0.9, 1), // cpu limiting task number = 10/1, gpu limiting task number = 1/0.9=1
      (1, 1.0, 1) // cpu limiting task number = 10/1, gpu limiting task number = 1/1.0=1
    ).foreach { case (cores: Int, gpus: Double, expectedTaskNumber: Int) =>
      treqs = new TaskResourceRequests().cpus(cores).resource("gpu", gpus)
      rp = new FakedTaskResourceProfile(defaultDp, treqs.requests)
      msg = intercept[SparkException] {
        warnOnWastedResources(rp, conf)
      }.getMessage
      assert(msg.contains("The configuration of cores (exec = 10 task = 1, runnable tasks = 10) " +
        "will result in wasted resources due to resource gpu limiting the number of runnable " +
        s"tasks per executor to: $expectedTaskNumber. Please adjust your configuration"))
    }
  }

  test("SPARK-45527 warnOnWastedResources for TaskResourceProfile when executor number > 1") {
    val conf = new SparkConf()
    conf.set("spark.executor.cores", "60")
    conf.set("spark.task.cpus", "1")
    conf.set(TASK_GPU_ID.amountConf, "0.1")
    conf.set(EXECUTOR_GPU_ID.amountConf, "6")
    conf.set(RESOURCES_WARNING_TESTING, true)

    val defaultDp = ResourceProfile.getOrCreateDefaultProfile(conf)

    // cpu limiting task number = 60/1, gpu limiting task number = 6/0.1
    var treqs = new TaskResourceRequests().cpus(1).resource("gpu", 0.1)
    var rp = new FakedTaskResourceProfile(defaultDp, treqs.requests)
    // no exception,
    warnOnWastedResources(rp, conf)

    // cpu limiting task number = 60/2 = 30, gpu limiting task number = 6/0.2 = 30
    treqs = new TaskResourceRequests().cpus(2).resource("gpu", 0.2)
    rp = new FakedTaskResourceProfile(defaultDp, treqs.requests)
    // no exception
    warnOnWastedResources(rp, conf)

    var msg: String = ""

    // Test cpu limiting task number
    // format: (task cpu cores, task gpu amount, expected runnable tasks)
    // spark.executor.cores=60, spark.task.cpus=1, EXECUTOR_GPU_ID=6, TASK_GPU_ID=0.1
    Seq(
      (4, 0.2, 15), // cpu limiting task number = 60/4=15, gpu limiting task number = 6/0.2 = 30
      (7, 0.2, 8), // cpu limiting task number = 60/7 = 8, gpu limiting task number = 6/0.2 = 30
      (30, 0.2, 2), // cpu limiting task number = 60/30 = 2, gpu limiting task number = 6/0.2 = 30
      (31, 0.2, 1), // cpu limiting task number = 60/31 = 1, gpu limiting task number = 6/0.2 = 30
      (55, 0.2, 1), // cpu limiting task number = 60/55 = 1, gpu limiting task number = 6/0.2 = 30
      (60, 0.2, 1) // cpu limiting task number = 60/60 = 1, gpu limiting task number = 6/0.2 = 30
    ).foreach { case (cores: Int, gpus: Double, expectedTaskNumber: Int) =>
      treqs = new TaskResourceRequests().cpus(cores).resource("gpu", gpus)
      rp = new FakedTaskResourceProfile(defaultDp, treqs.requests)
      msg = intercept[SparkException] {
        warnOnWastedResources(rp, conf)
      }.getMessage
      assert(msg.contains("The configuration of resource: gpu (exec = 6, task = 0.2/5, runnable " +
        "tasks = 30) will result in wasted resources due to resource cpus limiting the number of " +
        s"runnable tasks per executor to: $expectedTaskNumber. Please adjust your configuration.")
      )
    }

    // Test gpu limiting task number
    // format: (task cpu cores, task gpu amount, expected runnable tasks)
    // spark.executor.cores=60, spark.task.cpus=1, EXECUTOR_GPU_ID=6, TASK_GPU_ID=0.1
    Seq(
      (1, 0.111, 54), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.111=54
      (1, 0.125, 48), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.125=48
      (1, 0.142, 42), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.142=42
      (1, 0.166, 36), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.166=36
      (1, 0.2, 30), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.2=30
      (1, 0.25, 24), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.25=24
      (1, 0.33, 18), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.33=18
      (1, 0.5, 12), // cpu limiting task number = 60/1, gpu limiting task number = 6*1/0.5=12
      (1, 0.7, 6), // cpu limiting task number = 60/1 = 60, gpu limiting task number = 6*1/0.7 = 6
      (1, 1.0, 6), // cpu limiting task number = 60/1 = 60, gpu limiting task number = 6/1 = 6
      (1, 2.0, 3),  // cpu limiting task number = 60/1 = 60, gpu limiting task number = 6/2 = 3
      (1, 3.0, 2), // cpu limiting task number = 60/1 = 60, gpu limiting task number = 6/3 = 2
      (1, 4.0, 1), // cpu limiting task number = 60/1 = 60, gpu limiting task number = 6/4 = 1
      (1, 6.0, 1) // cpu limiting task number = 60/1 = 60, gpu limiting task number = 6/6 = 1
    ).foreach { case (cores: Int, gpus: Double, expectedTaskNumber: Int) =>
      treqs = new TaskResourceRequests().cpus(cores).resource("gpu", gpus)
      rp = new FakedTaskResourceProfile(defaultDp, treqs.requests)
      msg = intercept[SparkException] {
        warnOnWastedResources(rp, conf)
      }.getMessage
      assert(msg.contains("The configuration of cores (exec = 60 task = 1, runnable tasks = 60) " +
        "will result in wasted resources due to resource gpu limiting the number of runnable " +
        s"tasks per executor to: $expectedTaskNumber. Please adjust your configuration"))
    }
  }
}
