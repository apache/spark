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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceUtils._

class ResourceProfileSuite extends SparkFunSuite {

  override def afterEach() {
    try {
      ResourceProfile.resetDefaultProfile(new SparkConf)
    } finally {
      super.afterEach()
    }
  }

  test("Default ResourceProfile") {
    val rprof = ResourceProfile.getOrCreateDefaultProfile(new SparkConf)
    assert(rprof.id === ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rprof.executorResources.size === 2,
      "Executor resources should contain cores and memory by default")
    assert(rprof.executorResources(ResourceProfile.CORES).amount === 1,
      s"Executor resources should have 1 core")
    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 1024,
      s"Executor resources should have 1024 memory")
    assert(rprof.executorResources(ResourceProfile.MEMORY).units === "b",
      s"Executor resources should have 1024 memory")
    assert(rprof.taskResources.size === 1,
      "Task resources should just contain cpus by default")
    assert(rprof.taskResources(ResourceProfile.CPUS).amount === 1,
      s"Task resources should have 1 cpu")
  }

  test("Default ResourceProfile with app level resources specified") {
    val conf = new SparkConf
    conf.set("spark.task.resource.gpu.amount", "1")
    conf.set(s"$SPARK_EXECUTOR_PREFIX.resource.gpu.amount", "1")
    conf.set(s"$SPARK_EXECUTOR_PREFIX.resource.gpu.discoveryScript", "nameOfScript")
    val all = conf.getAllWithPrefix(s"$SPARK_EXECUTOR_PREFIX.$RESOURCE_DOT")
    logInfo("tests ids are: " + all.mkString(","))
    val rprof = ResourceProfile.getOrCreateDefaultProfile(conf)
    assert(rprof.id === ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val execResources = rprof.executorResources
    assert(execResources.size === 3,
      "Executor resources should contain cores, memory, and gpu " + execResources)
    assert(rprof.taskResources.size === 2,
      "Task resources should just contain cpus and gpu")
    assert(execResources.contains("resource.gpu"), "Executor resources should have gpu")
    assert(rprof.taskResources.contains("resource.gpu"), "Task resources should have gpu")
  }

  test("Create ResourceProfile") {
    val rprof = new ResourceProfile()
    assert(rprof.id > ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rprof.executorResources === Map.empty)
    assert(rprof.taskResources === Map.empty)

    val taskReq = new TaskResourceRequest("resource.gpu", 1)
    val execReq = new ExecutorResourceRequest("resource.gpu", 2, "", "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)

    assert(rprof.executorResources.size === 1)
    assert(rprof.executorResources.contains("resource.gpu"),
      "Executor resources should have gpu")
    assert(rprof.executorResources.get("resource.gpu").get.vendor === "nvidia",
      "gpu vendor should be nvidia")
    assert(rprof.executorResources.get("resource.gpu").get.discoveryScript === "myscript",
      "discoveryScript should be myscript")
    assert(rprof.executorResources.get("resource.gpu").get.amount === 2,
    "gpu amount should be 2")
    assert(rprof.executorResources.get("resource.gpu").get.units === "",
    "units should be empty")

    assert(rprof.taskResources.size === 1, "Should have 1 task resource")
    assert(rprof.taskResources.contains("resource.gpu"), "Task resources should have gpu")
    assert(rprof.taskResources.get("resource.gpu").get.amount === 1,
      "Task resources should have 1 gpu")

    val cpuTaskReq = new TaskResourceRequest(ResourceProfile.CPUS, 1)
    val coresExecReq = new ExecutorResourceRequest(ResourceProfile.CORES, 2)
    val memExecReq = new ExecutorResourceRequest(ResourceProfile.MEMORY, 4096, "mb")
    val omemExecReq = new ExecutorResourceRequest(ResourceProfile.OVERHEAD_MEM, 2048)
    val pysparkMemExecReq = new ExecutorResourceRequest(ResourceProfile.PYSPARK_MEM, 1024)

    rprof.require(cpuTaskReq)
    rprof.require(coresExecReq)
    rprof.require(memExecReq)
    rprof.require(omemExecReq)
    rprof.require(pysparkMemExecReq)

    assert(rprof.executorResources.size === 5)
    assert(rprof.executorResources(ResourceProfile.CORES).amount === 2,
      s"Executor resources should have 2 cores")
    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 4096,
      s"Executor resources should have 4096 memory")
    assert(rprof.executorResources(ResourceProfile.MEMORY).units === "mb",
      s"Executor resources should have memory units mb")
    assert(rprof.executorResources(ResourceProfile.OVERHEAD_MEM).amount === 2048,
      s"Executor resources should have 2048 overhead memory")
    assert(rprof.executorResources(ResourceProfile.PYSPARK_MEM).amount === 1024,
      s"Executor resources should have 1024 pyspark memory")

    assert(rprof.taskResources.size === 2)
    assert(rprof.taskResources("cpus").amount === 1, "Task resources should have cpu")

    val error = intercept[IllegalArgumentException] {
      rprof.require(new ExecutorResourceRequest("bogusResource", 1))
    }.getMessage()
    assert(error.contains("Executor resource not allowed"))

    val taskError = intercept[IllegalArgumentException] {
      rprof.require(new TaskResourceRequest("bogusTaskResource", 1))
    }.getMessage()
    assert(taskError.contains("Task resource not allowed"))
  }

  test("Test TaskResourceRequest fractional") {

    val rprof = new ResourceProfile()
    val taskReq = new TaskResourceRequest("resource.gpu", 0.33)
    rprof.require(taskReq)

    assert(rprof.taskResources.size === 1, "Should have 1 task resource")
    assert(rprof.taskResources.contains("resource.gpu"), "Task resources should have gpu")
    assert(rprof.taskResources.get("resource.gpu").get.amount === 0.33,
      "Task resources should have 0.33 gpu")

    val fpgaReq = new TaskResourceRequest("resource.fpga", 4.0)
    rprof.require(fpgaReq)

    assert(rprof.taskResources.size === 2, "Should have 2 task resource")
    assert(rprof.taskResources.contains("resource.fpga"), "Task resources should have gpu")
    assert(rprof.taskResources.get("resource.fpga").get.amount === 4.0,
      "Task resources should have 4.0 gpu")

    var taskError = intercept[AssertionError] {
      rprof.require(new TaskResourceRequest("resource.gpu", 1.5))
    }.getMessage()
    assert(taskError.contains("The resource amount 1.5 must be either <= 0.5, or a whole number."))

    taskError = intercept[AssertionError] {
      rprof.require(new TaskResourceRequest("resource.gpu", 0.7))
    }.getMessage()
    assert(taskError.contains("The resource amount 0.7 must be either <= 0.5, or a whole number."))
  }

  test("Internal confs") {
    val rprof = new ResourceProfile()
    val gpuTaskReq = new TaskResourceRequest("resource.gpu", 2)
    val gpuExecReq =
      new ExecutorResourceRequest("resource.gpu", 2, None, Some("someScript"))

    rprof.require(gpuTaskReq)
    rprof.require(gpuExecReq)

    val internalResourceConfs = ResourceProfile.createResourceProfileInternalConfs(rprof)
    val sparkConf = new SparkConf
    internalResourceConfs.foreach { case(key, value) => sparkConf.set(key, value) }
    val resourceReq = ResourceProfile.getResourceRequestsFromInternalConfs(sparkConf, rprof.getId)
    val taskReq = ResourceProfile.getTaskRequirementsFromInternalConfs(sparkConf, rprof.getId)

    assert(resourceReq.size === 1, "ResourceRequest should have 1 item")
    assert(resourceReq(0).id.resourceName === "gpu")
    assert(resourceReq(0).amount === 2)
    assert(resourceReq(0).discoveryScript === Some("someScript"))

    assert(taskReq.getTaskResources.size === 1,
      "TaskResourceRequirements should have 1 item")
    assert(taskReq.getTaskResources.contains("resource.gpu"))
    assert(taskReq.getTaskResources("resource.gpu").amount === 2)
  }
}
