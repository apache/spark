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
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY
import org.apache.spark.resource.TestResourceIDs._

class ResourceProfileSuite extends SparkFunSuite {

  override def beforeAll(): Unit = {
    try {
      ResourceProfile.clearDefaultProfile()
    } finally {
      super.beforeAll()
    }
  }

  override def afterEach(): Unit = {
    try {
      ResourceProfile.clearDefaultProfile()
    } finally {
      super.afterEach()
    }
  }

  test("Default ResourceProfile") {
    val rprof = ResourceProfile.getOrCreateDefaultProfile(new SparkConf)
    assert(rprof.id === ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rprof.executorResources.size === 3,
      "Executor resources should contain cores, heap and offheap memory by default")
    assert(rprof.executorResources(ResourceProfile.CORES).amount === 1,
      "Executor resources should have 1 core")
    assert(rprof.getExecutorCores.get === 1,
      "Executor resources should have 1 core")
    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 1024,
      "Executor resources should have 1024 memory")
    assert(rprof.executorResources.get(ResourceProfile.PYSPARK_MEM) == None,
      "pyspark memory empty if not specified")
    assert(rprof.executorResources.get(ResourceProfile.OVERHEAD_MEM) == None,
      "overhead memory empty if not specified")
    assert(rprof.executorResources(ResourceProfile.OFFHEAP_MEM).amount === 0,
      "Executor resources should have 0 offheap memory")
    assert(rprof.taskResources.size === 1,
      "Task resources should just contain cpus by default")
    assert(rprof.taskResources(ResourceProfile.CPUS).amount === 1,
      "Task resources should have 1 cpu")
    assert(rprof.getTaskCpus.get === 1,
      "Task resources should have 1 cpu")
  }

  test("Default ResourceProfile with app level resources specified") {
    val conf = new SparkConf
    conf.set(PYSPARK_EXECUTOR_MEMORY.key, "2g")
    conf.set(EXECUTOR_MEMORY_OVERHEAD.key, "1g")
    conf.set(EXECUTOR_MEMORY.key, "4g")
    conf.set(EXECUTOR_CORES.key, "4")
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "3m")
    conf.set(TASK_GPU_ID.amountConf, "1")
    conf.set(EXECUTOR_GPU_ID.amountConf, "1")
    conf.set(EXECUTOR_GPU_ID.discoveryScriptConf, "nameOfScript")
    val rprof = ResourceProfile.getOrCreateDefaultProfile(conf)
    assert(rprof.id === ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val execResources = rprof.executorResources
    assert(execResources.size === 6, s"Executor resources should contain cores, pyspark " +
      s"memory, memory overhead, memory, offHeap memory and gpu $execResources")
    assert(execResources.contains("gpu"), "Executor resources should have gpu")
    assert(rprof.executorResources(ResourceProfile.CORES).amount === 4,
      "Executor resources should have 4 core")
    assert(rprof.getExecutorCores.get === 4,
      "Executor resources should have 4 core")
    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 4096,
      "Executor resources should have 1024 memory")
    assert(rprof.executorResources(ResourceProfile.PYSPARK_MEM).amount == 2048,
      "pyspark memory empty if not specified")
    assert(rprof.executorResources(ResourceProfile.OVERHEAD_MEM).amount == 1024,
      "overhead memory empty if not specified")
    assert(rprof.executorResources(ResourceProfile.OFFHEAP_MEM).amount == 3,
      "Executor resources should have 3 offHeap memory")
    assert(rprof.taskResources.size === 2,
      "Task resources should just contain cpus and gpu")
    assert(rprof.taskResources.contains("gpu"), "Task resources should have gpu")
  }

  test("test default profile task gpus fractional") {
    val sparkConf = new SparkConf()
      .set(EXECUTOR_GPU_ID.amountConf, "2")
      .set(TASK_GPU_ID.amountConf, "0.33")
    val immrprof = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    assert(immrprof.taskResources.get("gpu").get.amount == 0.33)
  }

  test("maxTasksPerExecutor cpus") {
    val sparkConf = new SparkConf()
      .set(EXECUTOR_CORES, 1)
    val rprof = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val execReq =
      new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)
    val immrprof = new ResourceProfile(rprof.executorResources, rprof.taskResources)
    assert(immrprof.limitingResource(sparkConf) == "cpus")
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 1)
  }

  test("maxTasksPerExecutor/limiting no executor cores") {
    val sparkConf = new SparkConf().setMaster("spark://testing")
    val rprof = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val execReq =
      new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)
    val immrprof = new ResourceProfile(rprof.executorResources, rprof.taskResources)
    assert(immrprof.limitingResource(sparkConf) == "gpu")
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 2)
    assert(immrprof.isCoresLimitKnown == false)
  }

  test("maxTasksPerExecutor/limiting no other resource no executor cores") {
    val sparkConf = new SparkConf().setMaster("spark://testing")
    val immrprof = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    assert(immrprof.limitingResource(sparkConf) == "")
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 1)
    assert(immrprof.isCoresLimitKnown == false)
  }

  test("maxTasksPerExecutor/limiting executor cores") {
    val sparkConf = new SparkConf().setMaster("spark://testing").set(EXECUTOR_CORES, 2)
    val rprof = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val execReq =
      new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)
    val immrprof = new ResourceProfile(rprof.executorResources, rprof.taskResources)
    assert(immrprof.limitingResource(sparkConf) == ResourceProfile.CPUS)
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 2)
    assert(immrprof.isCoresLimitKnown == true)
  }


  test("Create ResourceProfile") {
    val rprof = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val eReq = new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(eReq)

    assert(rprof.executorResources.size === 1)
    assert(rprof.executorResources.contains("gpu"),
      "Executor resources should have gpu")
    assert(rprof.executorResources.get("gpu").get.vendor === "nvidia",
      "gpu vendor should be nvidia")
    assert(rprof.executorResources.get("gpu").get.discoveryScript === "myscript",
      "discoveryScript should be myscript")
    assert(rprof.executorResources.get("gpu").get.amount === 2,
    "gpu amount should be 2")

    assert(rprof.taskResources.size === 1, "Should have 1 task resource")
    assert(rprof.taskResources.contains("gpu"), "Task resources should have gpu")
    assert(rprof.taskResources.get("gpu").get.amount === 1,
      "Task resources should have 1 gpu")

    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(2).memory("4096")
    ereqs.memoryOverhead("2048").pysparkMemory("1024").offHeapMemory("3072")
    val treqs = new TaskResourceRequests()
    treqs.cpus(1)

    rprof.require(treqs)
    rprof.require(ereqs)

    assert(rprof.executorResources.size === 6)
    assert(rprof.executorResources(ResourceProfile.CORES).amount === 2,
      "Executor resources should have 2 cores")
    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 4096,
      "Executor resources should have 4096 memory")
    assert(rprof.executorResources(ResourceProfile.OVERHEAD_MEM).amount === 2048,
      "Executor resources should have 2048 overhead memory")
    assert(rprof.executorResources(ResourceProfile.PYSPARK_MEM).amount === 1024,
      "Executor resources should have 1024 pyspark memory")
    assert(rprof.executorResources(ResourceProfile.OFFHEAP_MEM).amount === 3072,
      "Executor resources should have 3072 offHeap memory")

    assert(rprof.taskResources.size === 2)
    assert(rprof.taskResources("cpus").amount === 1, "Task resources should have cpu")
  }

  test("test ResourceProfiles equal") {
    val rprofBuilder = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val eReq = new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprofBuilder.require(taskReq).require(eReq)
    val rprof = rprofBuilder.build

    val rprofBuilder2 = new ResourceProfileBuilder()
    val taskReq2 = new TaskResourceRequests().resource("gpu", 1)
    val eReq2 = new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprofBuilder2.require(taskReq2).require(eReq2)
    val rprof2 = rprofBuilder.build
    rprof2.setResourceProfileId(rprof.id)

    assert(rprof === rprof2, "resource profile equality not working")
    rprof2.setResourceProfileId(rprof.id + 1)
    assert(rprof.id != rprof2.id, "resource profiles should not have same id")
    assert(rprof.resourcesEqual(rprof2), "resource profile resourcesEqual not working")
  }

  test("Test ExecutorResourceRequests memory helpers") {
    val rprof = new ResourceProfileBuilder()
    val ereqs = new ExecutorResourceRequests()
    ereqs.memory("4g")
    ereqs.memoryOverhead("2000m").pysparkMemory("512000k").offHeapMemory("1g")
    rprof.require(ereqs)

    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 4096,
      "Executor resources should have 4096 memory")
    assert(rprof.executorResources(ResourceProfile.OVERHEAD_MEM).amount === 2000,
      "Executor resources should have 2000 overhead memory")
    assert(rprof.executorResources(ResourceProfile.PYSPARK_MEM).amount === 500,
      "Executor resources should have 512 pyspark memory")
    assert(rprof.executorResources(ResourceProfile.OFFHEAP_MEM).amount === 1024,
      "Executor resources should have 1024 offHeap memory")
  }

  test("Test TaskResourceRequest fractional") {
    val rprof = new ResourceProfileBuilder()
    val treqs = new TaskResourceRequests().resource("gpu", 0.33)
    rprof.require(treqs)

    assert(rprof.taskResources.size === 1, "Should have 1 task resource")
    assert(rprof.taskResources.contains("gpu"), "Task resources should have gpu")
    assert(rprof.taskResources.get("gpu").get.amount === 0.33,
      "Task resources should have 0.33 gpu")

    val fpgaReqs = new TaskResourceRequests().resource("fpga", 4.0)
    rprof.require(fpgaReqs)

    assert(rprof.taskResources.size === 2, "Should have 2 task resource")
    assert(rprof.taskResources.contains("fpga"), "Task resources should have gpu")
    assert(rprof.taskResources.get("fpga").get.amount === 4.0,
      "Task resources should have 4.0 gpu")

    var taskError = intercept[AssertionError] {
      rprof.require(new TaskResourceRequests().resource("gpu", 1.5))
    }.getMessage()
    assert(taskError.contains("The resource amount 1.5 must be either <= 0.5, or a whole number."))

    taskError = intercept[AssertionError] {
      rprof.require(new TaskResourceRequests().resource("gpu", 0.7))
    }.getMessage()
    assert(taskError.contains("The resource amount 0.7 must be either <= 0.5, or a whole number."))
  }

  test("ResourceProfile has correct custom executor resources") {
    val rprof = new ResourceProfileBuilder()
    val eReq = new ExecutorResourceRequests()
      .cores(2).memory("4096")
      .memoryOverhead("2048").pysparkMemory("1024").offHeapMemory("3072")
      .resource("gpu", 2)
    rprof.require(eReq)

    // Update this if new resource type added
    assert(ResourceProfile.allSupportedExecutorResources.size === 5,
      "Executor resources should have 5 supported resources")
    assert(ResourceProfile.getCustomExecutorResources(rprof.build).size === 1,
      "Executor resources should have 1 custom resource")
  }

  test("ResourceProfile has correct custom task resources") {
    val rprof = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests()
      .resource("gpu", 1)
    val eReq = new ExecutorResourceRequests()
      .cores(2).memory("4096")
      .memoryOverhead("2048").pysparkMemory("1024").offHeapMemory("3072")
    rprof.require(taskReq).require(eReq)

    assert(ResourceProfile.getCustomTaskResources(rprof.build).size === 1,
      "Task resources should have 1 custom resource")
  }
}
