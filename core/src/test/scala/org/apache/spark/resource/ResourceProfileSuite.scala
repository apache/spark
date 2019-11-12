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

    val taskReq = new TaskResourceRequests().resource("resource.gpu", 1)
    val eReq = new ExecutorResourceRequests().resource("resource.gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(eReq)

    assert(rprof.executorResources.size === 1)
    assert(rprof.executorResources.contains("resource.gpu"),
      "Executor resources should have gpu")
    assert(rprof.executorResources.get("resource.gpu").get.vendor === "nvidia",
      "gpu vendor should be nvidia")
    assert(rprof.executorResources.get("resource.gpu").get.discoveryScript === "myscript",
      "discoveryScript should be myscript")
    assert(rprof.executorResources.get("resource.gpu").get.amount === 2,
    "gpu amount should be 2")

    assert(rprof.taskResources.size === 1, "Should have 1 task resource")
    assert(rprof.taskResources.contains("resource.gpu"), "Task resources should have gpu")
    assert(rprof.taskResources.get("resource.gpu").get.amount === 1,
      "Task resources should have 1 gpu")

    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(2).memory("4096")
    ereqs.memoryOverhead("2048").pysparkMemory("1024")
    val treqs = new TaskResourceRequests()
    treqs.cpus(1)

    rprof.require(treqs)
    rprof.require(ereqs)

    assert(rprof.executorResources.size === 5)
    assert(rprof.executorResources(ResourceProfile.CORES).amount === 2,
      s"Executor resources should have 2 cores")
    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 4096,
      s"Executor resources should have 4096 memory")
    assert(rprof.executorResources(ResourceProfile.OVERHEAD_MEM).amount === 2048,
      s"Executor resources should have 2048 overhead memory")
    assert(rprof.executorResources(ResourceProfile.PYSPARK_MEM).amount === 1024,
      s"Executor resources should have 1024 pyspark memory")

    assert(rprof.taskResources.size === 2)
    assert(rprof.taskResources("cpus").amount === 1, "Task resources should have cpu")

    val error = intercept[IllegalArgumentException] {
      rprof.require(new ExecutorResourceRequests().resource("bogusResource", 1))
    }.getMessage()
    assert(error.contains("Executor resource not allowed"))

    val taskError = intercept[IllegalArgumentException] {
      rprof.require(new TaskResourceRequests().resource("bogusTaskResource", 1))
    }.getMessage()
    assert(taskError.contains("Task resource not allowed"))
  }

  test("Test ExecutorResourceRequests memory helpers") {
    val rprof = new ResourceProfile()
    val ereqs = new ExecutorResourceRequests()
    ereqs.memory("4g")
    ereqs.memoryOverhead("2000m").pysparkMemory("512000k")
    rprof.require(ereqs)

    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 4096,
      s"Executor resources should have 4096 memory")
    assert(rprof.executorResources(ResourceProfile.OVERHEAD_MEM).amount === 2000,
      s"Executor resources should have 2000 overhead memory")
    assert(rprof.executorResources(ResourceProfile.PYSPARK_MEM).amount === 500,
      s"Executor resources should have 512 pyspark memory")
  }

  test("Test TaskResourceRequest fractional") {
    val rprof = new ResourceProfile()
    val treqs = new TaskResourceRequests().resource("resource.gpu", 0.33)
    rprof.require(treqs)

    assert(rprof.taskResources.size === 1, "Should have 1 task resource")
    assert(rprof.taskResources.contains("resource.gpu"), "Task resources should have gpu")
    assert(rprof.taskResources.get("resource.gpu").get.amount === 0.33,
      "Task resources should have 0.33 gpu")

    val fpgaReqs = new TaskResourceRequests().resource("resource.fpga", 4.0)
    rprof.require(fpgaReqs)

    assert(rprof.taskResources.size === 2, "Should have 2 task resource")
    assert(rprof.taskResources.contains("resource.fpga"), "Task resources should have gpu")
    assert(rprof.taskResources.get("resource.fpga").get.amount === 4.0,
      "Task resources should have 4.0 gpu")

    var taskError = intercept[AssertionError] {
      rprof.require(new TaskResourceRequests().resource("resource.gpu", 1.5))
    }.getMessage()
    assert(taskError.contains("The resource amount 1.5 must be either <= 0.5, or a whole number."))

    taskError = intercept[AssertionError] {
      rprof.require(new TaskResourceRequests().resource("resource.gpu", 0.7))
    }.getMessage()
    assert(taskError.contains("The resource amount 0.7 must be either <= 0.5, or a whole number."))
  }
}

