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

class ImmutableResourceProfileSuite extends SparkFunSuite {

  override def afterEach() {
    try {
      ImmutableResourceProfile.reInitDefaultProfile(new SparkConf)
    } finally {
      super.afterEach()
    }
  }

  test("Default ImmutableResourceProfile") {
    val rprof = ImmutableResourceProfile.getOrCreateDefaultProfile(new SparkConf)
    assert(rprof.id === ImmutableResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rprof.executorResources.size === 2,
      "Executor resources should contain cores and memory by default")
    assert(rprof.executorResources(ResourceProfile.CORES).amount === 1,
      s"Executor resources should have 1 core")
    assert(rprof.getExecutorCores.get === 1,
      s"Executor resources should have 1 core")
    assert(rprof.executorResources(ResourceProfile.MEMORY).amount === 1024,
      s"Executor resources should have 1024 memory")
    assert(rprof.taskResources.size === 1,
      "Task resources should just contain cpus by default")
    assert(rprof.taskResources(ResourceProfile.CPUS).amount === 1,
      s"Task resources should have 1 cpu")
    assert(rprof.getTaskCpus.get === 1,
      s"Task resources should have 1 cpu")
  }

  test("Default ImmutableResourceProfile with app level resources specified") {
    val conf = new SparkConf
    conf.set("spark.task.resource.gpu.amount", "1")
    conf.set(s"$SPARK_EXECUTOR_PREFIX.resource.gpu.amount", "1")
    conf.set(s"$SPARK_EXECUTOR_PREFIX.resource.gpu.discoveryScript", "nameOfScript")
    val rprof = ImmutableResourceProfile.getOrCreateDefaultProfile(conf)
    assert(rprof.id === ImmutableResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val execResources = rprof.executorResources
    assert(execResources.size === 3,
      "Executor resources should contain cores, memory, and gpu " + execResources)
    assert(rprof.taskResources.size === 2,
      "Task resources should just contain cpus and gpu")
    assert(execResources.contains("gpu"), "Executor resources should have gpu")
    assert(rprof.taskResources.contains("gpu"), "Task resources should have gpu")
  }

  test("Internal confs") {
    val rprof = new ResourceProfile()
    val gpuExecReq =
      new ExecutorResourceRequests().resource("gpu", 2, "someScript")
    rprof.require(gpuExecReq)
    val immrprof = new ImmutableResourceProfile(rprof.executorResources, rprof.taskResources)
    val internalResourceConfs =
      ImmutableResourceProfile.createResourceProfileInternalConfs(immrprof)
    val sparkConf = new SparkConf
    internalResourceConfs.foreach { case(key, value) => sparkConf.set(key, value) }
    val resourceReq =
      ImmutableResourceProfile.getResourceRequestsFromInternalConfs(sparkConf, immrprof.id)

    assert(resourceReq.size === 1, "ResourceRequest should have 1 item")
    assert(resourceReq(0).id.resourceName === "gpu")
    assert(resourceReq(0).amount === 2)
    assert(resourceReq(0).discoveryScript === Some("someScript"))
  }

  test("maxTasksPerExecutor cpus") {
    val sparkConf = new SparkConf()
      .set(EXECUTOR_CORES, 1)
    val rprof = new ResourceProfile()
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val execReq =
      new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)
    val immrprof = new ImmutableResourceProfile(rprof.executorResources, rprof.taskResources)
    assert(immrprof.limitingResource(sparkConf) == "cpus")
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 1)
  }

  test("maxTasksPerExecutor gpus") {
    val sparkConf = new SparkConf()
      .set(EXECUTOR_CORES, 6)
    val rprof = new ResourceProfile()
    val taskReq = new TaskResourceRequests().resource("gpu", 2)
    val execReq =
      new ExecutorResourceRequests().resource("gpu", 4, "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)
    val immrprof = new ImmutableResourceProfile(rprof.executorResources, rprof.taskResources)
    assert(immrprof.limitingResource(sparkConf) == "gpu")
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 2)
    assert(immrprof.getNumSlotsPerAddress("gpu", sparkConf) == 1)
  }

  test("maxTasksPerExecutor gpus fractional") {
    val sparkConf = new SparkConf()
        .set(EXECUTOR_CORES, 6)
    val rprof = new ResourceProfile()
    val taskReq = new TaskResourceRequests().resource("gpu", 0.5)
    val execReq = new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)
    val immrprof = new ImmutableResourceProfile(rprof.executorResources, rprof.taskResources)
    assert(immrprof.limitingResource(sparkConf) == "gpu")
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 4)
    assert(immrprof.getNumSlotsPerAddress("gpu", sparkConf) == 2)
  }

  test("maxTasksPerExecutor multiple resources") {
    val sparkConf = new SparkConf()
      .set(EXECUTOR_CORES, 6)
    val rprof = new ResourceProfile()
    val taskReqs = new TaskResourceRequests()
    val execReqs = new ExecutorResourceRequests()
    taskReqs.resource("gpu", 1)
    execReqs.resource("gpu", 6, "myscript", "nvidia")
    taskReqs.resource("fpga", 1)
    execReqs.resource("fpga", 4, "myscript", "nvidia")
    rprof.require(taskReqs).require(execReqs)
    val immrprof = new ImmutableResourceProfile(rprof.executorResources, rprof.taskResources)
    assert(immrprof.limitingResource(sparkConf) == "fpga")
    assert(immrprof.maxTasksPerExecutor(sparkConf) == 4)
    assert(immrprof.getNumSlotsPerAddress("gpu", sparkConf) == 1)
    assert(immrprof.getNumSlotsPerAddress("fpga", sparkConf) == 1)
  }
}
