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

package org.apache.spark.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark.internal.config
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfile, ResourceProfileBuilder, TaskResourceRequests}
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}

class ResourceProfileSuite extends DAGSchedulerTestHelper {

  test("test default resource profile") {
    val rdd = sc.parallelize(1 to 10).map(x => (x, x))
    val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val rp = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    assert(rp.id == scheduler.sc.resourceProfileManager.defaultResourceProfile.id)
  }

  test("test 1 resource profile") {
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val rdd = sc.parallelize(1 to 10).map(x => (x, x)).withResources(rp1)
    val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val rpMerged = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    val expectedid = Option(rdd.getResourceProfile).map(_.id)
    assert(expectedid.isDefined)
    assert(expectedid.get != ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rpMerged.id == expectedid.get)
  }

  test("test 2 resource profiles errors by default") {
    import org.apache.spark.resource._
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(2)
    val rp2 = new ResourceProfileBuilder().require(ereqs2).require(treqs2).build

    val rdd = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val error = intercept[IllegalArgumentException] {
      val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
      scheduler.mergeResourceProfilesForStage(resourceprofiles)
    }.getMessage()

    assert(error.contains("Multiple ResourceProfiles specified in the RDDs"))
  }

  test("test 2 resource profile with merge conflict config true") {
    conf.set(config.RESOURCE_PROFILE_MERGE_CONFLICTS.key, "true")

    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(2)
    val rp2 = new ResourceProfileBuilder().require(ereqs2).require(treqs2).build

    val rdd = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val mergedRp = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)
  }

  test("test multiple resource profiles created from merging use same rp") {
    conf.set(config.RESOURCE_PROFILE_MERGE_CONFLICTS.key, "true")

    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(2)
    val rp2 = new ResourceProfileBuilder().require(ereqs2).require(treqs2).build

    val rdd = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val (_, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val mergedRp = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)

    // test that instead of creating a new merged profile, we use the already created one
    val rdd2 = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val (_, resourceprofiles2) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd2)
    val mergedRp2 = scheduler.mergeResourceProfilesForStage(resourceprofiles2)
    assert(mergedRp2.id === mergedRp.id)
    assert(mergedRp2.getTaskCpus.get == 2)
    assert(mergedRp2.getExecutorCores.get == 4)
  }

  test("test merge 2 resource profiles multiple configs") {
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(2)
    val rp1 = new ResourceProfile(ereqs.requests, treqs.requests)
    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(1)
    val rp2 = new ResourceProfile(ereqs2.requests, treqs2.requests)
    var mergedRp = scheduler.mergeResourceProfiles(rp1, rp2)

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)

    val ereqs3 = new ExecutorResourceRequests().cores(1).resource(GPU, 1, "disc")
    val treqs3 = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp3 = new ResourceProfile(ereqs3.requests, treqs3.requests)
    val ereqs4 = new ExecutorResourceRequests().cores(2)
    val treqs4 = new TaskResourceRequests().cpus(2)
    val rp4 = new ResourceProfile(ereqs4.requests, treqs4.requests)
    mergedRp = scheduler.mergeResourceProfiles(rp3, rp4)

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 2)
    assert(mergedRp.executorResources.size == 2)
    assert(mergedRp.taskResources.size == 2)
    assert(mergedRp.executorResources.get(GPU).get.amount == 1)
    assert(mergedRp.executorResources.get(GPU).get.discoveryScript == "disc")
    assert(mergedRp.taskResources.get(GPU).get.amount == 1)

    val ereqs5 = new ExecutorResourceRequests().cores(1).memory("3g")
      .memoryOverhead("1g").pysparkMemory("2g").offHeapMemory("4g").resource(GPU, 1, "disc")
    val treqs5 = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp5 = new ResourceProfile(ereqs5.requests, treqs5.requests)
    val ereqs6 = new ExecutorResourceRequests().cores(8).resource(FPGA, 2, "fdisc")
    val treqs6 = new TaskResourceRequests().cpus(2).resource(FPGA, 1)
    val rp6 = new ResourceProfile(ereqs6.requests, treqs6.requests)
    mergedRp = scheduler.mergeResourceProfiles(rp5, rp6)

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 8)
    assert(mergedRp.executorResources.size == 7)
    assert(mergedRp.taskResources.size == 3)
    assert(mergedRp.executorResources.get(GPU).get.amount == 1)
    assert(mergedRp.executorResources.get(GPU).get.discoveryScript == "disc")
    assert(mergedRp.taskResources.get(GPU).get.amount == 1)
    assert(mergedRp.executorResources.get(FPGA).get.amount == 2)
    assert(mergedRp.executorResources.get(FPGA).get.discoveryScript == "fdisc")
    assert(mergedRp.taskResources.get(FPGA).get.amount == 1)
    assert(mergedRp.executorResources.get(ResourceProfile.MEMORY).get.amount == 3072)
    assert(mergedRp.executorResources.get(ResourceProfile.PYSPARK_MEM).get.amount == 2048)
    assert(mergedRp.executorResources.get(ResourceProfile.OVERHEAD_MEM).get.amount == 1024)
    assert(mergedRp.executorResources.get(ResourceProfile.OFFHEAP_MEM).get.amount == 4096)

    val ereqs7 = new ExecutorResourceRequests().cores(1).memory("3g")
      .resource(GPU, 4, "disc")
    val treqs7 = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp7 = new ResourceProfile(ereqs7.requests, treqs7.requests)
    val ereqs8 = new ExecutorResourceRequests().cores(1).resource(GPU, 2, "fdisc")
    val treqs8 = new TaskResourceRequests().cpus(1).resource(GPU, 2)
    val rp8 = new ResourceProfile(ereqs8.requests, treqs8.requests)
    mergedRp = scheduler.mergeResourceProfiles(rp7, rp8)

    assert(mergedRp.getTaskCpus.get == 1)
    assert(mergedRp.getExecutorCores.get == 1)
    assert(mergedRp.executorResources.get(GPU).get.amount == 4)
    assert(mergedRp.executorResources.get(GPU).get.discoveryScript == "disc")
    assert(mergedRp.taskResources.get(GPU).get.amount == 2)
  }

  test("test merge 3 resource profiles") {
    conf.set(config.RESOURCE_PROFILE_MERGE_CONFLICTS.key, "true")
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfile(ereqs.requests, treqs.requests)
    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(1)
    val rp2 = new ResourceProfile(ereqs2.requests, treqs2.requests)
    val ereqs3 = new ExecutorResourceRequests().cores(3)
    val treqs3 = new TaskResourceRequests().cpus(2)
    val rp3 = new ResourceProfile(ereqs3.requests, treqs3.requests)
    var mergedRp = scheduler.mergeResourceProfilesForStage(HashSet(rp1, rp2, rp3))

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)
  }

}
