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

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests._
import org.apache.spark.scheduler.LiveListenerBus

class ResourceProfileManagerSuite extends SparkFunSuite {

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

  val listenerBus = new LiveListenerBus(new SparkConf())

  test("ResourceProfileManager") {
    val conf = new SparkConf().set(EXECUTOR_CORES, 4)
    val rpmanager = new ResourceProfileManager(conf, listenerBus)
    val defaultProf = rpmanager.defaultResourceProfile
    assert(defaultProf.id === ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(defaultProf.executorResources.size === 3,
      "Executor resources should contain cores, heap and offheap memory by default")
    assert(defaultProf.executorResources(ResourceProfile.CORES).amount === 4,
      s"Executor resources should have 4 cores")
  }

  test("isSupported yarn no dynamic allocation") {
    val conf = new SparkConf().setMaster("yarn").set(EXECUTOR_CORES, 4)
    conf.set(RESOURCE_PROFILE_MANAGER_TESTING.key, "true")
    val rpmanager = new ResourceProfileManager(conf, listenerBus)
    // default profile should always work
    val defaultProf = rpmanager.defaultResourceProfile
    val rprof = new ResourceProfileBuilder()
    val gpuExecReq =
      new ExecutorResourceRequests().resource("gpu", 2, "someScript")
    val immrprof = rprof.require(gpuExecReq).build
    val error = intercept[SparkException] {
      rpmanager.isSupported(immrprof)
    }.getMessage()

    assert(error.contains(
      "ResourceProfiles are only supported on YARN and Kubernetes and Standalone" +
        " with dynamic allocation"))
  }

  test("isSupported yarn with dynamic allocation") {
    val conf = new SparkConf().setMaster("yarn").set(EXECUTOR_CORES, 4)
    conf.set(DYN_ALLOCATION_ENABLED, true)
    conf.set(RESOURCE_PROFILE_MANAGER_TESTING.key, "true")
    val rpmanager = new ResourceProfileManager(conf, listenerBus)
    // default profile should always work
    val defaultProf = rpmanager.defaultResourceProfile
    val rprof = new ResourceProfileBuilder()
    val gpuExecReq =
      new ExecutorResourceRequests().resource("gpu", 2, "someScript")
    val immrprof = rprof.require(gpuExecReq).build
    assert(rpmanager.isSupported(immrprof) == true)
  }

  test("isSupported k8s with dynamic allocation") {
    val conf = new SparkConf().setMaster("k8s://foo").set(EXECUTOR_CORES, 4)
    conf.set(DYN_ALLOCATION_ENABLED, true)
    conf.set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
    conf.set(RESOURCE_PROFILE_MANAGER_TESTING.key, "true")
    val rpmanager = new ResourceProfileManager(conf, listenerBus)
    // default profile should always work
    val defaultProf = rpmanager.defaultResourceProfile
    val rprof = new ResourceProfileBuilder()
    val gpuExecReq =
      new ExecutorResourceRequests().resource("gpu", 2, "someScript", "nvidia")
    val immrprof = rprof.require(gpuExecReq).build
    assert(rpmanager.isSupported(immrprof) == true)
  }

  test("isSupported standalone with dynamic allocation") {
    val conf = new SparkConf().setMaster("spark://foo").set(EXECUTOR_CORES, 4)
    conf.set(DYN_ALLOCATION_ENABLED, true)
    conf.set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
    conf.set(RESOURCE_PROFILE_MANAGER_TESTING.key, "true")
    val rpmanager = new ResourceProfileManager(conf, listenerBus)
    // default profile should always work
    val defaultProf = rpmanager.defaultResourceProfile
    val rprof = new ResourceProfileBuilder()
    val gpuExecReq =
      new ExecutorResourceRequests().resource("gpu", 2, "someScript")
    val immrprof = rprof.require(gpuExecReq).build()
    assert(rpmanager.isSupported(immrprof))
  }

  test("isSupported with local mode") {
    val conf = new SparkConf().setMaster("local").set(EXECUTOR_CORES, 4)
    conf.set(RESOURCE_PROFILE_MANAGER_TESTING.key, "true")
    val rpmanager = new ResourceProfileManager(conf, listenerBus)
    // default profile should always work
    val defaultProf = rpmanager.defaultResourceProfile
    val rprof = new ResourceProfileBuilder()
    val gpuExecReq =
      new ExecutorResourceRequests().resource("gpu", 2, "someScript")
    val immrprof = rprof.require(gpuExecReq).build
    val error = intercept[SparkException] {
      rpmanager.isSupported(immrprof)
    }.getMessage()

    assert(error.contains(
      "ResourceProfiles are only supported on YARN and Kubernetes and Standalone" +
        " with dynamic allocation"))
  }

  test("ResourceProfileManager has equivalent profile") {
    val conf = new SparkConf().set(EXECUTOR_CORES, 4)
    val rpmanager = new ResourceProfileManager(conf, listenerBus)
    var rpAlreadyExist: Option[ResourceProfile] = None
    val checkId = 500
    for (i <- 1 to 1000) {
      val rprofBuilder = new ResourceProfileBuilder()
      val ereqs = new ExecutorResourceRequests()
      ereqs.cores(i).memory("4g").memoryOverhead("2000m")
      val treqs = new TaskResourceRequests()
      treqs.cpus(i)
      rprofBuilder.require(ereqs).require(treqs)
      val rprof = rprofBuilder.build
      rpmanager.addResourceProfile(rprof)
      if (i == checkId) rpAlreadyExist = Some(rprof)
    }
    val rpNotMatch = new ResourceProfileBuilder().build
    assert(rpmanager.getEquivalentProfile(rpNotMatch).isEmpty,
      s"resourceProfile should not have existed")

    val rprofBuilder = new ResourceProfileBuilder()
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(checkId).memory("4g").memoryOverhead("2000m")
    val treqs = new TaskResourceRequests()
    treqs.cpus(checkId)
    rprofBuilder.require(ereqs).require(treqs)
    val rpShouldMatch = rprofBuilder.build

    val equivProf = rpmanager.getEquivalentProfile(rpShouldMatch)
    assert(equivProf.nonEmpty)
    assert(equivProf.get.id == rpAlreadyExist.get.id, s"resourceProfile should have existed")
  }
}
