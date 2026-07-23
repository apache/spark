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

  test("SPARK-58192: malformed profiles are rejected before entering the registry") {
    val conf = new SparkConf().set(EXECUTOR_CORES, 4)
    val rpmanager = new ResourceProfileManager(conf, listenerBus)

    // A raw-constructed profile with a NaN cpus amount: the TaskResourceRequest constructor
    // stays lenient (deserialization of persisted data), so registration is the enforcement
    // point and must reject it before it becomes visible to the application.
    val nanProfile = new ResourceProfile(
      Map.empty,
      Map(ResourceProfile.CPUS -> new TaskResourceRequest(ResourceProfile.CPUS, Double.NaN)))
    val e1 = intercept[IllegalArgumentException] {
      rpmanager.addResourceProfile(nanProfile)
    }
    assert(e1.getMessage.contains("must be at least 1e-9"))
    intercept[SparkException] {
      rpmanager.resourceProfileFromId(nanProfile.id)
    }

    // The cpus amount is validated under the map key -- the identity scheduling uses -- so a
    // sub-scale amount smuggled under the cpus key with a different embedded resource name is
    // rejected the same way.
    val mismatchCpus = new TaskResourceProfile(
      Map(ResourceProfile.CPUS -> new TaskResourceRequest("gpu", 1e-10)))
    val e2 = intercept[IllegalArgumentException] {
      rpmanager.addResourceProfile(mismatchCpus)
    }
    assert(e2.getMessage.contains("must be at least 1e-9"))
    intercept[SparkException] {
      rpmanager.resourceProfileFromId(mismatchCpus.id)
    }

    // An amount above any possible executor's core count could never be scheduled.
    val oversized = new TaskResourceProfile(
      Map(ResourceProfile.CPUS ->
        new TaskResourceRequest(ResourceProfile.CPUS, 2147483647.5)))
    val e3 = intercept[IllegalArgumentException] {
      rpmanager.addResourceProfile(oversized)
    }
    assert(e3.getMessage.contains("at most"))
    intercept[SparkException] {
      rpmanager.resourceProfileFromId(oversized.id)
    }

    // The inverse mismatch -- a cpus-named request under a custom key -- fails the forced
    // limiting-resource computation (no matching executor resource) before insertion.
    val mismatchCustom = new TaskResourceProfile(
      Map("gpu" -> new TaskResourceRequest(ResourceProfile.CPUS, 1.5)))
    intercept[SparkException] {
      rpmanager.addResourceProfile(mismatchCustom)
    }
    intercept[SparkException] {
      rpmanager.resourceProfileFromId(mismatchCustom.id)
    }

    // A valid profile built through the public builder still registers.
    val valid = new ResourceProfileBuilder()
      .require(new ExecutorResourceRequests().cores(4))
      .require(new TaskResourceRequests().cpus(0.5))
      .build()
    rpmanager.addResourceProfile(valid)
    assert(rpmanager.resourceProfileFromId(valid.id) === valid)
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
    val immrprof = rprof.require(gpuExecReq).build()
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
    val immrprof = rprof.require(gpuExecReq).build()
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
    val immrprof = rprof.require(gpuExecReq).build()
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

  test("isSupported task resource profiles with dynamic allocation disabled") {
    val conf = new SparkConf().setMaster("spark://foo").set(EXECUTOR_CORES, 4)
    conf.set(DYN_ALLOCATION_ENABLED, false)
    conf.set(RESOURCE_PROFILE_MANAGER_TESTING.key, "true")

    var rpmanager = new ResourceProfileManager(conf, listenerBus)
    // default profile should always work
    val defaultProf = rpmanager.defaultResourceProfile
    assert(rpmanager.isSupported(defaultProf))

    // Standalone: supports task resource profile.
    val gpuTaskReq = new TaskResourceRequests().resource("gpu", 1)
    val taskProf = new TaskResourceProfile(gpuTaskReq.requests)
    assert(rpmanager.isSupported(taskProf))

    // Local: doesn't support task resource profile.
    conf.setMaster("local")
    rpmanager = new ResourceProfileManager(conf, listenerBus)
    val error = intercept[SparkException] {
      rpmanager.isSupported(taskProf)
    }.getMessage
    assert(error === "TaskResourceProfiles are only supported for Standalone, " +
      "Yarn and Kubernetes cluster for now when dynamic allocation is disabled.")

    // Local cluster: supports task resource profile.
    conf.setMaster("local-cluster[1, 1, 1024]")
    rpmanager = new ResourceProfileManager(conf, listenerBus)
    assert(rpmanager.isSupported(taskProf))

    // Yarn: supports task resource profile.
    conf.setMaster("yarn")
    rpmanager = new ResourceProfileManager(conf, listenerBus)
    assert(rpmanager.isSupported(taskProf))

    // K8s: supports task resource profile.
    conf.setMaster("k8s://foo")
    rpmanager = new ResourceProfileManager(conf, listenerBus)
    assert(rpmanager.isSupported(taskProf))
  }

  test("isSupported task resource profiles with dynamic allocation enabled") {
    val conf = new SparkConf().setMaster("spark://foo").set(EXECUTOR_CORES, 4)
    conf.set(DYN_ALLOCATION_ENABLED, true)
    conf.set(RESOURCE_PROFILE_MANAGER_TESTING.key, "true")

    val rpmanager = new ResourceProfileManager(conf, listenerBus)

    // task resource profile.
    val gpuTaskReq = new TaskResourceRequests().resource("gpu", 1)
    val taskProf = new TaskResourceProfile(gpuTaskReq.requests)
    assert(rpmanager.isSupported(taskProf))
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
    val immrprof = rprof.require(gpuExecReq).build()
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
      val rprof = rprofBuilder.build()
      rpmanager.addResourceProfile(rprof)
      if (i == checkId) rpAlreadyExist = Some(rprof)
    }
    val rpNotMatch = new ResourceProfileBuilder().build()
    assert(rpmanager.getEquivalentProfile(rpNotMatch).isEmpty,
      s"resourceProfile should not have existed")

    val rprofBuilder = new ResourceProfileBuilder()
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(checkId).memory("4g").memoryOverhead("2000m")
    val treqs = new TaskResourceRequests()
    treqs.cpus(checkId)
    rprofBuilder.require(ereqs).require(treqs)
    val rpShouldMatch = rprofBuilder.build()

    val equivProf = rpmanager.getEquivalentProfile(rpShouldMatch)
    assert(equivProf.nonEmpty)
    assert(equivProf.get.id == rpAlreadyExist.get.id, s"resourceProfile should have existed")
  }
}
