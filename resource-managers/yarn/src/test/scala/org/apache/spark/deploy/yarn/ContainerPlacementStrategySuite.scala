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

package org.apache.spark.deploy.yarn

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.resource.ResourceProfile

class ContainerPlacementStrategySuite extends SparkFunSuite with Matchers {

  private val yarnAllocatorSuite = new YarnAllocatorSuite
  import yarnAllocatorSuite._

  def createContainerRequest(nodes: Array[String]): ContainerRequest =
    new ContainerRequest(containerResource, nodes, null, Priority.newInstance(1))

  override def beforeEach(): Unit = {
    yarnAllocatorSuite.beforeEach()
  }

  override def afterEach(): Unit = {
    yarnAllocatorSuite.afterEach()
  }

  val defaultResourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID

  test("allocate locality preferred containers with enough resource and no matched existed " +
    "containers") {
    // 1. All the locations of current containers cannot satisfy the new requirements
    // 2. Current requested container number can fully satisfy the pending tasks.

    val (handler, allocatorConf) = createAllocator(2)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(createContainer("host1"), createContainer("host2")))

    ResourceProfile.clearDefaultProfile
    val rp = ResourceProfile.getOrCreateDefaultProfile(allocatorConf)
    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host3" -> 15, "host4" -> 15, "host5" -> 10),
      handler.allocatedHostToContainersMapPerRPId(defaultResourceProfileId), Seq.empty, rp)

    assert(localities.map(_.nodes) === Array(
      Array("host3", "host4", "host5"),
      Array("host3", "host4", "host5"),
      Array("host3", "host4")))
  }

  test("allocate locality preferred containers with enough resource and partially matched " +
    "containers") {
    // 1. Parts of current containers' locations can satisfy the new requirements
    // 2. Current requested container number can fully satisfy the pending tasks.

    val (handler, allocatorConf) = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    ResourceProfile.clearDefaultProfile
    val rp = ResourceProfile.getOrCreateDefaultProfile(allocatorConf)

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
      handler.allocatedHostToContainersMapPerRPId(defaultResourceProfileId), Seq.empty, rp)

    assert(localities.map(_.nodes) ===
      Array(null, Array("host2", "host3"), Array("host2", "host3")))
  }

  test("allocate locality preferred containers with limited resource and partially matched " +
    "containers") {
    // 1. Parts of current containers' locations can satisfy the new requirements
    // 2. Current requested container number cannot fully satisfy the pending tasks.

    val (handler, allocatorConf) = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    ResourceProfile.clearDefaultProfile
    val rp = ResourceProfile.getOrCreateDefaultProfile(allocatorConf)
    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
      handler.allocatedHostToContainersMapPerRPId(defaultResourceProfileId), Seq.empty, rp)

    assert(localities.map(_.nodes) === Array(Array("host2", "host3")))
  }

  test("allocate locality preferred containers with fully matched containers") {
    // Current containers' locations can fully satisfy the new requirements

    val (handler, allocatorConf) = createAllocator(5)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2"),
      createContainer("host2"),
      createContainer("host3")
    ))

    ResourceProfile.clearDefaultProfile
    val rp = ResourceProfile.getOrCreateDefaultProfile(allocatorConf)
    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
      handler.allocatedHostToContainersMapPerRPId(defaultResourceProfileId), Seq.empty, rp)

    assert(localities.map(_.nodes) === Array(null, null, null))
  }

  test("allocate containers with no locality preference") {
    // Request new container without locality preference

    val (handler, allocatorConf) = createAllocator(2)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(createContainer("host1"), createContainer("host2")))

    ResourceProfile.clearDefaultProfile
    val rp = ResourceProfile.getOrCreateDefaultProfile(allocatorConf)
    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 0, Map.empty,
      handler.allocatedHostToContainersMapPerRPId(defaultResourceProfileId), Seq.empty, rp)

    assert(localities.map(_.nodes) === Array(null))
  }

  test("allocate locality preferred containers by considering the localities of pending requests") {
    val (handler, allocatorConf) = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    val pendingAllocationRequests = Seq(
      createContainerRequest(Array("host2", "host3")),
      createContainerRequest(Array("host1", "host4")))

    ResourceProfile.clearDefaultProfile
    val rp = ResourceProfile.getOrCreateDefaultProfile(allocatorConf)
    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10),
      handler.allocatedHostToContainersMapPerRPId(defaultResourceProfileId),
      pendingAllocationRequests, rp)

    assert(localities.map(_.nodes) === Array(Array("host3")))
  }
}
