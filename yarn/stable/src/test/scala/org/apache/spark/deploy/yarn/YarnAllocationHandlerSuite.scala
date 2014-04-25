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

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.ShouldMatchers

import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.hadoop.net.DNSToSwitchMapping

import java.util.{List => JList}
import java.util.Arrays
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.yarn.api.records._

class MyResolver extends DNSToSwitchMapping {

  override def resolve(names: JList[String]): JList[String] = {
    if (names.size > 0 && names.get(0) == "host3") Arrays.asList("/rack2")
    else Arrays.asList("/rack1")
  }

  override def reloadCachedMappings() {}

  def reloadCachedMappings(names: JList[String]) {}
}

class YarnAllocationHandlerSuite extends FunSuite with ShouldMatchers with BeforeAndAfterEach {
  val conf = new Configuration()
  conf.setClass(
    CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
    classOf[MyResolver], classOf[DNSToSwitchMapping])

  val sparkConf = new SparkConf()
  sparkConf.set("spark.driver.host", "localhost")
  sparkConf.set("spark.driver.port", "4040")

  var rmClient: AMRMClient[ContainerRequest] = _

  var containerNum = 0

  val CONTAINER_RESOURCE = Resource.newInstance(2048 + YarnAllocationHandler.MEMORY_OVERHEAD, 6)

  override def beforeEach() {
    rmClient = AMRMClient.createAMRMClient()
    rmClient.init(conf)
    rmClient.start()

    // rmClient.registerApplicationMaster("Host", 10000, "")
  }

  override def afterEach() {
    rmClient.stop()
  }

  def createHandler(preferredHosts: Seq[String] = new Array[String](0),
      maxExecutors: Int = 5): YarnAllocationHandler = {
    val preferredHostToCount = preferredHosts.groupBy(identity).map(x => (x._1, x._2.size))
    new YarnAllocationHandler(conf,
      rmClient,
      null,
      maxExecutors,
      2048,
      5,
      preferredHostToCount,
      null,
      sparkConf)
  }

  def createContainer(host: String): Container = {
    val appId = ApplicationId.newInstance(0, 0)
    val appAttemptId = ApplicationAttemptId.newInstance(appId, 0)
    val containerId = ContainerId.newInstance(appAttemptId, containerNum)
    containerNum += 1
    val nodeId = NodeId.newInstance(host, 1000)
    // YARN can give larger containers than requested, so give 6 cores instead of the 5 requested
    Container.newInstance(containerId, nodeId, "",
      CONTAINER_RESOURCE,
      Priority.newInstance(YarnAllocationHandler.PRIORITY), null)
  }

  test("single container allocated") {
    // request a single container and receive it
    val handler = createHandler()
    handler.addResourceRequests(1)
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)
    handler.allocatedRackCount.get("/rack1").get should be (1)
    rmClient.getMatchingRequests(container.getPriority, "host1", CONTAINER_RESOURCE).size should be (0)
  }

  test("some containers allocated") {
    // request a few containers and receive some of them
    val handler = createHandler()
    handler.addResourceRequests(4)
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host1")
    val container3 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (3)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container3.getId).get should be ("host2")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container1.getId)
    handler.allocatedHostToContainersMap.get("host1").get should contain (container2.getId)
    handler.allocatedHostToContainersMap.get("host2").get should contain (container3.getId)
    handler.allocatedRackCount.get("/rack1").get should be (3)
  }

  test("some containers with locality preferences") {
    val handler = createHandler(Array("host1", "host1", "host2", "host3"))
    handler.addResourceRequests(4)
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host3")
    val container3 = createContainer("host4")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (3)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host3")
    handler.allocatedContainerToHostMap.get(container3.getId).get should be ("host4")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container1.getId)
    handler.allocatedHostToContainersMap.get("host3").get should contain (container2.getId)
    handler.allocatedHostToContainersMap.get("host4").get should contain (container3.getId)
    handler.allocatedRackCount.get("/rack1").get should be (2)
    handler.allocatedRackCount.get("/rack2").get should be (1)
  }

  test("receive more containers than requested") {
    val handler = createHandler(Array("host1", "host2"), 2)
    handler.addResourceRequests(2)
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    val container3 = createContainer("host4")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (2)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host2")
    handler.allocatedContainerToHostMap.contains(container3.getId) should be (false)
    handler.allocatedHostToContainersMap.get("host1").get should contain (container1.getId)
    handler.allocatedHostToContainersMap.get("host2").get should contain (container2.getId)
    handler.allocatedHostToContainersMap.contains("host4") should be (false)
    handler.allocatedRackCount.get("/rack1").get should be (2)
  }
}
