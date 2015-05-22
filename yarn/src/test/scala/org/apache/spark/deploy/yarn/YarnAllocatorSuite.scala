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

import java.util.{Arrays, List => JList}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.net.DNSToSwitchMapping
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest

import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.deploy.yarn.YarnAllocator._
import org.apache.spark.scheduler.SplitInfo

import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class MockResolver extends DNSToSwitchMapping {

  override def resolve(names: JList[String]): JList[String] = {
    if (names.size > 0 && names.get(0) == "host3") Arrays.asList("/rack2")
    else Arrays.asList("/rack1")
  }

  override def reloadCachedMappings() {}

  def reloadCachedMappings(names: JList[String]) {}
}

class YarnAllocatorSuite extends FunSuite with Matchers with BeforeAndAfterEach {
  val conf = new Configuration()
  conf.setClass(
    CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
    classOf[MockResolver], classOf[DNSToSwitchMapping])

  val sparkConf = new SparkConf()
  sparkConf.set("spark.driver.host", "localhost")
  sparkConf.set("spark.driver.port", "4040")
  sparkConf.set("spark.yarn.jar", "notarealjar.jar")
  sparkConf.set("spark.yarn.launchContainers", "false")

  val appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0)

  // Resource returned by YARN.  YARN can give larger containers than requested, so give 6 cores
  // instead of the 5 requested and 3 GB instead of the 2 requested.
  val containerResource = Resource.newInstance(3072, 6)

  var rmClient: AMRMClient[ContainerRequest] = _

  var containerNum = 0

  override def beforeEach() {
    rmClient = AMRMClient.createAMRMClient()
    rmClient.init(conf)
    rmClient.start()
  }

  override def afterEach() {
    rmClient.stop()
  }

  class MockSplitInfo(host: String) extends SplitInfo(null, host, null, 1, null) {
    override def equals(other: Any): Boolean = false
  }

  def createAllocator(maxExecutors: Int = 5): YarnAllocator = {
    val args = Array(
      "--num-executors", s"$maxExecutors",
      "--executor-cores", "5",
      "--executor-memory", "2048",
      "--jar", "somejar.jar",
      "--class", "SomeClass")
    new YarnAllocator(
      conf,
      sparkConf,
      rmClient,
      appAttemptId,
      new ApplicationMasterArguments(args),
      new SecurityManager(sparkConf))
  }

  def createContainer(host: String): Container = {
    val containerId = ContainerId.newInstance(appAttemptId, containerNum)
    containerNum += 1
    val nodeId = NodeId.newInstance(host, 1000)
    Container.newInstance(containerId, nodeId, "", containerResource, RM_REQUEST_PRIORITY, null)
  }

  test("single container allocated") {
    // request a single container and receive it
    val handler = createAllocator(1)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    val size = rmClient.getMatchingRequests(container.getPriority, "host1", containerResource).size
    size should be (0)
  }

  test("some containers allocated") {
    // request a few containers and receive some of them
    val handler = createAllocator(4)
    handler.updateResourceRequests()
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
  }

  test("receive more containers than requested") {
    val handler = createAllocator(2)
    handler.updateResourceRequests()
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
  }

  test("decrease total requested executors") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (4)

    handler.requestTotalExecutors(3)
    handler.updateResourceRequests()
    handler.getNumPendingAllocate should be (3)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    handler.requestTotalExecutors(2)
    handler.updateResourceRequests()
    handler.getNumPendingAllocate should be (1)
  }

  test("decrease total requested executors to less than currently running") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (4)

    handler.requestTotalExecutors(3)
    handler.updateResourceRequests()
    handler.getNumPendingAllocate should be (3)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.getNumExecutorsRunning should be (2)

    handler.requestTotalExecutors(1)
    handler.updateResourceRequests()
    handler.getNumPendingAllocate should be (0)
    handler.getNumExecutorsRunning should be (2)
  }

  test("kill executors") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.requestTotalExecutors(1)
    handler.executorIdToContainer.keys.foreach { id => handler.killExecutor(id ) }

    val statuses = Seq(container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Finished", 0)
    }
    handler.updateResourceRequests()
    handler.processCompletedContainers(statuses.toSeq)
    handler.getNumExecutorsRunning should be (0)
    handler.getNumPendingAllocate should be (1)
  }

  test("memory exceeded diagnostic regexes") {
    val diagnostics =
      "Container [pid=12465,containerID=container_1412887393566_0003_01_000002] is running " +
        "beyond physical memory limits. Current usage: 2.1 MB of 2 GB physical memory used; " +
        "5.8 GB of 4.2 GB virtual memory used. Killing container."
    val vmemMsg = memLimitExceededLogMessage(diagnostics, VMEM_EXCEEDED_PATTERN)
    val pmemMsg = memLimitExceededLogMessage(diagnostics, PMEM_EXCEEDED_PATTERN)
    assert(vmemMsg.contains("5.8 GB of 4.2 GB virtual memory used."))
    assert(pmemMsg.contains("2.1 MB of 2 GB physical memory used."))
  }

}
