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

import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class DecommissionTrackerSuite extends SparkFunSuite
  with BeforeAndAfter with BeforeAndAfterEach with MockitoSugar
  with LocalSparkContext {

  private var decommissionTracker: DecommissionTracker = _
  private var dagScheduler : DAGScheduler = _
  private var executorAllocationClient : ExecutorAllocationClient = _
  private var conf: SparkConf = _
  private var currentDecommissionNode = ""
  private var currentShuffleDecommissionNode = ""

  override def beforeAll(): Unit = {
    conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.GRACEFUL_DECOMMISSION_ENABLE.key, "true")
      .set(config.GRACEFUL_DECOMMISSION_MIN_TERMINATION_TIME_IN_SEC.key, "8s")

    executorAllocationClient = mock[ExecutorAllocationClient]
    dagScheduler = mock[DAGScheduler]
  }

  override def afterAll(): Unit = {
    if (decommissionTracker != null) {
      decommissionTracker = null
    }
  }

  before {
    val clock = new ManualClock(0)
    clock.setTime(0)
    decommissionTracker = new DecommissionTracker(conf,
      Some(executorAllocationClient), dagScheduler, clock = clock)

    when(executorAllocationClient.killExecutorsOnHost(anyString())).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        currentDecommissionNode = invocation.getArgument(0, classOf[String])
        true
      }
    })
    when(dagScheduler.nodeDecommissioned(anyString())).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        currentShuffleDecommissionNode = invocation.getArgument(0, classOf[String])
        true
      }
    })
  }

  after {
    decommissionTracker.removeNodeToDecommission("hostA")
    decommissionTracker.removeNodeToDecommission("hostB")
    super.afterEach()
  }

  test("Check Node Decommission state") {
    val clock = new ManualClock(0)
    decommissionTracker = new DecommissionTracker(conf,
      Some(executorAllocationClient), dagScheduler, clock = clock)

    when(executorAllocationClient.killExecutorsOnHost(anyString())).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        currentDecommissionNode = invocation.getArgument(0, classOf[String])
        true
      }
    })
    when(dagScheduler.nodeDecommissioned(anyString())).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        currentShuffleDecommissionNode = invocation.getArgument(0, classOf[String])
        true
      }
    })

    decommissionTracker.addNodeToDecommission("hostA", 10000, NodeLoss)
    assert(decommissionTracker.isNodeDecommissioning("hostA"))

    // Wait for executor decommission give 100ms over 50%
    Thread.sleep(5100)
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.EXECUTOR_DECOMMISSIONED))
    assert(currentDecommissionNode === "hostA")

    // Wait for shuffle data decommission
    Thread.sleep(4000)
    assert(decommissionTracker.isNodeDecommissioned("hostA"))
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED))
    assert(currentShuffleDecommissionNode === "hostA")

    // Wait for and terminate
    Thread.sleep(2000)
    decommissionTracker.updateNodeToDecommissionSetTerminate("hostA")
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.TERMINATED))

    // Recomission
    decommissionTracker.removeNodeToDecommission("hostA")
    assert(!decommissionTracker.isNodeDecommissioning("hostA"))
  }

  test("Check the Multiple Node Decommission state") {
    decommissionTracker.addNodeToDecommission("hostA", 10000, NodeLoss)
    decommissionTracker.addNodeToDecommission("hostB", 30000, NodeLoss)
    assert(decommissionTracker.isNodeDecommissioning("hostA"))
    assert(decommissionTracker.isNodeDecommissioning("hostB"))

    // Wait for hostA executor decommission
    Thread.sleep(5100)
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.EXECUTOR_DECOMMISSIONED))
    assert(currentDecommissionNode === "hostA")

    // Wait for hostA shuffle data decommission
    Thread.sleep(4000)
    assert(decommissionTracker.isNodeDecommissioned("hostA"))
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED))
    assert(currentShuffleDecommissionNode === "hostA")

    // Wait for hostA termination and trigger termination
    Thread.sleep(1000)
    decommissionTracker.updateNodeToDecommissionSetTerminate("hostA")
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.TERMINATED))

    // Recommission hostA
    decommissionTracker.removeNodeToDecommission("hostA")
    assert(!decommissionTracker.isNodeDecommissioning("hostA"))

    // Wait for hostB executor decommission
    Thread.sleep(5000)
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostB") === Some(NodeDecommissionState.EXECUTOR_DECOMMISSIONED))
    assert(currentDecommissionNode === "hostB")

    // Wait for hostB shuffledata decommission
    Thread.sleep(12000)
    assert(decommissionTracker.isNodeDecommissioned("hostB"))
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostB") === Some(NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED))
    assert(currentShuffleDecommissionNode === "hostB")

    // Wait for hostB termination and trigger termination
    Thread.sleep(3000)
    decommissionTracker.updateNodeToDecommissionSetTerminate("hostB")
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostB") === Some(NodeDecommissionState.TERMINATED))

    // Recommission hostB
    decommissionTracker.removeNodeToDecommission("hostB")
    assert(!decommissionTracker.isNodeDecommissioning("hostB"))
  }

  test("Check Multiple Node Decommission state at same time") {
    decommissionTracker.addNodeToDecommission("hostA", 10000, NodeLoss)
    decommissionTracker.addNodeToDecommission("hostB", 10000, NodeLoss)
    assert(decommissionTracker.isNodeDecommissioning("hostA"))
    assert(decommissionTracker.isNodeDecommissioning("hostB"))

    // Wait for both hostA hostB executor decommission
    Thread.sleep(5100)
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.EXECUTOR_DECOMMISSIONED))
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostB") === Some(NodeDecommissionState.EXECUTOR_DECOMMISSIONED))

    // Wait for both hostA hostB shuffle data decommission
    Thread.sleep(4000)
    assert(decommissionTracker.isNodeDecommissioned("hostA"))
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED))
    assert(decommissionTracker.isNodeDecommissioned("hostB"))
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostB") === Some(NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED))

    // Wait for both node termination and trigger termination
    Thread.sleep(2000)
    decommissionTracker.updateNodeToDecommissionSetTerminate("hostA")
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.TERMINATED))
    decommissionTracker.updateNodeToDecommissionSetTerminate("hostB")
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostB") === Some(NodeDecommissionState.TERMINATED))

    // Recommission both
    decommissionTracker.removeNodeToDecommission("hostA")
    assert(!decommissionTracker.isNodeDecommissioning("hostA"))
    decommissionTracker.removeNodeToDecommission("hostB")
    assert(!decommissionTracker.isNodeDecommissioning("hostB"))
  }

  test("Check Node decommissioning with minimum termination time config") {
    // Less than MIN_TERMINATION_TIME
    decommissionTracker.addNodeToDecommission("hostA", 5000, NodeLoss)
    // sleep for 1 sec which is lag between executor and shuffle
    // decommission.
    Thread.sleep(1100)
    assert(decommissionTracker.isNodeDecommissioned("hostA"))
  }

  test("Check Node Decommissioning with non-default executor and shuffle data lease time config") {

    val clock = new ManualClock(0)
    clock.setTime(0)
    // override and recreated
    conf = conf
      .set(config.GRACEFUL_DECOMMISSION_EXECUTOR_LEASETIME_PCT.key, "20")
      .set(config.GRACEFUL_DECOMMISSION_SHUFFLEDATA_LEASETIME_PCT.key, "50")

    decommissionTracker = new DecommissionTracker(conf,
      Some(executorAllocationClient), dagScheduler, clock = clock)

    decommissionTracker.addNodeToDecommission("hostA", 10000, NodeLoss)
    assert(decommissionTracker.isNodeDecommissioning("hostA"))

    // Wait for hostA executor decommission at 20%
    Thread.sleep(2100)
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.EXECUTOR_DECOMMISSIONED))
    assert(currentDecommissionNode === "hostA")

    // Wait for hostA shuffle data  decommission at 50%
    Thread.sleep(3000)
    assert(decommissionTracker.isNodeDecommissioned("hostA"))
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED))
    assert(currentShuffleDecommissionNode === "hostA")

    // Wait for terminate and trigger termination
    Thread.sleep(5000)
    decommissionTracker.updateNodeToDecommissionSetTerminate("hostA")
    assert(decommissionTracker.getDecommissionedNodeState(
      "hostA") === Some(NodeDecommissionState.TERMINATED))

    // Recommission
    decommissionTracker.removeNodeToDecommission("hostA")
    assert(!decommissionTracker.isNodeDecommissioning("hostA"))
  }
}
