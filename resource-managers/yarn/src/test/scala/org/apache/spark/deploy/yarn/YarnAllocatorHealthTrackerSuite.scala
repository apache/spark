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

import java.util.Arrays
import java.util.Collections

import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.ExecutorFailureTracker
import org.apache.spark.deploy.yarn.config.{YARN_EXCLUDE_NODES, YARN_EXECUTOR_LAUNCH_EXCLUDE_ON_FAILURE_ENABLED}
import org.apache.spark.internal.config.{EXCLUDE_ON_FAILURE_TIMEOUT_CONF, MAX_FAILED_EXEC_PER_NODE}
import org.apache.spark.util.ManualClock

class YarnAllocatorHealthTrackerSuite extends SparkFunSuite with Matchers
  with BeforeAndAfterEach {

  val EXCLUDE_TIMEOUT = 100L
  val MAX_FAILED_EXEC_PER_NODE_VALUE = 2

  var sparkConf: SparkConf = _
  var amClientMock: AMRMClient[ContainerRequest] = _
  var clock: ManualClock = _

  override def beforeEach(): Unit = {
    sparkConf = new SparkConf()
    sparkConf.set(EXCLUDE_ON_FAILURE_TIMEOUT_CONF, EXCLUDE_TIMEOUT)
    sparkConf.set(YARN_EXECUTOR_LAUNCH_EXCLUDE_ON_FAILURE_ENABLED, true)
    sparkConf.set(MAX_FAILED_EXEC_PER_NODE, MAX_FAILED_EXEC_PER_NODE_VALUE)
    clock = new ManualClock()
    amClientMock = mock(classOf[AMRMClient[ContainerRequest]])
    super.beforeEach()
  }

  private def createYarnAllocatorHealthTracker(
      sparkConf: SparkConf = sparkConf): YarnAllocatorNodeHealthTracker = {
    val failureTracker = new ExecutorFailureTracker(sparkConf, clock)
    val yarnHealthTracker =
      new YarnAllocatorNodeHealthTracker(sparkConf, amClientMock, failureTracker)
    yarnHealthTracker.setNumClusterNodes(4)
    yarnHealthTracker
  }

  test("expiring its own excluded nodes") {
    val yarnHealthTracker = createYarnAllocatorHealthTracker()
    (1 to MAX_FAILED_EXEC_PER_NODE_VALUE).foreach {
      _ => {
        yarnHealthTracker.handleResourceAllocationFailure(Some("host"))
        // host should not be excluded at these failures as MAX_FAILED_EXEC_PER_NODE is 2
        verify(amClientMock, never())
          .updateBlacklist(Arrays.asList("host"), Collections.emptyList())
      }
    }

    yarnHealthTracker.handleResourceAllocationFailure(Some("host"))
    // the third failure on the host triggers the exclusion
    verify(amClientMock).updateBlacklist(Arrays.asList("host"), Collections.emptyList())

    clock.advance(EXCLUDE_TIMEOUT)

    // trigger synchronisation of excluded nodes with YARN
    yarnHealthTracker.setSchedulerExcludedNodes(Set())
    verify(amClientMock).updateBlacklist(Collections.emptyList(), Arrays.asList("host"))
  }

  test("not handling the expiry of scheduler excluded nodes") {
    val yarnHealthTracker = createYarnAllocatorHealthTracker()

    yarnHealthTracker.setSchedulerExcludedNodes(Set("host1", "host2"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host1", "host2"), Collections.emptyList())

    // advance timer more then host1, host2 expiry time
    clock.advance(200L)

    // expired excluded nodes (simulating a resource request)
    yarnHealthTracker.setSchedulerExcludedNodes(Set("host1", "host2"))
    // no change is communicated to YARN regarding the exclusion
    verify(amClientMock, times(0)).updateBlacklist(Collections.emptyList(), Collections.emptyList())
  }

  test("SPARK-41585 YARN Exclude Nodes should work independently of dynamic allocation") {
    sparkConf.set(YARN_EXCLUDE_NODES, Seq("host1", "host2"))
    val yarnHealthTracker = createYarnAllocatorHealthTracker(sparkConf)

    // Check that host1 and host2 are in the exclude list
    // Note, this covers also non-dynamic allocation
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host1", "host2"), Collections.emptyList())
  }

  test("combining scheduler and allocation excluded node list") {
    sparkConf.set(YARN_EXCLUDE_NODES, Seq("initial1", "initial2"))
    val yarnHealthTracker = createYarnAllocatorHealthTracker(sparkConf)
    yarnHealthTracker.setSchedulerExcludedNodes(Set())

    // initial1 and initial2 is added as excluded nodes at the very first updateBlacklist call
    // and they are never removed
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("initial1", "initial2"), Collections.emptyList())

    (1 to MAX_FAILED_EXEC_PER_NODE_VALUE).foreach {
      _ => {
        yarnHealthTracker.handleResourceAllocationFailure(Some("host1"))
        // host1 should not be excluded at these failures as MAX_FAILED_EXEC_PER_NODE is 2
        verify(amClientMock, never())
          .updateBlacklist(Arrays.asList("host1"), Collections.emptyList())
      }
    }

    // as this is the third failure on host1 the node will be excluded
    yarnHealthTracker.handleResourceAllocationFailure(Some("host1"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host1"), Collections.emptyList())

    yarnHealthTracker.setSchedulerExcludedNodes(Set("host2", "host3"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host2", "host3"), Collections.emptyList())

    clock.advance(10L)

    yarnHealthTracker.setSchedulerExcludedNodes(Set("host3", "host4"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host4"), Arrays.asList("host2"))
  }

  test("exclude all available nodes") {
    val yarnHealthTracker = createYarnAllocatorHealthTracker()
    yarnHealthTracker.setSchedulerExcludedNodes(Set("host1", "host2", "host3"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host1", "host2", "host3"), Collections.emptyList())

    clock.advance(60L)
    (1 to MAX_FAILED_EXEC_PER_NODE_VALUE).foreach {
      _ => {
        yarnHealthTracker.handleResourceAllocationFailure(Some("host4"))
        // host4 should not be excluded at these failures as MAX_FAILED_EXEC_PER_NODE is 2
        verify(amClientMock, never())
          .updateBlacklist(Arrays.asList("host4"), Collections.emptyList())
      }
    }

    // the third failure on the host triggers the exclusion
    yarnHealthTracker.handleResourceAllocationFailure(Some("host4"))

    verify(amClientMock).updateBlacklist(Arrays.asList("host4"), Collections.emptyList())
    assert(yarnHealthTracker.isAllNodeExcluded)
  }

}
