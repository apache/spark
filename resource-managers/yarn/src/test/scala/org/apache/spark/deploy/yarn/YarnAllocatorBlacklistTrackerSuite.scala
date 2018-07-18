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
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.config.YARN_EXECUTOR_LAUNCH_BLACKLIST_ENABLED
import org.apache.spark.internal.config.{BLACKLIST_TIMEOUT_CONF, MAX_FAILED_EXEC_PER_NODE}
import org.apache.spark.util.ManualClock

class YarnAllocatorBlacklistTrackerSuite extends SparkFunSuite with Matchers
  with BeforeAndAfterEach {

  val BLACKLIST_TIMEOUT = 100L
  val MAX_FAILED_EXEC_PER_NODE_VALUE = 2

  var amClientMock: AMRMClient[ContainerRequest] = _
  var yarnBlacklistTracker: YarnAllocatorBlacklistTracker = _
  var failureTracker: FailureTracker = _
  var clock: ManualClock = _

  override def beforeEach(): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set(BLACKLIST_TIMEOUT_CONF, BLACKLIST_TIMEOUT)
    sparkConf.set(YARN_EXECUTOR_LAUNCH_BLACKLIST_ENABLED, true)
    sparkConf.set(MAX_FAILED_EXEC_PER_NODE, MAX_FAILED_EXEC_PER_NODE_VALUE)
    clock = new ManualClock()

    amClientMock = mock(classOf[AMRMClient[ContainerRequest]])
    failureTracker = new FailureTracker(sparkConf, clock)
    yarnBlacklistTracker =
      new YarnAllocatorBlacklistTracker(sparkConf, amClientMock, failureTracker)
    yarnBlacklistTracker.setNumClusterNodes(4)
    super.beforeEach()
  }

  test("expiring its own blacklisted nodes") {
    (1 to MAX_FAILED_EXEC_PER_NODE_VALUE).foreach {
      _ => {
        yarnBlacklistTracker.handleResourceAllocationFailure(Some("host"))
        // host should not be blacklisted at these failures as MAX_FAILED_EXEC_PER_NODE is 2
        verify(amClientMock, never())
          .updateBlacklist(Arrays.asList("host"), Collections.emptyList())
      }
    }

    yarnBlacklistTracker.handleResourceAllocationFailure(Some("host"))
    // the third failure on the host triggers the blacklisting
    verify(amClientMock).updateBlacklist(Arrays.asList("host"), Collections.emptyList())

    clock.advance(BLACKLIST_TIMEOUT)

    // trigger synchronisation of blacklisted nodes with YARN
    yarnBlacklistTracker.setSchedulerBlacklistedNodes(Set())
    verify(amClientMock).updateBlacklist(Collections.emptyList(), Arrays.asList("host"))
  }

  test("not handling the expiry of scheduler blacklisted nodes") {
    yarnBlacklistTracker.setSchedulerBlacklistedNodes(Set("host1", "host2"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host1", "host2"), Collections.emptyList())

    // advance timer more then host1, host2 expiry time
    clock.advance(200L)

    // expired blacklisted nodes (simulating a resource request)
    yarnBlacklistTracker.setSchedulerBlacklistedNodes(Set("host1", "host2"))
    // no change is communicated to YARN regarding the blacklisting
    verify(amClientMock).updateBlacklist(Collections.emptyList(), Collections.emptyList())
  }

  test("combining scheduler and allocation blacklist") {
    (1 to MAX_FAILED_EXEC_PER_NODE_VALUE).foreach {
      _ => {
        yarnBlacklistTracker.handleResourceAllocationFailure(Some("host1"))
        // host1 should not be blacklisted at these failures as MAX_FAILED_EXEC_PER_NODE is 2
        verify(amClientMock, never())
          .updateBlacklist(Arrays.asList("host1"), Collections.emptyList())
      }
    }

    // as this is the third failure on host1 the node will be blacklisted
    yarnBlacklistTracker.handleResourceAllocationFailure(Some("host1"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host1"), Collections.emptyList())

    yarnBlacklistTracker.setSchedulerBlacklistedNodes(Set("host2", "host3"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host2", "host3"), Collections.emptyList())

    clock.advance(10L)

    yarnBlacklistTracker.setSchedulerBlacklistedNodes(Set("host3", "host4"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host4"), Arrays.asList("host2"))
  }

  test("blacklist all available nodes") {
    yarnBlacklistTracker.setSchedulerBlacklistedNodes(Set("host1", "host2", "host3"))
    verify(amClientMock)
      .updateBlacklist(Arrays.asList("host1", "host2", "host3"), Collections.emptyList())

    clock.advance(60L)
    (1 to MAX_FAILED_EXEC_PER_NODE_VALUE).foreach {
      _ => {
        yarnBlacklistTracker.handleResourceAllocationFailure(Some("host4"))
        // host4 should not be blacklisted at these failures as MAX_FAILED_EXEC_PER_NODE is 2
        verify(amClientMock, never())
          .updateBlacklist(Arrays.asList("host4"), Collections.emptyList())
      }
    }

    // the third failure on the host triggers the blacklisting
    yarnBlacklistTracker.handleResourceAllocationFailure(Some("host4"))

    verify(amClientMock).updateBlacklist(Arrays.asList("host4"), Collections.emptyList())
    assert(yarnBlacklistTracker.isAllNodeBlacklisted === true)
  }
}
