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
package org.apache.spark.scheduler.cluster.kubernetes

import scala.collection.mutable.ArrayBuffer

import io.fabric8.kubernetes.api.model.{Pod, PodSpec, PodStatus}
import org.mockito.Mockito._

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.{FakeTask, FakeTaskScheduler, HostTaskLocation, TaskLocation}

class KubernetesTaskSetManagerSuite extends SparkFunSuite {

  val sc = new SparkContext("local", "test")
  val sched = new FakeTaskScheduler(sc,
    ("execA", "10.0.0.1"), ("execB", "10.0.0.2"), ("execC", "10.0.0.3"))
  val backend = mock(classOf[KubernetesClusterSchedulerBackend])
  sched.backend = backend

  test("Find pending tasks for executors using executor pod IP addresses") {
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("10.0.0.1", "execA")),  // Task 0 runs on executor pod 10.0.0.1.
      Seq(TaskLocation("10.0.0.1", "execA")),  // Task 1 runs on executor pod 10.0.0.1.
      Seq(TaskLocation("10.0.0.2", "execB"))   // Task 2 runs on executor pod 10.0.0.2.
    )

    val manager = new KubernetesTaskSetManager(sched, taskSet, maxTaskFailures = 2)
    assert(manager.getPendingTasksForHost("10.0.0.1") == ArrayBuffer(1, 0))
    assert(manager.getPendingTasksForHost("10.0.0.2") == ArrayBuffer(2))
  }

  test("Find pending tasks for executors using cluster node names that executor pods run on") {
    val taskSet = FakeTask.createTaskSet(2,
      Seq(HostTaskLocation("kube-node1")),  // Task 0's partition belongs to datanode on kube-node1
      Seq(HostTaskLocation("kube-node1"))   // Task 1's partition belongs to datanode on kube-node2
    )
    val spec1 = mock(classOf[PodSpec])
    when(spec1.getNodeName).thenReturn("kube-node1")
    val pod1 = mock(classOf[Pod])
    when(pod1.getSpec).thenReturn(spec1)
    when(backend.getExecutorPodByIP("10.0.0.1")).thenReturn(Some(pod1))

    val manager = new KubernetesTaskSetManager(sched, taskSet, maxTaskFailures = 2)
    assert(manager.getPendingTasksForHost("10.0.0.1") == ArrayBuffer(1, 0))
  }

  test("Find pending tasks for executors using cluster node IPs that executor pods run on") {
    val taskSet = FakeTask.createTaskSet(2,
      Seq(HostTaskLocation("196.0.0.5")),  // Task 0's partition belongs to datanode on 196.0.0.5.
      Seq(HostTaskLocation("196.0.0.5"))   // Task 1's partition belongs to datanode on 196.0.0.5.
    )
    val spec1 = mock(classOf[PodSpec])
    when(spec1.getNodeName).thenReturn("kube-node1")
    val pod1 = mock(classOf[Pod])
    when(pod1.getSpec).thenReturn(spec1)
    val status1 = mock(classOf[PodStatus])
    when(status1.getHostIP).thenReturn("196.0.0.5")
    when(pod1.getStatus).thenReturn(status1)
    when(backend.getExecutorPodByIP("10.0.0.1")).thenReturn(Some(pod1))
    val manager = new KubernetesTaskSetManager(sched, taskSet, maxTaskFailures = 2)
    assert(manager.getPendingTasksForHost("10.0.0.1") == ArrayBuffer(1, 0))
  }

  test("Find pending tasks for executors using cluster node FQDNs that executor pods run on") {
    val taskSet = FakeTask.createTaskSet(2,
      Seq(HostTaskLocation("kube-node1.domain1")),  // Task 0's partition belongs to datanode here.
      Seq(HostTaskLocation("kube-node1.domain1"))   // task 1's partition belongs to datanode here.
    )
    val spec1 = mock(classOf[PodSpec])
    when(spec1.getNodeName).thenReturn("kube-node1")
    val pod1 = mock(classOf[Pod])
    when(pod1.getSpec).thenReturn(spec1)
    val status1 = mock(classOf[PodStatus])
    when(status1.getHostIP).thenReturn("196.0.0.5")
    when(pod1.getStatus).thenReturn(status1)
    val inetAddressUtil = mock(classOf[InetAddressUtil])
    when(inetAddressUtil.getFullHostName("196.0.0.5")).thenReturn("kube-node1.domain1")
    when(backend.getExecutorPodByIP("10.0.0.1")).thenReturn(Some(pod1))

    val manager = new KubernetesTaskSetManager(sched, taskSet, maxTaskFailures = 2, inetAddressUtil)
    assert(manager.getPendingTasksForHost("10.0.0.1") == ArrayBuffer(1, 0))
  }

  test("Return empty pending tasks for executors when all look up fail") {
    val taskSet = FakeTask.createTaskSet(1,
      Seq(HostTaskLocation("kube-node1.domain1"))   // task 0's partition belongs to datanode here.
    )
    val spec1 = mock(classOf[PodSpec])
    when(spec1.getNodeName).thenReturn("kube-node2")
    val pod1 = mock(classOf[Pod])
    when(pod1.getSpec).thenReturn(spec1)
    val status1 = mock(classOf[PodStatus])
    when(status1.getHostIP).thenReturn("196.0.0.6")
    when(pod1.getStatus).thenReturn(status1)
    val inetAddressUtil = mock(classOf[InetAddressUtil])
    when(inetAddressUtil.getFullHostName("196.0.0.6")).thenReturn("kube-node2.domain1")
    when(backend.getExecutorPodByIP("10.0.0.1")).thenReturn(Some(pod1))

    val manager = new KubernetesTaskSetManager(sched, taskSet, maxTaskFailures = 2, inetAddressUtil)
    assert(manager.getPendingTasksForHost("10.0.0.1") == ArrayBuffer())
  }
}
