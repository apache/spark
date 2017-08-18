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

import io.fabric8.kubernetes.api.model.{Pod, PodSpec, PodStatus}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.scheduler.FakeTask

class KubernetesTaskSchedulerImplSuite extends SparkFunSuite with BeforeAndAfter {

  SparkContext.clearActiveContext()
  val sc = new SparkContext("local", "test")
  val backend = mock(classOf[KubernetesClusterSchedulerBackend])

  before {
    sc.conf.remove(KUBERNETES_DRIVER_CLUSTER_NODENAME_DNS_LOOKUP_ENABLED)
  }

  test("Create a k8s task set manager") {
    val sched = new KubernetesTaskSchedulerImpl(sc)
    sched.kubernetesSchedulerBackend = backend
    val taskSet = FakeTask.createTaskSet(0)

    val manager = sched.createTaskSetManager(taskSet, maxTaskFailures = 3)
    assert(manager.isInstanceOf[KubernetesTaskSetManager])
  }

  test("Gets racks for datanodes") {
    val rackResolverUtil = mock(classOf[RackResolverUtil])
    when(rackResolverUtil.isConfigured).thenReturn(true)
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node1"))
      .thenReturn(Option("/rack1"))
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node2"))
      .thenReturn(Option("/rack2"))
    val sched = new KubernetesTaskSchedulerImpl(sc, rackResolverUtil)
    sched.kubernetesSchedulerBackend = backend
    when(backend.getExecutorPodByIP("kube-node1")).thenReturn(None)
    when(backend.getExecutorPodByIP("kube-node2")).thenReturn(None)

    assert(sched.getRackForHost("kube-node1:60010") == Option("/rack1"))
    assert(sched.getRackForHost("kube-node2:60010") == Option("/rack2"))
  }

  test("Gets racks for executor pods") {
    sc.conf.set(KUBERNETES_DRIVER_CLUSTER_NODENAME_DNS_LOOKUP_ENABLED, true)
    val rackResolverUtil = mock(classOf[RackResolverUtil])
    when(rackResolverUtil.isConfigured).thenReturn(true)
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node1"))
      .thenReturn(Option("/rack1"))
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node2.mydomain.com"))
      .thenReturn(Option("/rack2"))
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node2"))
      .thenReturn(None)
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "192.168.1.5"))
      .thenReturn(None)
    val inetAddressUtil = mock(classOf[InetAddressUtil])
    val sched = new KubernetesTaskSchedulerImpl(sc, rackResolverUtil, inetAddressUtil)
    sched.kubernetesSchedulerBackend = backend

    val spec1 = mock(classOf[PodSpec])
    when(spec1.getNodeName).thenReturn("kube-node1")
    val status1 = mock(classOf[PodStatus])
    when(status1.getHostIP).thenReturn("192.168.1.4")
    val pod1 = mock(classOf[Pod])
    when(pod1.getSpec).thenReturn(spec1)
    when(pod1.getStatus).thenReturn(status1)
    when(backend.getExecutorPodByIP("10.0.0.1")).thenReturn(Some(pod1))

    val spec2 = mock(classOf[PodSpec])
    when(spec2.getNodeName).thenReturn("kube-node2")
    val status2 = mock(classOf[PodStatus])
    when(status2.getHostIP).thenReturn("192.168.1.5")
    val pod2 = mock(classOf[Pod])
    when(pod2.getSpec).thenReturn(spec2)
    when(pod2.getStatus).thenReturn(status2)
    when(inetAddressUtil.getFullHostName("192.168.1.5")).thenReturn("kube-node2.mydomain.com")
    when(backend.getExecutorPodByIP("10.0.1.1")).thenReturn(Some(pod2))

    assert(sched.getRackForHost("10.0.0.1:7079") == Option("/rack1"))
    assert(sched.getRackForHost("10.0.1.1:7079") == Option("/rack2"))

    verify(inetAddressUtil, times(1)).getFullHostName(anyString())
  }

  test("Gets racks for executor pods while disabling DNS lookup ") {
    sc.conf.set(KUBERNETES_DRIVER_CLUSTER_NODENAME_DNS_LOOKUP_ENABLED, false)
    val rackResolverUtil = mock(classOf[RackResolverUtil])
    when(rackResolverUtil.isConfigured).thenReturn(true)
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node1"))
      .thenReturn(Option("/rack1"))
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node2.mydomain.com"))
      .thenReturn(Option("/rack2"))
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "kube-node2"))
      .thenReturn(None)
    when(rackResolverUtil.resolveRack(sc.hadoopConfiguration, "192.168.1.5"))
      .thenReturn(None)
    val inetAddressUtil = mock(classOf[InetAddressUtil])
    val sched = new KubernetesTaskSchedulerImpl(sc, rackResolverUtil, inetAddressUtil)
    sched.kubernetesSchedulerBackend = backend

    val spec1 = mock(classOf[PodSpec])
    when(spec1.getNodeName).thenReturn("kube-node1")
    val status1 = mock(classOf[PodStatus])
    when(status1.getHostIP).thenReturn("192.168.1.4")
    val pod1 = mock(classOf[Pod])
    when(pod1.getSpec).thenReturn(spec1)
    when(pod1.getStatus).thenReturn(status1)
    when(backend.getExecutorPodByIP("10.0.0.1")).thenReturn(Some(pod1))

    val spec2 = mock(classOf[PodSpec])
    when(spec2.getNodeName).thenReturn("kube-node2")
    val status2 = mock(classOf[PodStatus])
    when(status2.getHostIP).thenReturn("192.168.1.5")
    val pod2 = mock(classOf[Pod])
    when(pod2.getSpec).thenReturn(spec2)
    when(pod2.getStatus).thenReturn(status2)
    when(inetAddressUtil.getFullHostName("192.168.1.5")).thenReturn("kube-node2.mydomain.com")
    when(backend.getExecutorPodByIP("10.0.1.1")).thenReturn(Some(pod2))

    assert(sched.getRackForHost("10.0.0.1:7079") == Option("/rack1"))
    assert(sched.getRackForHost("10.0.1.1:7079") == None)

    verify(inetAddressUtil, never).getFullHostName(anyString())
  }

  test("Does not get racks if plugin is not configured") {
    val rackResolverUtil = mock(classOf[RackResolverUtil])
    when(rackResolverUtil.isConfigured()).thenReturn(false)
    val sched = new KubernetesTaskSchedulerImpl(sc, rackResolverUtil)
    sched.kubernetesSchedulerBackend = backend
    when(backend.getExecutorPodByIP("kube-node1")).thenReturn(None)

    val spec1 = mock(classOf[PodSpec])
    when(spec1.getNodeName).thenReturn("kube-node1")
    val status1 = mock(classOf[PodStatus])
    when(status1.getHostIP).thenReturn("192.168.1.4")
    val pod1 = mock(classOf[Pod])
    when(pod1.getSpec).thenReturn(spec1)
    when(pod1.getStatus).thenReturn(status1)
    when(backend.getExecutorPodByIP("10.0.0.1")).thenReturn(Some(pod1))

    assert(sched.getRackForHost("kube-node1:60010").isEmpty)
    assert(sched.getRackForHost("10.0.0.1:7079").isEmpty)
  }
}
