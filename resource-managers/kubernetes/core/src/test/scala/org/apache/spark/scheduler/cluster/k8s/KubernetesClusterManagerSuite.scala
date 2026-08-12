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
package org.apache.spark.scheduler.cluster.k8s

import java.net.{InetSocketAddress, StandardProtocolFamily}
import java.nio.channels.ServerSocketChannel

import scala.util.Using

import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.util.RpcUtils

class KubernetesClusterManagerSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var sc: SparkContext = _

  @Mock
  private var env: SparkEnv = _

  @Mock
  private var sparkConf: SparkConf = _

  before {
    MockitoAnnotations.openMocks(this).close()
    when(sc.conf).thenReturn(sparkConf)
    when(sc.conf.get(KUBERNETES_DRIVER_POD_NAME)).thenReturn(None)
    when(sc.conf.get(EXECUTOR_INSTANCES)).thenReturn(None)
    when(sc.conf.get(MAX_EXECUTOR_FAILURES)).thenReturn(None)
    when(sc.conf.get(EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS)).thenReturn(None)
    when(sc.env).thenReturn(env)
  }

  test("constructing a AbstractPodsAllocator works") {
    val validConfigs = List("statefulset", "direct",
      classOf[StatefulSetPodsAllocator].getName,
      classOf[ExecutorPodsAllocator].getName)
    validConfigs.foreach { c =>
      val manager = new KubernetesClusterManager()
      when(sc.conf.get(KUBERNETES_ALLOCATION_PODS_ALLOCATOR)).thenReturn(c)
      manager.makeExecutorPodsAllocator(sc, kubernetesClient, null)
    }
  }

  test("SPARK-45948: Single-pod Spark jobs respect spark.app.id") {
    val conf = new SparkConf()
    conf.set(KUBERNETES_DRIVER_MASTER_URL, "local[2]")
    when(sc.conf).thenReturn(conf)
    val scheduler = mock[TaskSchedulerImpl]
    when(scheduler.sc).thenReturn(sc)
    val manager = new KubernetesClusterManager()

    val backend1 = manager.createSchedulerBackend(sc, "", scheduler)
    assert(backend1.isInstanceOf[LocalSchedulerBackend])
    assert(backend1.applicationId().startsWith("local-"))

    conf.set("spark.app.id", "user-app-id")
    val backend2 = manager.createSchedulerBackend(sc, "", scheduler)
    assert(backend2.isInstanceOf[LocalSchedulerBackend])
    assert(backend2.applicationId() === "user-app-id")
  }

  test("SPARK-58719: normalize IPv6 driver host when using the driver pod IP") {
    assume(
      Using(ServerSocketChannel.open(StandardProtocolFamily.INET6)) { channel =>
        channel.bind(new InetSocketAddress("::1", 0))
      }.isSuccess,
      "IPv6 loopback is unavailable")

    val rawAddress = "0:0:0:0:0:0:0:1"
    val conf = new SparkConf(false)
      .setAppName("ipv6-driver-host")
      .setMaster("k8s://test")
      .set(KUBERNETES_DRIVER_MASTER_URL, "local[2]")
      .set(KUBERNETES_EXECUTOR_USE_DRIVER_POD_IP, true)
      .set(DRIVER_BIND_ADDRESS, rawAddress)
      .set("spark.ui.enabled", "false")

    LocalSparkContext.withSpark(new SparkContext(conf)) { context =>
      assert(context.conf.get(DRIVER_BIND_ADDRESS) === rawAddress)
      assert(context.conf.get(DRIVER_HOST_ADDRESS) === "[::1]")
      assert(context.env.blockManager.blockManagerId.host === "[::1]")
      val driverRef = RpcUtils.makeDriverRef(
        HeartbeatReceiver.ENDPOINT_NAME, context.conf, context.env.rpcEnv)
      assert(driverRef.address.host === "[::1]")
    }
  }
}
