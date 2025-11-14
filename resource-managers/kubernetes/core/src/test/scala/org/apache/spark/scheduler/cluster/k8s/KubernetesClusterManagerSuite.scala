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

import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils.TEST_SPARK_APP_ID
import org.apache.spark.scheduler.local.LocalSchedulerBackend

class KubernetesClusterManagerSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var sc: SparkContext = _

  @Mock
  private var env: SparkEnv = _

  private var sparkConf: SparkConf = _

  before {
    MockitoAnnotations.openMocks(this).close()
    sparkConf = new SparkConf(false)
      .set("spark.app.id", TEST_SPARK_APP_ID)
      .set("spark.master", "k8s://test")
    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(env)
    when(env.securityManager).thenReturn(new SecurityManager(sparkConf))
    resetDynamicAllocatorConfig()
  }

  after {
    resetDynamicAllocatorConfig()
  }

  test("constructing a AbstractPodsAllocator works") {
    val validConfigs = List("statefulset", "deployment", "direct",
      classOf[StatefulSetPodsAllocator].getName,
      classOf[DeploymentPodsAllocator].getName,
      classOf[ExecutorPodsAllocator].getName)
    validConfigs.foreach { c =>
      val manager = new KubernetesClusterManager()
      sparkConf.set(KUBERNETES_ALLOCATION_PODS_ALLOCATOR, c)
      manager.makeExecutorPodsAllocator(sc, kubernetesClient, null)
      sparkConf.remove(KUBERNETES_ALLOCATION_PODS_ALLOCATOR)
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

  test("deployment allocator with dynamic allocation requires deletion cost") {
    val manager = new KubernetesClusterManager()
    sparkConf.set(KUBERNETES_ALLOCATION_PODS_ALLOCATOR, "deployment")
    sparkConf.set(DYN_ALLOCATION_ENABLED.key, "true")
    sparkConf.remove(KUBERNETES_EXECUTOR_POD_DELETION_COST.key)
    sparkConf.set("spark.shuffle.service.enabled", "true")

    val e = intercept[SparkException] {
      manager.makeExecutorPodsAllocator(sc, kubernetesClient, null)
    }
    assert(e.getMessage.contains(KUBERNETES_EXECUTOR_POD_DELETION_COST.key))
  }

  test("deployment allocator with dynamic allocation and deletion cost succeeds") {
    val manager = new KubernetesClusterManager()
    sparkConf.set(KUBERNETES_ALLOCATION_PODS_ALLOCATOR, "deployment")
    sparkConf.set(DYN_ALLOCATION_ENABLED.key, "true")
    sparkConf.set(KUBERNETES_EXECUTOR_POD_DELETION_COST, 1)
    sparkConf.set("spark.shuffle.service.enabled", "true")

    manager.makeExecutorPodsAllocator(sc, kubernetesClient, null)
  }

  private def resetDynamicAllocatorConfig(): Unit = {
    sparkConf.remove(KUBERNETES_ALLOCATION_PODS_ALLOCATOR)
    sparkConf.remove(DYN_ALLOCATION_ENABLED.key)
    sparkConf.remove(KUBERNETES_EXECUTOR_POD_DELETION_COST.key)
    sparkConf.remove("spark.shuffle.service.enabled")
  }
}
