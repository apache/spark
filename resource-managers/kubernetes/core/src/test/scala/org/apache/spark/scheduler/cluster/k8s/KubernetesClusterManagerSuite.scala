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

import org.apache.spark._
import org.apache.spark.deploy.k8s.Config._

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
}
