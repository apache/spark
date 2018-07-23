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
package org.apache.spark.deploy.k8s.features

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesExecutorSpecificConf, SparkPod}

class HostAliasesFeatureStepSuite extends SparkFunSuite {

  private val IP = "192.168.0.1"
  private val HOSTNAME_A = "login"
  private val HOSTNAME_B = "gateway"

  test("mounts all given host aliases") {
    val baseDriverPod = SparkPod.initialPod()
    val hostAliases = Map(
      IP -> Seq(HOSTNAME_A, HOSTNAME_B))
    val sparkConf = new SparkConf(false)
    val kubernetesConf = KubernetesConf(
      sparkConf,
      KubernetesExecutorSpecificConf("1", new PodBuilder().build()),
      "resource-name-prefix",
      "app-id",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      hostAliases,
      Seq.empty[String])

    val step = new HostAliasesFeatureStep(kubernetesConf)
    val driverPodWithHostAliases = step.configurePod(baseDriverPod).pod

    assert(podHasHostAliases(driverPodWithHostAliases, IP, Seq(HOSTNAME_A, HOSTNAME_B)))
  }
  
  def podHasHostAliases(pod: Pod, ip: String, hostnames: Seq[String]): Boolean = {
    pod.getSpec.getHostAliases.asScala.exists { alias =>
      alias.getIp == ip && hostnames.forall(hostname => alias.getHostnames().contains(hostname))
    }
  }
}
