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
package org.apache.spark.deploy.k8s.integrationtest.backend.minikube

import io.fabric8.kubernetes.client.DefaultKubernetesClient

import org.apache.spark.deploy.k8s.integrationtest.backend.IntegrationTestBackend

private[spark] object MinikubeTestBackend extends IntegrationTestBackend {

  private var defaultClient: DefaultKubernetesClient = _

  override def initialize(): Unit = {
    Minikube.logVersion()
    val minikubeStatus = Minikube.getMinikubeStatus
    require(minikubeStatus == MinikubeStatus.RUNNING,
        s"Minikube must be running to use the Minikube backend for integration tests." +
          s" Current status is: $minikubeStatus.")
    defaultClient = Minikube.getKubernetesClient
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
  }

  override def getKubernetesClient: DefaultKubernetesClient = {
    defaultClient
  }

  override def describePods(labels: String): Seq[String] =
    Minikube.executeMinikube(false, "kubectl", "--", "describe", "pods", "--all-namespaces",
      "-l", labels)
}
