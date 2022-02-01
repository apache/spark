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

package org.apache.spark.deploy.k8s.integrationtest.backend

import io.fabric8.kubernetes.client.DefaultKubernetesClient

import org.apache.spark.deploy.k8s.integrationtest.ProcessUtils
import org.apache.spark.deploy.k8s.integrationtest.TestConstants._
import org.apache.spark.deploy.k8s.integrationtest.backend.cloud.KubeConfigBackend
import org.apache.spark.deploy.k8s.integrationtest.backend.docker.DockerForDesktopBackend
import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.MinikubeTestBackend

private[spark] trait IntegrationTestBackend {
  def initialize(): Unit
  def getKubernetesClient: DefaultKubernetesClient
  def cleanUp(): Unit = {}
  def describePods(labels: String): Seq[String] =
    ProcessUtils.executeProcess(
      Array("bash", "-c", s"kubectl describe pods --all-namespaces -l $labels"),
      timeout = 60, dumpOutput = false).filter { !_.contains("https://github.com/kubernetes") }
}

private[spark] object IntegrationTestBackendFactory {
  def getTestBackend: IntegrationTestBackend = {
    val deployMode = Option(System.getProperty(CONFIG_KEY_DEPLOY_MODE))
      .getOrElse(BACKEND_MINIKUBE)
    deployMode match {
      case BACKEND_MINIKUBE => MinikubeTestBackend
      case BACKEND_CLOUD =>
        new KubeConfigBackend(System.getProperty(CONFIG_KEY_KUBE_CONFIG_CONTEXT))
      case BACKEND_DOCKER_FOR_DESKTOP => DockerForDesktopBackend
      case _ => throw new IllegalArgumentException("Invalid " +
        CONFIG_KEY_DEPLOY_MODE + ": " + deployMode)
    }
  }
}
