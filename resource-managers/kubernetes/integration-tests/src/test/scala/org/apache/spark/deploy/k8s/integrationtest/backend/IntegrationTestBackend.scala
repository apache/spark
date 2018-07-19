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

import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.MinikubeTestBackend

private[spark] trait IntegrationTestBackend {
  def initialize(): Unit
  def getKubernetesClient: DefaultKubernetesClient
  def cleanUp(): Unit = {}
}

private[spark] object IntegrationTestBackendFactory {
  val deployModeConfigKey = "spark.kubernetes.test.deployMode"

  def getTestBackend: IntegrationTestBackend = {
    val deployMode = Option(System.getProperty(deployModeConfigKey))
      .getOrElse("minikube")
    if (deployMode == "minikube") {
      MinikubeTestBackend
    } else {
      throw new IllegalArgumentException(
        "Invalid " + deployModeConfigKey + ": " + deployMode)
    }
  }
}
