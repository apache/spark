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
package org.apache.spark.deploy.k8s.integrationtest.backend.cloud

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}

import org.apache.spark.deploy.k8s.integrationtest.Utils
import org.apache.spark.deploy.k8s.integrationtest.backend.IntegrationTestBackend

private[spark] object CloudTestBackend extends IntegrationTestBackend {

  private var defaultClient: DefaultKubernetesClient = _

  override def initialize(): Unit = {
    val masterUrl = Option(System.getProperty("spark.kubernetes.test.master"))
      .getOrElse(throw new RuntimeException("Kubernetes master URL is not set"))
    val k8sConf = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(Utils.checkAndGetK8sMasterUrl(masterUrl).replaceFirst("k8s://", ""))
      .build()
    defaultClient = new DefaultKubernetesClient(k8sConf)
  }

  override def getKubernetesClient: DefaultKubernetesClient = {
    defaultClient
  }
}
