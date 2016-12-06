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
package org.apache.spark.deploy.kubernetes

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}

private[spark] object KubernetesClientBuilder {
  private val API_SERVER_TOKEN = new File("/var/run/secrets/kubernetes.io/serviceaccount/token")
  private val CA_CERT_FILE = new File("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

  /**
    * Creates a {@link KubernetesClient}, expecting to be from
    * within the context of a pod. When doing so, credentials files
    * are picked up from canonical locations, as they are injected
    * into the pod's disk space.
    */
  def buildFromWithinPod(
      kubernetesMaster: String,
      kubernetesNamespace: String): DefaultKubernetesClient = {
    var clientConfigBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(kubernetesMaster)
      .withNamespace(kubernetesNamespace)

    if (CA_CERT_FILE.isFile) {
      clientConfigBuilder = clientConfigBuilder.withCaCertFile(CA_CERT_FILE.getAbsolutePath)
    }

    if (API_SERVER_TOKEN.isFile) {
      clientConfigBuilder = clientConfigBuilder.withOauthToken(
        Files.toString(API_SERVER_TOKEN, Charsets.UTF_8))
    }
    new DefaultKubernetesClient(clientConfigBuilder.build)
  }
}
