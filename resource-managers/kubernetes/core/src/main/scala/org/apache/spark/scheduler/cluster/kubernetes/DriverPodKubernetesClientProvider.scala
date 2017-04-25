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

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{Config, ConfigBuilder, DefaultKubernetesClient}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

private[spark] class DriverPodKubernetesClientProvider(sparkConf: SparkConf, namespace: String) {
  private val SERVICE_ACCOUNT_TOKEN = new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)
  private val SERVICE_ACCOUNT_CA_CERT = new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)
  private val oauthTokenFile = sparkConf.get(KUBERNETES_DRIVER_MOUNTED_OAUTH_TOKEN)
  private val caCertFile = sparkConf.get(KUBERNETES_DRIVER_MOUNTED_CA_CERT_FILE)
  private val clientKeyFile = sparkConf.get(KUBERNETES_DRIVER_MOUNTED_CLIENT_KEY_FILE)
  private val clientCertFile = sparkConf.get(KUBERNETES_DRIVER_MOUNTED_CLIENT_CERT_FILE)

  /**
   * Creates a {@link KubernetesClient}, expecting to be from within the context of a pod. When
   * doing so, service account token files can be picked up from canonical locations.
   */
  def get: DefaultKubernetesClient = {
    val baseClientConfigBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(KUBERNETES_MASTER_INTERNAL_URL)
      .withNamespace(namespace)

    val configBuilder = oauthTokenFile
        .orElse(caCertFile)
        .orElse(clientKeyFile)
        .orElse(clientCertFile)
        .map { _ =>
      var mountedAuthConfigBuilder = baseClientConfigBuilder
      oauthTokenFile.foreach { tokenFilePath =>
        val tokenFile = new File(tokenFilePath)
        mountedAuthConfigBuilder = mountedAuthConfigBuilder
          .withOauthToken(Files.toString(tokenFile, Charsets.UTF_8))
      }
      caCertFile.foreach { caFile =>
        mountedAuthConfigBuilder = mountedAuthConfigBuilder.withCaCertFile(caFile)
      }
      clientKeyFile.foreach { keyFile =>
        mountedAuthConfigBuilder = mountedAuthConfigBuilder.withClientKeyFile(keyFile)
      }
      clientCertFile.foreach { certFile =>
        mountedAuthConfigBuilder = mountedAuthConfigBuilder.withClientCertFile(certFile)
      }
      mountedAuthConfigBuilder
    }.getOrElse {
      var serviceAccountConfigBuilder = baseClientConfigBuilder
      if (SERVICE_ACCOUNT_CA_CERT.isFile) {
        serviceAccountConfigBuilder = serviceAccountConfigBuilder.withCaCertFile(
          SERVICE_ACCOUNT_CA_CERT.getAbsolutePath)
      }

      if (SERVICE_ACCOUNT_TOKEN.isFile) {
        serviceAccountConfigBuilder = serviceAccountConfigBuilder.withOauthToken(
          Files.toString(SERVICE_ACCOUNT_TOKEN, Charsets.UTF_8))
      }
      serviceAccountConfigBuilder
    }
    new DefaultKubernetesClient(configBuilder.build)
  }
}
