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
package org.apache.spark.deploy.kubernetes.submit.v2

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.internal.Logging

trait SubmissionKubernetesClientProvider {
  def get: KubernetesClient
}

private[spark] class SubmissionKubernetesClientProviderImpl(sparkConf: SparkConf)
    extends SubmissionKubernetesClientProvider with Logging {

  private val namespace = sparkConf.get(KUBERNETES_NAMESPACE)
  private val master = resolveK8sMaster(sparkConf.get("spark.master"))

  override def get: KubernetesClient = {
    var k8ConfBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withNamespace(namespace)
    sparkConf.get(KUBERNETES_SUBMIT_CA_CERT_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withCaCertFile(f)
    }
    sparkConf.get(KUBERNETES_SUBMIT_CLIENT_KEY_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientKeyFile(f)
    }
    sparkConf.get(KUBERNETES_SUBMIT_CLIENT_CERT_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientCertFile(f)
    }
    sparkConf.get(KUBERNETES_SUBMIT_OAUTH_TOKEN).foreach { token =>
      k8ConfBuilder = k8ConfBuilder.withOauthToken(token)
    }
    val k8ClientConfig = k8ConfBuilder.build
    new DefaultKubernetesClient(k8ClientConfig)
  }
}
