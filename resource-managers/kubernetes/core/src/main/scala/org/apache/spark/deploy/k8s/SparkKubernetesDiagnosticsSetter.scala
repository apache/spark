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
package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.hadoop.util.StringUtils

import org.apache.spark.{SparkConf, SparkMasterRegex}
import org.apache.spark.deploy.SparkDiagnosticsSetter
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants.EXIT_EXCEPTION_ANNOTATION
import org.apache.spark.deploy.k8s.SparkKubernetesClientFactory.ClientType
import org.apache.spark.internal.Logging
import org.apache.spark.util.{SparkStringUtils, Utils}

/**
 * We use this trait and its implementation to allow for mocking the static
 * client creation in tests.
 */
private[spark] trait KubernetesClientProvider {
  def create(conf: SparkConf): KubernetesClient
}

private[spark] class DefaultKubernetesClientProvider extends KubernetesClientProvider {
  override def create(conf: SparkConf): KubernetesClient = {
    SparkKubernetesClientFactory.createKubernetesClient(
      conf.get(KUBERNETES_DRIVER_MASTER_URL),
      Option(conf.get(KUBERNETES_NAMESPACE)),
      KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
      ClientType.Driver,
      conf,
      None)
  }
}

private[spark] class SparkKubernetesDiagnosticsSetter(clientProvider: KubernetesClientProvider)
  extends SparkDiagnosticsSetter with Logging {

  private val KUBERNETES_EXIT_EXCEPTION_MESSAGE_LIMIT_BYTES = 64 * 1024 // 64 KiB

  def this() = {
    this(new DefaultKubernetesClientProvider)
  }

  override def setDiagnostics(throwable: Throwable, conf: SparkConf): Unit = {
    val diagnostics = SparkStringUtils.abbreviate(StringUtils.stringifyException(throwable),
      KUBERNETES_EXIT_EXCEPTION_MESSAGE_LIMIT_BYTES)
    Utils.tryWithResource(clientProvider.create(conf)) { client =>
      conf.get(KUBERNETES_DRIVER_POD_NAME).foreach { podName =>
        client.pods()
          .inNamespace(conf.get(KUBERNETES_NAMESPACE))
          .withName(podName)
          .edit((p: Pod) => new PodBuilder(p)
            .editOrNewMetadata()
            .addToAnnotations(EXIT_EXCEPTION_ANNOTATION, diagnostics)
            .endMetadata()
            .build());
      }
    }
  }

  override def supports(clusterManagerUrl: String): Boolean = {
    SparkMasterRegex.isK8s(clusterManagerUrl)
  }
}
