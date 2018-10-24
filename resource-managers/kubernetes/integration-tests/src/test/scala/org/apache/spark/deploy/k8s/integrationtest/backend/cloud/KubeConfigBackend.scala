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

import java.nio.file.Paths

import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient}
import org.apache.spark.deploy.k8s.integrationtest.TestConstants
import org.apache.spark.deploy.k8s.integrationtest.backend.IntegrationTestBackend
import org.apache.spark.internal.Logging

private[spark] class KubeConfigBackend(var context: String)
  extends IntegrationTestBackend with Logging {
  // If no context supplied see if one was specified in the system properties supplied
  // to the tests
  if (context == null) {
    context = System.getProperty(TestConstants.CONFIG_KEY_KUBE_CONFIG_CONTEXT)
  }
  logInfo(s"K8S Integration tests will run against " +
    s"${if (context != null) s"context ${context}" else "default context"} " +
    s" from users K8S config file")

  private var defaultClient: DefaultKubernetesClient = _

  override def initialize(): Unit = {
    // Auto-configure K8S client from K8S config file
    System.setProperty(Config.KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, "true");
    val userHome = System.getProperty("user.home")
    System.setProperty(Config.KUBERNETES_KUBECONFIG_FILE,
      Option(System.getenv("KUBECONFIG"))
        .getOrElse(Paths.get(userHome, ".kube", "config").toFile.getAbsolutePath))
    val config = Config.autoConfigure(context)

    defaultClient = new DefaultKubernetesClient(config)
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
  }

  override def getKubernetesClient: DefaultKubernetesClient = {
    defaultClient
  }
}
