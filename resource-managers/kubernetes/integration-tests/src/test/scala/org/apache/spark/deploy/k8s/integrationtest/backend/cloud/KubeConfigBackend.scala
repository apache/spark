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

import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient}
import io.fabric8.kubernetes.client.utils.Utils
import org.apache.commons.lang3.StringUtils

import org.apache.spark.deploy.k8s.integrationtest.TestConstants
import org.apache.spark.deploy.k8s.integrationtest.backend.IntegrationTestBackend
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils.checkAndGetK8sMasterUrl

private[spark] class KubeConfigBackend(var context: String)
  extends IntegrationTestBackend with Logging {
  logInfo(s"K8S Integration tests will run against " +
    s"${if (context != null) s"context ${context}" else "default context"}" +
    s" from users K8S config file")

  private var defaultClient: DefaultKubernetesClient = _

  override def initialize(): Unit = {
    // Auto-configure K8S client from K8S config file
    if (Utils.getSystemPropertyOrEnvVar(Config.KUBERNETES_KUBECONFIG_FILE, null: String) == null) {
      // Fabric 8 client will automatically assume a default location in this case
      logWarning("No explicit KUBECONFIG specified, will assume $HOME/.kube/config")
    }
    val config = Config.autoConfigure(context)

    // If an explicit master URL was specified then override that detected from the
    // K8S config if it is different
    var masterUrl = Option(System.getProperty(TestConstants.CONFIG_KEY_KUBE_MASTER_URL))
      .getOrElse(null)
    if (StringUtils.isNotBlank(masterUrl)) {
      // Clean up master URL which would have been specified in Spark format into a normal
      // K8S master URL
      masterUrl = checkAndGetK8sMasterUrl(masterUrl).replaceFirst("k8s://", "")
      if (!StringUtils.equals(config.getMasterUrl, masterUrl)) {
        logInfo(s"Overriding K8S master URL ${config.getMasterUrl} from K8S config file " +
          s"with user specified master URL ${masterUrl}")
        config.setMasterUrl(masterUrl)
      }
    }

    defaultClient = new DefaultKubernetesClient(config)
  }

  override def cleanUp(): Unit = {
    if (defaultClient != null) {
      defaultClient.close()
    }
    super.cleanUp()
  }

  override def getKubernetesClient: DefaultKubernetesClient = {
    defaultClient
  }
}
