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
package org.apache.spark.deploy.k8s.features

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME, KUBERNETES_WATCH_SERVICE_ACCOUNT_NAME}
import org.apache.spark.deploy.k8s.KubernetesUtils.buildPodWithServiceAccount

/**
 * Add the watch service account (or driver service account if no watch service account was
 * specified) to the pod.
 */
private[spark] class WatchKubernetesCredentialsFeatureStep(kubernetesConf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  private lazy val driverServiceAccount = kubernetesConf.get(KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME)
  private lazy val watchServiceAccount = kubernetesConf.get(KUBERNETES_WATCH_SERVICE_ACCOUNT_NAME)

  override def configurePod(pod: SparkPod): SparkPod = {
    if (Option(pod.pod.getSpec.getServiceAccount).nonEmpty) {
      return pod
    }

    pod.copy(
      pod = buildPodWithServiceAccount(watchServiceAccount
        .orElse(driverServiceAccount), pod).getOrElse(pod.pod)
    )
  }
}
