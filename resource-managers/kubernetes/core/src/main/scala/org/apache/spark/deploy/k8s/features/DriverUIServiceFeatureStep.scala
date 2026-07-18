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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{HasMetadata, ServiceBuilder}

import org.apache.spark.deploy.k8s.{KubernetesDriverConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.{
  KUBERNETES_DRIVER_UI_SERVICE_ENABLED,
  KUBERNETES_DRIVER_UI_SERVICE_NAME,
  KUBERNETES_DRIVER_UI_SERVICE_TYPE
}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.{config, Logging}

/**
 * Optionally provisions a dedicated Kubernetes Service exposing only the Spark driver's Web UI
 * port.
 *
 * At creation time, the Service's `targetPort` is set to the configured `spark.ui.port`
 * (a placeholder when the user has requested a random port). Once the driver's Jetty server
 * has bound, `K8sDriverUIServicePatcher` updates the Service's `targetPort` to reflect the
 * actual bound port.
 */
private[spark] class DriverUIServiceFeatureStep(kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {
  import DriverUIServiceFeatureStep._

  private val enabled = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_ENABLED)
  private lazy val serviceType = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_TYPE)
  private lazy val configuredUIPort = kubernetesConf.get(config.UI.UI_PORT)

  /**
   * Port value used when building the Service. When the user has requested a random UI port
   * (`spark.ui.port=0`), the actual port is only known after the driver's Jetty server binds,
   * so we substitute the default UI port (typically 4040) purely as a placeholder to satisfy
   * Kubernetes' Service port validation (must be > 0). After the driver JVM starts,
   * [[org.apache.spark.scheduler.cluster.k8s.K8sDriverUIServicePatcher]] updates the Service's
   * `targetPort` to the real bound port.
   */
  private lazy val servicePort: Int = if (configuredUIPort == 0) {
    config.UI.UI_PORT.defaultValue.get
  } else {
    configuredUIPort
  }

  private lazy val serviceName: String = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_NAME)
    .getOrElse(kubernetesConf.driverUIServiceName)

  override def configurePod(pod: SparkPod): SparkPod = pod

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    if (enabled) {
      Map(KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL -> serviceName)
    } else {
      Map.empty
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (!enabled) return Seq.empty

    val uiService = new ServiceBuilder()
      .withNewMetadata()
        .withName(serviceName)
        .addToAnnotations(kubernetesConf.serviceAnnotations.asJava)
        .addToLabels(SPARK_APP_ID_LABEL, kubernetesConf.appId)
        .addToLabels(kubernetesConf.serviceLabels.asJava)
        .endMetadata()
      .withNewSpec()
        .withType(serviceType)
        .withSelector(kubernetesConf.labels.asJava)
        .addNewPort()
          .withName(UI_PORT_NAME)
          .withPort(servicePort)
          .withNewTargetPort(servicePort)
          .endPort()
        .endSpec()
      .build()
    Seq(uiService)
  }
}

private[spark] object DriverUIServiceFeatureStep {
  /**
   * Internal spark conf key used to pass the UI service name from this feature step to the
   * driver runtime (SparkContext) so `K8sDriverUIServicePatcher` can look up the Service to
   * patch.
   */
  val KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL =
    "spark.kubernetes.driver.ui.service.name.internal"
}
