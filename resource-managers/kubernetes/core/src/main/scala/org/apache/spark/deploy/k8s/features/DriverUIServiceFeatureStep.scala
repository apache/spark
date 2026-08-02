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
  KUBERNETES_DRIVER_SERVICE_IP_FAMILIES,
  KUBERNETES_DRIVER_SERVICE_IP_FAMILY_POLICY,
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
 * The actual UI port is never known at submission time: even with a fixed `spark.ui.port`, Jetty
 * may bind a different port after a collision (`Utils.startServiceOnPort` retries the next ports),
 * and when TLS is enabled the reachable port is the secure connector's port, not the configured
 * one. Routing to a stale port is especially dangerous in `hostNetwork` mode, where a pod endpoint
 * is the node IP, so a wrong `targetPort` could reach an unrelated driver co-located on the same
 * node. To avoid that window, the Service is always created *without* a selector, leaving it
 * endpointless, with the configured (or default) UI port as a placeholder `port`/`targetPort`
 * purely to satisfy Kubernetes' Service port validation (must be > 0). Once the driver's Jetty
 * server has bound, [[org.apache.spark.scheduler.cluster.k8s.K8sDriverUIServicePatcher]] patches
 * the selector and the actual `targetPort` (`SparkUI.boundPort`) together, so the Service only
 * starts routing once it points at the correct port. When the Spark UI is disabled the Service is
 * not created at all; and if the driver dies before its UI binds, the created Service simply stays
 * endpointless and routes nowhere. This reconciliation requires `patch services` RBAC on the
 * driver's ServiceAccount whenever the feature is enabled.
 */
private[spark] class DriverUIServiceFeatureStep(kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {
  import DriverUIServiceFeatureStep._

  private val enabled = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_ENABLED)
  private lazy val serviceType = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_TYPE)
  private lazy val configuredUIPort = kubernetesConf.get(config.UI.UI_PORT)

  private val active: Boolean = if (enabled && !kubernetesConf.get(config.UI.UI_ENABLED)) {
    logWarning(s"Ignoring ${KUBERNETES_DRIVER_UI_SERVICE_ENABLED.key}=true because " +
      s"${config.UI.UI_ENABLED.key}=false; no driver UI Service will be created.")
    false
  } else {
    enabled
  }

  /**
   * Placeholder port used when building the Service. The real bound port is only known after the
   * driver's Jetty server binds (and may differ from `configuredUIPort` after a collision or when
   * TLS is enabled), so we substitute the configured UI port, or the default (typically 4040) when
   * a random port was requested (`spark.ui.port=0`), purely to satisfy Kubernetes' Service port
   * validation (must be > 0). After the driver JVM starts,
   * [[org.apache.spark.scheduler.cluster.k8s.K8sDriverUIServicePatcher]] updates the Service's
   * `targetPort` to the real bound port.
   */
  private lazy val servicePort: Int = if (configuredUIPort > 0) {
    configuredUIPort
  } else {
    config.UI.UI_PORT.defaultValue.get
  }

  private lazy val serviceName: String = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_NAME)
    .getOrElse(kubernetesConf.driverUIServiceName)

  // The UI Service reuses the driver Service IP family settings to keep the same IP family.
  private lazy val ipFamilyPolicy = kubernetesConf.get(KUBERNETES_DRIVER_SERVICE_IP_FAMILY_POLICY)
  private lazy val ipFamilies =
    kubernetesConf.get(KUBERNETES_DRIVER_SERVICE_IP_FAMILIES).split(",").toList.asJava

  override def configurePod(pod: SparkPod): SparkPod = pod

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    // These properties drive the runtime patch that installs the withheld selector and the actual
    // bound `targetPort` once the driver's UI has bound. They are always needed while the feature
    // is active, since the Service is created endpointless regardless of the configured port.
    if (active) {
      Map(
        KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL -> serviceName,
        KUBERNETES_DRIVER_UI_SERVICE_PORT_INTERNAL -> servicePort.toString,
        KUBERNETES_DRIVER_UI_SERVICE_SELECTOR_INTERNAL -> encodeSelector(kubernetesConf.labels))
    } else {
      Map.empty
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (!active) return Seq.empty

    val uiService = new ServiceBuilder()
      .withNewMetadata()
        .withName(serviceName)
        .addToAnnotations(kubernetesConf.serviceAnnotations.asJava)
        .addToLabels(SPARK_APP_ID_LABEL, kubernetesConf.appId)
        .addToLabels(kubernetesConf.serviceLabels.asJava)
        .endMetadata()
      .withNewSpec()
        .withType(serviceType)
        .withIpFamilyPolicy(ipFamilyPolicy)
        .withIpFamilies(ipFamilies)
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

  /**
   * Internal spark conf key used to pass the UI service's stable `port` from this feature step to
   * the driver runtime, so `K8sDriverUIServicePatcher` can use it as the strategic-merge key when
   * patching the Service's `targetPort`.
   */
  val KUBERNETES_DRIVER_UI_SERVICE_PORT_INTERNAL =
    "spark.kubernetes.driver.ui.service.port.internal"

  /**
   * Internal spark conf key carrying the Service selector that was withheld at creation time.
   * `K8sDriverUIServicePatcher` installs it together with the actual `targetPort` once the driver's
   * UI has bound.
   */
  val KUBERNETES_DRIVER_UI_SERVICE_SELECTOR_INTERNAL =
    "spark.kubernetes.driver.ui.service.selector.internal"

  /**
   * Encode a selector label map as a single conf value. Kubernetes label keys and values cannot
   * contain `,` or `=`, so `k=v` pairs joined by `,` round-trip unambiguously.
   */
  def encodeSelector(selector: Map[String, String]): String =
    selector.map { case (k, v) => s"$k=$v" }.mkString(",")

  /** Inverse of [[encodeSelector]]. */
  def decodeSelector(encoded: String): Map[String, String] = {
    if (encoded.isEmpty) {
      Map.empty
    } else {
      encoded.split(",").map { pair =>
        val Array(k, v) = pair.split("=", 2)
        k -> v
      }.toMap
    }
  }
}
