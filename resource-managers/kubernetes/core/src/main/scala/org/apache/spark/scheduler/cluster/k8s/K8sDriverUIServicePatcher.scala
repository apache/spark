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
package org.apache.spark.scheduler.cluster.k8s

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{IntOrString, ServiceBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.{PatchContext, PatchType}

import org.apache.spark.deploy.k8s.Constants.UI_PORT_NAME
import org.apache.spark.internal.Logging

/**
 * Driver-side utility that updates the `targetPort` of the dedicated Spark UI Kubernetes Service
 * (created by [[org.apache.spark.deploy.k8s.features.DriverUIServiceFeatureStep]]) to the actual
 * port the driver's Jetty server bound to.
 */
private[k8s] object K8sDriverUIServicePatcher extends Logging {

  /**
   * Bring the endpointless placeholder Service online: set its `spark-ui` `targetPort` to the
   * actual bound port and install the `selector` in the same patch, so it only starts routing
   * once it points at the correct port.
   *
   * @param client       Kubernetes client to use (typically the one already held by the backend).
   * @param namespace    Namespace where the Service lives.
   * @param serviceName  Name of the UI Service (from
   *                     `spark.kubernetes.driver.ui.service.name.internal`).
   * @param servicePort  The Service's stable `port` for the `spark-ui` entry (strategic merge key).
   * @param actualPort   The actual port the driver's Jetty server bound to
   *                     (typically `SparkUI.boundPort`).
   * @param selector     Selector matching the driver pod, withheld at creation time.
   */
  def patchTargetPortAndSelector(
      client: KubernetesClient,
      namespace: String,
      serviceName: String,
      servicePort: Int,
      actualPort: Int,
      selector: Map[String, String]): Unit = {
    try {
      val patch = new ServiceBuilder()
        .withNewSpec()
          .withSelector(selector.asJava)
          .addNewPort()
            .withName(UI_PORT_NAME)
            .withPort(servicePort)
            .withTargetPort(new IntOrString(actualPort))
            .endPort()
          .endSpec()
        .build()

      client.services()
        .inNamespace(namespace)
        .withName(serviceName)
        .patch(PatchContext.of(PatchType.STRATEGIC_MERGE), patch)

      logInfo(s"Patched UI service '$serviceName' targetPort to $actualPort and" +
        s" installed selector $selector")
    } catch {
      case e: Exception =>
        logError(s"Failed to patch UI service '$serviceName' targetPort to $actualPort", e)
        throw e
    }
  }
}
