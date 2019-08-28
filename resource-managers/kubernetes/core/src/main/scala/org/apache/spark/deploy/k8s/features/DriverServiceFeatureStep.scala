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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{HasMetadata, ServiceBuilder}

import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.{Clock, SystemClock}

private[spark] class DriverServiceFeatureStep(
    kubernetesConf: KubernetesDriverConf,
    clock: Clock = new SystemClock())
  extends KubernetesFeatureConfigStep with Logging {
  import DriverServiceFeatureStep._

  require(kubernetesConf.getOption(DRIVER_BIND_ADDRESS_KEY).isEmpty,
    s"$DRIVER_BIND_ADDRESS_KEY is not supported in Kubernetes mode, as the driver's bind " +
      "address is managed and set to the driver pod's IP address.")
  require(kubernetesConf.getOption(DRIVER_HOST_KEY).isEmpty,
    s"$DRIVER_HOST_KEY is not supported in Kubernetes mode, as the driver's hostname will be " +
      "managed via a Kubernetes service.")

  private val preferredServiceName = s"${kubernetesConf.resourceNamePrefix}$DRIVER_SVC_POSTFIX"
  private val resolvedServiceName = if (preferredServiceName.length <= MAX_SERVICE_NAME_LENGTH) {
    preferredServiceName
  } else {
    val randomServiceId = KubernetesUtils.uniqueID(clock = clock)
    val shorterServiceName = s"spark-$randomServiceId$DRIVER_SVC_POSTFIX"
    logWarning(s"Driver's hostname would preferably be $preferredServiceName, but this is " +
      s"too long (must be <= $MAX_SERVICE_NAME_LENGTH characters). Falling back to use " +
      s"$shorterServiceName as the driver service's name.")
    shorterServiceName
  }

  private val driverPort = kubernetesConf.sparkConf.getInt(
    config.DRIVER_PORT.key, DEFAULT_DRIVER_PORT)
  private val driverBlockManagerPort = kubernetesConf.sparkConf.getInt(
    config.DRIVER_BLOCK_MANAGER_PORT.key, DEFAULT_BLOCKMANAGER_PORT)
  private val  driverUIPort = kubernetesConf.get(config.UI.UI_PORT)

  override def configurePod(pod: SparkPod): SparkPod = pod

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val driverHostname = s"$resolvedServiceName.${kubernetesConf.namespace}.svc"
    Map(DRIVER_HOST_KEY -> driverHostname,
      config.DRIVER_PORT.key -> driverPort.toString,
      config.DRIVER_BLOCK_MANAGER_PORT.key -> driverBlockManagerPort.toString)
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val driverService = new ServiceBuilder()
      .withNewMetadata()
        .withName(resolvedServiceName)
        .endMetadata()
      .withNewSpec()
        .withClusterIP("None")
        .withSelector(kubernetesConf.labels.asJava)
        .addNewPort()
          .withName(DRIVER_PORT_NAME)
          .withPort(driverPort)
          .withNewTargetPort(driverPort)
          .endPort()
        .addNewPort()
          .withName(BLOCK_MANAGER_PORT_NAME)
          .withPort(driverBlockManagerPort)
          .withNewTargetPort(driverBlockManagerPort)
          .endPort()
        .addNewPort()
          .withName(UI_PORT_NAME)
          .withPort(driverUIPort)
          .withNewTargetPort(driverUIPort)
          .endPort()
        .endSpec()
      .build()
    Seq(driverService)
  }
}

private[spark] object DriverServiceFeatureStep {
  val DRIVER_BIND_ADDRESS_KEY = config.DRIVER_BIND_ADDRESS.key
  val DRIVER_HOST_KEY = config.DRIVER_HOST_ADDRESS.key
  val DRIVER_SVC_POSTFIX = "-driver-svc"
  val MAX_SERVICE_NAME_LENGTH = 63
}
