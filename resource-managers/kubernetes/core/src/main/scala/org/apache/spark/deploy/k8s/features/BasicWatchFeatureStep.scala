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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder, Quantity}

import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s.{KubernetesStepUtil, KubernetesWatchConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_WATCH_LIMIT_CORES, KUBERNETES_WATCH_REQUEST_CORES, WATCH_CONTAINER_IMAGE}
import org.apache.spark.deploy.k8s.Constants.{DEFAULT_WATCH_CONTAINER_NAME, ENV_SPARK_USER, WATCH_PORT_NAME}
import org.apache.spark.util.Utils

private[spark] class BasicWatchFeatureStep(
    kubernetesConf: KubernetesWatchConf)
  extends KubernetesFeatureConfigStep {

  private val podName: String = "spark-watch"

  private val containerImage = kubernetesConf
    .get(WATCH_CONTAINER_IMAGE)
    .getOrElse(throw new SparkException("Must specify the watch container image"))

  // CPU settings
  private val coresRequest = kubernetesConf.get(KUBERNETES_WATCH_REQUEST_CORES)
  private val driverLimitCores = kubernetesConf.get(KUBERNETES_WATCH_LIMIT_CORES)

  // Networking
  private val watchPort = KubernetesStepUtil.watchPort(kubernetesConf.sparkConf)


  // Memory

  override def configurePod(pod: SparkPod): SparkPod = {
    val cpuQuantity = new Quantity(coresRequest)
    val maybeCpuLimitQuantity = driverLimitCores.map { limitCores =>
      ("cpu", new Quantity(limitCores))
    }

    // Create an owner reference for the driver so the resources can be garbage collected
    // when the driver is closed
    val driverOwnerReference = kubernetesConf.driverPod.map(KubernetesStepUtil.driverOwnerReference)

    val watchContainer = new ContainerBuilder(pod.container)
      .withName(Option(pod.container.getName).getOrElse(DEFAULT_WATCH_CONTAINER_NAME))
      .withImage(containerImage)
      .withImagePullPolicy(kubernetesConf.imagePullPolicy)
      // Add a port where the watch service can be accessed
      .addNewPort()
        .withName(WATCH_PORT_NAME)
        .withContainerPort(watchPort)
        .withProtocol("TCP")
        .endPort()
      .editOrNewResources()
        .addToRequests("cpu", cpuQuantity)
        .addToLimits(maybeCpuLimitQuantity.toMap.asJava)
        .endResources()
      .addNewEnv()
        .withName(ENV_SPARK_USER)
        .withValue(Utils.getCurrentUserName())
        .endEnv()
      .addToArgs("watch")
      .build()

    // Create the K8s pod spec for the watcher
    val watchPod = new PodBuilder(pod.pod)
      .editOrNewMetadata()
        .withName(podName)
        .addToLabels(kubernetesConf.labels.asJava)
        .addToAnnotations(kubernetesConf.annotations.asJava)
        .addToOwnerReferences(driverOwnerReference.toSeq: _*)
        .endMetadata()
      .editOrNewSpec()
        .withRestartPolicy("Never")
        .withNodeSelector(kubernetesConf.nodeSelector.asJava)
        .withNodeSelector(kubernetesConf.watchNodeSelector.asJava)
        .addToImagePullSecrets(kubernetesConf.imagePullSecrets: _*)
        .endSpec()
      .build()

    kubernetesConf.schedulerName
      .foreach(watchPod.getSpec.setSchedulerName)


    SparkPod(watchPod, watchContainer)
  }
}
