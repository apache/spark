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

import io.fabric8.kubernetes.api.model.{OwnerReference, OwnerReferenceBuilder, Pod}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DRIVER_POD_NAME, KUBERNETES_NAMESPACE, KUBERNETES_WATCH_PORT}
import org.apache.spark.deploy.k8s.Constants.{DEFAULT_DRIVER_PORT, DEFAULT_WATCH_PORT}
import org.apache.spark.internal.config.{DRIVER_HOST_ADDRESS, DRIVER_PORT}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

private[spark] object KubernetesStepUtil {
  def namespace(conf: SparkConf): String = {
    conf.get(KUBERNETES_NAMESPACE)
  }

  def kubernetesDriverPodName(conf: SparkConf): Option[String] = {
    conf.get(KUBERNETES_DRIVER_POD_NAME)
  }

  def driverPod(conf: SparkConf, kubernetesClient: KubernetesClient): Option[Pod] = {
    val namespace = KubernetesStepUtil.namespace(conf)
    kubernetesDriverPodName(conf)
      .map(name => Option(kubernetesClient.pods()
        .inNamespace(namespace)
        .withName(name)
        .get())
        .getOrElse(throw new SparkException(
          s"No pod was found named $name in the cluster in the " +
            s"namespace $namespace (this was supposed to be the driver pod.).")))
  }
  def driverOwnerReference(pod: Pod): OwnerReference = {
    new OwnerReferenceBuilder()
      .withController(true)
      .withApiVersion(pod.getApiVersion)
      .withKind(pod.getKind)
      .withName(pod.getMetadata.getName)
      .withUid(pod.getMetadata.getUid)
      .build()
  }

  def driverEndpoint(conf: KubernetesConf): RpcEndpointAddress = {
    RpcEndpointAddress(
      conf.get(DRIVER_HOST_ADDRESS),
      conf.sparkConf.getInt(DRIVER_PORT.key, DEFAULT_DRIVER_PORT),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
  }

  def watchPort(sparkConf: SparkConf): Int = {
    sparkConf.getInt(KUBERNETES_WATCH_PORT.key, DEFAULT_WATCH_PORT)
  }
}
