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
package org.apache.spark.scheduler.cluster.k8s.watch

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesStepUtil}

private[spark] object WatchCreator {
  def create(sparkConf: SparkConf,
             kubernetesClient: KubernetesClient,
             appId: String): String = {
    val watchConf = KubernetesConf.createWatchConf(sparkConf,
      appId, KubernetesStepUtil.driverPod(sparkConf, kubernetesClient))
    val resolvedWatchSpec = KubernetesWatchBuilder.buildFromFeatures(watchConf, kubernetesClient)
    val watchPod = resolvedWatchSpec.pod
    val podWithAttachedContainer = new PodBuilder(watchPod.pod)
      .editOrNewSpec()
      .addToContainers(watchPod.container)
      .endSpec()
      .build()
    val createdWatchPod = kubernetesClient.pods()
      .inNamespace(KubernetesStepUtil.namespace(sparkConf))
      .resource(podWithAttachedContainer)
      .create()

    createdWatchPod.getAdditionalProperties.get("status.podIP").toString
  }
}
