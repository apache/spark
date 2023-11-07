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

import io.fabric8.kubernetes.api.model._
import io.fabric8.volcano.client.DefaultVolcanoClient
import io.fabric8.volcano.scheduling.v1beta1.{PodGroup, PodGroupSpec}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverConf, KubernetesExecutorConf, SparkPod}
import org.apache.spark.internal.Logging

private[spark] class VolcanoFeatureStep extends KubernetesDriverCustomFeatureConfigStep
  with KubernetesExecutorCustomFeatureConfigStep with Logging {
  import VolcanoFeatureStep._

  private var kubernetesConf: KubernetesConf = _

  private lazy val podGroupName = s"${kubernetesConf.appId}-podgroup"
  private lazy val namespace = kubernetesConf.namespace

  override def init(config: KubernetesDriverConf): Unit = {
    kubernetesConf = config
  }

  override def init(config: KubernetesExecutorConf): Unit = {
    kubernetesConf = config
  }

  override def getAdditionalPreKubernetesResources(): Seq[HasMetadata] = {
    if (kubernetesConf.isInstanceOf[KubernetesExecutorConf]) {
      logWarning("VolcanoFeatureStep#getAdditionalPreKubernetesResources() is not supported " +
        "for executor.")
      return Seq.empty
    }
    lazy val client = new DefaultVolcanoClient
    val template = kubernetesConf.getOption(POD_GROUP_TEMPLATE_FILE_KEY)
    val pg = template.map(client.podGroups.load(_).item).getOrElse(new PodGroup())
    var metadata = pg.getMetadata
    if (metadata == null) metadata = new ObjectMeta
    metadata.setName(podGroupName)
    metadata.setNamespace(namespace)
    pg.setMetadata(metadata)

    var spec = pg.getSpec
    if (spec == null) spec = new PodGroupSpec
    pg.setSpec(spec)

    Seq(pg)
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    val k8sPodBuilder = new PodBuilder(pod.pod)
      .editMetadata()
        .addToAnnotations(POD_GROUP_ANNOTATION, podGroupName)
      .endMetadata()
    val k8sPod = k8sPodBuilder.build()
    SparkPod(k8sPod, pod.container)
  }
}

private[spark] object VolcanoFeatureStep {
  val POD_GROUP_ANNOTATION = "scheduling.k8s.io/group-name"
  val POD_GROUP_TEMPLATE_FILE_KEY = "spark.kubernetes.scheduler.volcano.podGroupTemplateFile"
}
