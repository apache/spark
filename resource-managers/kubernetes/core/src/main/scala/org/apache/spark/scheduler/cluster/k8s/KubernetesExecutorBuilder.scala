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

import java.io.File

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features._

private[spark] class KubernetesExecutorBuilder(
    provideBasicStep: (KubernetesExecutorConf, SecurityManager) => BasicExecutorFeatureStep =
      new BasicExecutorFeatureStep(_, _),
    provideSecretsStep: (KubernetesConf => MountSecretsFeatureStep) =
      new MountSecretsFeatureStep(_),
    provideEnvSecretsStep: (KubernetesConf => EnvSecretsFeatureStep) =
      new EnvSecretsFeatureStep(_),
    provideLocalDirsStep: (KubernetesConf => LocalDirsFeatureStep) =
      new LocalDirsFeatureStep(_),
    provideVolumesStep: (KubernetesConf => MountVolumesFeatureStep) =
      new MountVolumesFeatureStep(_),
    provideInitialPod: () => SparkPod = () => SparkPod.initialPod()) {

  def buildFromFeatures(
      kubernetesConf: KubernetesExecutorConf,
      secMgr: SecurityManager): SparkPod = {
    val sparkConf = kubernetesConf.sparkConf
    val baseFeatures = Seq(provideBasicStep(kubernetesConf, secMgr),
      provideLocalDirsStep(kubernetesConf))
    val secretFeature = if (kubernetesConf.secretNamesToMountPaths.nonEmpty) {
      Seq(provideSecretsStep(kubernetesConf))
    } else Nil
    val secretEnvFeature = if (kubernetesConf.secretEnvNamesToKeyRefs.nonEmpty) {
      Seq(provideEnvSecretsStep(kubernetesConf))
    } else Nil
    val volumesFeature = if (kubernetesConf.volumes.nonEmpty) {
      Seq(provideVolumesStep(kubernetesConf))
    } else Nil

    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++
      secretFeature ++
      secretEnvFeature ++
      volumesFeature

    var executorPod = provideInitialPod()
    for (feature <- allFeatures) {
      executorPod = feature.configurePod(executorPod)
    }
    executorPod
  }
}

private[spark] object KubernetesExecutorBuilder {
  def apply(kubernetesClient: KubernetesClient, conf: SparkConf): KubernetesExecutorBuilder = {
    conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE)
      .map(new File(_))
      .map(file => new KubernetesExecutorBuilder(provideInitialPod = () =>
          KubernetesUtils.loadPodFromTemplate(
            kubernetesClient,
            file,
            conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME))
      ))
      .getOrElse(new KubernetesExecutorBuilder())
  }
}
