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

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesExecutorSpecificConf, KubernetesRoleSpecificConf, SparkPod}
<<<<<<< HEAD
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, LocalDirsFeatureStep, MountLocalFilesFeatureStep, MountSecretsFeatureStep}
=======
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, EnvSecretsFeatureStep, KubernetesFeatureConfigStep, LocalDirsFeatureStep, MountSecretsFeatureStep}
>>>>>>> master

private[spark] class KubernetesExecutorBuilder(
    provideBasicStep: (KubernetesConf[KubernetesExecutorSpecificConf]) => BasicExecutorFeatureStep =
      new BasicExecutorFeatureStep(_),
    provideSecretsStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf]) => MountSecretsFeatureStep =
      new MountSecretsFeatureStep(_),
<<<<<<< HEAD
    provideMountLocalFilesStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf]) => MountLocalFilesFeatureStep =
      new MountLocalFilesFeatureStep(_),
=======
    provideEnvSecretsStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf] => EnvSecretsFeatureStep) =
      new EnvSecretsFeatureStep(_),
>>>>>>> master
    provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => LocalDirsFeatureStep =
      new LocalDirsFeatureStep(_)) {

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf]): SparkPod = {
    val baseFeatures = Seq(
<<<<<<< HEAD
      provideBasicStep(kubernetesConf), provideLocalDirsStep(kubernetesConf))
    val withProvideSecretsStep = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      baseFeatures ++ Seq(provideSecretsStep(kubernetesConf))
    } else baseFeatures
    val allFeatures = if (kubernetesConf.mountLocalFilesSecretName.isDefined) {
      withProvideSecretsStep ++ Seq(provideMountLocalFilesStep(kubernetesConf))
    } else withProvideSecretsStep
=======
      provideBasicStep(kubernetesConf),
      provideLocalDirsStep(kubernetesConf))

    val maybeRoleSecretNamesStep = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Some(provideSecretsStep(kubernetesConf)) } else None

    val maybeProvideSecretsStep = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      Some(provideEnvSecretsStep(kubernetesConf)) } else None

    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++
      maybeRoleSecretNamesStep.toSeq ++
      maybeProvideSecretsStep.toSeq

>>>>>>> master
    var executorPod = SparkPod.initialPod()
    for (feature <- allFeatures) {
      executorPod = feature.configurePod(executorPod)
    }
    executorPod
  }
}
