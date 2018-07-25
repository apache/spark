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
import org.apache.spark.deploy.k8s.features._
||||||| merged common ancestors
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, EnvSecretsFeatureStep, KubernetesFeatureConfigStep, LocalDirsFeatureStep, MountSecretsFeatureStep}
=======
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, EnvSecretsFeatureStep, LocalDirsFeatureStep, MountSecretsFeatureStep}
>>>>>>> upstream/master

private[spark] class KubernetesExecutorBuilder(
    provideBasicStep: (KubernetesConf [KubernetesExecutorSpecificConf])
      => BasicExecutorFeatureStep =
      new BasicExecutorFeatureStep(_),
    provideSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => MountSecretsFeatureStep =
      new MountSecretsFeatureStep(_),
    provideMountLocalFilesStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf]) => MountLocalFilesFeatureStep =
      new MountLocalFilesFeatureStep(_),
    provideEnvSecretsStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf] => EnvSecretsFeatureStep) =
      new EnvSecretsFeatureStep(_),
    provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => LocalDirsFeatureStep =
      new LocalDirsFeatureStep(_),
    provideVolumesStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => MountVolumesFeatureStep) =
      new MountVolumesFeatureStep(_)) {

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf]): SparkPod = {

<<<<<<< HEAD
    val localFilesStep = if (kubernetesConf.mountLocalFilesSecretName.isDefined) {
      Some(provideMountLocalFilesStep(kubernetesConf)) } else None

    val maybeRoleSecretNamesStep = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Some(provideSecretsStep(kubernetesConf)) } else None
||||||| merged common ancestors
    val maybeRoleSecretNamesStep = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Some(provideSecretsStep(kubernetesConf)) } else None
=======
    val baseFeatures = Seq(provideBasicStep(kubernetesConf), provideLocalDirsStep(kubernetesConf))
    val secretFeature = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Seq(provideSecretsStep(kubernetesConf))
    } else Nil
    val secretEnvFeature = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      Seq(provideEnvSecretsStep(kubernetesConf))
    } else Nil
    val volumesFeature = if (kubernetesConf.roleVolumes.nonEmpty) {
      Seq(provideVolumesStep(kubernetesConf))
    } else Nil
>>>>>>> upstream/master

<<<<<<< HEAD
    val maybeProvideSecretsStep = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      Some(provideEnvSecretsStep(kubernetesConf)) } else None

    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++
      maybeRoleSecretNamesStep.toSeq ++
      maybeProvideSecretsStep.toSeq ++
      localFilesStep.toSeq
||||||| merged common ancestors
    val maybeProvideSecretsStep = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      Some(provideEnvSecretsStep(kubernetesConf)) } else None

    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++
      maybeRoleSecretNamesStep.toSeq ++
      maybeProvideSecretsStep.toSeq
=======
    val allFeatures = baseFeatures ++ secretFeature ++ secretEnvFeature ++ volumesFeature
>>>>>>> upstream/master

    var executorPod = SparkPod.initialPod()
    for (feature <- allFeatures) {
      executorPod = feature.configurePod(executorPod)
    }
    executorPod
  }
}
