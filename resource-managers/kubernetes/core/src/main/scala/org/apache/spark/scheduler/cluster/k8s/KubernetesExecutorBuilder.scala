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
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, EnvSecretsFeatureStep, LocalDirsFeatureStep, MountSecretsFeatureStep}

private[spark] class KubernetesExecutorBuilder(
    provideBasicStep: (KubernetesConf[KubernetesExecutorSpecificConf]) => BasicExecutorFeatureStep =
      new BasicExecutorFeatureStep(_),
    provideSecretsStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf]) => MountSecretsFeatureStep =
      new MountSecretsFeatureStep(_),
    provideEnvSecretsStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf] => EnvSecretsFeatureStep) =
      new EnvSecretsFeatureStep(_),
    provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => LocalDirsFeatureStep =
      new LocalDirsFeatureStep(_)) {

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf]): SparkPod = {
    val baseFeatures = Seq(provideBasicStep(kubernetesConf), provideLocalDirsStep(kubernetesConf))
    var allFeatures = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      baseFeatures ++ Seq(provideSecretsStep(kubernetesConf))
    } else baseFeatures

    allFeatures = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      allFeatures ++ Seq(provideEnvSecretsStep(kubernetesConf))
    } else allFeatures

    var executorPod = SparkPod.initialPod()
    for (feature <- allFeatures) {
      executorPod = feature.configurePod(executorPod)
    }
    executorPod
  }
}
