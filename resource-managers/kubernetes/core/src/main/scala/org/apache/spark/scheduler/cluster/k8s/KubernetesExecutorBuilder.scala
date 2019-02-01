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

import org.apache.spark.SecurityManager
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, EnvSecretsFeatureStep, LocalDirsFeatureStep, MountSecretsFeatureStep}

<<<<<<< HEAD
private[spark] class KubernetesExecutorBuilder(
    provideBasicStep: (KubernetesConf [KubernetesExecutorSpecificConf])
      => BasicExecutorFeatureStep =
      new BasicExecutorFeatureStep(_),
    provideSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => MountSecretsFeatureStep =
      new MountSecretsFeatureStep(_),
    provideMountLocalFilesStep:
      (KubernetesConf[KubernetesExecutorSpecificConf]) => MountLocalExecutorFilesFeatureStep =
      new MountLocalExecutorFilesFeatureStep(_),
    provideEnvSecretsStep:
      (KubernetesConf[_ <: KubernetesRoleSpecificConf] => EnvSecretsFeatureStep) =
      new EnvSecretsFeatureStep(_),
    provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => LocalDirsFeatureStep =
      new LocalDirsFeatureStep(_),
    provideVolumesStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => MountVolumesFeatureStep) =
      new MountVolumesFeatureStep(_),
    provideHadoopConfStep: (
      KubernetesConf[KubernetesExecutorSpecificConf]
      => HadoopConfExecutorFeatureStep) =
      new HadoopConfExecutorFeatureStep(_),
    provideKerberosConfStep: (
      KubernetesConf[KubernetesExecutorSpecificConf]
      => KerberosConfExecutorFeatureStep) =
      new KerberosConfExecutorFeatureStep(_),
    provideHadoopSparkUserStep: (
      KubernetesConf[KubernetesExecutorSpecificConf]
      => HadoopSparkUserExecutorFeatureStep) =
      new HadoopSparkUserExecutorFeatureStep(_),
    provideInitialPod: () => SparkPod = () => SparkPod.initialPod()) {

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf]): SparkPod = {
    val sparkConf = kubernetesConf.sparkConf
    val maybeHadoopConfigMap = sparkConf.getOption(HADOOP_CONFIG_MAP_NAME)
    val maybeDTSecretName = sparkConf.getOption(KERBEROS_DT_SECRET_NAME)
    val maybeDTDataItem = sparkConf.getOption(KERBEROS_DT_SECRET_KEY)

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
    val localFilesFeature = if (kubernetesConf.mountLocalFilesSecretName.isDefined) {
      Seq(provideMountLocalFilesStep(kubernetesConf))
    } else Nil

    val maybeHadoopConfFeatureSteps = maybeHadoopConfigMap.map { _ =>
      val maybeKerberosStep =
        if (maybeDTSecretName.isDefined && maybeDTDataItem.isDefined) {
          provideKerberosConfStep(kubernetesConf)
        } else {
          provideHadoopSparkUserStep(kubernetesConf)
        }
      Seq(provideHadoopConfStep(kubernetesConf)) :+
        maybeKerberosStep
    }.getOrElse(Seq.empty[KubernetesFeatureConfigStep])

    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++
      secretFeature ++
      secretEnvFeature ++
      volumesFeature ++
      maybeHadoopConfFeatureSteps ++
      localFilesFeature

    var executorPod = provideInitialPod()
    for (feature <- allFeatures) {
      executorPod = feature.configurePod(executorPod)
    }
    executorPod
=======
private[spark] class KubernetesExecutorBuilder {

  def buildFromFeatures(
      conf: KubernetesExecutorConf,
      secMgr: SecurityManager,
      client: KubernetesClient): SparkPod = {
    val initialPod = conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE)
      .map { file =>
        KubernetesUtils.loadPodFromTemplate(
          client,
          new File(file),
          conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME))
      }
      .getOrElse(SparkPod.initialPod())

    val features = Seq(
      new BasicExecutorFeatureStep(conf, secMgr),
      new MountSecretsFeatureStep(conf),
      new EnvSecretsFeatureStep(conf),
      new LocalDirsFeatureStep(conf),
      new MountVolumesFeatureStep(conf))

    features.foldLeft(initialPod) { case (pod, feature) => feature.configurePod(pod) }
>>>>>>> master
  }

}
