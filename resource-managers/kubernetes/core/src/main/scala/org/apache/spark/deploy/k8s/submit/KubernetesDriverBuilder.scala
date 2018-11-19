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
package org.apache.spark.deploy.k8s.submit

import java.io.File

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{Config, KubernetesConf, KubernetesDriverSpec, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.features._

private[spark] class KubernetesDriverBuilder(
    provideBasicStep: (KubernetesConf[KubernetesDriverSpecificConf]) => BasicDriverFeatureStep =
      new BasicDriverFeatureStep(_),
    provideCredentialsStep: (KubernetesConf[KubernetesDriverSpecificConf])
      => DriverKubernetesCredentialsFeatureStep =
      new DriverKubernetesCredentialsFeatureStep(_),
    provideServiceStep: (KubernetesConf[KubernetesDriverSpecificConf]) => DriverServiceFeatureStep =
      new DriverServiceFeatureStep(_),
    provideSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => MountSecretsFeatureStep) =
      new MountSecretsFeatureStep(_),
    provideEnvSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => EnvSecretsFeatureStep) =
      new EnvSecretsFeatureStep(_),
    provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => LocalDirsFeatureStep =
      new LocalDirsFeatureStep(_),
    provideVolumesStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => MountVolumesFeatureStep) =
      new MountVolumesFeatureStep(_),
    provideDriverCommandStep: (
      KubernetesConf[KubernetesDriverSpecificConf]
      => DriverCommandFeatureStep) =
      new DriverCommandFeatureStep(_),
    provideHadoopGlobalStep: (
      KubernetesConf[KubernetesDriverSpecificConf]
        => KerberosConfDriverFeatureStep) =
    new KerberosConfDriverFeatureStep(_),
    providePodTemplateConfigMapStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => PodTemplateConfigMapStep) =
    new PodTemplateConfigMapStep(_),
    provideInitialPod: () => SparkPod = () => SparkPod.initialPod()) {

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf]): KubernetesDriverSpec = {
    val baseFeatures = Seq(
      provideBasicStep(kubernetesConf),
      provideCredentialsStep(kubernetesConf),
      provideServiceStep(kubernetesConf),
      provideLocalDirsStep(kubernetesConf))

    val secretFeature = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Seq(provideSecretsStep(kubernetesConf))
    } else Nil
    val envSecretFeature = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      Seq(provideEnvSecretsStep(kubernetesConf))
    } else Nil
    val volumesFeature = if (kubernetesConf.roleVolumes.nonEmpty) {
      Seq(provideVolumesStep(kubernetesConf))
    } else Nil
    val podTemplateFeature = if (
      kubernetesConf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).isDefined) {
      Seq(providePodTemplateConfigMapStep(kubernetesConf))
    } else Nil

    val driverCommandStep = provideDriverCommandStep(kubernetesConf)

    val maybeHadoopConfigStep =
      kubernetesConf.hadoopConfSpec.map { _ =>
        provideHadoopGlobalStep(kubernetesConf)}

    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++ Seq(driverCommandStep) ++
        secretFeature ++ envSecretFeature ++ volumesFeature ++
        maybeHadoopConfigStep.toSeq ++ podTemplateFeature

    var spec = KubernetesDriverSpec(
      provideInitialPod(),
      driverKubernetesResources = Seq.empty,
      kubernetesConf.sparkConf.getAll.toMap)
    for (feature <- allFeatures) {
      val configuredPod = feature.configurePod(spec.pod)
      val addedSystemProperties = feature.getAdditionalPodSystemProperties()
      val addedResources = feature.getAdditionalKubernetesResources()
      spec = KubernetesDriverSpec(
        configuredPod,
        spec.driverKubernetesResources ++ addedResources,
        spec.systemProperties ++ addedSystemProperties)
    }
    spec
  }
}

private[spark] object KubernetesDriverBuilder {
  def apply(kubernetesClient: KubernetesClient, conf: SparkConf): KubernetesDriverBuilder = {
    conf.get(Config.KUBERNETES_DRIVER_PODTEMPLATE_FILE)
      .map(new File(_))
      .map(file => new KubernetesDriverBuilder(provideInitialPod = () =>
        KubernetesUtils.loadPodFromTemplate(
          kubernetesClient,
          file,
          conf.get(Config.KUBERNETES_DRIVER_PODTEMPLATE_CONTAINER_NAME))
      ))
      .getOrElse(new KubernetesDriverBuilder())
  }
}
