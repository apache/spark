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

import io.fabric8.kubernetes.api.model.JobBuilder

import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.deploy.k8s.features.bindings.{JavaDriverFeatureStep, PythonDriverFeatureStep}

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
    provideJavaStep: (
      KubernetesConf[KubernetesDriverSpecificConf]
        => JavaDriverFeatureStep) =
      new JavaDriverFeatureStep(_),
    providePythonStep: (
      KubernetesConf[KubernetesDriverSpecificConf]
      => PythonDriverFeatureStep) =
      new PythonDriverFeatureStep(_)) {

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

    val bindingsStep = kubernetesConf.roleSpecificConf.mainAppResource.map {
        case JavaMainAppResource(_) =>
          provideJavaStep(kubernetesConf)
        case PythonMainAppResource(_) =>
          providePythonStep(kubernetesConf)}
      .getOrElse(provideJavaStep(kubernetesConf))

    val allFeatures = (baseFeatures :+ bindingsStep) ++
      secretFeature ++ envSecretFeature ++ volumesFeature

    val spec = KubernetesDriverSpec.initialSpec(kubernetesConf.sparkConf.getAll.toMap)
    val (configuredSparkPod, configuredKubernetesResources, configuredSystemProperties) =
      allFeatures.foldLeft(
      (SparkPod.initialPod(), spec.driverKubernetesResources, spec.systemProperties)) {
      (init, feature) =>
        init match {
          case (sparkPod, driverKubernetesResource, systemProperties) =>
            (feature.configurePod(sparkPod),
              feature.getAdditionalKubernetesResources() ++ driverKubernetesResource,
              feature.getAdditionalPodSystemProperties() ++ systemProperties)
        }
    }
    new KubernetesDriverSpec(
      createJobFromPod(spec.job, configuredSparkPod),
      configuredKubernetesResources,
      configuredSystemProperties
    )
  }

  def createJobFromPod(sparkJob: SparkJob, sparkPod: SparkPod): SparkJob = {
    val job = new JobBuilder(sparkJob.job)
      .editOrNewMetadata()
      .withName(sparkPod.pod.getMetadata.getName)
      .endMetadata()
      .editOrNewSpec()
        .editOrNewTemplate()
          .withMetadata(sparkPod.pod.getMetadata)
          .withSpec(sparkPod.pod.getSpec)
        .endTemplate()
      .endSpec().build()
    new SparkJob(job, sparkPod.container)
  }
}
