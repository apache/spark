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

import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.util.Utils

<<<<<<< HEAD
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
    provideMountLocalFilesStep: (KubernetesConf[KubernetesDriverSpecificConf]
      => MountLocalDriverFilesFeatureStep) =
      new MountLocalDriverFilesFeatureStep(_),
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
=======
private[spark] class KubernetesDriverBuilder {
>>>>>>> master

  import KubernetesDriverBuilder._

  def buildFromFeatures(
      conf: KubernetesDriverConf,
      client: KubernetesClient): KubernetesDriverSpec = {
    val initialPod = conf.get(Config.KUBERNETES_DRIVER_PODTEMPLATE_FILE)
      .map { file =>
        KubernetesUtils.loadPodFromTemplate(
          client,
          new File(file),
          conf.get(Config.KUBERNETES_DRIVER_PODTEMPLATE_CONTAINER_NAME))
      }
      .getOrElse(SparkPod.initialPod())

<<<<<<< HEAD
    val localFilesFeature = provideMountLocalFilesStep(kubernetesConf)
    val localFiles = KubernetesUtils.submitterLocalFiles(localFilesFeature.allFiles)
        .map(new File(_))
    require(localFiles.forall(_.isFile), s"All submitted local files must be present and not" +
      s" directories, Got: ${localFiles.map(_.getAbsolutePath).mkString(",")}")

    val totalFileSize = localFiles.map(_.length()).sum
    val totalSizeBytesString = Utils.bytesToString(totalFileSize)
    require(totalFileSize < MAX_SECRET_BUNDLE_SIZE_BYTES,
      s"Total size of all files submitted must be less than $MAX_SECRET_BUNDLE_SIZE_BYTES_STRING." +
        s" Total size for files ended up being $totalSizeBytesString")
    val providedLocalFilesFeature = if (localFiles.nonEmpty) {
      Seq(localFilesFeature)
    } else Nil

    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++ Seq(driverCommandStep) ++
        secretFeature ++ envSecretFeature ++ volumesFeature ++
        maybeHadoopConfigStep.toSeq ++ podTemplateFeature ++
        providedLocalFilesFeature
=======
    val features = Seq(
      new BasicDriverFeatureStep(conf),
      new DriverKubernetesCredentialsFeatureStep(conf),
      new DriverServiceFeatureStep(conf),
      new MountSecretsFeatureStep(conf),
      new EnvSecretsFeatureStep(conf),
      new LocalDirsFeatureStep(conf),
      new MountVolumesFeatureStep(conf),
      new DriverCommandFeatureStep(conf),
      new HadoopConfDriverFeatureStep(conf),
      new KerberosConfDriverFeatureStep(conf),
      new PodTemplateConfigMapStep(conf))
>>>>>>> master

    val spec = KubernetesDriverSpec(
      initialPod,
      driverKubernetesResources = Seq.empty,
      conf.sparkConf.getAll.toMap)

    features.foldLeft(spec) { case (spec, feature) =>
      val configuredPod = feature.configurePod(spec.pod)
      val addedSystemProperties = feature.getAdditionalPodSystemProperties()
      val addedResources = feature.getAdditionalKubernetesResources()
      KubernetesDriverSpec(
        configuredPod,
        spec.driverKubernetesResources ++ addedResources,
        spec.systemProperties ++ addedSystemProperties)
    }
  }

<<<<<<< HEAD
private[spark] object KubernetesDriverBuilder {
  val MAX_SECRET_BUNDLE_SIZE_BYTES = 20480
  val MAX_SECRET_BUNDLE_SIZE_BYTES_STRING =
    Utils.bytesToString(MAX_SECRET_BUNDLE_SIZE_BYTES)

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
=======
>>>>>>> master
}
