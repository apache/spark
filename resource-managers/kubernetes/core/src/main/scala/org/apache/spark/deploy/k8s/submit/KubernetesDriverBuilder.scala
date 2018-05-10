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

<<<<<<< HEAD
import java.io.File

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpec, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf, KubernetesUtils}
import org.apache.spark.deploy.k8s.features.{BasicDriverFeatureStep, DriverKubernetesCredentialsFeatureStep, DriverServiceFeatureStep, MountLocalFilesFeatureStep, MountSecretsFeatureStep}
import org.apache.spark.util.Utils
||||||| merged common ancestors
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpec, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf}
import org.apache.spark.deploy.k8s.features.{BasicDriverFeatureStep, DriverKubernetesCredentialsFeatureStep, DriverServiceFeatureStep, MountSecretsFeatureStep}
=======
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpec, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf}
import org.apache.spark.deploy.k8s.features.{BasicDriverFeatureStep, DriverKubernetesCredentialsFeatureStep, DriverServiceFeatureStep, LocalDirsFeatureStep, MountSecretsFeatureStep}
>>>>>>> apache/master

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
<<<<<<< HEAD
      new MountSecretsFeatureStep(_),
    provideMountLocalFilesStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => MountLocalFilesFeatureStep) =
      new MountLocalFilesFeatureStep(_)) {
  import KubernetesDriverBuilder._
||||||| merged common ancestors
      new MountSecretsFeatureStep(_)) {
=======
      new MountSecretsFeatureStep(_),
    provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
      => LocalDirsFeatureStep =
      new LocalDirsFeatureStep(_)) {
>>>>>>> apache/master

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf]): KubernetesDriverSpec = {
    val baseFeatures = Seq(
      provideBasicStep(kubernetesConf),
      provideCredentialsStep(kubernetesConf),
<<<<<<< HEAD
      provideServiceStep(kubernetesConf))
    val withProvideSecretsStep = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
||||||| merged common ancestors
      provideServiceStep(kubernetesConf))
    val allFeatures = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
=======
      provideServiceStep(kubernetesConf),
      provideLocalDirsStep(kubernetesConf))
    val allFeatures = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
>>>>>>> apache/master
      baseFeatures ++ Seq(provideSecretsStep(kubernetesConf))
    } else baseFeatures

    val sparkFiles = kubernetesConf.sparkFiles()
    val localFiles = KubernetesUtils.submitterLocalFiles(sparkFiles).map(new File(_))
    require(localFiles.forall(_.isFile), s"All submitted local files must be present and not" +
      s" directories, Got got: ${localFiles.map(_.getAbsolutePath).mkString(",")}")

    val totalFileSize = localFiles.map(_.length()).sum
    val totalSizeBytesString = Utils.bytesToString(totalFileSize)
    require(totalFileSize < MAX_SECRET_BUNDLE_SIZE_BYTES,
      s"Total size of all files submitted must be less than $MAX_SECRET_BUNDLE_SIZE_BYTES_STRING." +
        s" Total size for files ended up being $totalSizeBytesString")
    val allFeatures = if (localFiles.nonEmpty) {
      withProvideSecretsStep ++ Seq(provideMountLocalFilesStep(kubernetesConf))
    } else withProvideSecretsStep

    var spec = KubernetesDriverSpec.initialSpec(kubernetesConf.sparkConf.getAll.toMap)
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

private object KubernetesDriverBuilder {
  val MAX_SECRET_BUNDLE_SIZE_BYTES = 10240
  val MAX_SECRET_BUNDLE_SIZE_BYTES_STRING =
    Utils.bytesToString(MAX_SECRET_BUNDLE_SIZE_BYTES)
}
