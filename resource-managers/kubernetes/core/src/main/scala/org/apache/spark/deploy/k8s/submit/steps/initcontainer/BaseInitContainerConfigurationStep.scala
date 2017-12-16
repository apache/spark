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
package org.apache.spark.deploy.k8s.submit.steps.initcontainer

import org.apache.spark.deploy.k8s.{InitContainerBootstrap, PodWithDetachedInitContainer}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.submit.KubernetesFileUtils

private[spark] class BaseInitContainerConfigurationStep(
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    jarsDownloadPath: String,
    filesDownloadPath: String,
    initContainerBootstrap: InitContainerBootstrap)
  extends InitContainerConfigurationStep {

  override def configureInitContainer(initContainerSpec: InitContainerSpec): InitContainerSpec = {
    val remoteJarsToDownload = KubernetesFileUtils.getOnlyRemoteFiles(sparkJars)
    val remoteFilesToDownload = KubernetesFileUtils.getOnlyRemoteFiles(sparkFiles)
    val remoteJarsConf = if (remoteJarsToDownload.nonEmpty) {
      Map(INIT_CONTAINER_REMOTE_JARS.key -> remoteJarsToDownload.mkString(","))
    } else {
      Map()
    }
    val remoteFilesConf = if (remoteFilesToDownload.nonEmpty) {
      Map(INIT_CONTAINER_REMOTE_FILES.key -> remoteFilesToDownload.mkString(","))
    } else {
      Map()
    }

    val baseInitContainerConfig = Map(
      JARS_DOWNLOAD_LOCATION.key -> jarsDownloadPath,
      FILES_DOWNLOAD_LOCATION.key -> filesDownloadPath) ++
      remoteJarsConf ++
      remoteFilesConf
    val bootstrappedPodAndInitContainer =
      initContainerBootstrap.bootstrapInitContainer(
        PodWithDetachedInitContainer(
          initContainerSpec.driverPod,
          initContainerSpec.initContainer,
          initContainerSpec.driverContainer))

    initContainerSpec.copy(
      initContainer = bootstrappedPodAndInitContainer.initContainer,
      driverContainer = bootstrappedPodAndInitContainer.mainContainer,
      driverPod = bootstrappedPodAndInitContainer.pod,
      initContainerProperties = baseInitContainerConfig)
  }
}
