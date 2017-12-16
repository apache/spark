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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.{ConfigurationUtils, InitContainerBootstrapImpl, MountSecretsBootstrapImpl}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

/**
 * Returns the complete ordered list of steps required to configure the init-container. This is
 * only used when there are remote application dependencies to localize.
 */
private[spark] class InitContainerConfigOrchestrator(
    namespace: String,
    kubernetesResourceNamePrefix: String,
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    jarsDownloadPath: String,
    filesDownloadPath: String,
    dockerImagePullPolicy: String,
    driverLabels: Map[String, String],
    initContainerConfigMapName: String,
    initContainerConfigMapKey: String,
    submissionSparkConf: SparkConf) {

  private val initContainerImage = submissionSparkConf
    .get(INIT_CONTAINER_IMAGE)
    .getOrElse(throw new SparkException(
      "Must specify the init-container image when there are remote dependencies"))
  private val downloadTimeoutMinutes = submissionSparkConf.get(INIT_CONTAINER_MOUNT_TIMEOUT)

  def getAllConfigurationSteps(): Seq[InitContainerConfigurationStep] = {
    val initContainerBootstrap = new InitContainerBootstrapImpl(
      initContainerImage,
      dockerImagePullPolicy,
      jarsDownloadPath,
      filesDownloadPath,
      downloadTimeoutMinutes,
      initContainerConfigMapName,
      initContainerConfigMapKey,
      SPARK_POD_DRIVER_ROLE,
      submissionSparkConf)
    val baseInitContainerStep = new BaseInitContainerConfigurationStep(
      sparkJars,
      sparkFiles,
      jarsDownloadPath,
      filesDownloadPath,
      initContainerBootstrap)

    val driverSecretNamesToMountPaths = ConfigurationUtils.parsePrefixedKeyValuePairs(
      submissionSparkConf,
      KUBERNETES_DRIVER_SECRETS_PREFIX)
    // Mount user-specified driver secrets also into the driver's init-container. The
    // init-container may need credentials in the secrets to be able to download remote
    // dependencies. The driver's main container and its init-container share the secrets
    // because the init-container is sort of an implementation details and this sharing
    // avoids introducing a dedicated configuration property just for the init-container.
    val maybeMountSecretsStep = if (driverSecretNamesToMountPaths.nonEmpty) {
      val mountSecretsBootstrap = new MountSecretsBootstrapImpl(driverSecretNamesToMountPaths)
      Some(new InitContainerMountSecretsStep(mountSecretsBootstrap))
    } else {
      None
    }

    Seq(baseInitContainerStep) ++
      maybeMountSecretsStep.toSeq
  }
}
