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
import org.apache.spark.deploy.k8s.{InitContainerBootstrap, KubernetesUtils, MountSecretsBootstrap}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

/**
 * Figures out and returns the complete ordered list of InitContainerConfigurationSteps required to
 * configure the driver init-container. The returned steps will be applied in the given order to
 * produce a final InitContainerSpec that is used to construct the driver init-container in
 * DriverInitContainerBootstrapStep. This class is only used when an init-container is needed, i.e.,
 * when there are remote application dependencies to localize.
 */
private[spark] class InitContainerConfigOrchestrator(
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    jarsDownloadPath: String,
    filesDownloadPath: String,
    imagePullPolicy: String,
    configMapName: String,
    configMapKey: String,
    sparkConf: SparkConf) {

  private val initContainerImage = sparkConf
    .get(INIT_CONTAINER_IMAGE)
    .getOrElse(throw new SparkException(
      "Must specify the init-container image when there are remote dependencies"))

  def getAllConfigurationSteps: Seq[InitContainerConfigurationStep] = {
    val initContainerBootstrap = new InitContainerBootstrap(
      initContainerImage,
      imagePullPolicy,
      jarsDownloadPath,
      filesDownloadPath,
      configMapName,
      configMapKey,
      SPARK_POD_DRIVER_ROLE,
      sparkConf)
    val baseStep = new BasicInitContainerConfigurationStep(
      sparkJars,
      sparkFiles,
      jarsDownloadPath,
      filesDownloadPath,
      initContainerBootstrap)

    val secretNamesToMountPaths = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_DRIVER_SECRETS_PREFIX)
    // Mount user-specified driver secrets also into the driver's init-container. The
    // init-container may need credentials in the secrets to be able to download remote
    // dependencies. The driver's main container and its init-container share the secrets
    // because the init-container is sort of an implementation details and this sharing
    // avoids introducing a dedicated configuration property just for the init-container.
    val mountSecretsStep = if (secretNamesToMountPaths.nonEmpty) {
      Seq(new InitContainerMountSecretsStep(new MountSecretsBootstrap(secretNamesToMountPaths)))
    } else {
      Nil
    }

    Seq(baseStep) ++ mountSecretsStep
  }
}
