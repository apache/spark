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
package org.apache.spark.deploy.kubernetes.submit.submitsteps.initcontainer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.{InitContainerResourceStagingServerSecretPluginImpl, OptionRequirements, SparkPodInitContainerBootstrapImpl}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.SubmittedDependencyUploaderImpl
import org.apache.spark.deploy.rest.kubernetes.{ResourceStagingServerSslOptionsProviderImpl, RetrofitClientFactoryImpl}
import org.apache.spark.util.Utils

/**
 * Returns the complete ordered list of steps required to configure the init-container.
 */
private[spark] class InitContainerConfigurationStepsOrchestrator(
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

  private val submittedResourcesSecretName = s"$kubernetesResourceNamePrefix-init-secret"
  private val resourceStagingServerUri = submissionSparkConf.get(RESOURCE_STAGING_SERVER_URI)
  private val resourceStagingServerInternalUri =
      submissionSparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_URI)
  private val initContainerImage = submissionSparkConf.get(INIT_CONTAINER_DOCKER_IMAGE)
  private val downloadTimeoutMinutes = submissionSparkConf.get(INIT_CONTAINER_MOUNT_TIMEOUT)
  private val maybeResourceStagingServerInternalTrustStore =
    submissionSparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_FILE)
      .orElse(submissionSparkConf.get(RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE))
  private val maybeResourceStagingServerInternalTrustStorePassword =
    submissionSparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_PASSWORD)
      .orElse(submissionSparkConf.get(RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD))
  private val maybeResourceStagingServerInternalTrustStoreType =
    submissionSparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_TYPE)
      .orElse(submissionSparkConf.get(RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE))
  private val maybeResourceStagingServerInternalClientCert =
    submissionSparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_CLIENT_CERT_PEM)
      .orElse(submissionSparkConf.get(RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM))
  private val resourceStagingServerInternalSslEnabled =
    submissionSparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_SSL_ENABLED)
      .orElse(submissionSparkConf.get(RESOURCE_STAGING_SERVER_SSL_ENABLED))
      .getOrElse(false)
  OptionRequirements.requireNandDefined(
    maybeResourceStagingServerInternalClientCert,
    maybeResourceStagingServerInternalTrustStore,
    "Cannot provide both a certificate file and a trustStore file for init-containers to" +
      " use for contacting the resource staging server over TLS.")

  require(maybeResourceStagingServerInternalTrustStore.forall { trustStore =>
    Option(Utils.resolveURI(trustStore).getScheme).getOrElse("file") match {
      case "file" | "local" => true
      case _ => false
    }
  }, "TrustStore URI used for contacting the resource staging server from init containers must" +
    " have no scheme, or scheme file://, or scheme local://.")

  require(maybeResourceStagingServerInternalClientCert.forall { trustStore =>
    Option(Utils.resolveURI(trustStore).getScheme).getOrElse("file") match {
      case "file" | "local" => true
      case _ => false
    }
  }, "Client cert file URI used for contacting the resource staging server from init containers" +
    " must have no scheme, or scheme file://, or scheme local://.")

  def getAllConfigurationSteps(): Seq[InitContainerConfigurationStep] = {
    val initContainerBootstrap = new SparkPodInitContainerBootstrapImpl(
        initContainerImage,
        dockerImagePullPolicy,
        jarsDownloadPath,
        filesDownloadPath,
        downloadTimeoutMinutes,
        initContainerConfigMapName,
        initContainerConfigMapKey)
    val baseInitContainerStep = new BaseInitContainerConfigurationStep(
        sparkJars,
        sparkFiles,
        jarsDownloadPath,
        filesDownloadPath,
        initContainerConfigMapName,
        initContainerConfigMapKey,
        initContainerBootstrap)
    val submittedResourcesInitContainerStep = resourceStagingServerUri.map {
        stagingServerUri =>
      val mountSecretPlugin = new InitContainerResourceStagingServerSecretPluginImpl(
          submittedResourcesSecretName,
          INIT_CONTAINER_SECRET_VOLUME_MOUNT_PATH)
      val submittedDependencyUploader = new SubmittedDependencyUploaderImpl(
          driverLabels,
          namespace,
          stagingServerUri,
          sparkJars,
          sparkFiles,
          new ResourceStagingServerSslOptionsProviderImpl(submissionSparkConf).getSslOptions,
          RetrofitClientFactoryImpl)
      new SubmittedResourcesInitContainerConfigurationStep(
          submittedResourcesSecretName,
          resourceStagingServerInternalUri.getOrElse(stagingServerUri),
          INIT_CONTAINER_SECRET_VOLUME_MOUNT_PATH,
          resourceStagingServerInternalSslEnabled,
          maybeResourceStagingServerInternalTrustStore,
          maybeResourceStagingServerInternalClientCert,
          maybeResourceStagingServerInternalTrustStorePassword,
          maybeResourceStagingServerInternalTrustStoreType,
          submittedDependencyUploader,
          mountSecretPlugin)
    }
    Seq(baseInitContainerStep) ++ submittedResourcesInitContainerStep.toSeq
  }
}
