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

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{Secret, SecretBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.deploy.kubernetes.InitContainerResourceStagingServerSecretPlugin
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.SubmittedDependencyUploader
import org.apache.spark.internal.config.OptionalConfigEntry
import org.apache.spark.util.Utils

private[spark] class SubmittedResourcesInitContainerConfigurationStep(
    submittedResourcesSecretName: String,
    internalResourceStagingServerUri: String,
    initContainerSecretMountPath: String,
    resourceStagingServerSslEnabled: Boolean,
    maybeInternalTrustStoreUri: Option[String],
    maybeInternalClientCertUri: Option[String],
    maybeInternalTrustStorePassword: Option[String],
    maybeInternalTrustStoreType: Option[String],
    submittedDependencyUploader: SubmittedDependencyUploader,
    submittedResourcesSecretPlugin: InitContainerResourceStagingServerSecretPlugin)
  extends InitContainerConfigurationStep {

  override def configureInitContainer(initContainerSpec: InitContainerSpec): InitContainerSpec = {
    val jarsIdAndSecret = submittedDependencyUploader.uploadJars()
    val filesIdAndSecret = submittedDependencyUploader.uploadFiles()

    val submittedResourcesInitContainerProperties = Map[String, String](
      RESOURCE_STAGING_SERVER_URI.key -> internalResourceStagingServerUri,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> jarsIdAndSecret.resourceId,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        s"$initContainerSecretMountPath/$INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY",
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> filesIdAndSecret.resourceId,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        s"$initContainerSecretMountPath/$INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY",
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> resourceStagingServerSslEnabled.toString) ++
      resolveSecretPath(
        maybeInternalTrustStoreUri,
        INIT_CONTAINER_STAGING_SERVER_TRUSTSTORE_SECRET_KEY,
        RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE,
        "TrustStore URI") ++
      resolveSecretPath(
        maybeInternalClientCertUri,
        INIT_CONTAINER_STAGING_SERVER_CLIENT_CERT_SECRET_KEY,
        RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM,
        "Client certificate URI") ++
      maybeInternalTrustStorePassword.map { password =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD.key, password)
      }.toMap ++
      maybeInternalTrustStoreType.map { storeType =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE.key, storeType)
      }.toMap
    val initContainerSecret = createResourceStagingServerSecret(
        jarsIdAndSecret.resourceSecret, filesIdAndSecret.resourceSecret)
    val additionalDriverSparkConf =
        Map(
          EXECUTOR_INIT_CONTAINER_SECRET.key -> initContainerSecret.getMetadata.getName,
          EXECUTOR_INIT_CONTAINER_SECRET_MOUNT_DIR.key -> initContainerSecretMountPath)
    val initContainerWithSecretVolumeMount = submittedResourcesSecretPlugin
        .mountResourceStagingServerSecretIntoInitContainer(initContainerSpec.initContainer)
    val podWithSecretVolume = submittedResourcesSecretPlugin
        .addResourceStagingServerSecretVolumeToPod(initContainerSpec.podToInitialize)
    initContainerSpec.copy(
        initContainer = initContainerWithSecretVolumeMount,
        podToInitialize = podWithSecretVolume,
        initContainerDependentResources =
            initContainerSpec.initContainerDependentResources ++ Seq(initContainerSecret),
        initContainerProperties =
            initContainerSpec.initContainerProperties ++ submittedResourcesInitContainerProperties,
        additionalDriverSparkConf = additionalDriverSparkConf)
  }

  private def createResourceStagingServerSecret(
      jarsResourceSecret: String, filesResourceSecret: String): Secret = {
    val trustStoreBase64 = convertFileToBase64IfSubmitterLocal(
      INIT_CONTAINER_STAGING_SERVER_TRUSTSTORE_SECRET_KEY, maybeInternalTrustStoreUri)
    val clientCertBase64 = convertFileToBase64IfSubmitterLocal(
      INIT_CONTAINER_STAGING_SERVER_CLIENT_CERT_SECRET_KEY, maybeInternalClientCertUri)
    val jarsSecretBase64 = BaseEncoding.base64().encode(jarsResourceSecret.getBytes(Charsets.UTF_8))
    val filesSecretBase64 = BaseEncoding.base64().encode(
      filesResourceSecret.getBytes(Charsets.UTF_8))
    val secretData = Map(
      INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY -> jarsSecretBase64,
      INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY -> filesSecretBase64) ++
      trustStoreBase64 ++
      clientCertBase64
    val kubernetesSecret = new SecretBuilder()
      .withNewMetadata()
        .withName(submittedResourcesSecretName)
        .endMetadata()
      .addToData(secretData.asJava)
      .build()
    kubernetesSecret
  }

  private def convertFileToBase64IfSubmitterLocal(secretKey: String, secretUri: Option[String])
      : Map[String, String] = {
    secretUri.filter { trustStore =>
      Option(Utils.resolveURI(trustStore).getScheme).getOrElse("file") == "file"
    }.map { uri =>
      val file = new File(Utils.resolveURI(uri).getPath)
      require(file.isFile, "Dependency server trustStore provided at" +
        file.getAbsolutePath + " does not exist or is not a file.")
      (secretKey, BaseEncoding.base64().encode(Files.toByteArray(file)))
    }.toMap
  }

  private def resolveSecretPath(
      maybeUri: Option[String],
      secretKey: String,
      configEntry: OptionalConfigEntry[String],
      uriType: String): Map[String, String] = {
    maybeUri.map(Utils.resolveURI).map { uri =>
      val resolvedPath = Option(uri.getScheme).getOrElse("file") match {
        case "file" => s"$initContainerSecretMountPath/$secretKey"
        case "local" => uri.getPath
        case invalid => throw new SparkException(s"$uriType has invalid scheme $invalid must be" +
          s" local://, file://, or empty.")
      }
      (configEntry.key, resolvedPath)
    }.toMap
  }
}
