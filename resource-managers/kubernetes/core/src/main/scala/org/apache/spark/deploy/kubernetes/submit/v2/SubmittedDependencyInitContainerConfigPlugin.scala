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
package org.apache.spark.deploy.kubernetes.submit.v2

import org.apache.spark.SSLOptions
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

private[spark] trait SubmittedDependencyInitContainerConfigPlugin {
  /**
   * Obtain configuration to fetch submitted dependencies from a resource staging server.
   * This includes the resource identifiers for the jar and file bundles, as well as the
   * remote location of the resource staging server, and the location of secret files for
   * authenticating to the resource staging server. Note that the secret file paths here need to
   * line up with the locations the secrets are mounted by
   * SubmittedDependencyInitContainerVolumesPlugin; constants provide the consistency and
   * convention for these to line up.
   */
  def configurationsToFetchSubmittedDependencies(): Map[String, String]
}

private[spark] class SubmittedDependencyInitContainerConfigPluginImpl(
    resourceStagingServerUri: String,
    jarsResourceId: String,
    filesResourceId: String,
    jarsSecretKey: String,
    filesSecretKey: String,
    trustStoreSecretKey: String,
    secretsVolumeMountPath: String,
    resourceStagingServiceSslOptions: SSLOptions)
    extends SubmittedDependencyInitContainerConfigPlugin {

  override def configurationsToFetchSubmittedDependencies(): Map[String, String] = {
    Map[String, String](
      RESOURCE_STAGING_SERVER_URI.key -> resourceStagingServerUri,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> jarsResourceId,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        s"$secretsVolumeMountPath/$jarsSecretKey",
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> filesResourceId,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        s"$secretsVolumeMountPath/$filesSecretKey",
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key ->
        resourceStagingServiceSslOptions.enabled.toString) ++
      resourceStagingServiceSslOptions.trustStore.map { _ =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE.key,
          s"$secretsVolumeMountPath/$trustStoreSecretKey")
      }.toMap ++
      resourceStagingServiceSslOptions.trustStorePassword.map { password =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD.key, password)
      }.toMap ++
      resourceStagingServiceSslOptions.trustStoreType.map { storeType =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE.key, storeType)
      }.toMap
  }
}
