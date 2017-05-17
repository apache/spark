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

import java.io.File

import org.apache.spark.{SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.config._

class SubmittedDependencyInitContainerConfigPluginSuite extends SparkFunSuite {
  private val STAGING_SERVER_URI = "http://localhost:9000"
  private val JARS_RESOURCE_ID = "jars-id"
  private val FILES_RESOURCE_ID = "files-id"
  private val JARS_SECRET_KEY = "jars"
  private val FILES_SECRET_KEY = "files"
  private val TRUSTSTORE_SECRET_KEY = "trustStore"
  private val SECRETS_VOLUME_MOUNT_PATH = "/var/data/"
  private val TRUSTSTORE_PASSWORD = "trustStore"
  private val TRUSTSTORE_FILE = "/mnt/secrets/trustStore.jks"
  private val TRUSTSTORE_TYPE = "jks"
  private val RESOURCE_STAGING_SERVICE_SSL_OPTIONS = SSLOptions(
    enabled = true,
    trustStore = Some(new File(TRUSTSTORE_FILE)),
    trustStorePassword = Some(TRUSTSTORE_PASSWORD),
    trustStoreType = Some(TRUSTSTORE_TYPE))

  test("Plugin should provide configuration for fetching uploaded dependencies") {
    val configPluginUnderTest = new SubmittedDependencyInitContainerConfigPluginImpl(
      STAGING_SERVER_URI,
      JARS_RESOURCE_ID,
      FILES_RESOURCE_ID,
      JARS_SECRET_KEY,
      FILES_SECRET_KEY,
      TRUSTSTORE_SECRET_KEY,
      SECRETS_VOLUME_MOUNT_PATH,
      SSLOptions())
    val addedConfigurations = configPluginUnderTest.configurationsToFetchSubmittedDependencies()
    val expectedConfigurations = Map(
      RESOURCE_STAGING_SERVER_URI.key -> STAGING_SERVER_URI,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> JARS_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> FILES_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        s"$SECRETS_VOLUME_MOUNT_PATH/$JARS_SECRET_KEY",
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        s"$SECRETS_VOLUME_MOUNT_PATH/$FILES_SECRET_KEY",
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> "false")
    assert(addedConfigurations === expectedConfigurations)
  }

  test("Plugin should set up SSL with the appropriate trustStore if it's provided.") {
    val configPluginUnderTest = new SubmittedDependencyInitContainerConfigPluginImpl(
      STAGING_SERVER_URI,
      JARS_RESOURCE_ID,
      FILES_RESOURCE_ID,
      JARS_SECRET_KEY,
      FILES_SECRET_KEY,
      TRUSTSTORE_SECRET_KEY,
      SECRETS_VOLUME_MOUNT_PATH,
      RESOURCE_STAGING_SERVICE_SSL_OPTIONS)
    val addedConfigurations = configPluginUnderTest.configurationsToFetchSubmittedDependencies()
    val expectedSslConfigurations = Map(
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> "true",
      RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE.key ->
          s"$SECRETS_VOLUME_MOUNT_PATH/$TRUSTSTORE_SECRET_KEY",
      RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD.key -> TRUSTSTORE_PASSWORD,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE.key -> TRUSTSTORE_TYPE)
    assert(expectedSslConfigurations.toSet.subsetOf(addedConfigurations.toSet))
  }
}
