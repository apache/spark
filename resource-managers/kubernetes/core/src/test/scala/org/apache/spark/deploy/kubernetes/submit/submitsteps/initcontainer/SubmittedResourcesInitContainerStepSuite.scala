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
import java.util.UUID

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model._
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.InitContainerResourceStagingServerSecretPlugin
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.{SubmittedDependencyUploader, SubmittedResourceIdAndSecret}
import org.apache.spark.util.Utils

class SubmittedResourcesInitContainerStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val RESOURCE_SECRET_NAME = "secret"
  private val JARS_RESOURCE_ID = "jarsID"
  private val JARS_SECRET = "jarsSecret"
  private val FILES_RESOURCE_ID = "filesID"
  private val FILES_SECRET = "filesSecret"
  private val STAGING_SERVER_URI = "http://localhost:8000"
  private val SECRET_MOUNT_PATH = "/tmp"
  private val RSS_SECRET = Map(
    INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY ->
      BaseEncoding.base64().encode(JARS_SECRET.getBytes(Charsets.UTF_8)),
    INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY ->
      BaseEncoding.base64().encode(FILES_SECRET.getBytes(Charsets.UTF_8))
  ).asJava
  private var RSS_WITH_SSL_SECRET: java.util.Map[String, String] = _
  private var TRUSTSTORE_FILENAME: String = ""
  private var TRUSTSTORE_FILE: File = _
  private var TRUSTSTORE_URI: Option[String] = None
  private val TRUSTSTORE_PASS = "trustStorePassword"
  private val TRUSTSTORE_TYPE = "jks"
  private var CERT_FILENAME: String = ""
  private var CERT_FILE: File = _
  private var CERT_URI: Option[String] = None

  @Mock
  private var submittedDependencyUploader: SubmittedDependencyUploader = _
  @Mock
  private var submittedResourcesSecretPlugin: InitContainerResourceStagingServerSecretPlugin = _

  before {
    MockitoAnnotations.initMocks(this)
    TRUSTSTORE_FILENAME = createTempFile(".jks")
    TRUSTSTORE_FILE = new File(TRUSTSTORE_FILENAME)
    TRUSTSTORE_URI = Some(TRUSTSTORE_FILENAME)
    CERT_FILENAME = createTempFile("pem")
    CERT_FILE = new File(CERT_FILENAME)
    CERT_URI = Some(CERT_FILENAME)
    RSS_WITH_SSL_SECRET =
      (RSS_SECRET.asScala ++ Map(
        INIT_CONTAINER_STAGING_SERVER_TRUSTSTORE_SECRET_KEY ->
          BaseEncoding.base64().encode(Files.toByteArray(TRUSTSTORE_FILE)),
        INIT_CONTAINER_STAGING_SERVER_CLIENT_CERT_SECRET_KEY ->
          BaseEncoding.base64().encode(Files.toByteArray(CERT_FILE))
      )).asJava
    when(submittedDependencyUploader.uploadJars()).thenReturn(
      SubmittedResourceIdAndSecret(JARS_RESOURCE_ID, JARS_SECRET)
    )
    when(submittedDependencyUploader.uploadFiles()).thenReturn(
      SubmittedResourceIdAndSecret(FILES_RESOURCE_ID, FILES_SECRET)
    )
    when(submittedResourcesSecretPlugin.addResourceStagingServerSecretVolumeToPod(
      any[Pod])).thenAnswer(new Answer[Pod] {
      override def answer(invocation: InvocationOnMock) : Pod = {
        val pod = invocation.getArgumentAt(0, classOf[Pod])
        new PodBuilder(pod)
          .withNewMetadata()
          .addToLabels("mountedSecret", "true")
          .endMetadata()
          .withNewSpec().endSpec()
          .build()
      }})
    when(submittedResourcesSecretPlugin.mountResourceStagingServerSecretIntoInitContainer(
      any[Container])).thenAnswer(new Answer[Container] {
      override def answer(invocation: InvocationOnMock) : Container = {
        val con = invocation.getArgumentAt(0, classOf[Container])
        new ContainerBuilder(con).withName("mountedSecret").build()
      }})
  }
  after {
    TRUSTSTORE_FILE.delete()
    CERT_FILE.delete()
  }
  test ("testing vanilla prepareInitContainer on resources and properties") {
    val submittedResourceStep = new SubmittedResourcesInitContainerConfigurationStep(
      RESOURCE_SECRET_NAME,
      STAGING_SERVER_URI,
      SECRET_MOUNT_PATH,
      false,
      None,
      None,
      None,
      None,
      submittedDependencyUploader,
      submittedResourcesSecretPlugin
    )
    val returnedInitContainer =
      submittedResourceStep.configureInitContainer(InitContainerSpec(
        Map.empty[String, String],
        Map.empty[String, String],
        new Container(),
        new Container(),
        new Pod(),
        Seq.empty[HasMetadata]))
    assert(returnedInitContainer.initContainer.getName === "mountedSecret")
    assert(returnedInitContainer.podToInitialize.getMetadata.getLabels.asScala
      === Map("mountedSecret" -> "true"))
    assert(returnedInitContainer.initContainerDependentResources.length == 1)
    val secret = returnedInitContainer.initContainerDependentResources.head.asInstanceOf[Secret]
    assert(secret.getData === RSS_SECRET)
    assert(secret.getMetadata.getName == RESOURCE_SECRET_NAME)
    val expectedinitContainerProperties = Map(
      RESOURCE_STAGING_SERVER_URI.key -> STAGING_SERVER_URI,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> JARS_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY",
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> FILES_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY",
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> false.toString)
    assert(returnedInitContainer.initContainerProperties === expectedinitContainerProperties)
    assert(returnedInitContainer.additionalDriverSparkConf ===
      Map(
        EXECUTOR_INIT_CONTAINER_SECRET.key -> RESOURCE_SECRET_NAME,
        EXECUTOR_INIT_CONTAINER_SECRET_MOUNT_DIR.key -> SECRET_MOUNT_PATH))
  }

  test ("testing prepareInitContainer w/ CERT and TrustStore Files w/o SSL") {
    val submittedResourceStep = new SubmittedResourcesInitContainerConfigurationStep(
      RESOURCE_SECRET_NAME,
      STAGING_SERVER_URI,
      SECRET_MOUNT_PATH,
      false,
      TRUSTSTORE_URI,
      CERT_URI,
      Some(TRUSTSTORE_PASS),
      Some(TRUSTSTORE_TYPE),
      submittedDependencyUploader,
      submittedResourcesSecretPlugin
    )
    val returnedInitContainer =
      submittedResourceStep.configureInitContainer(InitContainerSpec(
        Map.empty[String, String],
        Map.empty[String, String],
        new Container(),
        new Container(),
        new Pod(),
        Seq.empty[HasMetadata]))
    val expectedinitContainerProperties = Map(
      RESOURCE_STAGING_SERVER_URI.key -> STAGING_SERVER_URI,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> JARS_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY",
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> FILES_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY",
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> false.toString,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD.key -> TRUSTSTORE_PASS,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE.key -> TRUSTSTORE_TYPE,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_STAGING_SERVER_TRUSTSTORE_SECRET_KEY",
      RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_STAGING_SERVER_CLIENT_CERT_SECRET_KEY"
    )
    assert(returnedInitContainer.initContainerProperties === expectedinitContainerProperties)
    assert(returnedInitContainer.initContainerDependentResources.length == 1)
    val secret = returnedInitContainer.initContainerDependentResources.head.asInstanceOf[Secret]
    assert(secret.getData === RSS_WITH_SSL_SECRET)
    assert(secret.getMetadata.getName == RESOURCE_SECRET_NAME)

  }

  test ("testing prepareInitContainer w/ local CERT and TrustStore Files w/o SSL") {
    val LOCAL_TRUST_FILE = "local:///tmp/trust.jsk"
    val LOCAL_CERT_FILE = "local:///tmp/cert.pem"
    val submittedResourceStep = new SubmittedResourcesInitContainerConfigurationStep(
      RESOURCE_SECRET_NAME,
      STAGING_SERVER_URI,
      SECRET_MOUNT_PATH,
      false,
      Some(LOCAL_TRUST_FILE),
      Some(LOCAL_CERT_FILE),
      Some(TRUSTSTORE_PASS),
      Some(TRUSTSTORE_TYPE),
      submittedDependencyUploader,
      submittedResourcesSecretPlugin
    )
    val returnedInitContainer =
      submittedResourceStep.configureInitContainer(InitContainerSpec(
        Map.empty[String, String],
        Map.empty[String, String],
        new Container(),
        new Container(),
        new Pod(),
        Seq.empty[HasMetadata]))
    val expectedinitContainerProperties = Map(
      RESOURCE_STAGING_SERVER_URI.key -> STAGING_SERVER_URI,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> JARS_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY",
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> FILES_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        s"$SECRET_MOUNT_PATH/$INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY",
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> false.toString,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD.key -> TRUSTSTORE_PASS,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE.key -> TRUSTSTORE_TYPE,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE.key ->
        "/tmp/trust.jsk",
      RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM.key ->
        "/tmp/cert.pem"
    )
    assert(returnedInitContainer.initContainerProperties === expectedinitContainerProperties)
    assert(returnedInitContainer.initContainerDependentResources.length == 1)
    val secret = returnedInitContainer.initContainerDependentResources.head.asInstanceOf[Secret]
    assert(secret.getData === RSS_SECRET)
    assert(secret.getMetadata.getName == RESOURCE_SECRET_NAME)
  }
  private def createTempFile(extension: String): String = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}.$extension")
    Files.write(UUID.randomUUID().toString, file, Charsets.UTF_8)
    file.getAbsolutePath
  }
}
