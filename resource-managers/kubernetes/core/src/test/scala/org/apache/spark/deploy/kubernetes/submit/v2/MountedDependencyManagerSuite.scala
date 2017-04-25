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

import java.io.{ByteArrayOutputStream, File, StringReader}
import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, Container, Pod, PodBuilder, SecretBuilder}
import okhttp3.RequestBody
import okio.Okio
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import retrofit2.{Call, Response}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.rest.kubernetes.v2.{ResourceStagingServiceRetrofit, RetrofitClientFactory, StagedResourceIdentifier}
import org.apache.spark.util.Utils

private[spark] class MountedDependencyManagerSuite extends SparkFunSuite with BeforeAndAfter {
  import MountedDependencyManagerSuite.createTempFile

  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)
  private val APP_ID = "app-id"
  private val LABELS = Map("label1" -> "label1value", "label2" -> "label2value")
  private val NAMESPACE = "namespace"
  private val STAGING_SERVER_URI = "http://localhost:8000"
  private val INIT_CONTAINER_IMAGE = "spark-driver-init:latest"
  private val JARS_DOWNLOAD_PATH = DRIVER_LOCAL_JARS_DOWNLOAD_LOCATION.defaultValue.get
  private val FILES_DOWNLOAD_PATH = DRIVER_LOCAL_FILES_DOWNLOAD_LOCATION.defaultValue.get
  private val DOWNLOAD_TIMEOUT_MINUTES = 5
  private val LOCAL_JARS = Seq(createTempFile("jar"), createTempFile("jar"))
  private val JARS = Seq("hdfs://localhost:9000/jars/jar1.jar",
    s"file://${LOCAL_JARS.head}",
    LOCAL_JARS(1))
  private val LOCAL_FILES = Seq(createTempFile("txt"))
  private val FILES = Seq("hdfs://localhost:9000/files/file1.txt",
    LOCAL_FILES.head)
  private val TRUSTSTORE_FILE = new File(createTempFile(".jks"))
  private val TRUSTSTORE_PASSWORD = "trustStorePassword"
  private val TRUSTSTORE_TYPE = "jks"
  private val STAGING_SERVER_SSL_OPTIONS = SSLOptions(
    enabled = true,
    trustStore = Some(TRUSTSTORE_FILE),
    trustStorePassword = Some(TRUSTSTORE_PASSWORD),
    trustStoreType = Some(TRUSTSTORE_TYPE))
  private val JARS_RESOURCE_ID = "jarsId"
  private val JARS_SECRET = "jarsSecret"
  private val FILES_RESOURCE_ID = "filesId"
  private val FILES_SECRET = "filesSecret"
  private var retrofitClientFactory: RetrofitClientFactory = _
  private var retrofitClient: ResourceStagingServiceRetrofit = _

  private var dependencyManagerUnderTest: MountedDependencyManager = _

  before {
    retrofitClientFactory = mock[RetrofitClientFactory]
    retrofitClient = mock[ResourceStagingServiceRetrofit]
    Mockito.when(
      retrofitClientFactory.createRetrofitClient(
        STAGING_SERVER_URI, classOf[ResourceStagingServiceRetrofit], STAGING_SERVER_SSL_OPTIONS))
      .thenReturn(retrofitClient)
    dependencyManagerUnderTest = new MountedDependencyManagerImpl(
      APP_ID,
      LABELS,
      NAMESPACE,
      STAGING_SERVER_URI,
      INIT_CONTAINER_IMAGE,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      DOWNLOAD_TIMEOUT_MINUTES,
      JARS,
      FILES,
      STAGING_SERVER_SSL_OPTIONS,
      retrofitClientFactory)
  }

  test("Uploading jars should contact the staging server with the appropriate parameters") {
    val capturingArgumentsAnswer = new UploadDependenciesArgumentsCapturingAnswer(
      StagedResourceIdentifier("resourceId", "resourceSecret"))
    Mockito.when(retrofitClient.uploadResources(any(), any(), any(), any()))
      .thenAnswer(capturingArgumentsAnswer)
    dependencyManagerUnderTest.uploadJars()
    testUploadSendsCorrectFiles(LOCAL_JARS, capturingArgumentsAnswer)
  }

  test("Uploading files should contact the staging server with the appropriate parameters") {
    val capturingArgumentsAnswer = new UploadDependenciesArgumentsCapturingAnswer(
      StagedResourceIdentifier("resourceId", "resourceSecret"))
    Mockito.when(retrofitClient.uploadResources(any(), any(), any(), any()))
      .thenAnswer(capturingArgumentsAnswer)
    dependencyManagerUnderTest.uploadFiles()
    testUploadSendsCorrectFiles(LOCAL_FILES, capturingArgumentsAnswer)
  }

  test("Init container secret should contain jars, files, and trustStore") {
    val jarsSecretBase64 = BaseEncoding.base64().encode(JARS_SECRET.getBytes(Charsets.UTF_8))
    val filesSecretBase64 = BaseEncoding.base64().encode(FILES_SECRET.getBytes(Charsets.UTF_8))
    val trustStoreBase64 = BaseEncoding.base64().encode(Files.toByteArray(TRUSTSTORE_FILE))
    val secret = dependencyManagerUnderTest.buildInitContainerSecret("jarsSecret", "filesSecret")
    assert(secret.getMetadata.getName === s"$APP_ID-spark-init")
    val expectedSecrets = Map(
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_KEY -> jarsSecretBase64,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_KEY -> filesSecretBase64,
      INIT_CONTAINER_TRUSTSTORE_SECRET_KEY -> trustStoreBase64)
    assert(secret.getData.asScala === expectedSecrets)
  }

  test("Init container config map should contain parameters for downloading from staging server") {
    val configMap = dependencyManagerUnderTest.buildInitContainerConfigMap(
      JARS_RESOURCE_ID, FILES_RESOURCE_ID)
    assert(configMap.getMetadata.getName === s"$APP_ID-init-properties")
    val propertiesRawString = configMap.getData.get(INIT_CONTAINER_CONFIG_MAP_KEY)
    assert(propertiesRawString != null)
    val propertiesReader = new StringReader(propertiesRawString)
    val properties = new Properties()
    properties.load(propertiesReader)
    val propertiesMap = properties.stringPropertyNames().asScala.map { prop =>
      (prop, properties.getProperty(prop))
    }.toMap
    val expectedProperties = Map[String, String](
      RESOURCE_STAGING_SERVER_URI.key -> STAGING_SERVER_URI,
      DRIVER_LOCAL_JARS_DOWNLOAD_LOCATION.key -> JARS_DOWNLOAD_PATH,
      DRIVER_LOCAL_FILES_DOWNLOAD_LOCATION.key -> FILES_DOWNLOAD_PATH,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> JARS_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        INIT_CONTAINER_DOWNLOAD_JARS_SECRET_PATH,
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> FILES_RESOURCE_ID,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        INIT_CONTAINER_DOWNLOAD_FILES_SECRET_PATH,
      DRIVER_MOUNT_DEPENDENCIES_INIT_TIMEOUT.key -> s"${DOWNLOAD_TIMEOUT_MINUTES}m",
      RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE.key -> INIT_CONTAINER_TRUSTSTORE_PATH,
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> "true",
      RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD.key -> TRUSTSTORE_PASSWORD,
      RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE.key -> TRUSTSTORE_TYPE)
    assert(propertiesMap === expectedProperties)
  }

  test("Resolving jars should map local paths to their mounted counterparts") {
    val resolvedJars = dependencyManagerUnderTest.resolveSparkJars()
    val expectedResolvedJars = Seq(
      "hdfs://localhost:9000/jars/jar1.jar",
      s"file://$JARS_DOWNLOAD_PATH/${new File(JARS(1)).getName}",
      s"file://$JARS_DOWNLOAD_PATH/${new File(JARS(2)).getName}")
    assert(resolvedJars === expectedResolvedJars)
  }

  test("Resolving files should map local paths to their mounted counterparts") {
    val resolvedFiles = dependencyManagerUnderTest.resolveSparkFiles()
    val expectedResolvedFiles = Seq(
      "hdfs://localhost:9000/files/file1.txt",
      s"file://$FILES_DOWNLOAD_PATH/${new File(FILES(1)).getName}")
    assert(resolvedFiles === expectedResolvedFiles)
  }

  test("Downloading init container should be added to pod") {
    val driverPod = configureDriverPod()
    val podAnnotations = driverPod.getMetadata.getAnnotations
    assert(podAnnotations.size === 1)
    val initContainerRawAnnotation = podAnnotations.get(INIT_CONTAINER_ANNOTATION)
    val initContainers = OBJECT_MAPPER.readValue(
      initContainerRawAnnotation, classOf[Array[Container]])
    assert(initContainers.size === 1)
    val initContainer = initContainers.head
    assert(initContainer.getName === "spark-driver-init")
    assert(initContainer.getImage === INIT_CONTAINER_IMAGE)
    assert(initContainer.getImagePullPolicy === "IfNotPresent")
    val volumeMounts = initContainer.getVolumeMounts
      .asScala
      .map(mount => (mount.getName, mount.getMountPath))
      .toMap
    val expectedVolumeMounts = Map[String, String](
      DOWNLOAD_JARS_VOLUME_NAME -> JARS_DOWNLOAD_PATH,
      DOWNLOAD_FILES_VOLUME_NAME -> FILES_DOWNLOAD_PATH,
      INIT_CONTAINER_PROPERTIES_FILE_VOLUME -> INIT_CONTAINER_PROPERTIES_FILE_MOUNT_PATH,
      INIT_CONTAINER_SECRETS_VOLUME_NAME -> INIT_CONTAINER_SECRETS_VOLUME_MOUNT_PATH)
    assert(volumeMounts === expectedVolumeMounts)
  }

  test("Driver pod should have added volumes and volume mounts for file downloads") {
    val driverPod = configureDriverPod()
    val volumes = driverPod.getSpec.getVolumes.asScala.map(volume => (volume.getName, volume)).toMap
    val initContainerPropertiesVolume = volumes(INIT_CONTAINER_PROPERTIES_FILE_VOLUME).getConfigMap
    assert(initContainerPropertiesVolume != null)
    assert(initContainerPropertiesVolume.getName === "config")
    assert(initContainerPropertiesVolume.getItems.asScala.exists { keyToPath =>
      keyToPath.getKey == INIT_CONTAINER_CONFIG_MAP_KEY &&
        keyToPath.getPath == INIT_CONTAINER_PROPERTIES_FILE_NAME
    })
    val jarsVolume = volumes(DOWNLOAD_JARS_VOLUME_NAME)
    assert(jarsVolume.getEmptyDir != null)
    val filesVolume = volumes(DOWNLOAD_FILES_VOLUME_NAME)
    assert(filesVolume.getEmptyDir != null)
    val initContainerSecretVolume = volumes(INIT_CONTAINER_SECRETS_VOLUME_NAME)
    assert(initContainerSecretVolume.getSecret != null)
    assert(initContainerSecretVolume.getSecret.getSecretName === "secret")
    val driverContainer = driverPod.getSpec
      .getContainers
      .asScala
      .find(_.getName == "driver-container").get
    val driverContainerVolumeMounts = driverContainer.getVolumeMounts
      .asScala
      .map(mount => (mount.getName, mount.getMountPath))
      .toMap
    val expectedVolumeMountNamesAndPaths = Map[String, String](
      DOWNLOAD_JARS_VOLUME_NAME -> JARS_DOWNLOAD_PATH,
      DOWNLOAD_FILES_VOLUME_NAME -> FILES_DOWNLOAD_PATH)
    assert(driverContainerVolumeMounts === expectedVolumeMountNamesAndPaths)
    val envs = driverContainer.getEnv
    assert(envs.size() === 1)
    assert(envs.asScala.head.getName === ENV_UPLOADED_JARS_DIR)
    assert(envs.asScala.head.getValue === JARS_DOWNLOAD_PATH)
  }

  private def configureDriverPod(): Pod = {
    val initContainerSecret = new SecretBuilder()
      .withNewMetadata().withName("secret").endMetadata()
      .addToData("datakey", "datavalue")
      .build()
    val initContainerConfigMap = new ConfigMapBuilder()
      .withNewMetadata().withName("config").endMetadata()
      .addToData("datakey", "datavalue")
      .build()
    val basePod = new PodBuilder()
      .withNewMetadata()
        .withName("driver-pod")
        .endMetadata()
      .withNewSpec()
        .addNewContainer()
          .withName("driver-container")
          .withImage("spark-driver:latest")
          .endContainer()
      .endSpec()
    val adjustedPod = dependencyManagerUnderTest.configurePodToMountLocalDependencies(
      "driver-container",
      initContainerSecret,
      initContainerConfigMap,
      basePod).build()
    adjustedPod
  }

  private def testUploadSendsCorrectFiles(
      expectedFiles: Seq[String],
      capturingArgumentsAnswer: UploadDependenciesArgumentsCapturingAnswer) = {
    val requestLabelsBytes = requestBodyBytes(capturingArgumentsAnswer.podLabelsArg)
    val requestLabelsString = new String(requestLabelsBytes, Charsets.UTF_8)
    val requestLabelsMap = OBJECT_MAPPER.readValue(
      requestLabelsString, classOf[Map[String, String]])
    assert(requestLabelsMap === LABELS)
    val requestNamespaceBytes = requestBodyBytes(capturingArgumentsAnswer.podNamespaceArg)
    val requestNamespaceString = new String(requestNamespaceBytes, Charsets.UTF_8)
    assert(requestNamespaceString === NAMESPACE)
    val localJarsTarStream = new ByteArrayOutputStream()
    CompressionUtils.writeTarGzipToStream(localJarsTarStream, expectedFiles)
    val requestResourceBytes = requestBodyBytes(capturingArgumentsAnswer.podResourcesArg)
    assert(requestResourceBytes.sameElements(localJarsTarStream.toByteArray))
  }

  private def requestBodyBytes(requestBody: RequestBody): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream()) { outputStream =>
      Utils.tryWithResource(Okio.sink(outputStream)) { sink =>
        Utils.tryWithResource(Okio.buffer(sink)) { bufferedSink =>
          requestBody.writeTo(bufferedSink)
        }
      }
      outputStream.toByteArray
    }
  }
}

private class UploadDependenciesArgumentsCapturingAnswer(returnValue: StagedResourceIdentifier)
    extends Answer[Call[StagedResourceIdentifier]] {

  var podLabelsArg: RequestBody = _
  var podNamespaceArg: RequestBody = _
  var podResourcesArg: RequestBody = _
  var kubernetesCredentialsArg: RequestBody = _

  override def answer(invocationOnMock: InvocationOnMock): Call[StagedResourceIdentifier] = {
    podLabelsArg = invocationOnMock.getArgumentAt(0, classOf[RequestBody])
    podNamespaceArg = invocationOnMock.getArgumentAt(1, classOf[RequestBody])
    podResourcesArg = invocationOnMock.getArgumentAt(2, classOf[RequestBody])
    kubernetesCredentialsArg = invocationOnMock.getArgumentAt(3, classOf[RequestBody])
    val responseCall = mock[Call[StagedResourceIdentifier]]
    Mockito.when(responseCall.execute()).thenReturn(Response.success(returnValue))
    responseCall
  }
}

private object MountedDependencyManagerSuite {
  def createTempFile(extension: String): String = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}.$extension")
    Files.write(UUID.randomUUID().toString, file, Charsets.UTF_8)
    file.getAbsolutePath
  }
}
