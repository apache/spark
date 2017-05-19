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
package org.apache.spark.deploy.rest.kubernetes.v2

import java.io.{ByteArrayOutputStream, File}
import java.util.UUID
import javax.ws.rs.core

import com.google.common.base.Charsets
import com.google.common.io.Files
import okhttp3.{MediaType, ResponseBody}
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{doAnswer, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import retrofit2.{Call, Callback, Response}

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf, SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.util.Utils

class KubernetesSparkDependencyDownloadInitContainerSuite
    extends SparkFunSuite with BeforeAndAfter {
  import KubernetesSparkDependencyDownloadInitContainerSuite.createTempFile
  private val STAGING_SERVER_URI = "http://localhost:8000"
  private val TRUSTSTORE_FILE = new File(createTempFile(".jks"))
  private val TRUSTSTORE_PASSWORD = "trustStorePassword"
  private val TRUSTSTORE_TYPE = "jks"
  private val STAGING_SERVER_SSL_OPTIONS = SSLOptions(
    enabled = true,
    trustStore = Some(TRUSTSTORE_FILE),
    trustStorePassword = Some(TRUSTSTORE_PASSWORD),
    trustStoreType = Some(TRUSTSTORE_TYPE))
  private val JARS = Seq(createTempFile("jar"), createTempFile("jar"))
  private val FILES = Seq(createTempFile("txt"), createTempFile("csv"))
  private val DOWNLOAD_JARS_SECRET_LOCATION = createTempFile("txt")
  private val DOWNLOAD_FILES_SECRET_LOCATION = createTempFile("txt")
  private val JARS_RESOURCE_ID = "jarsId"
  private val FILES_RESOURCE_ID = "filesId"

  private var downloadJarsDir: File = _
  private var downloadFilesDir: File = _
  private var downloadJarsSecretValue: String = _
  private var downloadFilesSecretValue: String = _
  private var jarsCompressedBytes: Array[Byte] = _
  private var filesCompressedBytes: Array[Byte] = _
  private var retrofitClientFactory: RetrofitClientFactory = _
  private var retrofitClient: ResourceStagingServiceRetrofit = _
  private var fileFetcher: FileFetcher = _

  override def beforeAll(): Unit = {
    jarsCompressedBytes = compressPathsToBytes(JARS)
    filesCompressedBytes = compressPathsToBytes(FILES)
    downloadJarsSecretValue = Files.toString(
      new File(DOWNLOAD_JARS_SECRET_LOCATION), Charsets.UTF_8)
    downloadFilesSecretValue = Files.toString(
      new File(DOWNLOAD_FILES_SECRET_LOCATION), Charsets.UTF_8)
  }

  before {
    downloadJarsDir = Utils.createTempDir()
    downloadFilesDir = Utils.createTempDir()
    retrofitClientFactory = mock[RetrofitClientFactory]
    retrofitClient = mock[ResourceStagingServiceRetrofit]
    fileFetcher = mock[FileFetcher]
    when(retrofitClientFactory.createRetrofitClient(
        STAGING_SERVER_URI, classOf[ResourceStagingServiceRetrofit], STAGING_SERVER_SSL_OPTIONS))
      .thenReturn(retrofitClient)
  }

  after {
    downloadJarsDir.delete()
    downloadFilesDir.delete()
  }

  test("Downloads from resource staging server should unpack response body to directories") {
    val downloadJarsCall = mock[Call[ResponseBody]]
    val downloadFilesCall = mock[Call[ResponseBody]]
    val sparkConf = getSparkConfForResourceStagingServerDownloads
    val initContainerUnderTest = new KubernetesSparkDependencyDownloadInitContainer(
      sparkConf,
      retrofitClientFactory,
      fileFetcher,
      resourceStagingServerSslOptions = STAGING_SERVER_SSL_OPTIONS)
    when(retrofitClient.downloadResources(JARS_RESOURCE_ID, downloadJarsSecretValue))
      .thenReturn(downloadJarsCall)
    when(retrofitClient.downloadResources(FILES_RESOURCE_ID, downloadFilesSecretValue))
      .thenReturn(downloadFilesCall)
    val jarsResponseBody = ResponseBody.create(
      MediaType.parse(core.MediaType.APPLICATION_OCTET_STREAM), jarsCompressedBytes)
    val filesResponseBody = ResponseBody.create(
      MediaType.parse(core.MediaType.APPLICATION_OCTET_STREAM), filesCompressedBytes)
    doAnswer(new InvokeCallbackAnswer(downloadJarsCall, jarsResponseBody))
      .when(downloadJarsCall)
      .enqueue(any())
    doAnswer(new InvokeCallbackAnswer(downloadFilesCall, filesResponseBody))
      .when(downloadFilesCall)
      .enqueue(any())
    initContainerUnderTest.run()
    checkWrittenFilesAreTheSameAsOriginal(JARS, downloadJarsDir)
    checkWrittenFilesAreTheSameAsOriginal(FILES, downloadFilesDir)
    Mockito.verifyZeroInteractions(fileFetcher)
  }

  test("Downloads from remote server should invoke the file fetcher") {
    val sparkConf = getSparkConfForRemoteFileDownloads
    val initContainerUnderTest = new KubernetesSparkDependencyDownloadInitContainer(
      sparkConf,
      retrofitClientFactory,
      fileFetcher,
      resourceStagingServerSslOptions = STAGING_SERVER_SSL_OPTIONS)
    initContainerUnderTest.run()
    Mockito.verify(fileFetcher).fetchFile("http://localhost:9000/jar1.jar", downloadJarsDir)
    Mockito.verify(fileFetcher).fetchFile("hdfs://localhost:9000/jar2.jar", downloadJarsDir)
    Mockito.verify(fileFetcher).fetchFile("http://localhost:9000/file.txt", downloadFilesDir)

  }

  private def getSparkConfForResourceStagingServerDownloads: SparkConf = {
    new SparkConf(true)
      .set(RESOURCE_STAGING_SERVER_URI, STAGING_SERVER_URI)
      .set(INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER, JARS_RESOURCE_ID)
      .set(INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION, DOWNLOAD_JARS_SECRET_LOCATION)
      .set(INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER, FILES_RESOURCE_ID)
      .set(INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION, DOWNLOAD_FILES_SECRET_LOCATION)
      .set(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION, downloadJarsDir.getAbsolutePath)
      .set(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION, downloadFilesDir.getAbsolutePath)
      .set(RESOURCE_STAGING_SERVER_SSL_ENABLED, true)
      .set(RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE, TRUSTSTORE_FILE.getAbsolutePath)
      .set(RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD)
      .set(RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
  }

  private def getSparkConfForRemoteFileDownloads: SparkConf = {
    new SparkConf(true)
      .set(INIT_CONTAINER_REMOTE_JARS,
        "http://localhost:9000/jar1.jar,hdfs://localhost:9000/jar2.jar")
      .set(INIT_CONTAINER_REMOTE_FILES,
        "http://localhost:9000/file.txt")
      .set(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION, downloadJarsDir.getAbsolutePath)
      .set(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION, downloadFilesDir.getAbsolutePath)
  }

  private def checkWrittenFilesAreTheSameAsOriginal(
      originalFiles: Iterable[String], downloadDir: File): Unit = {
    originalFiles.map(new File(_)).foreach { file =>
      val writtenFile = new File(downloadDir, file.getName)
      assert(writtenFile.exists)
      val originalJarContents = Seq(Files.toByteArray(file): _*)
      val writtenJarContents = Seq(Files.toByteArray(writtenFile): _*)
      assert(writtenJarContents === originalJarContents)
    }
  }

  private def compressPathsToBytes(paths: Iterable[String]): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream()) { compressedBytes =>
      CompressionUtils.writeTarGzipToStream (compressedBytes, paths)
      compressedBytes.toByteArray
    }
  }
}

private object KubernetesSparkDependencyDownloadInitContainerSuite {
  def createTempFile(extension: String): String = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}.$extension")
    Files.write(UUID.randomUUID().toString, file, Charsets.UTF_8)
    file.getAbsolutePath
  }
}

private class InvokeCallbackAnswer(call: Call[ResponseBody], responseBody: ResponseBody)
    extends Answer[Unit] {
  override def answer(invocationOnMock: InvocationOnMock): Unit = {
    val callback = invocationOnMock.getArgumentAt(0, classOf[Callback[ResponseBody]])
    val response = Response.success(responseBody)
    callback.onResponse(call, response)
  }
}
