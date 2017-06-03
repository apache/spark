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
package org.apache.spark.deploy.kubernetes.submit

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import okhttp3.RequestBody
import okio.Okio
import org.mockito.{ArgumentCaptor, Mockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import retrofit2.{Call, Response}

import org.apache.spark.{SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.deploy.rest.kubernetes.{ResourceStagingServiceRetrofit, RetrofitClientFactory, StagedResourcesOwner}
import org.apache.spark.util.Utils

private[spark] class SubmittedDependencyUploaderSuite extends SparkFunSuite with BeforeAndAfter {
  import SubmittedDependencyUploaderSuite.createTempFile

  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)
  private val BASE_64 = BaseEncoding.base64()
  private val APP_ID = "app-id"
  private val LABELS = Map("label1" -> "label1value", "label2" -> "label2value")
  private val NAMESPACE = "namespace"
  private val STAGING_SERVER_URI = "http://localhost:8000"
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
  private val CLIENT_KEY_FILE = createTempFile("pem")
  private val CLIENT_CERT_FILE = createTempFile("pem")
  private val OAUTH_TOKEN = "token"
  private var retrofitClientFactory: RetrofitClientFactory = _
  private var retrofitClient: ResourceStagingServiceRetrofit = _
  private var resourcesOwnerCaptor: ArgumentCaptor[RequestBody] = _
  private var resourcesDataCaptor: ArgumentCaptor[RequestBody] = _

  private var dependencyUploaderUnderTest: SubmittedDependencyUploader = _

  before {
    resourcesOwnerCaptor = ArgumentCaptor.forClass(classOf[RequestBody])
    resourcesDataCaptor = ArgumentCaptor.forClass(classOf[RequestBody])
    retrofitClientFactory = mock[RetrofitClientFactory]
    retrofitClient = mock[ResourceStagingServiceRetrofit]
    Mockito.when(
      retrofitClientFactory.createRetrofitClient(
        STAGING_SERVER_URI, classOf[ResourceStagingServiceRetrofit], STAGING_SERVER_SSL_OPTIONS))
      .thenReturn(retrofitClient)
    val responseCall = mock[Call[SubmittedResourceIdAndSecret]]
    Mockito.when(responseCall.execute()).thenReturn(
        Response.success(SubmittedResourceIdAndSecret("resourceId", "resourceSecret")))
    Mockito.when(retrofitClient.uploadResources(
      resourcesDataCaptor.capture(), resourcesOwnerCaptor.capture()))
      .thenReturn(responseCall)
    dependencyUploaderUnderTest = new SubmittedDependencyUploaderImpl(
      APP_ID,
      LABELS,
      NAMESPACE,
      STAGING_SERVER_URI,
      JARS,
      FILES,
      STAGING_SERVER_SSL_OPTIONS,
      retrofitClientFactory)
  }

  test("Uploading jars should contact the staging server with the appropriate parameters") {
    dependencyUploaderUnderTest.uploadJars()
    testUploadSendsCorrectFiles(LOCAL_JARS)
  }

  test("Uploading files should contact the staging server with the appropriate parameters") {
    dependencyUploaderUnderTest.uploadFiles()
    testUploadSendsCorrectFiles(LOCAL_FILES)
  }

  private def testUploadSendsCorrectFiles(expectedFiles: Seq[String]) = {
    val resourceOwnerString = new String(
        requestBodyBytes(resourcesOwnerCaptor.getValue), Charsets.UTF_8)
    val resourceOwner = OBJECT_MAPPER.readValue(resourceOwnerString, classOf[StagedResourcesOwner])
    assert(resourceOwner.ownerLabels === LABELS)
    assert(resourceOwner.ownerNamespace === NAMESPACE)
    val unpackedFilesDir = Utils.createTempDir(namePrefix = "test-unpacked-files")
    val compressedBytesInput = new ByteArrayInputStream(
        requestBodyBytes(resourcesDataCaptor.getValue()))
    CompressionUtils.unpackTarStreamToDirectory(compressedBytesInput, unpackedFilesDir)
    val writtenFiles = unpackedFilesDir.listFiles
    assert(writtenFiles.size === expectedFiles.size)

    expectedFiles.map(new File(_)).foreach { expectedFile =>
      val maybeWrittenFile = writtenFiles.find(_.getName == expectedFile.getName)
      assert(maybeWrittenFile.isDefined)
      maybeWrittenFile.foreach { writtenFile =>
        val writtenFileBytes = Files.toByteArray(writtenFile)
        val expectedFileBytes = Files.toByteArray(expectedFile)
        assert(expectedFileBytes.toSeq === writtenFileBytes.toSeq)
      }
    }
  }

  private def requestBodyBytes(requestBody: RequestBody): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream()) { outputStream =>
      Utils.tryWithResource(Okio.sink(outputStream)) { sink =>
        Utils.tryWithResource(Okio.buffer(sink)) { bufferedSink =>
          try {
            requestBody.writeTo(bufferedSink)
          } finally {
            bufferedSink.flush()
          }
        }
      }
      outputStream.toByteArray
    }
  }
}

private object SubmittedDependencyUploaderSuite {
  def createTempFile(extension: String): String = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}.$extension")
    Files.write(UUID.randomUUID().toString, file, Charsets.UTF_8)
    file.getAbsolutePath
  }
}
