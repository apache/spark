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
package org.apache.spark.deploy.rest.kubernetes

import java.net.ServerSocket
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.ByteStreams
import okhttp3.{RequestBody, ResponseBody}
import org.eclipse.jetty.server.Server
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar.mock
import retrofit2.Call

import org.apache.spark.{SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Tests for {@link ResourceStagingServer} and its APIs. Note that this is not an end-to-end
 * integration test, and as such does not upload and download files in tar.gz as would be done
 * in production. Thus we use the retrofit clients directly despite the fact that in practice
 * we would likely want to create an opinionated abstraction on top of the retrofit client; we
 * can test this abstraction layer separately, however. This test is mainly for checking that
 * we've configured the Jetty server correctly and that the endpoints reached over HTTP can
 * receive streamed uploads and can stream downloads.
 */
class ResourceStagingServerSuite extends SparkFunSuite with BeforeAndAfter with Logging {

  private val MAX_SERVER_START_ATTEMPTS = 5
  private var serviceImpl: ResourceStagingService = _
  private var stagedResourcesCleaner: StagedResourcesCleaner = _
  private var server: Option[ResourceStagingServer] = None
  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)

  private val sslOptionsProvider = new SettableReferenceSslOptionsProvider()

  before {
    stagedResourcesCleaner = mock[StagedResourcesCleaner]
    serviceImpl = new ResourceStagingServiceImpl(
      new StagedResourcesStoreImpl(Utils.createTempDir()), stagedResourcesCleaner)
  }

  after {
    server.foreach { s =>
      try {
        s.stop()
      } catch {
        case e: Throwable =>
          log.warn("Failed to stop the resource staging server.", e)
      }
    }
    server = None
  }

  test("Accept file and jar uploads and downloads") {
    val serverPort = startServer()
    runUploadAndDownload(SSLOptions(), serverPort)
  }

  test("Enable SSL on the server") {
    val keyStoreAndTrustStore = SSLUtils.generateKeyStoreTrustStorePair(
      ipAddress = "127.0.0.1",
      keyStorePassword = "keyStore",
      keyPassword = "key",
      trustStorePassword = "trustStore")
    val sslOptions = SSLOptions(
      enabled = true,
      keyStore = Some(keyStoreAndTrustStore.keyStore),
      keyStorePassword = Some("keyStore"),
      keyPassword = Some("key"),
      trustStore = Some(keyStoreAndTrustStore.trustStore),
      trustStorePassword = Some("trustStore"))
    sslOptionsProvider.setOptions(sslOptions)
    val serverPort = startServer()
    runUploadAndDownload(sslOptions, serverPort)
  }

  private def runUploadAndDownload(sslOptions: SSLOptions, serverPort: Int): Unit = {
    val scheme = if (sslOptions.enabled) "https" else "http"
    val retrofitService = RetrofitClientFactoryImpl.createRetrofitClient(
      s"$scheme://127.0.0.1:$serverPort/",
      classOf[ResourceStagingServiceRetrofit],
      sslOptions)
    val resourcesBytes = Array[Byte](1, 2, 3, 4)
    val labels = Map("label1" -> "label1Value", "label2" -> "label2value")
    val namespace = "namespace"
    val resourcesOwner = StagedResourcesOwner(
      ownerLabels = labels,
      ownerNamespace = namespace,
      ownerType = StagedResourcesOwnerType.Pod)
    val resourcesOwnerJson = OBJECT_MAPPER.writeValueAsString(resourcesOwner)
    val resourcesOwnerRequestBody = RequestBody.create(
        okhttp3.MediaType.parse(MediaType.APPLICATION_JSON), resourcesOwnerJson)
    val resourcesRequestBody = RequestBody.create(
        okhttp3.MediaType.parse(MediaType.MULTIPART_FORM_DATA), resourcesBytes)
    val uploadResponse = retrofitService.uploadResources(
      resourcesRequestBody, resourcesOwnerRequestBody)
    val resourceIdentifier = getTypedResponseResult(uploadResponse)
    checkResponseBodyBytesMatches(
      retrofitService.downloadResources(
        resourceIdentifier.resourceId, resourceIdentifier.resourceSecret), resourcesBytes)
  }

  private def getTypedResponseResult[T](call: Call[T]): T = {
    val response = call.execute()
    assert(response.code() >= 200 && response.code() < 300, Option(response.errorBody())
      .map(_.string())
      .getOrElse("Error executing HTTP request, but error body was not provided."))
    val callResult = response.body()
    assert(callResult != null)
    callResult
  }

  private def checkResponseBodyBytesMatches(call: Call[ResponseBody], bytes: Array[Byte]): Unit = {
    val responseBody = getTypedResponseResult(call)
    val downloadedBytes = ByteStreams.toByteArray(responseBody.byteStream())
    assert(downloadedBytes.toSeq === bytes)
  }

  private def startServer(): Int = {
    var currentAttempt = 0
    var successfulStart = false
    var latestServerPort = new ServerSocket(0).getLocalPort
    while (currentAttempt < MAX_SERVER_START_ATTEMPTS && !successfulStart) {
      val newServer = new ResourceStagingServer(latestServerPort, serviceImpl, sslOptionsProvider)
      try {
        newServer.start()
        successfulStart = true
        server = Some(newServer)
      } catch {
        case e: Throwable =>
          try {
            newServer.stop()
          } catch {
            case e1: Throwable =>
              log.warn("Failed to stop a resource staging server that failed to start.", e1)
          }

          if (Utils.isBindCollision(e)) {
            currentAttempt += 1
            latestServerPort = latestServerPort + 1
            if (currentAttempt == MAX_SERVER_START_ATTEMPTS) {
              throw new RuntimeException(s"Failed to bind to a random port" +
                s" $MAX_SERVER_START_ATTEMPTS times. Last attempted port: $latestServerPort", e)
            } else {
              logWarning(s"Attempt $currentAttempt/$MAX_SERVER_START_ATTEMPTS failed to start" +
                s" server on port $latestServerPort.", e)
            }
          } else {
            throw e
          }
      }
    }
    logInfo(s"Started resource staging server on port $latestServerPort.")
    latestServerPort
  }
}

private class SettableReferenceSslOptionsProvider extends ResourceStagingServerSslOptionsProvider {
  private var options = SSLOptions()

  def setOptions(newOptions: SSLOptions): Unit = {
    this.options = newOptions
  }

  override def getSslOptions: SSLOptions = options
}
