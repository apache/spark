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

import java.net.ServerSocket
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.ByteStreams
import okhttp3.{RequestBody, ResponseBody}
import org.scalatest.BeforeAndAfterAll
import retrofit2.Call

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.rest.KubernetesCredentials
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
class ResourceStagingServerSuite extends SparkFunSuite with BeforeAndAfterAll {

  private val serverPort = new ServerSocket(0).getLocalPort
  private val serviceImpl = new ResourceStagingServiceImpl(Utils.createTempDir())
  private val server = new ResourceStagingServer(serverPort, serviceImpl)
  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)

  override def beforeAll(): Unit = {
    server.start()
  }

  override def afterAll(): Unit = {
    server.stop()
  }

  test("Accept file and jar uploads and downloads") {
    val retrofitService = RetrofitUtils.createRetrofitClient(s"http://localhost:$serverPort/",
      classOf[ResourceStagingServiceRetrofit])
    val resourcesBytes = Array[Byte](1, 2, 3, 4)
    val labels = Map("label1" -> "label1Value", "label2" -> "label2value")
    val namespace = "namespace"
    val labelsJson = OBJECT_MAPPER.writer().writeValueAsString(labels)
    val resourcesRequestBody = RequestBody.create(
        okhttp3.MediaType.parse(MediaType.MULTIPART_FORM_DATA), resourcesBytes)
    val labelsRequestBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.APPLICATION_JSON), labelsJson)
    val namespaceRequestBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.TEXT_PLAIN), namespace)
    val kubernetesCredentials = KubernetesCredentials(Some("token"), Some("ca-cert"), None, None)
    val kubernetesCredentialsString = OBJECT_MAPPER.writer()
      .writeValueAsString(kubernetesCredentials)
    val kubernetesCredentialsBody = RequestBody.create(
        okhttp3.MediaType.parse(MediaType.APPLICATION_JSON), kubernetesCredentialsString)
    val uploadResponse = retrofitService.uploadResources(
      labelsRequestBody, namespaceRequestBody, resourcesRequestBody, kubernetesCredentialsBody)
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

}
