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
package org.apache.spark.deploy.rest.kubernetes.v1

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonSubTypes, JsonTypeInfo}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.deploy.rest.{SubmitRestProtocolRequest, SubmitRestProtocolResponse}
import org.apache.spark.util.Utils

case class KubernetesCredentials(
    oauthToken: Option[String],
    caCertDataBase64: Option[String],
    clientKeyDataBase64: Option[String],
    clientCertDataBase64: Option[String])

case class KubernetesCreateSubmissionRequest(
    appResource: AppResource,
    mainClass: String,
    appArgs: Array[String],
    sparkProperties: Map[String, String],
    secret: String,
    driverPodKubernetesCredentials: KubernetesCredentials,
    uploadedJarsBase64Contents: TarGzippedData,
    uploadedFilesBase64Contents: TarGzippedData) extends SubmitRestProtocolRequest {
  @JsonIgnore
  override val messageType: String = s"kubernetes.v1.${Utils.getFormattedClassName(this)}"
  override val action = messageType
  message = "create"
  clientSparkVersion = SPARK_VERSION
}

case class TarGzippedData(
  dataBase64: String,
  blockSize: Int = 10240,
  recordSize: Int = 512,
  encoding: String
)

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
@JsonSubTypes(value = Array(
  new JsonSubTypes.Type(value = classOf[UploadedAppResource], name = "UploadedAppResource"),
  new JsonSubTypes.Type(value = classOf[ContainerAppResource], name = "ContainerLocalAppResource"),
  new JsonSubTypes.Type(value = classOf[RemoteAppResource], name = "RemoteAppResource")))
abstract class AppResource

case class UploadedAppResource(
  resourceBase64Contents: String,
  name: String = "spark-app-resource") extends AppResource

case class ContainerAppResource(resourcePath: String) extends AppResource

case class RemoteAppResource(resource: String) extends AppResource

class PingResponse extends SubmitRestProtocolResponse {
  val text = "pong"
  message = "pong"
  serverSparkVersion = SPARK_VERSION
  @JsonIgnore
  override val messageType: String = s"kubernetes.v1.${Utils.getFormattedClassName(this)}"
  override val action: String = messageType
}

