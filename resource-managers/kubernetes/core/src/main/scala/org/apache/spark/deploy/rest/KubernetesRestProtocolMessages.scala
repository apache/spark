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
package org.apache.spark.deploy.rest

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

import org.apache.spark.SPARK_VERSION

// TODO: jars should probably be compressed. Shipping tarballs would be optimal.
case class KubernetesCreateSubmissionRequest(
  val appResource: AppResource,
  val mainClass: String,
  val appArgs: Array[String],
  val sparkProperties: Map[String, String],
  val secret: String,
  val uploadedDriverExtraClasspathBase64Contents: Option[TarGzippedData],
  val uploadedJarsBase64Contents: Option[TarGzippedData]) extends SubmitRestProtocolRequest {
  message = "create"
  clientSparkVersion = SPARK_VERSION
}

case class TarGzippedData(
  val dataBase64: String,
  val blockSize: Int = 10240,
  val recordSize: Int = 512,
  val encoding: String
)

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
@JsonSubTypes(value = Array(
  new JsonSubTypes.Type(value = classOf[UploadedAppResource], name = "UploadedAppResource"),
  new JsonSubTypes.Type(value = classOf[RemoteAppResource], name = "RemoteAppResource")))
abstract class AppResource

case class UploadedAppResource(
  resourceBase64Contents: String,
  name: String = "spark-app-resource") extends AppResource

case class RemoteAppResource(resource: String) extends AppResource

class PingResponse extends SubmitRestProtocolResponse {
  val text = "pong"
  message = "pong"
  serverSparkVersion = SPARK_VERSION
}

