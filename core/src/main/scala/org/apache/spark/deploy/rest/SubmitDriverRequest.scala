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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.annotation.{JsonProperty, JsonIgnore, JsonInclude}

/**
 * A request to submit a driver in the REST application submission protocol.
 */
class SubmitDriverRequest extends SubmitRestProtocolRequest {
  var appName: String = null
  var appResource: String = null
  var mainClass: String = null
  var jars: String = null
  var files: String = null
  var pyFiles: String = null
  var driverMemory: String = null
  var driverCores: String = null
  var driverExtraJavaOptions: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var superviseDriver: String = null
  var executorMemory: String = null
  var totalExecutorCores: String = null

  // Special fields
  @JsonProperty("appArgs")
  private val _appArgs = new ArrayBuffer[String]
  @JsonProperty("sparkProperties")
  private val _sparkProperties = new mutable.HashMap[String, String]
  @JsonProperty("environmentVariables")
  private val _envVars = new mutable.HashMap[String, String]

  def appArgs: Array[String] = _appArgs.toArray
  def sparkProperties: Map[String, String] = _sparkProperties.toMap
  def environmentVariables: Map[String, String] = _envVars.toMap

  def addAppArg(s: String): this.type = { _appArgs += s; this }
  def setSparkProperty(k: String, v: String): this.type = { _sparkProperties(k) = v; this }
  def setEnvironmentVariable(k: String, v: String): this.type = { _envVars(k) = v; this }

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(appName, "appName")
    assertFieldIsSet(appResource, "appResource")
    assertFieldIsMemory(driverMemory, "driverMemory")
    assertFieldIsNumeric(driverCores, "driverCores")
    assertFieldIsBoolean(superviseDriver, "superviseDriver")
    assertFieldIsMemory(executorMemory, "executorMemory")
    assertFieldIsNumeric(totalExecutorCores, "totalExecutorCores")
  }
}

/**
 * A response to the [[SubmitDriverRequest]] in the REST application submission protocol.
 */
class SubmitDriverResponse extends SubmitRestProtocolResponse {
  var driverId: String = null
}
