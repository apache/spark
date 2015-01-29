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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.util.JsonProtocol

class SubmitDriverRequest extends SubmitRestProtocolRequest {
  protected override val action = SubmitRestProtocolAction.SUBMIT_DRIVER_REQUEST
  private val appName = new SubmitRestProtocolField[String]
  private val appResource = new SubmitRestProtocolField[String]
  private val mainClass = new SubmitRestProtocolField[String]
  private val jars = new SubmitRestProtocolField[String]
  private val files = new SubmitRestProtocolField[String]
  private val pyFiles = new SubmitRestProtocolField[String]
  private val driverMemory = new SubmitRestProtocolField[String]
  private val driverCores = new SubmitRestProtocolField[Int]
  private val driverExtraJavaOptions = new SubmitRestProtocolField[String]
  private val driverExtraClassPath = new SubmitRestProtocolField[String]
  private val driverExtraLibraryPath = new SubmitRestProtocolField[String]
  private val superviseDriver = new SubmitRestProtocolField[Boolean]
  private val executorMemory = new SubmitRestProtocolField[String]
  private val totalExecutorCores = new SubmitRestProtocolField[Int]

  // Special fields
  private val appArgs = new ArrayBuffer[String]
  private val sparkProperties = new mutable.HashMap[String, String]
  private val envVars = new mutable.HashMap[String, String]

  def getAppName: String = appName.toString
  def getAppResource: String = appResource.toString
  def getMainClass: String = mainClass.toString
  def getJars: String = jars.toString
  def getFiles: String = files.toString
  def getPyFiles: String = pyFiles.toString
  def getDriverMemory: String = driverMemory.toString
  def getDriverCores: String = driverCores.toString
  def getDriverExtraJavaOptions: String = driverExtraJavaOptions.toString
  def getDriverExtraClassPath: String = driverExtraClassPath.toString
  def getDriverExtraLibraryPath: String = driverExtraLibraryPath.toString
  def getSuperviseDriver: String = superviseDriver.toString
  def getExecutorMemory: String = executorMemory.toString
  def getTotalExecutorCores: String = totalExecutorCores.toString

  // Special getters required for JSON de/serialization
  @JsonProperty("appArgs")
  private def getAppArgsJson: String = arrayToJson(getAppArgs)
  @JsonProperty("sparkProperties")
  private def getSparkPropertiesJson: String = mapToJson(getSparkProperties)
  @JsonProperty("environmentVariables")
  private def getEnvironmentVariablesJson: String = mapToJson(getEnvironmentVariables)

  def setAppName(s: String): this.type = setField(appName, s)
  def setAppResource(s: String): this.type = setField(appResource, s)
  def setMainClass(s: String): this.type = setField(mainClass, s)
  def setJars(s: String): this.type = setField(jars, s)
  def setFiles(s: String): this.type = setField(files, s)
  def setPyFiles(s: String): this.type = setField(pyFiles, s)
  def setDriverMemory(s: String): this.type = setField(driverMemory, s)
  def setDriverCores(s: String): this.type = setNumericField(driverCores, s)
  def setDriverExtraJavaOptions(s: String): this.type = setField(driverExtraJavaOptions, s)
  def setDriverExtraClassPath(s: String): this.type = setField(driverExtraClassPath, s)
  def setDriverExtraLibraryPath(s: String): this.type = setField(driverExtraLibraryPath, s)
  def setSuperviseDriver(s: String): this.type = setBooleanField(superviseDriver, s)
  def setExecutorMemory(s: String): this.type = setField(executorMemory, s)
  def setTotalExecutorCores(s: String): this.type = setNumericField(totalExecutorCores, s)

  // Special setters required for JSON de/serialization
  @JsonProperty("appArgs")
  private def setAppArgsJson(s: String): Unit = {
    appArgs.clear()
    appArgs ++= JsonProtocol.arrayFromJson(parse(s))
  }
  @JsonProperty("sparkProperties")
  private def setSparkPropertiesJson(s: String): Unit = {
    sparkProperties.clear()
    sparkProperties ++= JsonProtocol.mapFromJson(parse(s))
  }
  @JsonProperty("environmentVariables")
  private def setEnvironmentVariablesJson(s: String): Unit = {
    envVars.clear()
    envVars ++= JsonProtocol.mapFromJson(parse(s))
  }

  @JsonIgnore
  def getAppArgs: Array[String] = appArgs.toArray
  @JsonIgnore
  def getSparkProperties: Map[String, String] = sparkProperties.toMap
  @JsonIgnore
  def getEnvironmentVariables: Map[String, String] = envVars.toMap
  @JsonIgnore
  def addAppArg(s: String): this.type = { appArgs += s; this }
  @JsonIgnore
  def setSparkProperty(k: String, v: String): this.type = { sparkProperties(k) = v; this }
  @JsonIgnore
  def setEnvironmentVariable(k: String, v: String): this.type = { envVars(k) = v; this }

  private def arrayToJson(arr: Array[String]): String = {
    if (arr.nonEmpty) { compact(render(JsonProtocol.arrayToJson(arr))) } else { null }
  }

  private def mapToJson(map: Map[String, String]): String = {
    if (map.nonEmpty) { compact(render(JsonProtocol.mapToJson(map))) } else { null }
  }

  override def validate(): Unit = {
    super.validate()
    assertFieldIsSet(appName, "app_name")
    assertFieldIsSet(appResource, "app_resource")
  }
}
