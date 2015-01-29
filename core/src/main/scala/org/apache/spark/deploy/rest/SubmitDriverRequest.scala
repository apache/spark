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

/**
 * A request to submit a driver in the REST application submission protocol.
 */
class SubmitDriverRequest extends SubmitRestProtocolRequest {
  private val appName = new SubmitRestProtocolField[String]("appName")
  private val appResource = new SubmitRestProtocolField[String]("appResource")
  private val mainClass = new SubmitRestProtocolField[String]("mainClass")
  private val jars = new SubmitRestProtocolField[String]("jars")
  private val files = new SubmitRestProtocolField[String]("files")
  private val pyFiles = new SubmitRestProtocolField[String]("pyFiles")
  private val driverMemory = new SubmitRestProtocolField[String]("driverMemory")
  private val driverCores = new SubmitRestProtocolField[Int]("driverCores")
  private val driverExtraJavaOptions = new SubmitRestProtocolField[String]("driverExtraJavaOptions")
  private val driverExtraClassPath = new SubmitRestProtocolField[String]("driverExtraClassPath")
  private val driverExtraLibraryPath = new SubmitRestProtocolField[String]("driverExtraLibraryPath")
  private val superviseDriver = new SubmitRestProtocolField[Boolean]("superviseDriver")
  private val executorMemory = new SubmitRestProtocolField[String]("executorMemory")
  private val totalExecutorCores = new SubmitRestProtocolField[Int]("totalExecutorCores")

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

  // Special getters required for JSON serialization
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

  // Special setters required for JSON deserialization
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

  /** Return an array of arguments to be passed to the application. */
  @JsonIgnore
  def getAppArgs: Array[String] = appArgs.toArray

  /** Return a map of Spark properties to be passed to the application as java options. */
  @JsonIgnore
  def getSparkProperties: Map[String, String] = sparkProperties.toMap

  /** Return a map of environment variables to be passed to the application. */
  @JsonIgnore
  def getEnvironmentVariables: Map[String, String] = envVars.toMap

  /** Add a command line argument to be passed to the application. */
  @JsonIgnore
  def addAppArg(s: String): this.type = { appArgs += s; this }

  /** Set a Spark property to be passed to the application as a java option. */
  @JsonIgnore
  def setSparkProperty(k: String, v: String): this.type = { sparkProperties(k) = v; this }

  /** Set an environment variable to be passed to the application. */
  @JsonIgnore
  def setEnvironmentVariable(k: String, v: String): this.type = { envVars(k) = v; this }

  /** Serialize the given Array to a compact JSON string. */
  private def arrayToJson(arr: Array[String]): String = {
    if (arr.nonEmpty) { compact(render(JsonProtocol.arrayToJson(arr))) } else null
  }

  /** Serialize the given Map to a compact JSON string. */
  private def mapToJson(map: Map[String, String]): String = {
    if (map.nonEmpty) { compact(render(JsonProtocol.mapToJson(map))) } else null
  }

  override def validate(): Unit = {
    super.validate()
    assertFieldIsSet(appName)
    assertFieldIsSet(appResource)
  }
}
