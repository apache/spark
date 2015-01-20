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

import java.net.URL

import org.apache.spark.{SPARK_VERSION => sparkVersion}
import org.apache.spark.deploy.SparkSubmitArguments
import org.apache.spark.util.Utils

/**
 * A client that submits Spark applications to the standalone Master using a stable REST protocol.
 * This client is intended to communicate with the StandaloneRestServer. Cluster mode only.
 */
private[spark] class StandaloneRestClient extends SubmitRestClient {

  /** Construct a submit driver request message. */
  override protected def constructSubmitRequest(
      args: SparkSubmitArguments): SubmitDriverRequestMessage = {
    import SubmitDriverRequestField._
    val driverMemory = Option(args.driverMemory)
      .map { m => Utils.memoryStringToMb(m).toString }
      .orNull
    val executorMemory = Option(args.executorMemory)
      .map { m => Utils.memoryStringToMb(m).toString }
      .orNull
    val message = new SubmitDriverRequestMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MASTER, args.master)
      .setField(APP_NAME, args.name)
      .setField(APP_RESOURCE, args.primaryResource)
      .setFieldIfNotNull(MAIN_CLASS, args.mainClass)
      .setFieldIfNotNull(JARS, args.jars)
      .setFieldIfNotNull(FILES, args.files)
      .setFieldIfNotNull(DRIVER_MEMORY, driverMemory)
      .setFieldIfNotNull(DRIVER_CORES, args.driverCores)
      .setFieldIfNotNull(DRIVER_EXTRA_JAVA_OPTIONS, args.driverExtraJavaOptions)
      .setFieldIfNotNull(DRIVER_EXTRA_CLASS_PATH, args.driverExtraClassPath)
      .setFieldIfNotNull(DRIVER_EXTRA_LIBRARY_PATH, args.driverExtraLibraryPath)
      .setFieldIfNotNull(SUPERVISE_DRIVER, args.supervise.toString)
      .setFieldIfNotNull(EXECUTOR_MEMORY, executorMemory)
      .setFieldIfNotNull(TOTAL_EXECUTOR_CORES, args.totalExecutorCores)
    args.childArgs.foreach(message.appendAppArg)
    args.sparkProperties.foreach { case (k, v) => message.setSparkProperty(k, v) }
    // TODO: set environment variables?
    message.validate()
  }

  /** Construct a kill driver request message. */
  override protected def constructKillRequest(
      master: String,
      driverId: String): KillDriverRequestMessage = {
    import KillDriverRequestField._
    new KillDriverRequestMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MASTER, master)
      .setField(DRIVER_ID, driverId)
      .validate()
  }

  /** Construct a driver status request message. */
  override protected def constructStatusRequest(
      master: String,
      driverId: String): DriverStatusRequestMessage = {
    import DriverStatusRequestField._
    new DriverStatusRequestMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MASTER, master)
      .setField(DRIVER_ID, driverId)
      .validate()
  }

  /** Throw an exception if this is not standalone mode. */
  override protected def validateMaster(master: String): Unit = {
    if (!master.startsWith("spark://")) {
      throw new IllegalArgumentException("This REST client is only supported in standalone mode.")
    }
  }

  /** Throw an exception if this is not cluster deploy mode. */
  override protected def validateDeployMode(deployMode: String): Unit = {
    if (deployMode != "cluster") {
      throw new IllegalArgumentException("This REST client is only supported in cluster mode.")
    }
  }

  /** Extract the URL portion of the master address. */
  override protected def getHttpUrl(master: String): URL = {
    validateMaster(master)
    new URL("http://" + master.stripPrefix("spark://"))
  }
}
