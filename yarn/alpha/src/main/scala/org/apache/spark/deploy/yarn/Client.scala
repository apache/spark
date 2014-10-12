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

package org.apache.spark.deploy.yarn

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Version of [[org.apache.spark.deploy.yarn.ClientBase]] tailored to YARN's alpha API.
 */
private[spark] class Client(
    val args: ClientArguments,
    val hadoopConf: Configuration,
    val sparkConf: SparkConf)
  extends YarnClientImpl with ClientBase with Logging {

  def this(clientArgs: ClientArguments, spConf: SparkConf) =
    this(clientArgs, SparkHadoopUtil.get.newConfiguration(spConf), spConf)

  def this(clientArgs: ClientArguments) = this(clientArgs, new SparkConf())

  val yarnConf: YarnConfiguration = new YarnConfiguration(hadoopConf)

  /* ------------------------------------------------------------------------------------- *
   | The following methods have much in common in the stable and alpha versions of Client, |
   | but cannot be implemented in the parent trait due to subtle API differences across    |
   | hadoop versions.                                                                      |
   * ------------------------------------------------------------------------------------- */

  /** Submit an application running our ApplicationMaster to the ResourceManager. */
  override def submitApplication(): ApplicationId = {
    init(yarnConf)
    start()

    logInfo("Requesting a new application from cluster with %d NodeManagers"
      .format(getYarnClusterMetrics.getNumNodeManagers))

    // Get a new application from our RM
    val newAppResponse = getNewApplication()
    val appId = newAppResponse.getApplicationId()

    // Verify whether the cluster has enough resources for our AM
    verifyClusterResources(newAppResponse)

    // Set up the appropriate contexts to launch our AM
    val containerContext = createContainerLaunchContext(newAppResponse)
    val appContext = createApplicationSubmissionContext(appId, containerContext)

    // Finally, submit and monitor the application
    logInfo(s"Submitting application ${appId.getId} to ResourceManager")
    submitApplication(appContext)
    appId
  }

  /**
   * Set up a context for launching our ApplicationMaster container.
   * In the Yarn alpha API, the memory requirements of this container must be set in
   * the ContainerLaunchContext instead of the ApplicationSubmissionContext.
   */
  override def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse)
      : ContainerLaunchContext = {
    val containerContext = super.createContainerLaunchContext(newAppResponse)
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(args.amMemory + amMemoryOverhead)
    containerContext.setResource(capability)
    containerContext
  }

  /** Set up the context for submitting our ApplicationMaster. */
  def createApplicationSubmissionContext(
      appId: ApplicationId,
      containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName(args.appName)
    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(containerContext)
    appContext.setUser(UserGroupInformation.getCurrentUser.getShortUserName)
    appContext
  }

  /**
   * Set up security tokens for launching our ApplicationMaster container.
   * ContainerLaunchContext#setContainerTokens is renamed `setTokens` in the stable API.
   */
  override def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    amContainer.setContainerTokens(ByteBuffer.wrap(dob.getData()))
  }

  /**
   * Return the security token used by this client to communicate with the ApplicationMaster.
   * If no security is enabled, the token returned by the report is null.
   * ApplicationReport#getClientToken is renamed `getClientToAMToken` in the stable API.
   */
  override def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToken).map(_.toString).getOrElse("")
}

object Client {
  def main(argStrings: Array[String]) {
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a " +
        "future version of Spark. Use ./bin/spark-submit with \"--master yarn\"")
    }

    // Set an env variable indicating we are running in YARN mode.
    // Note that any env variable with the SPARK_ prefix gets propagated to all (remote) processes
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf

    try {
      val args = new ClientArguments(argStrings, sparkConf)
      new Client(args, sparkConf).run()
    } catch {
      case e: Exception =>
        Console.err.println(e.getMessage)
        System.exit(1)
    }

    System.exit(0)
  }
}
