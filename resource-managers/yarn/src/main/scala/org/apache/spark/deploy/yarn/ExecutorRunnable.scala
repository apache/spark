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
import java.util.Collections

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.Records

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.internal.{Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys.{EXECUTOR_ENVS, EXECUTOR_LAUNCH_COMMANDS, EXECUTOR_RESOURCES}
import org.apache.spark.internal.config._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

private[yarn] class ExecutorRunnable(
    container: Option[Container],
    conf: YarnConfiguration,
    sparkConf: SparkConf,
    masterAddress: String,
    executorId: String,
    hostname: String,
    executorMemory: Int,
    executorCores: Int,
    appId: String,
    securityMgr: SecurityManager,
    localResources: Map[String, LocalResource],
    resourceProfileId: Int) extends Logging {

  var rpc: YarnRPC = YarnRPC.create(conf)
  var nmClient: NMClient = _

  def run(): Unit = {
    logDebug("Starting Executor Container")
    nmClient = NMClient.createNMClient()
    nmClient.init(conf)
    nmClient.start()
    startContainer()
  }

  def launchContextDebugInfo(): MessageWithContext = {
    val commands = prepareCommand()
    val env = prepareEnvironment()

    // scalastyle:off line.size.limit
    log"""
        |===============================================================================
        |Default YARN executor launch context:
        |  env:
        |${MDC(EXECUTOR_ENVS, Utils.redact(sparkConf, env.toSeq).map { case (k, v) => s"    $k -> $v\n" }.mkString)}
        |  command:
        |    ${MDC(EXECUTOR_LAUNCH_COMMANDS, Utils.redactCommandLineArgs(sparkConf, commands).mkString(" \\ \n      "))}
        |
        |  resources:
        |${MDC(EXECUTOR_RESOURCES, localResources.map { case (k, v) => s"    $k -> $v\n" }.mkString)}
        |===============================================================================""".stripMargin
    // scalastyle:on line.size.limit
  }

  def startContainer(): java.util.Map[String, ByteBuffer] = {
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]
    val env = prepareEnvironment().asJava

    ctx.setLocalResources(localResources.asJava)
    ctx.setEnvironment(env)

    val credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    ctx.setTokens(ByteBuffer.wrap(dob.getData()))

    val commands = prepareCommand()

    ctx.setCommands(commands.asJava)
    ctx.setApplicationACLs(
      YarnSparkHadoopUtil.getApplicationAclsForYarn(securityMgr).asJava)

    // If external shuffle service is enabled, register with the Yarn shuffle service already
    // started on the NodeManager and, if authentication is enabled, provide it with our secret
    // key for fetching shuffle files later
    if (sparkConf.get(SHUFFLE_SERVICE_ENABLED)) {
      configureServiceData(ctx)
    }

    // Send the start request to the ContainerManager
    try {
      nmClient.startContainer(container.get, ctx)
    } catch {
      case ex: Exception =>
        throw new SparkException(s"Exception while starting container ${container.get.getId}" +
          s" on host $hostname", ex)
    }
  }

  private[yarn] def configureServiceData(ctx: ContainerLaunchContext): Unit = {
    val secretString = securityMgr.getSecretKey()

    val serviceName = sparkConf.get(SHUFFLE_SERVICE_NAME)
    if (!sparkConf.get(SHUFFLE_SERVER_RECOVERY_DISABLED)) {
      val secretBytes =
        if (secretString != null) {
          // This conversion must match how the YarnShuffleService decodes our secret
          JavaUtils.stringToBytes(secretString)
        } else {
          // Authentication is not enabled, so just provide dummy metadata
          ByteBuffer.allocate(0)
        }
      ctx.setServiceData(Collections.singletonMap(serviceName, secretBytes))
    } else {
      val payload = new mutable.HashMap[String, Object]()
      payload.put(SHUFFLE_SERVER_RECOVERY_DISABLED.key, java.lang.Boolean.TRUE)
      if (secretString != null) {
        payload.put(ExecutorRunnable.SECRET_KEY, secretString)
      }
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val jsonString = mapper.writeValueAsString(payload)
      ctx.setServiceData(Collections.singletonMap(serviceName, JavaUtils.stringToBytes(jsonString)))
    }
  }

  private def prepareCommand(): List[String] = {
    // Extra options for the JVM
    val javaOpts = ListBuffer[String]()

    // Set the JVM memory
    val executorMemoryString = s"${executorMemory}m"
    javaOpts += "-Xmx" + executorMemoryString

    // Set extra Java options for the executor, if defined
    sparkConf.get(EXECUTOR_JAVA_OPTIONS).foreach { opts =>
      val subsOpt = Utils.substituteAppNExecIds(opts, appId, executorId)
      javaOpts ++= Utils.splitCommandString(subsOpt).map(YarnSparkHadoopUtil.escapeForShell)
    }

    // Set the library path through a command prefix to append to the existing value of the
    // env variable.
    val prefixEnv = sparkConf.get(EXECUTOR_LIBRARY_PATH).map { libPath =>
      Client.createLibraryPathPrefix(libPath, sparkConf)
    }

    javaOpts += "-Djava.io.tmpdir=" +
      new Path(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)

    // Certain configs need to be passed here because they are needed before the Executor
    // registers with the Scheduler and transfers the spark configs. Since the Executor backend
    // uses RPC to connect to the scheduler, the RPC settings are needed as well as the
    // authentication settings.
    sparkConf.getAll
      .filter { case (k, v) => SparkConf.isExecutorStartupConf(k) }
      .foreach { case (k, v) => javaOpts += YarnSparkHadoopUtil.escapeForShell(s"-D$k=$v") }

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    YarnSparkHadoopUtil.addOutOfMemoryErrorArgument(javaOpts)
    val commands = prefixEnv ++
      Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++
      javaOpts ++
      Seq("org.apache.spark.executor.YarnCoarseGrainedExecutorBackend",
        "--driver-url", masterAddress,
        "--executor-id", executorId,
        "--hostname", hostname,
        "--cores", executorCores.toString,
        "--app-id", appId,
        "--resourceProfileId", resourceProfileId.toString) ++
      Seq(
        s"1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout",
        s"2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    commands.map(s => if (s == null) "null" else s).toList
  }

  private def prepareEnvironment(): HashMap[String, String] = {
    val env = new HashMap[String, String]()
    Client.populateClasspath(null, conf, sparkConf, env, sparkConf.get(EXECUTOR_CLASS_PATH))

    System.getenv().asScala.filter { case (k, _) => k.startsWith("SPARK") }
      .foreach { case (k, v) => env(k) = v }

    sparkConf.getExecutorEnv.foreach { case (key, value) =>
      if (key == Environment.CLASSPATH.name()) {
        // If the key of env variable is CLASSPATH, we assume it is a path and append it.
        // This is kept for backward compatibility and consistency with hadoop
        YarnSparkHadoopUtil.addPathToEnvironment(env, key, value)
      } else {
        // For other env variables, simply overwrite the value.
        env(key) = value
      }
    }

    env
  }
}

private[yarn] object ExecutorRunnable {
  private[yarn] val SECRET_KEY = "secret"
}
