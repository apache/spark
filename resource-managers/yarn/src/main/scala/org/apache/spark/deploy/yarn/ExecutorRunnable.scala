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

import java.io.File
import java.nio.ByteBuffer
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

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
import org.apache.spark.internal.Logging
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

  def launchContextDebugInfo(): String = {
    val commands = prepareCommand()
    val env = prepareEnvironment()

    s"""
    |===============================================================================
    |Default YARN executor launch context:
    |  env:
    |${Utils.redact(sparkConf, env.toSeq).map { case (k, v) => s"    $k -> $v\n" }.mkString}
    |  command:
    |    ${Utils.redactCommandLineArgs(sparkConf, commands).mkString(" \\ \n      ")}
    |
    |  resources:
    |${localResources.map { case (k, v) => s"    $k -> $v\n" }.mkString}
    |===============================================================================""".stripMargin
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
      val secretString = securityMgr.getSecretKey()
      val secretBytes =
        if (secretString != null) {
          // This conversion must match how the YarnShuffleService decodes our secret
          JavaUtils.stringToBytes(secretString)
        } else {
          // Authentication is not enabled, so just provide dummy metadata
          ByteBuffer.allocate(0)
        }
      val serviceName = sparkConf.get(SHUFFLE_SERVICE_NAME)
      logInfo(s"Initializing service data for shuffle service using name '$serviceName'")
      ctx.setServiceData(Collections.singletonMap(serviceName, secretBytes))
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

  private def prepareCommand(): List[String] = {
    // Extra options for the JVM
    val javaOpts = ListBuffer[String]()

    // Set the JVM memory
    val executorMemoryString = executorMemory + "m"
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

    // Commenting it out for now - so that people can refer to the properties if required. Remove
    // it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence
    // if there are multiple containers in same node, spark gc effects all other containers
    // performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in
    // multi-tenant environments. Not sure how default java gc behaves if it is limited to subset
    // of cores on a node.
    /*
        else {
          // If no java_opts specified, default to using -XX:+CMSIncrementalMode
          // It might be possible that other modes/config is being done in
          // spark.executor.extraJavaOptions, so we don't want to mess with it.
          // In our expts, using (default) throughput collector has severe perf ramifications in
          // multi-tenant machines
          // The options are based on
          // http://www.oracle.com/technetwork/java/gc-tuning-5-138395.html#0.0.0.%20When%20to%20Use
          // %20the%20Concurrent%20Low%20Pause%20Collector|outline
          javaOpts += "-XX:+UseConcMarkSweepGC"
          javaOpts += "-XX:+CMSIncrementalMode"
          javaOpts += "-XX:+CMSIncrementalPacing"
          javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
          javaOpts += "-XX:CMSIncrementalDutyCycle=10"
        }
    */

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val userClassPath = Client.getUserClasspath(sparkConf).flatMap { uri =>
      val absPath =
        if (new File(uri.getPath()).isAbsolute()) {
          Client.getClusterPath(sparkConf, uri.getPath())
        } else {
          Client.buildPath(Environment.PWD.$(), uri.getPath())
        }
      Seq("--user-class-path", "file:" + absPath)
    }.toSeq

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
      userClassPath ++
      Seq(
        s"1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout",
        s"2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    commands.map(s => if (s == null) "null" else s).toList
  }

  private def prepareEnvironment(): HashMap[String, String] = {
    val env = new HashMap[String, String]()
    Client.populateClasspath(null, conf, sparkConf, env, sparkConf.get(EXECUTOR_CLASS_PATH))

    System.getenv().asScala.filterKeys(_.startsWith("SPARK"))
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
