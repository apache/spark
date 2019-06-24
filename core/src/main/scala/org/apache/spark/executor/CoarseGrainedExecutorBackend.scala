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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.json4s.DefaultFormats

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorLossReason, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv,
    resourcesFileOpt: Option[String])
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  private implicit val formats = DefaultFormats

  private[this] val stopping = new AtomicBoolean(false)
  var executor: Executor = null
  @volatile var driver: Option[RpcEndpointRef] = None

  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  /**
   * Map each taskId to the information about the resource allocated to it, Please refer to
   * [[ResourceInformation]] for specifics.
   * Exposed for testing only.
   */
  private[executor] val taskResources = new mutable.HashMap[Long, Map[String, ResourceInformation]]

  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    val resources = parseOrFindResources(resourcesFileOpt)
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, resources))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        // Always receive `true`. Just ignore it
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
  }

  // visible for testing
  def parseOrFindResources(resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    // only parse the resources if a task requires them
    val resourceInfo = if (parseTaskResourceRequirements(env.conf).nonEmpty) {
      val resources = getOrDiscoverAllResources(env.conf, SPARK_EXECUTOR_PREFIX, resourcesFileOpt)
      if (resources.isEmpty) {
        throw new SparkException("User specified resources per task via: " +
          s"$SPARK_TASK_PREFIX, but can't find any resources available on the executor.")
      }
      resources
    } else {
      if (resourcesFileOpt.nonEmpty) {
        logWarning("A resources file was specified but the application is not configured " +
          s"to use any resources, see the configs with prefix: ${SPARK_TASK_PREFIX}")
      }
      Map.empty[String, ResourceInformation]
    }
    resourceInfo
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
  }

  def extractAttributes: Map[String, String] = {
    val prefix = "SPARK_EXECUTOR_ATTRIBUTE_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toUpperCase(Locale.ROOT), e._2))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }

    case RegisterExecutorFailed(message) =>
      exitExecutor(1, "Slave registration failed: " + message)

    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        val taskDesc = TaskDescription.decode(data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        taskResources(taskDesc.taskId) = taskDesc.resources
        executor.launchTask(this, taskDesc)
      }

    case KillTask(taskId, _, interruptThread, reason) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread, reason)
      }

    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

    case Shutdown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
          // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
          // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
          // Therefore, we put this line in a new thread.
          executor.stop()
        }
      }.start()

    case UpdateDelegationTokens(tokenBytes) =>
      logInfo(s"Received tokens of ${tokenBytes.length} bytes")
      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(s"Driver from $remoteAddress disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.", null,
        notifyDriver = false)
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    val resources = taskResources.getOrElse(taskId, Map.empty[String, ResourceInformation])
    val msg = StatusUpdate(executorId, taskId, state, data, resources)
    if (TaskState.isFinished(state)) {
      taskResources.remove(taskId)
    }
    driver match {
      case Some(driverRef) => driverRef.send(msg)
      case None => logWarning(s"Drop $msg because has not yet connected to driver")
    }
  }

  /**
   * This function can be overloaded by other child classes to handle
   * executor exits differently. For e.g. when an executor goes down,
   * back-end may not want to take the parent process down.
   */
  protected def exitExecutor(code: Int,
                             reason: String,
                             throwable: Throwable = null,
                             notifyDriver: Boolean = true) = {
    val message = "Executor self-exiting due to : " + reason
    if (throwable != null) {
      logError(message, throwable)
    } else {
      logError(message)
    }

    if (notifyDriver && driver.nonEmpty) {
      driver.get.send(RemoveExecutor(executorId, new ExecutorLossReason(reason)))
    }

    System.exit(code)
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

  case class Arguments(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: mutable.ListBuffer[URL],
      resourcesFileOpt: Option[String])

  def main(args: Array[String]): Unit = {
    val createFn: (RpcEnv, Arguments, SparkEnv) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env) =>
      new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.hostname, arguments.cores, arguments.userClassPath, env,
        arguments.resourcesFileOpt)
    }
    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")), createFn)
    System.exit(0)
  }

  def run(
      arguments: Arguments,
      backendCreateFn: (RpcEnv, Arguments, SparkEnv) => CoarseGrainedExecutorBackend): Unit = {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(arguments.hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        arguments.hostname,
        -1,
        executorConf,
        new SecurityManager(executorConf),
        clientMode = true)
      val driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
      val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig)
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", arguments.appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }

      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      driverConf.set(EXECUTOR_ID, arguments.executorId)
      val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.hostname,
        arguments.cores, cfg.ioEncryptionKey, isLocal = false)

      env.rpcEnv.setupEndpoint("Executor", backendCreateFn(env.rpcEnv, arguments, env))
      arguments.workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
      }
      env.rpcEnv.awaitTermination()
    }
  }

  def parseArguments(args: Array[String], classNameForEntry: String): Arguments = {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var resourcesFileOpt: Option[String] = None
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--resourcesFile") :: value :: tail =>
          resourcesFileOpt = Some(value)
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit(classNameForEntry)
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit(classNameForEntry)
    }

    Arguments(driverUrl, executorId, hostname, cores, appId, workerUrl,
      userClassPath, resourcesFileOpt)
  }

  private def printUsageAndExit(classNameForEntry: String): Unit = {
    // scalastyle:off println
    System.err.println(
      s"""
      |Usage: $classNameForEntry [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --resourcesFile <fileWithJSONResourceInformation>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }
}
