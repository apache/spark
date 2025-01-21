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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import io.netty.util.internal.PlatformDependent

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile._
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorLossMessage, ExecutorLossReason, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, SignalUtils, ThreadUtils, Utils}

private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    bindAddress: String,
    hostname: String,
    cores: Int,
    env: SparkEnv,
    resourcesFileOpt: Option[String],
    resourceProfile: ResourceProfile)
  extends IsolatedThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  import CoarseGrainedExecutorBackend._

  private[spark] val stopping = new AtomicBoolean(false)
  var executor: Executor = null
  @volatile var driver: Option[RpcEndpointRef] = None

  private var _resources = Map.empty[String, ResourceInformation]

  private var decommissioned = false

  // Track the last time in ns that at least one task is running. If no task is running and all
  // shuffle/RDD data migration are done, the decommissioned executor should exit.
  private val lastTaskFinishTime = new AtomicLong(System.nanoTime())

  override def onStart(): Unit = {
    if (env.conf.get(DECOMMISSION_ENABLED)) {
      val signal = env.conf.get(EXECUTOR_DECOMMISSION_SIGNAL)
      logInfo(log"Registering SIG${MDC(LogKeys.SIGNAL, signal)}" +
        log" handler to trigger decommissioning.")
      SignalUtils.register(signal, log"Failed to register SIG${MDC(LogKeys.SIGNAL, signal)} " +
        log"handler - disabling executor decommission feature.")(
        self.askSync[Boolean](ExecutorDecommissionSigReceived))
    }

    logInfo(log"Connecting to driver: ${MDC(LogKeys.URL, driverUrl)}" )
    try {
      val securityManager = new SecurityManager(env.conf)
      val shuffleClientTransportConf = SparkTransportConf.fromSparkConf(
        env.conf, "shuffle", sslOptions = Some(securityManager.getRpcSSLOptions()))
      if (NettyUtils.preferDirectBufs(shuffleClientTransportConf) &&
          PlatformDependent.maxDirectMemory() < env.conf.get(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)) {
        throw new SparkException(s"Netty direct memory should at least be bigger than " +
          s"'${MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM.key}', but got " +
          s"${PlatformDependent.maxDirectMemory()} bytes < " +
          s"${env.conf.get(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)}")
      }

      _resources = parseOrFindResources(resourcesFileOpt)
    } catch {
      case NonFatal(e) =>
        exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
    }
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      env.executorBackend = Option(this)
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, _resources, resourceProfile.id))
    }(ThreadUtils.sameThread).onComplete {
      case Success(_) =>
        self.send(RegisteredExecutor)
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
  }

  /**
   * Create a classLoader for use for resource discovery. The user could provide a class
   * as a substitute for the default one so we have to be able to load it from a user specified
   * jar.
   */
  private def createClassLoader(): MutableURLClassLoader = {
    val currentLoader = Utils.getContextOrSparkClassLoader
    val urls = getUserClassPath.toArray
    if (env.conf.get(EXECUTOR_USER_CLASS_PATH_FIRST)) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  // visible for testing
  def parseOrFindResources(resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    // use a classloader that includes the user classpath in case they specified a class for
    // resource discovery
    val urlClassLoader = createClassLoader()
    logDebug(s"Resource profile id is: ${resourceProfile.id}")
    Utils.withContextClassLoader(urlClassLoader) {
      val resources = getOrDiscoverAllResourcesForResourceProfile(
        resourcesFileOpt,
        SPARK_EXECUTOR_PREFIX,
        resourceProfile,
        env.conf)
      logResourceInfo(SPARK_EXECUTOR_PREFIX, resources)
      resources
    }
  }

  def getUserClassPath: Seq[URL] = Nil

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filter { case (k, _) => k.startsWith(prefix) }
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
  }

  def extractAttributes: Map[String, String] = {
    val prefix = "SPARK_EXECUTOR_ATTRIBUTE_"
    sys.env.filter { case (k, _) => k.startsWith(prefix) }
      .map(e => (e._1.substring(prefix.length).toUpperCase(Locale.ROOT), e._2))
  }

  def notifyDriverAboutPushCompletion(shuffleId: Int, shuffleMergeId: Int, mapIndex: Int): Unit = {
    val msg = ShufflePushCompletion(shuffleId, shuffleMergeId, mapIndex)
    driver.foreach(_.send(msg))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        executor = new Executor(executorId, hostname, env, getUserClassPath, isLocal = false,
          resources = _resources)
        driver.get.send(LaunchedExecutor(executorId))
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }
    case UpdateExecutorLogLevel(newLogLevel) =>
      Utils.setLogLevelIfNeeded(newLogLevel)

    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        val taskDesc = TaskDescription.decode(data.value)
        logInfo(log"Got assigned task ${MDC(LogKeys.TASK_ID, taskDesc.taskId)}")
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
          // `executor` can be null if there's any error in `CoarseGrainedExecutorBackend.onStart`
          // or fail to create `Executor`.
          if (executor == null) {
            System.exit(1)
          } else {
            // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
            // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
            // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
            // Therefore, we put this line in a new thread.
            executor.stop()
          }
        }
      }.start()

    case UpdateDelegationTokens(tokenBytes) =>
      logInfo(log"Received tokens of ${MDC(LogKeys.NUM_BYTES, tokenBytes.length)} bytes")
      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)

    case DecommissionExecutor =>
      decommissionSelf()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ExecutorDecommissionSigReceived =>
      var driverNotified = false
      try {
        driver.foreach { driverRef =>
          // Tell driver that we are starting decommissioning so it stops trying to schedule us
          driverNotified = driverRef.askSync[Boolean](ExecutorDecommissioning(executorId))
          if (driverNotified) decommissionSelf()
        }
      } catch {
        case e: Exception =>
          if (driverNotified) {
            logError("Fail to decommission self (but driver has been notified).", e)
          } else {
            logError("Fail to tell driver that we are starting decommissioning", e)
          }
          decommissioned = false
      }
      context.reply(decommissioned)

    case TaskThreadDump(taskId) =>
      context.reply(executor.getTaskThreadDump(taskId))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(log"Driver from ${MDC(LogKeys.RPC_ADDRESS, remoteAddress)}" +
        log" disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.", null,
        notifyDriver = false)
    } else {
      logWarning(log"An unknown (${MDC(LogKeys.REMOTE_ADDRESS, remoteAddress)} " +
        log"driver disconnected.")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    val taskDescription = executor.runningTasks.get(taskId).taskDescription
    val resources = taskDescription.resources
    val cpus = taskDescription.cpus
    val msg = StatusUpdate(executorId, taskId, state, data, cpus, resources)
    if (TaskState.isFinished(state)) {
      lastTaskFinishTime.set(System.nanoTime())
    }
    driver match {
      case Some(driverRef) => driverRef.send(msg)
      case None =>
        logWarning(log"Drop ${MDC(LogKeys.MESSAGE, msg)} because has not yet connected to driver")
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
    if (stopping.compareAndSet(false, true)) {
      val message = log"Executor self-exiting due to : ${MDC(LogKeys.REASON, reason)}"
      if (throwable != null) {
        logError(message, throwable)
      } else {
        if (code == 0) {
          logInfo(message)
        } else {
          logError(message)
        }
      }

      if (notifyDriver && driver.nonEmpty) {
        driver.get.send(RemoveExecutor(executorId, new ExecutorLossReason(reason)))
      }
      self.send(Shutdown)
    } else {
      logInfo("Skip exiting executor since it's been already asked to exit before.")
    }
  }

  private def decommissionSelf(): Unit = {
    if (!env.conf.get(DECOMMISSION_ENABLED)) {
      logWarning("Receive decommission request, but decommission feature is disabled.")
      return
    } else if (decommissioned) {
      logWarning(log"Executor ${MDC(LogKeys.EXECUTOR_ID, executorId)} " +
        log"already started decommissioning.")
      return
    }
    logInfo(log"Decommission executor ${MDC(LogKeys.EXECUTOR_ID, executorId)}.")
    try {
      decommissioned = true
      val migrationEnabled = env.conf.get(STORAGE_DECOMMISSION_ENABLED) &&
        (env.conf.get(STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED) ||
          env.conf.get(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED))
      if (migrationEnabled) {
        env.blockManager.decommissionBlockManager()
      } else if (env.conf.get(STORAGE_DECOMMISSION_ENABLED)) {
        logError(log"Storage decommissioning attempted but neither " +
          log"${MDC(LogKeys.CONFIG, STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key)} or " +
          log"${MDC(LogKeys.CONFIG2, STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key)} is enabled ")
      }
      if (executor != null) {
        executor.decommission()
      }
      // Shutdown the executor once all tasks are gone & any configured migrations completed.
      // Detecting migrations completion doesn't need to be perfect and we want to minimize the
      // overhead for executors that are not in decommissioning state as overall that will be
      // more of the executors. For example, this will not catch a block which is already in
      // the process of being put from a remote executor before migration starts. This trade-off
      // is viewed as acceptable to minimize introduction of any new locking structures in critical
      // code paths.

      val shutdownThread = new Thread("wait-for-blocks-to-migrate") {
        override def run(): Unit = {
          val sleep_time = 1000 // 1s
          // This config is internal and only used by unit tests to force an executor
          // to hang around for longer when decommissioned.
          val initialSleepMillis = env.conf.getInt(
            "spark.test.executor.decommission.initial.sleep.millis", sleep_time)
          if (initialSleepMillis > 0) {
            Thread.sleep(initialSleepMillis)
          }
          while (true) {
            logInfo("Checking to see if we can shutdown.")
            if (executor == null || executor.numRunningTasks == 0) {
              if (migrationEnabled) {
                logInfo("No running tasks, checking migrations")
                val (migrationTime, allBlocksMigrated) = env.blockManager.lastMigrationInfo()
                // We can only trust allBlocksMigrated boolean value if there were no tasks running
                // since the start of computing it.
                if (allBlocksMigrated && (migrationTime > lastTaskFinishTime.get())) {
                  logInfo("No running tasks, all blocks migrated, stopping.")
                  exitExecutor(0, ExecutorLossMessage.decommissionFinished, notifyDriver = true)
                } else {
                  logInfo("All blocks not yet migrated.")
                }
              } else {
                logInfo("No running tasks, no block migration configured, stopping.")
                exitExecutor(0, ExecutorLossMessage.decommissionFinished, notifyDriver = true)
              }
            } else {
              logInfo(log"Blocked from shutdown by" +
                log" ${MDC(LogKeys.NUM_TASKS, executor.numRunningTasks)} running tasks")
            }
            Thread.sleep(sleep_time)
          }
        }
      }
      shutdownThread.setDaemon(true)
      shutdownThread.start()

      logInfo("Will exit when finished decommissioning")
    } catch {
      case e: Exception =>
        decommissioned = false
        logError("Unexpected error while decommissioning self", e)
    }
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

  // Message used internally to start the executor when the driver successfully accepted the
  // registration request.
  case object RegisteredExecutor

  case class Arguments(
      driverUrl: String,
      executorId: String,
      bindAddress: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      resourcesFileOpt: Option[String],
      resourceProfileId: Int)

  def main(args: Array[String]): Unit = {
    val createFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env, resourceProfile) =>
      new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.bindAddress, arguments.hostname, arguments.cores,
        env, arguments.resourcesFileOpt, resourceProfile)
    }
    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")), createFn)
    System.exit(0)
  }

  def run(
      arguments: Arguments,
      backendCreateFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
        CoarseGrainedExecutorBackend): Unit = {

    Utils.resetStructuredLogging()
    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(arguments.hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        arguments.bindAddress,
        arguments.hostname,
        -1,
        executorConf,
        new SecurityManager(executorConf),
        numUsableCores = 0,
        clientMode = true)

      var driver: RpcEndpointRef = null
      val nTries = 3
      for (i <- 0 until nTries if driver == null) {
        try {
          driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
        } catch {
          case e: Throwable => if (i == nTries - 1) {
            throw e
          }
        }
      }

      val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig(arguments.resourceProfileId))
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

      // Initialize logging system again after `spark.log.structuredLogging.enabled` takes effect
      Utils.resetStructuredLogging(driverConf)
      Logging.uninitialize()

      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      driverConf.set(EXECUTOR_ID, arguments.executorId)
      cfg.logLevel.foreach(logLevel => Utils.setLogLevelIfNeeded(logLevel))

      // Set executor memory related config here according to resource profile
      if (cfg.resourceProfile.id != ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) {
        cfg.resourceProfile
          .executorResources
          .foreach {
            case (ResourceProfile.OFFHEAP_MEM, request) =>
              driverConf.set(MEMORY_OFFHEAP_SIZE.key, request.amount.toString + "m")
              logInfo(log"Set executor off-heap memory to " +
                log"${MDC(LogKeys.EXECUTOR_MEMORY_OFFHEAP, request)}")
            case (ResourceProfile.MEMORY, request) =>
              driverConf.set(EXECUTOR_MEMORY.key, request.amount.toString + "m")
              logInfo(log"Set executor memory to ${MDC(LogKeys.EXECUTOR_MEMORY_SIZE, request)}")
            case (ResourceProfile.OVERHEAD_MEM, request) =>
              // Maybe don't need to set this since it's nearly used by tasks.
              driverConf.set(EXECUTOR_MEMORY_OVERHEAD.key, request.amount.toString + "m")
              logInfo(log"Set executor memory_overhead to " +
                log"${MDC(LogKeys.EXECUTOR_MEMORY_OVERHEAD_SIZE, request)}")
            case (ResourceProfile.CORES, request) =>
              driverConf.set(EXECUTOR_CORES.key, request.amount.toString)
              logInfo(log"Set executor cores to ${MDC(LogKeys.NUM_EXECUTOR_CORES, request)}")
            case _ =>
          }
      }
      val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.bindAddress,
        arguments.hostname, arguments.cores, cfg.ioEncryptionKey, isLocal = false)
      // Set the application attemptId in the BlockStoreClient if available.
      val appAttemptId = env.conf.get(APP_ATTEMPT_ID)
      appAttemptId.foreach(attemptId =>
        env.blockManager.blockStoreClient.setAppAttemptId(attemptId)
      )
      val backend = backendCreateFn(env.rpcEnv, arguments, env, cfg.resourceProfile)
      env.rpcEnv.setupEndpoint("Executor", backend)
      arguments.workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher",
          new WorkerWatcher(env.rpcEnv, url, isChildProcessStopping = backend.stopping))
      }
      env.rpcEnv.awaitTermination()
    }
  }

  def parseArguments(args: Array[String], classNameForEntry: String): Arguments = {
    var driverUrl: String = null
    var executorId: String = null
    var bindAddress: String = null
    var hostname: String = null
    var cores: Int = 0
    var resourcesFileOpt: Option[String] = None
    var appId: String = null
    var workerUrl: Option[String] = None
    var resourceProfileId: Int = DEFAULT_RESOURCE_PROFILE_ID

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--bind-address") :: value :: tail =>
          bindAddress = value
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
        case ("--resourceProfileId") :: value :: tail =>
          resourceProfileId = value.toInt
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit(classNameForEntry)
      }
    }

    if (hostname == null) {
      hostname = Utils.localHostName()
      log.info(s"Executor hostname is not provided, will use '$hostname' to advertise itself")
    }

    if (driverUrl == null || executorId == null || cores <= 0 || appId == null) {
      printUsageAndExit(classNameForEntry)
    }

    if (bindAddress == null) {
      bindAddress = hostname
    }

    Arguments(driverUrl, executorId, bindAddress, hostname, cores, appId, workerUrl,
      resourcesFileOpt, resourceProfileId)
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
      |   --bind-address <bindAddress>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --resourcesFile <fileWithJSONResourceInformation>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --resourceProfileId <id>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }
}
