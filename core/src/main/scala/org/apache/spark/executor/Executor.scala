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

import java.io.{File, NotSerializableException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{URI, URL}
import java.nio.ByteBuffer
import java.util.{Locale, Properties}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.MDC

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, LogKeys, MDC => LogMDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.{EXECUTOR_USER_CLASS_PATH_FIRST => EXECUTOR_USER_CLASS_PATH_FIRST_CONFIG}
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.metrics.source.JVMCPUSource
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler._
import org.apache.spark.serializer.SerializerHelper
import org.apache.spark.shuffle.{FetchFailedException, ShuffleBlockPusher}
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._

private[spark] class IsolatedSessionState(
  val sessionUUID: String,
  var urlClassLoader: MutableURLClassLoader,
  var replClassLoader: ClassLoader,
  val currentFiles: HashMap[String, Long],
  val currentJars: HashMap[String, Long],
  val currentArchives: HashMap[String, Long],
  val replClassDirUri: Option[String])

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with YARN, kubernetes and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false,
    uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler,
    resources: immutable.Map[String, ResourceInformation])
  extends Logging {

  logInfo(log"Starting executor ID ${LogMDC(LogKeys.EXECUTOR_ID, executorId)}" +
    log" on host ${LogMDC(HOST, executorHostname)}")
  logInfo(log"OS info ${LogMDC(OS_NAME, System.getProperty("os.name"))}," +
    log" ${LogMDC(OS_VERSION, System.getProperty("os.version"))}, " +
    log"${LogMDC(OS_ARCH, System.getProperty("os.arch"))}")
  logInfo(log"Java version ${LogMDC(JAVA_VERSION, System.getProperty("java.version"))}")

  private val executorShutdown = new AtomicBoolean(false)
  val stopHookReference = ShutdownHookManager.addShutdownHook(
    () => stop()
  )

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private[executor] val conf = env.conf

  // SPARK-48131: Unify MDC key mdc.taskName and task_name in Spark 4.0 release.
  private[executor] val taskNameMDCKey = if (conf.get(LEGACY_TASK_NAME_MDC_ENABLED)) {
    "mdc.taskName"
  } else {
    LogKeys.TASK_NAME.name
  }

  // SPARK-40235: updateDependencies() uses a ReentrantLock instead of the `synchronized` keyword
  // so that tasks can exit quickly if they are interrupted while waiting on another task to
  // finish downloading dependencies.
  private val updateDependenciesLock = new ReentrantLock()

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname)
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // Start worker thread pool
  // Use UninterruptibleThread to run tasks so that we can allow running codes without being
  // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
  // will hang forever if some methods are interrupted.
  private[executor] val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }
  private val schemes = conf.get(EXECUTOR_METRICS_FILESYSTEM_SCHEMES)
    .toLowerCase(Locale.ROOT).split(",").map(_.trim).filter(_.nonEmpty)
  private val executorSource = new ExecutorSource(threadPool, executorId, schemes)
  // Pool used for threads that supervise task killing / cancellation
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // For tasks which are in the process of being killed, this map holds the most recently created
  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
  // create. The map key is a task id.
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  val executorMetricsSource =
    if (conf.get(METRICS_EXECUTORMETRICS_SOURCE_ENABLED)) {
      Some(new ExecutorMetricsSource)
    } else {
      None
    }

  if (!isLocal) {
    env.blockManager.initialize(conf.getAppId)
    env.metricsSystem.registerSource(executorSource)
    env.metricsSystem.registerSource(new JVMCPUSource())
    executorMetricsSource.foreach(_.register(env.metricsSystem))
    env.metricsSystem.registerSource(env.blockManager.shuffleMetricsSource)
  } else {
    // This enable the registration of the executor source in local mode.
    // The actual registration happens in SparkContext,
    // it cannot be done here as the appId is not available yet
    Executor.executorSourceLocalModeOnly = executorSource
  }

  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.get(EXECUTOR_USER_CLASS_PATH_FIRST_CONFIG)

  // Whether to monitor killed / interrupted tasks
  private val taskReaperEnabled = conf.get(TASK_REAPER_ENABLED)

  private val killOnFatalErrorDepth = conf.get(KILL_ON_FATAL_ERROR_DEPTH)

  private val systemLoader = Utils.getContextOrSparkClassLoader

  private def newSessionState(jobArtifactState: JobArtifactState): IsolatedSessionState = {
    val currentFiles = new HashMap[String, Long]
    val currentJars = new HashMap[String, Long]
    val currentArchives = new HashMap[String, Long]
    val urlClassLoader =
      createClassLoader(currentJars, isStubbingEnabledForState(jobArtifactState.uuid))
    val replClassLoader = addReplClassLoaderIfNeeded(
      urlClassLoader, jobArtifactState.replClassDirUri, jobArtifactState.uuid)
    new IsolatedSessionState(
      jobArtifactState.uuid, urlClassLoader, replClassLoader,
      currentFiles,
      currentJars,
      currentArchives,
      jobArtifactState.replClassDirUri
    )
  }

  private def isStubbingEnabledForState(name: String) = {
    !isDefaultState(name) &&
      conf.get(CONNECT_SCALA_UDF_STUB_PREFIXES).nonEmpty
  }

  private def isDefaultState(name: String) = name == "default"

  // Classloader isolation
  // The default isolation group
  val defaultSessionState: IsolatedSessionState = newSessionState(JobArtifactState("default", None))

  val isolatedSessionCache: Cache[String, IsolatedSessionState] = CacheBuilder.newBuilder()
    .maximumSize(100)
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .removalListener(new RemovalListener[String, IsolatedSessionState]() {
      override def onRemoval(
          notification: RemovalNotification[String, IsolatedSessionState]): Unit = {
        val state = notification.getValue
        // Cache is always used for isolated sessions.
        assert(!isDefaultState(state.sessionUUID))
        val sessionBasedRoot = new File(SparkFiles.getRootDirectory(), state.sessionUUID)
        if (sessionBasedRoot.isDirectory && sessionBasedRoot.exists()) {
          Utils.deleteRecursively(sessionBasedRoot)
        }
        logInfo(log"Session evicted: ${LogMDC(SESSION_ID, state.sessionUUID)}")
      }
    })
    .build[String, IsolatedSessionState]

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(defaultSessionState.replClassLoader)
  // SPARK-21928.  SerializerManager's internal instance of Kryo might get used in netty threads
  // for fetching remote cached RDD blocks, so need to make sure it uses the right classloader too.
  env.serializerManager.setDefaultClassLoader(defaultSessionState.replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back. This is guaranteed to be smaller than array bytes limit (2GB)
  private val maxDirectResultSize = Math.min(
    conf.get(TASK_MAX_DIRECT_RESULT_SIZE),
    RpcUtils.maxMessageSizeBytes(conf))

  private val maxResultSize = conf.get(MAX_RESULT_SIZE)

  // Maintains the list of running tasks.
  private[executor] val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Kill mark TTL in milliseconds - 10 seconds.
  private val KILL_MARK_TTL_MS = 10000L

  // Kill marks with interruptThread flag, kill reason and timestamp.
  // This is to avoid dropping the kill event when killTask() is called before launchTask().
  private[executor] val killMarks = new ConcurrentHashMap[Long, (Boolean, String, Long)]

  private val killMarkCleanupTask = new Runnable {
    override def run(): Unit = {
      val oldest = System.currentTimeMillis() - KILL_MARK_TTL_MS
      val iter = killMarks.entrySet().iterator()
      while (iter.hasNext) {
        if (iter.next().getValue._3 < oldest) {
          iter.remove()
        }
      }
    }
  }

  // Kill mark cleanup thread executor.
  private val killMarkCleanupService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-kill-mark-cleanup")

  killMarkCleanupService.scheduleAtFixedRate(
    killMarkCleanupTask, KILL_MARK_TTL_MS, KILL_MARK_TTL_MS, TimeUnit.MILLISECONDS)

  /**
   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
   * times, it should kill itself. The default value is 60. For example, if max failures is 60 and
   * heartbeat interval is 10s, then it will try to send heartbeats for up to 600s (10 minutes).
   */
  private val HEARTBEAT_MAX_FAILURES = conf.get(EXECUTOR_HEARTBEAT_MAX_FAILURES)

  /**
   * Whether to drop empty accumulators from heartbeats sent to the driver. Including the empty
   * accumulators (that satisfy isZero) can make the size of the heartbeat message very large.
   */
  private val HEARTBEAT_DROP_ZEROES = conf.get(EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES)

  /**
   * Interval to send heartbeats, in milliseconds
   */
  private val HEARTBEAT_INTERVAL_MS = conf.get(EXECUTOR_HEARTBEAT_INTERVAL)

  /**
   * Interval to poll for executor metrics, in milliseconds
   */
  private val METRICS_POLLING_INTERVAL_MS = conf.get(EXECUTOR_METRICS_POLLING_INTERVAL)

  private val pollOnHeartbeat = if (METRICS_POLLING_INTERVAL_MS > 0) false else true

  // Poller for the memory metrics. Visible for testing.
  private[executor] val metricsPoller = new ExecutorMetricsPoller(
    env.memoryManager,
    METRICS_POLLING_INTERVAL_MS,
    executorMetricsSource)

  // Executor for the heartbeat task.
  private val heartbeater = new Heartbeater(
    () => Executor.this.reportHeartBeat(),
    "executor-heartbeater",
    HEARTBEAT_INTERVAL_MS)

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
   * successful heartbeat will reset it to 0.
   */
  private var heartbeatFailures = 0

  /**
   * Flag to prevent launching new tasks while decommissioned. There could be a race condition
   * accessing this, but decommissioning is only intended to help not be a hard stop.
   */
  private var decommissioned = false

  heartbeater.start()

  private val appStartTime = conf.getLong("spark.app.startTime", 0)

  // To allow users to distribute plugins and their required files
  // specified by --jars, --files and --archives on application submission, those
  // jars/files/archives should be downloaded and added to the class loader via
  // updateDependencies. This should be done before plugin initialization below
  // because executors search plugins from the class loader and initialize them.
  private val Seq(initialUserJars, initialUserFiles, initialUserArchives) =
    Seq("jar", "file", "archive").map { key =>
      conf.getOption(s"spark.app.initial.$key.urls").map { urls =>
        import org.apache.spark.util.ArrayImplicits._
        immutable.Map(urls.split(",").map(url => (url, appStartTime)).toImmutableArraySeq: _*)
      }.getOrElse(immutable.Map.empty)
    }
  updateDependencies(initialUserFiles, initialUserJars, initialUserArchives, defaultSessionState)

  // Plugins and shuffle managers need to load using a class loader that includes the executor's
  // user classpath. Plugins also needs to be initialized after the heartbeater started
  // to avoid blocking to send heartbeat (see SPARK-32175 and SPARK-45762).
  private val plugins: Option[PluginContainer] =
    Utils.withContextClassLoader(defaultSessionState.replClassLoader) {
      PluginContainer(env, resources.asJava)
    }

  // Skip local mode because the ShuffleManager is already initialized
  if (!isLocal) {
    Utils.withContextClassLoader(defaultSessionState.replClassLoader) {
      env.initializeShuffleManager()
    }
  }

  metricsPoller.start()

  private[executor] def numRunningTasks: Int = runningTasks.size()

  /**
   * Mark an executor for decommissioning and avoid launching new tasks.
   */
  private[spark] def decommission(): Unit = {
    decommissioned = true
  }

  private[executor] def createTaskRunner(context: ExecutorBackend,
    taskDescription: TaskDescription) = new TaskRunner(context, taskDescription, plugins)

  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    val taskId = taskDescription.taskId
    val tr = createTaskRunner(context, taskDescription)
    runningTasks.put(taskId, tr)
    val killMark = killMarks.get(taskId)
    if (killMark != null) {
      tr.kill(killMark._1, killMark._2)
      killMarks.remove(taskId)
    }
    threadPool.execute(tr)
    if (decommissioned) {
      log.error(s"Launching a task while in decommissioned state.")
    }
  }

  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    killMarks.put(taskId, (interruptThread, reason, System.currentTimeMillis()))
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // Execute the TaskReaper from outside of the synchronized block.
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
      // Safe to remove kill mark as we got a chance with the TaskRunner.
      killMarks.remove(taskId)
    }
  }

  /**
   * Function to kill the running tasks in an executor.
   * This can be called by executor back-ends to kill the
   * tasks instead of taking the JVM down.
   * @param interruptThread whether to interrupt the task thread
   */
  def killAllTasks(interruptThread: Boolean, reason: String) : Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  def stop(): Unit = {
    if (!executorShutdown.getAndSet(true)) {
      ShutdownHookManager.removeShutdownHook(stopHookReference)
      env.metricsSystem.report()
      try {
        if (metricsPoller != null) {
          metricsPoller.stop()
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Unable to stop executor metrics poller", e)
      }
      try {
        if (heartbeater != null) {
          heartbeater.stop()
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Unable to stop heartbeater", e)
      }
      ShuffleBlockPusher.stop()
      if (threadPool != null) {
        threadPool.shutdown()
      }
      if (killMarkCleanupService != null) {
        killMarkCleanupService.shutdown()
      }
      if (defaultSessionState != null && plugins != null) {
        // Notify plugins that executor is shutting down so they can terminate cleanly
        Utils.withContextClassLoader(defaultSessionState.replClassLoader) {
          plugins.foreach(_.shutdown())
        }
      }
      if (!isLocal) {
        env.stop()
      }
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  class TaskRunner(
      execBackend: ExecutorBackend,
      val taskDescription: TaskDescription,
      private val plugins: Option[PluginContainer])
    extends Runnable {

    val taskId = taskDescription.taskId
    val taskName = taskDescription.name
    val threadName = s"Executor task launch worker for $taskName"
    val mdcProperties = taskDescription.properties.asScala
      .filter(_._1.startsWith("mdc.")).toSeq

    /** If specified, this task has been killed and this option contains the reason. */
    @volatile private var reasonIfKilled: Option[String] = None

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** Whether this task has been finished. */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized { finished }

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _

    /**
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
     */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(log"Executor is trying to kill ${LogMDC(TASK_NAME, taskName)}," +
        log" reason: ${LogMDC(REASON, reason)}")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    /**
     * Set the finished flag to true and clear the current thread's interrupt status
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
      // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
      // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
      // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:
      notifyAll()
    }

    /**
     *  Utility function to:
     *    1. Report executor runtime and JVM gc time if possible
     *    2. Collect accumulator updates
     *    3. Set the finished flag to true and clear current thread's interrupt status
     */
    private def collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs: Long) = {
      // Report executor runtime and JVM gc time
      Option(task).foreach(t => {
        t.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis(
          // SPARK-32898: it's possible that a task is killed when taskStartTimeNs has the initial
          // value(=0) still. In this case, the executorRunTime should be considered as 0.
          if (taskStartTimeNs > 0) System.nanoTime() - taskStartTimeNs else 0))
        t.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
      })

      // Collect latest accumulator values to report back to the driver
      val accums: Seq[AccumulatorV2[_, _]] =
        Option(task).map(_.collectAccumulatorUpdates(taskFailed = true)).getOrElse(Seq.empty)
      val accUpdates = accums.map(acc => acc.toInfoUpdate)

      setTaskFinishedAndClearInterruptStatus()
      (accums, accUpdates)
    }

    override def run(): Unit = {

      // Classloader isolation
      val isolatedSession = taskDescription.artifacts.state match {
        case Some(jobArtifactState) =>
          isolatedSessionCache.get(jobArtifactState.uuid, () => newSessionState(jobArtifactState))
        case _ => defaultSessionState
      }

      setMDCForTask(taskName, mdcProperties)
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      val threadMXBean = ManagementFactory.getThreadMXBean
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTimeNs = System.nanoTime()
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      Thread.currentThread.setContextClassLoader(isolatedSession.replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(log"Running ${LogMDC(TASK_NAME, taskName)}")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStartTimeNs: Long = 0
      var taskStartCpu: Long = 0
      startGCTime = computeTotalGcTime()
      var taskStarted: Boolean = false

      try {
        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        Executor.taskDeserializationProps.set(taskDescription.properties)

        updateDependencies(
          taskDescription.artifacts.files,
          taskDescription.artifacts.jars,
          taskDescription.artifacts.archives,
          isolatedSession)
        // Always reset the thread class loader to ensure if any updates, all threads (not only
        // the thread that updated the dependencies) can update to the new class loader.
        Thread.currentThread.setContextClassLoader(isolatedSession.replClassLoader)
        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        task.localProperties = taskDescription.properties
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        val killReason = reasonIfKilled
        if (killReason.isDefined) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException(killReason.get)
        }

        // The purpose of updating the epoch here is to invalidate executor map output status cache
        // in case FetchFailures have occurred. In local mode `env.mapOutputTracker` will be
        // MapOutputTrackerMaster and its cache invalidation is not based on epoch numbers so
        // we don't need to make any special calls here.
        if (!isLocal) {
          logDebug(s"$taskName's epoch is ${task.epoch}")
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
        }

        metricsPoller.onTaskStart(taskId, task.stageId, task.stageAttemptId)
        taskStarted = true

        // Run the actual task and measure its runtime.
        taskStartTimeNs = System.nanoTime()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        // Convert resources amounts info to ResourceInformation
        val resources = taskDescription.resources.map { case (rName, addressesAmounts) =>
          rName -> new ResourceInformation(rName, addressesAmounts.keys.toSeq.sorted.toArray)
        }
        val value = Utils.tryWithSafeFinally {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem,
            cpus = taskDescription.cpus,
            resources = resources,
            plugins = plugins)
          threwException = false
          res
        } {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = log"Managed memory leak detected; size = " +
              log"${LogMDC(NUM_BYTES, freedMemory)} bytes, ${LogMDC(TASK_NAME, taskName)}"
            if (conf.get(UNSAFE_EXCEPTION_ON_MEMORY_LEAK)) {
              throw SparkException.internalError(errMsg.message, category = "EXECUTOR")
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              log"${LogMDC(NUM_RELEASED_LOCKS, releasedLocks.size)} block locks" +
                log" were not released by ${LogMDC(TASK_NAME, taskName)}\n" +
                log" ${LogMDC(RELEASED_LOCKS, releasedLocks.mkString("[", ", ", "]"))})"
            if (conf.get(STORAGE_EXCEPTION_PIN_LEAK)) {
              throw SparkException.internalError(errMsg.message, category = "EXECUTOR")
            } else {
              logInfo(errMsg)
            }
          }
        }
        task.context.fetchFailed.foreach { fetchFailure =>
          // uh-oh.  it appears the user code has caught the fetch-failure without throwing any
          // other exceptions.  Its *possible* this is what the user meant to do (though highly
          // unlikely).  So we will log an error and keep going.
          logError(log"${LogMDC(TASK_NAME, taskName)} completed successfully though internally " +
            log"it encountered unrecoverable fetch failures! Most likely this means user code " +
            log"is incorrectly swallowing Spark's internal " +
            log"${LogMDC(CLASS_NAME, classOf[FetchFailedException])}", fetchFailure)
        }
        val taskFinishNs = System.nanoTime()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // If the task has been killed, let's fail it.
        task.context.killTaskIfInterrupted()

        val resultSer = env.serializer.newInstance()
        val beforeSerializationNs = System.nanoTime()
        val valueByteBuffer = SerializerHelper.serializeToChunkedBuffer(resultSer, value)
        val afterSerializationNs = System.nanoTime()

        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        task.metrics.setExecutorDeserializeTime(TimeUnit.NANOSECONDS.toMillis(
          (taskStartTimeNs - deserializeStartTimeNs) + task.executorDeserializeTimeNs))
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis(
          (taskFinishNs - taskStartTimeNs) - task.executorDeserializeTimeNs))
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(TimeUnit.NANOSECONDS.toMillis(
          afterSerializationNs - beforeSerializationNs))
        task.metrics.setPeakOnHeapExecutionMemory(taskMemoryManager.getPeakOnHeapExecutionMemory)
        task.metrics.setPeakOffHeapExecutionMemory(taskMemoryManager.getPeakOffHeapExecutionMemory)
        // Expose task metrics using the Dropwizard metrics system.
        // Update task metrics counters
        executorSource.METRIC_CPU_TIME.inc(task.metrics.executorCpuTime)
        executorSource.METRIC_RUN_TIME.inc(task.metrics.executorRunTime)
        executorSource.METRIC_JVM_GC_TIME.inc(task.metrics.jvmGCTime)
        executorSource.METRIC_DESERIALIZE_TIME.inc(task.metrics.executorDeserializeTime)
        executorSource.METRIC_DESERIALIZE_CPU_TIME.inc(task.metrics.executorDeserializeCpuTime)
        executorSource.METRIC_RESULT_SERIALIZE_TIME.inc(task.metrics.resultSerializationTime)
        executorSource.METRIC_INPUT_BYTES_READ
          .inc(task.metrics.inputMetrics.bytesRead)
        executorSource.METRIC_INPUT_RECORDS_READ
          .inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_OUTPUT_BYTES_WRITTEN
          .inc(task.metrics.outputMetrics.bytesWritten)
        executorSource.METRIC_OUTPUT_RECORDS_WRITTEN
          .inc(task.metrics.outputMetrics.recordsWritten)
        executorSource.METRIC_RESULT_SIZE.inc(task.metrics.resultSize)
        executorSource.METRIC_DISK_BYTES_SPILLED.inc(task.metrics.diskBytesSpilled)
        executorSource.METRIC_MEMORY_BYTES_SPILLED.inc(task.metrics.memoryBytesSpilled)
        incrementShuffleMetrics(executorSource, task.metrics)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId)
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueByteBuffer, accumUpdates, metricPeaks)
        // try to estimate a reasonable upper bound of DirectTaskResult serialization
        val serializedDirectResult = SerializerHelper.serializeToChunkedBuffer(ser, directResult,
          valueByteBuffer.size + accumUpdates.size * 32 + metricPeaks.length * 8)
        val resultSize = serializedDirectResult.size

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(log"Finished ${LogMDC(TASK_NAME, taskName)}. " +
              log"Result is larger than maxResultSize " +
              log"(${LogMDC(RESULT_SIZE_BYTES, Utils.bytesToString(resultSize))} > " +
              log"${LogMDC(RESULT_SIZE_BYTES_MAX, Utils.bytesToString(maxResultSize))}), " +
              log"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              serializedDirectResult,
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(log"Finished ${LogMDC(TASK_NAME, taskName)}." +
              log" ${LogMDC(NUM_BYTES, resultSize)} bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(log"Finished ${LogMDC(TASK_NAME, taskName)}." +
              log" ${LogMDC(NUM_BYTES, resultSize)} bytes result sent to driver")
            // toByteBuffer is safe here, guarded by maxDirectResultSize
            serializedDirectResult.toByteBuffer
          }
        }

        executorSource.SUCCEEDED_TASKS.inc(1L)
        setTaskFinishedAndClearInterruptStatus()
        plugins.foreach(_.onTaskSucceeded())
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
      } catch {
        case t: TaskKilledException =>
          logInfo(log"Executor killed ${LogMDC(TASK_NAME, taskName)}," +
            log" reason: ${LogMDC(REASON, t.reason)}")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          // Here and below, put task metric peaks in an immutable.ArraySeq to expose them as an
          // immutable.Seq without requiring a copy.
          val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId).toImmutableArraySeq
          val reason = TaskKilled(t.reason, accUpdates, accums, metricPeaks)
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(log"Executor interrupted and killed ${LogMDC(TASK_NAME, taskName)}," +
            log" reason: ${LogMDC(REASON, killReason)}")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId).toImmutableArraySeq
          val reason = TaskKilled(killReason, accUpdates, accums, metricPeaks)
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case t: Throwable if hasFetchFailure && !Executor.isFatalError(t, killOnFatalErrorDepth) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // there was a fetch failure in the task, but some user code wrapped that exception
            // and threw something else.  Regardless, we treat it as a fetch failure.
            logWarning(log"${LogMDC(TASK_NAME, taskName)} encountered a " +
              log"${LogMDC(CLASS_NAME, classOf[FetchFailedException].getName)} " +
              log"and failed, but the " +
              log"${LogMDC(CLASS_NAME, classOf[FetchFailedException].getName)} " +
              log"was hidden by another exception. Spark is handling this like a fetch failure " +
              log"and ignoring the other exception: ${LogMDC(ERROR, t)}")
          }
          setTaskFinishedAndClearInterruptStatus()
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskCommitDeniedReason
          setTaskFinishedAndClearInterruptStatus()
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case t: Throwable if env.isStopped =>
          // Log the expected exception after executor.stop without stack traces
          // see: SPARK-19147
          logError(log"Exception in ${LogMDC(TASK_NAME, taskName)}: ${LogMDC(ERROR, t.getMessage)}")

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(log"Exception in ${LogMDC(TASK_NAME, taskName)}", t)

          // SPARK-20904: Do not report failure to driver if if happened during shut down. Because
          // libraries may set up shutdown hooks that race with running tasks during shutdown,
          // spurious failures may occur and can result in improper accounting in the driver (e.g.
          // the task failure would not be ignored if the shutdown happened because of preemption,
          // instead of an app issue).
          if (!ShutdownHookManager.inShutdown()) {
            val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
            val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId).toImmutableArraySeq

            val (taskFailureReason, serializedTaskFailureReason) = {
              try {
                val ef = new ExceptionFailure(t, accUpdates).withAccums(accums)
                  .withMetricPeaks(metricPeaks)
                (ef, ser.serialize(ef))
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  val ef = new ExceptionFailure(t, accUpdates, false).withAccums(accums)
                    .withMetricPeaks(metricPeaks)
                  (ef, ser.serialize(ef))
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            plugins.foreach(_.onTaskFailed(taskFailureReason))
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskFailureReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Executor.isFatalError(t, killOnFatalErrorDepth)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }
      } finally {
        cleanMDCForTask(taskName, mdcProperties)
        runningTasks.remove(taskId)
        if (taskStarted) {
          // This means the task was successfully deserialized, its stageId and stageAttemptId
          // are known, and metricsPoller.onTaskStart was called.
          metricsPoller.onTaskCompletion(taskId, task.stageId, task.stageAttemptId)
        }
      }
    }

    private def incrementShuffleMetrics(
      executorSource: ExecutorSource,
      metrics: TaskMetrics
    ): Unit = {
      executorSource.METRIC_SHUFFLE_FETCH_WAIT_TIME
        .inc(metrics.shuffleReadMetrics.fetchWaitTime)
      executorSource.METRIC_SHUFFLE_WRITE_TIME.inc(metrics.shuffleWriteMetrics.writeTime)
      executorSource.METRIC_SHUFFLE_TOTAL_BYTES_READ
        .inc(metrics.shuffleReadMetrics.totalBytesRead)
      executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ
        .inc(metrics.shuffleReadMetrics.remoteBytesRead)
      executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK
        .inc(metrics.shuffleReadMetrics.remoteBytesReadToDisk)
      executorSource.METRIC_SHUFFLE_LOCAL_BYTES_READ
        .inc(metrics.shuffleReadMetrics.localBytesRead)
      executorSource.METRIC_SHUFFLE_RECORDS_READ
        .inc(metrics.shuffleReadMetrics.recordsRead)
      executorSource.METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.remoteBlocksFetched)
      executorSource.METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.localBlocksFetched)
      executorSource.METRIC_SHUFFLE_REMOTE_REQS_DURATION
        .inc(metrics.shuffleReadMetrics.remoteReqsDuration)
      executorSource.METRIC_SHUFFLE_BYTES_WRITTEN
        .inc(metrics.shuffleWriteMetrics.bytesWritten)
      executorSource.METRIC_SHUFFLE_RECORDS_WRITTEN
        .inc(metrics.shuffleWriteMetrics.recordsWritten)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_CORRUPT_MERGED_BLOCK_CHUNKS
        .inc(metrics.shuffleReadMetrics.corruptMergedBlockChunks)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_FETCH_FALLBACK_COUNT
        .inc(metrics.shuffleReadMetrics.mergedFetchFallbackCount)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.remoteMergedBlocksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.localMergedBlocksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_CHUNKS_FETCHED
        .inc(metrics.shuffleReadMetrics.remoteMergedChunksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_CHUNKS_FETCHED
        .inc(metrics.shuffleReadMetrics.localMergedChunksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BYTES_READ
        .inc(metrics.shuffleReadMetrics.remoteMergedBytesRead)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BYTES_READ
        .inc(metrics.shuffleReadMetrics.localMergedBytesRead)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_REQS_DURATION
        .inc(metrics.shuffleReadMetrics.remoteMergedReqsDuration)
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }

    private[executor] def theadDump(): Option[ThreadStackTrace] = {
      Utils.getThreadDumpForThread(getThreadId)
    }
  }

  private def setMDCForTask(taskName: String, mdc: Seq[(String, String)]): Unit = {
    try {
      mdc.foreach { case (key, value) => MDC.put(key, value) }
      // avoid overriding the takName by the user
      MDC.put(taskNameMDCKey, taskName)
    } catch {
      case _: NoSuchFieldError => logInfo("MDC is not supported.")
    }
  }

  private def cleanMDCForTask(taskName: String, mdc: Seq[(String, String)]): Unit = {
    try {
      mdc.foreach { case (key, _) => MDC.remove(key) }
      MDC.remove(taskNameMDCKey)
    } catch {
      case _: NoSuchFieldError => logInfo("MDC is not supported.")
    }
  }

  /**
   * Supervises the killing / cancellation of a task by sending the interrupted flag, optionally
   * sending a Thread.interrupt(), and monitoring the task until it finishes.
   *
   * Spark's current task cancellation / task killing mechanism is "best effort" because some tasks
   * may not be interruptible or may not respond to their "killed" flags being set. If a significant
   * fraction of a cluster's task slots are occupied by tasks that have been marked as killed but
   * remain running then this can lead to a situation where new jobs and tasks are starved of
   * resources that are being used by these zombie tasks.
   *
   * The TaskReaper was introduced in SPARK-18761 as a mechanism to monitor and clean up zombie
   * tasks. For backwards-compatibility / backportability this component is disabled by default
   * and must be explicitly enabled by setting `spark.task.reaper.enabled=true`.
   *
   * A TaskReaper is created for a particular task when that task is killed / cancelled. Typically
   * a task will have only one TaskReaper, but it's possible for a task to have up to two reapers
   * in case kill is called twice with different values for the `interrupt` parameter.
   *
   * Once created, a TaskReaper will run until its supervised task has finished running. If the
   * TaskReaper has not been configured to kill the JVM after a timeout (i.e. if
   * `spark.task.reaper.killTimeout < 0`) then this implies that the TaskReaper may run indefinitely
   * if the supervised task never exits.
   */
  private class TaskReaper(
      taskRunner: TaskRunner,
      val interruptThread: Boolean,
      val reason: String)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long = conf.get(TASK_REAPER_POLLING_INTERVAL)

    private[this] val killTimeoutNs: Long = {
      TimeUnit.MILLISECONDS.toNanos(conf.get(TASK_REAPER_KILL_TIMEOUT))
    }

    private[this] val takeThreadDump: Boolean = conf.get(TASK_REAPER_THREAD_DUMP)

    override def run(): Unit = {
      setMDCForTask(taskRunner.taskName, taskRunner.mdcProperties)
      val startTimeNs = System.nanoTime()
      def elapsedTimeNs = System.nanoTime() - startTimeNs
      def timeoutExceeded(): Boolean = killTimeoutNs > 0 && elapsedTimeNs > killTimeoutNs
      try {
        // Only attempt to kill the task once. If interruptThread = false then a second kill
        // attempt would be a no-op and if interruptThread = true then it may not be safe or
        // effective to interrupt multiple times:
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // Monitor the killed task until it exits. The synchronization logic here is complicated
        // because we don't want to synchronize on the taskRunner while possibly taking a thread
        // dump, but we also need to be careful to avoid races between checking whether the task
        // has finished and wait()ing for it to finish.
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // We need to synchronize on the TaskRunner while checking whether the task has
            // finished in order to avoid a race where the task is marked as finished right after
            // we check and before we call wait().
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
            logWarning(log"Killed task ${LogMDC(TASK_ID, taskId)} " +
              log"is still running after ${LogMDC(TIME_UNITS, elapsedTimeMs)} ms")
            if (takeThreadDump) {
              try {
                taskRunner.theadDump().foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(log"Thread dump from task ${LogMDC(TASK_ID, taskId)}:\n" +
                      log"${LogMDC(THREAD, thread.toString)}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          val killTimeoutMs = TimeUnit.NANOSECONDS.toMillis(killTimeoutNs)
          if (isLocal) {
            logError(log"Killed task ${LogMDC(TASK_ID, taskId)} could not be stopped within " +
              log"${LogMDC(TIMEOUT, killTimeoutMs)} ms; " +
              log"not killing JVM because we are running in local mode.")
          } else {
            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
            // handler and cause the executor JVM to exit.
            throw new KilledByTaskReaperException(s"Killing executor JVM because killed task " +
              s"$taskId could not be stopped within $killTimeoutMs ms.")
          }
        }
      } finally {
        cleanMDCForTask(taskRunner.taskName, taskRunner.mdcProperties)
        // Clean up entries in the taskReaperForTask map.
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // This must have been a TaskReaper where interruptThread == false where a subsequent
              // killTask() call for the same task had interruptThread == true and overwrote the
              // map entry.
            }
          }
        }
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(
      currentJars: HashMap[String, Long],
      useStub: Boolean): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    createClassLoader(urls, useStub)
  }

  private def createClassLoader(urls: Array[URL], useStub: Boolean): MutableURLClassLoader = {
    logInfo(
      log"Starting executor with user classpath" +
        log" (userClassPathFirst =" +
        log" ${LogMDC(LogKeys.EXECUTOR_USER_CLASS_PATH_FIRST, userClassPathFirst)}): " +
        log"${LogMDC(URLS, urls.mkString("'", ",", "'"))}"
    )

    if (useStub) {
      createClassLoaderWithStub(urls, conf.get(CONNECT_SCALA_UDF_STUB_PREFIXES))
    } else {
      createClassLoader(urls)
    }
  }

  private def createClassLoader(urls: Array[URL]): MutableURLClassLoader = {
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, systemLoader)
    } else {
      new MutableURLClassLoader(urls, systemLoader)
    }
  }

  private def createClassLoaderWithStub(
      urls: Array[URL],
      binaryName: Seq[String]): MutableURLClassLoader = {
    if (userClassPathFirst) {
      // user -> (sys -> stub)
      val stubClassLoader =
        StubClassLoader(systemLoader, binaryName)
      new ChildFirstURLClassLoader(urls, stubClassLoader)
    } else {
      // sys -> user -> stub
      val stubClassLoader =
        StubClassLoader(null, binaryName)
      new ChildFirstURLClassLoader(urls, stubClassLoader, systemLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(
      parent: ClassLoader,
      sessionClassUri: Option[String],
      sessionUUID: String): ClassLoader = {
    val classUri = sessionClassUri.getOrElse(conf.get("spark.repl.class.uri", null))
    val classLoader = if (classUri != null) {
      logInfo(log"Using REPL class URI: ${LogMDC(LogKeys.URI, classUri)}")
      new ExecutorClassLoader(conf, env, classUri, parent, userClassPathFirst)
    } else {
      parent
    }
    logInfo(log"Created or updated repl class loader ${LogMDC(CLASS_LOADER, classLoader)}" +
      log" for ${LogMDC(SESSION_ID, sessionUUID)}.")
    classLoader
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   * Visible for testing.
   */
  private[executor] def updateDependencies(
      newFiles: immutable.Map[String, Long],
      newJars: immutable.Map[String, Long],
      newArchives: immutable.Map[String, Long],
      state: IsolatedSessionState,
      testStartLatch: Option[CountDownLatch] = None,
      testEndLatch: Option[CountDownLatch] = None): Unit = {
    var renewClassLoader = false;
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    updateDependenciesLock.lockInterruptibly()
    try {
      // For testing, so we can simulate a slow file download:
      testStartLatch.foreach(_.countDown())

      // If the session ID was specified from SparkSession, it's from a Spark Connect client.
      // Specify a dedicated directory for Spark Connect client.
      lazy val root = if (!isDefaultState(state.sessionUUID)) {
        val newDest = new File(SparkFiles.getRootDirectory(), state.sessionUUID)
        newDest.mkdir()
        newDest
      } else {
        new File(SparkFiles.getRootDirectory())
      }

      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if state.currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo(log"Fetching ${LogMDC(FILE_NAME, name)} with" +
          log" timestamp ${LogMDC(TIMESTAMP, timestamp)}")
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, root, conf, hadoopConf, timestamp, useCache = !isLocal)
        state.currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newArchives if
          state.currentArchives.getOrElse(name, -1L) < timestamp) {
        logInfo(log"Fetching ${LogMDC(ARCHIVE_NAME, name)} with" +
          log" timestamp ${LogMDC(TIMESTAMP, timestamp)}")
        val sourceURI = new URI(name)
        val uriToDownload = Utils.getUriBuilder(sourceURI).fragment(null).build()
        val source = Utils.fetchFile(uriToDownload.toString, Utils.createTempDir(), conf,
          hadoopConf, timestamp, useCache = !isLocal, shouldUntar = false)
        val dest = new File(
          root,
          if (sourceURI.getFragment != null) sourceURI.getFragment else source.getName)
        logInfo(
          log"Unpacking an archive ${LogMDC(ARCHIVE_NAME, name)}" +
            log" (${LogMDC(BYTE_SIZE, source.length)} bytes)" +
            log" from ${LogMDC(SOURCE_PATH, source.getAbsolutePath)}" +
            log" to ${LogMDC(DESTINATION_PATH, dest.getAbsolutePath)}")
        Utils.deleteRecursively(dest)
        Utils.unpack(source, dest)
        state.currentArchives(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = state.currentJars.get(name)
          .orElse(state.currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo(log"Fetching ${LogMDC(JAR_URL, name)} with" +
            log" timestamp ${LogMDC(TIMESTAMP, timestamp)}")
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, root, conf,
            hadoopConf, timestamp, useCache = !isLocal)
          state.currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(root, localName).toURI.toURL
          if (!state.urlClassLoader.getURLs().contains(url)) {
            logInfo(log"Adding ${LogMDC(LogKeys.URL, url)} to" +
              log" class loader ${LogMDC(UUID, state.sessionUUID)}")
            state.urlClassLoader.addURL(url)
            if (isStubbingEnabledForState(state.sessionUUID)) {
              renewClassLoader = true
            }
          }
        }
      }
      if (renewClassLoader) {
        // Recreate the class loader to ensure all classes are updated.
        state.urlClassLoader = createClassLoader(state.urlClassLoader.getURLs, useStub = true)
        state.replClassLoader =
          addReplClassLoaderIfNeeded(state.urlClassLoader, state.replClassDirUri, state.sessionUUID)
      }
      // For testing, so we can simulate a slow file download:
      testEndLatch.foreach(_.await())
    } finally {
      updateDependenciesLock.unlock()
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    if (pollOnHeartbeat) {
      metricsPoller.poll()
    }

    val executorUpdates = metricsPoller.getExecutorUpdates()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        val accumulatorsToReport =
          if (HEARTBEAT_DROP_ZEROES) {
            taskRunner.task.metrics.accumulators().filterNot(_.isZero)
          } else {
            taskRunner.task.metrics.accumulators()
          }
        accumUpdates += ((taskRunner.taskId, accumulatorsToReport))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId,
      executorUpdates)
    try {
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
        message, new RpcTimeout(HEARTBEAT_INTERVAL_MS.millis, EXECUTOR_HEARTBEAT_INTERVAL.key))
      if (!executorShutdown.get && response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(log"Exit as unable to send heartbeats to driver " +
            log"more than ${LogMDC(MAX_ATTEMPTS, HEARTBEAT_MAX_FAILURES)} times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  def getTaskThreadDump(taskId: Long): Option[ThreadStackTrace] = {
    val runner = runningTasks.get(taskId)
    if (runner != null) {
      runner.theadDump()
    } else {
      logWarning(log"Failed to dump thread for task ${LogMDC(TASK_ID, taskId)}")
      None
    }
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]

  // Used to store executorSource, for local mode only
  var executorSourceLocalModeOnly: ExecutorSource = null

  /**
   * Whether a `Throwable` thrown from a task is a fatal error. We will use this to decide whether
   * to kill the executor.
   *
   * @param depthToCheck The max depth of the exception chain we should search for a fatal error. 0
   *                     means not checking any fatal error (in other words, return false), 1 means
   *                     checking only the exception but not the cause, and so on. This is to avoid
   *                     `StackOverflowError` when hitting a cycle in the exception chain.
   */
  @scala.annotation.tailrec
  def isFatalError(t: Throwable, depthToCheck: Int): Boolean = {
    if (depthToCheck <= 0) {
      false
    } else {
      t match {
        case _: SparkOutOfMemoryError => false
        case e if Utils.isFatalError(e) => true
        case e if e.getCause != null => isFatalError(e.getCause, depthToCheck - 1)
        case _ => false
      }
    }
  }
}

class KilledByTaskReaperException(message: String) extends SparkException(message)
