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

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.concurrent._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark._
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Spark executor used with Mesos, YARN, and the standalone scheduler.
 */
private[spark] class Executor(
    executorId: String,
    slaveHostname: String,
    properties: Seq[(String, String)],
    isLocal: Boolean = false)
  extends Logging
{
  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  @volatile private var isStopped = false

  // No ip or host:port - just hostname
  Utils.checkHost(slaveHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(slaveHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(slaveHostname)

  // Set spark.* properties from executor arg
  val conf = new SparkConf(true)
  conf.setAll(properties)

  // If we are in yarn mode, systems can have different disk layouts so we must set it
  // to what Yarn on this system said was available. This will be used later when SparkEnv
  // created.
  if (java.lang.Boolean.valueOf(
      System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))) {
    conf.set("spark.local.dir", getYarnLocalDirs())
  } else if (sys.env.contains("SPARK_LOCAL_DIRS")) {
    conf.set("spark.local.dir", sys.env("SPARK_LOCAL_DIRS"))
  }

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(ExecutorUncaughtExceptionHandler)
  }

  val executorSource = new ExecutorSource(this, executorId)

  // Initialize Spark environment (using system properties read above)
  private val env = {
    if (!isLocal) {
      val _env = SparkEnv.create(conf, executorId, slaveHostname, 0,
        isDriver = false, isLocal = false)
      SparkEnv.set(_env)
      _env.metricsSystem.registerSource(executorSource)
      _env
    } else {
      SparkEnv.get
    }
  }

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(urlClassLoader)

  // Akka's message frame size. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)

  // Start worker thread pool
  val threadPool = Utils.newDaemonCachedThreadPool("Executor task launch worker")

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  startDriverHeartbeater()

  def launchTask(
      context: ExecutorBackend, taskId: Long, taskName: String, serializedTask: ByteBuffer) {
    val tr = new TaskRunner(context, taskId, taskName, serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean) {
    val tr = runningTasks.get(taskId)
    if (tr != null) {
      tr.kill(interruptThread)
    }
  }

  def stop() {
    env.metricsSystem.report()
    isStopped = true
    threadPool.shutdown()
  }

  /** Get the Yarn approved local directories. */
  private def getYarnLocalDirs(): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(System.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(System.getenv("LOCAL_DIRS"))
      .getOrElse(""))

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  class TaskRunner(
      execBackend: ExecutorBackend, val taskId: Long, taskName: String, serializedTask: ByteBuffer)
    extends Runnable {

    @volatile private var killed = false
    @volatile var task: Task[Any] = _
    @volatile var attemptedTask: Option[Task[Any]] = None

    def kill(interruptThread: Boolean) {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId)")
      killed = true
      if (task != null) {
        task.kill(interruptThread)
      }
    }

    override def run() {
      val startTime = System.currentTimeMillis()
      SparkEnv.set(env)
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = SparkEnv.get.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      def gcTime = ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).sum
      val startGCTime = gcTime

      try {
        SparkEnv.set(env)
        Accumulators.clear()
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
        updateDependencies(taskFiles, taskJars)
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        attemptedTask = Some(task)
        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        val value = task.run(taskId.toInt)
        val taskFinish = System.currentTimeMillis()

        // If the task has been killed, let's fail it.
        if (task.killed) {
          throw new TaskKilledException
        }

        val resultSer = SparkEnv.get.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        for (m <- task.metrics) {
          m.executorDeserializeTime = taskStart - startTime
          m.executorRunTime = taskFinish - taskStart
          m.jvmGCTime = gcTime - startGCTime
          m.resultSerializationTime = afterSerialization - beforeSerialization
        }

        val accumUpdates = Accumulators.values

        val directResult = new DirectTaskResult(valueBytes, accumUpdates, task.metrics.orNull)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val (serializedResult, directSend) = {
          if (resultSize >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
            (ser.serialize(new IndirectTaskResult[Any](blockId)), false)
          } else {
            (serializedDirectResult, true)
          }
        }

        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

        if (directSend) {
          logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
        } else {
          logInfo(
            s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
        }
      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
        }

        case _: TaskKilledException | _: InterruptedException if task.killed => {
          logInfo(s"Executor killed $taskName (TID $taskId)")
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))
        }

        case t: Throwable => {
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          val serviceTime = System.currentTimeMillis() - taskStart
          val metrics = attemptedTask.flatMap(t => t.metrics)
          for (m <- metrics) {
            m.executorRunTime = serviceTime
            m.jvmGCTime = gcTime - startGCTime
          }
          val reason = ExceptionFailure(t.getClass.getName, t.getMessage, t.getStackTrace, metrics)
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            ExecutorUncaughtExceptionHandler.uncaughtException(t)
          }
        }
      } finally {
        // Release memory used by this thread for shuffles
        env.shuffleMemoryManager.releaseMemoryForThisThread()
        // Release memory used by this thread for unrolling blocks
        env.blockManager.memoryStore.releaseUnrollMemoryForThisThread()
        runningTasks.remove(taskId)
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }.toArray
    val userClassPathFirst = conf.getBoolean("spark.files.userClassPathFirst", false)
    userClassPathFirst match {
      case true => new ChildExecutorURLClassLoader(urls, currentLoader)
      case false => new ExecutorURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      val userClassPathFirst: java.lang.Boolean =
        conf.getBoolean("spark.files.userClassPathFirst", false)
      try {
        val klass = Class.forName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[String], classOf[ClassLoader],
          classOf[Boolean])
        constructor.newInstance(classUri, parent, userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory), conf, env.securityManager)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars if currentJars.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory), conf, env.securityManager)
        currentJars(name) = timestamp
        // Add it to our class loader
        val localName = name.split("/").last
        val url = new File(SparkFiles.getRootDirectory, localName).toURI.toURL
        if (!urlClassLoader.getURLs.contains(url)) {
          logInfo("Adding " + url + " to class loader")
          urlClassLoader.addURL(url)
        }
      }
    }
  }

  def startDriverHeartbeater() {
    val interval = conf.getInt("spark.executor.heartbeatInterval", 10000)
    val timeout = AkkaUtils.lookupTimeout(conf)
    val retryAttempts = AkkaUtils.numRetries(conf)
    val retryIntervalMs = AkkaUtils.retryWaitMs(conf)
    val heartbeatReceiverRef = AkkaUtils.makeDriverRef("HeartbeatReceiver", conf, env.actorSystem)

    val t = new Thread() {
      override def run() {
        // Sleep a random interval so the heartbeats don't end up in sync
        Thread.sleep(interval + (math.random * interval).asInstanceOf[Int])

        while (!isStopped) {
          val tasksMetrics = new ArrayBuffer[(Long, TaskMetrics)]()
          for (taskRunner <- runningTasks.values()) {
            if (!taskRunner.attemptedTask.isEmpty) {
              Option(taskRunner.task).flatMap(_.metrics).foreach { metrics =>
                metrics.updateShuffleReadMetrics
                tasksMetrics += ((taskRunner.taskId, metrics))
              }
            }
          }

          val message = Heartbeat(executorId, tasksMetrics.toArray, env.blockManager.blockManagerId)
          val response = AkkaUtils.askWithReply[HeartbeatResponse](message, heartbeatReceiverRef,
            retryAttempts, retryIntervalMs, timeout)
          if (response.reregisterBlockManager) {
            logWarning("Told to re-register on heartbeat")
            env.blockManager.reregister()
          }
          Thread.sleep(interval)
        }
      }
    }
    t.setDaemon(true)
    t.setName("Driver Heartbeater")
    t.start()
  }
}
