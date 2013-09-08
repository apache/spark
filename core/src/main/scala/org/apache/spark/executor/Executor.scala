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

import java.io.{File}
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.concurrent._

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import org.apache.spark.scheduler._
import org.apache.spark._
import org.apache.spark.util.Utils


/**
 * The Mesos executor for Spark.
 */
private[spark] class Executor(
    executorId: String,
    slaveHostname: String,
    properties: Seq[(String, String)])
  extends Logging
{
  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  initLogging()

  // No ip or host:port - just hostname
  Utils.checkHost(slaveHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(slaveHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(slaveHostname)

  // Set spark.* system properties from executor arg
  for ((key, value) <- properties) {
    System.setProperty(key, value)
  }

  // If we are in yarn mode, systems can have different disk layouts so we must set it
  // to what Yarn on this system said was available. This will be used later when SparkEnv
  // created.
  if (java.lang.Boolean.valueOf(System.getenv("SPARK_YARN_MODE"))) {
    System.setProperty("spark.local.dir", getYarnLocalDirs())
  }

  // Create our ClassLoader and set it on this thread
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)
  Thread.currentThread.setContextClassLoader(replClassLoader)

  // Make any thread terminations due to uncaught exceptions kill the entire
  // executor process to avoid surprising stalls.
  Thread.setDefaultUncaughtExceptionHandler(
    new Thread.UncaughtExceptionHandler {
      override def uncaughtException(thread: Thread, exception: Throwable) {
        try {
          logError("Uncaught exception in thread " + thread, exception)

          // We may have been called from a shutdown hook. If so, we must not call System.exit().
          // (If we do, we will deadlock.)
          if (!Utils.inShutdown()) {
            if (exception.isInstanceOf[OutOfMemoryError]) {
              System.exit(ExecutorExitCode.OOM)
            } else {
              System.exit(ExecutorExitCode.UNCAUGHT_EXCEPTION)
            }
          }
        } catch {
          case oom: OutOfMemoryError => Runtime.getRuntime.halt(ExecutorExitCode.OOM)
          case t: Throwable => Runtime.getRuntime.halt(ExecutorExitCode.UNCAUGHT_EXCEPTION_TWICE)
        }
      }
    }
  )

  val executorSource = new ExecutorSource(this, executorId)

  // Initialize Spark environment (using system properties read above)
  val env = SparkEnv.createFromSystemProperties(executorId, slaveHostname, 0, false, false)
  SparkEnv.set(env)
  env.metricsSystem.registerSource(executorSource)

  private val akkaFrameSize = env.actorSystem.settings.config.getBytes("akka.remote.netty.message-frame-size")

  // Start worker thread pool
  val threadPool = new ThreadPoolExecutor(
    1, 128, 600, TimeUnit.SECONDS, new SynchronousQueue[Runnable])

  def launchTask(context: ExecutorBackend, taskId: Long, serializedTask: ByteBuffer) {
    threadPool.execute(new TaskRunner(context, taskId, serializedTask))
  }

  /** Get the Yarn approved local directories. */
  private def getYarnLocalDirs(): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(System.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(System.getenv("LOCAL_DIRS"))
      .getOrElse(""))

    if (localDirs.isEmpty()) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    return localDirs
  }

  class TaskRunner(context: ExecutorBackend, taskId: Long, serializedTask: ByteBuffer)
    extends Runnable {

    override def run() {
      val startTime = System.currentTimeMillis()
      SparkEnv.set(env)
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = SparkEnv.get.closureSerializer.newInstance()
      logInfo("Running task ID " + taskId)
      context.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var attemptedTask: Option[Task[Any]] = None
      var taskStart: Long = 0
      def getTotalGCTime = ManagementFactory.getGarbageCollectorMXBeans.map(g => g.getCollectionTime).sum
      val startGCTime = getTotalGCTime

      try {
        SparkEnv.set(env)
        Accumulators.clear()
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
        updateDependencies(taskFiles, taskJars)
        val task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        attemptedTask = Some(task)
        logInfo("Its epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)
        taskStart = System.currentTimeMillis()
        val value = task.run(taskId.toInt)
        val taskFinish = System.currentTimeMillis()
        for (m <- task.metrics) {
          m.hostname = Utils.localHostName
          m.executorDeserializeTime = (taskStart - startTime).toInt
          m.executorRunTime = (taskFinish - taskStart).toInt
          m.jvmGCTime = getTotalGCTime - startGCTime
        }
        //TODO I'd also like to track the time it takes to serialize the task results, but that is huge headache, b/c
        // we need to serialize the task metrics first.  If TaskMetrics had a custom serialized format, we could
        // just change the relevants bytes in the byte buffer
        val accumUpdates = Accumulators.values
        val result = new TaskResult(value, accumUpdates, task.metrics.getOrElse(null))
        val serializedResult = ser.serialize(result)
        logInfo("Serialized size of result for " + taskId + " is " + serializedResult.limit)
        if (serializedResult.limit >= (akkaFrameSize - 1024)) {
          context.statusUpdate(taskId, TaskState.FAILED, ser.serialize(TaskResultTooBigFailure()))
          return
        }
        context.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
        logInfo("Finished task ID " + taskId)
      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          context.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
        }

        case t: Throwable => {
          val serviceTime = (System.currentTimeMillis() - taskStart).toInt
          val metrics = attemptedTask.flatMap(t => t.metrics)
          for (m <- metrics) {
            m.executorRunTime = serviceTime
            m.jvmGCTime = getTotalGCTime - startGCTime
          }
          val reason = ExceptionFailure(t.getClass.getName, t.toString, t.getStackTrace, metrics)
          context.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

          // TODO: Should we exit the whole executor here? On the one hand, the failed task may
          // have left some weird state around depending on when the exception was thrown, but on
          // the other hand, maybe we could detect that when future tasks fail and exit then.
          logError("Exception in task ID " + taskId, t)
          //System.exit(1)
        }
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): ExecutorURLClassLoader = {
    var loader = this.getClass.getClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }.toArray
    new ExecutorURLClassLoader(urls, loader)
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = System.getProperty("spark.repl.class.uri")
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val klass = Class.forName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[String], classOf[ClassLoader])
        return constructor.newInstance(classUri, parent)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      return parent
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
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory))
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars if currentJars.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory))
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
}
