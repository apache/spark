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

package org.apache.spark.scheduler.local

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.ExecutorURLClassLoader
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.SchedulingMode.SchedulingMode
import akka.actor._
import org.apache.spark.util.Utils

/**
 * A FIFO or Fair TaskScheduler implementation that runs tasks locally in a thread pool. Optionally
 * the scheduler also allows each task to fail up to maxFailures times, which is useful for
 * testing fault recovery.
 */

private[spark]
case class LocalReviveOffers()

private[spark]
case class LocalStatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private[spark]
class LocalActor(localScheduler: LocalScheduler, var freeCores: Int) extends Actor with Logging {

  def receive = {
    case LocalReviveOffers =>
      launchTask(localScheduler.resourceOffer(freeCores))
    case LocalStatusUpdate(taskId, state, serializeData) =>
      freeCores += 1
      localScheduler.statusUpdate(taskId, state, serializeData)
      launchTask(localScheduler.resourceOffer(freeCores))
  }

  def launchTask(tasks : Seq[TaskDescription]) {
    for (task <- tasks) {
      freeCores -= 1
      localScheduler.threadPool.submit(new Runnable {
        def run() {
          localScheduler.runTask(task.taskId, task.serializedTask)
        }
      })
    }
  }
}

private[spark] class LocalScheduler(threads: Int, val maxFailures: Int, val sc: SparkContext)
  extends TaskScheduler
  with Logging {

  var attemptId = new AtomicInteger(0)
  var threadPool = Utils.newDaemonFixedThreadPool(threads)
  val env = SparkEnv.get
  var listener: TaskSchedulerListener = null

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  val classLoader = new ExecutorURLClassLoader(Array(), Thread.currentThread.getContextClassLoader)

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  val schedulingMode: SchedulingMode = SchedulingMode.withName(
    System.getProperty("spark.scheduler.mode", "FIFO"))
  val activeTaskSets = new HashMap[String, TaskSetManager]
  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  var localActor: ActorRef = null

  override def start() {
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool)
      }
    }
    schedulableBuilder.buildPools()

    localActor = env.actorSystem.actorOf(Props(new LocalActor(this, threads)), "Test")
  }

  override def setListener(listener: TaskSchedulerListener) {
    this.listener = listener
  }

  override def submitTasks(taskSet: TaskSet) {
    synchronized {
      val manager = new LocalTaskSetManager(this, taskSet)
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
      activeTaskSets(taskSet.id) = manager
      taskSetTaskIds(taskSet.id) = new HashSet[Long]()
      localActor ! LocalReviveOffers
    }
  }

  def resourceOffer(freeCores: Int): Seq[TaskDescription] = {
    synchronized {
      var freeCpuCores = freeCores
      val tasks = new ArrayBuffer[TaskDescription](freeCores)
      val sortedTaskSetQueue = rootPool.getSortedTaskSetQueue()
      for (manager <- sortedTaskSetQueue) {
        logDebug("parentName:%s,name:%s,runningTasks:%s".format(
          manager.parent.name, manager.name, manager.runningTasks))
      }

      var launchTask = false
      for (manager <- sortedTaskSetQueue) {
        do {
          launchTask = false
          manager.resourceOffer(null, null, freeCpuCores, null) match {
            case Some(task) =>
              tasks += task
              taskIdToTaskSetId(task.taskId) = manager.taskSet.id
              taskSetTaskIds(manager.taskSet.id) += task.taskId
              freeCpuCores -= 1
              launchTask = true
            case None => {}
          }
        } while(launchTask)
      }
      return tasks
    }
  }

  def taskSetFinished(manager: TaskSetManager) {
    synchronized {
      activeTaskSets -= manager.taskSet.id
      manager.parent.removeSchedulable(manager)
      logInfo("Remove TaskSet %s from pool %s".format(manager.taskSet.id, manager.parent.name))
      taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
      taskSetTaskIds -= manager.taskSet.id
    }
  }

  def runTask(taskId: Long, bytes: ByteBuffer) {
    logInfo("Running " + taskId)
    val info = new TaskInfo(taskId, 0, System.currentTimeMillis(), "local", "local:1", TaskLocality.NODE_LOCAL)
    // Set the Spark execution environment for the worker thread
    SparkEnv.set(env)
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val objectSer = SparkEnv.get.serializer.newInstance()
    var attemptedTask: Option[Task[_]] = None   
    val start = System.currentTimeMillis()
    var taskStart: Long = 0
    def getTotalGCTime = ManagementFactory.getGarbageCollectorMXBeans.map(g => g.getCollectionTime).sum
    val startGCTime = getTotalGCTime

    try {
      Accumulators.clear()
      Thread.currentThread().setContextClassLoader(classLoader)

      // Serialize and deserialize the task so that accumulators are changed to thread-local ones;
      // this adds a bit of unnecessary overhead but matches how the Mesos Executor works.
      val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(bytes)
      updateDependencies(taskFiles, taskJars)   // Download any files added with addFile
      val deserializedTask = ser.deserialize[Task[_]](
        taskBytes, Thread.currentThread.getContextClassLoader)
      attemptedTask = Some(deserializedTask)
      val deserTime = System.currentTimeMillis() - start
      taskStart = System.currentTimeMillis()

      // Run it
      val result: Any = deserializedTask.run(taskId)

      // Serialize and deserialize the result to emulate what the Mesos
      // executor does. This is useful to catch serialization errors early
      // on in development (so when users move their local Spark programs
      // to the cluster, they don't get surprised by serialization errors).
      val serResult = objectSer.serialize(result)
      deserializedTask.metrics.get.resultSize = serResult.limit()
      val resultToReturn = objectSer.deserialize[Any](serResult)
      val accumUpdates = ser.deserialize[collection.mutable.Map[Long, Any]](
        ser.serialize(Accumulators.values))
      val serviceTime = System.currentTimeMillis() - taskStart
      logInfo("Finished " + taskId)
      deserializedTask.metrics.get.executorRunTime = serviceTime.toInt
      deserializedTask.metrics.get.jvmGCTime = getTotalGCTime - startGCTime
      deserializedTask.metrics.get.executorDeserializeTime = deserTime.toInt
      val taskResult = new TaskResult(result, accumUpdates, deserializedTask.metrics.getOrElse(null))
      val serializedResult = ser.serialize(taskResult)
      localActor ! LocalStatusUpdate(taskId, TaskState.FINISHED, serializedResult)
    } catch {
      case t: Throwable => {
        val serviceTime = System.currentTimeMillis() - taskStart
        val metrics = attemptedTask.flatMap(t => t.metrics)
        for (m <- metrics) {
          m.executorRunTime = serviceTime.toInt
          m.jvmGCTime = getTotalGCTime - startGCTime
        }
        val failure = new ExceptionFailure(t.getClass.getName, t.toString, t.getStackTrace, metrics)
        localActor ! LocalStatusUpdate(taskId, TaskState.FAILED, ser.serialize(failure))
      }
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
        if (!classLoader.getURLs.contains(url)) {
          logInfo("Adding " + url + " to class loader")
          classLoader.addURL(url)
        }
      }
    }
  }

  def statusUpdate(taskId :Long, state: TaskState, serializedData: ByteBuffer) {
    synchronized {
      val taskSetId = taskIdToTaskSetId(taskId)
      val taskSetManager = activeTaskSets(taskSetId)
      taskSetTaskIds(taskSetId) -= taskId
      taskSetManager.statusUpdate(taskId, state, serializedData)
    }
  }

   override def stop() {
    threadPool.shutdownNow()
  }

  override def defaultParallelism() = threads
}
