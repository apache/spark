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

package org.apache.spark.scheduler.cluster.mesos

import java.io.File
import java.util.{ArrayList => JArrayList, List => JList}
import java.util.Collections

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import com.google.protobuf.ByteString
import org.apache.mesos.{Scheduler => MScheduler}
import org.apache.mesos._
import org.apache.mesos.Protos.{TaskInfo => MesosTaskInfo, TaskState => MesosTaskState, _}

import org.apache.spark.{Logging, SparkException, SparkContext, TaskState}
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.{ClusterScheduler, ExecutorExited, ExecutorLossReason}
import org.apache.spark.scheduler.cluster.{SchedulerBackend, SlaveLost, WorkerOffer}
import org.apache.spark.util.Utils

/**
 * A SchedulerBackend for running fine-grained tasks on Mesos. Each Spark task is mapped to a
 * separate Mesos task, allowing multiple applications to share cluster nodes both in space (tasks
 * from multiple apps can run on different cores) and in time (a core can switch ownership).
 */
private[spark] class MesosSchedulerBackend(
    scheduler: ClusterScheduler,
    sc: SparkContext,
    master: String,
    appName: String)
  extends SchedulerBackend
  with MScheduler
  with Logging {

  // Lock used to wait for scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null

  // Which slave IDs we have executors on
  val slaveIdsWithExecutors = new HashSet[String]
  val taskIdToSlaveId = new HashMap[Long, String]

  // An ExecutorInfo for our tasks
  var execArgs: Array[Byte] = null

  var classLoader: ClassLoader = null

  override def start() {
    synchronized {
      classLoader = Thread.currentThread.getContextClassLoader

      new Thread("MesosSchedulerBackend driver") {
        setDaemon(true)
        override def run() {
          val scheduler = MesosSchedulerBackend.this
          val fwInfo = FrameworkInfo.newBuilder().setUser("").setName(appName).build()
          driver = new MesosSchedulerDriver(scheduler, fwInfo, master)
          try {
            val ret = driver.run()
            logInfo("driver.run() returned with code " + ret)
          } catch {
            case e: Exception => logError("driver.run() failed", e)
          }
        }
      }.start()

      waitForRegister()
    }
  }

  def createExecutorInfo(execId: String): ExecutorInfo = {
    val sparkHome = sc.getSparkHome().getOrElse(throw new SparkException(
      "Spark home is not set; set it through the spark.home system " +
      "property, the SPARK_HOME environment variable or the SparkContext constructor"))
    val environment = Environment.newBuilder()
    sc.executorEnvs.foreach { case (key, value) =>
      environment.addVariables(Environment.Variable.newBuilder()
        .setName(key)
        .setValue(value)
        .build())
    }
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)
    val uri = System.getProperty("spark.executor.uri")
    if (uri == null) {
      command.setValue(new File(sparkHome, "spark-executor").getCanonicalPath)
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.split('/').last.split('.').head
      command.setValue("cd %s*; ./spark-executor".format(basename))
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri))
    }
    val memory = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(executorMemory).build())
      .build()
    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(execId).build())
      .setCommand(command)
      .setData(ByteString.copyFrom(createExecArg()))
      .addResources(memory)
      .build()
  }

  /**
   * Create and serialize the executor argument to pass to Mesos. Our executor arg is an array
   * containing all the spark.* system properties in the form of (String, String) pairs.
   */
  private def createExecArg(): Array[Byte] = {
    if (execArgs == null) {
      val props = new HashMap[String, String]
      val iterator = System.getProperties.entrySet.iterator
      while (iterator.hasNext) {
        val entry = iterator.next
        val (key, value) = (entry.getKey.toString, entry.getValue.toString)
        if (key.startsWith("spark.")) {
          props(key) = value
        }
      }
      // Serialize the map as an array of (String, String) pairs
      execArgs = Utils.serialize(props.toArray)
    }
    return execArgs
  }

  private def setClassLoader(): ClassLoader = {
    val oldClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(classLoader)
    return oldClassLoader
  }

  private def restoreClassLoader(oldClassLoader: ClassLoader) {
    Thread.currentThread.setContextClassLoader(oldClassLoader)
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
    val oldClassLoader = setClassLoader()
    try {
      logInfo("Registered as framework ID " + frameworkId.getValue)
      registeredLock.synchronized {
        isRegistered = true
        registeredLock.notifyAll()
      }
    } finally {
      restoreClassLoader(oldClassLoader)
    }
  }

  def waitForRegister() {
    registeredLock.synchronized {
      while (!isRegistered) {
        registeredLock.wait()
      }
    }
  }

  override def disconnected(d: SchedulerDriver) {}

  override def reregistered(d: SchedulerDriver, masterInfo: MasterInfo) {}

  /**
   * Method called by Mesos to offer resources on slaves. We resond by asking our active task sets
   * for tasks in order of priority. We fill each node with tasks in a round-robin manner so that
   * tasks are balanced across the cluster.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    val oldClassLoader = setClassLoader()
    try {
      synchronized {
        // Build a big list of the offerable workers, and remember their indices so that we can
        // figure out which Offer to reply to for each worker
        val offerableIndices = new ArrayBuffer[Int]
        val offerableWorkers = new ArrayBuffer[WorkerOffer]

        def enoughMemory(o: Offer) = {
          val mem = getResource(o.getResourcesList, "mem")
          val slaveId = o.getSlaveId.getValue
          mem >= executorMemory || slaveIdsWithExecutors.contains(slaveId)
        }

        for ((offer, index) <- offers.zipWithIndex if enoughMemory(offer)) {
          offerableIndices += index
          offerableWorkers += new WorkerOffer(
            offer.getSlaveId.getValue,
            offer.getHostname,
            getResource(offer.getResourcesList, "cpus").toInt)
        }

        // Call into the ClusterScheduler
        val taskLists = scheduler.resourceOffers(offerableWorkers)

        // Build a list of Mesos tasks for each slave
        val mesosTasks = offers.map(o => Collections.emptyList[MesosTaskInfo]())
        for ((taskList, index) <- taskLists.zipWithIndex) {
          if (!taskList.isEmpty) {
            val offerNum = offerableIndices(index)
            val slaveId = offers(offerNum).getSlaveId.getValue
            slaveIdsWithExecutors += slaveId
            mesosTasks(offerNum) = new JArrayList[MesosTaskInfo](taskList.size)
            for (taskDesc <- taskList) {
              taskIdToSlaveId(taskDesc.taskId) = slaveId
              mesosTasks(offerNum).add(createMesosTask(taskDesc, slaveId))
            }
          }
        }

        // Reply to the offers
        val filters = Filters.newBuilder().setRefuseSeconds(1).build() // TODO: lower timeout?
        for (i <- 0 until offers.size) {
          d.launchTasks(offers(i).getId, mesosTasks(i), filters)
        }
      }
    } finally {
      restoreClassLoader(oldClassLoader)
    }
  }

  /** Helper function to pull out a resource from a Mesos Resources protobuf */
  def getResource(res: JList[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    // If we reached here, no resource with the required name was present
    throw new IllegalArgumentException("No resource called " + name + " in " + res)
  }

  /** Turn a Spark TaskDescription into a Mesos task */
  def createMesosTask(task: TaskDescription, slaveId: String): MesosTaskInfo = {
    val taskId = TaskID.newBuilder().setValue(task.taskId.toString).build()
    val cpuResource = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(1).build())
      .build()
    return MesosTaskInfo.newBuilder()
      .setTaskId(taskId)
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId).build())
      .setExecutor(createExecutorInfo(slaveId))
      .setName(task.name)
      .addResources(cpuResource)
      .setData(ByteString.copyFrom(task.serializedTask))
      .build()
  }

  /** Check whether a Mesos task state represents a finished task */
  def isFinished(state: MesosTaskState) = {
    state == MesosTaskState.TASK_FINISHED ||
      state == MesosTaskState.TASK_FAILED ||
      state == MesosTaskState.TASK_KILLED ||
      state == MesosTaskState.TASK_LOST
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    val oldClassLoader = setClassLoader()
    try {
      val tid = status.getTaskId.getValue.toLong
      val state = TaskState.fromMesos(status.getState)
      synchronized {
        if (status.getState == MesosTaskState.TASK_LOST && taskIdToSlaveId.contains(tid)) {
          // We lost the executor on this slave, so remember that it's gone
          slaveIdsWithExecutors -= taskIdToSlaveId(tid)
        }
        if (isFinished(status.getState)) {
          taskIdToSlaveId.remove(tid)
        }
      }
      scheduler.statusUpdate(tid, state, status.getData.asReadOnlyByteBuffer)
    } finally {
      restoreClassLoader(oldClassLoader)
    }
  }

  override def error(d: SchedulerDriver, message: String) {
    val oldClassLoader = setClassLoader()
    try {
      logError("Mesos error: " + message)
      scheduler.error(message)
    } finally {
      restoreClassLoader(oldClassLoader)
    }
  }

  override def stop() {
    if (driver != null) {
      driver.stop()
    }
  }

  override def reviveOffers() {
    driver.reviveOffers()
  }

  override def frameworkMessage(d: SchedulerDriver, e: ExecutorID, s: SlaveID, b: Array[Byte]) {}

  private def recordSlaveLost(d: SchedulerDriver, slaveId: SlaveID, reason: ExecutorLossReason) {
    val oldClassLoader = setClassLoader()
    try {
      logInfo("Mesos slave lost: " + slaveId.getValue)
      synchronized {
        slaveIdsWithExecutors -= slaveId.getValue
      }
      scheduler.executorLost(slaveId.getValue, reason)
    } finally {
      restoreClassLoader(oldClassLoader)
    }
  }

  override def slaveLost(d: SchedulerDriver, slaveId: SlaveID) {
    recordSlaveLost(d, slaveId, SlaveLost())
  }

  override def executorLost(d: SchedulerDriver, executorId: ExecutorID,
                            slaveId: SlaveID, status: Int) {
    logInfo("Executor lost: %s, marking slave %s as lost".format(executorId.getValue,
                                                                 slaveId.getValue))
    recordSlaveLost(d, slaveId, ExecutorExited(status))
  }

  // TODO: query Mesos for number of cores
  override def defaultParallelism() = System.getProperty("spark.default.parallelism", "8").toInt
}
