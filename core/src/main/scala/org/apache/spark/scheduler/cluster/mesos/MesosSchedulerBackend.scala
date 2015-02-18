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

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}

import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.{Scheduler => MScheduler}
import org.apache.mesos._
import org.apache.mesos.Protos.{TaskInfo => MesosTaskInfo, TaskState => MesosTaskState,
  ExecutorInfo => MesosExecutorInfo, _}

import org.apache.spark.executor.MesosExecutorBackend
import org.apache.spark.{Logging, SparkContext, SparkException, TaskState}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

/**
 * A SchedulerBackend for running fine-grained tasks on Mesos. Each Spark task is mapped to a
 * separate Mesos task, allowing multiple applications to share cluster nodes both in space (tasks
 * from multiple apps can run on different cores) and in time (a core can switch ownership).
 */
private[spark] class MesosSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    master: String)
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

  // The listener bus to publish executor added/removed events.
  val listenerBus = sc.listenerBus

  @volatile var appId: String = _

  override def start() {
    synchronized {
      classLoader = Thread.currentThread.getContextClassLoader

      new Thread("MesosSchedulerBackend driver") {
        setDaemon(true)
        override def run() {
          val scheduler = MesosSchedulerBackend.this
          val fwInfo = FrameworkInfo.newBuilder().setUser(sc.sparkUser).setName(sc.appName).build()
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

  def createExecutorInfo(execId: String): MesosExecutorInfo = {
    val executorSparkHome = sc.conf.getOption("spark.mesos.executor.home")
      .orElse(sc.getSparkHome()) // Fall back to driver Spark home for backward compatibility
      .getOrElse {
        throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
      }
    val environment = Environment.newBuilder()
    sc.conf.getOption("spark.executor.extraClassPath").foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions").getOrElse("")

    val prefixEnv = sc.conf.getOption("spark.executor.extraLibraryPath").map { p =>
      Utils.libraryPathEnvPrefix(Seq(p))
    }.getOrElse("")

    environment.addVariables(
      Environment.Variable.newBuilder()
        .setName("SPARK_EXECUTOR_OPTS")
        .setValue(extraJavaOpts)
        .build())
    sc.executorEnvs.foreach { case (key, value) =>
      environment.addVariables(Environment.Variable.newBuilder()
        .setName(key)
        .setValue(value)
        .build())
    }
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)
    val uri = sc.conf.get("spark.executor.uri", null)
    val executorBackendName = classOf[MesosExecutorBackend].getName
    if (uri == null) {
      val executorPath = new File(executorSparkHome, "/bin/spark-class").getCanonicalPath
      command.setValue(s"$prefixEnv $executorPath $executorBackendName")
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.split('/').last.split('.').head
      command.setValue(s"cd ${basename}*; $prefixEnv ./bin/spark-class $executorBackendName")
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri))
    }
    val cpus = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder()
        .setValue(scheduler.CPUS_PER_TASK).build())
      .build()
    val memory = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(
        Value.Scalar.newBuilder()
          .setValue(MemoryUtils.calculateTotalMemory(sc)).build())
      .build()
    MesosExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(execId).build())
      .setCommand(command)
      .setData(ByteString.copyFrom(createExecArg()))
      .addResources(cpus)
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
      for ((key,value) <- sc.conf.getAll) {
        props(key) = value
      }
      // Serialize the map as an array of (String, String) pairs
      execArgs = Utils.serialize(props.toArray)
    }
    execArgs
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
    inClassLoader() {
      appId = frameworkId.getValue
      logInfo("Registered as framework ID " + appId)
      registeredLock.synchronized {
        isRegistered = true
        registeredLock.notifyAll()
      }
    }
  }

  def waitForRegister() {
    registeredLock.synchronized {
      while (!isRegistered) {
        registeredLock.wait()
      }
    }
  }

  private def inClassLoader()(fun: => Unit) = {
    val oldClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(classLoader)
    try {
      fun
    } finally {
      Thread.currentThread.setContextClassLoader(oldClassLoader)
    }
  }

  override def disconnected(d: SchedulerDriver) {}

  override def reregistered(d: SchedulerDriver, masterInfo: MasterInfo) {}

  /**
   * Method called by Mesos to offer resources on slaves. We respond by asking our active task sets
   * for tasks in order of priority. We fill each node with tasks in a round-robin manner so that
   * tasks are balanced across the cluster.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    inClassLoader() {
      // Fail-fast on offers we know will be rejected
      val (usableOffers, unUsableOffers) = offers.partition { o =>
        val mem = getResource(o.getResourcesList, "mem")
        val cpus = getResource(o.getResourcesList, "cpus")
        val slaveId = o.getSlaveId.getValue
        // TODO(pwendell): Should below be 1 + scheduler.CPUS_PER_TASK?
        (mem >= MemoryUtils.calculateTotalMemory(sc) &&
          // need at least 1 for executor, 1 for task
          cpus >= 2 * scheduler.CPUS_PER_TASK) ||
          (slaveIdsWithExecutors.contains(slaveId) &&
            cpus >= scheduler.CPUS_PER_TASK)
      }

      val workerOffers = usableOffers.map { o =>
        val cpus = if (slaveIdsWithExecutors.contains(o.getSlaveId.getValue)) {
          getResource(o.getResourcesList, "cpus").toInt
        } else {
          // If the executor doesn't exist yet, subtract CPU for executor
          // TODO(pwendell): Should below just subtract "1"?
          getResource(o.getResourcesList, "cpus").toInt -
            scheduler.CPUS_PER_TASK
        }
        new WorkerOffer(
          o.getSlaveId.getValue,
          o.getHostname,
          cpus)
      }

      val slaveIdToOffer = usableOffers.map(o => o.getSlaveId.getValue -> o).toMap
      val slaveIdToWorkerOffer = workerOffers.map(o => o.executorId -> o).toMap

      val mesosTasks = new HashMap[String, JArrayList[MesosTaskInfo]]

      val slavesIdsOfAcceptedOffers = HashSet[String]()

      // Call into the TaskSchedulerImpl
      val acceptedOffers = scheduler.resourceOffers(workerOffers).filter(!_.isEmpty)
      acceptedOffers
        .foreach { offer =>
          offer.foreach { taskDesc =>
            val slaveId = taskDesc.executorId
            slaveIdsWithExecutors += slaveId
            slavesIdsOfAcceptedOffers += slaveId
            taskIdToSlaveId(taskDesc.taskId) = slaveId
            mesosTasks.getOrElseUpdate(slaveId, new JArrayList[MesosTaskInfo])
              .add(createMesosTask(taskDesc, slaveId))
          }
        }

      // Reply to the offers
      val filters = Filters.newBuilder().setRefuseSeconds(1).build() // TODO: lower timeout?

      mesosTasks.foreach { case (slaveId, tasks) =>
        slaveIdToWorkerOffer.get(slaveId).foreach(o =>
          listenerBus.post(SparkListenerExecutorAdded(System.currentTimeMillis(), slaveId,
            // TODO: Add support for log urls for Mesos
            new ExecutorInfo(o.host, o.cores, Map.empty)))
        )
        d.launchTasks(Collections.singleton(slaveIdToOffer(slaveId).getId), tasks, filters)
      }

      // Decline offers that weren't used
      // NOTE: This logic assumes that we only get a single offer for each host in a given batch
      for (o <- usableOffers if !slavesIdsOfAcceptedOffers.contains(o.getSlaveId.getValue)) {
        d.declineOffer(o.getId)
      }

      // Decline offers we ruled out immediately
      unUsableOffers.foreach(o => d.declineOffer(o.getId))
    }
  }

  /** Helper function to pull out a resource from a Mesos Resources protobuf */
  def getResource(res: JList[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    0
  }

  /** Turn a Spark TaskDescription into a Mesos task */
  def createMesosTask(task: TaskDescription, slaveId: String): MesosTaskInfo = {
    val taskId = TaskID.newBuilder().setValue(task.taskId.toString).build()
    val cpuResource = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(scheduler.CPUS_PER_TASK).build())
      .build()
    MesosTaskInfo.newBuilder()
      .setTaskId(taskId)
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId).build())
      .setExecutor(createExecutorInfo(slaveId))
      .setName(task.name)
      .addResources(cpuResource)
      .setData(MesosTaskLaunchData(task.serializedTask, task.attemptNumber).toByteString)
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
    inClassLoader() {
      val tid = status.getTaskId.getValue.toLong
      val state = TaskState.fromMesos(status.getState)
      synchronized {
        if (status.getState == MesosTaskState.TASK_LOST && taskIdToSlaveId.contains(tid)) {
          // We lost the executor on this slave, so remember that it's gone
          removeExecutor(taskIdToSlaveId(tid), "Lost executor")
        }
        if (isFinished(status.getState)) {
          taskIdToSlaveId.remove(tid)
        }
      }
      scheduler.statusUpdate(tid, state, status.getData.asReadOnlyByteBuffer)
    }
  }

  override def error(d: SchedulerDriver, message: String) {
    inClassLoader() {
      logError("Mesos error: " + message)
      scheduler.error(message)
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

  /**
   * Remove executor associated with slaveId in a thread safe manner.
   */
  private def removeExecutor(slaveId: String, reason: String) = {
    synchronized {
      listenerBus.post(SparkListenerExecutorRemoved(System.currentTimeMillis(), slaveId, reason))
      slaveIdsWithExecutors -= slaveId
    }
  }

  private def recordSlaveLost(d: SchedulerDriver, slaveId: SlaveID, reason: ExecutorLossReason) {
    inClassLoader() {
      logInfo("Mesos slave lost: " + slaveId.getValue)
      removeExecutor(slaveId.getValue, reason.toString)
      scheduler.executorLost(slaveId.getValue, reason)
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

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit = {
    driver.killTask(
      TaskID.newBuilder()
        .setValue(taskId.toString).build()
    )
  }

  // TODO: query Mesos for number of cores
  override def defaultParallelism() = sc.conf.getInt("spark.default.parallelism", 8)

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

}
