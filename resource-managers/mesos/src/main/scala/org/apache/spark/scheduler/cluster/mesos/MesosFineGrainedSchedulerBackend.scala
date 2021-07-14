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
import java.util.{ArrayList => JArrayList, Collections, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, HashSet}

import org.apache.mesos.Protos.{ExecutorInfo => MesosExecutorInfo, SlaveID => AgentID,
  TaskInfo => MesosTaskInfo, _}
import org.apache.mesos.SchedulerDriver
import org.apache.mesos.protobuf.ByteString

import org.apache.spark.{SparkContext, SparkException, TaskState}
import org.apache.spark.deploy.mesos.{config => mesosConfig}
import org.apache.spark.executor.MesosExecutorBackend
import org.apache.spark.internal.config
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.Utils

/**
 * A SchedulerBackend for running fine-grained tasks on Mesos. Each Spark task is mapped to a
 * separate Mesos task, allowing multiple applications to share cluster nodes both in space (tasks
 * from multiple apps can run on different cores) and in time (a core can switch ownership).
 */
private[spark] class MesosFineGrainedSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    master: String)
  extends SchedulerBackend
  with MesosScheduler
  with MesosSchedulerUtils {

  // Stores the agent ids that has launched a Mesos executor.
  val agentIdToExecutorInfo = new HashMap[String, MesosExecutorInfo]
  val taskIdToAgentId = new HashMap[Long, String]

  // An ExecutorInfo for our tasks
  var execArgs: Array[Byte] = null

  var classLoader: ClassLoader = null

  // The listener bus to publish executor added/removed events.
  val listenerBus = sc.listenerBus

  private[mesos] val mesosExecutorCores = sc.conf.get(mesosConfig.EXECUTOR_CORES)

  // Offer constraints
  private[this] val agentOfferConstraints =
    parseConstraintString(sc.conf.get(mesosConfig.CONSTRAINTS))

  // reject offers with mismatched constraints in seconds
  private val rejectOfferDurationForUnmetConstraints =
    getRejectOfferDurationForUnmetConstraints(sc.conf)

  private var schedulerDriver: SchedulerDriver = _

  @volatile var appId: String = _

  override def start(): Unit = {
    classLoader = Thread.currentThread.getContextClassLoader
    val driver = createSchedulerDriver(
      master,
      MesosFineGrainedSchedulerBackend.this,
      sc.sparkUser,
      sc.appName,
      sc.conf,
      sc.conf.get(mesosConfig.DRIVER_WEBUI_URL).orElse(sc.ui.map(_.webUrl)),
      Option.empty,
      Option.empty,
      sc.conf.get(mesosConfig.DRIVER_FRAMEWORK_ID)
    )

    unsetFrameworkID(sc)
    startScheduler(driver)
  }

  /**
   * Creates a MesosExecutorInfo that is used to launch a Mesos executor.
 *
   * @param availableResources Available resources that is offered by Mesos
   * @param execId The executor id to assign to this new executor.
   * @return A tuple of the new mesos executor info and the remaining available resources.
   */
  def createExecutorInfo(
      availableResources: JList[Resource],
      execId: String): (MesosExecutorInfo, JList[Resource]) = {
    val executorSparkHome = sc.conf.get(mesosConfig.EXECUTOR_HOME)
      .orElse(sc.getSparkHome()) // Fall back to driver Spark home for backward compatibility
      .getOrElse {
      throw new SparkException(s"Executor Spark home `${mesosConfig.EXECUTOR_HOME}` is not set!")
    }
    val environment = Environment.newBuilder()
    sc.conf.get(config.EXECUTOR_CLASS_PATH).foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_EXECUTOR_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = sc.conf.get(config.EXECUTOR_JAVA_OPTIONS).map {
      Utils.substituteAppNExecIds(_, appId, execId)
    }.getOrElse("")

    val prefixEnv = sc.conf.get(config.EXECUTOR_LIBRARY_PATH).map { p =>
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
    val uri = sc.conf.get(mesosConfig.EXECUTOR_URI)
      .orElse(Option(System.getenv("SPARK_EXECUTOR_URI")))

    val executorBackendName = classOf[MesosExecutorBackend].getName
    if (uri.isEmpty) {
      val executorPath = new File(executorSparkHome, "/bin/spark-class").getPath
      command.setValue(s"$prefixEnv $executorPath $executorBackendName")
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.get.split('/').last.split('.').head
      command.setValue(s"cd ${basename}*; $prefixEnv ./bin/spark-class $executorBackendName")
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri.get))
    }
    val builder = MesosExecutorInfo.newBuilder()
    val (resourcesAfterCpu, usedCpuResources) =
      partitionResources(availableResources, "cpus", mesosExecutorCores)
    val (resourcesAfterMem, usedMemResources) =
      partitionResources(resourcesAfterCpu.asJava, "mem", executorMemory(sc))

    builder.addAllResources(usedCpuResources.asJava)
    builder.addAllResources(usedMemResources.asJava)

    setupUris(sc.conf.get(mesosConfig.URIS_TO_DOWNLOAD), command)

    val executorInfo = builder
      .setExecutorId(ExecutorID.newBuilder().setValue(execId).build())
      .setCommand(command)
      .setData(ByteString.copyFrom(createExecArg()))

    executorInfo.setContainer(
      MesosSchedulerBackendUtil.buildContainerInfo(sc.conf))
    (executorInfo.build(), resourcesAfterMem.asJava)
  }

  /**
   * Create and serialize the executor argument to pass to Mesos. Our executor arg is an array
   * containing all the spark.* system properties in the form of (String, String) pairs.
   */
  private def createExecArg(): Array[Byte] = {
    if (execArgs == null) {
      val props = new HashMap[String, String]
      for ((key, value) <- sc.conf.getAll) {
        props(key) = value
      }
      // Serialize the map as an array of (String, String) pairs
      execArgs = Utils.serialize(props.toArray)
    }
    execArgs
  }

  override def offerRescinded(d: org.apache.mesos.SchedulerDriver, o: OfferID): Unit = {}

  override def registered(
      driver: org.apache.mesos.SchedulerDriver,
      frameworkId: FrameworkID,
      masterInfo: MasterInfo): Unit = {
    inClassLoader() {
      appId = frameworkId.getValue
      logInfo("Registered as framework ID " + appId)
      this.schedulerDriver = driver
      markRegistered()
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

  override def disconnected(d: org.apache.mesos.SchedulerDriver): Unit = {}

  override def reregistered(d: org.apache.mesos.SchedulerDriver, masterInfo: MasterInfo): Unit = {}

  private def getTasksSummary(tasks: JArrayList[MesosTaskInfo]): String = {
    val builder = new StringBuilder
    tasks.asScala.foreach { t =>
      builder.append("Task id: ").append(t.getTaskId.getValue).append("\n")
        .append("Agent id: ").append(t.getSlaveId.getValue).append("\n")
        .append("Task resources: ").append(t.getResourcesList).append("\n")
        .append("Executor resources: ").append(t.getExecutor.getResourcesList)
        .append("---------------------------------------------\n")
    }
    builder.toString()
  }

  /**
   * Method called by Mesos to offer resources on agents. We respond by asking our active task sets
   * for tasks in order of priority. We fill each node with tasks in a round-robin manner so that
   * tasks are balanced across the cluster.
   */
  override def resourceOffers(d: org.apache.mesos.SchedulerDriver, offers: JList[Offer]): Unit = {
    inClassLoader() {
      // Fail first on offers with unmet constraints
      val (offersMatchingConstraints, offersNotMatchingConstraints) =
        offers.asScala.partition { o =>
          val offerAttributes = toAttributeMap(o.getAttributesList)
          val meetsConstraints =
            matchesAttributeRequirements(agentOfferConstraints, offerAttributes)

          // add some debug messaging
          if (!meetsConstraints) {
            val id = o.getId.getValue
            logDebug(s"Declining offer: $id with attributes: $offerAttributes")
          }

          meetsConstraints
        }

      // These offers do not meet constraints. We don't need to see them again.
      // Decline the offer for a long period of time.
      offersNotMatchingConstraints.foreach { o =>
        d.declineOffer(o.getId, Filters.newBuilder()
          .setRefuseSeconds(rejectOfferDurationForUnmetConstraints).build())
      }

      // Of the matching constraints, see which ones give us enough memory and cores
      val (usableOffers, unUsableOffers) = offersMatchingConstraints.partition { o =>
        val mem = getResource(o.getResourcesList, "mem")
        val cpus = getResource(o.getResourcesList, "cpus")
        val agentId = o.getSlaveId.getValue
        val offerAttributes = toAttributeMap(o.getAttributesList)

        // check offers for
        //  1. Memory requirements
        //  2. CPU requirements - need at least 1 for executor, 1 for task
        val meetsMemoryRequirements = mem >= executorMemory(sc)
        val meetsCPURequirements = cpus >= (mesosExecutorCores + scheduler.CPUS_PER_TASK)
        val meetsRequirements =
          (meetsMemoryRequirements && meetsCPURequirements) ||
          (agentIdToExecutorInfo.contains(agentId) && cpus >= scheduler.CPUS_PER_TASK)
        val debugstr = if (meetsRequirements) "Accepting" else "Declining"
        logDebug(s"$debugstr offer: ${o.getId.getValue} with attributes: "
          + s"$offerAttributes mem: $mem cpu: $cpus")

        meetsRequirements
      }

      // Decline offers we ruled out immediately
      unUsableOffers.foreach(o => d.declineOffer(o.getId))

      val workerOffers = usableOffers.map { o =>
        val cpus = if (agentIdToExecutorInfo.contains(o.getSlaveId.getValue)) {
          getResource(o.getResourcesList, "cpus").toInt
        } else {
          // If the Mesos executor has not been started on this agent yet, set aside a few
          // cores for the Mesos executor by offering fewer cores to the Spark executor
          (getResource(o.getResourcesList, "cpus") - mesosExecutorCores).toInt
        }
        new WorkerOffer(
          o.getSlaveId.getValue,
          o.getHostname,
          cpus)
      }.toIndexedSeq

      val agentIdToOffer = usableOffers.map(o => o.getSlaveId.getValue -> o).toMap
      val agentIdToWorkerOffer = workerOffers.map(o => o.executorId -> o).toMap
      val agentIdToResources = new HashMap[String, JList[Resource]]()
      usableOffers.foreach { o =>
        agentIdToResources(o.getSlaveId.getValue) = o.getResourcesList
      }

      val mesosTasks = new HashMap[String, JArrayList[MesosTaskInfo]]

      val agentsIdsOfAcceptedOffers = HashSet[String]()

      // Call into the TaskSchedulerImpl
      val acceptedOffers = scheduler.resourceOffers(workerOffers).filter(!_.isEmpty)
      acceptedOffers
        .foreach { offer =>
          offer.foreach { taskDesc =>
            val agentId = taskDesc.executorId
            agentsIdsOfAcceptedOffers += agentId
            taskIdToAgentId(taskDesc.taskId) = agentId
            val (mesosTask, remainingResources) = createMesosTask(
              taskDesc,
              agentIdToResources(agentId),
              agentId)
            mesosTasks.getOrElseUpdate(agentId, new JArrayList[MesosTaskInfo])
              .add(mesosTask)
            agentIdToResources(agentId) = remainingResources
          }
        }

      // Reply to the offers
      val filters = Filters.newBuilder().setRefuseSeconds(1).build() // TODO: lower timeout?

      mesosTasks.foreach { case (agentId, tasks) =>
        agentIdToWorkerOffer.get(agentId).foreach(o =>
          listenerBus.post(SparkListenerExecutorAdded(System.currentTimeMillis(), agentId,
            // TODO: Add support for log urls for Mesos
            new ExecutorInfo(o.host, o.cores, Map.empty, Map.empty)))
        )
        logTrace(s"Launching Mesos tasks on agent '$agentId', tasks:\n${getTasksSummary(tasks)}")
        d.launchTasks(Collections.singleton(agentIdToOffer(agentId).getId), tasks, filters)
      }

      // Decline offers that weren't used
      // NOTE: This logic assumes that we only get a single offer for each host in a given batch
      for (o <- usableOffers if !agentsIdsOfAcceptedOffers.contains(o.getSlaveId.getValue)) {
        d.declineOffer(o.getId)
      }
    }
  }

  /** Turn a Spark TaskDescription into a Mesos task and also resources unused by the task */
  def createMesosTask(
      task: TaskDescription,
      resources: JList[Resource],
      agentId: String): (MesosTaskInfo, JList[Resource]) = {
    val taskId = TaskID.newBuilder().setValue(task.taskId.toString).build()
    val (executorInfo, remainingResources) = if (agentIdToExecutorInfo.contains(agentId)) {
      (agentIdToExecutorInfo(agentId), resources)
    } else {
      createExecutorInfo(resources, agentId)
    }
    agentIdToExecutorInfo(agentId) = executorInfo
    val (finalResources, cpuResources) =
      partitionResources(remainingResources, "cpus", scheduler.CPUS_PER_TASK)
    val taskInfo = MesosTaskInfo.newBuilder()
      .setTaskId(taskId)
      .setSlaveId(AgentID.newBuilder().setValue(agentId).build())
      .setExecutor(executorInfo)
      .setName(task.name)
      .addAllResources(cpuResources.asJava)
      .setData(ByteString.copyFrom(TaskDescription.encode(task)))
      .build()
    (taskInfo, finalResources.asJava)
  }

  override def statusUpdate(d: org.apache.mesos.SchedulerDriver, status: TaskStatus): Unit = {
    inClassLoader() {
      val tid = status.getTaskId.getValue.toLong
      val state = mesosToTaskState(status.getState)
      synchronized {
        if (TaskState.isFailed(mesosToTaskState(status.getState))
          && taskIdToAgentId.contains(tid)) {
          // We lost the executor on this agent, so remember that it's gone
          removeExecutor(taskIdToAgentId(tid), "Lost executor")
        }
        if (TaskState.isFinished(state)) {
          taskIdToAgentId.remove(tid)
        }
      }
      scheduler.statusUpdate(tid, state, status.getData.asReadOnlyByteBuffer)
    }
  }

  override def error(d: org.apache.mesos.SchedulerDriver, message: String): Unit = {
    inClassLoader() {
      logError("Mesos error: " + message)
      markErr()
      scheduler.error(message)
    }
  }

  override def stop(): Unit = {
    if (schedulerDriver != null) {
      schedulerDriver.stop()
    }
  }

  override def reviveOffers(): Unit = {
    schedulerDriver.reviveOffers()
  }

  override def frameworkMessage(
      d: org.apache.mesos.SchedulerDriver, e: ExecutorID, s: AgentID, b: Array[Byte]): Unit = {}

  /**
   * Remove executor associated with agentId in a thread safe manner.
   */
  private def removeExecutor(agentId: String, reason: String) = {
    synchronized {
      listenerBus.post(SparkListenerExecutorRemoved(System.currentTimeMillis(), agentId, reason))
      agentIdToExecutorInfo -= agentId
    }
  }

  private def recordAgentLost(
      d: org.apache.mesos.SchedulerDriver, agentId: AgentID, reason: ExecutorLossReason): Unit = {
    inClassLoader() {
      logInfo("Mesos agent lost: " + agentId.getValue)
      removeExecutor(agentId.getValue, reason.toString)
      scheduler.executorLost(agentId.getValue, reason)
    }
  }

  override def agentLost(d: org.apache.mesos.SchedulerDriver, agentId: AgentID): Unit = {
    recordAgentLost(d, agentId, ExecutorProcessLost())
  }

  override def executorLost(
      d: org.apache.mesos.SchedulerDriver,
      executorId: ExecutorID,
      agentId: AgentID,
      status: Int): Unit = {
    logInfo("Executor lost: %s, marking agent %s as lost".format(executorId.getValue,
                                                                 agentId.getValue))
    recordAgentLost(d, agentId, ExecutorExited(status, exitCausedByApp = true))
  }

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String): Unit = {
    schedulerDriver.killTask(
      TaskID.newBuilder()
        .setValue(taskId.toString).build()
    )
  }

  // TODO: query Mesos for number of cores
  override def defaultParallelism(): Int = sc.conf.getInt("spark.default.parallelism", 8)

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  override def maxNumConcurrentTasks(rp: ResourceProfile): Int = {
    // TODO SPARK-25074 support this method for MesosFineGrainedSchedulerBackend
    0
  }
}
