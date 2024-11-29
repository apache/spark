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

package org.apache.spark.resource

import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException}
import org.apache.spark.annotation.{Evolving, Since}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY
import org.apache.spark.util.Utils

/**
 * Resource profile to associate with an RDD. A ResourceProfile allows the user to
 * specify executor and task requirements for an RDD that will get applied during a
 * stage. This allows the user to change the resource requirements between stages.
 * This is meant to be immutable so user can't change it after building. Users
 * should use [[ResourceProfileBuilder]] to build it.
 *
 * @param executorResources Resource requests for executors. Mapped from the resource
 *                          name (e.g., cores, memory, CPU) to its specific request.
 * @param taskResources Resource requests for tasks. Mapped from the resource
 *                      name (e.g., cores, memory, CPU) to its specific request.
 */
@Evolving
@Since("3.1.0")
class ResourceProfile(
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest]) extends Serializable with Logging {

  validate()

  // _id is only a var for testing purposes
  private var _id = ResourceProfile.getNextProfileId
  // This is used for any resources that use fractional amounts, the key is the resource name
  // and the value is the number of tasks that can share a resource address. For example,
  // if the user says task gpu amount is 0.5, that results in 2 tasks per resource address.
  private var _executorResourceSlotsPerAddr: Option[Map[String, Int]] = None
  private var _limitingResource: Option[String] = None
  private var _maxTasksPerExecutor: Option[Int] = None
  private var _coresLimitKnown: Boolean = false

  /**
   * Validate the ResourceProfile
   */
  protected def validate(): Unit = {
    // The task.amount in ResourceProfile falls within the range of 0 to 0.5,
    // or it's a whole number
    for ((_, taskReq) <- taskResources) {
      val taskAmount = taskReq.amount
      assert(taskAmount <= 0.5 || taskAmount % 1 == 0,
        s"The task resource amount ${taskAmount} must be either <= 0.5, or a whole number.")
    }
  }

  /**
   * A unique id of this ResourceProfile
   */
  def id: Int = _id

  /**
   * (Java-specific) gets a Java Map of resources to TaskResourceRequest
   */
  def taskResourcesJMap: JMap[String, TaskResourceRequest] = taskResources.asJava

  /**
   * (Java-specific) gets a Java Map of resources to ExecutorResourceRequest
   */
  def executorResourcesJMap: JMap[String, ExecutorResourceRequest] = {
    executorResources.asJava
  }

  // Note that some cluster managers don't set the executor cores explicitly so
  // be sure to check the Option as required
  private[spark] def getExecutorCores: Option[Int] = {
    executorResources.get(ResourceProfile.CORES).map(_.amount.toInt)
  }

  private[spark] def getTaskCpus: Option[Int] = {
    taskResources.get(ResourceProfile.CPUS).map(_.amount.toInt)
  }

  private[spark] def getPySparkMemory: Option[Long] = {
    executorResources.get(ResourceProfile.PYSPARK_MEM).map(_.amount)
  }

  private[spark] def getExecutorMemory: Option[Long] = {
    executorResources.get(ResourceProfile.MEMORY).map(_.amount)
  }

  private[spark] def getCustomTaskResources(): Map[String, TaskResourceRequest] = {
    taskResources.filter { case (k, _) => !k.equals(ResourceProfile.CPUS) }
  }

  protected[spark] def getCustomExecutorResources(): Map[String, ExecutorResourceRequest] = {
    executorResources.
      filter { case (k, _) => !ResourceProfile.allSupportedExecutorResources.contains(k) }
  }

  /*
   * This function takes into account fractional amounts for the task resource requirement.
   * Spark only supports fractional amounts &lt; 1 to basically allow for multiple tasks
   * to use the same resource address or a whole number to use the multiple whole addresses.
   */
  private[spark] def getSchedulerTaskResourceAmount(resource: String): Int = {
    val taskAmount = taskResources.getOrElse(resource,
      throw new SparkException(s"Resource $resource doesn't exist in profile id: $id"))
   if (taskAmount.amount < 1) 1 else taskAmount.amount.toInt
  }

  private[spark] def getNumSlotsPerAddress(resource: String, sparkConf: SparkConf): Int = {
    _executorResourceSlotsPerAddr.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
    }
    _executorResourceSlotsPerAddr.get.getOrElse(resource,
      throw new SparkException(s"Resource $resource doesn't exist in profile id: $id"))
  }

  // Maximum tasks you could put on an executor with this profile based on the limiting resource.
  // If the executor cores config is not present this value is based on the other resources
  // available or 1 if no other resources. You need to check the isCoresLimitKnown to
  // calculate proper value.
  private[spark] def maxTasksPerExecutor(sparkConf: SparkConf): Int = {
    _maxTasksPerExecutor.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
      _maxTasksPerExecutor.get
    }
  }

  // Returns whether the executor cores was available to use to calculate the max tasks
  // per executor and limiting resource. Some cluster managers (like standalone)
  // don't use the cores config by default so we can't use it to calculate slots.
  private[spark] def isCoresLimitKnown: Boolean = _coresLimitKnown

  // The resource that has the least amount of slots per executor. Its possible multiple or all
  // resources result in same number of slots and this could be any of those.
  // If the executor cores config is not present this value is based on the other resources
  // available or empty string if no other resources. You need to check the isCoresLimitKnown to
  // calculate proper value.
  private[spark] def limitingResource(sparkConf: SparkConf): String = {
    _limitingResource.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
      _limitingResource.get
    }
  }

  // executor cores config is not set for some masters by default and the default value
  // only applies to yarn/k8s
  private def shouldCheckExecutorCores(sparkConf: SparkConf): Boolean = {
    val master = sparkConf.getOption("spark.master")
    sparkConf.contains(EXECUTOR_CORES) ||
      (master.isDefined && (master.get.equalsIgnoreCase("yarn") || master.get.startsWith("k8s")))
  }

  /**
   * Utility function to calculate the number of tasks you can run on a single Executor based
   * on the task and executor resource requests in the ResourceProfile. This will be based
   * off the resource that is most restrictive. For instance, if the executor
   * request is for 4 cpus and 2 gpus and your task request is for 1 cpu and 1 gpu each, the
   * limiting resource is gpu and the number of tasks you can run on a single executor is 2.
   * This function also sets the limiting resource, isCoresLimitKnown and number of slots per
   * resource address.
   */
  private def calculateTasksAndLimitingResource(sparkConf: SparkConf): Unit = synchronized {
    val shouldCheckExecCores = shouldCheckExecutorCores(sparkConf)
    var (taskLimit, limitingResource) = if (shouldCheckExecCores) {
      val cpusPerTask = taskResources.get(ResourceProfile.CPUS)
        .map(_.amount).getOrElse(sparkConf.get(CPUS_PER_TASK).toDouble).toInt
      assert(cpusPerTask > 0, "CPUs per task configuration has to be > 0")
      val coresPerExecutor = getExecutorCores.getOrElse(sparkConf.get(EXECUTOR_CORES))
      _coresLimitKnown = true
      ResourceUtils.validateTaskCpusLargeEnough(sparkConf, coresPerExecutor, cpusPerTask)
      val tasksBasedOnCores = coresPerExecutor / cpusPerTask
      // Note that if the cores per executor aren't set properly this calculation could be off,
      // we default it to just be 1 in order to allow checking of the rest of the custom
      // resources. We set the limit based on the other resources available.
      (tasksBasedOnCores, ResourceProfile.CPUS)
    } else {
      (-1, "")
    }
    val numPartsPerResourceMap = new mutable.HashMap[String, Int]
    numPartsPerResourceMap(ResourceProfile.CORES) = 1
    val taskResourcesToCheck = new mutable.HashMap[String, TaskResourceRequest]
    taskResourcesToCheck ++= this.getCustomTaskResources()
    val execResourceToCheck = this.getCustomExecutorResources()
    execResourceToCheck.foreach { case (rName, execReq) =>
      val taskReq = taskResources.get(rName).map(_.amount).getOrElse(0.0)
      numPartsPerResourceMap(rName) = 1
      if (taskReq > 0.0) {
        if (taskReq > execReq.amount) {
          throw new SparkException(s"The executor resource: $rName, amount: ${execReq.amount} " +
            s"needs to be >= the task resource request amount of $taskReq")
        }
        val (numPerTask, parts) = ResourceUtils.calculateAmountAndPartsForFraction(taskReq)
        numPartsPerResourceMap(rName) = parts
        val numTasks = ((execReq.amount * parts) / numPerTask).toInt
        if (taskLimit == -1 || numTasks < taskLimit) {
          limitingResource = rName
          taskLimit = numTasks
        }
        taskResourcesToCheck -= rName
      } else {
        logWarning(log"The executor resource config for resource: ${MDC(RESOURCE_NAME, rName)} " +
          log"was specified but no corresponding task resource request was specified.")
      }
    }
    if (taskResourcesToCheck.nonEmpty) {
      throw new SparkException("No executor resource configs were specified for the " +
        s"following task configs: ${taskResourcesToCheck.keys.mkString(",")}")
    }
    val limiting =
      if (taskLimit == -1) "cpu" else s"$limitingResource at $taskLimit tasks per executor"
    logInfo(log"Limiting resource is ${MDC(RESOURCE, limiting)}")
    _executorResourceSlotsPerAddr = Some(numPartsPerResourceMap.toMap)
    _maxTasksPerExecutor = if (taskLimit == -1) Some(1) else Some(taskLimit)
    _limitingResource = Some(limitingResource)
    if (shouldCheckExecCores) {
      ResourceUtils.warnOnWastedResources(this, sparkConf)
    }
  }

  // to be used only by history server for reconstruction from events
  private[spark] def setResourceProfileId(id: Int): Unit = {
    _id = id
  }

  // testing only
  private[spark] def setToDefaultProfile(): Unit = {
    _id = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceProfile =>
        that.getClass == this.getClass && that.id == _id &&
          that.taskResources == taskResources && that.executorResources == executorResources
      case _ =>
        false
    }
  }

  // check that the task resources and executor resources are equal, but id's could be different
  private[spark] def resourcesEqual(rp: ResourceProfile): Boolean = {
    rp.taskResources == taskResources && rp.executorResources == executorResources &&
      rp.getClass == this.getClass
  }

  override def hashCode(): Int = Seq(taskResources, executorResources).hashCode()

  override def toString(): String = {
    s"Profile: id = ${_id}, executor resources: ${executorResources.mkString(",")}, " +
      s"task resources: ${taskResources.mkString(",")}"
  }
}

/**
 * Resource profile which only contains task resources, can be used for stage level task schedule
 * when dynamic allocation is disabled, tasks will be scheduled to executors with default resource
 * profile based on task resources described by this task resource profile.
 * And when dynamic allocation is enabled, will require new executors for this profile based on
 * the default executor resources requested at startup and assign tasks only on executors created
 * with this resource profile.
 *
 * @param taskResources Resource requests for tasks. Mapped from the resource
 *                      name (e.g., cores, memory, CPU) to its specific request.
 */
@Evolving
@Since("3.4.0")
private[spark] class TaskResourceProfile(
    override val taskResources: Map[String, TaskResourceRequest])
  extends ResourceProfile(Map.empty, taskResources) {

  // The task.amount in TaskResourceProfile falls within the range of 0 to 1.0,
  // or it's a whole number, and it has been checked in the TaskResourceRequest.
  // Therefore, we can safely skip this check.
  override protected def validate(): Unit = {}

  override protected[spark] def getCustomExecutorResources()
      : Map[String, ExecutorResourceRequest] = {
    if (SparkEnv.get == null) {
      // This will be called in standalone master when dynamic allocation enabled.
      return super.getCustomExecutorResources()
    }

    val sparkConf = SparkEnv.get.conf
    if (!Utils.isDynamicAllocationEnabled(sparkConf)) {
      ResourceProfile.getOrCreateDefaultProfile(sparkConf)
        .getCustomExecutorResources()
    } else {
      super.getCustomExecutorResources()
    }
  }
}

object ResourceProfile extends Logging {
  // task resources
  /**
   * built-in task resource: cpus
   */
  val CPUS = "cpus"
  // Executor resources
  // Make sure add new executor resource in below allSupportedExecutorResources
  /**
   * built-in executor resource: cores
   */
  val CORES = "cores"
  /**
   * built-in executor resource: memory
   */
  val MEMORY = "memory"
  /**
   * built-in executor resource: offHeap
   */
  val OFFHEAP_MEM = "offHeap"
  /**
   * built-in executor resource: memoryOverhead
   */
  val OVERHEAD_MEM = "memoryOverhead"
  /**
   * built-in executor resource: pyspark.memory
   */
  val PYSPARK_MEM = "pyspark.memory"

  /**
   * Return all supported Spark built-in executor resources, custom resources like GPUs/FPGAs
   * are excluded.
   */
  def allSupportedExecutorResources: Array[String] =
    Array(CORES, MEMORY, OVERHEAD_MEM, PYSPARK_MEM, OFFHEAP_MEM)

  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

  private lazy val nextProfileId = new AtomicInteger(0)
  private val DEFAULT_PROFILE_LOCK = new Object()

  // The default resource profile uses the application level configs.
  // var so that it can be reset for testing purposes.
  @GuardedBy("DEFAULT_PROFILE_LOCK")
  private var defaultProfile: Option[ResourceProfile] = None
  private var defaultProfileExecutorResources: Option[DefaultProfileExecutorResources] = None

  private[spark] def getNextProfileId: Int = nextProfileId.getAndIncrement()

  private[spark] def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    DEFAULT_PROFILE_LOCK.synchronized {
      defaultProfile match {
        case Some(prof) => prof
        case None =>
          val taskResources = getDefaultTaskResources(conf)
          val executorResources = getDefaultExecutorResources(conf)
          val defProf = new ResourceProfile(executorResources, taskResources)
          defProf.setToDefaultProfile()
          defaultProfile = Some(defProf)
          logInfo(log"Default ResourceProfile created, executor resources: " +
            log"${MDC(EXECUTOR_RESOURCES, defProf.executorResources)}, task resources: " +
            log"${MDC(TASK_RESOURCES, defProf.taskResources)}")
          defProf
      }
    }
  }

  private[spark] def getDefaultProfileExecutorResources(
      conf: SparkConf): DefaultProfileExecutorResources = {
    defaultProfileExecutorResources.getOrElse {
      getOrCreateDefaultProfile(conf)
      defaultProfileExecutorResources.get
    }
  }

  private def getDefaultTaskResources(conf: SparkConf): Map[String, TaskResourceRequest] = {
    val cpusPerTask = conf.get(CPUS_PER_TASK)
    val treqs = new TaskResourceRequests().cpus(cpusPerTask)
    ResourceUtils.addTaskResourceRequests(conf, treqs)
    treqs.requests
  }

  private def getDefaultExecutorResources(conf: SparkConf): Map[String, ExecutorResourceRequest] = {
    val ereqs = new ExecutorResourceRequests()

    val isStandalone = conf.getOption("spark.master").exists(_.startsWith("spark://"))
    // Since local-cluster and standalone share the same StandaloneSchedulerBackend and Master,
    // and the Master will schedule based on resource profile, so we also need to create default
    // resource profile for local-cluster here as well as standalone.
    val isLocalCluster = conf.getOption("spark.master").exists(_.startsWith("local-cluster"))
    // By default, standalone executors take all available cores, do not have a specific value.
    val cores = if (isStandalone || isLocalCluster) {
      conf.getOption(EXECUTOR_CORES.key).map(_.toInt)
    } else {
      Some(conf.get(EXECUTOR_CORES))
    }
    cores.foreach(ereqs.cores)

    val memory = if (isStandalone || isLocalCluster) {
      SparkContext.executorMemoryInMb(conf)
    } else {
      conf.get(EXECUTOR_MEMORY)
    }
    ereqs.memory(memory.toString)
    val overheadMem = conf.get(EXECUTOR_MEMORY_OVERHEAD)
    overheadMem.map(mem => ereqs.memoryOverhead(mem.toString))
    val pysparkMem = conf.get(PYSPARK_EXECUTOR_MEMORY)
    pysparkMem.map(mem => ereqs.pysparkMemory(mem.toString))
    val offheapMem = Utils.executorOffHeapMemorySizeAsMb(conf)
    ereqs.offHeapMemory(offheapMem.toString)
    val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)
    execReq.foreach { req =>
      ereqs.resource(req.id.resourceName, req.amount, req.discoveryScript.orElse(""),
        req.vendor.orElse(""))
    }
    val customResourceNames = execReq.map(_.id.resourceName).toSet
    val customResources = ereqs.requests.filter(v => customResourceNames.contains(v._1))
    defaultProfileExecutorResources =
      Some(DefaultProfileExecutorResources(cores, memory, offheapMem, pysparkMem,
        overheadMem, customResources))
    ereqs.requests
  }

  // for testing only
  private[spark] def reInitDefaultProfile(conf: SparkConf): ResourceProfile = {
    clearDefaultProfile()
    // force recreate it after clearing
    getOrCreateDefaultProfile(conf)
  }

  private[spark] def clearDefaultProfile(): Unit = {
    DEFAULT_PROFILE_LOCK.synchronized {
      defaultProfile = None
      defaultProfileExecutorResources = None
    }
  }

  /*
   * Get the number of cpus per task if its set in the profile, otherwise return the
   * cpus per task for the default profile.
   */
  private[spark] def getTaskCpusOrDefaultForProfile(rp: ResourceProfile, conf: SparkConf): Int = {
    rp.getTaskCpus.getOrElse(conf.get(CPUS_PER_TASK))
  }

  /**
   * Get offHeap memory size from [[ExecutorResourceRequest]]
   * return 0 if MEMORY_OFFHEAP_ENABLED is false.
   */
  private[spark] def executorOffHeapMemorySizeAsMb(sparkConf: SparkConf,
      execRequest: ExecutorResourceRequest): Long = {
    Utils.checkOffHeapEnabled(sparkConf, execRequest.amount)
  }

  private[spark] case class ExecutorResourcesOrDefaults(
      cores: Option[Int], // Can only be None for standalone and local-cluster.
      executorMemoryMiB: Long,
      memoryOffHeapMiB: Long,
      pysparkMemoryMiB: Long,
      memoryOverheadMiB: Long,
      totalMemMiB: Long,
      customResources: Map[String, ExecutorResourceRequest])

  private[spark] case class DefaultProfileExecutorResources(
      cores: Option[Int], // Can only be None for standalone cluster.
      executorMemoryMiB: Long,
      memoryOffHeapMiB: Long,
      pysparkMemoryMiB: Option[Long],
      memoryOverheadMiB: Option[Long],
      customResources: Map[String, ExecutorResourceRequest])

  private[spark] def calculateOverHeadMemory(
      overHeadMemFromConf: Option[Long],
      minimumOverHeadMemoryFromConf: Long,
      executorMemoryMiB: Long,
      overheadFactor: Double): Long = {
    overHeadMemFromConf.getOrElse(math.max((overheadFactor * executorMemoryMiB).toInt,
      minimumOverHeadMemoryFromConf))
  }

  /**
   * Gets the full list of resources to allow a cluster manager to request the appropriate
   * container. If the resource profile is not the default one we either get the resources
   * specified in the profile or fall back to the default profile resource size for everything
   * except for custom resources.
   */
  private[spark] def getResourcesForClusterManager(
      rpId: Int,
      execResources: Map[String, ExecutorResourceRequest],
      minimumOverheadMemory: Long,
      overheadFactor: Double,
      conf: SparkConf,
      isPythonApp: Boolean,
      resourceMappings: Map[String, String]): ExecutorResourcesOrDefaults = {
    val defaultResources = getDefaultProfileExecutorResources(conf)
    // set all the default values, which may change for custom ResourceProfiles
    var cores = defaultResources.cores
    var executorMemoryMiB = defaultResources.executorMemoryMiB
    var memoryOffHeapMiB = defaultResources.memoryOffHeapMiB
    var pysparkMemoryMiB = defaultResources.pysparkMemoryMiB.getOrElse(0L)
    var memoryOverheadMiB = calculateOverHeadMemory(defaultResources.memoryOverheadMiB,
      minimumOverheadMemory, executorMemoryMiB, overheadFactor)

    val finalCustomResources = if (rpId != DEFAULT_RESOURCE_PROFILE_ID) {
      val customResources = new mutable.HashMap[String, ExecutorResourceRequest]
      execResources.foreach { case (r, execReq) =>
        r match {
          case ResourceProfile.MEMORY =>
            executorMemoryMiB = execReq.amount
          case ResourceProfile.OVERHEAD_MEM =>
            memoryOverheadMiB = execReq.amount
          case ResourceProfile.PYSPARK_MEM =>
            pysparkMemoryMiB = execReq.amount
          case ResourceProfile.OFFHEAP_MEM =>
            memoryOffHeapMiB = executorOffHeapMemorySizeAsMb(conf, execReq)
          case ResourceProfile.CORES =>
            cores = Some(execReq.amount.toInt)
          case rName =>
            val nameToUse = resourceMappings.getOrElse(rName, rName)
            customResources(nameToUse) = execReq
        }
      }
      customResources.toMap
    } else {
      defaultResources.customResources.map { case (rName, execReq) =>
        val nameToUse = resourceMappings.getOrElse(rName, rName)
        (nameToUse, execReq)
      }
    }
    // only add in pyspark memory if actually a python application
    val pysparkMemToUseMiB = if (isPythonApp) {
      pysparkMemoryMiB
    } else {
      0L
    }
    val totalMemMiB =
      (executorMemoryMiB + memoryOverheadMiB + memoryOffHeapMiB + pysparkMemToUseMiB)
    ExecutorResourcesOrDefaults(cores, executorMemoryMiB, memoryOffHeapMiB,
      pysparkMemToUseMiB, memoryOverheadMiB, totalMemMiB, finalCustomResources)
  }

  private[spark] val PYSPARK_MEMORY_LOCAL_PROPERTY = "resource.pyspark.memory"
  private[spark] val EXECUTOR_CORES_LOCAL_PROPERTY = "resource.executor.cores"
}
