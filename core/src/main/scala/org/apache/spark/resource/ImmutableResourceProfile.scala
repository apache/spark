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

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceUtils.{RESOURCE_DOT, RESOURCE_PREFIX}

/**
 * Internal immutable version of Resource profile to associate with an RDD. This is an
 * immutable version that we don't have to worry about changing and adds in some extra
 * functionality.
 */
@Evolving
private[spark] class ImmutableResourceProfile(
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest]) extends Serializable with Logging {

  private var _id = ImmutableResourceProfile.getNextProfileId
  private var _executorResourceNumParts: Option[Map[String, Int]] = None
  private var _limitingResource: Option[String] = None
  private var _maxTasksPerExecutor: Option[Int] = None

  def id: Int = _id

  def getExecutorCores: Option[Int] = {
    executorResources.get(ResourceProfile.CORES).map(_.amount.toInt)
  }

  def getTaskCpus: Option[Int] = {
    taskResources.get(ResourceProfile.CPUS).map(_.amount.toInt)
  }

  def getNumSlotsPerAddress(resource: String, sparkConf: SparkConf): Int = {
    _executorResourceNumParts.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
    }
    _executorResourceNumParts.get.getOrElse(resource,
      throw new SparkException(s"Resource $resource doesn't existing in profile id: $id"))
  }

  // maximum tasks you could put on an executor with this profile based on the limiting resource
  def maxTasksPerExecutor(sparkConf: SparkConf): Int = {
    _maxTasksPerExecutor.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
      _maxTasksPerExecutor.get
    }
  }

  def limitingResource(sparkConf: SparkConf): String = {
    _limitingResource.getOrElse {
      calculateTasksAndLimitingResource(sparkConf)
      _limitingResource.get
    }
  }

  /**
   * Utility function to calculate the number of tasks you can run on a single Executor based
   * on the task and executor resource requests in the ResourceProfile. This will be based
   * off the resource that is most restrictive. For instance, if the executor
   * request is for 4 cpus and 2 gpus and your task request is for 1 cpu and 1 gpu each, the
   * limiting resource is gpu, and this function will return 2.
   */
  private def calculateTasksAndLimitingResource(sparkConf: SparkConf): Unit = synchronized {
    val coresPerExecutor = getExecutorCores.getOrElse(sparkConf.get(EXECUTOR_CORES))
    val cpusPerTask = taskResources.get(ResourceProfile.CPUS)
      .map(_.amount).getOrElse(sparkConf.get(CPUS_PER_TASK).toDouble).toInt
    val tasksBasedOnCores = coresPerExecutor / cpusPerTask
    var limitingResource = ResourceProfile.CPUS
    val numPartsMap = new mutable.HashMap[String, Int]
    var taskLimit = tasksBasedOnCores
    logInfo(s"in calculateTasksAndLimiting $taskLimit on cores")
    executorResources.foreach { case (rName, request) =>
      logInfo(s"executor resource $rName")
      val taskReq = taskResources.get(rName).map(_.amount).getOrElse(0.0)
      numPartsMap(rName) = 1
      if (taskReq > 0.0) {
        val (numPerTask, parts) = ResourceUtils.calculateAmountAndPartsForFraction(taskReq)
        numPartsMap(rName) = parts
        val numTasks = ((request.amount * parts) / numPerTask).toInt
        logInfo(s"executor resource $rName num " +
          s"tasks $numTasks ${request.amount} $parts $numPerTask")

        if (numTasks < taskLimit) {
          limitingResource = rName
          taskLimit = numTasks
        }
      }
    }
    logInfo(s"Limiting resource is $limitingResource at $taskLimit tasks per executor")
    _executorResourceNumParts = Some(numPartsMap.toMap)
    _maxTasksPerExecutor = Some(taskLimit)
    _limitingResource = Some(limitingResource)
  }


  // testing only
  def setToDefaultProfile(): Unit = {
    _id = ImmutableResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ImmutableResourceProfile =>
        that.getClass == this.getClass && that.id == _id &&
          that.taskResources == taskResources && that.executorResources == executorResources
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(taskResources, executorResources).hashCode()

  override def toString(): String = {
    s"Profile: id = ${_id}, executor resources: ${executorResources}, " +
      s"task resources: ${taskResources}"
  }
}

private[spark] object ImmutableResourceProfile extends Logging {

  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

  val SPARK_RP_EXEC_PREFIX = "spark.resourceProfile.executor"

  private def resourceProfileIntConfPrefix(rpId: Int): String = {
    s"$SPARK_RP_EXEC_PREFIX.$rpId."
  }

  // Helper class for constructing the resource profile internal configs used to pass to
  // executors. The configs look like:
  // spark.resourceProfile.executor.[rpId].[resourceName].[amount, vendor, discoveryScript]
  // Note custom resources have name like resource.gpu.
  case class ResourceProfileInternalConf(id: Int, resourceName: String) {
    def resourceNameConf: String = s"${resourceProfileIntConfPrefix(id)}$resourceName"
    def resourceNameAndAmount: String = s"$resourceName.${ResourceUtils.AMOUNT}"
    def resourceNameAndDiscovery: String = s"$resourceName.${ResourceUtils.DISCOVERY_SCRIPT}"
    def resourceNameAndVendor: String = s"$resourceName.${ResourceUtils.VENDOR}"

    def amountConf: String = s"${resourceProfileIntConfPrefix(id)}$resourceNameAndAmount"
    def discoveryScriptConf: String =
      s"${resourceProfileIntConfPrefix(id)}$resourceNameAndDiscovery"
    def vendorConf: String = s"${resourceProfileIntConfPrefix(id)}$resourceNameAndVendor"
  }

  private lazy val nextProfileId = new AtomicInteger(0)

  // The default resource profile uses the application level configs.
  // var so that it can be reset for testing purposes.
  private var defaultProfileRef: AtomicReference[ImmutableResourceProfile] =
    new AtomicReference[ImmutableResourceProfile]()

  def getNextProfileId: Int = nextProfileId.getAndIncrement()

  def getOrCreateDefaultProfile(conf: SparkConf): ImmutableResourceProfile = {
    val defaultProf = defaultProfileRef.get()
    // check to see if the default profile was initialized yet
    if (defaultProf == null) {
      val prof = synchronized {
        val taskResources = getDefaultTaskResources(conf)
        val executorResources = getDefaultExecutorResources(conf)
        val defProf = new ImmutableResourceProfile(executorResources, taskResources)
        defProf.setToDefaultProfile
        defProf
      }
      prof
    } else {
      defaultProf
    }
  }

  private def getDefaultTaskResources(conf: SparkConf): Map[String, TaskResourceRequest] = {
    val cpusPerTask = conf.get(CPUS_PER_TASK)
    val treqs = new TaskResourceRequests().cpus(cpusPerTask)
    val taskReq = ResourceUtils.parseResourceRequirements(conf, SPARK_TASK_PREFIX)
    taskReq.foreach { req =>
      val name = s"${RESOURCE_PREFIX}.${req.resourceName}"
      // TODO - test this - have to handle if fractional and numParts set!!
      treqs.resource(name, req.amount/req.numParts)
    }
    treqs.requests
  }

  private def getDefaultExecutorResources(conf: SparkConf): Map[String, ExecutorResourceRequest] = {
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(conf.get(EXECUTOR_CORES))
    ereqs.memory(conf.get(EXECUTOR_MEMORY).toString)
    val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)
    logInfo(s"add default executor resources $execReq")
    execReq.foreach { req =>
      val name = s"${RESOURCE_PREFIX}.${req.id.resourceName}"
      logInfo(s"adding resour with name $name")
      ereqs.resource(name, req.amount, req.discoveryScript.getOrElse(""),
        req.vendor.getOrElse(""))
    }
    ereqs.requests
  }

  // for testing only
  def reInitDefaultProfile(conf: SparkConf): Unit = {
    clearDefaultProfile
    val prof = getOrCreateDefaultProfile(conf)
  }

  // for testing only
  def clearDefaultProfile: Unit = {
    defaultProfileRef = new AtomicReference[ImmutableResourceProfile]()
  }

  /**
   * Create the ResourceProfile internal confs that are used to pass between Driver and Executors.
   * It pulls any "resource." resources from the ResourceProfile and returns a Map of key
   * to value where the keys get formatted as:
   *
   * spark.resourceProfile.executor.[rpId].[resourceName].[amount, vendor, discoveryScript]
   *
   * Keep this here as utility a function rather then in public ResourceProfile interface because
   * end users doesn't need this.
   */
  def createResourceProfileInternalConfs(rp: ImmutableResourceProfile): Map[String, String] = {
    val ret = new mutable.HashMap[String, String]()
    rp.executorResources.filterKeys(_.startsWith(RESOURCE_DOT)).foreach { case (name, req) =>
      val execIntConf = ResourceProfileInternalConf(rp.id, name)
      ret(execIntConf.amountConf) = req.amount.toString
      if (req.vendor.nonEmpty) ret(execIntConf.vendorConf) = req.vendor
      if (req.discoveryScript.nonEmpty) ret(execIntConf.discoveryScriptConf) = req.discoveryScript
    }
    ret.toMap
  }

  /**
   * Parse out just the resourceName given the map of confs. It only looks for confs that
   * end with .amount because we should always have one of those for every resource.
   * Format is expected to be: [resourcename].amount, where resourceName could have multiple
   * .'s like resource.gpu.foo.amount
   */
  private def listResourceNames(confs: Map[String, String]): Seq[String] = {
    confs.filterKeys(_.endsWith(ResourceUtils.AMOUNT)).
      map { case (key, _) => key.substring(0, key.lastIndexOf(s".${ResourceUtils.AMOUNT}")) }.toSeq
  }

  /**
   * Get the executor ResourceRequests from the internal resource confs
   * The configs looks like:
   * spark.resourceProfile.executor.[rpId].[resourceName].[amount, vendor, discoveryScript]
   * Note that custom resources have names prefixed with resource., ie resource.gpu
   */
  def getResourceRequestsFromInternalConfs(
      sparkConf: SparkConf,
      rpId: Int): Seq[ResourceRequest] = {
    val execRpIdConfPrefix = resourceProfileIntConfPrefix(rpId)
    val execConfs = sparkConf.getAllWithPrefix(execRpIdConfPrefix).toMap
    val execResourceNames = listResourceNames(execConfs)
    val resourceReqs = execResourceNames.map { rName =>
      val intConf = ResourceProfileInternalConf(rpId, rName)
      val amount = execConfs.get(intConf.resourceNameAndAmount).get.toInt
      val vendor = execConfs.get(intConf.resourceNameAndVendor)
      val discoveryScript = execConfs.get(intConf.resourceNameAndDiscovery)
      // note resourceName at this point is resource.[something] because with ResourceProfiles
      // the name matches the spark conf. Strip off the resource. part here to match how global
      // custom resource confs are parsed and how any resource files are handled.
      val shortResourceName = rName.substring(RESOURCE_DOT.length)
      val resourceId = ResourceID(SPARK_EXECUTOR_PREFIX, shortResourceName)
      ResourceRequest(resourceId, amount, discoveryScript, vendor)
    }
    resourceReqs
  }
}
