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
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceUtils.{RESOURCE_DOT, RESOURCE_PREFIX}

/**
 * Resource profile to associate with an RDD. A ResourceProfile allows the user to
 * specify executor and task requirements for an RDD that will get applied during a
 * stage. This allows the user to change the resource requirements between stages.
 *
 * This class is private now for initial development, once we have the feature in place
 * this will become public.
 */
@Evolving
private[spark] class ResourceProfile() extends Serializable {

  private val _id = ResourceProfile.getNextProfileId
  private val _taskResources = new mutable.HashMap[String, TaskResourceRequest]()
  private val _executorResources = new mutable.HashMap[String, ExecutorResourceRequest]()

  def id: Int = _id
  def taskResources: Map[String, TaskResourceRequest] = _taskResources.toMap
  def executorResources: Map[String, ExecutorResourceRequest] = _executorResources.toMap

  /**
   * (Java-specific) gets a Java Map of resources to TaskResourceRequest
   */
  def taskResourcesJMap: JMap[String, TaskResourceRequest] = _taskResources.asJava

  /**
   * (Java-specific) gets a Java Map of resources to ExecutorResourceRequest
   */
  def executorResourcesJMap: JMap[String, ExecutorResourceRequest] = _executorResources.asJava

  def reset(): Unit = {
    _taskResources.clear()
    _executorResources.clear()
  }

  def require(requests: ExecutorResourceRequests): this.type = {
    _executorResources ++= requests.requests
    this
  }

  def require(requests: TaskResourceRequests): this.type = {
    _taskResources ++= requests.requests
    this
  }

  override def toString(): String = {
    s"Profile: id = ${_id}, executor resources: ${_executorResources}, " +
      s"task resources: ${_taskResources}"
  }
}

private[spark] object ResourceProfile extends Logging {
  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

  val SPARK_RP_TASK_PREFIX = "spark.resourceProfile.task"
  val SPARK_RP_EXEC_PREFIX = "spark.resourceProfile.executor"

  // Helper class for constructing the resource profile internal configs used to pass to
  // executors
  case class ResourceProfileInternalConf(componentName: String,
      id: Int, resourceName: String) {
    def confPrefix: String = s"$componentName.$id.${RESOURCE_DOT}$resourceName."
    def amountConf: String = s"$confPrefix${ResourceUtils.AMOUNT}"
    def discoveryScriptConf: String = s"$confPrefix${ResourceUtils.DISCOVERY_SCRIPT}"
    def vendorConf: String = s"$confPrefix${ResourceUtils.VENDOR}"
  }

  val CPUS = "cpus"
  val CORES = "cores"
  val MEMORY = "memory"
  val OVERHEAD_MEM = "memoryOverhead"
  val PYSPARK_MEM = "pyspark.memory"

  private lazy val nextProfileId = new AtomicInteger(0)

  // The default resource profile uses the application level configs.
  // Create the default profile immediately to get ID 0, its initialized later when fetched.
  private val defaultProfileRef: AtomicReference[ResourceProfile] =
    new AtomicReference[ResourceProfile](new ResourceProfile())

  assert(defaultProfileRef.get().id == DEFAULT_RESOURCE_PROFILE_ID,
    s"Default Profile must have the default profile id: $DEFAULT_RESOURCE_PROFILE_ID")

  def getNextProfileId: Int = nextProfileId.getAndIncrement()

  def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    val defaultProf = defaultProfileRef.get()
    // check to see if the default profile was initialized yet
    if (defaultProf.executorResources == Map.empty) {
      synchronized {
        val prof = defaultProfileRef.get()
        if (prof.executorResources == Map.empty) {
          addDefaultTaskResources(prof, conf)
          addDefaultExecutorResources(prof, conf)
        }
        prof
      }
    } else {
      defaultProf
    }
  }

  private def addDefaultTaskResources(rprof: ResourceProfile, conf: SparkConf): Unit = {
    val cpusPerTask = conf.get(CPUS_PER_TASK)
    val treqs = new TaskResourceRequests().cpus(cpusPerTask)
    val taskReq = ResourceUtils.parseResourceRequirements(conf, SPARK_TASK_PREFIX)
    taskReq.foreach { req =>
      val name = s"${RESOURCE_PREFIX}.${req.resourceName}"
      treqs.resource(name, req.amount)
    }
    rprof.require(treqs)
  }

  private def addDefaultExecutorResources(rprof: ResourceProfile, conf: SparkConf): Unit = {
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(conf.get(EXECUTOR_CORES))
    ereqs.memory(conf.get(EXECUTOR_MEMORY).toString)
    val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)
    execReq.foreach { req =>
      val name = s"${RESOURCE_PREFIX}.${req.id.resourceName}"
      ereqs.resource(name, req.amount, req.discoveryScript.getOrElse(""),
        req.vendor.getOrElse(""))
    }
    rprof.require(ereqs)
  }

  // for testing purposes
  def resetDefaultProfile(conf: SparkConf): Unit = getOrCreateDefaultProfile(conf).reset()

  /**
   * Create the ResourceProfile internal confs that are used to pass between Driver and Executors.
   * It pulls any "resource." resources from the ResourceProfile and returns a Map of key
   * to value where the keys get formatted as:
   *
   * spark.resourceProfile.executor.[rpId].resource.[resourceName].[amount, vendor, discoveryScript]
   * spark.resourceProfile.task.[rpId].resource.[resourceName].amount
   *
   * Keep this here as utility a function rather then in public ResourceProfile interface because
   * end users shouldn't need this.
   */
  def createResourceProfileInternalConfs(rp: ResourceProfile): Map[String, String] = {
    val res = new mutable.HashMap[String, String]()
    // task resources
    rp.taskResources.filterKeys(_.startsWith(RESOURCE_DOT)).foreach { case (name, req) =>
      val taskIntConf = ResourceProfileInternalConf(SPARK_RP_TASK_PREFIX, rp.id, name)
      res(s"${taskIntConf.amountConf}") = req.amount.toString
    }
    // executor resources
    rp.executorResources.filterKeys(_.startsWith(RESOURCE_DOT)).foreach { case (name, req) =>
      val execIntConf = ResourceProfileInternalConf(SPARK_RP_EXEC_PREFIX, rp.id, name)

      res(execIntConf.amountConf) = req.amount.toString
      if (req.vendor.nonEmpty) res(execIntConf.vendorConf) = req.vendor
      if (req.discoveryScript.nonEmpty) res(execIntConf.discoveryScriptConf) = req.discoveryScript
    }
    res.toMap
  }

  /**
   * Parse out just the resourceName given the map of confs. It only looks for confs that
   * end with .amount because we should always have one of those for every resource.
   * Format is expected to be: [resourcesname].amount, where resourceName could have multiple
   * .'s like resource.gpu.foo.amount
   */
  private def listResourceNames(confs: Map[String, String]): Seq[String] = {
    confs.filterKeys(_.endsWith(ResourceUtils.AMOUNT)).
      map { case (key, _) => key.substring(0, key.lastIndexOf(s".${ResourceUtils.AMOUNT}")) }.toSeq
  }

  /**
   * Get a ResourceProfile with the task requirements from the internal resource confs.
   * The configs looks like:
   * spark.resourceProfile.task.[rpId].resource.[resourceName].amount
   */
  def getTaskRequirementsFromInternalConfs(sparkConf: SparkConf, rpId: Int): ResourceProfile = {
    val rp = new ResourceProfile()
    val taskRpIdConfPrefix = s"${SPARK_RP_TASK_PREFIX}.${rpId}."
    val taskConfs = sparkConf.getAllWithPrefix(taskRpIdConfPrefix).toMap
    val taskResourceNames = listResourceNames(taskConfs)
    val taskResourceRequests = new TaskResourceRequests()
    taskResourceNames.foreach { resource =>
      val amount = taskConfs.get(s"${resource}.amount").get.toInt
      taskResourceRequests.resource(resource, amount)
    }
    rp.require(taskResourceRequests)
    rp
  }

  /**
   * Get the executor ResourceRequests from the internal resource confs
   * The configs looks like:
   * spark.resourceProfile.executor.[rpId].resource.gpu.[amount, vendor, discoveryScript]
   */
  def getResourceRequestsFromInternalConfs(
      sparkConf: SparkConf,
      rpId: Int): Seq[ResourceRequest] = {
    val execRpIdConfPrefix = s"${SPARK_RP_EXEC_PREFIX}.${rpId}.${RESOURCE_DOT}"
    val execConfs = sparkConf.getAllWithPrefix(execRpIdConfPrefix).toMap
    val execResourceNames = listResourceNames(execConfs)
    val resourceReqs = execResourceNames.map { resource =>
      val amount = execConfs.get(s"${resource}.amount").get.toInt
      val vendor = execConfs.get(s"${resource}.vendor")
      val discoveryScript = execConfs.get(s"${resource}.discoveryScript")
      ResourceRequest(ResourceID(SPARK_EXECUTOR_PREFIX, resource), amount, discoveryScript, vendor)
    }
    resourceReqs
  }
}
