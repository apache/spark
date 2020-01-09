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
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY

/**
 * Resource profile to associate with an RDD.
 * A ResourceProfile allows the user to specify executor and task requirements for an RDD
 * that will get applied during a stage. This allows the user to change the resource
 * requirements between stages.
 * This is meant to be immutable so user can't change it after buliding.
 */
@Evolving
class ResourceProfile(
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest]) extends Serializable with Logging {

  // _id is only a var for testing purposes
  private var _id = ResourceProfile.getNextProfileId

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

  override def hashCode(): Int = Seq(taskResources, executorResources).hashCode()

  override def toString(): String = {
    s"Profile: id = ${_id}, executor resources: ${executorResources.mkString(",")}, " +
      s"task resources: ${taskResources.mkString(",")}"
  }
}

object ResourceProfile extends Logging {

  // task resources
  val CPUS = "cpus"
  // Executor resources
  val CORES = "cores"
  val MEMORY = "memory"
  val OVERHEAD_MEM = "memoryOverhead"
  val PYSPARK_MEM = "pyspark.memory"

  // all supported spark executor resources (minus the custom resources like GPUs/FPGAs)
  val allSupportedExecutorResources = Seq(CORES, MEMORY, OVERHEAD_MEM, PYSPARK_MEM)

  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

  private[spark] val SPARK_RP_EXEC_PREFIX = "spark.resourceProfile.executor"

  private[spark] def resourceProfileIntConfPrefix(rpId: Int): String = {
    s"$SPARK_RP_EXEC_PREFIX.$rpId."
  }

  private[spark] def resourceProfileCustomResourceIntConfPrefix(rpId: Int): String = {
    s"${resourceProfileIntConfPrefix(rpId)}resource."
  }

  // Helper class for constructing the resource profile internal configs used to pass to
  // executors. The configs look like:
  // spark.resourceProfile.executor.[rpId].[resourceName].[amount, vendor, discoveryScript]
  // Note the prefix is passed in because custom resource configs have an extra "resource."
  // in the name to differentiate them from the standard spark configs.
  private[spark] case class ResourceProfileInternalConf(prefix: String, resourceName: String) {
    def resourceNameConf: String = s"$prefix$resourceName"
    def resourceNameAndAmount: String = s"$resourceName.${ResourceUtils.AMOUNT}"
    def resourceNameAndDiscovery: String = s"$resourceName.${ResourceUtils.DISCOVERY_SCRIPT}"
    def resourceNameAndVendor: String = s"$resourceName.${ResourceUtils.VENDOR}"

    def amountConf: String = s"$prefix$resourceNameAndAmount"
    def discoveryScriptConf: String = s"$prefix$resourceNameAndDiscovery"
    def vendorConf: String = s"$prefix$resourceNameAndVendor"
  }

  private lazy val nextProfileId = new AtomicInteger(0)

  // The default resource profile uses the application level configs.
  // var so that it can be reset for testing purposes.
  private var defaultProfileRef: AtomicReference[ResourceProfile] =
    new AtomicReference[ResourceProfile]()

  private[spark] def getNextProfileId: Int = nextProfileId.getAndIncrement()

  private[spark] def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    val defaultProf = defaultProfileRef.get()
    // check to see if the default profile was initialized yet
    if (defaultProf == null) {
      val prof = synchronized {
        val taskResources = getDefaultTaskResources(conf)
        val executorResources = getDefaultExecutorResources(conf)
        val defProf = new ResourceProfile(executorResources, taskResources)
        logInfo("Default ResourceProfile created, executor resources: " +
          s"${defProf.executorResources}, task resources: " +
          s"${defProf.taskResources}")
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
    ResourceUtils.addTaskResourceRequests(conf, treqs)
    treqs.requests
  }

  private def getDefaultExecutorResources(conf: SparkConf): Map[String, ExecutorResourceRequest] = {
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(conf.get(EXECUTOR_CORES))
    ereqs.memory(conf.get(EXECUTOR_MEMORY).toString)
    conf.get(EXECUTOR_MEMORY_OVERHEAD).map(mem => ereqs.memoryOverhead(mem.toString))
    conf.get(PYSPARK_EXECUTOR_MEMORY).map(mem => ereqs.pysparkMemory(mem.toString))
    val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)
    execReq.foreach { req =>
      val name = req.id.resourceName
      ereqs.resource(name, req.amount, req.discoveryScript.getOrElse(""),
        req.vendor.getOrElse(""))
    }
    ereqs.requests
  }

  // for testing only
  private[spark] def reInitDefaultProfile(conf: SparkConf): Unit = {
    clearDefaultProfile
    // force recreate it after clearing
    getOrCreateDefaultProfile(conf)
  }

  // for testing only
  private[spark] def clearDefaultProfile: Unit = {
    defaultProfileRef = new AtomicReference[ResourceProfile]()
  }

  private[spark] def getCustomTaskResources(
      rp: ResourceProfile): Map[String, TaskResourceRequest] = {
    rp.taskResources.filterKeys(k => !k.equals(ResourceProfile.CPUS))
  }

  private[spark] def getCustomExecutorResources(
      rp: ResourceProfile): Map[String, ExecutorResourceRequest] = {
    rp.executorResources.filterKeys(k => !ResourceProfile.allSupportedExecutorResources.contains(k))
  }

  /**
   * Create the ResourceProfile internal confs that are used to pass between Driver and Executors.
   * It pulls any custom resources from the ResourceProfile and the pyspark.memory and
   * returns a Map of key to value where the keys get formatted as:
   *
   * spark.resourceProfile.executor.[rpId].[resourceName].[amount, vendor, discoveryScript]
   * Note that for custom resources the resourceName looks like: resource.gpu.
   */
  private[spark] def createResourceProfileInternalConfs(
      rp: ResourceProfile
  ): Map[String, String] = {
    val ret = new mutable.HashMap[String, String]()
    val customResource = getCustomExecutorResources(rp)
    customResource.foreach { case (name, req) =>
      val execIntConf =
        ResourceProfileInternalConf(resourceProfileCustomResourceIntConfPrefix(rp.id), name)
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
   * .'s like gpu.foo.amount
   */
  private def listResourceNames(confs: Map[String, String]): Seq[String] = {
    confs.filterKeys(_.endsWith(ResourceUtils.AMOUNT)).
      map { case (key, _) => key.substring(0, key.lastIndexOf(s".${ResourceUtils.AMOUNT}")) }.toSeq
  }

  /**
   * Get the executor custom ResourceRequests(GPUs, FPGAs, etc) from the internal resource confs
   * The configs looks like:
   * spark.resourceProfile.executor.[rpId].resource.[resourceName].[amount, vendor, discoveryScript]
   */
  private[spark] def getCustomResourceRequestsFromInternalConfs(
      sparkConf: SparkConf,
      rpId: Int): Seq[ResourceRequest] = {
    val execRpIdConfPrefix = resourceProfileCustomResourceIntConfPrefix(rpId)
    val execConfs = sparkConf.getAllWithPrefix(execRpIdConfPrefix).toMap
    val execResourceNames = listResourceNames(execConfs)
    val resourceReqs = execResourceNames.map { rName =>
      val intConf =
        ResourceProfileInternalConf(execRpIdConfPrefix, rName)
      val amount = execConfs.get(intConf.resourceNameAndAmount).get.toInt
      val vendor = execConfs.get(intConf.resourceNameAndVendor)
      val discoveryScript = execConfs.get(intConf.resourceNameAndDiscovery)
      val resourceId = ResourceID(SPARK_EXECUTOR_PREFIX, rName)
      ResourceRequest(resourceId, amount, discoveryScript, vendor)
    }
    resourceReqs
  }
}
