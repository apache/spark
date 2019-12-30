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

/**
 * Internal immutable version of Resource profile to associate with an RDD. This is an
 * immutable version that we don't have to worry about changing and adds in some extra
 * functionality.
 */
@Evolving
private[spark] class ImmutableResourceProfile(
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest]) extends Serializable with Logging {

  // _id is only a var for testing purposes
  private var _id = ImmutableResourceProfile.getNextProfileId

  def id: Int = _id

  // Note that some cluster managers don't set the executor cores explicitly so
  // be sure to check the Option as required
  def getExecutorCores: Option[Int] = {
    executorResources.get(ResourceProfile.CORES).map(_.amount.toInt)
  }

  def getTaskCpus: Option[Int] = {
    taskResources.get(ResourceProfile.CPUS).map(_.amount.toInt)
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
    s"Profile: id = ${_id}, executor resources: ${executorResources.mkString(",")}, " +
      s"task resources: ${taskResources.mkString(",")}"
  }
}

private[spark] object ImmutableResourceProfile extends Logging {

  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

  val SPARK_RP_EXEC_PREFIX = "spark.resourceProfile.executor"

  private def resourceProfileIntConfPrefix(rpId: Int): String = {
    s"$SPARK_RP_EXEC_PREFIX.$rpId.resource."
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
    val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)
    execReq.foreach { req =>
      val name = req.id.resourceName
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

  def getCustomTaskResources(
      rp: ImmutableResourceProfile): Map[String, TaskResourceRequest] = {
    rp.taskResources.filterKeys(k => !k.equals(ResourceProfile.CPUS))
  }

  def getCustomExecutorResources(
      rp: ImmutableResourceProfile): Map[String, ExecutorResourceRequest] = {
    rp.executorResources.filterKeys(k => !ResourceProfile.allSupportedExecutorResources.contains(k))
  }

  /**
   * Create the ResourceProfile internal confs that are used to pass between Driver and Executors.
   * It pulls any custom resources from the ResourceProfile and returns a Map of key
   * to value where the keys get formatted as:
   *
   * spark.resourceProfile.executor.[rpId].resource.[resourceName].[amount, vendor, discoveryScript]
   */
  def createResourceProfileInternalConfs(rp: ImmutableResourceProfile): Map[String, String] = {
    val ret = new mutable.HashMap[String, String]()

    val customResource = getCustomExecutorResources(rp)
    customResource.foreach { case (name, req) =>
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
      val resourceId = ResourceID(SPARK_EXECUTOR_PREFIX, rName)
      ResourceRequest(resourceId, amount, discoveryScript, vendor)
    }
    resourceReqs
  }
}
