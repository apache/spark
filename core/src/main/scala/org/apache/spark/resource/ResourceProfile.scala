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
import scala.collection.immutable.HashMap
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
 * Only support a subset of the resources for now. The config names supported correspond to the
 * regular Spark configs with the prefix removed. For instance overhead memory in this api
 * is memoryOverhead, which is spark.executor.memoryOverhead with spark.executor removed.
 * Resources like GPUs are resource.gpu (spark configs spark.executor.resource.gpu.*)
 *
 * Executor:
 *   memory - heap
 *   memoryOverhead
 *   pyspark.memory
 *   cores
 *   resource.[resourceName] - GPU, FPGA, etc
 *
 * Task requirements:
 *   cpus
 *   resource.[resourceName] - GPU, FPGA, etc
 *
 * This class is private now for initial development, once we have the feature in place
 * this will become public.
 */
@Evolving
private[spark] class ResourceProfile() extends Serializable {

  private val _id = ResourceProfile.getNextProfileId
  private val _taskResources = new mutable.HashMap[String, TaskResourceRequest]()
  private val _executorResources = new mutable.HashMap[String, ExecutorResourceRequest]()

  private val allowedExecutorResources = HashMap[String, Boolean](
    (ResourceProfile.MEMORY -> true),
    (ResourceProfile.OVERHEAD_MEM -> true),
    (ResourceProfile.PYSPARK_MEM -> true),
    (ResourceProfile.CORES -> true))

  private val allowedTaskResources = HashMap[String, Boolean]((
    ResourceProfile.CPUS -> true))

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

  def require(request: TaskResourceRequest): this.type = {
    if (allowedTaskResources.contains(request.resourceName) ||
      request.resourceName.startsWith(RESOURCE_DOT)) {
      _taskResources(request.resourceName) = request
    } else {
      throw new IllegalArgumentException(s"Task resource not allowed: ${request.resourceName}")
    }
    this
  }

  def require(request: ExecutorResourceRequest): this.type = {
    if (allowedExecutorResources.contains(request.resourceName) ||
        request.resourceName.startsWith(RESOURCE_DOT)) {
      _executorResources(request.resourceName) = request
    } else {
      throw new IllegalArgumentException(s"Executor resource not allowed: ${request.resourceName}")
    }
    this
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceProfile =>
        that.getClass == this.getClass &&
          that._taskResources == _taskResources && that._executorResources == _executorResources
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(_taskResources, _executorResources).hashCode()

  override def toString(): String = {
    s"Profile: id = ${_id}, executor resources: ${_executorResources}, " +
      s"task resources: ${_taskResources}"
  }
}

private[spark] object ResourceProfile extends Logging {
  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

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
    rprof.require(new TaskResourceRequest(CPUS, cpusPerTask))
    val taskReq = ResourceUtils.parseResourceRequirements(conf, SPARK_TASK_PREFIX)

    taskReq.foreach { req =>
      val name = s"${RESOURCE_PREFIX}.${req.resourceName}"
      rprof.require(new TaskResourceRequest(name, req.amount))
    }
  }

  private def addDefaultExecutorResources(rprof: ResourceProfile, conf: SparkConf): Unit = {
    rprof.require(new ExecutorResourceRequest(CORES, conf.get(EXECUTOR_CORES), None, None, None))
    rprof.require(
      new ExecutorResourceRequest(MEMORY, conf.get(EXECUTOR_MEMORY).toInt, Some("b"), None, None))
    val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)

    execReq.foreach { req =>
      val name = s"${RESOURCE_PREFIX}.${req.id.resourceName}"
      val execReq = new ExecutorResourceRequest(name, req.amount, None, req.discoveryScript,
        req.vendor)
      rprof.require(execReq)
    }
  }

  // for testing purposes
  def resetDefaultProfile(conf: SparkConf): Unit = getOrCreateDefaultProfile(conf).reset()
}
