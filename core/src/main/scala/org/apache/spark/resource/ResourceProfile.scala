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

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY

/**
 * Resource profile to associate with an RDD. A ResourceProfile allows the user to
 * specify executor and task requirements for an RDD that will get applied during a
 * stage. This allows the user to change the resource requirements between stages.
 * This is meant to be immutable so user can't change it after building.
 *
 * This api is currently private until the rest of the pieces are in place and then it
 * will become public.
 */
@Evolving
private[spark] class ResourceProfile(
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

  private lazy val nextProfileId = new AtomicInteger(0)
  private val DEFAULT_PROFILE_LOCK = new Object()

  // The default resource profile uses the application level configs.
  // var so that it can be reset for testing purposes.
  @GuardedBy("DEFAULT_PROFILE_LOCK")
  private var defaultProfile: Option[ResourceProfile] = None

  private[spark] def getNextProfileId: Int = nextProfileId.getAndIncrement()

  private[spark] def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    DEFAULT_PROFILE_LOCK.synchronized {
      defaultProfile match {
        case Some(prof) => prof
        case None =>
          val taskResources = getDefaultTaskResources(conf)
          val executorResources = getDefaultExecutorResources(conf)
          val defProf = new ResourceProfile(executorResources, taskResources)
          defProf.setToDefaultProfile
          defaultProfile = Some(defProf)
          logInfo("Default ResourceProfile created, executor resources: " +
            s"${defProf.executorResources}, task resources: " +
            s"${defProf.taskResources}")
          defProf
      }
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
      ereqs.resource(name, req.amount, req.discoveryScript.orElse(""),
        req.vendor.orElse(""))
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
    DEFAULT_PROFILE_LOCK.synchronized {
      defaultProfile = None
    }
  }

  private[spark] def getCustomTaskResources(
      rp: ResourceProfile): Map[String, TaskResourceRequest] = {
    rp.taskResources.filterKeys(k => !k.equals(ResourceProfile.CPUS))
  }

  private[spark] def getCustomExecutorResources(
      rp: ResourceProfile): Map[String, ExecutorResourceRequest] = {
    rp.executorResources.filterKeys(k => !ResourceProfile.allSupportedExecutorResources.contains(k))
  }
}
