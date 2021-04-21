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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.annotation.{Evolving, Since}
import org.apache.spark.resource.ResourceProfile._

/**
 * A set of task resource requests. This is used in conjunction with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 */
@Evolving
@Since("3.1.0")
class TaskResourceRequests() extends Serializable {

  private val _taskResources = new ConcurrentHashMap[String, TaskResourceRequest]()

  /**
   * Returns all the resource requests for the task.
   */
  def requests: Map[String, TaskResourceRequest] = _taskResources.asScala.toMap

  /**
   * (Java-specific) Returns all the resource requests for the task.
   */
  def requestsJMap: JMap[String, TaskResourceRequest] = requests.asJava

  /**
   * Specify number of cpus per Task.
   * This is a convenient API to add [[TaskResourceRequest]] for cpus.
   *
   * @param amount Number of cpus to allocate per Task.
   */
  def cpus(amount: Int): this.type = {
    val treq = new TaskResourceRequest(CPUS, amount)
    _taskResources.put(CPUS, treq)
    this
  }

  /**
   * Amount of a particular custom resource(GPU, FPGA, etc) to use.
   * This is a convenient API to add [[TaskResourceRequest]] for custom resources.
   *
   * @param resourceName Name of the resource.
   * @param amount Amount requesting as a Double to support fractional resource requests.
   *               Valid values are less than or equal to 0.5 or whole numbers. This essentially
   *               lets you configure X number of tasks to run on a single resource,
   *               ie amount equals 0.5 translates into 2 tasks per resource address.
   */
  def resource(resourceName: String, amount: Double): this.type = {
    val treq = new TaskResourceRequest(resourceName, amount)
    _taskResources.put(resourceName, treq)
    this
  }

  /**
   * Add a certain [[TaskResourceRequest]] to the request set.
   */
  def addRequest(treq: TaskResourceRequest): this.type = {
    _taskResources.put(treq.resourceName, treq)
    this
  }

  override def toString: String = {
    s"Task resource requests: ${_taskResources}"
  }
}
