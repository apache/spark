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

import scala.jdk.CollectionConverters._

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
  def cpus(amount: Int): this.type = cpus(amount.toDouble)

  /**
   * Specify number of cpus per Task.
   * This is a convenient API to add [[TaskResourceRequest]] for cpus.
   *
   * @param amount Number of cpus to allocate per Task. Any positive value is valid, including
   *               fractional ones: below 1 (e.g. 0.2) to let multiple tasks share a CPU core,
   *               or above 1 (e.g. 1.5). The value is rounded to the nearest 1e-9 for internal
   *               CPU accounting, so precision beyond 9 decimal places is not preserved (this
   *               also absorbs the binary noise of computed `Double`s such as `0.1 + 0.2`).
   */
  @Since("4.3.0")
  def cpus(amount: Double): this.type = {
    // A positive amount below half of the internal accounting scale (1e-9) would silently
    // round to zero cpus downstream; reject it here where the original value is still
    // visible. Validation lives at this request entry point rather than in the
    // TaskResourceRequest constructor, which must stay lenient to deserialize historical
    // event logs and history-server stores written before cpus amounts were validated.
    require(!amount.isNaN && !amount.isInfinity &&
      CpuAmount.normalize(BigDecimal(amount.toString)).signum > 0,
      s"The cpus amount ${amount} must be at least 1e-9.")
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
    // Cpus amounts are validated only at request entry points (the TaskResourceRequest
    // constructor stays lenient to deserialize persisted data), so route them through the
    // validating method regardless of which entry point the caller picked.
    if (resourceName == CPUS) {
      cpus(amount)
    } else {
      val treq = new TaskResourceRequest(resourceName, amount)
      _taskResources.put(resourceName, treq)
      this
    }
  }

  /**
   * Add a certain [[TaskResourceRequest]] to the request set.
   */
  def addRequest(treq: TaskResourceRequest): this.type = {
    if (treq.resourceName == CPUS) {
      cpus(treq.amount)
    } else {
      _taskResources.put(treq.resourceName, treq)
      this
    }
  }

  override def toString: String = {
    s"Task resource requests: ${_taskResources}"
  }
}
