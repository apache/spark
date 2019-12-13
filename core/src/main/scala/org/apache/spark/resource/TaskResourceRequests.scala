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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.resource.ResourceProfile._

/**
 * A set of task resource requests. This is used in conjuntion with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 *
 * This api is currently private until the rest of the pieces are in place and then it
 * will become public.
 */
private[spark] class TaskResourceRequests() extends Serializable {

  private val _customResources = new ConcurrentHashMap[String, TaskResourceRequest]()

  @volatile private var _cpus: Int = -1

  /**
   * Returns a Map of the custom resources (GPU, FPGA, etc) set by the user
   */
  def resources: Map[String, TaskResourceRequest] = _customResources.asScala.toMap

  /**
   * Returns the number of cpus specified by the user or -1 if not set
   */
  def cpus: Int = _cpus

  /**
   * Specify number of cpus per Task.
   *
   * @param amount Number of cpus to allocate per Task.
   */
  def cpus(amount: Int): this.type = {
    _cpus = amount
    this
  }

  /**
   *  Amount of a particular custom resource(GPU, FPGA, etc) to use. The resource names supported
   *  correspond to the regular Spark configs with the prefix removed. For instance, resources
   *  like GPUs are resource.gpu (spark configs spark.task.resource.gpu.*)
   *
   * @param resourceName Name of the resource.
   * @param amount Amount requesting as a Double to support fractional resource requests.
   *               Valid values are less than or equal to 0.5 or whole numbers. This essentially
   *               lets you configure X number of tasks to run on a single resource,
   *               ie amount equals 0.5 translates into 2 tasks per resource address.
   */
  def resource(rName: String, amount: Double): this.type = {
    val t = new TaskResourceRequest(rName, amount)
    _customResources(rName) = t
    this
  }

  override def toString: String = {
    s"Task cpus: ${_cpus}, resource requests: ${_customResources}"
  }

  override def clone(): TaskResourceRequests = {
    val newReq = new TaskResourceRequests()
      .cpus(_cpus)
    resources.foreach { case (name, res) =>
      newReq.resource(res.resourceName, res.amount)
    }
    newReq
  }
}
