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

import scala.collection.mutable

import org.apache.spark.resource.ResourceProfile._
import org.apache.spark.util.Utils

/**
 * A set of Executor resource requests. This is used in conjunction with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 *
 * This api is currently private until the rest of the pieces are in place and then it
 * will become public.
 */
private[spark] class ExecutorResourceRequests() extends Serializable {

  private val _executorResources = new mutable.HashMap[String, ExecutorResourceRequest]()

  def requests: Map[String, ExecutorResourceRequest] = _executorResources.toMap

  /**
   * Specify heap memory.
   *
   * @param amount Amount of memory.
   * @param units Units of the amount. For things like Memory, default is no units,
   *              only byte types (b, mb, gb, etc) are currently supported.
   */
  def memory(amount: Long, units: String): this.type = {
    val rr = new ExecutorResourceRequest(MEMORY, amount, units)
    _executorResources(MEMORY) = rr
    this
  }

  /**
   * Specify overhead memory.
   *
   * @param amount Amount of memory.
   * @param units Units of the amount. For things like Memory, default is no units,
   *              only byte types (b, mb, gb, etc) are currently supported.
   */
  def memoryOverhead(amount: Long, units: String): this.type = {
    val rr = new ExecutorResourceRequest(OVERHEAD_MEM, amount, units)
    _executorResources(OVERHEAD_MEM) = rr
    this
  }

  /**
   * Specify pyspark memory.
   *
   * @param amount Amount of memory.
   * @param units Units of the amount. For things like Memory, default is no units,
   *              only byte types (b, mb, gb, etc) are currently supported.
   */
  def pysparkMemory(amount: Long, units: String): this.type = {
    val rr = new ExecutorResourceRequest(PYSPARK_MEM, amount, units)
    _executorResources(PYSPARK_MEM) = rr
    this
  }

  private def memoryAsMib(amount: Long, units: String): Long = {
    Utils.byteStringAsMb(amount + units)
  }

  /**
   * Specify number of cores per Executor.
   *
   * @param amount Number of cores to allocate per Executor.
   */
  def cores(amount: Int): this.type = {
    val t = new ExecutorResourceRequest(CORES, amount)
    _executorResources(CORES) = t
    this
  }

  /**
   *  Amount of a particular custom resource(GPU, FPGA, etc) to use. The resource names supported
   *  correspond to the regular Spark configs with the prefix removed. For instance, resources
   *  like GPUs are resource.gpu (spark configs spark.executor.resource.gpu.*)
   *
   * @param resourceName Name of the resource.
   * @param amount amount of that resource per executor to use.
   * @param discoveryScript Optional script used to discover the resources. This is required on
   *                        some cluster managers that don't tell Spark the addresses of
   *                        the resources allocated. The script runs on Executors startup to
   *                        of the resources available.
   * @param vendor Optional vendor, required for some cluster managers
   */
  def resource(
      resourceName: String,
      amount: Long,
      discoveryScript: String = "",
      vendor: String = ""): this.type = {
    // a bit weird but for Java api use empty string as meaning None because empty
    // string is otherwise invalid for those paramters anyway
    val eReq = new ExecutorResourceRequest(resourceName, amount, "", discoveryScript, vendor)
    _executorResources(resourceName) = eReq
    this
  }

  def addRequest(ereq: ExecutorResourceRequest): this.type = {
    _executorResources(ereq.resourceName) = ereq
    this
  }

  override def toString: String = {
    s"Executor resource requests: ${_executorResources}"
  }
}
