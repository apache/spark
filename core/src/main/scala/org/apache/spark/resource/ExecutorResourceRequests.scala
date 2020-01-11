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

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.resource.ResourceProfile._

/**
 * A set of Executor resource requests. This is used in conjunction with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 */
class ExecutorResourceRequests() extends Serializable with Logging {

  private val _executorResources = new ConcurrentHashMap[String, ExecutorResourceRequest]()

  def requests: Map[String, ExecutorResourceRequest] = _executorResources.asScala.toMap

  def requestsJMap: JMap[String, ExecutorResourceRequest] = _executorResources.asScala.asJava

  /**
   * Specify heap memory. The value specified will be converted to MiB.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def memory(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    if (amountMiB <= 0) {
      throw new IllegalArgumentException("Memory size must be > 0")
    }
    val req = new ExecutorResourceRequest(MEMORY, amountMiB)
    _executorResources.put(MEMORY, req)
    this
  }

  /**
   * Removes any heap memory requests.
   */
  def removeMemory(): this.type = {
    _executorResources.remove(MEMORY)
    this
  }

  /**
   * Specify overhead memory. The value specified will be converted to MiB.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def memoryOverhead(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    if (amountMiB <= 0) {
      throw new IllegalArgumentException("Overhead memory size must be > 0")
    }
    val req = new ExecutorResourceRequest(OVERHEAD_MEM, amountMiB)
    _executorResources.put(OVERHEAD_MEM, req)
    this
  }

  /**
   * Removes any overhead memory requests.
   */
  def removeMemoryOverhead(): this.type = {
    _executorResources.remove(OVERHEAD_MEM)
    this
  }

  /**
   * Specify pyspark memory. The value specified will be converted to MiB.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def pysparkMemory(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    if (amountMiB <= 0) {
      throw new IllegalArgumentException("Pyspark memory size must be > 0")
    }
    val req = new ExecutorResourceRequest(PYSPARK_MEM, amountMiB)
    _executorResources.put(PYSPARK_MEM, req)
    this
  }

  /**
   * Removes any pyspark memory requests.
   */
  def removePysparkMemory(): this.type = {
    _executorResources.remove(PYSPARK_MEM)
    this
  }

  /**
   * Specify number of cores per Executor.
   *
   * @param amount Number of cores to allocate per Executor.
   */
  def cores(amount: Int): this.type = {
    if (amount <= 0) {
      throw new IllegalArgumentException("Cores amount must be > 0")
    }
    val req = new ExecutorResourceRequest(CORES, amount)
    _executorResources.put(CORES, req)
    this
  }

  /**
   * Removes any executor core requests.
   */
  def removeCores(): this.type = {
    _executorResources.remove(CORES)
    this
  }

  /**
   *  Amount of a particular custom resource(GPU, FPGA, etc) to use. The resource names supported
   *  correspond to the regular Spark configs with the prefix removed. For instance, resources
   *  like GPUs are gpu (spark configs spark.executor.resource.gpu.*). If you pass in a resource
   *  that the cluster manager doesn't support the result is undefined, it may error or may just
   *  be ignored.
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
    if (amount <= 0) {
      throw new IllegalArgumentException(s"$resourceName amount must be > 0")
    }
    val req = new ExecutorResourceRequest(resourceName, amount, discoveryScript, vendor)
    _executorResources.put(resourceName, req)
    this
  }

  /**
   * Removes the specific custom resource requests.
   *
   * @param resourceName name of the resource to remove
   */
  def removeResource(resourceName: String): this.type = {
    if (resourceName != null) {
      _executorResources.remove(resourceName)
    }
    this
  }

  override def toString: String = {
    s"Executor resource requests: ${_executorResources}"
  }
}
