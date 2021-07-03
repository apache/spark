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
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.resource.ResourceProfile._

/**
 * A set of Executor resource requests. This is used in conjunction with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 */
@Evolving
@Since("3.1.0")
class ExecutorResourceRequests() extends Serializable {

  private val _executorResources = new ConcurrentHashMap[String, ExecutorResourceRequest]()

  /**
   * Returns all the resource requests for the task.
   */
  def requests: Map[String, ExecutorResourceRequest] = _executorResources.asScala.toMap

  /**
   * (Java-specific) Returns all the resource requests for the executor.
   */
  def requestsJMap: JMap[String, ExecutorResourceRequest] = requests.asJava

  /**
   * Specify heap memory. The value specified will be converted to MiB.
   * This is a convenient API to add [[ExecutorResourceRequest]] for "memory" resource.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def memory(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    val req = new ExecutorResourceRequest(MEMORY, amountMiB)
    _executorResources.put(MEMORY, req)
    this
  }

  /**
   * Specify off heap memory. The value specified will be converted to MiB.
   * This value only take effect when MEMORY_OFFHEAP_ENABLED is true.
   * This is a convenient API to add [[ExecutorResourceRequest]] for "offHeap" resource.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def offHeapMemory(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    val req = new ExecutorResourceRequest(OFFHEAP_MEM, amountMiB)
    _executorResources.put(OFFHEAP_MEM, req)
    this
  }

  /**
   * Specify overhead memory. The value specified will be converted to MiB.
   * This is a convenient API to add [[ExecutorResourceRequest]] for "memoryOverhead" resource.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def memoryOverhead(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    val req = new ExecutorResourceRequest(OVERHEAD_MEM, amountMiB)
    _executorResources.put(OVERHEAD_MEM, req)
    this
  }

  /**
   * Specify pyspark memory. The value specified will be converted to MiB.
   * This is a convenient API to add [[ExecutorResourceRequest]] for "pyspark.memory" resource.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def pysparkMemory(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    val req = new ExecutorResourceRequest(PYSPARK_MEM, amountMiB)
    _executorResources.put(PYSPARK_MEM, req)
    this
  }

  /**
   * Specify number of cores per Executor.
   * This is a convenient API to add [[ExecutorResourceRequest]] for "cores" resource.
   *
   * @param amount Number of cores to allocate per Executor.
   */
  def cores(amount: Int): this.type = {
    val req = new ExecutorResourceRequest(CORES, amount)
    _executorResources.put(CORES, req)
    this
  }

  /**
   *  Amount of a particular custom resource(GPU, FPGA, etc) to use. The resource names supported
   *  correspond to the regular Spark configs with the prefix removed. For instance, resources
   *  like GPUs are gpu (spark configs spark.executor.resource.gpu.*). If you pass in a resource
   *  that the cluster manager doesn't support the result is undefined, it may error or may just
   *  be ignored.
   *  This is a convenient API to add [[ExecutorResourceRequest]] for custom resources.
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
    // string is otherwise invalid for those parameters anyway
    val req = new ExecutorResourceRequest(resourceName, amount, discoveryScript, vendor)
    _executorResources.put(resourceName, req)
    this
  }

  override def toString: String = {
    s"Executor resource requests: ${_executorResources}"
  }
}
