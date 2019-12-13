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

import org.apache.spark.network.util.JavaUtils


private[spark] class ExecutorResourceRequestsBuilder() {
  private val _customResources = new ConcurrentHashMap[String, ExecutorResourceRequest]()

  @volatile private var _cores: Int = -1
  @volatile private var _memory: Long = -1
  @volatile private var _memoryOverhead: Long = -1
  @volatile private var _pysparkMemory: Long = -1

  /**
   * Returns the number of cores specified by the user or -1 if not set
   */
  def cores: Int = _cores

  /**
   * Returns the heap memory specified by the user in MiB or -1 if not set
   */
  def memory: Long = _memory

  /**
   * Returns the amount of overhead memory specified by the user in MiB or -1 if not set
   */
  def overheadMemory: Long = _memoryOverhead

  /**
   * Returns the amount of pyspark memory specified by the user in MiB or -1 if not set
   */
  def pysparkMemory: Long = _pysparkMemory

  /**
   * Returns a Map of the custom resources (GPU, FPGA, etc) set by the user
   */
  def resources: Map[String, ExecutorResourceRequest] = _customResources.asScala.toMap

  /**
   * (Java-specific) gets a Java Map of custom resources (GPU, FPGA, etc)
   */
  def resourcesJMap: JMap[String, ExecutorResourceRequest] = {
    _customResources.asScala.toMap.asJava
  }

  /**
   * Specify heap memory. The value specified will be converted to MiB.
   *
   * @param amount Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
   *               Default unit is MiB if not specified.
   */
  def memory(amount: String): this.type = {
    val amountMiB = JavaUtils.byteStringAsMb(amount)
    _memory = amountMiB
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
    _memoryOverhead = amountMiB
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
    _pysparkMemory = amountMiB
    this
  }

  /**
   * Specify number of cores per Executor.
   *
   * @param amount Number of cores to allocate per Executor.
   */
  def cores(amount: Int): this.type = {
    _cores = amount
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
    val eReq = new ExecutorResourceRequest(resourceName, amount, discoveryScript, vendor)
    _customResources(resourceName) = eReq
    this
  }

  override def toString: String = {
    s"Executor cores: ${_cores}, memory: ${_memory}, overhead memory: " +
      s"${_memoryOverhead}, pyspark memory: ${_pysparkMemory}, " +
      s"custom resource requests: ${_customResources}"
  }

  def build(): ExecutorResourceRequests = {
    new ExecutorResourceRequests(_cores, _memory, _memoryOverhead,
      _pysparkMemory, _customResources.asScala.toMap)
  }
}

/**
 * A set of Executor resource requests. This is used in conjunction with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 *
 * This api is currently private until the rest of the pieces are in place and then it
 * will become public.
 */
private[spark] class ExecutorResourceRequests(
    val cores: Int,
    val memory: Long,
    val overheadMemory: Long,
    val pysparkMemory: Long,
    val resources: Map[String, ExecutorResourceRequest]) extends Serializable
