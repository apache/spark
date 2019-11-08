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

import org.apache.spark.util.Utils
import org.apache.spark.internal.config._

/**
 * An Executor resource request. This is used in conjunction with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 *
 * This is used to specify what the resource requirements are for an Executor and how
 * Spark can find out specific details about those resources. Not all the parameters are
 * required for every resource type. The resources names supported
 * correspond to the regular Spark configs with the prefix removed. For instance overhead
 * memory in this api is memoryOverhead, which is spark.executor.memoryOverhead with
 * spark.executor removed. Resources like GPUs are resource.gpu
 * (spark configs spark.executor.resource.gpu.*). The amount, discoveryScript, and vendor
 * parameters for resources are all the same parameters a user would specify through the
 * configs: spark.executor.resource.{resourceName}.{amount, discoveryScript, vendor}.
 *
 * For instance, a user wants to allocate an Executor with GPU resources on YARN. The user has
 * to specify the resource name (resource.gpu), the amount or number of GPUs per Executor,
 * units would not be used as its not a memory config, the discovery script would be specified
 * so that when the Executor starts up it can discovery what GPU addresses are available for it to
 * use because YARN doesn't tell Spark that, then vendor would not be used because
 * its specific for Kubernetes.
 *
 * See the configuration and cluster specific docs for more details.
 *
 * There are alternative constructors for working with Java.
 *
 * @param resourceName Name of the resource
 * @param amount Amount requesting
 *
 * This api is currently private until the rest of the pieces are in place and then it
 * will become public.
 */
private[spark] class ExecutorResourceRequests() extends Serializable {

  private val CORES = "cores"
  private val MEMORY = "memory"
  private val OVERHEAD_MEM = "memoryOverhead"
  private val PYSPARK_MEM = "pyspark.memory"

  private val _executorResources = new mutable.HashMap[String, ResourceRequest]()

  /**
   * Specify heap memory.
   *
   * @param amount Amount of memory.
   * @param units Optional units of the amount. For things like Memory, default is no units,
   *              only byte types (b, mb, gb, etc) are currently supported.
   *
   */
  def memory(amount: Long, units: String): Unit = {
    val amountAsBytes = Utils.byteStringAsMb(amount + units)
    val rr =
      new ResourceRequest(ResourceID(SPARK_EXECUTOR_PREFIX, MEMORY), amountAsBytes, None, None)
    _executorResources(MEMORY) = rr
  }

  def memoryOverhead(amount: Int, units: String): Unit = {
    rName = "memoryOverhead"
    rAmount = amount
    rUnits = units
  }

  def pysparkMemory(amount: Int, units: String): Unit = {
    rName = "pyspark.memory"
    rAmount = amount
    rUnits = units
  }

  def cores(amount: Int): Unit = {
    val t = new ExecutorResourceRequest(CORES, amount)
    _executorResources(CORES) = t
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
      amount: Int,
      discoveryScript: String,
      vendor: String): Unit = {
    rName = resourceName
    rAmount = amount
    rDiscoveryScript = discoveryScript
    rVendor = vendor
  }

}
