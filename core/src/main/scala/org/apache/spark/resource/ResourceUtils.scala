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

import java.nio.file.{Files, Paths}
import java.util.Optional

import scala.util.control.NonFatal

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EXECUTOR_CORES, RESOURCES_DISCOVERY_PLUGIN, SPARK_TASK_PREFIX}
import org.apache.spark.internal.config.Tests.{RESOURCES_WARNING_TESTING}
import org.apache.spark.util.Utils

/**
 * Resource identifier.
 * @param componentName spark.driver / spark.executor / spark.task
 * @param resourceName  gpu, fpga, etc
 *
 * @since 3.0.0
 */
@DeveloperApi
class ResourceID(val componentName: String, val resourceName: String) {
  private[spark] def confPrefix: String = {
    s"$componentName.${ResourceUtils.RESOURCE_PREFIX}.$resourceName."
  }
  private[spark] def amountConf: String = s"$confPrefix${ResourceUtils.AMOUNT}"
  private[spark] def discoveryScriptConf: String = s"$confPrefix${ResourceUtils.DISCOVERY_SCRIPT}"
  private[spark] def vendorConf: String = s"$confPrefix${ResourceUtils.VENDOR}"

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceID =>
        that.getClass == this.getClass &&
          that.componentName == componentName && that.resourceName == resourceName
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(componentName, resourceName).hashCode()
}

/**
 * Class that represents a resource request.
 *
 * The class used when discovering resources (using the discovery script),
 * or via the context as it is parsing configuration for the ResourceID.
 *
 * @param id object identifying the resource
 * @param amount integer amount for the resource. Note that for a request (executor level),
 *               fractional resources does not make sense, so amount is an integer.
 * @param discoveryScript optional discovery script file name
 * @param vendor optional vendor name
 *
 * @since 3.0.0
 */
@DeveloperApi
class ResourceRequest(
    val id: ResourceID,
    val amount: Long,
    val discoveryScript: Optional[String],
    val vendor: Optional[String]) {

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceRequest =>
        that.getClass == this.getClass &&
          that.id == id && that.amount == amount && discoveryScript == discoveryScript &&
          vendor == vendor
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(id, amount, discoveryScript, vendor).hashCode()
}

/**
 * Case class that represents resource requirements for a component in a
 * an application (components are driver, executor or task).
 *
 * A configuration of spark.task.resource.[resourceName].amount = 4, equates to:
 * amount = 4, and numParts = 1.
 *
 * A configuration of spark.task.resource.[resourceName].amount = 0.25, equates to:
 * amount = 1, and numParts = 4.
 *
 * @param resourceName gpu, fpga, etc.
 * @param amount whole units of the resource we expect (e.g. 1 gpus, 2 fpgas)
 * @param numParts if not 1, the number of ways a whole resource is subdivided.
 *                 This is always an integer greater than or equal to 1,
 *                 where 1 is whole resource, 2 is divide a resource in two, and so on.
 */
private[spark] case class ResourceRequirement(
    resourceName: String,
    amount: Int,
    numParts: Int = 1)

/**
 * Case class representing allocated resource addresses for a specific resource.
 * Cluster manager uses the JSON serialization of this case class to pass allocated resource info to
 * driver and executors. See the ``--resourcesFile`` option there.
 */
private[spark] case class ResourceAllocation(id: ResourceID, addresses: Seq[String]) {
  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(id.resourceName, addresses.toArray)
  }
}

private[spark] object ResourceUtils extends Logging {
  // config suffixes
  val DISCOVERY_SCRIPT = "discoveryScript"
  val VENDOR = "vendor"
  // user facing configs use .amount to allow to extend in the future,
  // internally we currently only support addresses, so its just an integer count
  val AMOUNT = "amount"

  def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest = {
    val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
    val amount = settings.getOrElse(AMOUNT,
      throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}")
    ).toInt
    val discoveryScript = Optional.ofNullable(settings.get(DISCOVERY_SCRIPT).orNull)
    val vendor = Optional.ofNullable(settings.get(VENDOR).orNull)
    new ResourceRequest(resourceId, amount, discoveryScript, vendor)
  }

  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.$RESOURCE_PREFIX.").map { case (key, _) =>
      val index = key.indexOf('.')
      if (index < 0) {
        throw new SparkException(s"You must specify an amount config for resource: $key " +
          s"config: $componentName.$RESOURCE_PREFIX.$key")
      }
      key.substring(0, index)
    }.distinct.map(name => new ResourceID(componentName, name))
  }

  def parseAllResourceRequests(
      sparkConf: SparkConf,
      componentName: String): Seq[ResourceRequest] = {
    listResourceIds(sparkConf, componentName)
      .map(id => parseResourceRequest(sparkConf, id))
      .filter(_.amount > 0)
  }

  // Used to take a fraction amount from a task resource requirement and split into a real
  // integer amount and the number of slots per address. For instance, if the amount is 0.5,
  // the we get (1, 2) back out. This indicates that for each 1 address, it has 2 slots per
  // address, which allows you to put 2 tasks on that address. Note if amount is greater
  // than 1, then the number of slots per address has to be 1. This would indicate that a
  // would have multiple addresses assigned per task. This can be used for calculating
  // the number of tasks per executor -> (executorAmount * numParts) / (integer amount).
  // Returns tuple of (integer amount, numParts)
  def calculateAmountAndPartsForFraction(doubleAmount: Double): (Int, Int) = {
    val parts = if (doubleAmount <= 0.5) {
      Math.floor(1.0 / doubleAmount).toInt
    } else if (doubleAmount % 1 != 0) {
      throw new SparkException(
        s"The resource amount ${doubleAmount} must be either <= 0.5, or a whole number.")
    } else {
      1
    }
    (Math.ceil(doubleAmount).toInt, parts)
  }

  // Add any task resource requests from the spark conf to the TaskResourceRequests passed in
  def addTaskResourceRequests(
      sparkConf: SparkConf,
      treqs: TaskResourceRequests): Unit = {
    listResourceIds(sparkConf, SPARK_TASK_PREFIX).map { resourceId =>
      val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
      val amountDouble = settings.getOrElse(AMOUNT,
        throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}")
      ).toDouble
      treqs.resource(resourceId.resourceName, amountDouble)
    }
  }

  def parseResourceRequirements(sparkConf: SparkConf, componentName: String)
    : Seq[ResourceRequirement] = {
    val resourceIds = listResourceIds(sparkConf, componentName)
    val rnamesAndAmounts = resourceIds.map { resourceId =>
      val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
      val amountDouble = settings.getOrElse(AMOUNT,
        throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}")
      ).toDouble
      (resourceId.resourceName, amountDouble)
    }
    rnamesAndAmounts.filter { case (_, amount) => amount > 0 }.map { case (rName, amountDouble) =>
      val (amount, parts) = if (componentName.equalsIgnoreCase(SPARK_TASK_PREFIX)) {
        calculateAmountAndPartsForFraction(amountDouble)
      } else if (amountDouble % 1 != 0) {
        throw new SparkException(
          s"Only tasks support fractional resources, please check your $componentName settings")
      } else {
        (amountDouble.toInt, 1)
      }
      ResourceRequirement(rName, amount, parts)
    }
  }

  def resourcesMeetRequirements(
      resourcesFree: Map[String, Int],
      resourceRequirements: Seq[ResourceRequirement])
    : Boolean = {
    resourceRequirements.forall { req =>
      resourcesFree.getOrElse(req.resourceName, 0) >= req.amount
    }
  }

  def withResourcesJson[T](resourcesFile: String)(extract: String => Seq[T]): Seq[T] = {
    val json = new String(Files.readAllBytes(Paths.get(resourcesFile)))
    try {
      extract(json)
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing resources file $resourcesFile", e)
    }
  }

  def parseAllocatedFromJsonFile(resourcesFile: String): Seq[ResourceAllocation] = {
    withResourcesJson[ResourceAllocation](resourcesFile) { json =>
      implicit val formats = DefaultFormats
      parse(json).extract[Seq[ResourceAllocation]]
    }
  }

  def parseAllocated(
      resourcesFileOpt: Option[String],
      componentName: String): Seq[ResourceAllocation] = {
    resourcesFileOpt.toSeq.flatMap(parseAllocatedFromJsonFile)
      .filter(_.id.componentName == componentName)
  }

  private def parseAllocatedOrDiscoverResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Seq[ResourceAllocation] = {
    val allocated = parseAllocated(resourcesFileOpt, componentName)
    val otherResourceIds = listResourceIds(sparkConf, componentName).diff(allocated.map(_.id))
    val otherResources = otherResourceIds.flatMap { id =>
      val request = parseResourceRequest(sparkConf, id)
      if (request.amount > 0) {
        Some(ResourceAllocation(id, discoverResource(sparkConf, request).addresses))
      } else {
        None
      }
    }
    allocated ++ otherResources
  }

  private def assertResourceAllocationMeetsRequest(
      allocation: ResourceAllocation,
      request: ResourceRequest): Unit = {
    require(allocation.id == request.id && allocation.addresses.size >= request.amount,
      s"Resource: ${allocation.id.resourceName}, with addresses: " +
      s"${allocation.addresses.mkString(",")} " +
      s"is less than what the user requested: ${request.amount})")
  }

  private def assertAllResourceAllocationsMeetRequests(
      allocations: Seq[ResourceAllocation],
      requests: Seq[ResourceRequest]): Unit = {
    val allocated = allocations.map(x => x.id -> x).toMap
    requests.foreach(r => assertResourceAllocationMeetsRequest(allocated(r.id), r))
  }

  private def assertAllResourceAllocationsMatchResourceProfile(
      allocations: Map[String, ResourceInformation],
      execReqs: Map[String, ExecutorResourceRequest]): Unit = {
    execReqs.foreach { case (rName, req) =>
      require(allocations.contains(rName) && allocations(rName).addresses.size >= req.amount,
        s"Resource: ${rName}, with addresses: " +
          s"${allocations(rName).addresses.mkString(",")} " +
          s"is less than what the user requested: ${req.amount})")
    }
  }

  /**
   * Gets all allocated resource information for the input component from input resources file and
   * the application level Spark configs. It first looks to see if resource were explicitly
   * specified in the resources file (this would include specified address assignments and it only
   * specified in certain cluster managers) and then it looks at the Spark configs to get any
   * others not specified in the file. The resources not explicitly set in the file require a
   * discovery script for it to run to get the addresses of the resource.
   * It also verifies the resource allocation meets required amount for each resource.
   * @return a map from resource name to resource info
   */
  def getOrDiscoverAllResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    val requests = parseAllResourceRequests(sparkConf, componentName)
    val allocations = parseAllocatedOrDiscoverResources(sparkConf, componentName, resourcesFileOpt)
    assertAllResourceAllocationsMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
    resourceInfoMap
  }

  // create an empty Optional if the string is empty
  private def emptyStringToOptional(optStr: String): Optional[String] = {
    if (optStr.isEmpty) {
      Optional.empty[String]
    } else {
      Optional.of(optStr)
    }
  }

  /**
   * This function is similar to getOrDiscoverallResources, except for it uses the ResourceProfile
   * information instead of the application level configs.
   *
   * It first looks to see if resource were explicitly specified in the resources file
   * (this would include specified address assignments and it only specified in certain
   * cluster managers) and then it looks at the ResourceProfile to get
   * any others not specified in the file. The resources not explicitly set in the file require a
   * discovery script for it to run to get the addresses of the resource.
   * It also verifies the resource allocation meets required amount for each resource.
   *
   * @return a map from resource name to resource info
   */
  def getOrDiscoverAllResourcesForResourceProfile(
      resourcesFileOpt: Option[String],
      componentName: String,
      resourceProfile: ResourceProfile,
      sparkConf: SparkConf): Map[String, ResourceInformation] = {
    val fileAllocated = parseAllocated(resourcesFileOpt, componentName)
    val fileAllocResMap = fileAllocated.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
    // only want to look at the ResourceProfile for resources not in the resources file
    val execReq = ResourceProfile.getCustomExecutorResources(resourceProfile)
    val filteredExecreq = execReq.filterNot { case (rname, _) => fileAllocResMap.contains(rname) }
    val rpAllocations = filteredExecreq.map { case (rName, execRequest) =>
      val resourceId = new ResourceID(componentName, rName)
      val scriptOpt = emptyStringToOptional(execRequest.discoveryScript)
      val vendorOpt = emptyStringToOptional(execRequest.vendor)
      val resourceReq = new ResourceRequest(resourceId, execRequest.amount, scriptOpt, vendorOpt)
      val addrs = discoverResource(sparkConf, resourceReq).addresses
      (rName, new ResourceInformation(rName, addrs))
    }
    val allAllocations = fileAllocResMap ++ rpAllocations
    assertAllResourceAllocationsMatchResourceProfile(allAllocations, execReq)
    allAllocations
  }

  def logResourceInfo(componentName: String, resources: Map[String, ResourceInformation])
    : Unit = {
    val resourceInfo = if (resources.isEmpty) {
      s"No custom resources configured for $componentName."
    } else {
      s"Custom resources for $componentName:\n${resources.mkString("\n")}"
    }
    logInfo("==============================================================")
    logInfo(resourceInfo)
    logInfo("==============================================================")
  }

  private[spark] def discoverResource(
      sparkConf: SparkConf,
      resourceRequest: ResourceRequest): ResourceInformation = {
    // always put the discovery script plugin as last plugin
    val discoveryScriptPlugin = "org.apache.spark.resource.ResourceDiscoveryScriptPlugin"
    val pluginClasses = sparkConf.get(RESOURCES_DISCOVERY_PLUGIN) :+ discoveryScriptPlugin
    val resourcePlugins = Utils.loadExtensions(classOf[ResourceDiscoveryPlugin], pluginClasses,
      sparkConf)
    // apply each plugin until one of them returns the information for this resource
    var riOption: Optional[ResourceInformation] = Optional.empty()
    resourcePlugins.foreach { plugin =>
      val riOption = plugin.discoverResource(resourceRequest, sparkConf)
      if (riOption.isPresent()) {
        return riOption.get()
      }
    }
    throw new SparkException(s"None of the discovery plugins returned ResourceInformation for " +
      s"${resourceRequest.id.resourceName}")
  }

  def validateTaskCpusLargeEnough(sparkConf: SparkConf, execCores: Int, taskCpus: Int): Boolean = {
    // Number of cores per executor must meet at least one task requirement.
    if (execCores < taskCpus) {
      throw new SparkException(s"The number of cores per executor (=$execCores) has to be >= " +
        s"the number of cpus per task = $taskCpus.")
    }
    true
  }

  // the option executor cores parameter is by the different local modes since it not configured
  // via the config
  def warnOnWastedResources(
      rp: ResourceProfile,
      sparkConf: SparkConf,
      execCores: Option[Int] = None): Unit = {
    // There have been checks on the ResourceProfile to make sure the executor resources were
    // specified and are large enough if any task resources were specified.
    // Now just do some sanity test and log warnings when it looks like the user will
    // waste some resources.
    val coresKnown = rp.isCoresLimitKnown
    var limitingResource = rp.limitingResource(sparkConf)
    var maxTaskPerExec = rp.maxTasksPerExecutor(sparkConf)
    val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(rp, sparkConf)
    val cores = if (execCores.isDefined) {
      execCores.get
    } else if (coresKnown) {
      rp.getExecutorCores.getOrElse(sparkConf.get(EXECUTOR_CORES))
    } else {
      // can't calculate cores limit
      return
    }
    // when executor cores config isn't set, we can't calculate the real limiting resource
    // and number of tasks per executor ahead of time, so calculate it now.
    if (!coresKnown) {
      val numTasksPerExecCores = cores / taskCpus
      val numTasksPerExecCustomResource = rp.maxTasksPerExecutor(sparkConf)
      if (limitingResource.isEmpty ||
        (limitingResource.nonEmpty && numTasksPerExecCores < numTasksPerExecCustomResource)) {
        limitingResource = ResourceProfile.CPUS
        maxTaskPerExec = numTasksPerExecCores
      }
    }
    val taskReq = ResourceProfile.getCustomTaskResources(rp)
    val execReq = ResourceProfile.getCustomExecutorResources(rp)

    if (limitingResource.nonEmpty && !limitingResource.equals(ResourceProfile.CPUS)) {
      if ((taskCpus * maxTaskPerExec) < cores) {
        val resourceNumSlots = Math.floor(cores/taskCpus).toInt
        val message = s"The configuration of cores (exec = ${cores} " +
          s"task = ${taskCpus}, runnable tasks = ${resourceNumSlots}) will " +
          s"result in wasted resources due to resource ${limitingResource} limiting the " +
          s"number of runnable tasks per executor to: ${maxTaskPerExec}. Please adjust " +
          "your configuration."
        if (sparkConf.get(RESOURCES_WARNING_TESTING)) {
          throw new SparkException(message)
        } else {
          logWarning(message)
        }
      }
    }

    taskReq.foreach { case (rName, treq) =>
      val execAmount = execReq(rName).amount
      // handles fractional
      val taskAmount = rp.getSchedulerTaskResourceAmount(rName)
      val numParts = rp.getNumSlotsPerAddress(rName, sparkConf)
      if (maxTaskPerExec < (execAmount * numParts / taskAmount)) {
        val origTaskAmount = treq.amount
        val taskReqStr = s"${origTaskAmount}/${numParts}"
        val resourceNumSlots = Math.floor(execAmount * numParts / taskAmount).toInt
        val message = s"The configuration of resource: ${treq.resourceName} " +
          s"(exec = ${execAmount}, task = ${taskReqStr}, " +
          s"runnable tasks = ${resourceNumSlots}) will " +
          s"result in wasted resources due to resource ${limitingResource} limiting the " +
          s"number of runnable tasks per executor to: ${maxTaskPerExec}. Please adjust " +
          "your configuration."
        if (sparkConf.get(RESOURCES_WARNING_TESTING)) {
          throw new SparkException(message)
        } else {
          logWarning(message)
        }
      }
    }
  }

  // known types of resources
  final val GPU: String = "gpu"
  final val FPGA: String = "fpga"

  final val RESOURCE_PREFIX: String = "resource"
}
