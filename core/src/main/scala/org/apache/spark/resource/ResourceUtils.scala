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

import java.io.File
import java.nio.file.{Files, Paths}

import scala.util.control.NonFatal

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SPARK_TASK_PREFIX
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Resource identifier.
 * @param componentName spark.driver / spark.executor / spark.task
 * @param resourceName  gpu, fpga, etc
 */
private[spark] case class ResourceID(componentName: String, resourceName: String) {
  def confPrefix: String = s"$componentName.${ResourceUtils.RESOURCE_PREFIX}.$resourceName."
  def amountConf: String = s"$confPrefix${ResourceUtils.AMOUNT}"
  def discoveryScriptConf: String = s"$confPrefix${ResourceUtils.DISCOVERY_SCRIPT}"
  def vendorConf: String = s"$confPrefix${ResourceUtils.VENDOR}"
}

/**
 * Case class that represents a resource request at the executor level.
 *
 * The class used when discovering resources (using the discovery script),
 * or via the context as it is parsing configuration, for SPARK_EXECUTOR_PREFIX.
 *
 * @param id object identifying the resource
 * @param amount integer amount for the resource. Note that for a request (executor level),
 *               fractional resources does not make sense, so amount is an integer.
 * @param discoveryScript optional discovery script file name
 * @param vendor optional vendor name
 */
private[spark] case class ResourceRequest(
    id: ResourceID,
    amount: Int,
    discoveryScript: Option[String],
    vendor: Option[String])

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
    val discoveryScript = settings.get(DISCOVERY_SCRIPT)
    val vendor = settings.get(VENDOR)
    ResourceRequest(resourceId, amount, discoveryScript, vendor)
  }

  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.$RESOURCE_PREFIX.").map { case (key, _) =>
      key.substring(0, key.indexOf('.'))
    }.toSet.toSeq.map(name => ResourceID(componentName, name))
  }

  def parseAllResourceRequests(
      sparkConf: SparkConf,
      componentName: String): Seq[ResourceRequest] = {
    listResourceIds(sparkConf, componentName)
      .map(id => parseResourceRequest(sparkConf, id))
      .filter(_.amount > 0)
  }

  // Used to take a fraction amount from a task resource requirement and split into a real
  // integer amount and the number of parts expected. For instance, if the amount is 0.5,
  // the we get (1, 2) back out.
  // Returns tuple of (amount, numParts)
  def calculateAmountAndPartsForFraction(amount: Double): (Int, Int) = {
    val parts = if (amount <= 0.5) {
      Math.floor(1.0 / amount).toInt
    } else if (amount % 1 != 0) {
      throw new SparkException(
        s"The resource amount ${amount} must be either <= 0.5, or a whole number.")
    } else {
      1
    }
    (Math.ceil(amount).toInt, parts)
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
        Some(ResourceAllocation(id, discoverResource(request).addresses))
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
      resourceProfile: ResourceProfile): Map[String, ResourceInformation] = {
    val fileAllocated = parseAllocated(resourcesFileOpt, componentName)
    val fileAllocResMap = fileAllocated.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
    // only want to look at the ResourceProfile for resources not in the resources file
    val execReq = ResourceProfile.getCustomExecutorResources(resourceProfile)
    val filteredExecreq = execReq.filterNot { case (rname, _) => fileAllocResMap.contains(rname) }
    val rpAllocations = filteredExecreq.map { case (rName, execRequest) =>
      val addrs = discoverResource(rName, Option(execRequest.discoveryScript)).addresses
      (rName, new ResourceInformation(rName, addrs))
    }
    val allAllocations = fileAllocResMap ++ rpAllocations
    assertAllResourceAllocationsMatchResourceProfile(allAllocations, execReq)
    allAllocations
  }

  def logResourceInfo(componentName: String, resources: Map[String, ResourceInformation])
    : Unit = {
    logInfo("==============================================================")
    logInfo(s"Resources for $componentName:\n${resources.mkString("\n")}")
    logInfo("==============================================================")
  }

  // visible for test
  private[spark] def discoverResource(
      resourceName: String,
      script: Option[String]): ResourceInformation = {
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        val output = executeAndGetOutput(Seq(script.get), new File("."))
        ResourceInformation.parseJson(output)
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
          "doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use resource: $resourceName, but " +
        "didn't specify a discovery script!")
    }
    if (!result.name.equals(resourceName)) {
      throw new SparkException(s"Error running the resource discovery script ${script.get}: " +
        s"script returned resource name ${result.name} and we were expecting $resourceName.")
    }
    result
  }

  // visible for test
  private[spark] def discoverResource(resourceRequest: ResourceRequest): ResourceInformation = {
    val resourceName = resourceRequest.id.resourceName
    val script = resourceRequest.discoveryScript
    discoverResource(resourceName, script)
  }

  // known types of resources
  final val GPU: String = "gpu"
  final val FPGA: String = "fpga"

  final val RESOURCE_PREFIX: String = "resource"
}
