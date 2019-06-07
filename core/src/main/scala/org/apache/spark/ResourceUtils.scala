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

package org.apache.spark

import java.io.{BufferedInputStream, File, FileInputStream}

import com.fasterxml.jackson.databind.exc.MismatchedInputException
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Resource identifier.
 * @param componentName spark.driver / spark.executor / spark.task
 * @param resourceName  gpu, fpga, etc
 */
private[spark] case class ResourceID(componentName: String, resourceName: String) {
  def confPrefix: String = s"$componentName.resource.$resourceName." // with ending dot
}

private[spark] case class ResourceRequest(
    id: ResourceID,
    count: Int,
    discoveryScript: Option[String],
    vendor: Option[String])

private[spark] case class TaskResourceRequirement(resourceName: String, count: Int)

private[spark] case class ResourceAllocation(id: ResourceID, addresses: Seq[String]) {
  def toResourceInfo: ResourceInformation = {
    new ResourceInformation(id.resourceName, addresses.toArray)
  }

  def toJson: JObject = {
    ("id" ->
      ("componentName" -> id.componentName) ~
      ("resourceName" -> id.resourceName)) ~
    ("addresses" -> addresses)
  }
}

private[spark] object ResourceUtils extends Logging {

  // config suffixes
  val DISCOVERY_SCRIPT = "discoveryScript"
  val VENDOR = "vendor"
  // user facing configs use .amount to allow to extend in the future,
  // internally we currnetly only support addresses, so its just an integer count
  val AMOUNT = "amount"

  // case class to make extracting the JSON resource information easy
  case class JsonResourceInformation(name: String, addresses: Seq[String])

  def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest = {
    val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
    val amount = settings.get(AMOUNT).getOrElse(
      throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}")).toInt
    val discoveryScript = settings.get(DISCOVERY_SCRIPT)
    val vendor = settings.get(VENDOR)
    ResourceRequest(resourceId, amount, discoveryScript, vendor)
  }

  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.resource.").map { case (key, _) =>
      key.substring(0, key.indexOf('.'))
    }.toSet.toSeq.map(name => ResourceID(componentName, name))
  }

  def parseAllResourceRequests(
      sparkConf: SparkConf,
      componentName: String): Seq[ResourceRequest] = {
    listResourceIds(sparkConf, componentName).map { id =>
      parseResourceRequest(sparkConf, id)
    }
  }

  def parseTaskResourceRequirements(sparkConf: SparkConf): Seq[TaskResourceRequirement] = {
    listResourceIds(sparkConf, SPARK_TASK_PREFIX).map { id =>
      val settings = sparkConf.getAllWithPrefix(id.confPrefix).toMap
      val amount = settings.get(AMOUNT).getOrElse(
        throw new SparkException(s"You must specify an amount for ${id.resourceName}")).toInt
      TaskResourceRequirement(id.resourceName, amount)
    }
  }

  def hasTaskResourceRequirements(sparkConf: SparkConf): Boolean = {
    sparkConf.getAllWithPrefix(SPARK_TASK_PREFIX).nonEmpty
  }

  def parseAllocatedFromJsonFile(resourcesFile: String): Seq[ResourceAllocation] = {
    implicit val formats = DefaultFormats
    val resourceInput = new BufferedInputStream(new FileInputStream(resourcesFile))
    try {
      parse(resourceInput).extract[Seq[ResourceAllocation]]
    } catch {
      case e@(_: MappingException | _: MismatchedInputException | _: ClassCastException) =>
        throw new SparkException(s"Exception parsing the resources in $resourcesFile", e)
    } finally {
      resourceInput.close()
    }
  }

  def parseResourceInformationFromJson(resourcesJson: String): JsonResourceInformation = {
    implicit val formats = DefaultFormats
    try {
      parse(resourcesJson).extract[JsonResourceInformation]
    } catch {
      case e@(_: MappingException | _: MismatchedInputException | _: ClassCastException) =>
        throw new SparkException(s"Exception parsing the resources in $resourcesJson", e)
    }
  }

  def parseAllocatedAndDiscoverResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Seq[ResourceAllocation] = {
    val allocated = resourcesFileOpt.map(parseAllocatedFromJsonFile(_))
      .getOrElse(Seq.empty[ResourceAllocation])
      .filter(_.id.componentName == componentName)
    val otherResourceIds = listResourceIds(sparkConf, componentName).diff(allocated.map(_.id))
    allocated ++ otherResourceIds.map { id =>
      val request = parseResourceRequest(sparkConf, id)
      ResourceAllocation(id, discoverResource(request).addresses)
    }
  }

  def assertResourceAllocationMeetsRequest(
      allocation: ResourceAllocation,
      request: ResourceRequest): Unit = {
    require(allocation.id == request.id && allocation.addresses.size >= request.count,
      s"Resource: ${allocation.id.resourceName}, with addresses: " +
      s"${allocation.addresses.mkString(",")} " +
      s"is less than what the user requested: ${request.count})")
  }

  def assertAllResourceAllocationsMeetRequests(
      allocations: Seq[ResourceAllocation],
      requests: Seq[ResourceRequest]): Unit = {
    val allocated = allocations.map(x => x.id -> x).toMap
    requests.foreach(r => assertResourceAllocationMeetsRequest(allocated(r.id), r))
  }

  def getAllResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    val requests = parseAllResourceRequests(sparkConf, componentName)
    val allocations = parseAllocatedAndDiscoverResources(sparkConf, componentName, resourcesFileOpt)
    assertAllResourceAllocationsMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInfo)).toMap
    logInfo("==============================================================")
    logInfo("Resources:")
    resourceInfoMap.foreach { case (k, v) => logInfo(s"$k -> $v") }
    logInfo("==============================================================")
    resourceInfoMap
  }

  def discoverResource(resourceRequest: ResourceRequest): JsonResourceInformation = {
    val resourceName = resourceRequest.id.resourceName
    val script = resourceRequest.discoveryScript
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        val output = executeAndGetOutput(Seq(script.get), new File("."))
        parseResourceInformationFromJson(output)
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
          "doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use resource: $resourceName but " +
        "didn't specify a discovery script!")
    }
    if (!result.name.equals(resourceName)) {
      throw new SparkException("Error running the resource discovery script, script returned " +
        s"resource name: ${result.name} and we were expecting $resourceName")
    }
    result
  }

  def resourceAmountConfigName(id: ResourceID): String = s"${id.confPrefix}$AMOUNT"

  def resourceDiscoveryScriptConfigName(id: ResourceID): String = {
    s"${id.confPrefix}$DISCOVERY_SCRIPT"
  }

  def resourceVendorConfigName(id: ResourceID): String = s"${id.confPrefix}$VENDOR"

  def setResourceAmountConf(conf: SparkConf, id: ResourceID, value: String) {
    conf.set(resourceAmountConfigName(id), value)
  }

  def setResourceDiscoveryScriptConf(conf: SparkConf, id: ResourceID, value: String) {
    conf.set(resourceDiscoveryScriptConfigName(id), value)
  }

  def setResourceVendorConf(conf: SparkConf, id: ResourceID, value: String) {
    conf.set(resourceVendorConfigName(id), value)
  }

  def setDriverResourceAmountConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_DRIVER_PREFIX, resourceName)
    setResourceAmountConf(conf, resourceId, value)
  }

  def setDriverResourceDiscoveryConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_DRIVER_PREFIX, resourceName)
    setResourceDiscoveryScriptConf(conf, resourceId, value)
  }

  def setDriverResourceVendorConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_DRIVER_PREFIX, resourceName)
    setResourceVendorConf(conf, resourceId, value)
  }

  def setExecutorResourceAmountConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_EXECUTOR_PREFIX, resourceName)
    setResourceAmountConf(conf, resourceId, value)
  }

  def setExecutorResourceDiscoveryConf(
      conf: SparkConf,
      resourceName: String,
      value: String): Unit = {
    val resourceId = ResourceID(SPARK_EXECUTOR_PREFIX, resourceName)
    setResourceDiscoveryScriptConf(conf, resourceId, value)
  }

  def setExecutorResourceVendorConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_EXECUTOR_PREFIX, resourceName)
    setResourceVendorConf(conf, resourceId, value)
  }

  def setTaskResourceAmountConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_TASK_PREFIX, resourceName)
    setResourceAmountConf(conf, resourceId, value)
  }

  // known types of resources
  final val GPU: String = "gpu"
  final val FPGA: String = "fpga"
}
