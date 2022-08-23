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

package org.apache.spark.deploy.yarn

import java.lang.{Long => JLong}
import java.lang.reflect.InvocationTargetException

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.yarn.api.records.Resource

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceID
import org.apache.spark.resource.ResourceUtils.{AMOUNT, FPGA, GPU}
import org.apache.spark.util.{CausedBy, Utils}

/**
 * This helper class uses some of Hadoop 3 methods from the YARN API,
 * so we need to use reflection to avoid compile error when building against Hadoop 2.x
 */
private object ResourceRequestHelper extends Logging {
  private val AMOUNT_AND_UNIT_REGEX = "([0-9]+)([A-Za-z]*)".r
  private val RESOURCE_INFO_CLASS = "org.apache.hadoop.yarn.api.records.ResourceInformation"
  private val RESOURCE_NOT_FOUND = "org.apache.hadoop.yarn.exceptions.ResourceNotFoundException"
  @volatile private var numResourceErrors: Int = 0

  private[yarn] def getYarnResourcesAndAmounts(
      sparkConf: SparkConf,
      componentName: String): Map[String, String] = {
    sparkConf.getAllWithPrefix(s"$componentName").map { case (key, value) =>
      val splitIndex = key.lastIndexOf('.')
      if (splitIndex == -1) {
        val errorMessage = s"Missing suffix for ${componentName}${key}, you must specify" +
          s" a suffix - $AMOUNT is currently the only supported suffix."
        throw new IllegalArgumentException(errorMessage)
      }
      val resourceName = key.substring(0, splitIndex)
      val resourceSuffix = key.substring(splitIndex + 1)
      if (!AMOUNT.equals(resourceSuffix)) {
        val errorMessage = s"Unsupported suffix: $resourceSuffix in: ${componentName}${key}, " +
          s"only .$AMOUNT is supported."
        throw new IllegalArgumentException(errorMessage)
      }
      (resourceName, value)
    }.toMap
  }

  private[yarn] def getResourceNameMapping(sparkConf: SparkConf): Map[String, String] = {
    Map(GPU -> sparkConf.get(YARN_GPU_DEVICE), FPGA -> sparkConf.get(YARN_FPGA_DEVICE))
  }

  /**
   * Convert Spark resources into YARN resources.
   * The only resources we know how to map from spark configs to yarn configs are
   * gpus and fpgas, everything else the user has to specify them in both the
   * spark.yarn.*.resource and the spark.*.resource configs.
   */
  private[yarn] def getYarnResourcesFromSparkResources(
      confPrefix: String,
      sparkConf: SparkConf
  ): Map[String, String] = {
    getResourceNameMapping(sparkConf).map {
      case (rName, yarnName) =>
        (yarnName -> sparkConf.get(new ResourceID(confPrefix, rName).amountConf, "0"))
    }.filter { case (_, count) => count.toLong > 0 }
  }

  /**
   * Validates sparkConf and throws a SparkException if any of standard resources (memory or cores)
   * is defined with the property spark.yarn.x.resource.y
   * Need to reject all combinations of AM / Driver / Executor and memory / CPU cores resources, as
   * Spark has its own names for them (memory, cores),
   * but YARN have its names too: (memory, memory-mb, mb) and (cores, vcores, cpu-vcores).
   * We need to disable every possible way YARN could receive the resource definitions above.
   */
  def validateResources(sparkConf: SparkConf): Unit = {
    val resourceDefinitions = Seq[(String, String)](
      (AM_MEMORY.key, YARN_AM_RESOURCE_TYPES_PREFIX + "memory"),
      (DRIVER_MEMORY.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "memory"),
      (EXECUTOR_MEMORY.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "memory"),
      (AM_MEMORY.key, YARN_AM_RESOURCE_TYPES_PREFIX + "mb"),
      (DRIVER_MEMORY.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "mb"),
      (EXECUTOR_MEMORY.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "mb"),
      (AM_MEMORY.key, YARN_AM_RESOURCE_TYPES_PREFIX + "memory-mb"),
      (DRIVER_MEMORY.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "memory-mb"),
      (EXECUTOR_MEMORY.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "memory-mb"),
      (AM_CORES.key, YARN_AM_RESOURCE_TYPES_PREFIX + "cores"),
      (DRIVER_CORES.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "cores"),
      (EXECUTOR_CORES.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "cores"),
      (AM_CORES.key, YARN_AM_RESOURCE_TYPES_PREFIX + "vcores"),
      (DRIVER_CORES.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "vcores"),
      (EXECUTOR_CORES.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "vcores"),
      (AM_CORES.key, YARN_AM_RESOURCE_TYPES_PREFIX + "cpu-vcores"),
      (DRIVER_CORES.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "cpu-vcores"),
      (EXECUTOR_CORES.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "cpu-vcores"),
      (new ResourceID(SPARK_EXECUTOR_PREFIX, "fpga").amountConf,
        s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${sparkConf.get(YARN_FPGA_DEVICE)}"),
      (new ResourceID(SPARK_DRIVER_PREFIX, "fpga").amountConf,
        s"${YARN_DRIVER_RESOURCE_TYPES_PREFIX}${sparkConf.get(YARN_FPGA_DEVICE)}"),
      (new ResourceID(SPARK_EXECUTOR_PREFIX, "gpu").amountConf,
        s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${sparkConf.get(YARN_GPU_DEVICE)}"),
      (new ResourceID(SPARK_DRIVER_PREFIX, "gpu").amountConf,
        s"${YARN_DRIVER_RESOURCE_TYPES_PREFIX}${sparkConf.get(YARN_GPU_DEVICE)}"))

    val errorMessage = new mutable.StringBuilder()

    resourceDefinitions.foreach { case (sparkName, resourceRequest) =>
      val resourceRequestAmount = s"${resourceRequest}.${AMOUNT}"
      if (sparkConf.contains(resourceRequestAmount)) {
        errorMessage.append(s"Error: Do not use $resourceRequestAmount, " +
            s"please use $sparkName instead!\n")
      }
    }

    if (errorMessage.nonEmpty) {
      throw new SparkException(errorMessage.toString())
    }
  }

  /**
   * Sets resource amount with the corresponding unit to the passed resource object.
   * @param resources resource values to set
   * @param resource resource object to update
   */
  def setResourceRequests(
      resources: Map[String, String],
      resource: Resource): Unit = {
    require(resource != null, "Resource parameter should not be null!")

    logDebug(s"Custom resources requested: $resources")
    if (resources.isEmpty) {
      // no point in going forward, as we don't have anything to set
      return
    }

    if (!isYarnResourceTypesAvailable()) {
      logWarning("Ignoring custom resource requests because " +
          "the version of YARN does not support it!")
      return
    }

    val resInfoClass = Utils.classForName(RESOURCE_INFO_CLASS)
    val setResourceInformationMethod =
      try {
        resource.getClass.getMethod("setResourceInformation", classOf[String], resInfoClass)
      } catch {
        case e: NoSuchMethodException =>
          throw new SparkException(
            s"Cannot find setResourceInformation in ${resource.getClass}. " +
              "This is likely due to a JAR conflict between different YARN versions.", e)
      }

    resources.foreach { case (name, rawAmount) =>
      try {
        val AMOUNT_AND_UNIT_REGEX(amountPart, unitPart) = rawAmount
        val amount = amountPart.toLong
        val unit = unitPart match {
          case "g" => "G"
          case "t" => "T"
          case "p" => "P"
          case _ => unitPart
        }
        logDebug(s"Registering resource with name: $name, amount: $amount, unit: $unit")
        val resourceInformation = createResourceInformation(name, amount, unit, resInfoClass)
        setResourceInformationMethod.invoke(
          resource, name, resourceInformation.asInstanceOf[AnyRef])
      } catch {
        case _: MatchError =>
          throw new IllegalArgumentException(s"Resource request for '$name' ('$rawAmount') " +
              s"does not match pattern $AMOUNT_AND_UNIT_REGEX.")
        case CausedBy(e: IllegalArgumentException) =>
          throw new IllegalArgumentException(s"Invalid request for $name: ${e.getMessage}")
        case e: InvocationTargetException =>
          if (e.getCause != null) {
            if (Try(Utils.classForName(RESOURCE_NOT_FOUND)).isSuccess) {
              if (e.getCause().getClass().getName().equals(RESOURCE_NOT_FOUND)) {
                // warn a couple times and then stop so we don't spam the logs
                if (numResourceErrors < 2) {
                  logWarning(s"YARN doesn't know about resource $name, your resource discovery " +
                    s"has to handle properly discovering and isolating the resource! Error: " +
                    s"${e.getCause().getMessage}")
                  numResourceErrors += 1
                }
              } else {
                throw e.getCause
              }
            } else {
              throw e.getCause
            }
          }
      }
    }
  }

  private def createResourceInformation(
      resourceName: String,
      amount: Long,
      unit: String,
      resInfoClass: Class[_]): Any = {
    val resourceInformation =
      if (unit.nonEmpty) {
        val resInfoNewInstanceMethod = resInfoClass.getMethod("newInstance",
          classOf[String], classOf[String], JLong.TYPE)
        resInfoNewInstanceMethod.invoke(null, resourceName, unit, amount.asInstanceOf[JLong])
      } else {
        val resInfoNewInstanceMethod = resInfoClass.getMethod("newInstance",
          classOf[String], JLong.TYPE)
        resInfoNewInstanceMethod.invoke(null, resourceName, amount.asInstanceOf[JLong])
      }
    resourceInformation
  }

  def isYarnCustomResourcesNonEmpty(resource: Resource): Boolean = {
    try {
      // Use reflection as this uses APIs only available in Hadoop 3
      val getResourcesMethod = resource.getClass().getMethod("getResources")
      val resources = getResourcesMethod.invoke(resource).asInstanceOf[Array[Any]]
      if (resources.nonEmpty) true else false
    } catch {
      case  _: NoSuchMethodException => false
    }
  }

  /**
   * Checks whether Hadoop 2.x or 3 is used as a dependency.
   * In case of Hadoop 3 and later, the ResourceInformation class
   * should be available on the classpath.
   */
  def isYarnResourceTypesAvailable(): Boolean = {
    Try(Utils.classForName(RESOURCE_INFO_CLASS)).isSuccess
  }
}
