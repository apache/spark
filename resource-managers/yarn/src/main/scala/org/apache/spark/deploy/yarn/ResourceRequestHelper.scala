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

import java.lang.{Integer => JInteger, Long => JLong}
import java.lang.reflect.InvocationTargetException

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.yarn.api.records.Resource

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.yarn.config.{AM_CORES, AM_MEMORY, DRIVER_CORES, EXECUTOR_CORES, YARN_AM_RESOURCE_TYPES_PREFIX, YARN_DRIVER_RESOURCE_TYPES_PREFIX, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_MEMORY}
import org.apache.spark.util.Utils

/**
 * This helper class uses some of Hadoop 3 methods from the YARN API,
 * so we need to use reflection to avoid compile error when building against Hadoop 2.x
 */
private object ResourceRequestHelper extends Logging {
  private val AMOUNT_AND_UNIT_REGEX = "([0-9]+)([A-Za-z]*)".r
  private val RESOURCE_INFO_CLASS = "org.apache.hadoop.yarn.api.records.ResourceInformation"
  private val ERROR_PREFIX: String = "Error:"

  /**
   * Validates sparkConf and throws a SparkException if any of standard resources (memory or cores)
   * is defined with the property spark.yarn.x.resource.y
   * @param sparkConf
   */
  def validateResources(sparkConf: SparkConf): Unit = {
    val resourceDefinitions = Seq[(String, String)](
      (AM_MEMORY.key, YARN_AM_RESOURCE_TYPES_PREFIX + "memory"),
      (AM_CORES.key, YARN_AM_RESOURCE_TYPES_PREFIX + "cores"),
      (DRIVER_MEMORY.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "memory"),
      (DRIVER_CORES.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "cores"),
      (EXECUTOR_MEMORY.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "memory"),
      (EXECUTOR_CORES.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "cores"))
    val errorMessage = new mutable.StringBuilder()

    resourceDefinitions.foreach { case (sparkName, resourceRequest) =>
      if (sparkConf.contains(resourceRequest)) {
        errorMessage.append(s"$ERROR_PREFIX Do not use $resourceRequest, " +
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
    require(!resources.contains("memory"),
      "This method is meant to set custom resources to the resource object, " +
          "but memory is a standard resource!")
    require(!resources.contains("cores"),
      "This method is meant to set custom resources to the resource object, " +
          "but cores is a standard resource!")

    logDebug(s"Custom resources requested: $resources")
    if (!isYarnResourceTypesAvailable()) {
      if (resources.nonEmpty) {
        logWarning("Ignoring updating resource with resource types because " +
            "the version of YARN does not support it!")
      }
      return
    }

    val resInfoClass = Utils.classForName(RESOURCE_INFO_CLASS)
    val setResourceInformationMethod =
      resource.getClass.getMethod("setResourceInformation", classOf[String], resInfoClass)
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
        val resourceInformation = createResourceInformation(
          name, amount, unit, resInfoClass)
        setResourceInformationMethod.invoke(
          resource, name, resourceInformation.asInstanceOf[AnyRef])
      } catch {
        case _: MatchError =>
          throw new IllegalArgumentException(s"Resource request for '$name' ('$rawAmount') " +
              s"does not match pattern $AMOUNT_AND_UNIT_REGEX.")
        case e: InvocationTargetException if e.getCause != null => throw e.getCause
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

  /**
   * Checks whether Hadoop 2.x or 3 is used as a dependency.
   * In case of Hadoop 3 and later, the ResourceInformation class
   * should be available on the classpath.
   */
  def isYarnResourceTypesAvailable(): Boolean = {
    Try(Utils.classForName(RESOURCE_INFO_CLASS)).isSuccess
  }
}
