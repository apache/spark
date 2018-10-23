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

import scala.collection.JavaConverters._

import org.apache.hadoop.yarn.api.records.Resource

import org.apache.spark.util.Utils

object ResourceRequestTestHelper {
  def initializeResourceTypes(resourceTypes: Seq[String]): Unit = {
    if (!ResourceRequestHelper.isYarnResourceTypesAvailable()) {
      throw new IllegalStateException("This method should not be invoked " +
        "since YARN resource types is not available because of old Hadoop version!" )
    }

    // ResourceUtils.reinitializeResources() is the YARN-way
    // to specify resources for the execution of the tests.
    // This method should receive standard resources with names of memory-mb and vcores.
    // Without specifying the standard resources or specifying them
    // with different names e.g. memory, YARN would throw various exceptions
    // because it relies on that standard resources are always specified.
    val defaultResourceTypes = List(
      createResourceTypeInfo("memory-mb"),
      createResourceTypeInfo("vcores"))
    val customResourceTypes = resourceTypes.map(createResourceTypeInfo)
    val allResourceTypes = defaultResourceTypes ++ customResourceTypes

    val resourceUtilsClass =
      Utils.classForName("org.apache.hadoop.yarn.util.resource.ResourceUtils")
    val reinitializeResourcesMethod = resourceUtilsClass.getMethod("reinitializeResources",
      classOf[java.util.List[AnyRef]])
    reinitializeResourcesMethod.invoke(null, allResourceTypes.asJava)
  }

  private def createResourceTypeInfo(resourceName: String): AnyRef = {
    val resTypeInfoClass = Utils.classForName("org.apache.hadoop.yarn.api.records.ResourceTypeInfo")
    val resTypeInfoNewInstanceMethod = resTypeInfoClass.getMethod("newInstance", classOf[String])
    resTypeInfoNewInstanceMethod.invoke(null, resourceName)
  }

  def getRequestedValue(res: Resource, rtype: String): AnyRef = {
    val resourceInformation = getResourceInformation(res, rtype)
    invokeMethod(resourceInformation, "getValue")
  }

  def getResourceInformationByName(res: Resource, nameParam: String): ResourceInformation = {
    val resourceInformation: AnyRef = getResourceInformation(res, nameParam)
    val name = invokeMethod(resourceInformation, "getName").asInstanceOf[String]
    val value = invokeMethod(resourceInformation, "getValue").asInstanceOf[Long]
    val units = invokeMethod(resourceInformation, "getUnits").asInstanceOf[String]
    ResourceInformation(name, value, units)
  }

  private def getResourceInformation(res: Resource, name: String): AnyRef = {
    if (!ResourceRequestHelper.isYarnResourceTypesAvailable()) {
      throw new IllegalStateException("assertResourceTypeValue() should not be invoked " +
        "since yarn resource types is not available because of old Hadoop version!")
    }

    val getResourceInformationMethod = res.getClass.getMethod("getResourceInformation",
      classOf[String])
    val resourceInformation = getResourceInformationMethod.invoke(res, name)
    resourceInformation
  }

  private def invokeMethod(resourceInformation: AnyRef, methodName: String): AnyRef = {
    val getValueMethod = resourceInformation.getClass.getMethod(methodName)
    getValueMethod.invoke(resourceInformation)
  }

  case class ResourceInformation(name: String, value: Long, unit: String)
}
