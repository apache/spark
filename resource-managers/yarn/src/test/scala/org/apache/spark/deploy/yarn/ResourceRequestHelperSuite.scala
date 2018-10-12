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

import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.util.Records
import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.yarn.ResourceRequestTestHelper.ResourceInformation
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_MEMORY}

class ResourceRequestHelperSuite extends SparkFunSuite with Matchers {

  private val CUSTOM_RES_1 = "custom-resource-type-1"
  private val CUSTOM_RES_2 = "custom-resource-type-2"
  private val MEMORY = "memory"
  private val CORES = "cores"
  private val NEW_CONFIG_EXECUTOR_MEMORY = YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + MEMORY
  private val NEW_CONFIG_EXECUTOR_CORES = YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + CORES
  private val NEW_CONFIG_AM_MEMORY = YARN_AM_RESOURCE_TYPES_PREFIX + MEMORY
  private val NEW_CONFIG_AM_CORES = YARN_AM_RESOURCE_TYPES_PREFIX + CORES
  private val NEW_CONFIG_DRIVER_MEMORY = YARN_DRIVER_RESOURCE_TYPES_PREFIX + MEMORY
  private val NEW_CONFIG_DRIVER_CORES = YARN_DRIVER_RESOURCE_TYPES_PREFIX + CORES

  test("resource request value does not match pattern") {
    verifySetResourceRequestsException(List(CUSTOM_RES_1),
      Map(CUSTOM_RES_1 -> "**@#"), CUSTOM_RES_1)
  }

  test("resource request just unit defined") {
    verifySetResourceRequestsException(List(), Map(CUSTOM_RES_1 -> "m"), CUSTOM_RES_1)
  }

  test("resource request with null value should not be allowed") {
    verifySetResourceRequestsException(List(), null, Map(CUSTOM_RES_1 -> "123"),
      "requirement failed: Resource parameter should not be null!")
  }

  test("resource request with valid value and invalid unit") {
    verifySetResourceRequestsException(List(CUSTOM_RES_1), createResource,
      Map(CUSTOM_RES_1 -> "123ppp"), "")
  }

  test("resource request with valid value and without unit") {
    verifySetResourceRequestsSuccessful(List(CUSTOM_RES_1), Map(CUSTOM_RES_1 -> "123"),
      Map(CUSTOM_RES_1 -> ResourceInformation(CUSTOM_RES_1, 123, "")))
  }

  test("resource request with valid value and unit") {
    verifySetResourceRequestsSuccessful(List(CUSTOM_RES_1), Map(CUSTOM_RES_1 -> "2g"),
      Map(CUSTOM_RES_1 -> ResourceInformation(CUSTOM_RES_1, 2, "G")))
  }

  test("two resource requests with valid values and units") {
    verifySetResourceRequestsSuccessful(List(CUSTOM_RES_1, CUSTOM_RES_2),
      Map(CUSTOM_RES_1 -> "123m", CUSTOM_RES_2 -> "10G"),
      Map(CUSTOM_RES_1 -> ResourceInformation(CUSTOM_RES_1, 123, "m"),
        CUSTOM_RES_2 -> ResourceInformation(CUSTOM_RES_2, 10, "G")))
  }

  test("empty SparkConf should be valid") {
    val sparkConf = new SparkConf()
    ResourceRequestHelper.validateResources(sparkConf)
  }

  test("just normal resources are defined") {
    val sparkConf = new SparkConf()
    sparkConf.set(DRIVER_MEMORY.key, "3G")
    sparkConf.set(DRIVER_CORES.key, "4")
    sparkConf.set(EXECUTOR_MEMORY.key, "4G")
    sparkConf.set(EXECUTOR_CORES.key, "2")
    ResourceRequestHelper.validateResources(sparkConf)
  }

  test("memory defined with new config for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_EXECUTOR_MEMORY, "30G")
    verifyValidateResourcesException(sparkConf, NEW_CONFIG_EXECUTOR_MEMORY)
  }

  test("memory defined with new config for executor 2") {
    val sparkConf = new SparkConf()
    sparkConf.set(YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "memory-mb", "30G")
    verifyValidateResourcesException(sparkConf, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "memory-mb")
  }

  test("memory defined with new config for executor 3") {
    val sparkConf = new SparkConf()
    sparkConf.set(YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "mb", "30G")
    verifyValidateResourcesException(sparkConf, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "mb")
  }

  test("cores defined with new config for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_EXECUTOR_CORES, "5")
    verifyValidateResourcesException(sparkConf, NEW_CONFIG_EXECUTOR_CORES)
  }

  test("cores defined with new config for executor 2") {
    val sparkConf = new SparkConf()
    sparkConf.set(YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "vcores", "5")
    verifyValidateResourcesException(sparkConf, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "vcores")
  }

  test("memory defined with new config, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_AM_MEMORY, "1G")
    verifyValidateResourcesException(sparkConf, NEW_CONFIG_AM_MEMORY)
  }

  test("memory defined with new config for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_DRIVER_MEMORY, "1G")
    verifyValidateResourcesException(sparkConf, NEW_CONFIG_DRIVER_MEMORY)
  }

  test("cores defined with new config, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_AM_CORES, "3")
    verifyValidateResourcesException(sparkConf, NEW_CONFIG_AM_CORES)
  }

  test("cores defined with new config for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_DRIVER_CORES, "1G")
    verifyValidateResourcesException(sparkConf, NEW_CONFIG_DRIVER_CORES)
  }

  test("various duplicated definitions") {
    val sparkConf = new SparkConf()
    sparkConf.set(DRIVER_MEMORY.key, "2G")
    sparkConf.set(DRIVER_CORES.key, "2")
    sparkConf.set(EXECUTOR_MEMORY.key, "2G")
    sparkConf.set(EXECUTOR_CORES.key, "4")
    sparkConf.set(AM_MEMORY.key, "3G")
    sparkConf.set(NEW_CONFIG_EXECUTOR_MEMORY, "3G")
    sparkConf.set(NEW_CONFIG_AM_MEMORY, "2G")
    sparkConf.set(NEW_CONFIG_DRIVER_MEMORY, "2G")

    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should (
      include(NEW_CONFIG_EXECUTOR_MEMORY) and
      include(NEW_CONFIG_AM_MEMORY) and
      include(NEW_CONFIG_DRIVER_MEMORY))
  }

  private def verifySetResourceRequestsSuccessful(
      definedResourceTypes: List[String],
      resourceRequests: Map[String, String],
      expectedResources: Map[String, ResourceInformation]): Unit = {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(definedResourceTypes)

    val resource = createResource()
    ResourceRequestHelper.setResourceRequests(resourceRequests, resource)

    expectedResources.foreach { case (name, ri) =>
      val resourceInfo = ResourceRequestTestHelper.getResourceInformationByName(resource, name)
      assert(resourceInfo === ri)
    }
  }

  private def verifySetResourceRequestsException(
      definedResourceTypes: List[String],
      resourceRequests: Map[String, String],
      message: String): Unit = {
    val resource = createResource()
    verifySetResourceRequestsException(definedResourceTypes, resource, resourceRequests, message)
  }

  private def verifySetResourceRequestsException(
      definedResourceTypes: List[String],
      resource: Resource,
      resourceRequests: Map[String, String],
      message: String) = {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(definedResourceTypes)
    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceRequests, resource)
    }
    if (!message.isEmpty) {
      thrown.getMessage should include (message)
    }
  }

  private def verifyValidateResourcesException(sparkConf: SparkConf, message: String) = {
    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should include (message)
  }

  private def createResource(): Resource = {
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(512)
    resource.setVirtualCores(2)
    resource
  }
}
