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
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.yarn.ResourceRequestTestHelper.ResourceInformation
import org.apache.spark.deploy.yarn.config.{AM_CORES, AM_MEMORY, DRIVER_CORES, EXECUTOR_CORES, YARN_AM_RESOURCE_TYPES_PREFIX, YARN_DRIVER_RESOURCE_TYPES_PREFIX, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}
import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_MEMORY}

class ResourceRequestHelperSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll {

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

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def getExpectedUnmatchedErrorMessage(name: String, value: String): String = {
    s"Resource request for '$name' ('$value') does not match pattern ([0-9]+)([A-Za-z]*)."
  }

  test("resource type value does not match pattern") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "**@#")

    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceTypes, createAResource)
    }
    thrown.getMessage should equal (getExpectedUnmatchedErrorMessage(CUSTOM_RES_1, "**@#"))
  }

  test("resource type just unit defined") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List())

    val resourceTypes = Map(CUSTOM_RES_1 -> "m")

    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceTypes, createAResource)
    }
    thrown.getMessage should equal (getExpectedUnmatchedErrorMessage(CUSTOM_RES_1, "m"))
  }

  test("resource type with null value should not be allowed") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List())

    val resourceTypes = Map(CUSTOM_RES_1 -> "123")

    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceTypes, null)
    }
    thrown.getMessage should equal ("requirement failed: Resource parameter should not be null!")
  }

  test("resource type with valid value and invalid unit") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "123ppp")
    val resource = createAResource

    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceTypes, resource)
    }
    thrown.getMessage should fullyMatch regex
      """Unknown unit 'ppp'\. Known units are \[.*\]"""
  }

  test("resource type with valid value and without unit") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "123")
    val resource = createAResource

    ResourceRequestHelper.setResourceRequests(resourceTypes, resource)
    val resourceInfo: ResourceInformation = ResourceRequestTestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_1)
    assert(resourceInfo === ResourceInformation(CUSTOM_RES_1, 123, ""))
  }

  test("resource type with valid value and unit") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "2g")
    val resource = createAResource

    ResourceRequestHelper.setResourceRequests(resourceTypes, resource)
    val resourceInfo: ResourceInformation = ResourceRequestTestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_1)
    assert(resourceInfo === ResourceInformation(CUSTOM_RES_1, 2, "G"))
  }

  test("two resource types with valid values and units") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List(CUSTOM_RES_1, CUSTOM_RES_2))

    val resourceTypes = Map(
      CUSTOM_RES_1 -> "123m",
      CUSTOM_RES_2 -> "10G"
    )
    val resource = createAResource

    ResourceRequestHelper.setResourceRequests(resourceTypes, resource)
    val resourceInfo: ResourceInformation = ResourceRequestTestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_1)
    assert(resourceInfo === ResourceInformation(CUSTOM_RES_1, 123, "m"))

    val resourceInfo2: ResourceInformation = ResourceRequestTestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_2)
    assert(resourceInfo2 === ResourceInformation(CUSTOM_RES_2, 10, "G"))
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

  test("Memory defined with new config for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_EXECUTOR_MEMORY, "30G")

    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should include (NEW_CONFIG_EXECUTOR_MEMORY)
  }

  test("Cores defined with new config for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_EXECUTOR_CORES, "5")

    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should include (NEW_CONFIG_EXECUTOR_CORES)
  }

  test("Memory defined with new config, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_AM_MEMORY, "1G")

    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should include (NEW_CONFIG_AM_MEMORY)
  }

  test("Memory defined with new config for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_DRIVER_MEMORY, "1G")

    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should include (NEW_CONFIG_DRIVER_MEMORY)
  }

  test("Cores defined with new config, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(DRIVER_MEMORY.key, "2G")
    sparkConf.set(DRIVER_CORES.key, "4")
    sparkConf.set(EXECUTOR_MEMORY.key, "4G")
    sparkConf.set(AM_CORES.key, "2")
    sparkConf.set(NEW_CONFIG_AM_CORES, "3")

    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should include (NEW_CONFIG_AM_CORES)
  }

  test("Cores defined with new config for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set(NEW_CONFIG_DRIVER_CORES, "1G")

    val thrown = intercept[SparkException] {
      ResourceRequestHelper.validateResources(sparkConf)
    }
    thrown.getMessage should include (NEW_CONFIG_DRIVER_CORES)
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
            include(NEW_CONFIG_DRIVER_MEMORY)
        )
  }

  private def createAResource: Resource = {
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(512)
    resource.setVirtualCores(2)
    resource
  }


}
