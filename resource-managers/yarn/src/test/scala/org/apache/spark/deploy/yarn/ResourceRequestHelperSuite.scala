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
import org.apache.spark.internal.config.{DRIVER_CORES, DRIVER_MEMORY, EXECUTOR_CORES, EXECUTOR_MEMORY}

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

  Seq(
    "value with unit" -> Seq(ResourceInformation(CUSTOM_RES_1, 2, "G")),
    "value without unit" -> Seq(ResourceInformation(CUSTOM_RES_1, 123, "")),
    "multiple resources" -> Seq(ResourceInformation(CUSTOM_RES_1, 123, "m"),
      ResourceInformation(CUSTOM_RES_2, 10, "G"))
  ).foreach { case (name, resources) =>
    test(s"valid request: $name") {
      assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
      val resourceDefs = resources.map { r => r.name }
      val requests = resources.map { r => (r.name, r.value.toString + r.unit) }.toMap

      ResourceRequestTestHelper.initializeResourceTypes(resourceDefs)

      val resource = createResource()
      ResourceRequestHelper.setResourceRequests(requests, resource)

      resources.foreach { r =>
        val requested = ResourceRequestTestHelper.getResourceInformationByName(resource, r.name)
        assert(requested === r)
      }
    }
  }

  Seq(
    ("value does not match pattern", CUSTOM_RES_1, "**@#"),
    ("only unit defined", CUSTOM_RES_1, "m"),
    ("invalid unit", CUSTOM_RES_1, "123ppp")
  ).foreach { case (name, key, value) =>
    test(s"invalid request: $name") {
      assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
      ResourceRequestTestHelper.initializeResourceTypes(Seq(key))

      val resource = createResource()
      val thrown = intercept[IllegalArgumentException] {
        ResourceRequestHelper.setResourceRequests(Map(key -> value), resource)
      }
      thrown.getMessage should include (key)
    }
  }

  Seq(
    NEW_CONFIG_EXECUTOR_MEMORY -> "30G",
    YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "memory-mb" -> "30G",
    YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "mb" -> "30G",
    NEW_CONFIG_EXECUTOR_CORES -> "5",
    YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "vcores" -> "5",
    NEW_CONFIG_AM_MEMORY -> "1G",
    NEW_CONFIG_DRIVER_MEMORY -> "1G",
    NEW_CONFIG_AM_CORES -> "3",
    NEW_CONFIG_DRIVER_CORES -> "1G"
  ).foreach { case (key, value) =>
    test(s"disallowed resource request: $key") {
      assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
      val conf = new SparkConf(false).set(key, value)
      val thrown = intercept[SparkException] {
        ResourceRequestHelper.validateResources(conf)
      }
      thrown.getMessage should include (key)
    }
  }

  test("multiple disallowed resources in config") {
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

  private def createResource(): Resource = {
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(512)
    resource.setVirtualCores(2)
    resource
  }
}
