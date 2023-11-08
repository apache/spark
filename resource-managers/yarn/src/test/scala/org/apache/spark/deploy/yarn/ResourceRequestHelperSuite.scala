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
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.yarn.ResourceRequestHelper._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config.{DRIVER_CORES, DRIVER_MEMORY, EXECUTOR_CORES, EXECUTOR_MEMORY}
import org.apache.spark.resource.ResourceUtils.AMOUNT

class ResourceRequestHelperSuite extends SparkFunSuite
    with Matchers
    with ResourceRequestTestHelper {

  private val CUSTOM_RES_1 = "custom-resource-type-1"
  private val CUSTOM_RES_2 = "custom-resource-type-2"
  private val MEMORY = "memory"
  private val CORES = "cores"
  private val NEW_CONFIG_EXECUTOR_MEMORY =
    s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${MEMORY}.${AMOUNT}"
  private val NEW_CONFIG_EXECUTOR_CORES =
    s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${CORES}.${AMOUNT}"
  private val NEW_CONFIG_AM_MEMORY = s"${YARN_AM_RESOURCE_TYPES_PREFIX}${MEMORY}.${AMOUNT}"
  private val NEW_CONFIG_AM_CORES = s"${YARN_AM_RESOURCE_TYPES_PREFIX}${CORES}.${AMOUNT}"
  private val NEW_CONFIG_DRIVER_MEMORY = s"${YARN_DRIVER_RESOURCE_TYPES_PREFIX}${MEMORY}.${AMOUNT}"
  private val NEW_CONFIG_DRIVER_CORES = s"${YARN_DRIVER_RESOURCE_TYPES_PREFIX}${CORES}.${AMOUNT}"

  test("empty SparkConf should be valid") {
    val sparkConf = new SparkConf()
    validateResources(sparkConf)
  }

  test("just normal resources are defined") {
    val sparkConf = new SparkConf()
    sparkConf.set(DRIVER_MEMORY.key, "3G")
    sparkConf.set(DRIVER_CORES.key, "4")
    sparkConf.set(EXECUTOR_MEMORY.key, "4G")
    sparkConf.set(EXECUTOR_CORES.key, "2")
    validateResources(sparkConf)
  }

  test("get yarn resources from configs") {
    val sparkConf = new SparkConf()
    val resources = Map(sparkConf.get(YARN_GPU_DEVICE) -> "2G",
      sparkConf.get(YARN_GPU_DEVICE) -> "3G", "custom" -> "4")
    resources.foreach { case (name, value) =>
      sparkConf.set(s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${name}.${AMOUNT}", value)
      sparkConf.set(s"${YARN_DRIVER_RESOURCE_TYPES_PREFIX}${name}.${AMOUNT}", value)
      sparkConf.set(s"${YARN_AM_RESOURCE_TYPES_PREFIX}${name}.${AMOUNT}", value)
    }
    var parsedResources = getYarnResourcesAndAmounts(sparkConf, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX)
    assert(parsedResources === resources)
    parsedResources = getYarnResourcesAndAmounts(sparkConf, YARN_DRIVER_RESOURCE_TYPES_PREFIX)
    assert(parsedResources === resources)
    parsedResources = getYarnResourcesAndAmounts(sparkConf, YARN_AM_RESOURCE_TYPES_PREFIX)
    assert(parsedResources === resources)
  }

  test("get invalid yarn resources from configs") {
    val sparkConf = new SparkConf()

    val missingAmountConfig = s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}missingAmount"
    // missing .amount
    sparkConf.set(missingAmountConfig, "2g")
    var thrown = intercept[IllegalArgumentException] {
      getYarnResourcesAndAmounts(sparkConf, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX)
    }
    thrown.getMessage should include("Missing suffix for")

    sparkConf.remove(missingAmountConfig)
    sparkConf.set(s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}customResource.invalid", "2g")

    thrown = intercept[IllegalArgumentException] {
      getYarnResourcesAndAmounts(sparkConf, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX)
    }
    thrown.getMessage should include("Unsupported suffix")
  }

  Seq(
    "value with unit" -> Seq((CUSTOM_RES_1, 2, "G")),
    "value without unit" -> Seq((CUSTOM_RES_1, 123, "")),
    "multiple resources" -> Seq((CUSTOM_RES_1, 123, "m"), (CUSTOM_RES_2, 10, "G"))
  ).foreach { case (name, resources) =>
    test(s"valid request: $name") {
      val resourceDefs = resources.map { case (rName, _, _) => rName }
      val requests = resources.map { case (rName, rValue, rUnit) =>
        (rName, rValue.toString + rUnit)
      }.toMap
      withResourceTypes(resourceDefs) {
        val resource = createResource()
        setResourceRequests(requests, resource)

        resources.foreach { case (rName, rValue, rUnit) =>
          val requested = resource.getResourceInformation(rName)
          assert(requested.getName === rName)
          assert(requested.getValue === rValue)
          assert(requested.getUnits === rUnit)
        }
      }
    }
  }

  Seq(
    ("value does not match pattern", CUSTOM_RES_1, "**@#"),
    ("only unit defined", CUSTOM_RES_1, "m"),
    ("invalid unit", CUSTOM_RES_1, "123ppp")
  ).foreach { case (name, key, value) =>
    test(s"invalid request: $name") {
      withResourceTypes(Seq(key)) {
        val resource = createResource()
        val thrown = intercept[IllegalArgumentException] {
          setResourceRequests(Map(key -> value), resource)
        }
        thrown.getMessage should include (key)
      }
    }
  }

  Seq(
    NEW_CONFIG_EXECUTOR_MEMORY -> "30G",
    s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}memory-mb.$AMOUNT" -> "30G",
    s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}mb.$AMOUNT" -> "30G",
    NEW_CONFIG_EXECUTOR_CORES -> "5",
    s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}vcores.$AMOUNT" -> "5",
    NEW_CONFIG_AM_MEMORY -> "1G",
    NEW_CONFIG_DRIVER_MEMORY -> "1G",
    NEW_CONFIG_AM_CORES -> "3",
    NEW_CONFIG_DRIVER_CORES -> "1G"
  ).foreach { case (key, value) =>
    test(s"disallowed resource request: $key") {
      val conf = new SparkConf(false).set(key, value)
      val thrown = intercept[SparkException] {
        validateResources(conf)
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
      validateResources(sparkConf)
    }
    thrown.getMessage should (
      include(NEW_CONFIG_EXECUTOR_MEMORY) and
      include(NEW_CONFIG_AM_MEMORY) and
      include(NEW_CONFIG_DRIVER_MEMORY))
  }

  private def createResource(): Resource = {
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemorySize(512)
    resource.setVirtualCores(2)
    resource
  }
}
