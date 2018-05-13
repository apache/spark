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

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.yarn.TestYarnResourceRequestHelper.ResourceInformation

class ResourceRequestHelperSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll {

  private val CUSTOM_RES_1 = "custom-resource-type-1"
  private val CUSTOM_RES_2 = "custom-resource-type-2"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def getExpectedUnmatchedErrorMessage(name: String, value: String): String = {
    s"Resource request for '$name' ('$value') does not match pattern ([0-9]+)([A-Za-z]*)."
  }

  test("resource type value does not match pattern") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    TestYarnResourceRequestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "**@#")

    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceTypes, createAResource)
    }
    thrown.getMessage should equal (getExpectedUnmatchedErrorMessage(CUSTOM_RES_1, "**@#"))
  }

  test("resource type just unit defined") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    TestYarnResourceRequestHelper.initializeResourceTypes(List())

    val resourceTypes = Map(CUSTOM_RES_1 -> "m")

    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceTypes, createAResource)
    }
    thrown.getMessage should equal (getExpectedUnmatchedErrorMessage(CUSTOM_RES_1, "m"))
  }

  test("resource type with null value should not be allowed") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    TestYarnResourceRequestHelper.initializeResourceTypes(List())

    val resourceTypes = Map(CUSTOM_RES_1 -> "123")

    val thrown = intercept[IllegalArgumentException] {
      ResourceRequestHelper.setResourceRequests(resourceTypes, null)
    }
    thrown.getMessage should equal ("requirement failed: Resource parameter should not be null!")
  }

  test("resource type with valid value and invalid unit") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    TestYarnResourceRequestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

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
    TestYarnResourceRequestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "123")
    val resource = createAResource

    ResourceRequestHelper.setResourceRequests(resourceTypes, resource)
    val customResource: ResourceInformation = TestYarnResourceRequestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_1)
    customResource.name should equal (CUSTOM_RES_1)
    customResource.value should be (123)
    customResource.units should be ("")
  }

  test("resource type with valid value and unit") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    TestYarnResourceRequestHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "2g")
    val resource = createAResource

    ResourceRequestHelper.setResourceRequests(resourceTypes, resource)
    val customResource: ResourceInformation = TestYarnResourceRequestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_1)
    customResource.name should equal (CUSTOM_RES_1)
    customResource.value should be (2)
    customResource.units should be ("G")
  }

  test("two resource types with valid values and units") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    TestYarnResourceRequestHelper.initializeResourceTypes(List(CUSTOM_RES_1, CUSTOM_RES_2))

    val resourceTypes = Map(
      CUSTOM_RES_1 -> "123m",
      CUSTOM_RES_2 -> "10G"
    )
    val resource = createAResource

    ResourceRequestHelper.setResourceRequests(resourceTypes, resource)
    val customResource1: ResourceInformation = TestYarnResourceRequestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_1)
    customResource1.name should equal (CUSTOM_RES_1)
    customResource1.value should be (123)
    customResource1.units should be ("M")

    val customResource2: ResourceInformation = TestYarnResourceRequestHelper
      .getResourceInformationByName(resource, CUSTOM_RES_2)
    customResource2.name should equal (CUSTOM_RES_2)
    customResource2.value should be (10)
    customResource2.units should be ("G")
  }

  private def createAResource: Resource = {
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(512)
    resource.setVirtualCores(2)
    resource
  }


}
