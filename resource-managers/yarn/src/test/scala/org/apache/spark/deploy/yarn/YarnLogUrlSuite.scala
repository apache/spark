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

import org.apache.spark.SparkFunSuite

class YarnLogUrlSuite extends SparkFunSuite {

  private val testHttpScheme = "https://"
  private val testNodeHttpAddress = "nodeManager:1234"
  private val testContainerId = "testContainer"
  private val testUser = "testUser"
  private val testEnvNameToFileNameMap = Map("TEST_ENV_STDOUT" -> "stdout",
    "TEST_ENV_STDERR" -> "stderr")

  test("Custom log URL - leverage all patterns, all values for patterns are available") {
    val logUrlPattern = "{{HttpScheme}}{{NodeHttpAddress}}/logs/clusters/{{ClusterId}}" +
      "/containers/{{ContainerId}}/users/{{User}}/files/{{FileName}}"

    val clusterId = Some("testCluster")

    val logUrls = ExecutorRunnable.buildLogUrls(logUrlPattern, testHttpScheme, testNodeHttpAddress,
      clusterId, testContainerId, testUser, testEnvNameToFileNameMap)

    val expectedLogUrls = testEnvNameToFileNameMap.map { case (envName, fileName) =>
      envName -> (s"$testHttpScheme$testNodeHttpAddress/logs/clusters/${clusterId.get}" +
        s"/containers/$testContainerId/users/$testUser/files/$fileName")
    }

    assert(logUrls === expectedLogUrls)
  }

  test("Custom log URL - optional pattern is not used in log URL") {
    // here {{ClusterId}} is excluded in this pattern
    val logUrlPattern = "{{HttpScheme}}{{NodeHttpAddress}}/logs/containers/{{ContainerId}}" +
      "/users/{{User}}/files/{{FileName}}"

    // suppose the value of {{ClusterId}} pattern is not available
    val clusterId = None

    // This should not throw an exception: the value for optional pattern is not available
    // but we also don't use the pattern in log URL.
    val logUrls = ExecutorRunnable.buildLogUrls(logUrlPattern, testHttpScheme, testNodeHttpAddress,
      clusterId, testContainerId, testUser, testEnvNameToFileNameMap)

    val expectedLogUrls = testEnvNameToFileNameMap.map { case (envName, fileName) =>
      envName -> (s"$testHttpScheme$testNodeHttpAddress/logs/containers/$testContainerId" +
        s"/users/$testUser/files/$fileName")
    }

    assert(logUrls === expectedLogUrls)
  }

  test("Custom log URL - optional pattern is used in log URL but the value " +
    "is not present") {
    // here {{ClusterId}} is included in this pattern
    val logUrlPattern = "{{HttpScheme}}{{NodeHttpAddress}}/logs/clusters/{{ClusterId}}" +
      "/containers/{{ContainerId}}/users/{{User}}/files/{{FileName}}"

    // suppose the value of {{ClusterId}} pattern is not available
    val clusterId = None

    intercept[IllegalArgumentException] {
      ExecutorRunnable.buildLogUrls(logUrlPattern, testHttpScheme, testNodeHttpAddress,
        clusterId, testContainerId, testUser, testEnvNameToFileNameMap)
    }
  }
}
