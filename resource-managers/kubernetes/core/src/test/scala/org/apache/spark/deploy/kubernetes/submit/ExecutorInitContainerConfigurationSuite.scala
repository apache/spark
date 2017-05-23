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
package org.apache.spark.deploy.kubernetes.submit

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._

class ExecutorInitContainerConfigurationSuite extends SparkFunSuite {

  private val SECRET_NAME = "init-container-secret"
  private val SECRET_MOUNT_DIR = "/mnt/secrets/spark"
  private val CONFIG_MAP_NAME = "spark-config-map"
  private val CONFIG_MAP_KEY = "spark-config-map-key"

  test("Not passing a secret name should not set the secret value.") {
    val baseSparkConf = new SparkConf(false)
    val configurationUnderTest = new ExecutorInitContainerConfigurationImpl(
      None,
      SECRET_MOUNT_DIR,
      CONFIG_MAP_NAME,
      CONFIG_MAP_KEY)
    val resolvedSparkConf = configurationUnderTest
        .configureSparkConfForExecutorInitContainer(baseSparkConf)
    assert(resolvedSparkConf.get(EXECUTOR_INIT_CONTAINER_CONFIG_MAP).contains(CONFIG_MAP_NAME))
    assert(resolvedSparkConf.get(EXECUTOR_INIT_CONTAINER_CONFIG_MAP_KEY).contains(CONFIG_MAP_KEY))
    assert(resolvedSparkConf.get(EXECUTOR_INIT_CONTAINER_SECRET_MOUNT_DIR)
        .contains(SECRET_MOUNT_DIR))
    assert(resolvedSparkConf.get(EXECUTOR_INIT_CONTAINER_SECRET).isEmpty)
  }

  test("Passing a secret name should set the secret value.") {
    val baseSparkConf = new SparkConf(false)
    val configurationUnderTest = new ExecutorInitContainerConfigurationImpl(
      Some(SECRET_NAME),
      SECRET_MOUNT_DIR,
      CONFIG_MAP_NAME,
      CONFIG_MAP_KEY)
    val resolvedSparkConf = configurationUnderTest
      .configureSparkConfForExecutorInitContainer(baseSparkConf)
    assert(resolvedSparkConf.get(EXECUTOR_INIT_CONTAINER_SECRET).contains(SECRET_NAME))
  }
}
