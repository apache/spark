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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._

private[spark] trait ExecutorInitContainerConfiguration {
  /**
   * Provide the driver with configuration that allows it to configure executors to
   * fetch resources in the same way the driver does.
   */
  def configureSparkConfForExecutorInitContainer(originalSparkConf: SparkConf): SparkConf
}

private[spark] class ExecutorInitContainerConfigurationImpl(
    initContainerSecretName: Option[String],
    initContainerSecretMountDir: String,
    initContainerConfigMapName: String,
    initContainerConfigMapKey: String)
    extends ExecutorInitContainerConfiguration {
  def configureSparkConfForExecutorInitContainer(originalSparkConf: SparkConf): SparkConf = {
    val configuredSparkConf = originalSparkConf.clone()
      .set(EXECUTOR_INIT_CONTAINER_CONFIG_MAP,
        initContainerConfigMapName)
      .set(EXECUTOR_INIT_CONTAINER_CONFIG_MAP_KEY,
        initContainerConfigMapKey)
      .set(EXECUTOR_INIT_CONTAINER_SECRET_MOUNT_DIR, initContainerSecretMountDir)
    initContainerSecretName.map { secret =>
      configuredSparkConf.set(EXECUTOR_INIT_CONTAINER_SECRET, secret)
    }.getOrElse(configuredSparkConf)
  }
}
