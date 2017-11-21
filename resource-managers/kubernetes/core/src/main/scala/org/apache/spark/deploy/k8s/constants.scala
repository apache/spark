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
package org.apache.spark.deploy.k8s

private[spark] object constants {

  // Labels
  val SPARK_APP_ID_LABEL = "spark-app-selector"
  val SPARK_EXECUTOR_ID_LABEL = "spark-exec-id"
  val SPARK_ROLE_LABEL = "spark-role"
  val SPARK_POD_DRIVER_ROLE = "driver"
  val SPARK_POD_EXECUTOR_ROLE = "executor"

  // Default and fixed ports
  val DEFAULT_DRIVER_PORT = 7078
  val DEFAULT_BLOCKMANAGER_PORT = 7079
  val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  val EXECUTOR_PORT_NAME = "executor"

  // Environment Variables
  val ENV_EXECUTOR_PORT = "SPARK_EXECUTOR_PORT"
  val ENV_DRIVER_URL = "SPARK_DRIVER_URL"
  val ENV_EXECUTOR_CORES = "SPARK_EXECUTOR_CORES"
  val ENV_EXECUTOR_MEMORY = "SPARK_EXECUTOR_MEMORY"
  val ENV_APPLICATION_ID = "SPARK_APPLICATION_ID"
  val ENV_EXECUTOR_ID = "SPARK_EXECUTOR_ID"
  val ENV_EXECUTOR_POD_IP = "SPARK_EXECUTOR_POD_IP"
  val ENV_EXECUTOR_EXTRA_CLASSPATH = "SPARK_EXECUTOR_EXTRA_CLASSPATH"
  val ENV_MOUNTED_CLASSPATH = "SPARK_MOUNTED_CLASSPATH"
  val ENV_JAVA_OPT_PREFIX = "SPARK_JAVA_OPT_"

  // Miscellaneous
  val KUBERNETES_MASTER_INTERNAL_URL = "https://kubernetes.default.svc"
  val MEMORY_OVERHEAD_FACTOR = 0.10
  val MEMORY_OVERHEAD_MIN_MIB = 384L
}
