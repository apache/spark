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

package object constants {
  // Labels
  private[spark] val SPARK_APP_ID_LABEL = "spark-app-selector"
  private[spark] val SPARK_EXECUTOR_ID_LABEL = "spark-exec-id"
  private[spark] val SPARK_ROLE_LABEL = "spark-role"
  private[spark] val SPARK_POD_DRIVER_ROLE = "driver"
  private[spark] val SPARK_POD_EXECUTOR_ROLE = "executor"

  // Credentials secrets
  private[spark] val DRIVER_CREDENTIALS_SECRETS_BASE_DIR =
    "/mnt/secrets/spark-kubernetes-credentials"
  private[spark] val DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME = "ca-cert"
  private[spark] val DRIVER_CREDENTIALS_CA_CERT_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME"
  private[spark] val DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME = "client-key"
  private[spark] val DRIVER_CREDENTIALS_CLIENT_KEY_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME"
  private[spark] val DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME = "client-cert"
  private[spark] val DRIVER_CREDENTIALS_CLIENT_CERT_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME"
  private[spark] val DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME = "oauth-token"
  private[spark] val DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME"
  private[spark] val DRIVER_CREDENTIALS_SECRET_VOLUME_NAME = "kubernetes-credentials"

  // Default and fixed ports
  private[spark] val DEFAULT_DRIVER_PORT = 7078
  private[spark] val DEFAULT_BLOCKMANAGER_PORT = 7079
  private[spark] val DEFAULT_UI_PORT = 4040
  private[spark] val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  private[spark] val DRIVER_PORT_NAME = "driver-rpc-port"
  private[spark] val EXECUTOR_PORT_NAME = "executor"

  // Environment Variables
  private[spark] val ENV_EXECUTOR_PORT = "SPARK_EXECUTOR_PORT"
  private[spark] val ENV_DRIVER_URL = "SPARK_DRIVER_URL"
  private[spark] val ENV_EXECUTOR_CORES = "SPARK_EXECUTOR_CORES"
  private[spark] val ENV_EXECUTOR_MEMORY = "SPARK_EXECUTOR_MEMORY"
  private[spark] val ENV_APPLICATION_ID = "SPARK_APPLICATION_ID"
  private[spark] val ENV_EXECUTOR_ID = "SPARK_EXECUTOR_ID"
  private[spark] val ENV_EXECUTOR_POD_IP = "SPARK_EXECUTOR_POD_IP"
  private[spark] val ENV_DRIVER_MEMORY = "SPARK_DRIVER_MEMORY"
  private[spark] val ENV_SUBMIT_EXTRA_CLASSPATH = "SPARK_SUBMIT_EXTRA_CLASSPATH"
  private[spark] val ENV_EXECUTOR_EXTRA_CLASSPATH = "SPARK_EXECUTOR_EXTRA_CLASSPATH"
  private[spark] val ENV_MOUNTED_CLASSPATH = "SPARK_MOUNTED_CLASSPATH"
  private[spark] val ENV_DRIVER_MAIN_CLASS = "SPARK_DRIVER_CLASS"
  private[spark] val ENV_DRIVER_ARGS = "SPARK_DRIVER_ARGS"
  private[spark] val ENV_DRIVER_JAVA_OPTS = "SPARK_DRIVER_JAVA_OPTS"
  private[spark] val ENV_MOUNTED_FILES_DIR = "SPARK_MOUNTED_FILES_DIR"
  private[spark] val ENV_PYSPARK_FILES = "PYSPARK_FILES"
  private[spark] val ENV_PYSPARK_PRIMARY = "PYSPARK_PRIMARY"
  private[spark] val ENV_JAVA_OPT_PREFIX = "SPARK_JAVA_OPT_"

  // Miscellaneous
  private[spark] val DRIVER_CONTAINER_NAME = "spark-kubernetes-driver"
  private[spark] val KUBERNETES_MASTER_INTERNAL_URL = "https://kubernetes.default.svc"
  private[spark] val MEMORY_OVERHEAD_FACTOR = 0.10
  private[spark] val MEMORY_OVERHEAD_MIN_MIB = 384L
}
