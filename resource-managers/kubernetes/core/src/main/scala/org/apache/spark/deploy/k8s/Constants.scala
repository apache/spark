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

private[spark] object Constants {

  // Labels
  val SPARK_APP_ID_LABEL = "spark-app-selector"
  val SPARK_EXECUTOR_ID_LABEL = "spark-exec-id"
  val SPARK_ROLE_LABEL = "spark-role"
  val SPARK_POD_DRIVER_ROLE = "driver"
  val SPARK_POD_EXECUTOR_ROLE = "executor"

  // Credentials secrets
  val DRIVER_CREDENTIALS_SECRETS_BASE_DIR =
    "/mnt/secrets/spark-kubernetes-credentials"
  val DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME = "ca-cert"
  val DRIVER_CREDENTIALS_CA_CERT_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME"
  val DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME = "client-key"
  val DRIVER_CREDENTIALS_CLIENT_KEY_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME"
  val DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME = "client-cert"
  val DRIVER_CREDENTIALS_CLIENT_CERT_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME"
  val DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME = "oauth-token"
  val DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME"
  val DRIVER_CREDENTIALS_SECRET_VOLUME_NAME = "kubernetes-credentials"

  // Default and fixed ports
  val DEFAULT_DRIVER_PORT = 7078
  val DEFAULT_BLOCKMANAGER_PORT = 7079
  val DRIVER_PORT_NAME = "driver-rpc-port"
  val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  val UI_PORT_NAME = "spark-ui"

  // Environment Variables
  val ENV_DRIVER_URL = "SPARK_DRIVER_URL"
  val ENV_EXECUTOR_CORES = "SPARK_EXECUTOR_CORES"
  val ENV_EXECUTOR_MEMORY = "SPARK_EXECUTOR_MEMORY"
  val ENV_APPLICATION_ID = "SPARK_APPLICATION_ID"
  val ENV_EXECUTOR_ID = "SPARK_EXECUTOR_ID"
  val ENV_EXECUTOR_POD_IP = "SPARK_EXECUTOR_POD_IP"
  val ENV_JAVA_OPT_PREFIX = "SPARK_JAVA_OPT_"
  val ENV_CLASSPATH = "SPARK_CLASSPATH"
  val ENV_DRIVER_BIND_ADDRESS = "SPARK_DRIVER_BIND_ADDRESS"
  val ENV_SPARK_CONF_DIR = "SPARK_CONF_DIR"
  val ENV_SPARK_USER = "SPARK_USER"
  // Spark app configs for containers
  val SPARK_CONF_VOLUME = "spark-conf-volume"
  val SPARK_CONF_DIR_INTERNAL = "/opt/spark/conf"
  val SPARK_CONF_FILE_NAME = "spark.properties"
  val SPARK_CONF_PATH = s"$SPARK_CONF_DIR_INTERNAL/$SPARK_CONF_FILE_NAME"
  val ENV_HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION"

  // BINDINGS
  val ENV_PYSPARK_MAJOR_PYTHON_VERSION = "PYSPARK_MAJOR_PYTHON_VERSION"

  // Pod spec templates
  val EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME = "pod-spec-template.yml"
  val EXECUTOR_POD_SPEC_TEMPLATE_MOUNTPATH = "/opt/spark/pod-template"
  val POD_TEMPLATE_VOLUME = "pod-template-volume"
  val POD_TEMPLATE_CONFIGMAP = "podspec-configmap"
  val POD_TEMPLATE_KEY = "podspec-configmap-key"

  // Miscellaneous
  val KUBERNETES_MASTER_INTERNAL_URL = "https://kubernetes.default.svc"
  val DEFAULT_DRIVER_CONTAINER_NAME = "spark-kubernetes-driver"
  val DEFAULT_EXECUTOR_CONTAINER_NAME = "spark-kubernetes-executor"
  val MEMORY_OVERHEAD_MIN_MIB = 384L
  val NON_JVM_MEMORY_OVERHEAD_FACTOR = 0.4d

  // Hadoop Configuration
  val HADOOP_CONF_VOLUME = "hadoop-properties"
  val KRB_FILE_VOLUME = "krb5-file"
  val HADOOP_CONF_DIR_PATH = "/opt/hadoop/conf"
  val KRB_FILE_DIR_PATH = "/etc"
  val ENV_HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
  val HADOOP_CONFIG_MAP_NAME =
    "spark.kubernetes.executor.hadoopConfigMapName"

  // Kerberos Configuration
  val KERBEROS_DT_SECRET_NAME =
    "spark.kubernetes.kerberos.dt-secret-name"
  val KERBEROS_DT_SECRET_KEY =
    "spark.kubernetes.kerberos.dt-secret-key"
  val KERBEROS_SECRET_KEY = "hadoop-tokens"
  val KERBEROS_KEYTAB_VOLUME = "kerberos-keytab"
  val KERBEROS_KEYTAB_MOUNT_POINT = "/mnt/secrets/kerberos-keytab"

  // Hadoop credentials secrets for the Spark app.
  val SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR = "/mnt/secrets/hadoop-credentials"
  val SPARK_APP_HADOOP_SECRET_VOLUME_NAME = "hadoop-secret"

  // Application resource types.
  val APP_RESOURCE_TYPE_JAVA = "java"
  val APP_RESOURCE_TYPE_PYTHON = "python"
  val APP_RESOURCE_TYPE_R = "r"
}
