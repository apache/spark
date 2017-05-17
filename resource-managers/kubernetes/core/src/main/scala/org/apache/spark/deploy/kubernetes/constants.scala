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
package org.apache.spark.deploy.kubernetes

package object constants {
  // Labels
  private[spark] val SPARK_DRIVER_LABEL = "spark-driver"
  private[spark] val SPARK_APP_ID_LABEL = "spark-app-id"
  private[spark] val SPARK_APP_NAME_LABEL = "spark-app-name"
  private[spark] val SPARK_EXECUTOR_ID_LABEL = "spark-exec-id"

  // Secrets
  private[spark] val DRIVER_CONTAINER_SUBMISSION_SECRETS_BASE_DIR =
    "/var/run/secrets/spark-submission"
  private[spark] val SUBMISSION_APP_SECRET_NAME = "spark-submission-server-secret"
  private[spark] val SUBMISSION_APP_SECRET_PREFIX = "spark-submission-server-secret"
  private[spark] val SUBMISSION_APP_SECRET_VOLUME_NAME = "spark-submission-secret-volume"
  private[spark] val SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME =
    "spark-submission-server-key-password"
  private[spark] val SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME =
    "spark-submission-server-keystore-password"
  private[spark] val SUBMISSION_SSL_KEYSTORE_SECRET_NAME = "spark-submission-server-keystore"
  private[spark] val SUBMISSION_SSL_SECRETS_PREFIX = "spark-submission-server-ssl"
  private[spark] val SUBMISSION_SSL_SECRETS_VOLUME_NAME = "spark-submission-server-ssl-secrets"
  private[spark] val SUBMISSION_SSL_KEY_PEM_SECRET_NAME = "spark-submission-server-key-pem"
  private[spark] val SUBMISSION_SSL_CERT_PEM_SECRET_NAME = "spark-submission-server-cert-pem"

  // Default and fixed ports
  private[spark] val SUBMISSION_SERVER_PORT = 7077
  private[spark] val DEFAULT_DRIVER_PORT = 7078
  private[spark] val DEFAULT_BLOCKMANAGER_PORT = 7079
  private[spark] val DEFAULT_UI_PORT = 4040
  private[spark] val UI_PORT_NAME = "spark-ui-port"
  private[spark] val SUBMISSION_SERVER_PORT_NAME = "submit-server"
  private[spark] val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  private[spark] val DRIVER_PORT_NAME = "driver"
  private[spark] val EXECUTOR_PORT_NAME = "executor"

  // Environment Variables
  private[spark] val ENV_SUBMISSION_SECRET_LOCATION = "SPARK_SUBMISSION_SECRET_LOCATION"
  private[spark] val ENV_SUBMISSION_SERVER_PORT = "SPARK_SUBMISSION_SERVER_PORT"
  private[spark] val ENV_SUBMISSION_KEYSTORE_FILE = "SPARK_SUBMISSION_KEYSTORE_FILE"
  private[spark] val ENV_SUBMISSION_KEYSTORE_PASSWORD_FILE =
    "SPARK_SUBMISSION_KEYSTORE_PASSWORD_FILE"
  private[spark] val ENV_SUBMISSION_KEYSTORE_KEY_PASSWORD_FILE =
    "SPARK_SUBMISSION_KEYSTORE_KEY_PASSWORD_FILE"
  private[spark] val ENV_SUBMISSION_KEYSTORE_TYPE = "SPARK_SUBMISSION_KEYSTORE_TYPE"
  private[spark] val ENV_SUBMISSION_KEY_PEM_FILE = "SPARK_SUBMISSION_KEY_PEM_FILE"
  private[spark] val ENV_SUBMISSION_CERT_PEM_FILE = "SPARK_SUBMISSION_CERT_PEM_FILE"
  private[spark] val ENV_SUBMISSION_USE_SSL = "SPARK_SUBMISSION_USE_SSL"
  private[spark] val ENV_EXECUTOR_PORT = "SPARK_EXECUTOR_PORT"
  private[spark] val ENV_DRIVER_URL = "SPARK_DRIVER_URL"
  private[spark] val ENV_EXECUTOR_CORES = "SPARK_EXECUTOR_CORES"
  private[spark] val ENV_EXECUTOR_MEMORY = "SPARK_EXECUTOR_MEMORY"
  private[spark] val ENV_APPLICATION_ID = "SPARK_APPLICATION_ID"
  private[spark] val ENV_EXECUTOR_ID = "SPARK_EXECUTOR_ID"
  private[spark] val ENV_EXECUTOR_POD_IP = "SPARK_EXECUTOR_POD_IP"
  private[spark] val ENV_DRIVER_MEMORY = "SPARK_DRIVER_MEMORY"
  private[spark] val ENV_UPLOADED_JARS_DIR = "SPARK_UPLOADED_JARS_DIR"
  private[spark] val ENV_SUBMIT_EXTRA_CLASSPATH = "SPARK_SUBMIT_EXTRA_CLASSPATH"
  private[spark] val ENV_MOUNTED_CLASSPATH = "SPARK_MOUNTED_CLASSPATH"
  private[spark] val ENV_DRIVER_MAIN_CLASS = "SPARK_DRIVER_CLASS"
  private[spark] val ENV_DRIVER_ARGS = "SPARK_DRIVER_ARGS"
  private[spark] val ENV_DRIVER_JAVA_OPTS = "SPARK_DRIVER_JAVA_OPTS"

  // Annotation keys
  private[spark] val ANNOTATION_PROVIDE_EXTERNAL_URI =
    "spark-job.alpha.apache.org/provideExternalUri"
  private[spark] val ANNOTATION_RESOLVED_EXTERNAL_URI =
    "spark-job.alpha.apache.org/resolvedExternalUri"

  // Miscellaneous
  private[spark] val DRIVER_CONTAINER_NAME = "spark-kubernetes-driver"
  private[spark] val DRIVER_SUBMIT_SSL_NAMESPACE = "kubernetes.driversubmitserver"
  private[spark] val KUBERNETES_MASTER_INTERNAL_URL = "https://kubernetes.default.svc"
  private[spark] val MEMORY_OVERHEAD_FACTOR = 0.10
  private[spark] val MEMORY_OVERHEAD_MIN = 384L

  // V2 submission init container
  private[spark] val INIT_CONTAINER_ANNOTATION = "pod.beta.kubernetes.io/init-containers"
  private[spark] val INIT_CONTAINER_SECRETS_VOLUME_NAME = "dependency-secret"
  private[spark] val INIT_CONTAINER_SECRETS_VOLUME_MOUNT_PATH = "/mnt/secrets/spark-init"
  private[spark] val INIT_CONTAINER_DOWNLOAD_JARS_SECRET_KEY = "downloadJarsSecret"
  private[spark] val INIT_CONTAINER_DOWNLOAD_FILES_SECRET_KEY = "downloadFilesSecret"
  private[spark] val INIT_CONTAINER_TRUSTSTORE_SECRET_KEY = "trustStore"
  private[spark] val INIT_CONTAINER_DOWNLOAD_JARS_SECRET_PATH =
    s"$INIT_CONTAINER_SECRETS_VOLUME_MOUNT_PATH/$INIT_CONTAINER_DOWNLOAD_JARS_SECRET_KEY"
  private[spark] val INIT_CONTAINER_DOWNLOAD_FILES_SECRET_PATH =
    s"$INIT_CONTAINER_SECRETS_VOLUME_MOUNT_PATH/$INIT_CONTAINER_DOWNLOAD_FILES_SECRET_KEY"
  private[spark] val INIT_CONTAINER_TRUSTSTORE_PATH =
    s"$INIT_CONTAINER_SECRETS_VOLUME_MOUNT_PATH/$INIT_CONTAINER_TRUSTSTORE_SECRET_KEY"
  private[spark] val INIT_CONTAINER_DOWNLOAD_CREDENTIALS_PATH =
    "/mnt/secrets/kubernetes-credentials"
  private[spark] val INIT_CONTAINER_CONFIG_MAP_KEY = "init-driver"
  private[spark] val INIT_CONTAINER_PROPERTIES_FILE_VOLUME = "init-container-properties"
  private[spark] val INIT_CONTAINER_PROPERTIES_FILE_MOUNT_PATH = "/etc/spark-init/"
  private[spark] val INIT_CONTAINER_PROPERTIES_FILE_NAME = "init-driver.properties"
  private[spark] val INIT_CONTAINER_PROPERTIES_FILE_PATH =
    s"$INIT_CONTAINER_PROPERTIES_FILE_MOUNT_PATH/$INIT_CONTAINER_PROPERTIES_FILE_NAME"
  private[spark] val DOWNLOAD_JARS_VOLUME_NAME = "download-jars"
  private[spark] val DOWNLOAD_FILES_VOLUME_NAME = "download-files"
  private[spark] val DEFAULT_SHUFFLE_MOUNT_NAME = "shuffle"
}
