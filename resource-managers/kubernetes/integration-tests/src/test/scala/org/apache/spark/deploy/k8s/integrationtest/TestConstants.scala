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
package org.apache.spark.deploy.k8s.integrationtest

object TestConstants {
  val BACKEND_MINIKUBE = "minikube"
  val BACKEND_DOCKER_FOR_DESKTOP = "docker-for-desktop"
  val BACKEND_DOCKER_DESKTOP = "docker-desktop"
  val BACKEND_CLOUD = "cloud"

  val CONFIG_KEY_DEPLOY_MODE = "spark.kubernetes.test.deployMode"
  val CONFIG_KEY_KUBE_CONFIG_CONTEXT = "spark.kubernetes.test.kubeConfigContext"
  val CONFIG_KEY_KUBE_MASTER_URL = "spark.kubernetes.test.master"
  val CONFIG_KEY_KUBE_NAMESPACE = "spark.kubernetes.test.namespace"
  val CONFIG_KEY_KUBE_SVC_ACCOUNT = "spark.kubernetes.test.serviceAccountName"
  val CONFIG_KEY_IMAGE_JVM = "spark.kubernetes.test.jvmImage"
  val CONFIG_KEY_IMAGE_PYTHON = "spark.kubernetes.test.pythonImage"
  val CONFIG_KEY_IMAGE_R = "spark.kubernetes.test.rImage"
  val CONFIG_KEY_IMAGE_TAG = "spark.kubernetes.test.imageTag"
  val CONFIG_KEY_IMAGE_TAG_FILE = "spark.kubernetes.test.imageTagFile"
  val CONFIG_KEY_IMAGE_REPO = "spark.kubernetes.test.imageRepo"
  val CONFIG_KEY_UNPACK_DIR = "spark.kubernetes.test.unpackSparkDir"
}
