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
  val BACKEND_CLOUD = "cloud"
  val BACKEND_CLOUD_URL = "cloud-url"

  val CONFIG_KEY_DEPLOY_MODE = "spark.kubernetes.test.deployMode"
  val CONFIG_KEY_KUBE_CONFIG_CONTEXT = "spark.kubernetes.test.kubeConfigContext"
  val CONFIG_KEY_KUBE_MASTER_URL = "spark.kubernetes.test.master"


  /*
  <spark.kubernetes.test.unpackSparkDir>${project.build.directory}/spark-dist-unpacked</spark.kubernetes.test.unpackSparkDir>
    <spark.kubernetes.test.imageTag>N/A</spark.kubernetes.test.imageTag>
    <spark.kubernetes.test.imageTagFile>${project.build.directory}/imageTag.txt</spark.kubernetes.test.imageTagFile>
    <spark.kubernetes.test.deployMode>minikube</spark.kubernetes.test.deployMode>
    <spark.kubernetes.test.imageRepo>docker.io/kubespark</spark.kubernetes.test.imageRepo>
    <spark.kubernetes.test.kubeConfigContext></spark.kubernetes.test.kubeConfigContext>
    <spark.kubernetes.test.master></spark.kubernetes.test.master>
    <spark.kubernetes.test.namespace></spark.kubernetes.test.namespace>
    <spark.kubernetes.test.serviceAccountName></spark.kubernetes.test.serviceAccountName>
   */
}
