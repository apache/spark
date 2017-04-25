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
package org.apache.spark.deploy.kubernetes.integrationtest

import java.util.UUID

import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube

private[spark] class KubernetesTestComponents {

  val namespace = UUID.randomUUID().toString.replaceAll("-", "")
  val kubernetesClient = Minikube.getKubernetesClient.inNamespace(namespace)
  val clientConfig = kubernetesClient.getConfiguration

  def createNamespace(): Unit = {
    Minikube.getKubernetesClient.namespaces.createNew()
      .withNewMetadata()
      .withName(namespace)
      .endMetadata()
      .done()
  }

  def deleteNamespace(): Unit = {
    Minikube.getKubernetesClient.namespaces.withName(namespace).delete()
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val namespaceList = Minikube.getKubernetesClient
        .namespaces()
        .list()
        .getItems()
        .asScala
      require(!namespaceList.exists(_.getMetadata.getName == namespace))
    }
  }

  def newSparkConf(): SparkConf = {
    new SparkConf(true)
      .setMaster(s"k8s://https://${Minikube.getMinikubeIp}:8443")
      .set(KUBERNETES_SUBMIT_CA_CERT_FILE, clientConfig.getCaCertFile)
      .set(KUBERNETES_SUBMIT_CLIENT_KEY_FILE, clientConfig.getClientKeyFile)
      .set(KUBERNETES_SUBMIT_CLIENT_CERT_FILE, clientConfig.getClientCertFile)
      .set(KUBERNETES_NAMESPACE, namespace)
      .set(DRIVER_DOCKER_IMAGE, "spark-driver:latest")
      .set(EXECUTOR_DOCKER_IMAGE, "spark-executor:latest")
      .setJars(Seq(KubernetesSuite.HELPER_JAR_FILE.getAbsolutePath))
      .set("spark.executor.memory", "500m")
      .set("spark.executor.cores", "1")
      .set("spark.executors.instances", "1")
      .set("spark.app.name", "spark-pi")
      .set("spark.ui.enabled", "true")
      .set("spark.testing", "false")
      .set(WAIT_FOR_APP_COMPLETION, false)
  }
}
