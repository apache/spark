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
import javax.net.ssl.X509TrustManager

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.fabric8.kubernetes.client.DefaultKubernetesClient
import io.fabric8.kubernetes.client.internal.SSLUtils
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.rest.kubernetes.v1.HttpClientUtil

private[spark] class KubernetesTestComponents(defaultClient: DefaultKubernetesClient) {

  val namespace = UUID.randomUUID().toString.replaceAll("-", "")
  val kubernetesClient = defaultClient.inNamespace(namespace)
  val clientConfig = kubernetesClient.getConfiguration

  def createNamespace(): Unit = {
    defaultClient.namespaces.createNew()
      .withNewMetadata()
      .withName(namespace)
      .endMetadata()
      .done()
  }

  def deleteNamespace(): Unit = {
    defaultClient.namespaces.withName(namespace).delete()
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val namespaceList = defaultClient
        .namespaces()
        .list()
        .getItems()
        .asScala
      require(!namespaceList.exists(_.getMetadata.getName == namespace))
    }
  }

  def newSparkConf(): SparkConf = {
    new SparkConf(true)
      .setMaster(s"k8s://${kubernetesClient.getMasterUrl}")
      .set(KUBERNETES_NAMESPACE, namespace)
      .set(DRIVER_DOCKER_IMAGE,
        System.getProperty("spark.docker.test.driverImage", "spark-driver:latest"))
      .set(EXECUTOR_DOCKER_IMAGE,
        System.getProperty("spark.docker.test.executorImage", "spark-executor:latest"))
      .setJars(Seq(KubernetesSuite.HELPER_JAR_FILE.getAbsolutePath))
      .set("spark.executor.memory", "500m")
      .set("spark.executor.cores", "1")
      .set("spark.executors.instances", "1")
      .set("spark.app.name", "spark-pi")
      .set("spark.ui.enabled", "true")
      .set("spark.testing", "false")
      .set(WAIT_FOR_APP_COMPLETION, false)
  }

  def getService[T: ClassTag](
    serviceName: String,
    namespace: String,
    servicePortName: String,
    servicePath: String = ""): T = synchronized {
    val kubernetesMaster = s"${defaultClient.getMasterUrl}"

    val url = s"${
      Array[String](
        s"${kubernetesClient.getMasterUrl}",
        "api", "v1", "proxy",
        "namespaces", namespace,
        "services", serviceName).mkString("/")
    }" +
      s":$servicePortName$servicePath"
    val userHome = System.getProperty("user.home")
    val kubernetesConf = kubernetesClient.getConfiguration
    val sslContext = SSLUtils.sslContext(kubernetesConf)
    val trustManager = SSLUtils.trustManagers(kubernetesConf)(0).asInstanceOf[X509TrustManager]
    HttpClientUtil.createClient[T](Set(url), 5, sslContext.getSocketFactory, trustManager)
  }
}