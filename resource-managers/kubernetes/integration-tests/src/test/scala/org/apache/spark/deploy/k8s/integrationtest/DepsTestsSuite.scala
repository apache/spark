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

import java.net.URL

import scala.collection.JavaConverters._

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Span}

import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s.integrationtest.DepsTestsSuite.{DEPS_TIMEOUT, FILE_CONTENTS, HOST_PATH}
import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{INTERVAL, MinikubeTag, TIMEOUT}
import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.Minikube

private[spark] trait DepsTestsSuite { k8sSuite: KubernetesSuite =>
  import KubernetesSuite.k8sTestTag

  val cName = "ceph-nano"
  val svcName = s"$cName-s3"
  val bucket = "spark"

  private def getCephContainer(): Container = {
    val envVars = Map ( "NETWORK_AUTO_DETECT" -> "4",
      "RGW_FRONTEND_PORT" -> "8000",
      "SREE_PORT" -> "5001",
      "CEPH_DEMO_UID" -> "nano",
      "CEPH_DAEMON" -> "demo",
      "DEBUG" -> "verbose"
    ).map( envV =>
      new EnvVarBuilder()
        .withName(envV._1)
        .withValue(envV._2)
        .build()
    ).toArray

    val resources = Map(
      "cpu" -> new QuantityBuilder()
        .withAmount("1")
        .build(),
      "memory" -> new QuantityBuilder()
        .withAmount("512M")
        .build()
    ).asJava

    new ContainerBuilder()
      .withImage("ceph/daemon:latest")
      .withImagePullPolicy("Always")
      .withName(cName)
      .withPorts(new ContainerPortBuilder()
          .withName(svcName)
          .withProtocol("TCP")
          .withContainerPort(8000)
        .build()
      )
      .withResources(new ResourceRequirementsBuilder()
        .withLimits(resources)
        .withRequests(resources)
        .build()
      )
      .withEnv(envVars: _*)
      .build()
  }

  // Based on https://github.com/ceph/cn
  private def setupCephStorage(): Unit = {
    val labels = Map("app" -> "ceph", "daemon" -> "nano").asJava
    val cephService = new ServiceBuilder()
      .withNewMetadata()
        .withName(svcName)
      .withLabels(labels)
      .endMetadata()
      .withNewSpec()
        .withPorts(new ServicePortBuilder()
          .withName("https")
          .withPort(8000)
          .withProtocol("TCP")
          .withTargetPort(new IntOrString(8000))
          .build()
        )
        .withType("NodePort")
        .withSelector(labels)
      .endSpec()
      .build()

    val cephStatefulSet = new StatefulSetBuilder()
      .withNewMetadata()
        .withName(cName)
        .withLabels(labels)
      .endMetadata()
      .withNewSpec()
        .withReplicas(1)
        .withNewSelector()
          .withMatchLabels(Map("app" -> "ceph").asJava)
        .endSelector()
        .withServiceName(cName)
        .withNewTemplate()
          .withNewMetadata()
            .withName(cName)
            .withLabels(labels)
           .endMetadata()
          .withNewSpec()
            .withContainers(getCephContainer())
          .endSpec()
        .endTemplate()
      .endSpec()
      .build()

    kubernetesTestComponents
      .kubernetesClient
      .services()
      .create(cephService)

    kubernetesTestComponents
      .kubernetesClient
      .apps()
      .statefulSets()
      .create(cephStatefulSet)
  }

 private def deleteCephStorage(): Unit = {
    kubernetesTestComponents
      .kubernetesClient
      .apps()
      .statefulSets()
      .withName(cName)
      .delete()

    kubernetesTestComponents
      .kubernetesClient
      .services()
      .withName(svcName)
      .delete()
  }

  test("Launcher client dependencies", k8sTestTag, MinikubeTag) {
    val fileName = Utils.createTempFile(FILE_CONTENTS, HOST_PATH)
    try {
      setupCephStorage()
      val cephUrlStr = getServiceUrl(svcName)
      val cephUrl = new URL(cephUrlStr)
      val cephHost = cephUrl.getHost
      val cephPort = cephUrl.getPort
      val examplesJar = Utils.getExamplesJarAbsolutePath(sparkHomeDir)
      val (accessKey, secretKey) = getCephCredentials()
      sparkAppConf
        .set("spark.hadoop.fs.s3a.access.key", accessKey)
        .set("spark.hadoop.fs.s3a.secret.key", secretKey)
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .set("spark.hadoop.fs.s3a.endpoint", s"$cephHost:$cephPort")
        .set("spark.kubernetes.file.upload.path", s"s3a://$bucket")
        .set("spark.files", s"$HOST_PATH/$fileName")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.jars.packages", "com.amazonaws:aws-java-sdk:" +
          "1.7.4,org.apache.hadoop:hadoop-aws:2.7.6")
        .set("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
      createS3Bucket(accessKey, secretKey, cephUrlStr)
      runSparkRemoteCheckAndVerifyCompletion(appResource = examplesJar,
        appArgs = Array(fileName),
        timeout = Option(DEPS_TIMEOUT))
    } finally {
      // make sure this always runs
      deleteCephStorage()
    }
  }

  // There isn't a cleaner way to get the credentials
  // when ceph-nano runs on k8s
  private def getCephCredentials(): (String, String) = {
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val cephPod = kubernetesTestComponents
        .kubernetesClient
        .pods()
        .withName(s"$cName-0")
        .get()
      implicit val podName: String = cephPod.getMetadata.getName
      implicit val components: KubernetesTestComponents = kubernetesTestComponents
      val contents = Utils.executeCommand("cat", "/nano_user_details")
    (extractS3Key(contents, "access_key"), extractS3Key(contents, "secret_key"))
    }
  }

  private def extractS3Key(data: String, key: String): String = {
    data.split("\n")
      .filter(_.contains(key))
      .head
      .split(":")
      .last
      .trim
      .replaceAll("[,|\"]", "")
  }

  private def createS3Bucket(accessKey: String, secretKey: String, endPoint: String): Unit = {
    Eventually.eventually(TIMEOUT, INTERVAL) {
      try {
        val credentials = new BasicAWSCredentials(accessKey, secretKey)
        val s3client = new AmazonS3Client(credentials)
        s3client.setEndpoint(endPoint)
        s3client.createBucket(bucket)
      } catch {
        case e: Exception =>
          throw new SparkException(s"Failed to create bucket $bucket.", e)
      }
    }
  }

  private def getServiceUrl(serviceName: String): String = {
    val fuzzyUrlMatcher = """^(.*?)([a-zA-Z]+://.*?)(\s*)$""".r
    Eventually.eventually(TIMEOUT, INTERVAL) {
      // ns is always available either random or provided by the user
      val rawUrl = Minikube.minikubeServiceAction(
        serviceName, "-n", kubernetesTestComponents.namespace, "--url")
      val url = rawUrl match {
        case fuzzyUrlMatcher(junk, url, extra) =>
          logDebug(s"Service url matched junk ${junk} - url ${url} - extra ${extra}")
          url
        case _ =>
          logWarning(s"Response from minikube ${rawUrl} did not match URL regex")
          rawUrl
      }
      url
    }
  }
}

private[spark] object DepsTestsSuite {
  val HOST_PATH = "/tmp"
  val FILE_CONTENTS = "test deps"
  // increase the default because jar resolution takes time in the container
  val DEPS_TIMEOUT = PatienceConfiguration.Timeout(Span(4, Minutes))
}
