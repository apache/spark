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

import java.io.File
import java.net.URL
import java.nio.file.Files

import scala.collection.JavaConverters._

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder
import org.apache.hadoop.util.VersionInfo
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Span}

import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s.integrationtest.DepsTestsSuite.{DEPS_TIMEOUT, FILE_CONTENTS, HOST_PATH}
import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{INTERVAL, MinikubeTag, TIMEOUT}
import org.apache.spark.deploy.k8s.integrationtest.Utils.getExamplesJarName
import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.Minikube
import org.apache.spark.internal.config.{ARCHIVES, PYSPARK_DRIVER_PYTHON, PYSPARK_PYTHON}

private[spark] trait DepsTestsSuite { k8sSuite: KubernetesSuite =>
  import KubernetesSuite.k8sTestTag

  val cName = "minio"
  val svcName = s"$cName-s3"
  val BUCKET = "spark"
  val ACCESS_KEY = "minio"
  val SECRET_KEY = "miniostorage"

  private def getMinioContainer(): Container = {
    val envVars = Map (
      "MINIO_ACCESS_KEY" -> ACCESS_KEY,
      "MINIO_SECRET_KEY" -> SECRET_KEY
    ).map( envV =>
      new EnvVarBuilder()
        .withName(envV._1)
        .withValue(envV._2)
        .build()
    ).toArray

    val resources = Map(
      "cpu" -> new Quantity("250m"),
      "memory" -> new Quantity("512M")
    ).asJava

    new ContainerBuilder()
      .withImage("minio/minio:latest")
      .withImagePullPolicy("Always")
      .withName(cName)
      .withArgs("server", "/data")
      .withPorts(new ContainerPortBuilder()
          .withName(svcName)
          .withProtocol("TCP")
          .withContainerPort(9000)
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

  private def setupMinioStorage(): Unit = {
    val labels = Map("app" -> "minio").asJava
    val minioService = new ServiceBuilder()
      .withNewMetadata()
        .withName(svcName)
      .withLabels(labels)
      .endMetadata()
      .withNewSpec()
        .withPorts(new ServicePortBuilder()
          .withName("https")
          .withPort(9000)
          .withProtocol("TCP")
          .withTargetPort(new IntOrString(9000))
          .build()
        )
        .withType("NodePort")
        .withSelector(labels)
      .endSpec()
      .build()

    val minioStatefulSet = new StatefulSetBuilder()
      .withNewMetadata()
        .withName(cName)
        .withLabels(labels)
      .endMetadata()
      .withNewSpec()
        .withReplicas(1)
        .withNewSelector()
          .withMatchLabels(Map("app" -> "minio").asJava)
        .endSelector()
        .withServiceName(cName)
        .withNewTemplate()
          .withNewMetadata()
            .withName(cName)
            .withLabels(labels)
           .endMetadata()
          .withNewSpec()
            .withContainers(getMinioContainer())
          .endSpec()
        .endTemplate()
      .endSpec()
      .build()

    // try until the service from a previous test is deleted
    Eventually.eventually(TIMEOUT, INTERVAL) (kubernetesTestComponents
      .kubernetesClient
      .services()
      .create(minioService))

    // try until the stateful set of a previous test is deleted
    Eventually.eventually(TIMEOUT, INTERVAL) (kubernetesTestComponents
      .kubernetesClient
      .apps()
      .statefulSets()
      .create(minioStatefulSet))
  }

  private def deleteMinioStorage(): Unit = {
    kubernetesTestComponents
      .kubernetesClient
      .apps()
      .statefulSets()
      .withName(cName)
      .withGracePeriod(0)
      .delete()

    kubernetesTestComponents
      .kubernetesClient
      .services()
      .withName(svcName)
      .withGracePeriod(0)
      .delete()
  }

  test("Launcher client dependencies", k8sTestTag, MinikubeTag) {
    tryDepsTest({
      val fileName = Utils.createTempFile(FILE_CONTENTS, HOST_PATH)
      sparkAppConf.set("spark.files", s"$HOST_PATH/$fileName")
      val examplesJar = Utils.getTestFileAbsolutePath(getExamplesJarName(), sparkHomeDir)
      runSparkRemoteCheckAndVerifyCompletion(appResource = examplesJar,
        appArgs = Array(fileName),
        timeout = Option(DEPS_TIMEOUT))
    })
  }

  test("SPARK-33615: Launcher client archives", k8sTestTag, MinikubeTag) {
    tryDepsTest {
      val fileName = Utils.createTempFile(FILE_CONTENTS, HOST_PATH)
      Utils.createTarGzFile(s"$HOST_PATH/$fileName", s"$HOST_PATH/$fileName.tar.gz")
      sparkAppConf.set(ARCHIVES.key, s"$HOST_PATH/$fileName.tar.gz#test_tar_gz")
      val examplesJar = Utils.getTestFileAbsolutePath(getExamplesJarName(), sparkHomeDir)
      runSparkRemoteCheckAndVerifyCompletion(appResource = examplesJar,
        appArgs = Array(s"test_tar_gz/$fileName"),
        timeout = Option(DEPS_TIMEOUT))
    }
  }

  test(
    "SPARK-33748: Launcher python client respecting PYSPARK_PYTHON", k8sTestTag, MinikubeTag) {
    val fileName = Utils.createTempFile(
      """
        |#!/usr/bin/env bash
        |export IS_CUSTOM_PYTHON=1
        |python3 "$@"
      """.stripMargin, HOST_PATH)
    Utils.createTarGzFile(s"$HOST_PATH/$fileName", s"$HOST_PATH/$fileName.tgz")
    sparkAppConf.set(ARCHIVES.key, s"$HOST_PATH/$fileName.tgz#test_env")
    val pySparkFiles = Utils.getTestFileAbsolutePath("python_executable_check.py", sparkHomeDir)
    testPython(pySparkFiles,
      Seq(
        s"PYSPARK_PYTHON: ./test_env/$fileName",
        s"PYSPARK_DRIVER_PYTHON: ./test_env/$fileName",
        "Custom Python used on executor: True",
        "Custom Python used on driver: True"),
      env = Map("PYSPARK_PYTHON" -> s"./test_env/$fileName"))
  }

  test(
    "SPARK-33748: Launcher python client respecting " +
      s"${PYSPARK_PYTHON.key} and ${PYSPARK_DRIVER_PYTHON.key}", k8sTestTag, MinikubeTag) {
    val fileName = Utils.createTempFile(
      """
        |#!/usr/bin/env bash
        |export IS_CUSTOM_PYTHON=1
        |python3 "$@"
      """.stripMargin, HOST_PATH)
    Utils.createTarGzFile(s"$HOST_PATH/$fileName", s"$HOST_PATH/$fileName.tgz")
    sparkAppConf.set(ARCHIVES.key, s"$HOST_PATH/$fileName.tgz#test_env")
    sparkAppConf.set(PYSPARK_PYTHON.key, s"./test_env/$fileName")
    sparkAppConf.set(PYSPARK_DRIVER_PYTHON.key, "python3")
    val pySparkFiles = Utils.getTestFileAbsolutePath("python_executable_check.py", sparkHomeDir)
    testPython(pySparkFiles,
      Seq(
        s"PYSPARK_PYTHON: ./test_env/$fileName",
        "PYSPARK_DRIVER_PYTHON: python3",
        "Custom Python used on executor: True",
        "Custom Python used on driver: False"))
  }

  test("Launcher python client dependencies using a zip file", k8sTestTag, MinikubeTag) {
    val pySparkFiles = Utils.getTestFileAbsolutePath("pyfiles.py", sparkHomeDir)
    val inDepsFile = Utils.getTestFileAbsolutePath("py_container_checks.py", sparkHomeDir)
    val outDepsFile = s"${inDepsFile.substring(0, inDepsFile.lastIndexOf("."))}.zip"
    try {
      Utils.createZipFile(inDepsFile, outDepsFile)
      testPython(
        pySparkFiles,
        Seq(
          "Python runtime version check is: True",
          "Python environment version check is: True",
          "Python runtime version check for executor is: True"),
        Some(outDepsFile))
    } finally {
      Files.delete(new File(outDepsFile).toPath)
    }
  }

  private def testPython(
      pySparkFiles: String,
      expectedDriverLogs: Seq[String],
      depsFile: Option[String] = None,
      env: Map[String, String] = Map.empty[String, String]): Unit = {
    tryDepsTest {
      setPythonSparkConfProperties(sparkAppConf)
      runSparkApplicationAndVerifyCompletion(
        appResource = pySparkFiles,
        mainClass = "",
        expectedDriverLogOnCompletion = expectedDriverLogs,
        appArgs = Array("python3"),
        driverPodChecker = doBasicDriverPyPodCheck,
        executorPodChecker = doBasicExecutorPyPodCheck,
        isJVM = false,
        pyFiles = depsFile,
        env = env)
    }
  }

  private def createS3Bucket(accessKey: String, secretKey: String, endPoint: String): Unit = {
    Eventually.eventually(TIMEOUT, INTERVAL) {
      try {
        val credentials = new BasicAWSCredentials(accessKey, secretKey)
        val s3client = new AmazonS3Client(credentials)
        s3client.setEndpoint(endPoint)
        s3client.createBucket(BUCKET)
      } catch {
        case e: Exception =>
          throw new SparkException(s"Failed to create bucket $BUCKET.", e)
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

  private def getServiceHostAndPort(minioUrlStr : String) : (String, Int) = {
    val minioUrl = new URL(minioUrlStr)
    (minioUrl.getHost, minioUrl.getPort)
  }

  private def setCommonSparkConfPropertiesForS3Access(
      conf: SparkAppConf,
      minioUrlStr: String): Unit = {
    val (minioHost, minioPort) = getServiceHostAndPort(minioUrlStr)
    val packages = if (Utils.isHadoop3) {
      s"org.apache.hadoop:hadoop-aws:${VersionInfo.getVersion}"
    } else {
      "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.6"
    }
    conf.set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
      .set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.endpoint", s"$minioHost:$minioPort")
      .set("spark.kubernetes.file.upload.path", s"s3a://$BUCKET")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.jars.packages", packages)
      .set("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
  }

  private def setPythonSparkConfProperties(conf: SparkAppConf): Unit = {
    sparkAppConf.set("spark.kubernetes.container.image", pyImage)
  }

  private def tryDepsTest(runTest: => Unit): Unit = {
    try {
      setupMinioStorage()
      val minioUrlStr = getServiceUrl(svcName)
      createS3Bucket(ACCESS_KEY, SECRET_KEY, minioUrlStr)
      setCommonSparkConfPropertiesForS3Access(sparkAppConf, minioUrlStr)
      runTest
    } finally {
      // make sure this always runs
      deleteMinioStorage()
    }
  }
}

private[spark] object DepsTestsSuite {
  val HOST_PATH = "/tmp"
  val FILE_CONTENTS = "test deps"
  // increase the default because jar resolution takes time in the container
  val DEPS_TIMEOUT = PatienceConfiguration.Timeout(Span(4, Minutes))
}
