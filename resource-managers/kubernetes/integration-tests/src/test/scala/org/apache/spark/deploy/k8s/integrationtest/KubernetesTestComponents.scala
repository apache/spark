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

import java.nio.file.{Path, Paths}
import java.util.UUID

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.NamespaceBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.integrationtest.TestConstants._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.JARS
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI.UI_ENABLED

private[spark] class KubernetesTestComponents(val kubernetesClient: KubernetesClient) {

  val namespaceOption = Option(System.getProperty(CONFIG_KEY_KUBE_NAMESPACE))
  val hasUserSpecifiedNamespace = namespaceOption.isDefined
  val namespace = namespaceOption.getOrElse("spark-" +
    UUID.randomUUID().toString.replaceAll("-", ""))
  val serviceAccountName =
    Option(System.getProperty(CONFIG_KEY_KUBE_SVC_ACCOUNT))
      .getOrElse("default")
  val clientConfig = kubernetesClient.getConfiguration

  def createNamespace(): Unit = {
    kubernetesClient.namespaces.create(new NamespaceBuilder()
      .withNewMetadata()
      .withName(namespace)
      .endMetadata()
      .build())
  }

  def deleteNamespace(): Unit = {
    kubernetesClient.namespaces.withName(namespace).delete()
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val namespaceList = kubernetesClient
        .namespaces()
        .list()
        .getItems
        .asScala
      require(!namespaceList.exists(_.getMetadata.getName == namespace))
    }
  }

  def newSparkAppConf(): SparkAppConf = {
    new SparkAppConf()
      .set("spark.master", s"k8s://${kubernetesClient.getMasterUrl}")
      .set("spark.kubernetes.namespace", namespace)
      .set("spark.executor.cores", "1")
      .set("spark.executor.instances", "1")
      .set("spark.app.name", "spark-test-app")
      .set(IS_TESTING.key, "false")
      .set(UI_ENABLED.key, "true")
      .set("spark.kubernetes.submission.waitAppCompletion", "false")
      .set("spark.kubernetes.authenticate.driver.serviceAccountName", serviceAccountName)
      .set("spark.kubernetes.driver.request.cores", "0.2")
      .set("spark.kubernetes.executor.request.cores", "0.2")
  }
}

private[spark] class SparkAppConf {

  private val map = mutable.Map[String, String]()

  def set(key: String, value: String): SparkAppConf = {
    map.put(key, value)
    this
  }

  def get(key: String): String = map.getOrElse(key, "")

  def setJars(jars: Seq[String]): Unit = set(JARS.key, jars.mkString(","))

  override def toString: String = map.toString

  def toStringArray: Iterable[String] = map.toList.flatMap(t => List("--conf", s"${t._1}=${t._2}"))

  def toSparkConf: SparkConf = new SparkConf().setAll(map)
}

private[spark] case class SparkAppArguments(
    mainAppResource: String,
    mainClass: String,
    appArgs: Array[String])

private[spark] object SparkAppLauncher extends Logging {
  def launch(
      appArguments: SparkAppArguments,
      appConf: SparkAppConf,
      timeoutSecs: Int,
      sparkHomeDir: Path,
      isJVM: Boolean,
      pyFiles: Option[String] = None,
      env: Map[String, String] = Map.empty[String, String]): Unit = {
    val sparkSubmitExecutable = sparkHomeDir.resolve(Paths.get("bin", "spark-submit"))
    logInfo(s"Launching a spark app with arguments $appArguments and conf $appConf")
    val preCommandLine = if (isJVM) {
      mutable.ArrayBuffer(sparkSubmitExecutable.toFile.getAbsolutePath,
      "--deploy-mode", "cluster",
      "--class", appArguments.mainClass,
      "--master", appConf.get("spark.master"))
    } else {
      mutable.ArrayBuffer(sparkSubmitExecutable.toFile.getAbsolutePath,
        "--deploy-mode", "cluster",
        "--master", appConf.get("spark.master"))
    }
    val commandLine =
      pyFiles.map(s => preCommandLine ++ Array("--py-files", s)).getOrElse(preCommandLine) ++
        appConf.toStringArray :+ appArguments.mainAppResource

    if (appArguments.appArgs.nonEmpty) {
      commandLine ++= appArguments.appArgs
    }
    logInfo(s"Launching a spark app with command line: ${commandLine.mkString(" ")}")
    ProcessUtils.executeProcess(commandLine.toArray, timeoutSecs, env = env)
  }
}
