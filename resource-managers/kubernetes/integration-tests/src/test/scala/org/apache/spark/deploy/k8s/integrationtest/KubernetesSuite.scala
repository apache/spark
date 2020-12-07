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
import java.nio.file.{Path, Paths}
import java.util.UUID
import java.util.regex.Pattern

import com.google.common.io.PatternFilenameFilter
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Tag}
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.integrationtest.TestConfig._
import org.apache.spark.deploy.k8s.integrationtest.backend.{IntegrationTestBackend, IntegrationTestBackendFactory}
import org.apache.spark.internal.Logging

private[spark] class KubernetesSuite extends SparkFunSuite
  with BeforeAndAfterAll with BeforeAndAfter with BasicTestsSuite with SecretsTestsSuite
  with PythonTestsSuite with ClientModeTestsSuite
  with Logging with Eventually with Matchers {

  import KubernetesSuite._

  private var sparkHomeDir: Path = _
  private var pyImage: String = _
  private var rImage: String = _

  protected var image: String = _
  protected var testBackend: IntegrationTestBackend = _
  protected var driverPodName: String = _
  protected var kubernetesTestComponents: KubernetesTestComponents = _
  protected var sparkAppConf: SparkAppConf = _
  protected var containerLocalSparkDistroExamplesJar: String = _
  protected var appLocator: String = _

  // Default memory limit is 1024M + 384M (minimum overhead constant)
  private val baseMemory = s"${1024 + 384}Mi"
  protected val memOverheadConstant = 0.8
  private val standardNonJVMMemory = s"${(1024 + 0.4*1024).toInt}Mi"
  protected val additionalMemory = 200
  // 209715200 is 200Mi
  protected val additionalMemoryInBytes = 209715200
  private val extraDriverTotalMemory = s"${(1024 + memOverheadConstant*1024).toInt}Mi"
  private val extraExecTotalMemory =
    s"${(1024 + memOverheadConstant*1024 + additionalMemory).toInt}Mi"

  override def beforeAll(): Unit = {
    // The scalatest-maven-plugin gives system properties that are referenced but not set null
    // values. We need to remove the null-value properties before initializing the test backend.
    val nullValueProperties = System.getProperties.asScala
      .filter(entry => entry._2.equals("null"))
      .map(entry => entry._1.toString)
    nullValueProperties.foreach { key =>
      System.clearProperty(key)
    }

    val sparkDirProp = System.getProperty("spark.kubernetes.test.unpackSparkDir")
    require(sparkDirProp != null, "Spark home directory must be provided in system properties.")
    sparkHomeDir = Paths.get(sparkDirProp)
    require(sparkHomeDir.toFile.isDirectory,
      s"No directory found for spark home specified at $sparkHomeDir.")
    val imageTag = getTestImageTag
    val imageRepo = getTestImageRepo
    image = s"$imageRepo/spark:$imageTag"
    pyImage = s"$imageRepo/spark-py:$imageTag"
    rImage = s"$imageRepo/spark-r:$imageTag"

    val sparkDistroExamplesJarFile: File = sparkHomeDir.resolve(Paths.get("examples", "jars"))
      .toFile
      .listFiles(new PatternFilenameFilter(Pattern.compile("^spark-examples_.*\\.jar$")))(0)
    containerLocalSparkDistroExamplesJar = s"local:///opt/spark/examples/jars/" +
      s"${sparkDistroExamplesJarFile.getName}"
    testBackend = IntegrationTestBackendFactory.getTestBackend
    testBackend.initialize()
    kubernetesTestComponents = new KubernetesTestComponents(testBackend.getKubernetesClient)
  }

  override def afterAll(): Unit = {
    testBackend.cleanUp()
  }

  before {
    appLocator = UUID.randomUUID().toString.replaceAll("-", "")
    driverPodName = "spark-test-app-" + UUID.randomUUID().toString.replaceAll("-", "")
    sparkAppConf = kubernetesTestComponents.newSparkAppConf()
      .set("spark.kubernetes.container.image", image)
      .set("spark.kubernetes.driver.pod.name", driverPodName)
      .set("spark.kubernetes.driver.label.spark-app-locator", appLocator)
      .set("spark.kubernetes.driverEnv.HTTP2_DISABLE", "true")
      .set("spark.kubernetes.executor.label.spark-app-locator", appLocator)
    if (!kubernetesTestComponents.hasUserSpecifiedNamespace) {
      kubernetesTestComponents.createNamespace()
    }
  }

  after {
    if (!kubernetesTestComponents.hasUserSpecifiedNamespace) {
      kubernetesTestComponents.deleteNamespace()
    }
    deleteDriverPod()
  }

  protected def runSparkPiAndVerifyCompletion(
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String] = Array.empty[String],
      appLocator: String = appLocator,
      isJVM: Boolean = true ): Unit = {
    runSparkApplicationAndVerifyCompletion(
      appResource,
      SPARK_PI_MAIN_CLASS,
      Seq("Pi is roughly 3"),
      appArgs,
      driverPodChecker,
      executorPodChecker,
      appLocator,
      isJVM)
  }

  protected def runSparkRemoteCheckAndVerifyCompletion(
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String],
      appLocator: String = appLocator): Unit = {
    runSparkApplicationAndVerifyCompletion(
      appResource,
      SPARK_REMOTE_MAIN_CLASS,
      Seq(s"Mounting of ${appArgs.head} was true"),
      appArgs,
      driverPodChecker,
      executorPodChecker,
      appLocator,
      true)
  }

  protected def runSparkJVMCheckAndVerifyCompletion(
      appResource: String = containerLocalSparkDistroExamplesJar,
      mainClass: String = SPARK_DRIVER_MAIN_CLASS,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      appArgs: Array[String] = Array("5"),
      expectedJVMValue: Seq[String]): Unit = {
    val appArguments = SparkAppArguments(
      mainAppResource = appResource,
      mainClass = mainClass,
      appArgs = appArgs)
    SparkAppLauncher.launch(
      appArguments,
      sparkAppConf,
      TIMEOUT.value.toSeconds.toInt,
      sparkHomeDir,
      true)

    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "driver")
      .list()
      .getItems
      .get(0)
    doBasicDriverPodCheck(driverPod)

    Eventually.eventually(TIMEOUT, INTERVAL) {
      expectedJVMValue.foreach { e =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog
          .contains(e), "The application did not complete.")
      }
    }
  }

  protected def runSparkApplicationAndVerifyCompletion(
      appResource: String,
      mainClass: String,
      expectedLogOnCompletion: Seq[String],
      appArgs: Array[String],
      driverPodChecker: Pod => Unit,
      executorPodChecker: Pod => Unit,
      appLocator: String,
      isJVM: Boolean,
      pyFiles: Option[String] = None): Unit = {
    val appArguments = SparkAppArguments(
      mainAppResource = appResource,
      mainClass = mainClass,
      appArgs = appArgs)
    SparkAppLauncher.launch(
      appArguments,
      sparkAppConf,
      TIMEOUT.value.toSeconds.toInt,
      sparkHomeDir,
      isJVM,
      pyFiles)

    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "driver")
      .list()
      .getItems
      .get(0)
    driverPodChecker(driverPod)
    val execPods = scala.collection.mutable.Map[String, Pod]()
    val execWatcher = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "executor")
      .watch(new Watcher[Pod] {
        logInfo("Beginning watch of executors")
        override def onClose(cause: KubernetesClientException): Unit =
          logInfo("Ending watch of executors")
        override def eventReceived(action: Watcher.Action, resource: Pod): Unit = {
          val name = resource.getMetadata.getName
          action match {
            case Action.ADDED | Action.MODIFIED =>
              execPods(name) = resource
            case Action.DELETED | Action.ERROR =>
              execPods.remove(name)
          }
        }
      })
    Eventually.eventually(TIMEOUT, INTERVAL) { execPods.values.nonEmpty should be (true) }
    execWatcher.close()
    execPods.values.foreach(executorPodChecker(_))
    Eventually.eventually(TIMEOUT, INTERVAL) {
      expectedLogOnCompletion.foreach { e =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog
          .contains(e), "The application did not complete.")
      }
    }
  }
  protected def doBasicDriverPodCheck(driverPod: Pod): Unit = {
    assert(driverPod.getMetadata.getName === driverPodName)
    assert(driverPod.getSpec.getContainers.get(0).getImage === image)
    assert(driverPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-driver")
    assert(driverPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === baseMemory)
  }

  protected def doExecutorServiceAccountCheck(executorPod: Pod, account: String): Unit = {
    doBasicExecutorPodCheck(executorPod)
    assert(executorPod.getSpec.getServiceAccount == kubernetesTestComponents.serviceAccountName)
  }

  protected def doBasicDriverPyPodCheck(driverPod: Pod): Unit = {
    assert(driverPod.getMetadata.getName === driverPodName)
    assert(driverPod.getSpec.getContainers.get(0).getImage === pyImage)
    assert(driverPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-driver")
    assert(driverPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === standardNonJVMMemory)
  }

  protected def doBasicDriverRPodCheck(driverPod: Pod): Unit = {
    assert(driverPod.getMetadata.getName === driverPodName)
    assert(driverPod.getSpec.getContainers.get(0).getImage === rImage)
    assert(driverPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-driver")
    assert(driverPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === standardNonJVMMemory)
  }


  protected def doBasicExecutorPodCheck(executorPod: Pod): Unit = {
    assert(executorPod.getSpec.getContainers.get(0).getImage === image)
    assert(executorPod.getSpec.getContainers.get(0).getName === "executor")
    assert(executorPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === baseMemory)
  }

  protected def doBasicExecutorPyPodCheck(executorPod: Pod): Unit = {
    assert(executorPod.getSpec.getContainers.get(0).getImage === pyImage)
    assert(executorPod.getSpec.getContainers.get(0).getName === "executor")
    assert(executorPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === standardNonJVMMemory)
  }

  protected def doBasicExecutorRPodCheck(executorPod: Pod): Unit = {
    assert(executorPod.getSpec.getContainers.get(0).getImage === rImage)
    assert(executorPod.getSpec.getContainers.get(0).getName === "executor")
    assert(executorPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === standardNonJVMMemory)
  }

  protected def doDriverMemoryCheck(driverPod: Pod): Unit = {
    assert(driverPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === extraDriverTotalMemory)
  }

  protected def doExecutorMemoryCheck(executorPod: Pod): Unit = {
    assert(executorPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === extraExecTotalMemory)
  }

  protected def checkCustomSettings(pod: Pod): Unit = {
    assert(pod.getMetadata.getLabels.get("label1") === "label1-value")
    assert(pod.getMetadata.getLabels.get("label2") === "label2-value")
    assert(pod.getMetadata.getAnnotations.get("annotation1") === "annotation1-value")
    assert(pod.getMetadata.getAnnotations.get("annotation2") === "annotation2-value")

    val container = pod.getSpec.getContainers.get(0)
    val envVars = container
      .getEnv
      .asScala
      .map { env =>
        (env.getName, env.getValue)
      }
      .toMap
    assert(envVars("ENV1") === "VALUE1")
    assert(envVars("ENV2") === "VALUE2")
  }

  private def deleteDriverPod(): Unit = {
    kubernetesTestComponents.kubernetesClient.pods().withName(driverPodName).delete()
    Eventually.eventually(TIMEOUT, INTERVAL) {
      assert(kubernetesTestComponents.kubernetesClient
        .pods()
        .withName(driverPodName)
        .get() == null)
    }
  }
}

private[spark] object KubernetesSuite {
  val k8sTestTag = Tag("k8s")
  val SPARK_PI_MAIN_CLASS: String = "org.apache.spark.examples.SparkPi"
  val SPARK_REMOTE_MAIN_CLASS: String = "org.apache.spark.examples.SparkRemoteFileTest"
  val SPARK_DRIVER_MAIN_CLASS: String = "org.apache.spark.examples.DriverSubmissionTest"
  val TIMEOUT = PatienceConfiguration.Timeout(Span(3, Minutes))
  val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
}
