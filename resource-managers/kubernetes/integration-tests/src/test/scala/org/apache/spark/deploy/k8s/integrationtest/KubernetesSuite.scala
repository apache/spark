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

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Tag}
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}

import org.apache.spark.{SPARK_VERSION, SparkFunSuite}
import org.apache.spark.deploy.k8s.integrationtest.TestConstants._
import org.apache.spark.deploy.k8s.integrationtest.backend.{IntegrationTestBackend, IntegrationTestBackendFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

class KubernetesSuite extends SparkFunSuite
    with BeforeAndAfterAll with BeforeAndAfter
    with BasicTestsSuite
    with DecommissionSuite
    with SecretsTestsSuite
    with PythonTestsSuite with ClientModeTestsSuite with PodTemplateSuite
  with Logging with Eventually with Matchers {

  import KubernetesSuite._

  protected var sparkHomeDir: Path = _
  protected var pyImage: String = _
  protected var rImage: String = _

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

  /**
   * Build the image ref for the given image name, taking the repo and tag from the
   * test configuration.
   */
  private def testImageRef(name: String): String = {
    val tag = sys.props.get(CONFIG_KEY_IMAGE_TAG_FILE)
      .map { path =>
        val tagFile = new File(path)
        require(tagFile.isFile,
          s"No file found for image tag at ${tagFile.getAbsolutePath}.")
        Files.toString(tagFile, Charsets.UTF_8).trim
      }
      .orElse(sys.props.get(CONFIG_KEY_IMAGE_TAG))
      .getOrElse {
        throw new IllegalArgumentException(
          s"One of $CONFIG_KEY_IMAGE_TAG_FILE or $CONFIG_KEY_IMAGE_TAG is required.")
      }
    val repo = sys.props.get(CONFIG_KEY_IMAGE_REPO)
      .map { _ + "/" }
      .getOrElse("")

    s"$repo$name:$tag"
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // The scalatest-maven-plugin gives system properties that are referenced but not set null
    // values. We need to remove the null-value properties before initializing the test backend.
    val nullValueProperties = System.getProperties.asScala
      .filter(entry => entry._2.equals("null"))
      .map(entry => entry._1.toString)
    nullValueProperties.foreach { key =>
      System.clearProperty(key)
    }

    val possible_spark_dirs = List(
      // If someone specified the tgz for the tests look at the extraction dir
      System.getProperty(CONFIG_KEY_UNPACK_DIR),
      // If otherwise use my working dir + 3 up
      new File(Paths.get(System.getProperty("user.dir")).toFile, ("../" * 3)).getAbsolutePath()
    )
    val sparkDirProp = possible_spark_dirs.filter(x =>
      new File(Paths.get(x).toFile, "bin/spark-submit").exists).headOption.getOrElse(null)
    require(sparkDirProp != null,
      s"Spark home directory must be provided in system properties tested $possible_spark_dirs")
    sparkHomeDir = Paths.get(sparkDirProp)
    require(sparkHomeDir.toFile.isDirectory,
      s"No directory found for spark home specified at $sparkHomeDir.")
    image = testImageRef("spark")
    pyImage = testImageRef("spark-py")
    rImage = testImageRef("spark-r")

    val scalaVersion = scala.util.Properties.versionNumberString
      .split("\\.")
      .take(2)
      .mkString(".")
    containerLocalSparkDistroExamplesJar =
      s"local:///opt/spark/examples/jars/spark-examples_$scalaVersion-${SPARK_VERSION}.jar"
    testBackend = IntegrationTestBackendFactory.getTestBackend
    testBackend.initialize()
    kubernetesTestComponents = new KubernetesTestComponents(testBackend.getKubernetesClient)
  }

  override def afterAll(): Unit = {
    try {
      testBackend.cleanUp()
    } finally {
      super.afterAll()
    }
  }

  before {
    appLocator = UUID.randomUUID().toString.replaceAll("-", "")
    driverPodName = "spark-test-app-" + UUID.randomUUID().toString.replaceAll("-", "")
    sparkAppConf = kubernetesTestComponents.newSparkAppConf()
      .set("spark.kubernetes.container.image", image)
      .set("spark.kubernetes.driver.pod.name", driverPodName)
      .set("spark.kubernetes.driver.label.spark-app-locator", appLocator)
      .set("spark.kubernetes.executor.label.spark-app-locator", appLocator)
      .set(NETWORK_AUTH_ENABLED.key, "true")
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
      pyFiles: Option[String] = None,
      decomissioningTest: Boolean = false): Unit = {
    val appArguments = SparkAppArguments(
      mainAppResource = appResource,
      mainClass = mainClass,
      appArgs = appArgs)

    val execPods = scala.collection.mutable.Map[String, Pod]()
    def checkPodReady(namespace: String, name: String) = {
      println(s"!!! doing ready check on pod $name")
      val execPod = kubernetesTestComponents.kubernetesClient
        .pods()
        .inNamespace(namespace)
        .withName(name)
        .get()
      println(s"!!! god pod $execPod for $name")
      val resourceStatus = execPod.getStatus
      println(s"!!! status $resourceStatus for $name")
      val conditions = resourceStatus.getConditions().asScala
      println(s"!!! conditions $conditions for $name")
      val result = conditions
        .map(cond => cond.getStatus() == "True" && cond.getType() == "Ready")
        .headOption.getOrElse(false)
      println(s"Pod name ${name} with entry ${execPod} has status" +
        s"${resourceStatus} with conditions ${conditions} result: ${result}")
      result
    }
    println("Creating watcher...")
    val execWatcher = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "executor")
      .watch(new Watcher[Pod] {
        logInfo("Beginning watch of executors")
        override def onClose(cause: KubernetesClientException): Unit =
          logInfo("Ending watch of executors")
        override def eventReceived(action: Watcher.Action, resource: Pod): Unit = {
          println("Event received.")
          val name = resource.getMetadata.getName
          val namespace = resource.getMetadata().getNamespace()
          action match {
            case Action.ADDED | Action.MODIFIED =>
              println(s"Add or modification event received for $name.")
              execPods(name) = resource
              // If testing decomissioning delete the node 5 seconds after it starts running
              // Open question: could we put this in the checker
              if (decomissioningTest) {
                // Wait for all the containers in the pod to be running
                println("Waiting for pod to become OK then delete.")
                Eventually.eventually(POD_RUNNING_TIMEOUT, INTERVAL) {
                  val result = checkPodReady(namespace, name)
                  result shouldBe (true)
                }
                // Sleep a small interval to allow execution & downstream pod ready check to also catch up
                println("Sleeping before killing pod.")
                Thread.sleep(100)
                // Delete the pod to simulate cluster scale down/migration.
                val pod = kubernetesTestComponents.kubernetesClient.pods().withName(name)
                pod.delete()
                println(s"Pod: $name deleted")
              } else {
                println(s"Resource $name added")
              }
            case Action.DELETED | Action.ERROR =>
              println("Deleted or error event received.")
              execPods.remove(name)
              println("Resrouce $name removed")
          }
        }
      })

    println("Running spark job.")
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
    println("Doing driver pod check")
    driverPodChecker(driverPod)
    println("Done driver pod check")
    // If we're testing decomissioning we delete all the executors, but we should have
    // an executor at some point.
    Eventually.eventually(POD_RUNNING_TIMEOUT, INTERVAL) {
      println(s"Driver podcheck iteration non empty: ${execPods.values.nonEmpty} with ${execPods}")
      execPods.values.nonEmpty should be (true)
    }
    // If decomissioning we need to wait and check the executors were removed
    if (decomissioningTest) {
      // Sleep a small interval to ensure everything is registered.
      Thread.sleep(100)
      // Wait for the executors to become ready
      Eventually.eventually(POD_RUNNING_TIMEOUT, INTERVAL) {
        val anyReadyPods = ! execPods.map{
          case (name, resource) =>
            (name, resource.getMetadata().getNamespace())
        }.filter{
          case (name, namespace) => checkPodReady(namespace, name)
        }.isEmpty
        val podsEmpty = execPods.values.isEmpty
        val podsReadyOrDead = anyReadyPods || podsEmpty
        podsReadyOrDead shouldBe (true)
      }
      // Sleep a small interval to allow execution
      Thread.sleep(3000)
      // Wait for the executors to be removed
      Eventually.eventually(TIMEOUT, INTERVAL) {
        println(s"decom iteration pods non-empty ${execPods.values.nonEmpty} with ${execPods}")
        execPods.values.nonEmpty should be (false)
      }
    }
    println(s"Closing watcher with execPods $execPods nonEmpty: ${execPods.values.nonEmpty}")
    execWatcher.close()
    execPods.values.foreach(executorPodChecker(_))
    println(s"Close to the end exec pods are $execPods")
    Eventually.eventually(TIMEOUT, INTERVAL) {
      expectedLogOnCompletion.foreach { e =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog
          .contains(e), "The application did not complete.")
      }
    }
    println(s"end exec pods are $execPods")
  }
  protected def doBasicDriverPodCheck(driverPod: Pod): Unit = {
    assert(driverPod.getMetadata.getName === driverPodName)
    assert(driverPod.getSpec.getContainers.get(0).getImage === image)
    assert(driverPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-driver")
    assert(driverPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === baseMemory)
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
    assert(executorPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-executor")
    assert(executorPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === baseMemory)
  }

  protected def doBasicExecutorPyPodCheck(executorPod: Pod): Unit = {
    assert(executorPod.getSpec.getContainers.get(0).getImage === pyImage)
    assert(executorPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-executor")
    assert(executorPod.getSpec.getContainers.get(0).getResources.getRequests.get("memory").getAmount
      === standardNonJVMMemory)
  }

  protected def doBasicExecutorRPodCheck(executorPod: Pod): Unit = {
    assert(executorPod.getSpec.getContainers.get(0).getImage === rImage)
    assert(executorPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-executor")
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
  val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  val POD_RUNNING_TIMEOUT = PatienceConfiguration.Timeout(Span(5, Minutes))
  val INTERVAL = PatienceConfiguration.Interval(Span(1, Seconds))
}
