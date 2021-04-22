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
import io.fabric8.kubernetes.client.{Watcher, WatcherException}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Tag}
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Minutes, Seconds, Span}

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.integrationtest.TestConstants._
import org.apache.spark.deploy.k8s.integrationtest.backend.{IntegrationTestBackend, IntegrationTestBackendFactory}
import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.Minikube
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

class KubernetesSuite extends SparkFunSuite
  with BeforeAndAfterAll with BeforeAndAfter with BasicTestsSuite with SparkConfPropagateSuite
  with SecretsTestsSuite with PythonTestsSuite with ClientModeTestsSuite with PodTemplateSuite
  with PVTestsSuite with DepsTestsSuite with DecommissionSuite with RTestsSuite with Logging
  with Eventually with Matchers {


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
  private val baseMemory = s"${1024 + 384}"
  protected val memOverheadConstant = 0.8
  private val standardNonJVMMemory = s"${(1024 + 0.4*1024).toInt}"
  protected val additionalMemory = 200
  // 209715200 is 200Mi
  protected val additionalMemoryInBytes = 209715200
  private val extraDriverTotalMemory = s"${(1024 + memOverheadConstant*1024).toInt}"
  private val extraExecTotalMemory =
    s"${(1024 + memOverheadConstant*1024 + additionalMemory).toInt}"

  protected override def logForFailedTest(): Unit = {
    logInfo("\n\n===== EXTRA LOGS FOR THE FAILED TEST\n")
    logInfo("BEGIN DESCRIBE PODS for application\n" +
      Minikube.describePods(s"spark-app-locator=$appLocator").mkString("\n"))
    logInfo("END DESCRIBE PODS for the application")
    val driverPodOption = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "driver")
      .list()
      .getItems
      .asScala
      .headOption
    driverPodOption.foreach { driverPod =>
      logInfo("BEGIN driver POD log\n" +
        kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog)
      logInfo("END driver POD log")
    }
    kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "executor")
      .list()
      .getItems.asScala.foreach { execPod =>
        logInfo(s"\nBEGIN executor (${execPod.getMetadata.getName}) POD log:\n" +
          kubernetesTestComponents.kubernetesClient
            .pods()
            .withName(execPod.getMetadata.getName)
            .getLog)
        logInfo(s"END executor (${execPod.getMetadata.getName}) POD log")
      }
  }

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
      // Try the spark test home
      sys.props("spark.test.home")
    )
    val sparkDirProp = possible_spark_dirs.filter(x =>
      new File(Paths.get(x).toFile, "bin/spark-submit").exists).headOption.getOrElse(null)
    require(sparkDirProp != null,
      s"Spark home directory must be provided in system properties tested $possible_spark_dirs")
    sparkHomeDir = Paths.get(sparkDirProp)
    require(sparkHomeDir.toFile.isDirectory,
      s"No directory found for spark home specified at $sparkHomeDir.")
    image = testImageRef(sys.props.getOrElse(CONFIG_KEY_IMAGE_JVM, "spark"))
    pyImage = testImageRef(sys.props.getOrElse(CONFIG_KEY_IMAGE_PYTHON, "spark-py"))
    rImage = testImageRef(sys.props.getOrElse(CONFIG_KEY_IMAGE_R, "spark-r"))

    containerLocalSparkDistroExamplesJar =
      s"local:///opt/spark/examples/jars/${Utils.getExamplesJarName()}"
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
    deleteExecutorPod()
  }

  protected def runSparkPiAndVerifyCompletion(
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String] = Array.empty[String],
      isJVM: Boolean = true ): Unit = {
    runSparkApplicationAndVerifyCompletion(
      appResource,
      SPARK_PI_MAIN_CLASS,
      Seq("Pi is roughly 3"),
      Seq(),
      appArgs,
      driverPodChecker,
      executorPodChecker,
      isJVM)
  }

  protected def runDFSReadWriteAndVerifyCompletion(
      wordCount: Int,
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String] = Array.empty[String],
      isJVM: Boolean = true,
      interval: Option[PatienceConfiguration.Interval] = None): Unit = {
    runSparkApplicationAndVerifyCompletion(
      appResource,
      SPARK_DFS_READ_WRITE_TEST,
      Seq(s"Success! Local Word Count $wordCount and " +
    s"DFS Word Count $wordCount agree."),
      Seq(),
      appArgs,
      driverPodChecker,
      executorPodChecker,
      isJVM,
      None,
      Option((interval, None)))
  }

  protected def runSparkRemoteCheckAndVerifyCompletion(
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String],
      timeout: Option[PatienceConfiguration.Timeout] = None): Unit = {
    runSparkApplicationAndVerifyCompletion(
      appResource,
      SPARK_REMOTE_MAIN_CLASS,
      Seq(s"Mounting of ${appArgs.head} was true"),
      Seq(),
      appArgs,
      driverPodChecker,
      executorPodChecker,
      true,
      executorPatience = Option((None, timeout)))
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

  // scalastyle:off argcount
  protected def runSparkApplicationAndVerifyCompletion(
      appResource: String,
      mainClass: String,
      expectedDriverLogOnCompletion: Seq[String],
      expectedExecutorLogOnCompletion: Seq[String] = Seq(),
      appArgs: Array[String],
      driverPodChecker: Pod => Unit,
      executorPodChecker: Pod => Unit,
      isJVM: Boolean,
      pyFiles: Option[String] = None,
      executorPatience: Option[(Option[Interval], Option[Timeout])] = None,
      decommissioningTest: Boolean = false,
      env: Map[String, String] = Map.empty[String, String]): Unit = {

  // scalastyle:on argcount
    val appArguments = SparkAppArguments(
      mainAppResource = appResource,
      mainClass = mainClass,
      appArgs = appArgs)

    val execPods = scala.collection.mutable.Map[String, Pod]()
    val podsDeleted = scala.collection.mutable.HashSet[String]()
    val (patienceInterval, patienceTimeout) = {
      executorPatience match {
        case Some(patience) => (patience._1.getOrElse(INTERVAL), patience._2.getOrElse(TIMEOUT))
        case _ => (INTERVAL, TIMEOUT)
      }
    }
    def checkPodReady(namespace: String, name: String) = {
      val execPod = kubernetesTestComponents.kubernetesClient
        .pods()
        .inNamespace(namespace)
        .withName(name)
        .get()
      val resourceStatus = execPod.getStatus
      val conditions = resourceStatus.getConditions().asScala
      val conditionTypes = conditions.map(_.getType())
      val readyConditions = conditions.filter{cond => cond.getType() == "Ready"}
      val result = readyConditions
        .map(cond => cond.getStatus() == "True")
        .headOption.getOrElse(false)
      result
    }

    val execWatcher = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "executor")
      .watch(new Watcher[Pod] {
        logDebug("Beginning watch of executors")
        override def onClose(): Unit = logInfo("Ending watch of executors")
        override def onClose(cause: WatcherException): Unit =
          logInfo("Ending watch of executors")
        override def eventReceived(action: Watcher.Action, resource: Pod): Unit = {
          val name = resource.getMetadata.getName
          val namespace = resource.getMetadata().getNamespace()
          action match {
            case Action.MODIFIED =>
              execPods(name) = resource
            case Action.ADDED =>
              logDebug(s"Add event received for $name.")
              execPods(name) = resource
              // If testing decommissioning start a thread to simulate
              // decommissioning on the first exec pod.
              if (decommissioningTest && execPods.size == 1) {
                // Wait for all the containers in the pod to be running
                logDebug("Waiting for pod to become OK prior to deletion")
                Eventually.eventually(patienceTimeout, patienceInterval) {
                  val result = checkPodReady(namespace, name)
                  result shouldBe (true)
                }
                // Look for the string that indicates we're good to trigger decom on the driver
                logDebug("Waiting for first collect...")
                Eventually.eventually(TIMEOUT, INTERVAL) {
                  assert(kubernetesTestComponents.kubernetesClient
                    .pods()
                    .withName(driverPodName)
                    .getLog
                    .contains("Waiting to give nodes time to finish migration, decom exec 1."),
                    "Decommission test did not complete first collect.")
                }
                // Delete the pod to simulate cluster scale down/migration.
                // This will allow the pod to remain up for the grace period
                // We set an intentionally long grace period to test that Spark
                // exits once the blocks are done migrating and doesn't wait for the
                // entire grace period if it does not need to.
                kubernetesTestComponents.kubernetesClient.pods()
                  .withName(name).withGracePeriod(Int.MaxValue).delete()
                logDebug(s"Triggered pod decom/delete: $name deleted")
                // Make sure this pod is deleted
                Eventually.eventually(TIMEOUT, INTERVAL) {
                  assert(podsDeleted.contains(name))
                }
                // Then make sure this pod is replaced
                Eventually.eventually(TIMEOUT, INTERVAL) {
                  assert(execPods.size == 3)
                }
              }
            case Action.DELETED | Action.ERROR =>
              execPods.remove(name)
              podsDeleted += name
          }
        }
      })

    logDebug("Starting Spark K8s job")
    SparkAppLauncher.launch(
      appArguments,
      sparkAppConf,
      TIMEOUT.value.toSeconds.toInt,
      sparkHomeDir,
      isJVM,
      pyFiles,
      env)

    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "driver")
      .list()
      .getItems
      .get(0)
    driverPodChecker(driverPod)

    // If we're testing decommissioning we an executors, but we should have an executor
    // at some point.
    Eventually.eventually(TIMEOUT, patienceInterval) {
      execPods.values.nonEmpty should be (true)
    }
    execPods.values.foreach(executorPodChecker(_))

    val execPod: Option[Pod] = if (expectedExecutorLogOnCompletion.nonEmpty) {
      Some(kubernetesTestComponents.kubernetesClient
        .pods()
        .withLabel("spark-app-locator", appLocator)
        .withLabel("spark-role", "executor")
        .list()
        .getItems
        .get(0))
    } else {
      None
    }

    Eventually.eventually(patienceTimeout, patienceInterval) {
      expectedDriverLogOnCompletion.foreach { e =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog
          .contains(e),
          s"The application did not complete, driver log did not contain str ${e}")
      }
      expectedExecutorLogOnCompletion.foreach { e =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(execPod.get.getMetadata.getName)
          .getLog
          .contains(e),
          s"The application did not complete, executor log did not contain str ${e}")
      }
    }
    execWatcher.close()
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

  private def deleteExecutorPod(): Unit = {
    kubernetesTestComponents
      .kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "executor")
      .delete()
    Eventually.eventually(TIMEOUT, INTERVAL) {
      assert(kubernetesTestComponents.kubernetesClient
        .pods()
        .withLabel("spark-app-locator", appLocator)
        .withLabel("spark-role", "executor")
        .list()
        .getItems.isEmpty)
    }
  }
}

private[spark] object KubernetesSuite {
  val k8sTestTag = Tag("k8s")
  val rTestTag = Tag("r")
  val MinikubeTag = Tag("minikube")
  val SPARK_PI_MAIN_CLASS: String = "org.apache.spark.examples.SparkPi"
  val SPARK_DFS_READ_WRITE_TEST = "org.apache.spark.examples.DFSReadWriteTest"
  val SPARK_REMOTE_MAIN_CLASS: String = "org.apache.spark.examples.SparkRemoteFileTest"
  val SPARK_DRIVER_MAIN_CLASS: String = "org.apache.spark.examples.DriverSubmissionTest"
  val TIMEOUT = PatienceConfiguration.Timeout(Span(3, Minutes))
  val INTERVAL = PatienceConfiguration.Interval(Span(1, Seconds))
}
