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

import java.io.{File, FileInputStream}
import java.time.Instant
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
// scalastyle:off executioncontextglobal
import scala.concurrent.ExecutionContext.Implicits.global
// scalastyle:on executioncontextglobal
import scala.concurrent.Future

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.NamespacedKubernetesClient
import io.fabric8.volcano.client.VolcanoClient
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.features.VolcanoFeatureStep
import org.apache.spark.internal.config.NETWORK_AUTH_ENABLED

private[spark] trait VolcanoTestsSuite extends BeforeAndAfterEach { k8sSuite: KubernetesSuite =>
  import VolcanoTestsSuite._
  import org.apache.spark.deploy.k8s.integrationtest.VolcanoSuite.volcanoTag
  import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{k8sTestTag, INTERVAL, TIMEOUT,
    SPARK_DRIVER_MAIN_CLASS}

  lazy val volcanoClient: VolcanoClient
    = kubernetesTestComponents.kubernetesClient.adapt(classOf[VolcanoClient])
  lazy val k8sClient: NamespacedKubernetesClient = kubernetesTestComponents.kubernetesClient
  private val testGroups: mutable.Set[String] = mutable.Set.empty
  private val testYAMLPaths: mutable.Set[String] = mutable.Set.empty

  private def deletePodInTestGroup(): Unit = {
    testGroups.foreach { g =>
      k8sClient.pods().withLabel("spark-group-locator", g).delete()
      Eventually.eventually(TIMEOUT, INTERVAL) {
        assert(k8sClient.pods().withLabel("spark-group-locator", g).list().getItems.isEmpty)
      }
    }
    testGroups.clear()
  }

  private def deleteYamlResources(): Unit = {
    testYAMLPaths.foreach { yaml =>
      deleteYAMLResource(yaml)
      Eventually.eventually(TIMEOUT, INTERVAL) {
        val resources = k8sClient.load(new FileInputStream(yaml)).fromServer.get.asScala
        // Make sure all elements are null (no specific resources in cluster)
        resources.foreach { r => assert(r === null) }
      }
    }
    testYAMLPaths.clear()
  }

  override protected def afterEach(): Unit = {
    deletePodInTestGroup()
    deleteYamlResources()
    super.afterEach()
  }

  protected def generateGroupName(name: String): String = {
    val groupName = GROUP_PREFIX + name
    // Append to testGroups
    testGroups += groupName
    groupName
  }

  protected def checkScheduler(pod: Pod): Unit = {
    assert(pod.getSpec.getSchedulerName === "volcano")
  }

  protected def checkAnnotaion(pod: Pod): Unit = {
    val appId = pod.getMetadata.getLabels.get("spark-app-selector")
    val annotations = pod.getMetadata.getAnnotations
    assert(annotations.get("scheduling.k8s.io/group-name") === s"$appId-podgroup")
  }

  protected def checkPodGroup(
      pod: Pod,
      queue: Option[String] = None,
      priorityClassName: Option[String] = None): Unit = {
    val appId = pod.getMetadata.getLabels.get("spark-app-selector")
    val podGroupName = s"$appId-podgroup"
    val podGroup = volcanoClient.podGroups().withName(podGroupName).get()
    assert(podGroup.getMetadata.getOwnerReferences.get(0).getName === pod.getMetadata.getName)
    queue.foreach(q => assert(q === podGroup.getSpec.getQueue))
    priorityClassName.foreach(_ =>
      assert(pod.getSpec.getPriorityClassName === podGroup.getSpec.getPriorityClassName))
  }

  private def createOrReplaceYAMLResource(yamlPath: String): Unit = {
    k8sClient.load(new FileInputStream(yamlPath)).createOrReplace()
    testYAMLPaths += yamlPath
  }

  private def deleteYAMLResource(yamlPath: String): Unit = {
    k8sClient.load(new FileInputStream(yamlPath)).delete()
  }

  private def getPods(
      role: String,
      groupLocator: String,
      statusPhase: String): mutable.Buffer[Pod] = {
    k8sClient
      .pods()
      .withLabel("spark-group-locator", groupLocator)
      .withLabel("spark-role", role)
      .withField("status.phase", statusPhase)
      .list()
      .getItems.asScala
  }

  def runJobAndVerify(
      batchSuffix: String,
      groupLoc: Option[String] = None,
      queue: Option[String] = None,
      driverTemplate: Option[String] = None,
      isDriverJob: Boolean = false,
      driverPodGroupTemplate: Option[String] = None): Unit = {
    val appLoc = s"${appLocator}${batchSuffix}"
    val podName = s"${driverPodName}-${batchSuffix}"
    // create new configuration for every job
    val conf = createVolcanoSparkConf(podName, appLoc, groupLoc, queue, driverTemplate,
      driverPodGroupTemplate)
    if (isDriverJob) {
      runSparkDriverSubmissionAndVerifyCompletion(
        driverPodChecker = (driverPod: Pod) => {
          checkScheduler(driverPod)
          checkAnnotaion(driverPod)
          checkPodGroup(driverPod, queue)
        },
        customSparkConf = Option(conf),
        customAppLocator = Option(appLoc)
      )
    } else {
      runSparkPiAndVerifyCompletion(
        driverPodChecker = (driverPod: Pod) => {
          checkScheduler(driverPod)
          checkAnnotaion(driverPod)
          checkPodGroup(driverPod, queue)
        },
        executorPodChecker = (executorPod: Pod) => {
          checkScheduler(executorPod)
          checkAnnotaion(executorPod)
        },
        customSparkConf = Option(conf),
        customAppLocator = Option(appLoc)
      )
    }
  }

  protected def runSparkDriverSubmissionAndVerifyCompletion(
      appResource: String = containerLocalSparkDistroExamplesJar,
      mainClass: String = SPARK_DRIVER_MAIN_CLASS,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      appArgs: Array[String] = Array("2"),
      customSparkConf: Option[SparkAppConf] = None,
      customAppLocator: Option[String] = None): Unit = {
    val appArguments = SparkAppArguments(
      mainAppResource = appResource,
      mainClass = mainClass,
      appArgs = appArgs)
    SparkAppLauncher.launch(
      appArguments,
      customSparkConf.getOrElse(sparkAppConf),
      TIMEOUT.value.toSeconds.toInt,
      sparkHomeDir,
      true)
    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", customAppLocator.getOrElse(appLocator))
      .withLabel("spark-role", "driver")
      .list()
      .getItems
      .get(0)
    driverPodChecker(driverPod)
  }

  private def createVolcanoSparkConf(
      driverPodName: String = driverPodName,
      appLoc: String = appLocator,
      groupLoc: Option[String] = None,
      queue: Option[String] = None,
      driverTemplate: Option[String] = None,
      driverPodGroupTemplate: Option[String] = None): SparkAppConf = {
    val conf = kubernetesTestComponents.newSparkAppConf()
      .set(CONTAINER_IMAGE.key, image)
      .set(KUBERNETES_DRIVER_POD_NAME.key, driverPodName)
      .set(s"${KUBERNETES_DRIVER_LABEL_PREFIX}spark-app-locator", appLoc)
      .set(s"${KUBERNETES_EXECUTOR_LABEL_PREFIX}spark-app-locator", appLoc)
      .set(NETWORK_AUTH_ENABLED.key, "true")
      // below is volcano specific configuration
      .set(KUBERNETES_SCHEDULER_NAME.key, "volcano")
      .set(KUBERNETES_DRIVER_POD_FEATURE_STEPS.key, VOLCANO_FEATURE_STEP)
      .set(KUBERNETES_EXECUTOR_POD_FEATURE_STEPS.key, VOLCANO_FEATURE_STEP)
    queue.foreach { q =>
      conf.set(VolcanoFeatureStep.POD_GROUP_TEMPLATE_FILE_KEY,
        new File(
          getClass.getResource(s"/volcano/$q-driver-podgroup-template.yml").getFile
        ).getAbsolutePath)
    }
    driverPodGroupTemplate.foreach(conf.set(VolcanoFeatureStep.POD_GROUP_TEMPLATE_FILE_KEY, _))
    groupLoc.foreach { locator =>
      conf.set(s"${KUBERNETES_DRIVER_LABEL_PREFIX}spark-group-locator", locator)
      conf.set(s"${KUBERNETES_EXECUTOR_LABEL_PREFIX}spark-group-locator", locator)
    }
    driverTemplate.foreach(conf.set(KUBERNETES_DRIVER_PODTEMPLATE_FILE.key, _))
    conf
  }

  test("Run SparkPi with volcano scheduler", k8sTestTag, volcanoTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.pod.featureSteps", VOLCANO_FEATURE_STEP)
      .set("spark.kubernetes.executor.pod.featureSteps", VOLCANO_FEATURE_STEP)
    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkScheduler(driverPod)
        checkAnnotaion(driverPod)
        checkPodGroup(driverPod)
      },
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        checkScheduler(executorPod)
        checkAnnotaion(executorPod)
      }
    )
  }

  private def verifyJobsSucceededOneByOne(jobNum: Int, groupName: String): Unit = {
    // Check Pending jobs completed one by one
    (1 until jobNum).map { completedNum =>
      Eventually.eventually(TIMEOUT, INTERVAL) {
        val pendingPods = getPods(role = "driver", groupName, statusPhase = "Pending")
        assert(pendingPods.size === jobNum - completedNum)
      }
    }
    // All jobs succeeded finally
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val succeededPods = getPods(role = "driver", groupName, statusPhase = "Succeeded")
      assert(succeededPods.size === jobNum)
    }
  }

  test("SPARK-38187: Run SparkPi Jobs with minCPU", k8sTestTag, volcanoTag) {
    val groupName = generateGroupName("min-cpu")
    // Create a queue with 2 CPU, 3G memory capacity
    createOrReplaceYAMLResource(QUEUE_2U_3G_YAML)
    // Submit 3 jobs with minCPU = 2
    val jobNum = 3
    (1 to jobNum).map { i =>
      Future {
        runJobAndVerify(
          i.toString,
          groupLoc = Option(groupName),
          driverPodGroupTemplate = Option(DRIVER_PG_TEMPLATE_CPU_2U))
      }
    }
    verifyJobsSucceededOneByOne(jobNum, groupName)
  }

  test("SPARK-38187: Run SparkPi Jobs with minMemory", k8sTestTag, volcanoTag) {
    val groupName = generateGroupName("min-mem")
    // Create a queue with 2 CPU, 3G memory capacity
    createOrReplaceYAMLResource(QUEUE_2U_3G_YAML)
    // Submit 3 jobs with minMemory = 3g
    val jobNum = 3
    (1 to jobNum).map { i =>
      Future {
        runJobAndVerify(
          i.toString,
          groupLoc = Option(groupName),
          driverPodGroupTemplate = Option(DRIVER_PG_TEMPLATE_MEMORY_3G))
      }
    }
    verifyJobsSucceededOneByOne(jobNum, groupName)
  }

  test("SPARK-38188: Run SparkPi jobs with 2 queues (only 1 enabled)", k8sTestTag, volcanoTag) {
    // Disabled queue0 and enabled queue1
    createOrReplaceYAMLResource(VOLCANO_Q0_DISABLE_Q1_ENABLE_YAML)
    // Submit jobs into disabled queue0 and enabled queue1
    val jobNum = 4
    (1 to jobNum).foreach { i =>
      Future {
        val queueName = s"queue${i % 2}"
        val groupName = generateGroupName(queueName)
        runJobAndVerify(i.toString, Option(groupName), Option(queueName))
      }
    }
    // There are two `Succeeded` jobs and two `Pending` jobs
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val completedPods = getPods("driver", s"${GROUP_PREFIX}queue1", "Succeeded")
      assert(completedPods.size === 2)
      val pendingPods = getPods("driver", s"${GROUP_PREFIX}queue0", "Pending")
      assert(pendingPods.size === 2)
    }
  }

  test("SPARK-38188: Run SparkPi jobs with 2 queues (all enabled)", k8sTestTag, volcanoTag) {
    val groupName = generateGroupName("queue-enable")
    // Enable all queues
    createOrReplaceYAMLResource(VOLCANO_ENABLE_Q0_AND_Q1_YAML)
    val jobNum = 4
    // Submit jobs into these two queues
    (1 to jobNum).foreach { i =>
      Future {
        val queueName = s"queue${i % 2}"
        runJobAndVerify(i.toString, Option(groupName), Option(queueName))
      }
    }
    // All jobs "Succeeded"
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val completedPods = getPods("driver", groupName, "Succeeded")
      assert(completedPods.size === jobNum)
    }
  }

  test("SPARK-38423: Run driver job to validate priority order", k8sTestTag, volcanoTag) {
    // Prepare the priority resource and queue
    createOrReplaceYAMLResource(DISABLE_QUEUE)
    createOrReplaceYAMLResource(VOLCANO_PRIORITY_YAML)
    // Submit 3 jobs with different priority
    val priorities = Seq("low", "medium", "high")
    priorities.foreach { p =>
      Future {
        val templatePath = new File(
          getClass.getResource(s"/volcano/$p-priority-driver-template.yml").getFile
        ).getAbsolutePath
        val pgTemplatePath = new File(
          getClass.getResource(s"/volcano/$p-priority-driver-podgroup-template.yml").getFile
        ).getAbsolutePath
        val groupName = generateGroupName(p)
        runJobAndVerify(
          p, groupLoc = Option(groupName),
          queue = Option("queue"),
          driverTemplate = Option(templatePath),
          driverPodGroupTemplate = Option(pgTemplatePath),
          isDriverJob = true
        )
      }
    }
    // Make sure 3 jobs are pending
    Eventually.eventually(TIMEOUT, INTERVAL) {
      priorities.foreach { p =>
        val pods = getPods(role = "driver", s"$GROUP_PREFIX$p", statusPhase = "Pending")
        assert(pods.size === 1)
      }
    }

    // Enable queue to let jobs running one by one
    createOrReplaceYAMLResource(ENABLE_QUEUE)

    // Verify scheduling order follow the specified priority
    Eventually.eventually(TIMEOUT, INTERVAL) {
      var m = Map.empty[String, Instant]
      priorities.foreach { p =>
        val pods = getPods(role = "driver", s"$GROUP_PREFIX$p", statusPhase = "Succeeded")
        assert(pods.size === 1)
        val conditions = pods.head.getStatus.getConditions.asScala
        val scheduledTime
          = conditions.filter(_.getType === "PodScheduled").head.getLastTransitionTime
        m += (p -> Instant.parse(scheduledTime))
      }
      // high --> medium --> low
      assert(m("high").isBefore(m("medium")))
      assert(m("medium").isBefore(m("low")))
    }
  }
}

private[spark] object VolcanoTestsSuite extends SparkFunSuite {
  val VOLCANO_FEATURE_STEP = classOf[VolcanoFeatureStep].getName
  val VOLCANO_ENABLE_Q0_AND_Q1_YAML = new File(
    getClass.getResource("/volcano/enable-queue0-enable-queue1.yml").getFile
  ).getAbsolutePath
  val VOLCANO_Q0_DISABLE_Q1_ENABLE_YAML = new File(
    getClass.getResource("/volcano/disable-queue0-enable-queue1.yml").getFile
  ).getAbsolutePath
  val GROUP_PREFIX = "volcano-test" + UUID.randomUUID().toString.replaceAll("-", "") + "-"
  val VOLCANO_PRIORITY_YAML
    = new File(getClass.getResource("/volcano/priorityClasses.yml").getFile).getAbsolutePath
  val ENABLE_QUEUE = new File(
    getClass.getResource("/volcano/enable-queue.yml").getFile
  ).getAbsolutePath
  val DISABLE_QUEUE = new File(
    getClass.getResource("/volcano/disable-queue.yml").getFile
  ).getAbsolutePath
  val QUEUE_2U_3G_YAML = new File(
    getClass.getResource("/volcano/queue-2u-3g.yml").getFile
  ).getAbsolutePath
  val DRIVER_PG_TEMPLATE_CPU_2U = new File(
    getClass.getResource("/volcano/driver-podgroup-template-cpu-2u.yml").getFile
  ).getAbsolutePath
  val DRIVER_PG_TEMPLATE_MEMORY_3G = new File(
    getClass.getResource("/volcano/driver-podgroup-template-memory-3g.yml").getFile
  ).getAbsolutePath
}
