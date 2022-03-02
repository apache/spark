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
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.features.VolcanoFeatureStep
import org.apache.spark.internal.config.NETWORK_AUTH_ENABLED

private[spark] trait VolcanoTestsSuite { k8sSuite: KubernetesSuite =>
  import VolcanoTestsSuite._
  import org.apache.spark.deploy.k8s.integrationtest.VolcanoSuite.volcanoTag
  import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{k8sTestTag, INTERVAL, TIMEOUT}

  lazy val volcanoClient: VolcanoClient
    = kubernetesTestComponents.kubernetesClient.adapt(classOf[VolcanoClient])
  lazy val k8sClient: NamespacedKubernetesClient = kubernetesTestComponents.kubernetesClient

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
      queue: Option[String] = None): Unit = {
    val appId = pod.getMetadata.getLabels.get("spark-app-selector")
    val podGroupName = s"$appId-podgroup"
    val podGroup = volcanoClient.podGroups().withName(podGroupName).get()
    assert(podGroup.getMetadata.getOwnerReferences.get(0).getName === pod.getMetadata.getName)
    queue.foreach(q => assert(q === podGroup.getSpec.getQueue))
  }

  private def createOrReplaceYAMLResource(yamlPath: String): Unit = {
    k8sClient.load(new FileInputStream(yamlPath)).createOrReplace()
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
      queue: Option[String] = None): Unit = {
    val appLoc = s"${appLocator}${batchSuffix}"
    val podName = s"${driverPodName}-${batchSuffix}"
    // create new configuration for every job
    val conf = createVolcanoSparkConf(podName, appLoc, groupLoc, queue)
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

  private def createVolcanoSparkConf(
      driverPodName: String = driverPodName,
      appLoc: String = appLocator,
      groupLoc: Option[String] = None,
      queue: Option[String] = None): SparkAppConf = {
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
    queue.foreach(conf.set(KUBERNETES_JOB_QUEUE.key, _))
    groupLoc.foreach { locator =>
      conf.set(s"${KUBERNETES_DRIVER_LABEL_PREFIX}spark-group-locator", locator)
      conf.set(s"${KUBERNETES_EXECUTOR_LABEL_PREFIX}spark-group-locator", locator)
    }
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

  test("SPARK-38188: Run SparkPi jobs with 2 queues (only 1 enable)", k8sTestTag, volcanoTag) {
    // Disabled queue0 and enabled queue1
    createOrReplaceYAMLResource(VOLCANO_Q0_DISABLE_Q1_ENABLE_YAML)
    // Submit jobs into disabled queue0 and enabled queue1
    val jobNum = 4
    (1 to jobNum).foreach { i =>
      Future {
        val queueName = s"queue${i % 2}"
        runJobAndVerify(i.toString, Option(s"$GROUP_PREFIX-$queueName"), Option(queueName))
      }
    }
    // There are two `Succeeded` jobs and two `Pending` jobs
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val completedPods = getPods("driver", s"$GROUP_PREFIX-queue1", "Succeeded")
      assert(completedPods.size === 2)
      val pendingPods = getPods("driver", s"$GROUP_PREFIX-queue0", "Pending")
      assert(pendingPods.size === 2)
    }
    deleteYAMLResource(VOLCANO_Q0_DISABLE_Q1_ENABLE_YAML)
  }

  test("SPARK-38188: Run SparkPi jobs with 2 queues (all enable)", k8sTestTag, volcanoTag) {
    // Enable all queues
    createOrReplaceYAMLResource(VOLCANO_ENABLE_Q0_AND_Q1_YAML)
    val jobNum = 4
    // Submit jobs into these two queues
    (1 to jobNum).foreach { i =>
      Future {
        val queueName = s"queue${i % 2}"
        runJobAndVerify(i.toString, Option(s"$GROUP_PREFIX"), Option(queueName))
      }
    }
    // All jobs "Succeeded"
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val completedPods = getPods("driver", GROUP_PREFIX, "Succeeded")
      assert(completedPods.size === jobNum)
    }
    deleteYAMLResource(VOLCANO_ENABLE_Q0_AND_Q1_YAML)
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
  val GROUP_PREFIX = "volcano-test" + UUID.randomUUID().toString.replaceAll("-", "")
}
