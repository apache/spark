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

package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.{LocalObjectReferenceBuilder, PodBuilder}

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit._
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID

class KubernetesConfSuite extends SparkFunSuite {

  private val APP_ARGS = Array("arg1", "arg2")
  private val CUSTOM_NODE_SELECTOR = Map(
    "nodeSelectorKey1" -> "nodeSelectorValue1",
    "nodeSelectorKey2" -> "nodeSelectorValue2")
  private val CUSTOM_DRIVER_NODE_SELECTOR = Map(
    "driverNodeSelectorKey1" -> "driverNodeSelectorValue1",
    "driverNodeSelectorKey2" -> "driverNodeSelectorValue2")
  private val CUSTOM_EXECUTOR_NODE_SELECTOR = Map(
    "execNodeSelectorKey1" -> "execNodeSelectorValue1",
    "execNodeSelectorKey2" -> "execNodeSelectorValue2")
  private val CUSTOM_LABELS = Map(
    "customLabel1Key" -> "customLabel1Value",
    "customLabel2Key" -> "customLabel2Value")
  private val CUSTOM_ANNOTATIONS = Map(
    "customAnnotation1Key" -> "customAnnotation1Value",
    "customAnnotation2Key" -> "customAnnotation2Value")
  private val SECRET_NAMES_TO_MOUNT_PATHS = Map(
    "secret1" -> "/mnt/secrets/secret1",
    "secret2" -> "/mnt/secrets/secret2")
  private val SECRET_ENV_VARS = Map(
    "envName1" -> "name1:key1",
    "envName2" -> "name2:key2")
  private val CUSTOM_ENVS = Map(
    "customEnvKey1" -> "customEnvValue1",
    "customEnvKey2" -> "customEnvValue2")
  private val DRIVER_POD = new PodBuilder().build()
  private val EXECUTOR_ID = "executor-id"
  private val EXECUTOR_ENV_VARS = Map(
    "spark.executorEnv.1executorEnvVars1/var1" -> "executorEnvVars1",
    "spark.executorEnv.executorEnvVars2*var2" -> "executorEnvVars2",
    "spark.executorEnv.executorEnvVars3_var3" -> "executorEnvVars3",
    "spark.executorEnv.executorEnvVars4-var4" -> "executorEnvVars4",
    "spark.executorEnv.executorEnvVars5-var5" -> "executorEnvVars5/var5")

  test("Resolve driver labels, annotations, secret mount paths, envs, and memory overhead") {
    val sparkConf = new SparkConf(false)
      .set(MEMORY_OVERHEAD_FACTOR, 0.3)
    CUSTOM_LABELS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_DRIVER_LABEL_PREFIX$key", value)
    }
    CUSTOM_ANNOTATIONS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_DRIVER_ANNOTATION_PREFIX$key", value)
    }
    SECRET_NAMES_TO_MOUNT_PATHS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_DRIVER_SECRETS_PREFIX$key", value)
    }
    SECRET_ENV_VARS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_DRIVER_SECRET_KEY_REF_PREFIX$key", value)
    }
    CUSTOM_ENVS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_DRIVER_ENV_PREFIX$key", value)
    }

    val conf = KubernetesConf.createDriverConf(
      sparkConf,
      KubernetesTestConf.APP_ID,
      JavaMainAppResource(None),
      KubernetesTestConf.MAIN_CLASS,
      APP_ARGS,
      None)
    assert(conf.labels === Map(
      SPARK_VERSION_LABEL -> SPARK_VERSION,
      SPARK_APP_ID_LABEL -> KubernetesTestConf.APP_ID,
      SPARK_APP_NAME_LABEL -> KubernetesConf.getAppNameLabel(conf.appName),
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE) ++
      CUSTOM_LABELS)
    assert(conf.annotations === CUSTOM_ANNOTATIONS)
    assert(conf.secretNamesToMountPaths === SECRET_NAMES_TO_MOUNT_PATHS)
    assert(conf.secretEnvNamesToKeyRefs === SECRET_ENV_VARS)
    assert(conf.environment === CUSTOM_ENVS)
    assert(conf.sparkConf.get(MEMORY_OVERHEAD_FACTOR) === 0.3)
  }

  test("Basic executor translated fields.") {
    val conf = KubernetesConf.createExecutorConf(
      new SparkConf(false),
      EXECUTOR_ID,
      KubernetesTestConf.APP_ID,
      Some(DRIVER_POD))
    assert(conf.executorId === EXECUTOR_ID)
    assert(conf.driverPod.get === DRIVER_POD)
    assert(conf.resourceProfileId === DEFAULT_RESOURCE_PROFILE_ID)
  }

  test("resource profile not default.") {
    val conf = KubernetesConf.createExecutorConf(
      new SparkConf(false),
      EXECUTOR_ID,
      KubernetesTestConf.APP_ID,
      Some(DRIVER_POD),
      10)
    assert(conf.resourceProfileId === 10)
  }

  test("Image pull secrets.") {
    val conf = KubernetesConf.createExecutorConf(
      new SparkConf(false)
        .set(IMAGE_PULL_SECRETS, Seq("my-secret-1", "my-secret-2 ")),
      EXECUTOR_ID,
      KubernetesTestConf.APP_ID,
      Some(DRIVER_POD))
    assert(conf.imagePullSecrets ===
      Seq(
        new LocalObjectReferenceBuilder().withName("my-secret-1").build(),
        new LocalObjectReferenceBuilder().withName("my-secret-2").build()))
  }

  test("Set executor labels, annotations, and secrets") {
    val sparkConf = new SparkConf(false)
    CUSTOM_LABELS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_EXECUTOR_LABEL_PREFIX$key", value)
    }
    CUSTOM_ANNOTATIONS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_EXECUTOR_ANNOTATION_PREFIX$key", value)
    }
    SECRET_ENV_VARS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX$key", value)
    }
    SECRET_NAMES_TO_MOUNT_PATHS.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_EXECUTOR_SECRETS_PREFIX$key", value)
    }

    val conf = KubernetesConf.createExecutorConf(
      sparkConf,
      EXECUTOR_ID,
      KubernetesTestConf.APP_ID,
      Some(DRIVER_POD))
    assert(conf.labels === Map(
      SPARK_VERSION_LABEL -> SPARK_VERSION,
      SPARK_EXECUTOR_ID_LABEL -> EXECUTOR_ID,
      SPARK_APP_ID_LABEL -> KubernetesTestConf.APP_ID,
      SPARK_APP_NAME_LABEL -> KubernetesConf.getAppNameLabel(conf.appName),
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE,
      SPARK_RESOURCE_PROFILE_ID_LABEL -> DEFAULT_RESOURCE_PROFILE_ID.toString) ++ CUSTOM_LABELS)
    assert(conf.annotations === CUSTOM_ANNOTATIONS)
    assert(conf.secretNamesToMountPaths === SECRET_NAMES_TO_MOUNT_PATHS)
    assert(conf.secretEnvNamesToKeyRefs === SECRET_ENV_VARS)
  }

  test("Verify that executorEnv key conforms to the regular specification") {
    val sparkConf = new SparkConf(false)
    EXECUTOR_ENV_VARS.foreach { case (key, value) =>
      sparkConf.set(key, value)
    }

    val conf = KubernetesConf.createExecutorConf(
      sparkConf,
      EXECUTOR_ID,
      KubernetesTestConf.APP_ID,
      Some(DRIVER_POD))
    assert(conf.environment ===
      Map(
        "executorEnvVars3_var3" -> "executorEnvVars3",
        "executorEnvVars4-var4" -> "executorEnvVars4",
        "executorEnvVars5-var5" -> "executorEnvVars5/var5"))
  }

  test("SPARK-36075: Set nodeSelector, driverNodeSelector, executorNodeSelect") {
    val sparkConf = new SparkConf(false)
    CUSTOM_NODE_SELECTOR.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_NODE_SELECTOR_PREFIX$key", value)
    }
    CUSTOM_DRIVER_NODE_SELECTOR.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_DRIVER_NODE_SELECTOR_PREFIX$key", value)
    }
    CUSTOM_EXECUTOR_NODE_SELECTOR.foreach { case (key, value) =>
      sparkConf.set(s"$KUBERNETES_EXECUTOR_NODE_SELECTOR_PREFIX$key", value)
    }
    val execConf = KubernetesTestConf.createExecutorConf(sparkConf)
    assert(execConf.nodeSelector === CUSTOM_NODE_SELECTOR)
    assert(execConf.executorNodeSelector === CUSTOM_EXECUTOR_NODE_SELECTOR)
    val driverConf = KubernetesTestConf.createDriverConf(sparkConf)
    assert(driverConf.nodeSelector === CUSTOM_NODE_SELECTOR)
    assert(driverConf.driverNodeSelector === CUSTOM_DRIVER_NODE_SELECTOR)
  }

  test("SPARK-36059: Set driver.scheduler and executor.scheduler") {
    val sparkConf = new SparkConf(false)
    val execUnsetConf = KubernetesTestConf.createExecutorConf(sparkConf)
    val driverUnsetConf = KubernetesTestConf.createDriverConf(sparkConf)
    assert(execUnsetConf.schedulerName === None)
    assert(driverUnsetConf.schedulerName === None)

    sparkConf.set(KUBERNETES_SCHEDULER_NAME, "sameScheduler")
    // Use KUBERNETES_SCHEDULER_NAME when is NOT set
    assert(KubernetesTestConf.createDriverConf(sparkConf).schedulerName === Some("sameScheduler"))
    assert(KubernetesTestConf.createExecutorConf(sparkConf).schedulerName === Some("sameScheduler"))

    // Override by driver/executor side scheduler when ""
    sparkConf.set(KUBERNETES_DRIVER_SCHEDULER_NAME, "")
    sparkConf.set(KUBERNETES_EXECUTOR_SCHEDULER_NAME, "")
    assert(KubernetesTestConf.createDriverConf(sparkConf).schedulerName === Some(""))
    assert(KubernetesTestConf.createExecutorConf(sparkConf).schedulerName === Some(""))

    // Override by driver/executor side scheduler when set
    sparkConf.set(KUBERNETES_DRIVER_SCHEDULER_NAME, "driverScheduler")
    sparkConf.set(KUBERNETES_EXECUTOR_SCHEDULER_NAME, "executorScheduler")
    val execConf = KubernetesTestConf.createExecutorConf(sparkConf)
    assert(execConf.schedulerName === Some("executorScheduler"))
    val driverConf = KubernetesTestConf.createDriverConf(sparkConf)
    assert(driverConf.schedulerName === Some("driverScheduler"))
  }

  test("SPARK-37735: access appId in KubernetesConf") {
    val sparkConf = new SparkConf(false)
    val driverConf = KubernetesTestConf.createDriverConf(sparkConf)
    val execConf = KubernetesTestConf.createExecutorConf(sparkConf)
    assert(driverConf.appId === KubernetesTestConf.APP_ID)
    assert(execConf.appId === KubernetesTestConf.APP_ID)
  }

  test("SPARK-36566: get app name label") {
    assert(KubernetesConf.getAppNameLabel(" Job+Spark-Pi 2021") === "job-spark-pi-2021")
    assert(KubernetesConf.getAppNameLabel("a" * 63) === "a" * 63)
    assert(KubernetesConf.getAppNameLabel("a" * 64) === "a" * 63)
    assert(KubernetesConf.getAppNameLabel("a" * 253) === "a" * 63)
  }

  test("SPARK-38630: K8s label value should start and end with alphanumeric") {
    assert(KubernetesConf.getAppNameLabel("-hello-") === "hello")
    assert(KubernetesConf.getAppNameLabel("a" * 62 + "-aaa") === "a" * 62)
    assert(KubernetesConf.getAppNameLabel("-" + "a" * 63) === "a" * 62)
  }
}
