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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource

class KubernetesConfSuite extends SparkFunSuite {

  private val APP_NAME = "test-app"
  private val RESOURCE_NAME_PREFIX = "prefix"
  private val APP_ID = "test-id"
  private val MAIN_CLASS = "test-class"
  private val APP_ARGS = Array("arg1", "arg2")
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

  test("Basic driver translated fields.") {
    val sparkConf = new SparkConf(false)
    val conf = KubernetesConf.createDriverConf(
      sparkConf,
      APP_NAME,
      RESOURCE_NAME_PREFIX,
      APP_ID,
      None,
      MAIN_CLASS,
      APP_ARGS)
    assert(conf.appId === APP_ID)
    assert(conf.sparkConf.getAll.toMap === sparkConf.getAll.toMap)
    assert(conf.appResourceNamePrefix === RESOURCE_NAME_PREFIX)
    assert(conf.roleSpecificConf.appName === APP_NAME)
    assert(conf.roleSpecificConf.mainAppResource.isEmpty)
    assert(conf.roleSpecificConf.mainClass === MAIN_CLASS)
    assert(conf.roleSpecificConf.appArgs === APP_ARGS)
  }

  test("Creating driver conf with and without the main app jar influences spark.jars") {
    val sparkConf = new SparkConf(false)
      .setJars(Seq("local:///opt/spark/jar1.jar"))
    val mainAppJar = Some(JavaMainAppResource("local:///opt/spark/main.jar"))
    val kubernetesConfWithMainJar = KubernetesConf.createDriverConf(
      sparkConf,
      APP_NAME,
      RESOURCE_NAME_PREFIX,
      APP_ID,
      mainAppJar,
      MAIN_CLASS,
      APP_ARGS)
    assert(kubernetesConfWithMainJar.sparkConf.get("spark.jars")
      .split(",")
      === Array("local:///opt/spark/jar1.jar", "local:///opt/spark/main.jar"))
    val kubernetesConfWithoutMainJar = KubernetesConf.createDriverConf(
      sparkConf,
      APP_NAME,
      RESOURCE_NAME_PREFIX,
      APP_ID,
      None,
      MAIN_CLASS,
      APP_ARGS)
    assert(kubernetesConfWithoutMainJar.sparkConf.get("spark.jars").split(",")
      === Array("local:///opt/spark/jar1.jar"))
  }

  test("Resolve driver labels, annotations, secret mount paths, and envs.") {
    val sparkConf = new SparkConf(false)
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
      APP_NAME,
      RESOURCE_NAME_PREFIX,
      APP_ID,
      None,
      MAIN_CLASS,
      APP_ARGS)
    assert(conf.roleLabels === Map(
      SPARK_APP_ID_LABEL -> APP_ID,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE) ++
      CUSTOM_LABELS)
    assert(conf.roleAnnotations === CUSTOM_ANNOTATIONS)
    assert(conf.roleSecretNamesToMountPaths === SECRET_NAMES_TO_MOUNT_PATHS)
    assert(conf.roleSecretEnvNamesToKeyRefs === SECRET_ENV_VARS)
    assert(conf.roleEnvs === CUSTOM_ENVS)
  }

  test("Basic executor translated fields.") {
    val conf = KubernetesConf.createExecutorConf(
      new SparkConf(false),
      EXECUTOR_ID,
      APP_ID,
      DRIVER_POD)
    assert(conf.roleSpecificConf.executorId === EXECUTOR_ID)
    assert(conf.roleSpecificConf.driverPod === DRIVER_POD)
  }

  test("Image pull secrets.") {
    val conf = KubernetesConf.createExecutorConf(
      new SparkConf(false)
        .set(IMAGE_PULL_SECRETS, "my-secret-1,my-secret-2 "),
      EXECUTOR_ID,
      APP_ID,
      DRIVER_POD)
    assert(conf.imagePullSecrets() ===
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
      APP_ID,
      DRIVER_POD)
    assert(conf.roleLabels === Map(
      SPARK_EXECUTOR_ID_LABEL -> EXECUTOR_ID,
      SPARK_APP_ID_LABEL -> APP_ID,
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE) ++ CUSTOM_LABELS)
    assert(conf.roleAnnotations === CUSTOM_ANNOTATIONS)
    assert(conf.roleSecretNamesToMountPaths === SECRET_NAMES_TO_MOUNT_PATHS)
    assert(conf.roleSecretEnvNamesToKeyRefs === SECRET_ENV_VARS)
  }
}
