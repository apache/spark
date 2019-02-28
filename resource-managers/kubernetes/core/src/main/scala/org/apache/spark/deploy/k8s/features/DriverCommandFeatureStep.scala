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
package org.apache.spark.deploy.k8s.features

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder}

import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit._
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.Utils

/**
 * Creates the driver command for running the user app, and propagates needed configuration so
 * executors can also find the app code.
 */
private[spark] class DriverCommandFeatureStep(conf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep {

  override def configurePod(pod: SparkPod): SparkPod = {
    conf.mainAppResource match {
      case JavaMainAppResource(_) =>
        configureForJava(pod)

      case PythonMainAppResource(res) =>
        configureForPython(pod, res)

      case RMainAppResource(res) =>
        configureForR(pod, res)
    }
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    conf.mainAppResource match {
      case JavaMainAppResource(res) =>
        res.map(additionalJavaProperties).getOrElse(Map.empty)

      case PythonMainAppResource(res) =>
        additionalPythonProperties(res)

      case RMainAppResource(res) =>
        additionalRProperties(res)
    }
  }

  private def configureForJava(pod: SparkPod): SparkPod = {
    // The user application jar is merged into the spark.jars list and managed through that
    // property, so use a "blank" resource for the Java driver.
    val driverContainer = baseDriverContainer(pod, SparkLauncher.NO_RESOURCE).build()
    SparkPod(pod.pod, driverContainer)
  }

  private def configureForPython(pod: SparkPod, res: String): SparkPod = {
    val maybePythonFiles = if (conf.pyFiles.nonEmpty) {
      // Delineation by ":" is to append the PySpark Files to the PYTHONPATH
      // of the respective PySpark pod
      val resolved = KubernetesUtils.resolveFileUrisAndPath(conf.pyFiles)
      Some(new EnvVarBuilder()
        .withName(ENV_PYSPARK_FILES)
        .withValue(resolved.mkString(":"))
        .build())
    } else {
      None
    }
    val pythonEnvs =
      Seq(new EnvVarBuilder()
          .withName(ENV_PYSPARK_MAJOR_PYTHON_VERSION)
          .withValue(conf.get(PYSPARK_MAJOR_PYTHON_VERSION))
        .build()) ++
      maybePythonFiles

    val pythonContainer = baseDriverContainer(pod, KubernetesUtils.resolveFileUri(res))
      .addAllToEnv(pythonEnvs.asJava)
      .build()

    SparkPod(pod.pod, pythonContainer)
  }

  private def configureForR(pod: SparkPod, res: String): SparkPod = {
    val rContainer = baseDriverContainer(pod, KubernetesUtils.resolveFileUri(res)).build()
    SparkPod(pod.pod, rContainer)
  }

  private def baseDriverContainer(pod: SparkPod, resource: String): ContainerBuilder = {
    new ContainerBuilder(pod.container)
      .addToArgs("driver")
      .addToArgs("--properties-file", SPARK_CONF_PATH)
      .addToArgs("--class", conf.mainClass)
      .addToArgs(resource)
      .addToArgs(conf.appArgs: _*)
  }

  private def additionalJavaProperties(resource: String): Map[String, String] = {
    resourceType(APP_RESOURCE_TYPE_JAVA) ++ mergeFileList(JARS, Seq(resource))
  }

  private def additionalPythonProperties(resource: String): Map[String, String] = {
    resourceType(APP_RESOURCE_TYPE_PYTHON) ++
      mergeFileList(FILES, Seq(resource) ++ conf.pyFiles)
  }

  private def additionalRProperties(resource: String): Map[String, String] = {
    resourceType(APP_RESOURCE_TYPE_R) ++ mergeFileList(FILES, Seq(resource))
  }

  private def mergeFileList(key: ConfigEntry[Seq[String]], filesToAdd: Seq[String])
    : Map[String, String] = {
    val existing = conf.get(key)
    Map(key.key -> (existing ++ filesToAdd).distinct.mkString(","))
  }

  private def resourceType(resType: String): Map[String, String] = {
    Map(APP_RESOURCE_TYPE.key -> resType)
  }
}
