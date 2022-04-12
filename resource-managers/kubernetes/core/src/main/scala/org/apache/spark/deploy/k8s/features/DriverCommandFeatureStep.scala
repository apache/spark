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
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{PYSPARK_DRIVER_PYTHON, PYSPARK_PYTHON}
import org.apache.spark.launcher.SparkLauncher

/**
 * Creates the driver command for running the user app, and propagates needed configuration so
 * executors can also find the app code.
 */
private[spark] class DriverCommandFeatureStep(conf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {

  override def configurePod(pod: SparkPod): SparkPod = {
    conf.mainAppResource match {
      case JavaMainAppResource(res) =>
        configureForJava(pod, res.getOrElse(SparkLauncher.NO_RESOURCE))

      case PythonMainAppResource(res) =>
        configureForPython(pod, res)

      case RMainAppResource(res) =>
        configureForR(pod, res)
    }
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val appType = conf.mainAppResource match {
      case JavaMainAppResource(_) =>
        APP_RESOURCE_TYPE_JAVA

      case PythonMainAppResource(_) =>
        APP_RESOURCE_TYPE_PYTHON

      case RMainAppResource(_) =>
        APP_RESOURCE_TYPE_R
    }

    Map(APP_RESOURCE_TYPE.key -> appType)
  }

  private def configureForJava(pod: SparkPod, res: String): SparkPod = {
    // re-write primary resource, app jar is also added to spark.jars by default in SparkSubmit
    // no uploading takes place here
    val newResName = KubernetesUtils
      .renameMainAppResource(resource = res, shouldUploadLocal = false)
    val driverContainer = baseDriverContainer(pod, newResName).build()
    SparkPod(pod.pod, driverContainer)
  }

  // Exposed for testing purpose.
  private[spark] def environmentVariables: Map[String, String] = sys.env

  private def configureForPython(pod: SparkPod, res: String): SparkPod = {
    if (conf.get(PYSPARK_MAJOR_PYTHON_VERSION).isDefined) {
      logWarning(
          s"${PYSPARK_MAJOR_PYTHON_VERSION.key} was deprecated in Spark 3.1. " +
          s"Please set '${PYSPARK_PYTHON.key}' and '${PYSPARK_DRIVER_PYTHON.key}' " +
          s"configurations or $ENV_PYSPARK_PYTHON and $ENV_PYSPARK_DRIVER_PYTHON environment " +
          "variables instead.")
    }

    val pythonEnvs =
      Seq(
        conf.get(PYSPARK_PYTHON)
          .orElse(environmentVariables.get(ENV_PYSPARK_PYTHON)).map { value =>
          new EnvVarBuilder()
            .withName(ENV_PYSPARK_PYTHON)
            .withValue(value)
            .build()
        },
        conf.get(PYSPARK_DRIVER_PYTHON)
          .orElse(conf.get(PYSPARK_PYTHON))
          .orElse(environmentVariables.get(ENV_PYSPARK_DRIVER_PYTHON))
          .orElse(environmentVariables.get(ENV_PYSPARK_PYTHON)).map { value =>
          new EnvVarBuilder()
            .withName(ENV_PYSPARK_DRIVER_PYTHON)
            .withValue(value)
            .build()
        }
      ).flatten

    // re-write primary resource to be the remote one and upload the related file
    val newResName = KubernetesUtils
      .renameMainAppResource(res, Option(conf.sparkConf), true)
    val pythonContainer = baseDriverContainer(pod, newResName)
      .addAllToEnv(pythonEnvs.asJava)
      .build()

    SparkPod(pod.pod, pythonContainer)
  }

  private def configureForR(pod: SparkPod, res: String): SparkPod = {
    val rContainer = baseDriverContainer(pod, res).build()
    SparkPod(pod.pod, rContainer)
  }

  private def baseDriverContainer(pod: SparkPod, resource: String): ContainerBuilder = {
    var proxyUserArgs = Seq[String]()
    if (!conf.proxyUser.isEmpty) {
      proxyUserArgs = proxyUserArgs :+ "--proxy-user"
      proxyUserArgs = proxyUserArgs :+ conf.proxyUser.get
    }
    new ContainerBuilder(pod.container)
      .addToArgs("driver")
      .addToArgs(proxyUserArgs: _*)
      .addToArgs("--properties-file", SPARK_CONF_PATH)
      .addToArgs("--class", conf.mainClass)
      .addToArgs(resource)
      .addToArgs(conf.appArgs: _*)
  }
}
