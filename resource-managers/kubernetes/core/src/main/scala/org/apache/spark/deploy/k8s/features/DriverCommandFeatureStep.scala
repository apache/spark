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
    val driverContainer = baseDriverContainer(pod, res).build()
    SparkPod(pod.pod, driverContainer)
  }

  private def configureForPython(pod: SparkPod, res: String): SparkPod = {
    val pythonEnvs =
      Seq(new EnvVarBuilder()
          .withName(ENV_PYSPARK_MAJOR_PYTHON_VERSION)
          .withValue(conf.get(PYSPARK_MAJOR_PYTHON_VERSION))
        .build())

    val pythonContainer = baseDriverContainer(pod, res)
      .addAllToEnv(pythonEnvs.asJava)
      .build()

    SparkPod(pod.pod, pythonContainer)
  }

  private def configureForR(pod: SparkPod, res: String): SparkPod = {
    val rContainer = baseDriverContainer(pod, res).build()
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
}
