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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit._
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

class DriverCommandFeatureStepSuite extends SparkFunSuite {

  test("java resource") {
    val mainResource = "local:/main.jar"
    val spec = applyFeatureStep(
      JavaMainAppResource(Some(mainResource)),
      appArgs = Array("5", "7"))
    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      mainResource, "5", "7"))
  }

  test("python resource") {
    val mainResource = "local:/main.py"
    val sparkConf = new SparkConf(false)
      .set(PYSPARK_MAJOR_PYTHON_VERSION, "2")
    val spec = applyFeatureStep(
      PythonMainAppResource(mainResource),
      conf = sparkConf,
      appArgs = Array("5", "7", "9"))

    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      mainResource, "5", "7", "9"))

    val envs = spec.pod.container.getEnv.asScala
      .map { env => (env.getName, env.getValue) }
      .toMap
    val expected = Map(ENV_PYSPARK_MAJOR_PYTHON_VERSION -> "2")
    assert(envs === expected)
  }

  test("R resource") {
    val mainResource = "local:/main.R"

    val spec = applyFeatureStep(
      RMainAppResource(mainResource),
      appArgs = Array("5", "7", "9"))

    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      mainResource, "5", "7", "9"))
  }

  private def applyFeatureStep(
      resource: MainAppResource,
      conf: SparkConf = new SparkConf(false),
      appArgs: Array[String] = Array()): KubernetesDriverSpec = {
    val kubernetesConf = KubernetesTestConf.createDriverConf(
      sparkConf = conf,
      mainAppResource = resource,
      appArgs = appArgs)
    val step = new DriverCommandFeatureStep(kubernetesConf)
    val pod = step.configurePod(SparkPod.initialPod())
    val props = step.getAdditionalPodSystemProperties()
    KubernetesDriverSpec(pod, Nil, props)
  }

}
