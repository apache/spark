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
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit._
import org.apache.spark.internal.config.{PYSPARK_DRIVER_PYTHON, PYSPARK_PYTHON}

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
    val spec = applyFeatureStep(
      PythonMainAppResource(mainResource),
      conf = sparkConf,
      appArgs = Array("5", "7", "9"))

    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      mainResource, "5", "7", "9"))
  }

  test("python executable precedence") {
    val mainResource = "local:/main.py"

    val pythonExecutables = Seq(
      (Some("conf_py"), Some("conf_driver_py"), Some("env_py"), Some("env_driver_py")),
      (Some("conf_py"), None, Some("env_py"), Some("env_driver_py")),
      (None, None, Some("env_py"), Some("env_driver_py")),
      (None, None, Some("env_py"), None)
    )

    val expectedResults = Seq(
      ("conf_py", "conf_driver_py"),
      ("conf_py", "conf_py"),
      ("env_py", "env_driver_py"),
      ("env_py", "env_py")
    )

    pythonExecutables.zip(expectedResults).foreach { case (pythonExecutable, expected) =>
      val sparkConf = new SparkConf(false)
      val (confPy, confDriverPy, envPy, envDriverPy) = pythonExecutable
      confPy.foreach(sparkConf.set(PYSPARK_PYTHON, _))
      confDriverPy.foreach(sparkConf.set(PYSPARK_DRIVER_PYTHON, _))
      val pythonEnvs = Map(
        (
          envPy.map(v => ENV_PYSPARK_PYTHON -> v :: Nil) ++
          envDriverPy.map(v => ENV_PYSPARK_DRIVER_PYTHON -> v :: Nil)
        ).flatten.toArray: _*)

      val spec = applyFeatureStep(
        PythonMainAppResource(mainResource),
        conf = sparkConf,
        appArgs = Array("foo"),
        env = pythonEnvs)

      val envs = spec.pod.container.getEnv.asScala
        .map { env => (env.getName, env.getValue) }
        .toMap

      val (expectedEnvPy, expectedDriverPy) = expected
      assert(envs === Map(
        ENV_PYSPARK_PYTHON -> expectedEnvPy,
        ENV_PYSPARK_DRIVER_PYTHON -> expectedDriverPy))
    }
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

  test("SPARK-25355: java resource args with proxy-user") {
    val mainResource = "local:/main.jar"
    val spec = applyFeatureStep(
      JavaMainAppResource(Some(mainResource)),
      appArgs = Array("5", "7"),
      proxyUser = Some("test.user"))
    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--proxy-user", "test.user",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      mainResource, "5", "7"))
  }

  test("SPARK-25355: python resource args with proxy-user") {
    val mainResource = "local:/main.py"
    val sparkConf = new SparkConf(false)
    val spec = applyFeatureStep(
      PythonMainAppResource(mainResource),
      conf = sparkConf,
      appArgs = Array("5", "7", "9"),
      proxyUser = Some("test.user"))

    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--proxy-user", "test.user",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      mainResource, "5", "7", "9"))
  }

  test("SPARK-25355: R resource args with proxy-user") {
    val mainResource = "local:/main.R"

    val spec = applyFeatureStep(
      RMainAppResource(mainResource),
      appArgs = Array("5", "7", "9"),
      proxyUser = Some("test.user"))

    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--proxy-user", "test.user",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      mainResource, "5", "7", "9"))
  }

  private def applyFeatureStep(
      resource: MainAppResource,
      conf: SparkConf = new SparkConf(false),
      appArgs: Array[String] = Array(),
      proxyUser: Option[String] = None,
      env: Map[String, String] = Map.empty[String, String]): KubernetesDriverSpec = {
    val kubernetesConf = KubernetesTestConf.createDriverConf(
      sparkConf = conf,
      mainAppResource = resource,
      appArgs = appArgs,
      proxyUser = proxyUser)
    val step = new DriverCommandFeatureStep(kubernetesConf) {
      private[spark] override val environmentVariables: Map[String, String] = env
    }
    val pod = step.configurePod(SparkPod.initialPod())
    val props = step.getAdditionalPodSystemProperties()
    KubernetesDriverSpec(pod, Nil, props)
  }

}
