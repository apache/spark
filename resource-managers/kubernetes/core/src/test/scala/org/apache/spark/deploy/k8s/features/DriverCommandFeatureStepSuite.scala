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
import org.apache.spark.util.Utils

class DriverCommandFeatureStepSuite extends SparkFunSuite {

  test("java resource") {
    val mainResource = "local:///main.jar"
    val spec = applyFeatureStep(
      JavaMainAppResource(Some(mainResource)),
      appArgs = Array("5", "7"))
    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      "spark-internal", "5", "7"))

    val jars = Utils.stringToSeq(spec.systemProperties("spark.jars"))
    assert(jars.toSet === Set(mainResource))
  }

  test("python resource with no extra files") {
    val mainResource = "local:///main.py"
    val sparkConf = new SparkConf(false)
      .set(PYSPARK_MAJOR_PYTHON_VERSION, "3")

    val spec = applyFeatureStep(
      PythonMainAppResource(mainResource),
      conf = sparkConf)
    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      "/main.py"))
    val envs = spec.pod.container.getEnv.asScala
      .map { env => (env.getName, env.getValue) }
      .toMap
    assert(envs(ENV_PYSPARK_MAJOR_PYTHON_VERSION) === "3")

    val files = Utils.stringToSeq(spec.systemProperties("spark.files"))
    assert(files.toSet === Set(mainResource))
  }

  test("python resource with extra files") {
    val expectedMainResource = "/main.py"
    val expectedPySparkFiles = "/example2.py:/example3.py"
    val filesInConf = Set("local:///example.py")

    val mainResource = s"local://$expectedMainResource"
    val pyFiles = Seq("local:///example2.py", "local:///example3.py")

    val sparkConf = new SparkConf(false)
      .set("spark.files", filesInConf.mkString(","))
      .set(PYSPARK_MAJOR_PYTHON_VERSION, "2")
    val spec = applyFeatureStep(
      PythonMainAppResource(mainResource),
      conf = sparkConf,
      appArgs = Array("5", "7", "9"),
      pyFiles = pyFiles)

    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      "/main.py", "5", "7", "9"))

    val envs = spec.pod.container.getEnv.asScala
      .map { env => (env.getName, env.getValue) }
      .toMap
    val expected = Map(
      ENV_PYSPARK_FILES -> expectedPySparkFiles,
      ENV_PYSPARK_MAJOR_PYTHON_VERSION -> "2")
    assert(envs === expected)

    val files = Utils.stringToSeq(spec.systemProperties("spark.files"))
    assert(files.toSet === pyFiles.toSet ++ filesInConf ++ Set(mainResource))
  }

  test("R resource") {
    val expectedMainResource = "/main.R"
    val mainResource = s"local://$expectedMainResource"

    val spec = applyFeatureStep(
      RMainAppResource(mainResource),
      appArgs = Array("5", "7", "9"))

    assert(spec.pod.container.getArgs.asScala === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", KubernetesTestConf.MAIN_CLASS,
      "/main.R", "5", "7", "9"))
  }

  private def applyFeatureStep(
      resource: MainAppResource,
      conf: SparkConf = new SparkConf(false),
      appArgs: Array[String] = Array(),
      pyFiles: Seq[String] = Nil): KubernetesDriverSpec = {
<<<<<<< HEAD
    val driverConf = new KubernetesDriverSpecificConf(
      resource, MAIN_CLASS, "appName", appArgs, pyFiles = pyFiles)
    val kubernetesConf = KubernetesConf(
      conf,
      driverConf,
      "resource-prefix",
      "appId",
      None,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      hadoopConfSpec = None)
=======
    val kubernetesConf = KubernetesTestConf.createDriverConf(
      sparkConf = conf,
      mainAppResource = resource,
      appArgs = appArgs,
      pyFiles = pyFiles)
>>>>>>> master
    val step = new DriverCommandFeatureStep(kubernetesConf)
    val pod = step.configurePod(SparkPod.initialPod())
    val props = step.getAdditionalPodSystemProperties()
    KubernetesDriverSpec(pod, Nil, props)
  }

}
