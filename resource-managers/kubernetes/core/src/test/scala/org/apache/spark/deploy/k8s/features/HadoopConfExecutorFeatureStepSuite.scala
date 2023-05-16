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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.Files
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesTestConf, SecretVolumeUtils, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.KubernetesFeaturesTestUtils.containerHasEnvVar
import org.apache.spark.util.{SparkConfWithEnv, Utils}

class HadoopConfExecutorFeatureStepSuite extends SparkFunSuite with BeforeAndAfter  {
  import SecretVolumeUtils._

  private var baseConf: SparkConf = _

  before {
    baseConf = new SparkConf(false)
  }

  private def newExecutorConf(environment: Map[String, String] = Map.empty):
  KubernetesExecutorConf = {
    KubernetesTestConf.createExecutorConf(
      sparkConf = baseConf,
      environment = environment)
  }

  test("SPARK-43504: mount hadoop config map in executor side") {
    val confDir = Utils.createTempDir()
    val confFiles = Set("core-site.xml", "hdfs-site.xml")

    confFiles.foreach { f =>
      Files.write("some data", new File(confDir, f), UTF_8)
    }

    val sparkConf = new SparkConfWithEnv(Map(ENV_HADOOP_CONF_DIR -> confDir.getAbsolutePath()))
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)

    val driverStep = new HadoopConfDriverFeatureStep(conf)
    driverStep.getAdditionalPodSystemProperties().foreach { case (key, value) =>
      baseConf.set(key, value)
    }

    val executorStep = new HadoopConfExecutorFeatureStep(newExecutorConf())
    val executorPod = executorStep.configurePod(SparkPod.initialPod())

    assert(podHasVolume(executorPod.pod, HADOOP_CONF_VOLUME))
    assert(containerHasVolume(executorPod.container, HADOOP_CONF_VOLUME, HADOOP_CONF_DIR_PATH))
    assert(containerHasEnvVar(executorPod.container, ENV_HADOOP_CONF_DIR))
  }
}
