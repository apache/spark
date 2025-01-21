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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Constants, KubernetesTestConf, SecretVolumeUtils, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.KubernetesFeaturesTestUtils.containerHasEnvVar
import org.apache.spark.util.{SparkConfWithEnv, Utils}

class HadoopConfExecutorFeatureStepSuite extends SparkFunSuite  {
  import SecretVolumeUtils._

  test("SPARK-43504: mounts the hadoop config map on the executor pod") {
    val confDir = Utils.createTempDir()
    val confFiles = Set("core-site.xml", "hdfs-site.xml")

    confFiles.foreach { f =>
      Files.asCharSink(new File(confDir, f), UTF_8).write("some data")
    }

    Seq(
      Map(ENV_HADOOP_CONF_DIR -> confDir.getAbsolutePath()),
      Map.empty[String, String]).foreach { env =>
      val hasHadoopConf = env.contains(ENV_HADOOP_CONF_DIR)

      val driverSparkConf = new SparkConfWithEnv(env)
      val executorSparkConf = new SparkConf(false)

      val driverConf = KubernetesTestConf.createDriverConf(sparkConf = driverSparkConf)
      val driverStep = new HadoopConfDriverFeatureStep(driverConf)

      val additionalPodSystemProperties = driverStep.getAdditionalPodSystemProperties()
      if (hasHadoopConf) {
        assert(additionalPodSystemProperties.contains(Constants.HADOOP_CONFIG_MAP_NAME))
        additionalPodSystemProperties.foreach { case (key, value) =>
          executorSparkConf.set(key, value)
        }
      } else {
        assert(additionalPodSystemProperties.isEmpty)
      }

      val executorConf = KubernetesTestConf.createExecutorConf(sparkConf = executorSparkConf)
      val executorStep = new HadoopConfExecutorFeatureStep(executorConf)
      val executorPod = executorStep.configurePod(SparkPod.initialPod())

      checkPod(executorPod, hasHadoopConf)
    }
  }

  private def checkPod(pod: SparkPod, hasHadoopConf: Boolean): Unit = {
    if (hasHadoopConf) {
      assert(podHasVolume(pod.pod, HADOOP_CONF_VOLUME))
      assert(containerHasVolume(pod.container, HADOOP_CONF_VOLUME, HADOOP_CONF_DIR_PATH))
      assert(containerHasEnvVar(pod.container, ENV_HADOOP_CONF_DIR))
    } else {
      assert(!podHasVolume(pod.pod, HADOOP_CONF_VOLUME))
      assert(!containerHasVolume(pod.container, HADOOP_CONF_VOLUME, HADOOP_CONF_DIR_PATH))
      assert(!containerHasEnvVar(pod.container, ENV_HADOOP_CONF_DIR))
    }
  }
}
