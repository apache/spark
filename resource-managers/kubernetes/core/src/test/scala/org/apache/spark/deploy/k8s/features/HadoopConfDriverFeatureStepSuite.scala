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

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.ConfigMap

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.{SparkConfWithEnv, Utils}

class HadoopConfDriverFeatureStepSuite extends SparkFunSuite {

  import KubernetesFeaturesTestUtils._
  import SecretVolumeUtils._

  test("mount hadoop config map if defined") {
    val sparkConf = new SparkConf(false)
      .set(Config.KUBERNETES_HADOOP_CONF_CONFIG_MAP, "testConfigMap")
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new HadoopConfDriverFeatureStep(conf)
    checkPod(step.configurePod(SparkPod.initialPod()))
    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  test("create hadoop config map if config dir is defined") {
    val confDir = Utils.createTempDir()
    val confFiles = Set("core-site.xml", "hdfs-site.xml")

    confFiles.foreach { f =>
      Files.write("some data", new File(confDir, f), UTF_8)
    }

    val sparkConf = new SparkConfWithEnv(Map(ENV_HADOOP_CONF_DIR -> confDir.getAbsolutePath()))
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)

    val step = new HadoopConfDriverFeatureStep(conf)
    checkPod(step.configurePod(SparkPod.initialPod()))

    val hadoopConfMap = filter[ConfigMap](step.getAdditionalKubernetesResources()).head
    assert(hadoopConfMap.getData().keySet().asScala === confFiles)
  }

  private def checkPod(pod: SparkPod): Unit = {
    assert(podHasVolume(pod.pod, HADOOP_CONF_VOLUME))
    assert(containerHasVolume(pod.container, HADOOP_CONF_VOLUME, HADOOP_CONF_DIR_PATH))
    assert(containerHasEnvVar(pod.container, ENV_HADOOP_CONF_DIR))
  }

}
