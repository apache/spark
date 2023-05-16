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

import io.fabric8.kubernetes.api.model.{ConfigMap, ContainerBuilder, EnvVarBuilder}

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._

class ReconstructEnvFeatureStepSuite extends SparkFunSuite {

  test("SPARK-43505: Do nothing when no special environment variables are specified") {
    val conf = KubernetesTestConf.createDriverConf()
    val step = new ReconstructEnvFeatureStep(conf)

    val initialPod = SparkPod.initialPod()
    val configuredPod = step.configurePod(initialPod)
    assert(configuredPod === initialPod)

    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  test("SPARK-43505: Special env variables are reconstructed for both driver and executor") {
    val confs = Seq(KubernetesTestConf.createDriverConf(), KubernetesTestConf.createExecutorConf())
    for (conf <- confs) {
      val env = Map(
        "PATH" -> "\"/usr/bin:${JAVA_HOME}/bin:$PATH\"",
        "LD_LIBRARY_PATH" -> "$JAVA_HOME/lib/amd64/server:$HADOOP_HOME/native/lib:$LD_LIBRARY_PATH",
        "JAVA_HOME" -> "/usr/lib/jvm/java-8-openjdk-amd64",
        "HADOOP_HOME" -> "${SPARK_HOME}/../hadoop"
      )
      val envVarList = env.map { case (k, v) =>
        new EnvVarBuilder().withName(k).withValue(v).build()
      }.toSeq
      val step = new ReconstructEnvFeatureStep(conf)
      val initialPod = SparkPod.initialPod().transform { case SparkPod(pod, container) =>
        val containerWithEnv = new ContainerBuilder(container)
          .withEnv(envVarList.asJava)
          .build()
        SparkPod(pod, containerWithEnv)
      }
      val configuredPod = step.configurePod(initialPod)
      // after this step, JAVA_HOME has no dependency, and would be kept as it is
      // PATH, HADOOP_HOME, LD_LIBRARY_PATH variables should be expanded, and would be
      // mounted as a .profile file in config map.
      val expectedContainerEnv = Map(
        "JAVA_HOME" -> "/usr/lib/jvm/java-8-openjdk-amd64"
      )
      val got = configuredPod.container.getEnv.asScala.map { envVar =>
        envVar.getName -> envVar.getValue
      }.toMap
      assert(expectedContainerEnv === got)

      // check config map and volume maps
      assert(configuredPod.pod.getSpec.getVolumes.size() === 1)

      val volume = configuredPod.pod.getSpec.getVolumes.get(0)
      val generatedResourceName = s"${conf.resourceNamePrefix}-$POD_SHELL_PROFILE_CONFIGMAP"
      assert(volume.getName === POD_SHELL_PROFILE_VOLUME)
      assert(volume.getConfigMap.getName === generatedResourceName)
      assert(volume.getConfigMap.getItems.size() === 1)
      assert(volume.getConfigMap.getItems.get(0).getKey === POD_SHELL_PROFILE_KEY)
      assert(volume.getConfigMap.getItems.get(0).getPath === POD_SHELL_PROFILE_FILE_NAME)

      assert(configuredPod.container.getVolumeMounts.size() === 1)
      val volumeMount = configuredPod.container.getVolumeMounts.get(0)
      assert(volumeMount.getMountPath === POD_SHELL_PROFILE_MOUNTPATH)
      assert(volumeMount.getName === POD_SHELL_PROFILE_VOLUME)

      // check config map content
      val additionalResources = step.getAdditionalKubernetesResources()
      assert(additionalResources.length === 1)
      assert(additionalResources.head.getMetadata.getName === generatedResourceName)
      assert(additionalResources.head.isInstanceOf[ConfigMap])
      val configMap = additionalResources.head.asInstanceOf[ConfigMap]
      assert(configMap.getData.size() === 1)
      assert(configMap.getImmutable())
      assert(configMap.getData.containsKey(POD_SHELL_PROFILE_KEY))

      // LD_LIBRARY_PATH depends on HADOOP_HOME, so HADOOP_HOME is placed earlier
      val expectedData =
        """
          |PATH="/usr/bin:${JAVA_HOME}/bin:$PATH"
          |HADOOP_HOME="${SPARK_HOME}/../hadoop"
          |LD_LIBRARY_PATH="$JAVA_HOME/lib/amd64/server:$HADOOP_HOME/native/lib:$LD_LIBRARY_PATH"
          |""".stripMargin.trim
      assert(configMap.getData.get(POD_SHELL_PROFILE_KEY) === expectedData)
    }

  }
}
