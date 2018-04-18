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
package org.apache.spark.deploy.k8s.features.bindings

import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.HasMetadata

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesRoleSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants.{ENV_PYSPARK_ARGS, ENV_PYSPARK_FILES, ENV_PYSPARK_PRIMARY}
import org.apache.spark.deploy.k8s.KubernetesUtils
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep

private[spark] class PythonDriverFeatureStep(
  kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: SparkPod): SparkPod = {
    val mainResource = kubernetesConf.pySparkMainResource()
    require(mainResource.isDefined, "PySpark Main Resource must be defined")
    val otherPyFiles = kubernetesConf.pyFiles().map(pyFile =>
      KubernetesUtils.resolveFileUrisAndPath(pyFile.split(","))
        .mkString(",")).getOrElse("")
    val withPythonPrimaryFileContainer = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName(ENV_PYSPARK_ARGS)
        .withValue(kubernetesConf.pySparkAppArgs().getOrElse(""))
        .endEnv()
      .addNewEnv()
        .withName(ENV_PYSPARK_PRIMARY)
        .withValue(KubernetesUtils.resolveFileUri(mainResource.get))
        .endEnv()
      .addNewEnv()
        .withName(ENV_PYSPARK_FILES)
        .withValue(if (otherPyFiles == "") {""} else otherPyFiles)
        .endEnv()
      .build()
    SparkPod(pod.pod, withPythonPrimaryFileContainer)
  }
  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
