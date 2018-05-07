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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarBuilder
import io.fabric8.kubernetes.api.model.HasMetadata

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesDriverSpecificConf
import org.apache.spark.deploy.k8s.KubernetesUtils
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep

private[spark] class PythonDriverFeatureStep(
  kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: SparkPod): SparkPod = {
    val roleConf = kubernetesConf.roleSpecificConf
    require(roleConf.mainAppResource.isDefined, "PySpark Main Resource must be defined")
    val maybePythonArgs: Option[EnvVar] = Option(roleConf.appArgs).filter(_.nonEmpty).map(
      s =>
        new EnvVarBuilder()
          .withName(ENV_PYSPARK_ARGS)
          .withValue(s.mkString(","))
          .build())
    val maybePythonFiles: Option[EnvVar] = kubernetesConf.pyFiles().map(
      pyFiles =>
        new EnvVarBuilder()
          .withName(ENV_PYSPARK_FILES)
          .withValue(KubernetesUtils.resolveFileUrisAndPath(pyFiles.split(","))
            .mkString(":"))
          .build())
    val envSeq : Seq[EnvVar] =
      Seq(new EnvVarBuilder()
          .withName(ENV_PYSPARK_PRIMARY)
          .withValue(KubernetesUtils.resolveFileUri(kubernetesConf.pySparkMainResource().get))
        .build(),
          new EnvVarBuilder()
          .withName(ENV_PYSPARK_PYTHON_VERSION)
          .withValue(kubernetesConf.pySparkPythonVersion())
        .build())
    val pythonEnvs = envSeq ++
      maybePythonArgs.toSeq ++
      maybePythonFiles.toSeq

    val withPythonPrimaryContainer = new ContainerBuilder(pod.container)
        .addAllToEnv(pythonEnvs.asJava).build()

    SparkPod(pod.pod, withPythonPrimaryContainer)
  }
  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
