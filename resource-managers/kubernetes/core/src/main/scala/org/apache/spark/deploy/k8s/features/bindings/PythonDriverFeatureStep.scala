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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, HasMetadata}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config.APP_RESOURCE_TYPE
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep

private[spark] class PythonDriverFeatureStep(
  kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: SparkPod): SparkPod = {
    val roleConf = kubernetesConf.roleSpecificConf
    require(roleConf.mainAppResource.isDefined, "PySpark Main Resource must be defined")
    // Delineation is done by " " because that is input into PythonRunner
    val maybePythonArgs = Option(roleConf.appArgs).filter(_.nonEmpty).map(
      pyArgs =>
        new EnvVarBuilder()
          .withName(ENV_PYSPARK_ARGS)
          .withValue(pyArgs.mkString(" "))
          .build())
    val maybePythonFiles = kubernetesConf.pyFiles().map(
      // Dilineation by ":" is to append the PySpark Files to the PYTHONPATH
      // of the respective PySpark pod
      pyFiles =>
        new EnvVarBuilder()
          .withName(ENV_PYSPARK_FILES)
          .withValue(KubernetesUtils.resolveFileUrisAndPath(pyFiles.split(","))
            .mkString(":"))
          .build())
    val envSeq =
      Seq(new EnvVarBuilder()
          .withName(ENV_PYSPARK_PRIMARY)
          .withValue(KubernetesUtils.resolveFileUri(kubernetesConf.pySparkMainResource().get))
        .build(),
          new EnvVarBuilder()
          .withName(ENV_PYSPARK_MAJOR_PYTHON_VERSION)
          .withValue(kubernetesConf.pySparkPythonVersion())
        .build())
    val pythonEnvs = envSeq ++
      maybePythonArgs.toSeq ++
      maybePythonFiles.toSeq

    val withPythonPrimaryContainer = new ContainerBuilder(pod.container)
        .addAllToEnv(pythonEnvs.asJava)
        .addToArgs("driver-py")
        .addToArgs("--properties-file", SPARK_CONF_PATH)
        .addToArgs("--class", roleConf.mainClass)
      .build()

    SparkPod(pod.pod, withPythonPrimaryContainer)
  }
  override def getAdditionalPodSystemProperties(): Map[String, String] =
    Map(APP_RESOURCE_TYPE.key -> "python")

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
