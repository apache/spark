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

import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, ContainerBuilder, HasMetadata, PodBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesRoleSpecificConf
import org.apache.spark.deploy.k8s.features.hadoopsteps.{HadoopConfigSpec, HadoopConfigurationStep}
import org.apache.spark.internal.Logging

 /**
  * This is the main method that runs the hadoopConfigurationSteps defined
  * by the HadoopStepsOrchestrator. These steps are run to modify the
  * SparkPod and Kubernetes Resources using the additive method of the feature steps
  */
private[spark] class HadoopGlobalFeatureStep(
  kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep with Logging {
   private val hadoopTestOrchestrator =
     kubernetesConf.getHadoopStepsOrchestrator
   require(hadoopTestOrchestrator.isDefined, "Ensure that HADOOP_CONF_DIR is defined")
   private val hadoopSteps =
     hadoopTestOrchestrator
       .map(hto => hto.getHadoopSteps(kubernetesConf.getTokenManager))
     .getOrElse(Seq.empty[HadoopConfigurationStep])

   var currentHadoopSpec = HadoopConfigSpec(
     podVolumes = Seq.empty,
     containerEnvs = Seq.empty,
     containerVMs = Seq.empty,
     configMapProperties = Map.empty[String, String],
     dtSecret = None,
     dtSecretName = KERBEROS_DELEGEGATION_TOKEN_SECRET_NAME,
     dtSecretItemKey = None,
     jobUserName = None)

   for (nextStep <- hadoopSteps) {
     currentHadoopSpec = nextStep.configureHadoopSpec(currentHadoopSpec)
   }

  override def configurePod(pod: SparkPod): SparkPod = {
    val hadoopBasedPod = new PodBuilder(pod.pod)
        .editSpec()
          .addAllToVolumes(currentHadoopSpec.podVolumes.asJava)
        .endSpec()
      .build()

    val hadoopBasedContainer = new ContainerBuilder(pod.container)
      .addAllToEnv(currentHadoopSpec.containerEnvs.asJava)
      .addAllToVolumeMounts(currentHadoopSpec.containerVMs.asJava)
      .build()

    val maybeKerberosModification =
      for {
        secretItemKey <- currentHadoopSpec.dtSecretItemKey
        userName <- currentHadoopSpec.jobUserName
      } yield {
        val kerberizedPod = new PodBuilder(hadoopBasedPod)
          .editOrNewSpec()
            .addNewVolume()
              .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
              .withNewSecret()
                .withSecretName(currentHadoopSpec.dtSecretName)
                .endSecret()
              .endVolume()
            .endSpec()
          .build()
        val kerberizedContainer = new ContainerBuilder(hadoopBasedContainer)
          .addNewVolumeMount()
            .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
            .withMountPath(SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
            .endVolumeMount()
          .addNewEnv()
            .withName(ENV_HADOOP_TOKEN_FILE_LOCATION)
            .withValue(s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$secretItemKey")
            .endEnv()
          .addNewEnv()
            .withName(ENV_SPARK_USER)
            .withValue(userName)
            .endEnv()
          .build()
      SparkPod(kerberizedPod, kerberizedContainer) }
    maybeKerberosModification.getOrElse(SparkPod(hadoopBasedPod, hadoopBasedContainer))
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val configMap =
      new ConfigMapBuilder()
        .withNewMetadata()
          .withName(kubernetesConf.getHadoopConfigMapName)
          .endMetadata()
        .addToData(currentHadoopSpec.configMapProperties.asJava)
        .build()
    Seq(configMap) ++ currentHadoopSpec.dtSecret.toSeq
  }
}
