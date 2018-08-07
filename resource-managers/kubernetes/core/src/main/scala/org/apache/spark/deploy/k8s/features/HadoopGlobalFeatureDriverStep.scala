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
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_KERBEROS_KRB5_FILE, KUBERNETES_KERBEROS_PROXY_USER}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesDriverSpecificConf
import org.apache.spark.deploy.k8s.features.hadoopsteps.{HadoopBootstrapUtil, HadoopConfigSpec, HadoopConfigurationStep}
import org.apache.spark.internal.Logging

 /**
  * This is the main method that runs the hadoopConfigurationSteps defined
  * by the HadoopStepsOrchestrator. These steps are run to modify the
  * SparkPod and Kubernetes Resources using the additive method of the feature steps
  */
private[spark] class HadoopGlobalFeatureDriverStep(
  kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
  extends KubernetesFeatureConfigStep with Logging {
   private val hadoopTestOrchestrator =
     kubernetesConf.getHadoopStepsOrchestrator
   require(kubernetesConf.hadoopConfDir.isDefined &&
     hadoopTestOrchestrator.isDefined, "Ensure that HADOOP_CONF_DIR is defined")
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

    val hadoopBasedSparkPod = HadoopBootstrapUtil.bootstrapHadoopConfDir(
      kubernetesConf.hadoopConfDir.get,
      kubernetesConf.getHadoopConfigMapName,
      kubernetesConf.getTokenManager,
      SparkPod(hadoopBasedPod, hadoopBasedContainer))

    val maybeKerberosModification =
      for {
        secretItemKey <- currentHadoopSpec.dtSecretItemKey
        userName <- currentHadoopSpec.jobUserName
        krb5fileLocation <- kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
      } yield {
        HadoopBootstrapUtil.bootstrapKerberosPod(
          currentHadoopSpec.dtSecretName,
          secretItemKey,
          userName,
          krb5fileLocation,
          kubernetesConf.getKRBConfigMapName,
          hadoopBasedSparkPod)
      }
    maybeKerberosModification.getOrElse(
      HadoopBootstrapUtil.bootstrapSparkUserPod(
        kubernetesConf.getTokenManager.getCurrentUser.getShortUserName,
        hadoopBasedSparkPod))
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val maybeKerberosConfValues =
      for {
        secretItemKey <- currentHadoopSpec.dtSecretItemKey
        userName <- currentHadoopSpec.jobUserName
      } yield {
        Map(KERBEROS_KEYTAB_SECRET_NAME -> currentHadoopSpec.dtSecretName,
          KERBEROS_KEYTAB_SECRET_KEY -> secretItemKey,
          KERBEROS_SPARK_USER_NAME -> userName)
    }
    val resolvedConfValues = maybeKerberosConfValues.getOrElse(
        Map(KERBEROS_SPARK_USER_NAME ->
          kubernetesConf.getTokenManager.getCurrentUser.getShortUserName)
      )
    Map(HADOOP_CONFIG_MAP_SPARK_CONF_NAME -> kubernetesConf.getHadoopConfigMapName,
      HADOOP_CONF_DIR_LOC -> kubernetesConf.hadoopConfDir.get) ++ resolvedConfValues
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val krb5ConfigMap = kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
      .map(fileLocation => HadoopBootstrapUtil.buildkrb5ConfigMap(
        kubernetesConf.getKRBConfigMapName,
        fileLocation))
    val configMap =
      new ConfigMapBuilder()
        .withNewMetadata()
          .withName(kubernetesConf.getHadoopConfigMapName)
          .endMetadata()
        .addToData(currentHadoopSpec.configMapProperties.asJava)
        .build()
    Seq(configMap) ++
      krb5ConfigMap.toSeq ++
      currentHadoopSpec.dtSecret.toSeq
  }
}
