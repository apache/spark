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

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, HasMetadata}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesDriverSpecificConf
import org.apache.spark.deploy.k8s.features.hadoopsteps._
import org.apache.spark.internal.Logging

 /**
  * Runs the necessary Hadoop-based logic based on Kerberos configs and the presence of the
  * HADOOP_CONF_DIR. This runs various bootstrap methods defined in HadoopBootstrapUtil.
  */
private[spark] class HadoopGlobalFeatureDriverStep(
    kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
    extends KubernetesFeatureConfigStep with Logging {

    private val conf = kubernetesConf.sparkConf
    private val maybePrincipal = conf.get(org.apache.spark.internal.config.PRINCIPAL)
    private val maybeKeytab = conf.get(org.apache.spark.internal.config.KEYTAB)
    private val maybeExistingSecretName = conf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)
    private val maybeExistingSecretItemKey =
      conf.get(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY)
    private val kubeTokenManager = kubernetesConf.tokenManager
    private val isKerberosEnabled = kubeTokenManager.isSecurityEnabled

    require(maybeKeytab.forall( _ => isKerberosEnabled ),
      "You must enable Kerberos support if you are specifying a Kerberos Keytab")

    require(maybeExistingSecretName.forall( _ => isKerberosEnabled ),
      "You must enable Kerberos support if you are specifying a Kerberos Secret")

    KubernetesUtils.requireBothOrNeitherDefined(
      maybeKeytab,
      maybePrincipal,
      "If a Kerberos principal is specified you must also specify a Kerberos keytab",
      "If a Kerberos keytab is specified you must also specify a Kerberos principal")

    KubernetesUtils.requireBothOrNeitherDefined(
      maybeExistingSecretName,
      maybeExistingSecretItemKey,
      "If a secret data item-key where the data of the Kerberos Delegation Token is specified" +
        " you must also specify the name of the secret",
      "If a secret storing a Kerberos Delegation Token is specified you must also" +
        " specify the item-key where the data is stored")

    require(kubernetesConf.hadoopConfDir.isDefined, "Ensure that HADOOP_CONF_DIR is defined")
    private val hadoopConfDir = kubernetesConf.hadoopConfDir.get
    private val hadoopConfigurationFiles = kubeTokenManager.getHadoopConfFiles(hadoopConfDir)

    // Either use pre-existing secret or login to create new Secret with DT stored within
    private val hadoopSpec: Option[KerberosConfigSpec] = (for {
      secretName <- maybeExistingSecretName
      secretItemKey <- maybeExistingSecretItemKey
    } yield {
      KerberosConfigSpec(
         dtSecret = None,
         dtSecretName = secretName,
         dtSecretItemKey = secretItemKey,
         jobUserName = kubeTokenManager.getCurrentUser.getShortUserName)
    }).orElse(
      if (isKerberosEnabled) {
         Some(HadoopKerberosLogin.buildSpec(
             conf,
             kubernetesConf.appResourceNamePrefix,
             kubeTokenManager))
       } else None )

    override def configurePod(pod: SparkPod): SparkPod = {
      val hadoopBasedSparkPod = HadoopBootstrapUtil.bootstrapHadoopConfDir(
        hadoopConfDir,
        kubernetesConf.hadoopConfigMapName,
        kubeTokenManager,
        pod)
      (for {
        hSpec <- hadoopSpec
        krb5fileLocation <- kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
      } yield {
        HadoopBootstrapUtil.bootstrapKerberosPod(
            hSpec.dtSecretName,
            hSpec.dtSecretItemKey,
            hSpec.jobUserName,
            krb5fileLocation,
            kubernetesConf.kRBConfigMapName,
            hadoopBasedSparkPod)
      }).getOrElse(
        HadoopBootstrapUtil.bootstrapSparkUserPod(
          kubeTokenManager.getCurrentUser.getShortUserName,
          hadoopBasedSparkPod))
    }

    override def getAdditionalPodSystemProperties(): Map[String, String] = {
      val resolvedConfValues = hadoopSpec.map{ hSpec =>
         Map(KERBEROS_KEYTAB_SECRET_NAME -> hSpec.dtSecretName,
            KERBEROS_KEYTAB_SECRET_KEY -> hSpec.dtSecretItemKey,
            KERBEROS_SPARK_USER_NAME -> hSpec.jobUserName)
      }.getOrElse(
          Map(KERBEROS_SPARK_USER_NAME ->
            kubernetesConf.tokenManager.getCurrentUser.getShortUserName))
      Map(HADOOP_CONFIG_MAP_SPARK_CONF_NAME -> kubernetesConf.hadoopConfigMapName,
          HADOOP_CONF_DIR_LOC -> kubernetesConf.hadoopConfDir.get) ++ resolvedConfValues
    }

    override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
      val krb5ConfigMap = kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
        .map(fileLocation => HadoopBootstrapUtil.buildkrb5ConfigMap(
          kubernetesConf.kRBConfigMapName,
          fileLocation))
      val kerberosDTSecret = for {
        hSpec <- hadoopSpec
        kDtSecret <- hSpec.dtSecret
      } yield {
        kDtSecret
      }
      val configMap =
        new ConfigMapBuilder()
          .withNewMetadata()
            .withName(kubernetesConf.hadoopConfigMapName)
            .endMetadata()
          .addToData(hadoopConfigurationFiles.map(file =>
            (file.toPath.getFileName.toString, Files.toString(file, Charsets.UTF_8))).toMap.asJava)
          .build()
      Seq(configMap) ++
        krb5ConfigMap.toSeq ++
        kerberosDTSecret.toSeq
    }
}
