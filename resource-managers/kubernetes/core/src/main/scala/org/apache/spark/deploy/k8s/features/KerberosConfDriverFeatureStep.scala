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

import io.fabric8.kubernetes.api.model.HasMetadata

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesDriverSpecificConf
import org.apache.spark.deploy.k8s.features.hadooputils._
import org.apache.spark.internal.Logging

 /**
  * Runs the necessary Hadoop-based logic based on Kerberos configs and the presence of the
  * HADOOP_CONF_DIR. This runs various bootstrap methods defined in HadoopBootstrapUtil.
  */
private[spark] class KerberosConfDriverFeatureStep(
   kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
   extends KubernetesFeatureConfigStep with Logging {

   require(kubernetesConf.hadoopConfDir.isDefined,
     "Ensure that HADOOP_CONF_DIR is defined either via env or a pre-defined ConfigMap")
   private val hadoopConfDirSpec = kubernetesConf.hadoopConfDir.get
   private val conf = kubernetesConf.sparkConf
   private val maybePrincipal = conf.get(org.apache.spark.internal.config.PRINCIPAL)
   private val maybeKeytab = conf.get(org.apache.spark.internal.config.KEYTAB)
   private val maybeExistingSecretName = conf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)
   private val maybeExistingSecretItemKey =
     conf.get(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY)
   private val maybeKrb5File =
      conf.get(KUBERNETES_KERBEROS_KRB5_FILE)
   private val maybeKrb5CMap =
     conf.get(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP)
   private val kubeTokenManager = kubernetesConf.tokenManager(conf,
     SparkHadoopUtil.get.newConfiguration(conf))
   private val isKerberosEnabled =
     (hadoopConfDirSpec.hadoopConfDir.isDefined && kubeTokenManager.isSecurityEnabled) ||
       (hadoopConfDirSpec.hadoopConfigMapName.isDefined &&
         (maybeKrb5File.isDefined || maybeKrb5CMap.isDefined))

   require(maybeKeytab.isEmpty || isKerberosEnabled,
     "You must enable Kerberos support if you are specifying a Kerberos Keytab")

   require(maybeExistingSecretName.isEmpty || isKerberosEnabled,
     "You must enable Kerberos support if you are specifying a Kerberos Secret")

   require((maybeKrb5File.isEmpty || maybeKrb5CMap.isEmpty) || isKerberosEnabled,
     "You must specify either a krb5 file location or a ConfigMap with a krb5 file")

   KubernetesUtils.requireNandDefined(
     maybeKrb5File,
     maybeKrb5CMap,
     "Do not specify both a Krb5 local file and the ConfigMap as the creation " +
       "of an additional ConfigMap, when one is already specified, is extraneous")

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

   private val hadoopConfigurationFiles = hadoopConfDirSpec.hadoopConfDir.map { hConfDir =>
      HadoopBootstrapUtil.getHadoopConfFiles(hConfDir)
   }
   private val newHadoopConfigMapName =
     if (hadoopConfDirSpec.hadoopConfigMapName.isEmpty) {
        Some(kubernetesConf.hadoopConfigMapName)
     } else {
        None
     }

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
       } else {
        None
      }
   )

   override def configurePod(pod: SparkPod): SparkPod = {
     val hadoopBasedSparkPod = HadoopBootstrapUtil.bootstrapHadoopConfDir(
        hadoopConfDirSpec.hadoopConfDir,
        newHadoopConfigMapName,
        hadoopConfDirSpec.hadoopConfigMapName,
        pod)
      hadoopSpec.map { hSpec =>
        HadoopBootstrapUtil.bootstrapKerberosPod(
            hSpec.dtSecretName,
            hSpec.dtSecretItemKey,
            hSpec.jobUserName,
            maybeKrb5File,
            Some(kubernetesConf.kRBConfigMapName),
            maybeKrb5CMap,
            hadoopBasedSparkPod)
      }.getOrElse(
        HadoopBootstrapUtil.bootstrapSparkUserPod(
          kubeTokenManager.getCurrentUser.getShortUserName,
          hadoopBasedSparkPod))
    }

   override def getAdditionalPodSystemProperties(): Map[String, String] = {
     val resolvedConfValues = hadoopSpec.map { hSpec =>
       Map(KERBEROS_KEYTAB_SECRET_NAME -> hSpec.dtSecretName,
         KERBEROS_KEYTAB_SECRET_KEY -> hSpec.dtSecretItemKey,
         KERBEROS_SPARK_USER_NAME -> hSpec.jobUserName,
         KRB5_CONFIG_MAP_NAME -> maybeKrb5CMap.getOrElse(kubernetesConf.kRBConfigMapName))
       }.getOrElse(
          Map(KERBEROS_SPARK_USER_NAME ->
            kubeTokenManager.getCurrentUser.getShortUserName))
       Map(HADOOP_CONFIG_MAP_NAME ->
         hadoopConfDirSpec.hadoopConfigMapName.getOrElse(
           kubernetesConf.hadoopConfigMapName)) ++ resolvedConfValues
    }

   override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
     // HADOOP_CONF_DIR ConfigMap
     val hadoopConfConfigMap = for {
        hName <- newHadoopConfigMapName
        hFiles <- hadoopConfigurationFiles
      } yield {
        HadoopBootstrapUtil.buildHadoopConfigMap(
          hName,
          hFiles)
      }

     // krb5 ConfigMap
     val krb5ConfigMap = maybeKrb5File.map { fileLocation =>
       HadoopBootstrapUtil.buildkrb5ConfigMap(
         kubernetesConf.kRBConfigMapName,
         fileLocation)
     }

     // Kerberos DT Secret
     val kerberosDTSecret = for {
        hSpec <- hadoopSpec
        kDtSecret <- hSpec.dtSecret
      } yield {
        kDtSecret
      }

     hadoopConfConfigMap.toSeq ++
       krb5ConfigMap.toSeq ++
       kerberosDTSecret.toSeq
    }
}
