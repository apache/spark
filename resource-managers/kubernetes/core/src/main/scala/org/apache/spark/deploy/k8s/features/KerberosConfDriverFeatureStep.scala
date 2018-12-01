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

import io.fabric8.kubernetes.api.model.{HasMetadata, Secret, SecretBuilder}
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.hadooputils._
import org.apache.spark.deploy.security.HadoopDelegationTokenManager

/**
 * Runs the necessary Hadoop-based logic based on Kerberos configs and the presence of the
 * HADOOP_CONF_DIR. This runs various bootstrap methods defined in HadoopBootstrapUtil.
 */
private[spark] class KerberosConfDriverFeatureStep(kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep {

  private val hadoopConfDir = Option(kubernetesConf.sparkConf.getenv(ENV_HADOOP_CONF_DIR))
  private val hadoopConfigMapName = kubernetesConf.get(KUBERNETES_HADOOP_CONF_CONFIG_MAP)
  KubernetesUtils.requireNandDefined(
    hadoopConfDir,
    hadoopConfigMapName,
    "Do not specify both the `HADOOP_CONF_DIR` in your ENV and the ConfigMap " +
    "as the creation of an additional ConfigMap, when one is already specified is extraneous")

  private val principal = kubernetesConf.get(org.apache.spark.internal.config.PRINCIPAL)
  private val keytab = kubernetesConf.get(org.apache.spark.internal.config.KEYTAB)
  private val existingSecretName = kubernetesConf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)
  private val existingSecretItemKey = kubernetesConf.get(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY)
  private val krb5File = kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
  private val krb5CMap = kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP)
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(kubernetesConf.sparkConf)
  private val tokenManager = new HadoopDelegationTokenManager(kubernetesConf.sparkConf, hadoopConf)
  private val isKerberosEnabled =
    (hadoopConfDir.isDefined && UserGroupInformation.isSecurityEnabled) ||
      (hadoopConfigMapName.isDefined && (krb5File.isDefined || krb5CMap.isDefined))
  require(keytab.isEmpty || isKerberosEnabled,
    "You must enable Kerberos support if you are specifying a Kerberos Keytab")

  require(existingSecretName.isEmpty || isKerberosEnabled,
    "You must enable Kerberos support if you are specifying a Kerberos Secret")

  KubernetesUtils.requireNandDefined(
    krb5File,
    krb5CMap,
    "Do not specify both a Krb5 local file and the ConfigMap as the creation " +
       "of an additional ConfigMap, when one is already specified, is extraneous")

  KubernetesUtils.requireBothOrNeitherDefined(
    keytab,
    principal,
    "If a Kerberos principal is specified you must also specify a Kerberos keytab",
    "If a Kerberos keytab is specified you must also specify a Kerberos principal")

  KubernetesUtils.requireBothOrNeitherDefined(
    existingSecretName,
    existingSecretItemKey,
    "If a secret data item-key where the data of the Kerberos Delegation Token is specified" +
      " you must also specify the name of the secret",
    "If a secret storing a Kerberos Delegation Token is specified you must also" +
      " specify the item-key where the data is stored")

  private val hadoopConfigurationFiles = hadoopConfDir.map { hConfDir =>
    HadoopBootstrapUtil.getHadoopConfFiles(hConfDir)
  }
  private val newHadoopConfigMapName =
    if (hadoopConfigMapName.isEmpty) {
      Some(kubernetesConf.hadoopConfigMapName)
    } else {
      None
    }

  // Either use pre-existing secret or login to create new Secret with DT stored within
  private val kerberosConfSpec: Option[KerberosConfigSpec] = (for {
    secretName <- existingSecretName
    secretItemKey <- existingSecretItemKey
  } yield {
    KerberosConfigSpec(
      dtSecret = None,
      dtSecretName = secretName,
      dtSecretItemKey = secretItemKey,
      jobUserName = UserGroupInformation.getCurrentUser.getShortUserName)
  }).orElse(
    if (isKerberosEnabled) {
      Some(buildKerberosSpec())
    } else {
      None
    }
  )

  override def configurePod(pod: SparkPod): SparkPod = {
    if (!isKerberosEnabled) {
      return pod
    }

    val hadoopBasedSparkPod = HadoopBootstrapUtil.bootstrapHadoopConfDir(
      hadoopConfDir,
      newHadoopConfigMapName,
      hadoopConfigMapName,
      pod)
    kerberosConfSpec.map { hSpec =>
      HadoopBootstrapUtil.bootstrapKerberosPod(
        hSpec.dtSecretName,
        hSpec.dtSecretItemKey,
        hSpec.jobUserName,
        krb5File,
        Some(kubernetesConf.krbConfigMapName),
        krb5CMap,
        hadoopBasedSparkPod)
    }.getOrElse(
      HadoopBootstrapUtil.bootstrapSparkUserPod(
        UserGroupInformation.getCurrentUser.getShortUserName,
        hadoopBasedSparkPod))
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    if (!isKerberosEnabled) {
      return Map.empty
    }

    val resolvedConfValues = kerberosConfSpec.map { hSpec =>
      Map(KERBEROS_DT_SECRET_NAME -> hSpec.dtSecretName,
        KERBEROS_DT_SECRET_KEY -> hSpec.dtSecretItemKey,
        KERBEROS_SPARK_USER_NAME -> hSpec.jobUserName,
        KRB5_CONFIG_MAP_NAME -> krb5CMap.getOrElse(kubernetesConf.krbConfigMapName))
      }.getOrElse(
        Map(KERBEROS_SPARK_USER_NAME ->
          UserGroupInformation.getCurrentUser.getShortUserName))
    Map(HADOOP_CONFIG_MAP_NAME ->
      hadoopConfigMapName.getOrElse(kubernetesConf.hadoopConfigMapName)) ++ resolvedConfValues
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (!isKerberosEnabled) {
      return Seq.empty
    }

    val hadoopConfConfigMap = for {
      hName <- newHadoopConfigMapName
      hFiles <- hadoopConfigurationFiles
    } yield {
      HadoopBootstrapUtil.buildHadoopConfigMap(hName, hFiles)
    }

    val krb5ConfigMap = krb5File.map { fileLocation =>
      HadoopBootstrapUtil.buildkrb5ConfigMap(
        kubernetesConf.krbConfigMapName,
        fileLocation)
    }

    val kerberosDTSecret = kerberosConfSpec.flatMap(_.dtSecret)

    hadoopConfConfigMap.toSeq ++
      krb5ConfigMap.toSeq ++
      kerberosDTSecret.toSeq
  }

  private def buildKerberosSpec(): KerberosConfigSpec = {
    // The JobUserUGI will be taken fom the Local Ticket Cache or via keytab+principal
    // The login happens in the SparkSubmit so login logic is not necessary to include
    val jobUserUGI = UserGroupInformation.getCurrentUser
    val creds = jobUserUGI.getCredentials
    tokenManager.obtainDelegationTokens(creds)
    val tokenData = SparkHadoopUtil.get.serialize(creds)
    require(tokenData.nonEmpty, "Did not obtain any delegation tokens")
    val newSecretName =
      s"${kubernetesConf.resourceNamePrefix}-$KERBEROS_DELEGEGATION_TOKEN_SECRET_NAME"
    val secretDT =
      new SecretBuilder()
        .withNewMetadata()
          .withName(newSecretName)
          .endMetadata()
        .addToData(KERBEROS_SECRET_KEY, Base64.encodeBase64String(tokenData))
        .build()
    KerberosConfigSpec(
      dtSecret = Some(secretDT),
      dtSecretName = newSecretName,
      dtSecretItemKey = KERBEROS_SECRET_KEY,
      jobUserName = jobUserUGI.getShortUserName)
  }

  private case class KerberosConfigSpec(
      dtSecret: Option[Secret],
      dtSecretName: String,
      dtSecretItemKey: String,
      jobUserName: String)
}
