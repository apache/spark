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

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._
import org.apache.commons.codec.binary.Base64

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesDriverSpecificConf
import org.apache.spark.deploy.k8s.features.hadooputils._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

/**
 * Runs the necessary Hadoop-based logic based on Kerberos configs and the presence of the
 * HADOOP_CONF_DIR. This runs various bootstrap methods defined in HadoopBootstrapUtil.
 */
private[spark] class KerberosConfDriverFeatureStep(
    kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
  extends KubernetesFeatureConfigStep with Logging {

  require(kubernetesConf.hadoopConfSpec.isDefined,
     "Ensure that HADOOP_CONF_DIR is defined either via env or a pre-defined ConfigMap")
  private val hadoopConfDirSpec = kubernetesConf.hadoopConfSpec.get
  private val conf = kubernetesConf.sparkConf
  private val principal = conf.get(org.apache.spark.internal.config.PRINCIPAL)
  private val keytab = conf.get(org.apache.spark.internal.config.KEYTAB)
  private val existingSecretName = conf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)
  private val existingSecretItemKey = conf.get(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY)
  private val krb5File = conf.get(KUBERNETES_KERBEROS_KRB5_FILE)
  private val krb5CMap = conf.get(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP)
  private val kubeTokenManager = kubernetesConf.tokenManager(conf,
    SparkHadoopUtil.get.newConfiguration(conf))
  private val isKerberosEnabled =
    (hadoopConfDirSpec.hadoopConfDir.isDefined && kubeTokenManager.isSecurityEnabled) ||
      (hadoopConfDirSpec.hadoopConfigMapName.isDefined &&
        (krb5File.isDefined || krb5CMap.isDefined))
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
  private val kerberosConfSpec: Option[KerberosConfigSpec] = (for {
    secretName <- existingSecretName
    secretItemKey <- existingSecretItemKey
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

  private def ktSecretName: String = s"${kubernetesConf.appResourceNamePrefix}-kerberos-keytab"

  override def configurePod(pod: SparkPod): SparkPod = {
    val hadoopBasedSparkPod = HadoopBootstrapUtil.bootstrapHadoopConfDir(
      hadoopConfDirSpec.hadoopConfDir,
      newHadoopConfigMapName,
      hadoopConfDirSpec.hadoopConfigMapName,
      pod)

    val kerberizedPod = kerberosConfSpec.map { hSpec =>
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
        kubeTokenManager.getCurrentUser.getShortUserName,
        hadoopBasedSparkPod))

    if (keytab.isDefined) {
      val podWitKeytab = new PodBuilder(kerberizedPod.pod)
        .editOrNewSpec()
          .addNewVolume()
            .withName(KERBEROS_KEYTAB_VOLUME)
            .withNewSecret()
              .withSecretName(ktSecretName)
              .endSecret()
            .endVolume()
          .endSpec()
        .build()

      val containerWithKeytab = new ContainerBuilder(kerberizedPod.container)
        .addNewVolumeMount()
          .withName(KERBEROS_KEYTAB_VOLUME)
          .withMountPath(KERBEROS_KEYTAB_MOUNT_POINT)
          .endVolumeMount()
        .build()

      SparkPod(podWitKeytab, containerWithKeytab)
    } else {
      kerberizedPod
    }
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val krbConfValues = kerberosConfSpec match {
      case Some(hSpec) =>
        Seq(KERBEROS_DT_SECRET_NAME -> hSpec.dtSecretName,
          KERBEROS_DT_SECRET_KEY -> hSpec.dtSecretItemKey,
          KERBEROS_SPARK_USER_NAME -> hSpec.jobUserName,
          KRB5_CONFIG_MAP_NAME -> krb5CMap.getOrElse(kubernetesConf.krbConfigMapName))

      case _ =>
        Seq(KERBEROS_SPARK_USER_NAME -> kubeTokenManager.getCurrentUser.getShortUserName)
    }

    val keytabConf = keytab.map { path =>
      val ktName = new File(path).getName()
      (KEYTAB.key -> s"$KERBEROS_KEYTAB_MOUNT_POINT/$ktName")
    }

    val hadoopConf = Seq(HADOOP_CONFIG_MAP_NAME ->
      hadoopConfDirSpec.hadoopConfigMapName.getOrElse(kubernetesConf.hadoopConfigMapName))

    (hadoopConf ++ krbConfValues ++ keytabConf).toMap
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
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

    val keytabSecret = keytab.map { kt =>
      val ktName = new File(kt).getName()
      val ktData = Files.toByteArray(new File(kt))
      new SecretBuilder()
        .withNewMetadata()
          .withName(ktSecretName)
          .endMetadata()
        .addToData(ktName, Base64.encodeBase64String(ktData))
        .build()
    }

    hadoopConfConfigMap.toSeq ++
      krb5ConfigMap.toSeq ++
      kerberosDTSecret.toSeq ++
      keytabSecret
  }
}
