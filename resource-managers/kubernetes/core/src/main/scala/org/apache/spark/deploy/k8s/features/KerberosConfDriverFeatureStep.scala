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
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Provide kerberos / service credentials to the Spark driver.
 *
 * There are three use cases, in order of precedence:
 *
 * - keytab: if a kerberos keytab is defined, it is provided to the driver, and the driver will
 *   manage the kerberos login and the creation of delegation tokens.
 * - existing tokens: if a secret containing delegation tokens is provided, it will be mounted
 *   on the driver pod, and the driver will handle distribution of those tokens to executors.
 * - tgt only: if Hadoop security is enabled, the local TGT will be used to create delegation
 *   tokens which will be provided to the driver. The driver will handle distribution of the
 *   tokens to executors.
 */
private[spark] class KerberosConfDriverFeatureStep(kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {

  private val principal = kubernetesConf.get(org.apache.spark.internal.config.PRINCIPAL)
  private val keytab = kubernetesConf.get(org.apache.spark.internal.config.KEYTAB)
  private val existingSecretName = kubernetesConf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)
  private val existingSecretItemKey = kubernetesConf.get(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY)
  private val krb5File = kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
  private val krb5CMap = kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP)

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

  if (!hasKerberosConf) {
    logInfo("You have not specified a krb5.conf file locally or via a ConfigMap. " +
      "Make sure that you have the krb5.conf locally on the driver image.")
  }

  // Create delegation tokens if needed. This is a lazy val so that it's not populated
  // unnecessarily. But it needs to be accessible to different methods in this class,
  // since it's not clear based solely on available configuration options that delegation
  // tokens are needed when other credentials are not available.
  private lazy val delegationTokens: Array[Byte] = {
    if (keytab.isEmpty && existingSecretName.isEmpty) {
      val tokenManager = new HadoopDelegationTokenManager(kubernetesConf.sparkConf,
        SparkHadoopUtil.get.newConfiguration(kubernetesConf.sparkConf), null)
      val creds = UserGroupInformation.getCurrentUser().getCredentials()
      tokenManager.obtainDelegationTokens(creds)
      // If no tokens and no secrets are stored in the credentials, make sure nothing is returned,
      // to avoid creating an unnecessary secret.
      if (creds.numberOfTokens() > 0 || creds.numberOfSecretKeys() > 0) {
        SparkHadoopUtil.get.serialize(creds)
      } else {
        null
      }
    } else {
      null
    }
  }

  private def needKeytabUpload: Boolean = keytab.exists(!Utils.isLocalUri(_))

  private def dtSecretName: String = s"${kubernetesConf.resourceNamePrefix}-delegation-tokens"

  private def ktSecretName: String = s"${kubernetesConf.resourceNamePrefix}-kerberos-keytab"

  private def hasKerberosConf: Boolean = krb5CMap.isDefined | krb5File.isDefined

  private def newConfigMapName: String = s"${kubernetesConf.resourceNamePrefix}-krb5-file"

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if hasKerberosConf =>
      val configMapVolume = if (krb5CMap.isDefined) {
        new VolumeBuilder()
          .withName(KRB_FILE_VOLUME)
          .withNewConfigMap()
            .withName(krb5CMap.get)
            .endConfigMap()
          .build()
      } else {
        val krb5Conf = new File(krb5File.get)
        new VolumeBuilder()
          .withName(KRB_FILE_VOLUME)
          .withNewConfigMap()
          .withName(newConfigMapName)
          .withItems(new KeyToPathBuilder()
            .withKey(krb5Conf.getName())
            .withPath(krb5Conf.getName())
            .build())
          .endConfigMap()
          .build()
      }

      val podWithVolume = new PodBuilder(pod.pod)
        .editSpec()
          .addNewVolumeLike(configMapVolume)
            .endVolume()
          .endSpec()
        .build()

      val containerWithMount = new ContainerBuilder(pod.container)
        .addNewVolumeMount()
          .withName(KRB_FILE_VOLUME)
          .withMountPath(KRB_FILE_DIR_PATH + "/krb5.conf")
          .withSubPath("krb5.conf")
          .endVolumeMount()
        .build()

      SparkPod(podWithVolume, containerWithMount)
    }.transform {
      case pod if needKeytabUpload =>
        // If keytab is defined and is a submission-local file (not local: URI), then create a
        // secret for it. The keytab data will be stored in this secret below.
        val podWitKeytab = new PodBuilder(pod.pod)
          .editOrNewSpec()
            .addNewVolume()
              .withName(KERBEROS_KEYTAB_VOLUME)
              .withNewSecret()
                .withSecretName(ktSecretName)
                .endSecret()
              .endVolume()
            .endSpec()
          .build()

        val containerWithKeytab = new ContainerBuilder(pod.container)
          .addNewVolumeMount()
            .withName(KERBEROS_KEYTAB_VOLUME)
            .withMountPath(KERBEROS_KEYTAB_MOUNT_POINT)
            .endVolumeMount()
          .build()

        SparkPod(podWitKeytab, containerWithKeytab)

      case pod if existingSecretName.isDefined | delegationTokens != null =>
        val secretName = existingSecretName.getOrElse(dtSecretName)
        val itemKey = existingSecretItemKey.getOrElse(KERBEROS_SECRET_KEY)

        val podWithTokens = new PodBuilder(pod.pod)
          .editOrNewSpec()
            .addNewVolume()
              .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
              .withNewSecret()
                .withSecretName(secretName)
                .endSecret()
              .endVolume()
            .endSpec()
          .build()

        val containerWithTokens = new ContainerBuilder(pod.container)
          .addNewVolumeMount()
            .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
            .withMountPath(SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
            .endVolumeMount()
          .addNewEnv()
            .withName(ENV_HADOOP_TOKEN_FILE_LOCATION)
            .withValue(s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$itemKey")
            .endEnv()
          .build()

        SparkPod(podWithTokens, containerWithTokens)
    }
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    // If a submission-local keytab is provided, update the Spark config so that it knows the
    // path of the keytab in the driver container.
    if (needKeytabUpload) {
      val ktName = new File(keytab.get).getName()
      Map(KEYTAB.key -> s"$KERBEROS_KEYTAB_MOUNT_POINT/$ktName")
    } else {
      Map.empty
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    Seq[HasMetadata]() ++ {
      krb5File.map { path =>
        val file = new File(path)
        new ConfigMapBuilder()
          .withNewMetadata()
            .withName(newConfigMapName)
            .endMetadata()
          .withImmutable(true)
          .addToData(
            Map(file.getName() -> Files.asCharSource(file, StandardCharsets.UTF_8).read()).asJava)
          .build()
      }
    } ++ {
      // If a submission-local keytab is provided, stash it in a secret.
      if (needKeytabUpload) {
        val kt = new File(keytab.get)
        Seq(new SecretBuilder()
          .withNewMetadata()
            .withName(ktSecretName)
            .endMetadata()
          .withImmutable(true)
          .addToData(kt.getName(), Base64.encodeBase64String(Files.toByteArray(kt)))
          .build())
      } else {
        Nil
      }
    } ++ {
      if (delegationTokens != null) {
        Seq(new SecretBuilder()
          .withNewMetadata()
            .withName(dtSecretName)
            .endMetadata()
          .withImmutable(true)
          .addToData(KERBEROS_SECRET_KEY, Base64.encodeBase64String(delegationTokens))
          .build())
      } else {
        Nil
      }
    }
  }
}
