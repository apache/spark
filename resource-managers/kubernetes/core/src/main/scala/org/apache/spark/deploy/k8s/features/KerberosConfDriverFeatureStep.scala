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

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._
import org.apache.commons.codec.binary.Base64

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Runs the necessary Hadoop-based logic based on Kerberos configs and the presence of the
 * HADOOP_CONF_DIR. This runs various bootstrap methods defined in HadoopBootstrapUtil.
 */
private[spark] class KerberosConfDriverFeatureStep(kubernetesConf: KubernetesConf[_])
  extends KubernetesFeatureConfigStep with Logging {

  private val conf = kubernetesConf.sparkConf
  private val principal = conf.get(org.apache.spark.internal.config.PRINCIPAL)
  private val keytab = conf.get(org.apache.spark.internal.config.KEYTAB)
  private val krb5File = conf.get(KUBERNETES_KERBEROS_KRB5_FILE)
  private val krb5CMap = conf.get(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP)

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

  if (!hasKerberosConf) {
    logInfo("You have not specified a krb5.conf file locally or via a ConfigMap. " +
      "Make sure that you have the krb5.conf locally on the driver image.")
  }

  private val needKeytabUpload = keytab.exists(!Utils.isLocalUri(_))

  private def ktSecretName: String = s"${kubernetesConf.appResourceNamePrefix}-kerberos-keytab"

  private def hasKerberosConf: Boolean = krb5CMap.isDefined | krb5File.isDefined

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
          .withName(kubernetesConf.krbConfigMapName)
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
    }.transform { case pod if needKeytabUpload =>
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
            .withName(kubernetesConf.krbConfigMapName)
            .endMetadata()
          .addToData(
            Map(file.getName() -> Files.toString(file, StandardCharsets.UTF_8)).asJava)
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
          .addToData(kt.getName(), Base64.encodeBase64String(Files.toByteArray(kt)))
          .build())
      } else {
        Nil
      }
    }
  }

}
