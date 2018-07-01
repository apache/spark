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
package org.apache.spark.deploy.k8s.features.hadoopsteps

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.features.OptionRequirements
import org.apache.spark.deploy.k8s.security.KubernetesHadoopDelegationTokenManager
import org.apache.spark.internal.Logging

private[spark] class HadoopStepsOrchestrator(
  conf: SparkConf,
  kubernetesResourceNamePrefix: String,
  hadoopConfDir: String,
  hadoopConfigMapName: String) extends Logging {

  private val isKerberosEnabled = conf.get(KUBERNETES_KERBEROS_SUPPORT)
  private val maybePrincipal = conf.get(KUBERNETES_KERBEROS_PRINCIPAL)
  private val maybeKeytab = conf.get(KUBERNETES_KERBEROS_KEYTAB)
    .map(k => new File(k))
  private val maybeExistingSecretName = conf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)
  private val maybeExistingSecretItemKey =
    conf.get(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY)
  private val maybeRenewerPrincipal =
    conf.get(KUBERNETES_KERBEROS_RENEWER_PRINCIPAL)

  require(maybeKeytab.forall( _ => isKerberosEnabled ),
    "You must enable Kerberos support if you are specifying a Kerberos Keytab")

  require(maybeExistingSecretName.forall( _ => isKerberosEnabled ),
    "You must enable Kerberos support if you are specifying a Kerberos Secret")

  OptionRequirements.requireBothOrNeitherDefined(
    maybeKeytab,
    maybePrincipal,
    "If a Kerberos keytab is specified you must also specify a Kerberos principal",
    "If a Kerberos principal is specified you must also specify a Kerberos keytab")

  OptionRequirements.requireBothOrNeitherDefined(
    maybeExistingSecretName,
    maybeExistingSecretItemKey,
    "If a secret storing a Kerberos Delegation Token is specified you must also" +
      " specify the label where the data is stored",
    "If a secret data item-key where the data of the Kerberos Delegation Token is specified" +
      " you must also specify the name of the secret")

  def getHadoopSteps(kubeTokenManager: KubernetesHadoopDelegationTokenManager):
    Seq[HadoopConfigurationStep] = {
    val hadoopConfigurationFiles = kubeTokenManager.getHadoopConfFiles(hadoopConfDir)
    logInfo(s"Hadoop Conf directory: $hadoopConfDir")
    val hadoopConfMounterStep = new HadoopConfigMounterStep(
      hadoopConfigMapName, hadoopConfigurationFiles)
    val maybeKerberosStep =
      if (isKerberosEnabled) {
        val maybeExistingSecretStep = for {
          secretName <- maybeExistingSecretName
          secretItemKey <- maybeExistingSecretItemKey
        } yield {
          Some(new HadoopKerberosSecretResolverStep(secretName, secretItemKey, kubeTokenManager))
        }
        maybeExistingSecretStep.getOrElse(Some(new HadoopKerberosKeytabResolverStep(
          conf,
          kubernetesResourceNamePrefix,
          maybePrincipal,
          maybeKeytab,
          maybeRenewerPrincipal,
          kubeTokenManager)))
      } else None
    Seq(hadoopConfMounterStep) ++ maybeKerberosStep.toSeq
  }
}
