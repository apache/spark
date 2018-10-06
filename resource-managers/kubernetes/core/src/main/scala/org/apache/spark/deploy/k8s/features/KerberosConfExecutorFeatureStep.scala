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

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesExecutorSpecificConf
import org.apache.spark.deploy.k8s.features.hadooputils.HadoopBootstrapUtil
import org.apache.spark.internal.Logging

 /**
  * This step is responsible for mounting the DT secret for the executors
  */
private[spark] class KerberosConfExecutorFeatureStep(
  kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf])
  extends KubernetesFeatureConfigStep with Logging{

  override def configurePod(pod: SparkPod): SparkPod = {
    val sparkConf = kubernetesConf.sparkConf
    val maybeKrb5File = sparkConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
    val maybeKrb5CMap = sparkConf.get(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP)
    KubernetesUtils.requireNandDefined(
      maybeKrb5File,
      maybeKrb5CMap,
      "Do not specify both a Krb5 local file and the ConfigMap as the creation" +
        "of an additional ConfigMap, when one is already specified, is extraneous")
    logInfo(s"Mounting HDFS DT for Secure HDFS")
    HadoopBootstrapUtil.bootstrapKerberosPod(
      sparkConf.get(KERBEROS_KEYTAB_SECRET_NAME),
      sparkConf.get(KERBEROS_KEYTAB_SECRET_KEY),
      sparkConf.get(KERBEROS_SPARK_USER_NAME),
      maybeKrb5File,
      kubernetesConf.kRBConfigMapName,
      maybeKrb5CMap,
      pod)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    kubernetesConf.get(KUBERNETES_KERBEROS_KRB5_FILE)
      .map(fileLocation => HadoopBootstrapUtil.buildkrb5ConfigMap(
        kubernetesConf.kRBConfigMapName,
        fileLocation)).toSeq
  }
}
