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

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesExecutorSpecificConf
import org.apache.spark.deploy.k8s.features.hadooputils.HadoopBootstrapUtil
import org.apache.spark.internal.Logging

/**
 * This step is responsible for mounting the DT secret for the executors
 */
private[spark] class KerberosConfExecutorFeatureStep(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf])
  extends KubernetesFeatureConfigStep with Logging {

  private val sparkConf = kubernetesConf.sparkConf
  private val maybeKrb5CMap = sparkConf.getOption(KRB5_CONFIG_MAP_NAME)
  require(maybeKrb5CMap.isDefined, "HADOOP_CONF_DIR ConfigMap not found")

  override def configurePod(pod: SparkPod): SparkPod = {
    logInfo(s"Mounting Resources for Kerberos")
    HadoopBootstrapUtil.bootstrapKerberosPod(
      sparkConf.get(KERBEROS_DT_SECRET_NAME),
      sparkConf.get(KERBEROS_DT_SECRET_KEY),
      sparkConf.get(KERBEROS_SPARK_USER_NAME),
      None,
      None,
      maybeKrb5CMap,
      pod)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty[HasMetadata]
}
