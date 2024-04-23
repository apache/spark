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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.networking.v1.{HTTPIngressPathBuilder, IngressBuilder, IngressRuleBuilder, ServiceBackendPortBuilder}

import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants.SPARK_APP_ID_LABEL
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI._

class DriverIngressFeatureStep(kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {

  private lazy val driverServiceName: String = kubernetesConf.driverServiceName

  override def configurePod(pod: SparkPod): SparkPod = pod

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (!kubernetesConf.get(UI_ENABLED) || !kubernetesConf.get(KUBERNETES_INGRESS_ENABLED)) {
      logInfo(
        s"Skip building ingress for Spark UI, due to" +
          s"${UI_ENABLED.key} or ${KUBERNETES_INGRESS_ENABLED.key} is false.")
      return Seq.empty
    }

    val appId = kubernetesConf.appId
    val uiPort = kubernetesConf.get(UI_PORT)
    val ingressHost = kubernetesConf.get(KUBERNETES_INGRESS_HOST_PATTERN) match {
      case Some(ingressHostPattern) =>
        ingressHostPattern.replace("{{APP_ID}}", appId)
      case None =>
        logWarning(s"Skip building ingress for Spark UI, due to " +
          s"${KUBERNETES_INGRESS_HOST_PATTERN.key} is absent.")
        return Seq.empty
    }

    val customLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      kubernetesConf.sparkConf,
      KUBERNETES_DRIVER_INGRESS_LABEL_PREFIX)
    val labels = customLabels ++ Map(SPARK_APP_ID_LABEL -> appId)

    val annotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      kubernetesConf.sparkConf,
      KUBERNETES_DRIVER_INGRESS_ANNOTATION_PREFIX)

    val path = new HTTPIngressPathBuilder()
      .withPath(kubernetesConf.get(KUBERNETES_INGRESS_PATH))
      .withPathType(kubernetesConf.get(KUBERNETES_INGRESS_PATH_TYPE))
      .withNewBackend()
        .withNewService()
          .withName(driverServiceName)
          .withPort(new ServiceBackendPortBuilder().withNumber(uiPort).build())
        .endService()
      .endBackend()
      .build()

    val uiRule = new IngressRuleBuilder()
      .withHost(ingressHost)
      .withNewHttp()
        .addToPaths(path)
      .endHttp()
      .build()

    val ingress = new IngressBuilder()
      .withNewMetadata()
        .withName(s"$driverServiceName-ingress")
        .addToLabels(labels.asJava)
        .addToAnnotations(annotations.asJava)
      .endMetadata()
      .withNewSpec()
        .withIngressClassName(kubernetesConf.get(KUBERNETES_INGRESS_CLASS_NAME).orNull)
        .withRules(uiRule)
      .endSpec()
      .build()
    Seq(ingress)
  }
}
