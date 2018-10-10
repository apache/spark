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
package org.apache.spark.shuffle.k8s

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.SparkKubernetesClientFactory
import org.apache.spark.shuffle.{DefaultShuffleServiceAddressProvider, ShuffleServiceAddressProvider, ShuffleServiceAddressProviderFactory}
import org.apache.spark.util.ThreadUtils

class KubernetesShuffleServiceAddressProviderFactory extends ShuffleServiceAddressProviderFactory {
  override def canCreate(masterUrl: String): Boolean = masterUrl.startsWith("k8s://")

  override def create(conf: SparkConf): ShuffleServiceAddressProvider = {
    if (conf.get(KUBERNETES_BACKUP_SHUFFLE_SERVICE_ENABLED)) {
      val kubernetesClient = SparkKubernetesClientFactory.getDriverKubernetesClient(
        conf, conf.get("spark.master"))
      val pollForPodsExecutor = ThreadUtils.newDaemonThreadPoolScheduledExecutor(
          "poll-shuffle-service-pods", 1)
      val shuffleServiceLabels = conf.getAllWithPrefix(KUBERNETES_BACKUP_SHUFFLE_SERVICE_LABELS)
      val shuffleServicePodsNamespace = conf.get(KUBERNETES_BACKUP_SHUFFLE_SERVICE_PODS_NAMESPACE)
      require(shuffleServicePodsNamespace.isDefined, "Namespace for the pods running the backup" +
        s" shuffle service must be defined by" +
        s" ${KUBERNETES_BACKUP_SHUFFLE_SERVICE_PODS_NAMESPACE.key}")
      require(shuffleServiceLabels.nonEmpty, "Requires labels for the backup shuffle service pods.")

      val port: Int = conf.get(KUBERNETES_BACKUP_SHUFFLE_SERVICE_PORT)
      new KubernetesShuffleServiceAddressProvider(
        kubernetesClient,
        pollForPodsExecutor,
        shuffleServiceLabels.toMap,
        shuffleServicePodsNamespace.get,
        port)
    } else DefaultShuffleServiceAddressProvider
  }
}
