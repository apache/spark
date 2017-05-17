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
package org.apache.spark.deploy.kubernetes.integrationtest

import io.fabric8.kubernetes.api.model.{HTTPGetActionBuilder, Pod}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.util.Utils

/**
 * Launches a simple HTTP server which provides jars that can be downloaded by Spark applications
 * in integration tests.
 */
private[spark] class StaticAssetServerLauncher(kubernetesClient: KubernetesClient) {

  // Returns the HTTP Base URI of the server.
  def launchStaticAssetServer(): String = {
    val readinessWatcher = new SparkReadinessWatcher[Pod]
    val probePingHttpGet = new HTTPGetActionBuilder()
      .withNewPort(8080)
      .withScheme("HTTP")
      .withPath("/")
      .build()
    Utils.tryWithResource(kubernetesClient
        .pods()
        .withName("integration-test-static-assets")
        .watch(readinessWatcher)) { _ =>
      val pod = kubernetesClient.pods().createNew()
        .withNewMetadata()
          .withName("integration-test-static-assets")
          .endMetadata()
        .withNewSpec()
          .addNewContainer()
            .withName("static-asset-server-container")
            .withImage("spark-integration-test-asset-server:latest")
            .withImagePullPolicy("IfNotPresent")
            .withNewReadinessProbe()
              .withHttpGet(probePingHttpGet)
              .endReadinessProbe()
            .endContainer()
          .endSpec()
        .done()
      readinessWatcher.waitUntilReady()
      val podIP = kubernetesClient.pods().withName(pod.getMetadata.getName).get()
        .getStatus
        .getPodIP
      s"http://$podIP:8080"
    }
  }
}
