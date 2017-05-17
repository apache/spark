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

import java.io.StringWriter
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.common.io.{BaseEncoding, Files}
import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, Endpoints, HasMetadata, HTTPGetActionBuilder, KeyToPathBuilder, Pod, PodBuilder, SecretBuilder, ServiceBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.internal.readiness.Readiness
import scala.collection.JavaConverters._

import org.apache.spark.SSLOptions
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.submit.v2.ContainerNameEqualityPredicate
import org.apache.spark.util.Utils

/**
 * Launches a pod that runs the resource staging server, exposing it over a NodePort.
 */
private[spark] class ResourceStagingServerLauncher(kubernetesClient: KubernetesClient) {

  private val KEYSTORE_DIR = "/mnt/secrets/spark-staging"
  private val KEYSTORE_FILE = s"$KEYSTORE_DIR/keyStore"
  private val PROPERTIES_FILE_NAME = "staging-server.properties"
  private val PROPERTIES_DIR = "/var/data/spark-staging-server"
  private val PROPERTIES_FILE_PATH = s"$PROPERTIES_DIR/$PROPERTIES_FILE_NAME"

  // Returns the NodePort the staging server is listening on
  def launchStagingServer(sslOptions: SSLOptions): Int = {
    val stagingServerProperties = new Properties()
    val stagingServerSecret = sslOptions.keyStore.map { keyStore =>
      val keyStoreBytes = Files.toByteArray(keyStore)
      val keyStoreBase64 = BaseEncoding.base64().encode(keyStoreBytes)
      new SecretBuilder()
        .withNewMetadata()
          .withName("resource-staging-server-keystore")
          .endMetadata()
        .addToData("keyStore", keyStoreBase64)
        .build()
    }
    stagingServerProperties.setProperty(
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key, sslOptions.enabled.toString)
    sslOptions.keyStorePassword.foreach { password =>
      stagingServerProperties.setProperty(
        "spark.ssl.kubernetes.resourceStagingServer.keyStorePassword", password)
    }
    sslOptions.keyPassword.foreach { password =>
      stagingServerProperties.setProperty(
        "spark.ssl.kubernetes.resourceStagingServer.keyPassword", password)
    }
    stagingServerSecret.foreach { _ =>
      stagingServerProperties.setProperty(
        "spark.ssl.kubernetes.resourceStagingServer.keyStore", KEYSTORE_FILE)
    }
    val propertiesWriter = new StringWriter()
    stagingServerProperties.store(propertiesWriter, "Resource staging server properties.")
    val stagingServerConfigMap = new ConfigMapBuilder()
      .withNewMetadata()
      .withName(s"staging-server-properties")
      .endMetadata()
      .addToData("staging-server", propertiesWriter.toString)
      .build()
    val probePingHttpGet = new HTTPGetActionBuilder()
      .withScheme(if (sslOptions.enabled) "HTTPS" else "HTTP")
      .withPath("/api/v0/ping")
      .withNewPort(RESOURCE_STAGING_SERVER_PORT.defaultValue.get)
      .build()
    val basePod = new PodBuilder()
      .withNewMetadata()
        .withName("resource-staging-server")
        .addToLabels("resource-staging-server", "staging-server")
        .endMetadata()
      .withNewSpec()
        .addNewVolume()
          .withName("staging-server-properties")
          .withNewConfigMap()
            .withName(stagingServerConfigMap.getMetadata.getName)
            .withItems(
              new KeyToPathBuilder()
                .withKey("staging-server")
                .withPath(PROPERTIES_FILE_NAME)
                .build())
            .endConfigMap()
          .endVolume()
        .addNewContainer()
          .withName("staging-server-container")
          .withImage("spark-resource-staging-server:latest")
          .withImagePullPolicy("IfNotPresent")
          .withNewReadinessProbe()
            .withHttpGet(probePingHttpGet)
            .endReadinessProbe()
          .addNewVolumeMount()
            .withName("staging-server-properties")
            .withMountPath(PROPERTIES_DIR)
            .endVolumeMount()
          .addToArgs(PROPERTIES_FILE_PATH)
          .endContainer()
        .endSpec()
    val withMountedKeyStorePod = stagingServerSecret.map { secret =>
      basePod.editSpec()
        .addNewVolume()
          .withName("keystore-volume")
          .withNewSecret()
            .withSecretName(secret.getMetadata.getName)
            .endSecret()
          .endVolume()
        .editMatchingContainer(new ContainerNameEqualityPredicate("staging-server-container"))
          .addNewVolumeMount()
            .withName("keystore-volume")
            .withMountPath(KEYSTORE_DIR)
            .endVolumeMount()
          .endContainer()
        .endSpec()
    }.getOrElse(basePod).build()
    val stagingServerService = new ServiceBuilder()
      .withNewMetadata()
        .withName("resource-staging-server")
        .endMetadata()
      .withNewSpec()
        .withType("NodePort")
        .addToSelector("resource-staging-server", "staging-server")
        .addNewPort()
          .withName("staging-server-port")
          .withPort(RESOURCE_STAGING_SERVER_PORT.defaultValue.get)
          .withNewTargetPort(RESOURCE_STAGING_SERVER_PORT.defaultValue.get)
          .endPort()
        .endSpec()
      .build()
    val stagingServerPodReadyWatcher = new SparkReadinessWatcher[Pod]
    val serviceReadyWatcher = new SparkReadinessWatcher[Endpoints]
    val allResources = Seq(
      stagingServerService,
      stagingServerConfigMap,
      withMountedKeyStorePod) ++
      stagingServerSecret.toSeq
    Utils.tryWithResource(kubernetesClient.pods()
        .withName(withMountedKeyStorePod.getMetadata.getName)
        .watch(stagingServerPodReadyWatcher)) { _ =>
      Utils.tryWithResource(kubernetesClient.endpoints()
          .withName(stagingServerService.getMetadata.getName)
          .watch(serviceReadyWatcher)) { _ =>
        kubernetesClient.resourceList(allResources: _*).createOrReplace()
        stagingServerPodReadyWatcher.waitUntilReady()
        serviceReadyWatcher.waitUntilReady()
      }
    }
    kubernetesClient.services().withName(stagingServerService.getMetadata.getName).get()
      .getSpec
      .getPorts
      .get(0)
      .getNodePort
  }
}
