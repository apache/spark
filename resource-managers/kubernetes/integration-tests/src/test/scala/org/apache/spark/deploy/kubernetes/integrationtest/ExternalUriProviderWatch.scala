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

import java.util.concurrent.atomic.AtomicBoolean

import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._

import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.integrationtest.backend.minikube.Minikube
import org.apache.spark.internal.Logging

/**
 * A slightly unrealistic implementation of external URI provision, but works
 * for tests - essentially forces the service to revert back to being exposed
 * on NodePort.
 */
private[spark] class ExternalUriProviderWatch(kubernetesClient: KubernetesClient)
    extends Watcher[Service] with Logging {

  // Visible for testing
  val annotationSet = new AtomicBoolean(false)

  override def eventReceived(action: Action, service: Service): Unit = {
    if (action == Action.ADDED) {
      service.getMetadata
          .getAnnotations
          .asScala
          .get(ANNOTATION_PROVIDE_EXTERNAL_URI).foreach { _ =>
        if (!annotationSet.getAndSet(true)) {
          val nodePortService = kubernetesClient.services().withName(service.getMetadata.getName)
            .edit()
              .editSpec()
                .withType("NodePort")
                .endSpec()
            .done()
          val submissionServerPort = nodePortService
            .getSpec()
            .getPorts
            .asScala
            .find(_.getName == SUBMISSION_SERVER_PORT_NAME)
            .map(_.getNodePort)
            .getOrElse(throw new IllegalStateException("Submission server port not found."))
          val resolvedNodePortUri = s"http://${Minikube.getMinikubeIp}:$submissionServerPort"
          kubernetesClient.services().withName(service.getMetadata.getName).edit()
            .editMetadata()
              .addToAnnotations(ANNOTATION_RESOLVED_EXTERNAL_URI, resolvedNodePortUri)
              .endMetadata()
            .done()
        }
      }
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {
    logWarning("External URI provider watch closed.", cause)
  }
}
