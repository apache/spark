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
package org.apache.spark.deploy.kubernetes.submit.v1

import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model.{Service, ServiceBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Creates the service with an annotation that is expected to be detected by another process
 * which the user provides and is not built in this project. When the external process detects
 * the creation of the service with the appropriate annotation, it is expected to populate the
 * value of a second annotation that is the URI of the driver submission server.
 */
private[spark] class ExternalSuppliedUrisDriverServiceManager
  extends DriverServiceManager with Logging {

  private val externalUriFuture = SettableFuture.create[String]
  private var externalUriSetWatch: Option[Watch] = None

  override def onStart(
      kubernetesClient: KubernetesClient,
      serviceName: String,
      sparkConf: SparkConf): Unit = {
    externalUriSetWatch = Some(kubernetesClient
      .services()
      .withName(serviceName)
      .watch(new ExternalUriSetWatcher(externalUriFuture)))
  }

  override def getServiceManagerType: String = ExternalSuppliedUrisDriverServiceManager.TYPE

  override def customizeDriverService(driverServiceTemplate: ServiceBuilder): ServiceBuilder = {
    require(serviceName != null, "Service name was null; was start() called?")
    driverServiceTemplate
      .editMetadata()
      .addToAnnotations(ANNOTATION_PROVIDE_EXTERNAL_URI, "true")
      .endMetadata()
      .editSpec()
      .withType("ClusterIP")
      .endSpec()
  }

  override def getDriverServiceSubmissionServerUris(driverService: Service): Set[String] = {
    val timeoutSeconds = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_TIMEOUT)
    require(externalUriSetWatch.isDefined, "The watch that listens for the provision of" +
      " the external URI was not started; was start() called?")
    Set(externalUriFuture.get(timeoutSeconds, TimeUnit.SECONDS))
  }

  override def onStop(): Unit = {
    Utils.tryLogNonFatalError {
      externalUriSetWatch.foreach(_.close())
      externalUriSetWatch = None
    }
  }
}

private[spark] object ExternalSuppliedUrisDriverServiceManager {
  val TYPE = "ExternalAnnotation"
}

private[spark] class ExternalUriSetWatcher(externalUriFuture: SettableFuture[String])
  extends Watcher[Service] with Logging {

  override def eventReceived(action: Action, service: Service): Unit = {
    if (action == Action.MODIFIED && !externalUriFuture.isDone) {
      service
        .getMetadata
        .getAnnotations
        .asScala
        .get(ANNOTATION_RESOLVED_EXTERNAL_URI)
        .foreach(externalUriFuture.set)
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {
    logDebug("External URI set watcher closed.", cause)
  }
}

