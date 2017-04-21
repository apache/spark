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

import io.fabric8.kubernetes.api.model.{Service, ServiceBuilder}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkConf

/**
 * Implementations of this interface are responsible for exposing the driver pod by:
 * - Creating a Kubernetes Service that is backed by the driver pod, and
 * - Providing one or more URIs that the service can be reached at from the submission client.
 *
 * In general, one should not need to implement custom variants of this interface. Consider
 * if the built-in service managers, NodePort and ExternalAnnotation, suit your needs first.
 *
 * This API is in an alpha state and may break without notice.
 */
trait DriverServiceManager {

  protected var kubernetesClient: KubernetesClient = _
  protected var serviceName: String = _
  protected var sparkConf: SparkConf = _

  /**
   * The tag that identifies this service manager type. This service manager will be loaded
   * only if the Spark configuration spark.kubernetes.driver.serviceManagerType matches this
   * value.
   */
  def getServiceManagerType: String

  final def start(
      kubernetesClient: KubernetesClient,
      serviceName: String,
      sparkConf: SparkConf): Unit = {
    this.kubernetesClient = kubernetesClient
    this.serviceName = serviceName
    this.sparkConf = sparkConf
    onStart(kubernetesClient, serviceName, sparkConf)
  }

  /**
   * Guaranteed to be called before {@link createDriverService} or
   * {@link getDriverServiceSubmissionServerUris} is called.
   */
  protected def onStart(
      kubernetesClient: KubernetesClient,
      serviceName: String,
      sparkConf: SparkConf): Unit = {}

  /**
   * Customize the driver service that overlays on the driver pod.
   *
   * Implementations are expected to take the service template and adjust it
   * according to the particular needs of how the Service will be accessed by
   * URIs provided in {@link getDriverServiceSubmissionServerUris}.
   *
   * @param driverServiceTemplate Base settings for the driver service.
   * @return The same ServiceBuilder object with any required customizations.
   */
  def customizeDriverService(driverServiceTemplate: ServiceBuilder): ServiceBuilder

  /**
   * Return the set of URIs that can be used to reach the submission server that
   * is running on the driver pod.
   */
  def getDriverServiceSubmissionServerUris(driverService: Service): Set[String]

  /**
   * Called when the Spark application failed to start. Allows the service
   * manager to clean up any state it may have created that should not be persisted
   * in the case of an unsuccessful launch. Note that stop() is still called
   * regardless if this method is called.
   */
  def handleSubmissionError(cause: Throwable): Unit = {}

  final def stop(): Unit = onStop()

  /**
   * Perform any cleanup of this service manager.
   * the super implementation.
   */
  protected def onStop(): Unit = {}
}
