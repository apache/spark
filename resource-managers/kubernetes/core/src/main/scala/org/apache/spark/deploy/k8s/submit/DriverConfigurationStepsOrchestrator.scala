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
package org.apache.spark.deploy.k8s.submit

import java.util.UUID

import com.google.common.primitives.Longs

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.ConfigurationUtils
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.steps._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.SystemClock

/**
 * Constructs the complete list of driver configuration steps to run to deploy the Spark driver.
 */
private[spark] class DriverConfigurationStepsOrchestrator(
    namespace: String,
    kubernetesAppId: String,
    launchTime: Long,
    mainAppResource: Option[MainAppResource],
    appName: String,
    mainClass: String,
    appArgs: Array[String],
    submissionSparkConf: SparkConf) {

  // The resource name prefix is derived from the application name, making it easy to connect the
  // names of the Kubernetes resources from e.g. kubectl or the Kubernetes dashboard to the
  // application the user submitted. However, we can't use the application name in the label, as
  // label values are considerably restrictive, e.g. must be no longer than 63 characters in
  // length. So we generate a separate identifier for the app ID itself, and bookkeeping that
  // requires finding "all pods for this application" should use the kubernetesAppId.
  private val kubernetesResourceNamePrefix = {
    val uuid = UUID.nameUUIDFromBytes(Longs.toByteArray(launchTime))
    s"$appName-$uuid".toLowerCase.replaceAll("\\.", "-")
  }

  private val dockerImagePullPolicy = submissionSparkConf.get(DOCKER_IMAGE_PULL_POLICY)
  private val jarsDownloadPath = submissionSparkConf.get(JARS_DOWNLOAD_LOCATION)
  private val filesDownloadPath = submissionSparkConf.get(FILES_DOWNLOAD_LOCATION)

  def getAllConfigurationSteps(): Seq[DriverConfigurationStep] = {
    val driverCustomLabels = ConfigurationUtils.parsePrefixedKeyValuePairs(
      submissionSparkConf,
      KUBERNETES_DRIVER_LABEL_PREFIX)
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), "Label with key " +
      s"$SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    require(!driverCustomLabels.contains(SPARK_ROLE_LABEL), "Label with key " +
      s"$SPARK_ROLE_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")

    val allDriverLabels = driverCustomLabels ++ Map(
      SPARK_APP_ID_LABEL -> kubernetesAppId,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)

    val initialSubmissionStep = new BaseDriverConfigurationStep(
      kubernetesAppId,
      kubernetesResourceNamePrefix,
      allDriverLabels,
      dockerImagePullPolicy,
      appName,
      mainClass,
      appArgs,
      submissionSparkConf)

    val driverAddressStep = new DriverServiceBootstrapStep(
      kubernetesResourceNamePrefix,
      allDriverLabels,
      submissionSparkConf,
      new SystemClock)

    val kubernetesCredentialsStep = new DriverKubernetesCredentialsStep(
      submissionSparkConf, kubernetesResourceNamePrefix)

    val additionalMainAppJar = if (mainAppResource.nonEmpty) {
       val mayBeResource = mainAppResource.get match {
        case JavaMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
          Some(resource)
        case _ => Option.empty
      }
      mayBeResource
    } else {
      None
    }

    val sparkJars = submissionSparkConf.getOption("spark.jars")
      .map(_.split(","))
      .getOrElse(Array.empty[String]) ++
      additionalMainAppJar.toSeq
    val sparkFiles = submissionSparkConf.getOption("spark.files")
      .map(_.split(","))
      .getOrElse(Array.empty[String])

    val maybeDependencyResolutionStep = if (sparkJars.nonEmpty || sparkFiles.nonEmpty) {
      Some(new DependencyResolutionStep(
        sparkJars,
        sparkFiles,
        jarsDownloadPath,
        filesDownloadPath))
    } else {
      None
    }

    Seq(
      initialSubmissionStep,
      driverAddressStep,
      kubernetesCredentialsStep) ++
      maybeDependencyResolutionStep.toSeq
  }
}
