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
import org.apache.spark.deploy.k8s.{KubernetesUtils, MountSecretsBootstrap}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.steps._
import org.apache.spark.deploy.k8s.submit.steps.initcontainer.InitContainerConfigOrchestrator
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.SystemClock
import org.apache.spark.util.Utils

/**
 * Figures out and returns the complete ordered list of needed DriverConfigurationSteps to
 * configure the Spark driver pod. The returned steps will be applied one by one in the given
 * order to produce a final KubernetesDriverSpec that is used in KubernetesClientApplication
 * to construct and create the driver pod. It uses the InitContainerConfigOrchestrator to
 * configure the driver init-container if one is needed, i.e., when there are remote dependencies
 * to localize.
 */
private[spark] class DriverConfigOrchestrator(
    kubernetesAppId: String,
    launchTime: Long,
    mainAppResource: Option[MainAppResource],
    appName: String,
    mainClass: String,
    appArgs: Array[String],
    sparkConf: SparkConf) {

  // The resource name prefix is derived from the Spark application name, making it easy to connect
  // the names of the Kubernetes resources from e.g. kubectl or the Kubernetes dashboard to the
  // application the user submitted.
  private val kubernetesResourceNamePrefix = {
    val uuid = UUID.nameUUIDFromBytes(Longs.toByteArray(launchTime)).toString.replaceAll("-", "")
    s"$appName-$uuid".toLowerCase.replaceAll("\\.", "-")
  }

  private val imagePullPolicy = sparkConf.get(CONTAINER_IMAGE_PULL_POLICY)
  private val initContainerConfigMapName = s"$kubernetesResourceNamePrefix-init-config"
  private val jarsDownloadPath = sparkConf.get(JARS_DOWNLOAD_LOCATION)
  private val filesDownloadPath = sparkConf.get(FILES_DOWNLOAD_LOCATION)

  def getAllConfigurationSteps: Seq[DriverConfigurationStep] = {
    val driverCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_DRIVER_LABEL_PREFIX)
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), "Label with key " +
      s"$SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    require(!driverCustomLabels.contains(SPARK_ROLE_LABEL), "Label with key " +
      s"$SPARK_ROLE_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")

    val secretNamesToMountPaths = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_DRIVER_SECRETS_PREFIX)

    val allDriverLabels = driverCustomLabels ++ Map(
      SPARK_APP_ID_LABEL -> kubernetesAppId,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)

    val initialSubmissionStep = new BasicDriverConfigurationStep(
      kubernetesAppId,
      kubernetesResourceNamePrefix,
      allDriverLabels,
      imagePullPolicy,
      appName,
      mainClass,
      appArgs,
      sparkConf)

    val serviceBootstrapStep = new DriverServiceBootstrapStep(
      kubernetesResourceNamePrefix,
      allDriverLabels,
      sparkConf,
      new SystemClock)

    val kubernetesCredentialsStep = new DriverKubernetesCredentialsStep(
      sparkConf, kubernetesResourceNamePrefix)

    val additionalMainAppJar = if (mainAppResource.nonEmpty) {
       val mayBeResource = mainAppResource.get match {
        case JavaMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
          Some(resource)
        case _ => None
      }
      mayBeResource
    } else {
      None
    }

    val sparkJars = sparkConf.getOption("spark.jars")
      .map(_.split(","))
      .getOrElse(Array.empty[String]) ++
      additionalMainAppJar.toSeq
    val sparkFiles = sparkConf.getOption("spark.files")
      .map(_.split(","))
      .getOrElse(Array.empty[String])

    val dependencyResolutionStep = if (sparkJars.nonEmpty || sparkFiles.nonEmpty) {
      Seq(new DependencyResolutionStep(
        sparkJars,
        sparkFiles,
        jarsDownloadPath,
        filesDownloadPath))
    } else {
      Nil
    }

    val initContainerBootstrapStep = if (areAnyFilesNonContainerLocal(sparkJars ++ sparkFiles)) {
      val orchestrator = new InitContainerConfigOrchestrator(
        sparkJars,
        sparkFiles,
        jarsDownloadPath,
        filesDownloadPath,
        imagePullPolicy,
        initContainerConfigMapName,
        INIT_CONTAINER_PROPERTIES_FILE_NAME,
        sparkConf)
      val bootstrapStep = new DriverInitContainerBootstrapStep(
        orchestrator.getAllConfigurationSteps,
        initContainerConfigMapName,
        INIT_CONTAINER_PROPERTIES_FILE_NAME)

      Seq(bootstrapStep)
    } else {
      Nil
    }

    val mountSecretsStep = if (secretNamesToMountPaths.nonEmpty) {
      Seq(new DriverMountSecretsStep(new MountSecretsBootstrap(secretNamesToMountPaths)))
    } else {
      Nil
    }

    Seq(
      initialSubmissionStep,
      serviceBootstrapStep,
      kubernetesCredentialsStep) ++
      dependencyResolutionStep ++
      initContainerBootstrapStep ++
      mountSecretsStep
  }

  private def areAnyFilesNonContainerLocal(files: Seq[String]): Boolean = {
    files.exists { uri =>
      Utils.resolveURI(uri).getScheme != "local"
    }
  }
}
