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
package org.apache.spark.deploy.k8s.submit.submitsteps

import java.io.File

import scala.collection.JavaConverters._

import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.SecretBuilder

import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.MountSmallFilesBootstrap
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec
import org.apache.spark.deploy.k8s.submit.steps.DriverConfigurationStep
import org.apache.spark.util.Utils

private[spark] class DriverMountLocalFilesStep(
    submitterLocalFiles: Iterable[String],
    smallFilesSecretName: String,
    smallFilesSecretMountPath: String,
    mountSmallFilesBootstrap: MountSmallFilesBootstrap) extends DriverConfigurationStep {

  import DriverMountLocalFilesStep._
  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val localFiles = submitterLocalFiles.map { localFileUri =>
      new File(Utils.resolveURI(localFileUri).getPath)
    }
    val totalSizeBytes = localFiles.map(_.length()).sum
    val totalSizeBytesString = Utils.bytesToString(totalSizeBytes)
    require(totalSizeBytes < MAX_SECRET_BUNDLE_SIZE_BYTES,
      s"Total size of all files submitted must be less than $MAX_SECRET_BUNDLE_SIZE_BYTES_STRING." +
        s" Total size for files ended up being $totalSizeBytesString")
    val localFileBase64Contents = localFiles.map { file =>
      val fileBase64 = BaseEncoding.base64().encode(Files.toByteArray(file))
      (file.getName, fileBase64)
    }.toMap
    val localFilesSecret = new SecretBuilder()
      .withNewMetadata()
        .withName(smallFilesSecretName)
        .endMetadata()
      .withData(localFileBase64Contents.asJava)
      .build()
    val (resolvedDriverPod, resolvedDriverContainer) =
      mountSmallFilesBootstrap.mountSmallFilesSecret(
        driverSpec.driverPod, driverSpec.driverContainer)
    val resolvedSparkConf = driverSpec.driverSparkConf.clone()
      .set(EXECUTOR_SUBMITTED_SMALL_FILES_SECRET, smallFilesSecretName)
      .set(EXECUTOR_SUBMITTED_SMALL_FILES_SECRET_MOUNT_PATH, smallFilesSecretMountPath)
    driverSpec.copy(
      driverPod = resolvedDriverPod,
      driverContainer = resolvedDriverContainer,
      driverSparkConf = resolvedSparkConf,
      otherKubernetesResources = driverSpec.otherKubernetesResources ++ Seq(localFilesSecret))
  }
}

private[spark] object DriverMountLocalFilesStep {
  val MAX_SECRET_BUNDLE_SIZE_BYTES = 10240
  val MAX_SECRET_BUNDLE_SIZE_BYTES_STRING =
    Utils.bytesToString(MAX_SECRET_BUNDLE_SIZE_BYTES)
}
