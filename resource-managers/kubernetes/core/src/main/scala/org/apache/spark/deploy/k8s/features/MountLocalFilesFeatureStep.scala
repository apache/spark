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

import java.io.File
import java.nio.file.Paths

import scala.collection.JavaConverters._

import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, SecretBuilder}

import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.{JavaMainAppResource, PythonMainAppResource, RMainAppResource}
import org.apache.spark.util.Utils

private[spark] class MountLocalDriverFilesFeatureStep(
    kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
  extends MountLocalFilesFeatureStep(kubernetesConf) {

  lazy val allFiles: Seq[String] = {
    Utils.stringToSeq(kubernetesConf.sparkConf.get("spark.files", "")) ++
      kubernetesConf.roleSpecificConf.pyFiles ++
      (kubernetesConf.roleSpecificConf.mainAppResource match {
        case JavaMainAppResource(_) => Nil
        case PythonMainAppResource(res) => Seq(res)
        case RMainAppResource(res) => Seq(res)
      })
  }
}

private[spark] class MountLocalExecutorFilesFeatureStep(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf])
  extends MountLocalFilesFeatureStep(kubernetesConf) {

  lazy val allFiles: Seq[String] = Nil
}

private[spark] abstract class MountLocalFilesFeatureStep(
    kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {
  require(kubernetesConf.mountLocalFilesSecretName.isDefined,
    "Shouldn't be using this feature without a secret name.")
  private val secretName = kubernetesConf.mountLocalFilesSecretName.get

  override def configurePod(pod: SparkPod): SparkPod = {
    val resolvedPod = new PodBuilder(pod.pod)
      .editOrNewSpec()
      .addNewVolume()
      .withName("submitted-files")
      .withNewSecret()
      .withSecretName(secretName)
      .endSecret()
      .endVolume()
      .endSpec()
      .build()
    val resolvedContainer = new ContainerBuilder(pod.container)
      .addNewEnv()
      .withName(ENV_MOUNTED_FILES_FROM_SECRET_DIR)
      .withValue(MOUNTED_FILES_SECRET_DIR)
      .endEnv()
      .addNewVolumeMount()
      .withName("submitted-files")
      .withMountPath(MOUNTED_FILES_SECRET_DIR)
      .endVolumeMount()
      .build()
    SparkPod(resolvedPod, resolvedContainer)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val resolvedFiles = allFiles()
      .map(file => {
        val uri = Utils.resolveURI(file)
        val scheme = Option(uri.getScheme).getOrElse("file")
        if (scheme != "file") {
          file
        } else {
          val fileName = Paths.get(uri.getPath).getFileName.toString
          s"$MOUNTED_FILES_SECRET_DIR/$fileName"
        }
      })
    Map(
      EXECUTOR_SUBMITTED_SMALL_FILES_SECRET.key -> secretName,
      "spark.files" -> resolvedFiles.mkString(","))
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val localFiles = allFiles()
      .map(Utils.resolveURI)
      .filter { file =>
        Option(file.getScheme).getOrElse("file") == "file"
      }
      .map(_.getPath)
      .map(new File(_))
    val localFileBase64Contents = localFiles.map { file =>
      val fileBase64 = BaseEncoding.base64().encode(Files.toByteArray(file))
      (file.getName, fileBase64)
    }.toMap
    val localFilesSecret = new SecretBuilder()
      .withNewMetadata()
      .withName(secretName)
      .endMetadata()
      .withData(localFileBase64Contents.asJava)
      .build()
    Seq(localFilesSecret)
  }

  def allFiles(): Seq[String]
}
