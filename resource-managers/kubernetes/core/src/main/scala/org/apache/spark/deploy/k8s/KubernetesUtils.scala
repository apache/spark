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
package org.apache.spark.deploy.k8s

import java.io.File

import io.fabric8.kubernetes.api.model.{Container, Pod, PodBuilder}

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

private[spark] object KubernetesUtils {

  /**
   * Extract and parse Spark configuration properties with a given name prefix and
   * return the result as a Map. Keys must not have more than one value.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing the configuration property keys and values
   */
  def parsePrefixedKeyValuePairs(
      sparkConf: SparkConf,
      prefix: String): Map[String, String] = {
    sparkConf.getAllWithPrefix(prefix).toMap
  }

  def requireNandDefined(opt1: Option[_], opt2: Option[_], errMessage: String): Unit = {
    opt1.foreach { _ => require(opt2.isEmpty, errMessage) }
  }

  /**
   * Append the given init-container to a pod's list of init-containers..
   *
   * @param originalPodSpec original specification of the pod
   * @param initContainer the init-container to add to the pod
   * @return the pod with the init-container added to the list of InitContainers
   */
  def appendInitContainer(originalPodSpec: Pod, initContainer: Container): Pod = {
    new PodBuilder(originalPodSpec)
      .editOrNewSpec()
        .addToInitContainers(initContainer)
        .endSpec()
      .build()
  }

  /**
   * For the given collection of file URIs, resolves them as follows:
   * - File URIs with scheme file:// are resolved to the given download path.
   * - File URIs with scheme local:// resolve to just the path of the URI.
   * - Otherwise, the URIs are returned as-is.
   */
  def resolveFileUris(
      fileUris: Iterable[String],
      fileDownloadPath: String): Iterable[String] = {
    fileUris.map { uri =>
      resolveFileUri(uri, fileDownloadPath, false)
    }
  }

  /**
   * If any file uri has any scheme other than local:// it is mapped as if the file
   * was downloaded to the file download path. Otherwise, it is mapped to the path
   * part of the URI.
   */
  def resolveFilePaths(fileUris: Iterable[String], fileDownloadPath: String): Iterable[String] = {
    fileUris.map { uri =>
      resolveFileUri(uri, fileDownloadPath, true)
    }
  }

  /**
   * Get from a given collection of file URIs the ones that represent remote files.
   */
  def getOnlyRemoteFiles(uris: Iterable[String]): Iterable[String] = {
    uris.filter { uri =>
      val scheme = Utils.resolveURI(uri).getScheme
      scheme != "file" && scheme != "local"
    }
  }

  private def resolveFileUri(
      uri: String,
      fileDownloadPath: String,
      assumesDownloaded: Boolean): String = {
    val fileUri = Utils.resolveURI(uri)
    val fileScheme = Option(fileUri.getScheme).getOrElse("file")
    fileScheme match {
      case "local" =>
        fileUri.getPath
      case _ =>
        if (assumesDownloaded || fileScheme == "file") {
          val fileName = new File(fileUri.getPath).getName
          s"$fileDownloadPath/$fileName"
        } else {
          uri
        }
    }
  }
}
