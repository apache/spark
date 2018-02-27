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
   * If any file uri has any scheme other than local:// it is mapped as if the file
   * was downloaded to the file download path. Otherwise, it is mapped to the path
   * part of the URI.
   */
  def resolveFilePaths(fileUris: Iterable[String]): Iterable[String] = {
    fileUris.map { uri =>
      resolveFileUri(uri)
    }
  }

  private def resolveFileUri(uri: String): String = {
    val fileUri = Utils.resolveURI(uri)
    val fileScheme = Option(fileUri.getScheme).getOrElse("file")
    fileScheme match {
      case "local" =>
        fileUri.getPath
    }
  }
}
