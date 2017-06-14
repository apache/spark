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
package org.apache.spark.deploy.kubernetes.submit

import org.apache.spark.util.Utils

private[spark] object KubernetesFileUtils {

  private def filterUriStringsByScheme(
      uris: Iterable[String], schemeFilter: (String => Boolean)): Iterable[String] = {
    uris.filter(uri => schemeFilter(Option(Utils.resolveURI(uri).getScheme).getOrElse("file")))
  }

  def getNonSubmitterLocalFiles(uris: Iterable[String]): Iterable[String] = {
    filterUriStringsByScheme(uris, _ != "file")
  }

  def getOnlyContainerLocalFiles(uris: Iterable[String]): Iterable[String] = {
    filterUriStringsByScheme(uris, _ == "local")
  }

  def getOnlySubmitterLocalFiles(uris: Iterable[String]): Iterable[String] = {
    filterUriStringsByScheme(uris, _ == "file")
  }

  def isUriLocalFile(uri: String): Boolean = {
    Option(Utils.resolveURI(uri).getScheme).getOrElse("file") == "file"
  }

  def getOnlyRemoteFiles(uris: Iterable[String]): Iterable[String] = {
    filterUriStringsByScheme(uris, scheme => scheme != "file" && scheme != "local")
  }
}
