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

import java.io.File

import org.apache.spark.util.Utils

private[spark] trait ContainerLocalizedFilesResolver {
  def resolveSubmittedAndRemoteSparkJars(): Seq[String]
  def resolveSubmittedSparkJars(): Seq[String]
  def resolveSubmittedSparkFiles(): Seq[String]
}

private[spark] class ContainerLocalizedFilesResolverImpl(
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    jarsDownloadPath: String,
    filesDownloadPath: String) extends ContainerLocalizedFilesResolver {

  override def resolveSubmittedAndRemoteSparkJars(): Seq[String] = {
    sparkJars.map { jar =>
      val jarUri = Utils.resolveURI(jar)
      Option(jarUri.getScheme).getOrElse("file") match {
        case "local" =>
          jarUri.getPath
        case _ =>
          val jarFileName = new File(jarUri.getPath).getName
          s"$jarsDownloadPath/$jarFileName"
      }
    }
  }

  override def resolveSubmittedSparkJars(): Seq[String] = {
    resolveSubmittedFiles(sparkJars, jarsDownloadPath)
  }

  override def resolveSubmittedSparkFiles(): Seq[String] = {
    resolveSubmittedFiles(sparkFiles, filesDownloadPath)
  }

  private def resolveSubmittedFiles(files: Seq[String], downloadPath: String): Seq[String] = {
    files.map { file =>
      val fileUri = Utils.resolveURI(file)
      Option(fileUri.getScheme).getOrElse("file") match {
        case "file" =>
          val fileName = new File(fileUri.getPath).getName
          s"$downloadPath/$fileName"
        case _ =>
          file
      }
    }
  }
}
