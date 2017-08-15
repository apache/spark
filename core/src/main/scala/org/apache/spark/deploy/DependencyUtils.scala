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

package org.apache.spark.deploy

import java.io.File
import java.nio.file.Files

import scala.collection.mutable.HashMap

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.util.MutableURLClassLoader

private[deploy] object DependencyUtils {

  def resolveMavenDependencies(
      packagesExclusions: String,
      packages: String,
      repositories: String,
      ivyRepoPath: String): String = {
    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(packagesExclusions)) {
        packagesExclusions.split(",")
      } else {
        Nil
      }
    // Create the IvySettings, either load from file or build defaults
    val ivySettings = sys.props.get("spark.jars.ivySettings").map { ivySettingsFile =>
      SparkSubmitUtils.loadIvySettings(ivySettingsFile, Option(repositories), Option(ivyRepoPath))
    }.getOrElse {
      SparkSubmitUtils.buildIvySettings(Option(repositories), Option(ivyRepoPath))
    }

    SparkSubmitUtils.resolveMavenCoordinates(packages, ivySettings, exclusions = exclusions)
  }

  def createTempDir(): File = {
    val targetDir = Files.createTempDirectory("tmp").toFile
    // scalastyle:off runtimeaddshutdownhook
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        FileUtils.deleteQuietly(targetDir)
      }
    })
    // scalastyle:on runtimeaddshutdownhook
    targetDir
  }

  def resolveAndDownloadJars(jars: String, userJar: String): String = {
    val targetDir = DependencyUtils.createTempDir()
    val hadoopConf = new Configuration()
    val sparkProperties = new HashMap[String, String]()
    val securityProperties = List("spark.ssl.fs.trustStore", "spark.ssl.trustStore",
      "spark.ssl.fs.trustStorePassword", "spark.ssl.trustStorePassword",
      "spark.ssl.fs.protocol", "spark.ssl.protocol")

    securityProperties.foreach { pName =>
      sys.props.get(pName).foreach { pValue =>
        sparkProperties.put(pName, pValue)
      }
    }

    Option(jars)
      .map {
        SparkSubmit.resolveGlobPaths(_, hadoopConf)
          .split(",")
          .filterNot(_.contains(userJar.split("/").last))
          .mkString(",")
      }
      .filterNot(_ == "")
      .map(SparkSubmit.downloadFileList(_, targetDir, sparkProperties, hadoopConf))
      .orNull
  }

  def addJarsToClassPath(jars: String, loader: MutableURLClassLoader): Unit = {
    if (jars != null) {
      for (jar <- jars.split(",")) {
        SparkSubmit.addJarToClasspath(jar, loader)
      }
    }
  }
}
