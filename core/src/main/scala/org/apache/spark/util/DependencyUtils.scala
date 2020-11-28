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

package org.apache.spark.util

import java.io.File
import java.net.URI

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.internal.Logging

private[spark] object DependencyUtils extends Logging {

  def getIvyProperties(): Seq[String] = {
    Seq(
      "spark.jars.excludes",
      "spark.jars.packages",
      "spark.jars.repositories",
      "spark.jars.ivy",
      "spark.jars.ivySettings"
    ).map(sys.props.get(_).orNull)
  }


  private def parseURLQueryParameter(queryString: String, queryTag: String): Array[String] = {
    if (queryString == null || queryString.isEmpty) {
      Array.empty[String]
    } else {
      val mapTokens = queryString.split("&")
      assert(mapTokens.forall(_.split("=").length == 2)
        , "Invalid URI query string: [ " + queryString + " ]")
      mapTokens.map(_.split("=")).map(kv => (kv(0), kv(1))).filter(_._1 == queryTag).map(_._2)
    }
  }

  /**
   * Parse excluded list in ivy URL. When download ivy URL jar, Spark won't download transitive jar
   * in excluded list.
   *
   * @param queryString Ivy URI query part string.
   * @return Exclude list which contains grape parameters of exclude.
   *         Example: Input:  exclude=org.mortbay.jetty:jetty,org.eclipse.jetty:jetty-http
   *         Output:  [org.mortbay.jetty:jetty, org.eclipse.jetty:jetty-http]
   */
  private def parseExcludeList(queryString: String): String = {
    parseURLQueryParameter(queryString, "exclude")
      .flatMap { excludeString =>
        val excludes: Array[String] = excludeString.split(",")
        assert(excludes.forall(_.split(":").length == 2),
          "Invalid exclude string: expected 'org:module,org:module,..'," +
            " found [ " + excludeString + " ]")
        excludes
      }.mkString(":")
  }

  /**
   * Parse transitive parameter in ivy URL, default value is false.
   *
   * @param queryString Ivy URI query part string.
   * @return Exclude list which contains grape parameters of transitive.
   *         Example: Input:  exclude=org.mortbay.jetty:jetty&transitive=true
   *         Output:  true
   */
  private def parseTransitive(queryString: String): Boolean = {
    val transitive = parseURLQueryParameter(queryString, "transitive")
    if (transitive.isEmpty) {
      false
    } else {
      if (transitive.length > 1) {
        logWarning("It's best to specify `transitive` parameter in ivy URL query only once." +
          " If there are multiple `transitive` parameter, we will select the last one")
      }
      transitive.last.toBoolean
    }
  }

  /**
   * Download Ivy URIs dependent jars.
   *
   * @param uri Ivy uri need to be downloaded.
   * @return Comma separated string list of URIs of downloaded jars
   */
  def resolveMavenDependencies(uri: URI): String = {
    val Seq(_, _, repositories, ivyRepoPath, ivySettingsPath) =
      DependencyUtils.getIvyProperties()
    resolveMavenDependencies(
      parseTransitive(uri.getQuery),
      parseExcludeList(uri.getQuery),
      uri.getAuthority,
      repositories,
      ivyRepoPath,
      Option(ivySettingsPath)
    )
  }

  def resolveMavenDependencies(
      packagesTransitive: Boolean,
      packagesExclusions: String,
      packages: String,
      repositories: String,
      ivyRepoPath: String,
      ivySettingsPath: Option[String]): String = {
    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(packagesExclusions)) {
        packagesExclusions.split(",")
      } else {
        Nil
      }
    // Create the IvySettings, either load from file or build defaults
    val ivySettings = ivySettingsPath match {
      case Some(path) =>
        SparkSubmitUtils.loadIvySettings(path, Option(repositories), Option(ivyRepoPath))

      case None =>
        SparkSubmitUtils.buildIvySettings(Option(repositories), Option(ivyRepoPath))
    }

    SparkSubmitUtils.resolveMavenCoordinates(packages, ivySettings,
      transitive = packagesTransitive, exclusions = exclusions)
  }

  def resolveAndDownloadJars(
      jars: String,
      userJar: String,
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      secMgr: SecurityManager): String = {
    val targetDir = Utils.createTempDir()
    val userJarName = userJar.split(File.separatorChar).last
    Option(jars)
      .map {
        resolveGlobPaths(_, hadoopConf)
          .split(",")
          .filterNot(_.contains(userJarName))
          .mkString(",")
      }
      .filterNot(_ == "")
      .map(downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr))
      .orNull
  }

  def addJarsToClassPath(jars: String, loader: MutableURLClassLoader): Unit = {
    if (jars != null) {
      for (jar <- jars.split(",")) {
        addJarToClasspath(jar, loader)
      }
    }
  }

  /**
   * Download a list of remote files to temp local files. If the file is local, the original file
   * will be returned.
   *
   * @param fileList A comma separated file list.
   * @param targetDir A temporary directory for which downloaded files.
   * @param sparkConf Spark configuration.
   * @param hadoopConf Hadoop configuration.
   * @param secMgr Spark security manager.
   * @return A comma separated local files list.
   */
  def downloadFileList(
      fileList: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      secMgr: SecurityManager): String = {
    require(fileList != null, "fileList cannot be null.")
    Utils.stringToSeq(fileList)
      .map(downloadFile(_, targetDir, sparkConf, hadoopConf, secMgr))
      .mkString(",")
  }

  /**
   * Download a file from the remote to a local temporary directory. If the input path points to
   * a local path, returns it with no operation.
   *
   * @param path A file path from where the files will be downloaded.
   * @param targetDir A temporary directory for which downloaded files.
   * @param sparkConf Spark configuration.
   * @param hadoopConf Hadoop configuration.
   * @param secMgr Spark security manager.
   * @return Path to the local file.
   */
  def downloadFile(
      path: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      secMgr: SecurityManager): String = {
    require(path != null, "path cannot be null.")
    val uri = Utils.resolveURI(path)

    uri.getScheme match {
      case "file" | "local" => path
      case "http" | "https" | "ftp" if Utils.isTesting =>
        // This is only used for SparkSubmitSuite unit test. Instead of downloading file remotely,
        // return a dummy local path instead.
        val file = new File(uri.getPath)
        new File(targetDir, file.getName).toURI.toString
      case _ =>
        val fname = new Path(uri).getName()
        val localFile = Utils.doFetchFile(uri.toString(), targetDir, fname, sparkConf, secMgr,
          hadoopConf)
        localFile.toURI().toString()
    }
  }

  def resolveGlobPaths(paths: String, hadoopConf: Configuration): String = {
    require(paths != null, "paths cannot be null.")
    Utils.stringToSeq(paths).flatMap { path =>
      val (base, fragment) = splitOnFragment(path)
      (resolveGlobPath(base, hadoopConf), fragment) match {
        case (resolved, Some(_)) if resolved.length > 1 => throw new SparkException(
            s"${base.toString} resolves ambiguously to multiple files: ${resolved.mkString(",")}")
        case (resolved, Some(namedAs)) => resolved.map(_ + "#" + namedAs)
        case (resolved, _) => resolved
      }
    }.mkString(",")
  }

  def addJarToClasspath(localJar: String, loader: MutableURLClassLoader): Unit = {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          logWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        logWarning(s"Skip remote jar $uri.")
    }
  }

  /**
   * Merge a sequence of comma-separated file lists, some of which may be null to indicate
   * no files, into a single comma-separated string.
   */
  def mergeFileLists(lists: String*): String = {
    val merged = lists.filterNot(StringUtils.isBlank)
      .flatMap(Utils.stringToSeq)
    if (merged.nonEmpty) merged.mkString(",") else null
  }

  private def splitOnFragment(path: String): (URI, Option[String]) = {
    val uri = Utils.resolveURI(path)
    val withoutFragment = new URI(uri.getScheme, uri.getSchemeSpecificPart, null)
    (withoutFragment, Option(uri.getFragment))
  }

  private def resolveGlobPath(uri: URI, hadoopConf: Configuration): Array[String] = {
    uri.getScheme match {
      case "local" | "http" | "https" | "ftp" => Array(uri.toString)
      case _ =>
        val fs = FileSystem.get(uri, hadoopConf)
        Option(fs.globStatus(new Path(uri))).map { status =>
          status.filter(_.isFile).map(_.getPath.toUri.toString)
        }.getOrElse(Array(uri.toString))
    }
  }

}
