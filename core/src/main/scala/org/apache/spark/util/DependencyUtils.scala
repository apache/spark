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
import java.net.{URI, URISyntaxException}

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

  private def parseQueryParams(uriQuery: String): (Boolean, String) = {
    if (uriQuery == null) {
      (false, "")
    } else {
      val mapTokens = uriQuery.split("&").map(_.split("="))
      if (mapTokens.exists(_.length != 2)) {
        throw new URISyntaxException(uriQuery, s"Invalid query string: $uriQuery")
      }
      val groupedParams = mapTokens.map(kv => (kv(0), kv(1))).groupBy(_._1)
      // Parse transitive parameters (e.g., transitive=true) in an ivy URL, default value is false
      var transitive = false
      groupedParams.get("transitive").foreach { params =>
        if (params.length > 1) {
          logWarning("It's best to specify `transitive` parameter in ivy URL query only once." +
            " If there are multiple `transitive` parameter, we will select the last one")
        }
        params.map(_._2).foreach {
          case "true" => transitive = true
          case _ => transitive = false
        }
      }
      // Parse an excluded list (e.g., exclude=org.mortbay.jetty:jetty,org.eclipse.jetty:jetty-http)
      // in an ivy URL. When download ivy URL jar, Spark won't download transitive jar
      // in a excluded list.
      val exclusionList = groupedParams.get("exclude").map { params =>
        params.flatMap { case (_, excludeString) =>
          val excludes = excludeString.split(",")
          if (excludes.exists(_.split(":").length != 2)) {
            throw new URISyntaxException(excludeString, "Invalid exclude string: " +
              "expected 'org:module,org:module,..', found " + excludeString)
          }
          excludes
        }.mkString(",")
      }.getOrElse("")

      (transitive, exclusionList)
    }
  }

  /**
   * Download Ivy URIs dependency jars.
   *
   * @param uri Ivy uri need to be downloaded. The URI format should be:
   *              `ivy://group:module:version[?query]`
   *            Ivy URI query part format should be:
   *              `parameter=value&parameter=value...`
   *            Note that currently ivy URI query part support two parameters:
   *             1. transitive: whether to download dependent jars related to your ivy URL.
   *                transitive=false or `transitive=true`, if not set, the default value is false.
   *             2. exclude: exclusion list when download ivy URL jar and dependency jars.
   *                The `exclude` parameter content is a ',' separated `group:module` pair string :
   *                `exclude=group:module,group:module...`
   * @return Comma separated string list of URIs of downloaded jars
   */
  def resolveMavenDependencies(uri: URI): Seq[String] = {
    val Seq(_, _, repositories, ivyRepoPath, ivySettingsPath) =
      DependencyUtils.getIvyProperties()
    val authority = uri.getAuthority
    if (authority == null) {
      throw new URISyntaxException(
        authority, "Invalid url: Expected 'org:module:version', found null")
    }
    if (authority.split(":").length != 3) {
      throw new URISyntaxException(
        authority, "Invalid url: Expected 'org:module:version', found " + authority)
    }

    val (transitive, exclusionList) = parseQueryParams(uri.getQuery)

    resolveMavenDependencies(
      transitive,
      exclusionList,
      authority,
      repositories,
      ivyRepoPath,
      Option(ivySettingsPath)
    ).split(",")
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
