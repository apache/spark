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

import java.io.{File, PrintStream}
import java.net.URI

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.util.ArrayImplicits._

private[spark] case class IvyProperties(
    packagesExclusions: String,
    packages: String,
    repositories: String,
    ivyRepoPath: String,
    ivySettingsPath: String)

private[spark] object DependencyUtils extends Logging {

  def getIvyProperties(): IvyProperties = {
    val Seq(packagesExclusions, packages, repositories, ivyRepoPath, ivySettingsPath) = Seq(
      JAR_PACKAGES_EXCLUSIONS.key,
      JAR_PACKAGES.key,
      JAR_REPOSITORIES.key,
      JAR_IVY_REPO_PATH.key,
      JAR_IVY_SETTING_PATH.key
    ).map(sys.props.get(_).orNull)
    IvyProperties(packagesExclusions, packages, repositories, ivyRepoPath, ivySettingsPath)
  }

  /**
   * Download Ivy URI's dependency jars.
   *
   * @param uri Ivy URI need to be downloaded. The URI format should be:
   *              `ivy://group:module:version[?query]`
   *            Ivy URI query part format should be:
   *              `parameter=value&parameter=value...`
   *            Note that currently Ivy URI query part support two parameters:
   *             1. transitive: whether to download dependent jars related to your Ivy URI.
   *                transitive=false or `transitive=true`, if not set, the default value is true.
   *             2. exclude: exclusion list when download Ivy URI jar and dependency jars.
   *                The `exclude` parameter content is a ',' separated `group:module` pair string :
   *                `exclude=group:module,group:module...`
   * @return List of jars downloaded.
   */
  def resolveMavenDependencies(uri: URI): Seq[String] = {
    val ivyProperties = DependencyUtils.getIvyProperties()
    val authority = uri.getAuthority
    if (authority == null) {
      throw new IllegalArgumentException(
        s"Invalid Ivy URI authority in uri ${uri.toString}:" +
          " Expected 'org:module:version', found null.")
    }
    if (authority.split(":").length != 3) {
      throw new IllegalArgumentException(
        s"Invalid Ivy URI authority in uri ${uri.toString}:" +
          s" Expected 'org:module:version', found $authority.")
    }

    val (transitive, exclusionList, repos) = MavenUtils.parseQueryParams(uri)
    val fullReposList = Seq(ivyProperties.repositories, repos)
      .filter(!StringUtils.isBlank(_))
      .mkString(",")
    resolveMavenDependencies(
      transitive,
      exclusionList,
      authority,
      fullReposList,
      ivyProperties.ivyRepoPath,
      Option(ivyProperties.ivySettingsPath)
    )
  }

  def resolveMavenDependencies(
      packagesTransitive: Boolean,
      packagesExclusions: String,
      packages: String,
      repositories: String,
      ivyRepoPath: String,
      ivySettingsPath: Option[String]): Seq[String] = {
    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(packagesExclusions)) {
        packagesExclusions.split(",").toImmutableArraySeq
      } else {
        Nil
      }
    // Create the IvySettings, either load from file or build defaults
    implicit val printStream: PrintStream = SparkSubmit.printStream
    val ivySettings = ivySettingsPath match {
      case Some(path) =>
        MavenUtils.loadIvySettings(path, Option(repositories), Option(ivyRepoPath))

      case None =>
        MavenUtils.buildIvySettings(
          Option(repositories),
          Option(ivyRepoPath))
    }

    MavenUtils.resolveMavenCoordinates(packages, ivySettings,
      transitive = packagesTransitive, exclusions = exclusions)
  }

  def resolveAndDownloadJars(
      jars: String,
      userJar: String,
      sparkConf: SparkConf,
      hadoopConf: Configuration): String = {
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
      .map(downloadFileList(_, targetDir, sparkConf, hadoopConf))
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
   * @return A comma separated local files list.
   */
  def downloadFileList(
      fileList: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration): String = {
    require(fileList != null, "fileList cannot be null.")
    Utils.stringToSeq(fileList)
      .map(downloadFile(_, targetDir, sparkConf, hadoopConf))
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
   * @return Path to the local file.
   */
  def downloadFile(
      path: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration): String = {
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
        val localFile = Utils.doFetchFile(uri.toString(), targetDir, fname, sparkConf, hadoopConf)
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
          logWarning(log"Local jar ${MDC(FILE_NAME, file)} does not exist, skipping.")
        }
      case _ =>
        logWarning(log"Skip remote jar ${MDC(LogKeys.URI, uri)}.")
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
