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
package org.apache.spark.deploy.rest.k8s

import java.io.File
import java.util.concurrent.TimeUnit

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Process that fetches files from a resource staging server and/or arbitrary remote locations.
 *
 * The init-container can handle fetching files from any of those sources, but not all of the
 * sources need to be specified. This allows for composing multiple instances of this container
 * with different configurations for different download sources, or using the same container to
 * download everything at once.
 */
private[spark] class SparkPodInitContainer(
    sparkConf: SparkConf,
    fileFetcher: FileFetcher) extends Logging {

  private implicit val downloadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("download-executor"))

  private val jarsDownloadDir = new File(
    sparkConf.get(JARS_DOWNLOAD_LOCATION))
  private val filesDownloadDir = new File(
    sparkConf.get(FILES_DOWNLOAD_LOCATION))

  private val remoteJars = sparkConf.get(INIT_CONTAINER_REMOTE_JARS)
  private val remoteFiles = sparkConf.get(INIT_CONTAINER_REMOTE_FILES)

  private val downloadTimeoutMinutes = sparkConf.get(INIT_CONTAINER_MOUNT_TIMEOUT)

  def run(): Unit = {
    val remoteJarsDownload = Future[Unit] {
      logInfo(s"Downloading remote jars: $remoteJars")
      downloadFiles(
        remoteJars,
        jarsDownloadDir,
        s"Remote jars download directory specified at $jarsDownloadDir does not exist " +
          "or is not a directory.")
    }
    val remoteFilesDownload = Future[Unit] {
      logInfo(s"Downloading remote files: $remoteFiles")
      downloadFiles(
        remoteFiles,
        filesDownloadDir,
        s"Remote files download directory specified at $filesDownloadDir does not exist " +
          "or is not a directory.")
    }

    Seq(remoteJarsDownload, remoteFilesDownload).foreach {
      ThreadUtils.awaitResult(_, Duration.create(downloadTimeoutMinutes, TimeUnit.MINUTES))
    }
  }

  private def downloadFiles(
      filesCommaSeparated: Option[String],
      downloadDir: File,
      errMessageOnDestinationNotADirectory: String): Unit = {
    if (filesCommaSeparated.isDefined) {
      require(downloadDir.isDirectory, errMessageOnDestinationNotADirectory)
    }
    filesCommaSeparated.map(_.split(",")).toSeq.flatten.foreach { file =>
      fileFetcher.fetchFile(file, downloadDir)
    }
  }
}

private class FileFetcher(sparkConf: SparkConf, securityManager: SparkSecurityManager) {

  def fetchFile(uri: String, targetDir: File): Unit = {
    Utils.fetchFile(
      url = uri,
      targetDir = targetDir,
      conf = sparkConf,
      securityMgr = securityManager,
      hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf),
      timestamp = System.currentTimeMillis(),
      useCache = false)
  }
}

object SparkPodInitContainer extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo("Starting init-container to download Spark application dependencies.")
    val sparkConf = if (args.nonEmpty) {
      SparkConfPropertiesParser.getSparkConfFromPropertiesFile(new File(args(0)))
    } else {
      new SparkConf(true)
    }

    val securityManager = new SparkSecurityManager(sparkConf)
    val fileFetcher = new FileFetcher(sparkConf, securityManager)
    new SparkPodInitContainer(sparkConf, fileFetcher).run()
    logInfo("Finished downloading application dependencies.")
  }
}
