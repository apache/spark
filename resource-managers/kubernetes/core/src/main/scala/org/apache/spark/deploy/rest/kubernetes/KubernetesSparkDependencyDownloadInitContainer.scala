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
package org.apache.spark.deploy.rest.kubernetes

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.google.common.util.concurrent.SettableFuture
import okhttp3.ResponseBody
import retrofit2.{Call, Callback, Response}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf, SSLOptions}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.kubernetes.{CompressionUtils, KubernetesCredentials}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

private trait WaitableCallback[T] extends Callback[T] {
  private val complete = SettableFuture.create[Boolean]

  override final def onFailure(call: Call[T], t: Throwable): Unit = complete.setException(t)

  override final def onResponse(call: Call[T], response: Response[T]): Unit = {
    require(response.code() >= 200 && response.code() < 300, Option(response.errorBody())
      .map(_.string())
      .getOrElse(s"Error executing HTTP request, but error body was not provided."))
    handleResponse(response.body())
    complete.set(true)
  }

  protected def handleResponse(body: T): Unit

  final def waitForCompletion(time: Long, timeUnit: TimeUnit): Unit = {
    complete.get(time, timeUnit)
  }
}

private class DownloadTarGzCallback(downloadDir: File) extends WaitableCallback[ResponseBody] {

  override def handleResponse(responseBody: ResponseBody): Unit = {
    Utils.tryWithResource(responseBody.byteStream()) { responseStream =>
      CompressionUtils.unpackTarStreamToDirectory(responseStream, downloadDir)
    }
  }
}
/**
 * Process that fetches files from a resource staging server and/or arbitrary remote locations.
 *
 * The init-container can handle fetching files from any of those sources, but not all of the
 * sources need to be specified. This allows for composing multiple instances of this container
 * with different configurations for different download sources, or using the same container to
 * download everything at once.
 */
private[spark] class KubernetesSparkDependencyDownloadInitContainer(
    sparkConf: SparkConf,
    retrofitClientFactory: RetrofitClientFactory,
    fileFetcher: FileFetcher,
    resourceStagingServerSslOptions: SSLOptions) extends Logging {


  private implicit val downloadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("download-executor"))
  private val maybeResourceStagingServerUri = sparkConf.get(RESOURCE_STAGING_SERVER_URI)

  private val maybeDownloadJarsResourceIdentifier = sparkConf
    .get(INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER)
  private val downloadJarsSecretLocation = new File(
    sparkConf.get(INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION))
  private val maybeDownloadFilesResourceIdentifier = sparkConf
    .get(INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER)
  private val downloadFilesSecretLocation = new File(
    sparkConf.get(INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION))

  private val jarsDownloadDir = new File(
    sparkConf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION))
  private val filesDownloadDir = new File(
    sparkConf.get(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION))

  private val remoteJars = sparkConf.get(INIT_CONTAINER_REMOTE_JARS)
  private val remoteFiles = sparkConf.get(INIT_CONTAINER_REMOTE_FILES)

  private val downloadTimeoutMinutes = sparkConf.get(INIT_CONTAINER_MOUNT_TIMEOUT)

  def run(): Unit = {
    val resourceStagingServerJarsDownload = Future[Unit] {
      downloadResourcesFromStagingServer(
        maybeDownloadJarsResourceIdentifier,
        downloadJarsSecretLocation,
        jarsDownloadDir,
        "Starting to download jars from resource staging server...",
        "Finished downloading jars from resource staging server.",
        s"Application jars download secret provided at" +
          s" ${downloadJarsSecretLocation.getAbsolutePath} does not exist or is not a file.",
        s"Application jars download directory provided at" +
          s" ${jarsDownloadDir.getAbsolutePath} does not exist or is not a directory.")
    }
    val resourceStagingServerFilesDownload = Future[Unit] {
      downloadResourcesFromStagingServer(
        maybeDownloadFilesResourceIdentifier,
        downloadFilesSecretLocation,
        filesDownloadDir,
        "Starting to download files from resource staging server...",
        "Finished downloading files from resource staging server.",
        s"Application files download secret provided at" +
          s" ${downloadFilesSecretLocation.getAbsolutePath} does not exist or is not a file.",
        s"Application files download directory provided at" +
          s" ${filesDownloadDir.getAbsolutePath} does not exist or is not" +
          s" a directory.")
    }
    val remoteJarsDownload = Future[Unit] {
      downloadFiles(remoteJars,
        jarsDownloadDir,
        s"Remote jars download directory specified at $jarsDownloadDir does not exist" +
          s" or is not a directory.")
    }
    val remoteFilesDownload = Future[Unit] {
      downloadFiles(remoteFiles,
        filesDownloadDir,
        s"Remote files download directory specified at $filesDownloadDir does not exist" +
          s" or is not a directory.")
    }
    waitForFutures(
      resourceStagingServerJarsDownload,
      resourceStagingServerFilesDownload,
      remoteJarsDownload,
      remoteFilesDownload)
  }

  private def downloadResourcesFromStagingServer(
      maybeResourceId: Option[String],
      resourceSecretLocation: File,
      resourceDownloadDir: File,
      downloadStartMessage: String,
      downloadFinishedMessage: String,
      errMessageOnSecretNotAFile: String,
      errMessageOnDownloadDirNotADirectory: String): Unit = {
    maybeResourceStagingServerUri.foreach { resourceStagingServerUri =>
      maybeResourceId.foreach { resourceId =>
        require(resourceSecretLocation.isFile, errMessageOnSecretNotAFile)
        require(resourceDownloadDir.isDirectory, errMessageOnDownloadDirNotADirectory)
        val service = retrofitClientFactory.createRetrofitClient(
          resourceStagingServerUri,
          classOf[ResourceStagingServiceRetrofit],
          resourceStagingServerSslOptions)
        val resourceSecret = Files.toString(resourceSecretLocation, Charsets.UTF_8)
        val downloadResourceCallback = new DownloadTarGzCallback(resourceDownloadDir)
        logInfo(downloadStartMessage)
        service.downloadResources(resourceId, resourceSecret).enqueue(downloadResourceCallback)
        downloadResourceCallback.waitForCompletion(downloadTimeoutMinutes, TimeUnit.MINUTES)
        logInfo(downloadFinishedMessage)
      }
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

  private def waitForFutures(futures: Future[_]*) {
    futures.foreach {
      ThreadUtils.awaitResult(_, Duration.create(downloadTimeoutMinutes, TimeUnit.MINUTES))
    }
  }
}

private class FileFetcherImpl(sparkConf: SparkConf, securityManager: SparkSecurityManager)
    extends FileFetcher {
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

object KubernetesSparkDependencyDownloadInitContainer extends Logging {
  def main(args: Array[String]): Unit = {
    logInfo("Starting init-container to download Spark application dependencies.")
    val sparkConf = if (args.nonEmpty) {
      SparkConfPropertiesParser.getSparkConfFromPropertiesFile(new File(args(0)))
    } else {
      new SparkConf(true)
    }
    val securityManager = new SparkSecurityManager(sparkConf)
    val resourceStagingServerSslOptions =
      new ResourceStagingServerSslOptionsProviderImpl(sparkConf).getSslOptions
    val fileFetcher = new FileFetcherImpl(sparkConf, securityManager)
    new KubernetesSparkDependencyDownloadInitContainer(
      sparkConf,
      RetrofitClientFactoryImpl,
      fileFetcher,
      resourceStagingServerSslOptions).run()
    logInfo("Finished downloading application dependencies.")
  }
}
