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

package org.apache.spark.deploy.rest.kubernetes.v2

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.google.common.util.concurrent.SettableFuture
import okhttp3.ResponseBody
import retrofit2.{Call, Callback, Response}

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

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

private[spark] class KubernetesSparkDependencyDownloadInitContainer(
    sparkConf: SparkConf, retrofitClientFactory: RetrofitClientFactory) extends Logging {

  private val resourceStagingServerUri = sparkConf.get(RESOURCE_STAGING_SERVER_URI)
    .getOrElse(throw new SparkException("No dependency server URI was provided."))

  private val downloadJarsResourceIdentifier = sparkConf
    .get(INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER)
    .getOrElse(throw new SparkException("No resource identifier provided for jars."))
  private val downloadJarsSecretLocation = new File(
    sparkConf.get(INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION))
  private val downloadFilesResourceIdentifier = sparkConf
    .get(INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER)
    .getOrElse(throw new SparkException("No resource identifier provided for files."))
  private val downloadFilesSecretLocation = new File(
    sparkConf.get(INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION))
  require(downloadJarsSecretLocation.isFile, "Application jars download secret provided" +
    s" at ${downloadJarsSecretLocation.getAbsolutePath} does not exist or is not a file.")
  require(downloadFilesSecretLocation.isFile, "Application files download secret provided" +
    s" at ${downloadFilesSecretLocation.getAbsolutePath} does not exist or is not a file.")

  private val jarsDownloadDir = new File(sparkConf.get(DRIVER_LOCAL_JARS_DOWNLOAD_LOCATION))
  require(jarsDownloadDir.isDirectory, "Application jars download directory provided at" +
    s" ${jarsDownloadDir.getAbsolutePath} does not exist or is not a directory.")

  private val filesDownloadDir = new File(sparkConf.get(DRIVER_LOCAL_FILES_DOWNLOAD_LOCATION))
  require(filesDownloadDir.isDirectory, "Application files download directory provided at" +
    s" ${filesDownloadDir.getAbsolutePath} does not exist or is not a directory.")
  private val downloadTimeoutMinutes = sparkConf.get(DRIVER_MOUNT_DEPENDENCIES_INIT_TIMEOUT)

  def run(): Unit = {
    val securityManager = new SparkSecurityManager(sparkConf)
    val sslOptions = securityManager.getSSLOptions("kubernetes.resourceStagingServer")
    val service = retrofitClientFactory.createRetrofitClient(
      resourceStagingServerUri, classOf[ResourceStagingServiceRetrofit], sslOptions)
    val jarsSecret = Files.toString(downloadJarsSecretLocation, Charsets.UTF_8)
    val filesSecret = Files.toString(downloadFilesSecretLocation, Charsets.UTF_8)
    val downloadJarsCallback = new DownloadTarGzCallback(jarsDownloadDir)
    val downloadFilesCallback = new DownloadTarGzCallback(filesDownloadDir)
    service.downloadResources(downloadJarsResourceIdentifier, jarsSecret)
      .enqueue(downloadJarsCallback)
    service.downloadResources(downloadFilesResourceIdentifier, filesSecret)
      .enqueue(downloadFilesCallback)
    logInfo("Waiting to download jars...")
    downloadJarsCallback.waitForCompletion(downloadTimeoutMinutes, TimeUnit.MINUTES)
    logInfo(s"Jars downloaded to ${jarsDownloadDir.getAbsolutePath}")
    logInfo("Waiting to download files...")
    downloadFilesCallback.waitForCompletion(downloadTimeoutMinutes, TimeUnit.MINUTES)
    logInfo(s"Files downloaded to ${filesDownloadDir.getAbsolutePath}")
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
    new KubernetesSparkDependencyDownloadInitContainer(sparkConf, RetrofitClientFactoryImpl).run()
    logInfo("Finished downloading application dependencies.")
  }
}
