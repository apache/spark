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
import java.util.UUID

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.mockito.Mockito
import org.scalatest.BeforeAndAfter
import org.scalatest.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.util.Utils

class SparkPodInitContainerSuite extends SparkFunSuite with BeforeAndAfter {

  private val DOWNLOAD_JARS_SECRET_LOCATION = createTempFile("txt")
  private val DOWNLOAD_FILES_SECRET_LOCATION = createTempFile("txt")

  private var downloadJarsDir: File = _
  private var downloadFilesDir: File = _
  private var downloadJarsSecretValue: String = _
  private var downloadFilesSecretValue: String = _
  private var fileFetcher: FileFetcher = _

  override def beforeAll(): Unit = {
    downloadJarsSecretValue = Files.toString(
      new File(DOWNLOAD_JARS_SECRET_LOCATION), Charsets.UTF_8)
    downloadFilesSecretValue = Files.toString(
      new File(DOWNLOAD_FILES_SECRET_LOCATION), Charsets.UTF_8)
  }

  before {
    downloadJarsDir = Utils.createTempDir()
    downloadFilesDir = Utils.createTempDir()
    fileFetcher = mock[FileFetcher]
  }

  after {
    downloadJarsDir.delete()
    downloadFilesDir.delete()
  }

  test("Downloads from remote server should invoke the file fetcher") {
    val sparkConf = getSparkConfForRemoteFileDownloads
    val initContainerUnderTest = new SparkPodInitContainer(sparkConf, fileFetcher)
    initContainerUnderTest.run()
    Mockito.verify(fileFetcher).fetchFile("http://localhost:9000/jar1.jar", downloadJarsDir)
    Mockito.verify(fileFetcher).fetchFile("hdfs://localhost:9000/jar2.jar", downloadJarsDir)
    Mockito.verify(fileFetcher).fetchFile("http://localhost:9000/file.txt", downloadFilesDir)
  }

  private def getSparkConfForRemoteFileDownloads: SparkConf = {
    new SparkConf(true)
      .set(INIT_CONTAINER_REMOTE_JARS,
        "http://localhost:9000/jar1.jar,hdfs://localhost:9000/jar2.jar")
      .set(INIT_CONTAINER_REMOTE_FILES,
        "http://localhost:9000/file.txt")
      .set(JARS_DOWNLOAD_LOCATION, downloadJarsDir.getAbsolutePath)
      .set(FILES_DOWNLOAD_LOCATION, downloadFilesDir.getAbsolutePath)
  }

  private def createTempFile(extension: String): String = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}.$extension")
    Files.write(UUID.randomUUID().toString, file, Charsets.UTF_8)
    file.getAbsolutePath
  }
}
