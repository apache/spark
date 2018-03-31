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
package org.apache.spark.deploy.k8s.submit.steps

import java.io.File

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, HasMetadata, Pod, PodBuilder, Secret}
import org.mockito.MockitoAnnotations
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.MountSmallFilesBootstrap
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec
import org.apache.spark.deploy.k8s.submit.submitsteps.DriverMountLocalFilesStep
import org.apache.spark.util.Utils

private[spark] class DriverMountLocalFilesStepSuite extends SparkFunSuite with BeforeAndAfter {

  private val FIRST_TEMP_FILE_NAME = "file1.txt"
  private val SECOND_TEMP_FILE_NAME = "file2.txt"
  private val FIRST_TEMP_FILE_CONTENTS = "123"
  private val SECOND_TEMP_FILE_CONTENTS = "456"
  private val SECRET_NAME = "secret"
  private val MOUNTED_SMALL_FILES_SECRET_MOUNT_PATH = "/etc/spark-submitted-files"

  private var tempFolder: File = _

  private val mountSmallFilesBootstrap = new DummyMountSmallFilesBootstrap

  before {
    MockitoAnnotations.initMocks(this)
    tempFolder = Utils.createTempDir()
  }

  after {
    tempFolder.delete()
  }

  test("Local files should be added to the secret.") {
    val firstTempFile = createTempFileWithContents(
      tempFolder, FIRST_TEMP_FILE_NAME, FIRST_TEMP_FILE_CONTENTS)
    val secondTempFile = createTempFileWithContents(
      tempFolder, SECOND_TEMP_FILE_NAME, SECOND_TEMP_FILE_CONTENTS)
    val sparkFiles = Seq(
      s"file://${firstTempFile.getAbsolutePath}",
      secondTempFile.getAbsolutePath)
    val configurationStep = new DriverMountLocalFilesStep(
      sparkFiles,
      SECRET_NAME,
      MOUNTED_SMALL_FILES_SECRET_MOUNT_PATH,
      mountSmallFilesBootstrap)
    val baseDriverSpec = new KubernetesDriverSpec(
      new PodBuilder().build(),
      new ContainerBuilder().build(),
      Seq.empty[HasMetadata],
      new SparkConf(false))
    val configuredDriverSpec = configurationStep.configureDriver(baseDriverSpec)
    assert(configuredDriverSpec.otherKubernetesResources.size === 1)
    assert(configuredDriverSpec.otherKubernetesResources(0).isInstanceOf[Secret])
    val localFilesSecret = configuredDriverSpec.otherKubernetesResources(0).asInstanceOf[Secret]
    assert(localFilesSecret.getMetadata.getName === SECRET_NAME)
    val expectedSecretContents = Map(
      FIRST_TEMP_FILE_NAME -> BaseEncoding.base64().encode(
        FIRST_TEMP_FILE_CONTENTS.getBytes(Charsets.UTF_8)),
      SECOND_TEMP_FILE_NAME -> BaseEncoding.base64().encode(
        SECOND_TEMP_FILE_CONTENTS.getBytes(Charsets.UTF_8)))
    assert(localFilesSecret.getData.asScala === expectedSecretContents)
    assert(configuredDriverSpec.driverPod.getMetadata.getLabels.asScala ===
      Map(mountSmallFilesBootstrap.LABEL_KEY -> mountSmallFilesBootstrap.LABEL_VALUE))
    assert(configuredDriverSpec.driverContainer.getEnv.size() === 1)
    assert(configuredDriverSpec.driverContainer.getEnv.get(0).getName ===
      mountSmallFilesBootstrap.ENV_KEY)
    assert(configuredDriverSpec.driverContainer.getEnv.get(0).getValue ===
      mountSmallFilesBootstrap.ENV_VALUE)
    assert(configuredDriverSpec.driverSparkConf.get(
      EXECUTOR_SUBMITTED_SMALL_FILES_SECRET) ===
      Some(SECRET_NAME))
    assert(configuredDriverSpec.driverSparkConf.get(
      EXECUTOR_SUBMITTED_SMALL_FILES_SECRET_MOUNT_PATH) ===
      Some(MOUNTED_SMALL_FILES_SECRET_MOUNT_PATH))
  }

  test("Using large files should throw an exception.") {
    val largeTempFileContents = BaseEncoding.base64().encode(new Array[Byte](10241))
    val largeTempFile = createTempFileWithContents(tempFolder, "large.txt", largeTempFileContents)
    val configurationStep = new DriverMountLocalFilesStep(
      Seq(largeTempFile.getAbsolutePath),
      SECRET_NAME,
      MOUNTED_SMALL_FILES_SECRET_MOUNT_PATH,
      mountSmallFilesBootstrap)
    val baseDriverSpec = new KubernetesDriverSpec(
      new PodBuilder().build(),
      new ContainerBuilder().build(),
      Seq.empty[HasMetadata],
      new SparkConf(false))
    try {
      configurationStep.configureDriver(baseDriverSpec)
      fail("Using the small local files mounter should not be allowed with big files.")
    } catch {
      case e: Throwable =>
        assert(e.getMessage ===
          s"requirement failed: Total size of all files submitted must be less than" +
            s" ${DriverMountLocalFilesStep.MAX_SECRET_BUNDLE_SIZE_BYTES_STRING}." +
            s" Total size for files ended up being ${Utils.bytesToString(largeTempFile.length())}.")
    }
  }

  private def createTempFileWithContents(
    root: File,
    fileName: String,
    fileContents: String): File = {
    val tempFile = new File(root, fileName)
    tempFile.createNewFile()
    Files.write(fileContents, tempFile, Charsets.UTF_8)
    tempFile
  }

  private class DummyMountSmallFilesBootstrap
      extends MountSmallFilesBootstrap("ignored", "ignored") {
    val LABEL_KEY = "smallFilesLabelKey"
    val LABEL_VALUE = "smallFilesLabelValue"
    val ENV_KEY = "smallFilesEnvKey"
    val ENV_VALUE = "smallFilesEnvValue"

    override def mountSmallFilesSecret(pod: Pod, container: Container): (Pod, Container) = {
      val editedPod = new PodBuilder(pod)
        .editOrNewMetadata()
        .addToLabels(LABEL_KEY, LABEL_VALUE)
        .endMetadata()
        .build()
      val editedContainer = new ContainerBuilder(container)
        .addNewEnv()
        .withName(ENV_KEY)
        .withValue(ENV_VALUE)
        .endEnv()
        .build()
      (editedPod, editedContainer)
    }
  }
}
