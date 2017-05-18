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

import java.io.{ByteArrayInputStream, File}
import java.nio.file.Paths

import com.google.common.io.Files

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.KubernetesCredentials
import org.apache.spark.util.Utils

/**
 * Unit, scala-level tests for KubernetesSparkDependencyServiceImpl. The coverage here
 * differs from that of KubernetesSparkDependencyServerSuite as here we invoke the
 * implementation methods directly as opposed to over HTTP, as well as check the
 * data written to the underlying disk.
 */
class ResourceStagingServiceImplSuite extends SparkFunSuite {

  private val dependencyRootDir = Utils.createTempDir()
  private val serviceImpl = new ResourceStagingServiceImpl(dependencyRootDir)
  private val resourceBytes = Array[Byte](1, 2, 3, 4)
  private val kubernetesCredentials = KubernetesCredentials(
    Some("token"), Some("caCert"), Some("key"), Some("cert"))
  private val namespace = "namespace"
  private val labels = Map("label1" -> "label1value", "label2" -> "label2value")

  test("Uploads should write data to the underlying disk") {
    Utils.tryWithResource(new ByteArrayInputStream(resourceBytes)) { resourceStream =>
      serviceImpl.uploadResources(labels, namespace, resourceStream, kubernetesCredentials)
    }
    val resourceNamespaceDir = Paths.get(dependencyRootDir.getAbsolutePath, "namespace").toFile
    assert(resourceNamespaceDir.isDirectory, s"Resource namespace dir was not created at" +
      s" ${resourceNamespaceDir.getAbsolutePath} or is not a directory.")
    val resourceDirs = resourceNamespaceDir.listFiles()
    assert(resourceDirs.length === 1, s"Resource root directory did not have exactly one" +
      s" subdirectory. Got: ${resourceDirs.map(_.getAbsolutePath).mkString(",")}")
    val resourceTgz = new File(resourceDirs(0), "resources.data")
    assert(resourceTgz.isFile,
      s"Resources written to ${resourceTgz.getAbsolutePath} does not exist or is not a file.")
    val resourceTgzBytes = Files.toByteArray(resourceTgz)
    assert(resourceTgzBytes.toSeq === resourceBytes.toSeq, "Incorrect resource bytes were written.")
  }
}
