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

import java.io.{ByteArrayInputStream, File}
import java.nio.file.Paths

import com.google.common.io.Files
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

private[spark] class StagedResourcesStoreSuite extends SparkFunSuite with BeforeAndAfter {

  private val resourceBytes = Array[Byte](1, 2, 3, 4)
  private val namespace = "namespace"
  private var dependencyRootDir: File = _
  private var stagedResourcesStore: StagedResourcesStore = _

  before {
    dependencyRootDir = Utils.createTempDir()
    stagedResourcesStore = new StagedResourcesStoreImpl(dependencyRootDir)
  }

  after {
    dependencyRootDir.delete()
  }

  test("Uploads should write data to the underlying disk") {
    val resourceIdAndSecret = Utils.tryWithResource(new ByteArrayInputStream(resourceBytes)) {
        resourceStream =>
      stagedResourcesStore.addResources(namespace, resourceStream)
    }
    val resourceNamespaceDir = Paths.get(dependencyRootDir.getAbsolutePath, "namespace").toFile
    assert(resourceNamespaceDir.isDirectory, s"Resource namespace dir was not created at" +
        s" ${resourceNamespaceDir.getAbsolutePath} or is not a directory.")
    val resourceDirs = resourceNamespaceDir.listFiles()
    assert(resourceDirs.length === 1, s"Resource root directory did not have exactly one" +
        s" subdirectory. Got: ${resourceDirs.map(_.getAbsolutePath).mkString(",")}")
    assert(resourceDirs(0).getName === resourceIdAndSecret.resourceId)
    val resourceTgz = new File(resourceDirs(0), "resources.data")
    assert(resourceTgz.isFile,
        s"Resources written to ${resourceTgz.getAbsolutePath} does not exist or is not a file.")
    val resourceTgzBytes = Files.toByteArray(resourceTgz)
    assert(resourceTgzBytes.toSeq === resourceBytes.toSeq, "Incorrect resource bytes were written.")
  }

  test("Uploading and then getting should return a stream with the written bytes.") {
    val resourceIdAndSecret = Utils.tryWithResource(new ByteArrayInputStream(resourceBytes)) {
      resourceStream =>
        stagedResourcesStore.addResources(namespace, resourceStream)
    }
    val resources = stagedResourcesStore.getResources(resourceIdAndSecret.resourceId)
    assert(resources.map(_.resourcesFile)
        .map(Files.toByteArray)
        .exists(resourceBytes.sameElements(_)))
    assert(resources.exists(_.resourceId == resourceIdAndSecret.resourceId))
    assert(resources.exists(_.resourceSecret == resourceIdAndSecret.resourceSecret))
  }

  test("Uploading and then deleting should result in the resource directory being deleted.") {
    val resourceIdAndSecret = Utils.tryWithResource(new ByteArrayInputStream(resourceBytes)) {
      resourceStream =>
        stagedResourcesStore.addResources(namespace, resourceStream)
    }
    stagedResourcesStore.removeResources(resourceIdAndSecret.resourceId)
    val resourceNamespaceDir = Paths.get(dependencyRootDir.getAbsolutePath, "namespace").toFile
    assert(resourceNamespaceDir.listFiles().isEmpty)
    assert(stagedResourcesStore.getResources(resourceIdAndSecret.resourceId).isEmpty)
  }
}
