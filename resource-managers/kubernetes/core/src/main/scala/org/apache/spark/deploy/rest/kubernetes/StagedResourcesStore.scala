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

import java.io.{File, FileOutputStream, InputStream, IOException}
import java.security.SecureRandom
import java.util.UUID

import com.google.common.io.{BaseEncoding, ByteStreams}
import org.apache.commons.io.FileUtils
import scala.collection.concurrent.TrieMap

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


private[spark] trait StagedResourcesStore {

  /**
   * Store the given stream on disk and return its resource ID and secret.
   */
  def addResources(
      podNamespace: String,
      resources: InputStream): StagedResources

  /**
   * Retrieve a resource bundle with the given id. Returns empty if no resources match this id.
   */
  def getResources(resourceId: String): Option[StagedResources]

  def removeResources(resourceId: String): Unit
}

private[spark] class StagedResourcesStoreImpl(dependenciesRootDir: File)
    extends StagedResourcesStore with Logging {

  private val SECURE_RANDOM = new SecureRandom()
  private val stagedResources = TrieMap.empty[String, StagedResources]

  override def addResources(
      podNamespace: String,
      resources: InputStream): StagedResources = {
    val resourceId = UUID.randomUUID().toString
    val secretBytes = new Array[Byte](1024)
    SECURE_RANDOM.nextBytes(secretBytes)
    val resourceSecret = resourceId + "-" + BaseEncoding.base64().encode(secretBytes)

    val namespaceDir = new File(dependenciesRootDir, podNamespace)
    val resourcesDir = new File(namespaceDir, resourceId)
    try {
      if (!resourcesDir.exists()) {
        if (!resourcesDir.mkdirs()) {
          throw new SparkException("Failed to create dependencies directory for application" +
            s" at ${resourcesDir.getAbsolutePath}")
        }
      }
      // TODO encrypt the written data with the secret.
      val resourcesFile = new File(resourcesDir, "resources.data")
      Utils.tryWithResource(new FileOutputStream(resourcesFile)) {
        ByteStreams.copy(resources, _)
      }
      val resourceBundle = StagedResources(resourceId, resourceSecret, resourcesFile)
      stagedResources(resourceId) = resourceBundle
      resourceBundle
    } catch {
      case e: Throwable =>
        if (!resourcesDir.delete()) {
          logWarning(s"Failed to delete application directory $resourcesDir.")
        }
        stagedResources.remove(resourceId)
        throw e
    }
  }

  override def getResources(resourceId: String): Option[StagedResources] = {
    stagedResources.get(resourceId)
  }

  override def removeResources(resourceId: String): Unit = {
    stagedResources.remove(resourceId)
        .map(_.resourcesFile.getParentFile)
        .foreach { resourcesDirectory =>
      try {
        FileUtils.deleteDirectory(resourcesDirectory)
      } catch {
        case e: IOException =>
          logWarning(s"Failed to delete resources directory" +
            s" at ${resourcesDirectory.getAbsolutePath}", e)
      }
    }
  }
}

