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

import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.security.SecureRandom
import java.util.UUID
import javax.ws.rs.{NotAuthorizedException, NotFoundException}
import javax.ws.rs.core.StreamingOutput

import com.google.common.io.{BaseEncoding, ByteStreams, Files}
import scala.collection.concurrent.TrieMap

import org.apache.spark.SparkException
import org.apache.spark.deploy.rest.KubernetesCredentials
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class ResourceStagingServiceImpl(dependenciesRootDir: File)
    extends ResourceStagingService with Logging {

  private val SECURE_RANDOM = new SecureRandom()
  // TODO clean up these resources based on the driver's lifecycle
  private val stagedResources = TrieMap.empty[String, StagedResources]

  override def uploadResources(
      podLabels: Map[String, String],
      podNamespace: String,
      resources: InputStream,
      kubernetesCredentials: KubernetesCredentials): StagedResourceIdentifier = {
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
      val resourcesTgz = new File(resourcesDir, "resources.data")
      Utils.tryWithResource(new FileOutputStream(resourcesTgz)) { ByteStreams.copy(resources, _) }
      stagedResources(resourceId) = StagedResources(
        resourceSecret,
        podLabels,
        podNamespace,
        resourcesTgz,
        kubernetesCredentials)
      StagedResourceIdentifier(resourceId, resourceSecret)
    } catch {
      case e: Throwable =>
        if (!resourcesDir.delete()) {
          logWarning(s"Failed to delete application directory $resourcesDir.")
        }
        throw e
    }
  }

  override def downloadResources(resourceId: String, resourceSecret: String): StreamingOutput = {
    val resource = stagedResources
        .get(resourceId)
        .getOrElse(throw new NotFoundException(s"No resource bundle found with id $resourceId"))
    if (!resource.resourceSecret.equals(resourceSecret)) {
      throw new NotAuthorizedException(s"Unauthorized to download resource with id $resourceId")
    }
    new StreamingOutput {
      override def write(outputStream: OutputStream) = {
        Files.copy(resource.resourcesFile, outputStream)
      }
    }
  }
}

private case class StagedResources(
  resourceSecret: String,
  podLabels: Map[String, String],
  podNamespace: String,
  resourcesFile: File,
  kubernetesCredentials: KubernetesCredentials)
