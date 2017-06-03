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

import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.security.SecureRandom
import java.util.UUID
import javax.ws.rs.{NotAuthorizedException, NotFoundException}
import javax.ws.rs.core.StreamingOutput

import com.google.common.io.{BaseEncoding, ByteStreams, Files}
import scala.collection.concurrent.TrieMap

import org.apache.spark.SparkException
import org.apache.spark.deploy.kubernetes.KubernetesCredentials
import org.apache.spark.deploy.kubernetes.submit.SubmittedResourceIdAndSecret
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class ResourceStagingServiceImpl(
      stagedResourcesStore: StagedResourcesStore,
      stagedResourcesCleaner: StagedResourcesCleaner)
    extends ResourceStagingService with Logging {

  override def uploadResources(
      resources: InputStream,
      resourcesOwner: StagedResourcesOwner): SubmittedResourceIdAndSecret = {
    val stagedResources = stagedResourcesStore.addResources(
        resourcesOwner.ownerNamespace, resources)
    stagedResourcesCleaner.registerResourceForCleaning(
      stagedResources.resourceId, resourcesOwner)
    SubmittedResourceIdAndSecret(stagedResources.resourceId, stagedResources.resourceSecret)
  }

  override def downloadResources(resourceId: String, resourceSecret: String): StreamingOutput = {
    val resource = stagedResourcesStore.getResources(resourceId)
        .getOrElse(throw new NotFoundException(s"No resource bundle found with id $resourceId"))
    if (!resource.resourceSecret.equals(resourceSecret)) {
      throw new NotAuthorizedException(s"Unauthorized to download resource with id $resourceId")
    }
    stagedResourcesCleaner.markResourceAsUsed(resourceId)
    new StreamingOutput {
      override def write(outputStream: OutputStream) = {
        Files.copy(resource.resourcesFile, outputStream)
      }
    }
  }

  override def ping(): String = "pong"
}
