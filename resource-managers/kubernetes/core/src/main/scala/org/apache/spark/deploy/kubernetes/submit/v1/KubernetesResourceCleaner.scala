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
package org.apache.spark.deploy.kubernetes.submit.v1

import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class KubernetesResourceCleaner extends Logging {

  private val resources = mutable.HashMap.empty[(String, String), HasMetadata]

  // Synchronized because deleteAllRegisteredResourcesFromKubernetes may be called from a
  // shutdown hook
  def registerOrUpdateResource(resource: HasMetadata): Unit = synchronized {
    resources.put((resource.getMetadata.getName, resource.getKind), resource)
  }

  def unregisterResource(resource: HasMetadata): Unit = synchronized {
    resources.remove((resource.getMetadata.getName, resource.getKind))
  }

  def deleteAllRegisteredResourcesFromKubernetes(kubernetesClient: KubernetesClient): Unit = {
    synchronized {
      val resourceCount = resources.size
      logInfo(s"Deleting ${resourceCount} registered Kubernetes resources...")
      resources.values.foreach { resource =>
        Utils.tryLogNonFatalError {
          kubernetesClient.resource(resource).delete()
        }
      }
      resources.clear()
      logInfo(s"Deleted ${resourceCount} registered Kubernetes resources.")
    }
  }
}
