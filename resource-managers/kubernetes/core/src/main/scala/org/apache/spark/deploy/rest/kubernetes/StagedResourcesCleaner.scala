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

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Clock

private[spark] trait StagedResourcesCleaner {

  def start(): Unit

  def registerResourceForCleaning(
      resourceId: String, stagedResourceOwner: StagedResourcesOwner): Unit

  def markResourceAsUsed(resourceId: String): Unit
}

private class StagedResourcesCleanerImpl(
      stagedResourcesStore: StagedResourcesStore,
      kubernetesClient: KubernetesClient,
      cleanupExecutorService: ScheduledExecutorService,
      clock: Clock,
      initialAccessExpirationMs: Long)
    extends StagedResourcesCleaner {

  private val CLEANUP_INTERVAL_MS = 30000
  private val RESOURCE_LOCK = new Object()
  private val activeResources = mutable.Map.empty[String, MonitoredResource]
  private val unusedResources = mutable.Map.empty[String, UnusedMonitoredResource]

  override def start(): Unit = {
    cleanupExecutorService.scheduleAtFixedRate(
        new CleanupRunnable(),
        CLEANUP_INTERVAL_MS,
        CLEANUP_INTERVAL_MS,
        TimeUnit.MILLISECONDS)
  }

  override def registerResourceForCleaning(
      resourceId: String, stagedResourceOwner: StagedResourcesOwner): Unit = {
    RESOURCE_LOCK.synchronized {
      unusedResources(resourceId) = UnusedMonitoredResource(
          clock.getTimeMillis() + initialAccessExpirationMs,
          MonitoredResource(resourceId, stagedResourceOwner))

    }
  }

  override def markResourceAsUsed(resourceId: String): Unit = RESOURCE_LOCK.synchronized {
    val resource = unusedResources.remove(resourceId)
    resource.foreach { res =>
      activeResources(resourceId) = res.resource
    }
  }

  private class CleanupRunnable extends Runnable with Logging {

    override def run(): Unit = {
      // Make a copy so we can iterate through this while modifying
      val activeResourcesCopy = RESOURCE_LOCK.synchronized {
        Map.apply(activeResources.toSeq: _*)
      }
      for ((resourceId, resource) <- activeResourcesCopy) {
        val namespace = kubernetesClient.namespaces()
            .withName(resource.stagedResourceOwner.ownerNamespace)
            .get()
        if (namespace == null) {
          logInfo(s"Resource files with id $resourceId is being removed. The owner's namespace" +
            s" ${resource.stagedResourceOwner.ownerNamespace} was not found.")
          stagedResourcesStore.removeResources(resourceId)
          RESOURCE_LOCK.synchronized {
            activeResources.remove(resourceId)
          }
        } else {
          val metadataOperation = resource.stagedResourceOwner.ownerType match {
            case StagedResourcesOwnerType.Pod =>
              kubernetesClient.pods().inNamespace(resource.stagedResourceOwner.ownerNamespace)
            case _ =>
              throw new SparkException(s"Unsupported resource owner type for cleanup:" +
                s" ${resource.stagedResourceOwner.ownerType}")
          }
          if (metadataOperation
            .withLabels(resource.stagedResourceOwner.ownerLabels.asJava)
            .list()
            .getItems
            .isEmpty) {
            logInfo(s"Resource files with id $resourceId is being removed. Owners of the" +
              s" resource with namespace: ${resource.stagedResourceOwner.ownerNamespace}," +
              s" type: ${resource.stagedResourceOwner.ownerType}, and labels:" +
              s" ${resource.stagedResourceOwner.ownerLabels} was not found on the API server.")
            stagedResourcesStore.removeResources(resourceId)
            RESOURCE_LOCK.synchronized {
              activeResources.remove(resourceId)
            }
          }
        }
      }

      // Make a copy so we can iterate through this while modifying
      val unusedResourcesCopy = RESOURCE_LOCK.synchronized {
        Map.apply(unusedResources.toSeq: _*)
      }

      for ((resourceId, resource) <- unusedResourcesCopy) {
        if (resource.expiresAt < clock.getTimeMillis()) {
          RESOURCE_LOCK.synchronized {
            // Check for existence again here (via foreach) because in between the time we starting
            // iterating over the unused resources copy, we might have already marked the resource
            // as active in-between, and likely shouldn't remove the resources in such a case.
            unusedResources.remove(resourceId).foreach { _ =>
              logInfo(s"Resources with id $resourceId was not accessed after being added to" +
                s" the staging server at least $initialAccessExpirationMs ms ago. The resource" +
                s" will be deleted.")
              stagedResourcesStore.removeResources(resourceId)
            }
          }
        }
      }
    }
  }

  private case class MonitoredResource(
      resourceId: String,
      stagedResourceOwner: StagedResourcesOwner)

  private case class UnusedMonitoredResource(expiresAt: Long, resource: MonitoredResource)
}


