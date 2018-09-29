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

package org.apache.spark.deploy.k8s.integrationtest.kerberos

import io.fabric8.kubernetes.api.model.{PersistentVolume, PersistentVolumeClaim}
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{INTERVAL, TIMEOUT}
import org.apache.spark.internal.Logging

 /**
  * This class is responsible for ensuring that the persistent volume claims are bounded
  * to the correct persistent volume and that they are both created before launching the
  * pods which expect to use them.
  */
private[spark] class KerberosPVWatcherCache(
    kerberosUtils: KerberosUtils,
    labels: Map[String, String]) extends Logging with Eventually with Matchers {
    private val kubernetesClient = kerberosUtils.getClient
    private val namespace = kerberosUtils.getNamespace

    // Cache for PVs and PVCs
    private val pvCache = scala.collection.mutable.Map[String, String]()
    private val pvcCache = scala.collection.mutable.Map[String, String]()

    // Watching PVs
    logInfo("Beginning the watch of Persistent Volumes")
    private val pvWatcher: Watch = kubernetesClient
      .persistentVolumes()
      .withLabels(labels.asJava)
      .watch(new Watcher[PersistentVolume] {
        override def onClose(cause: KubernetesClientException): Unit =
          logInfo("Ending the watch of Persistent Volumes", cause)
        override def eventReceived(action: Watcher.Action, resource: PersistentVolume): Unit = {
          val name = resource.getMetadata.getName
          action match {
            case Action.DELETED | Action.ERROR =>
              logInfo(s"$name either deleted or error")
              pvCache.remove(name)
            case Action.ADDED | Action.MODIFIED =>
              val phase = resource.getStatus.getPhase
              logInfo(s"$name is at stage: $phase")
              pvCache(name) = phase }}})

    // Watching PVCs
    logInfo("Beginning the watch of Persistent Volume Claims")
    private val pvcWatcher: Watch = kubernetesClient
      .persistentVolumeClaims()
      .withLabels(labels.asJava)
      .watch(new Watcher[PersistentVolumeClaim] {
        override def onClose(cause: KubernetesClientException): Unit =
          logInfo("Ending the watch of Persistent Volume Claims")
        override def eventReceived(
          action: Watcher.Action,
          resource: PersistentVolumeClaim): Unit = {
          val name = resource.getMetadata.getName
          action match {
            case Action.DELETED | Action.ERROR =>
              logInfo(s"$name either deleted or error")
              pvcCache.remove(name)
            case Action.ADDED | Action.MODIFIED =>
              val volumeName = resource.getSpec.getVolumeName
              val state = resource.getStatus.getPhase
              logInfo(s"$name claims itself to $volumeName and is $state")
              pvcCache(name) = s"$volumeName $state"}}})

    // Check for PVC being bounded to correct PV
    private def check(name: String): Boolean = {
      pvCache.get(name).contains("Bound") &&
      pvcCache.get(name).contains(s"$name Bound")
    }

    def deploy(kbs: KerberosStorage) : Unit = {
      logInfo("Launching the Persistent Storage")
      kubernetesClient
        .persistentVolumes().create(kbs.persistentVolume)
      // Making sure PV is Available for creation of PVC
      Eventually.eventually(TIMEOUT, INTERVAL) {
        (pvCache(kbs.name) == "Available") should be (true) }
      kubernetesClient
        .persistentVolumeClaims().inNamespace(namespace).create(kbs.persistentVolumeClaim)
      Eventually.eventually(TIMEOUT, INTERVAL) { check(kbs.name) should be (true) }
    }

     def stopWatch(): Unit = {
       // Closing Watchers
       pvWatcher.close()
       pvcWatcher.close()
     }
}
