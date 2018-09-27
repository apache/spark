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

import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{PersistentVolume, PersistentVolumeClaim}
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.internal.Logging

 /**
  * This class is responsible for ensuring that the persistent volume claims are bounded
  * to the correct persistent volume and that they are both created before launching the
  * pods which expect to use them.
  */
private[spark] class KerberosPVWatcherCache(
    kerberosUtils: KerberosUtils,
    labels: Map[String, String]) extends Logging {
    private val kubernetesClient = kerberosUtils.getClient
    private val namespace = kerberosUtils.getNamespace
    private var pvWatcher: Watch = _
    private var pvcWatcher: Watch = _
    private var pvCache =
      scala.collection.mutable.Map[String, String]()
    private var pvcCache =
      scala.collection.mutable.Map[String, String]()
    private var lock: Lock = new ReentrantLock()
    private var nnBounded: Condition = lock.newCondition()
    private var ktBounded: Condition = lock.newCondition()
    private var nnIsUp: Boolean = false
    private var ktIsUp: Boolean = false
    private var nnSpawned: Boolean = false
    private var ktSpawned: Boolean = false
    private val blockingThread = new Thread(new Runnable {
      override def run(): Unit = {
        logInfo("Beginning of Persistent Storage Lock")
        lock.lock()
        try {
          while (!nnIsUp) nnBounded.await()
          while (!ktIsUp) ktBounded.await()
        } finally {
          logInfo("Ending the Persistent Storage lock")
          lock.unlock()
          stop()
        }
      }
    })
    private val pvWatcherThread = new Thread(new Runnable {
      override def run(): Unit = {
        logInfo("Beginning the watch of Persistent Volumes")
        pvWatcher = kubernetesClient
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
                  pvCache(name) = phase
                  if (maybeDeploymentAndServiceDone(name)) {
                    val modifyAndSignal: Runnable = new MSThread(name)
                    new Thread(modifyAndSignal).start()
                  }}}})
      }})
    private val pvcWatcherThread = new Thread(new Runnable {
      override def run(): Unit = {
        logInfo("Beginning the watch of Persistent Volume Claims")
        pvcWatcher = kubernetesClient
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
                  logInfo(s"$name claims itself to $volumeName")
                  pvcCache(name) = volumeName
                  if (maybeDeploymentAndServiceDone(name)) {
                    val modifyAndSignal: Runnable = new MSThread(name)
                    new Thread(modifyAndSignal).start()
                  }}}})
        logInfo("Launching the Persistent Storage")
        if (!nnSpawned) {
          logInfo("Launching the NN Hadoop PV+PVC")
          nnSpawned = true
          deploy(kerberosUtils.getNNStorage)
        }
      }})

    def start(): Unit = {
      blockingThread.start()
      pvWatcherThread.start()
      pvcWatcherThread.start()
      blockingThread.join()
      pvWatcherThread.join()
      pvcWatcherThread.join()
    }
    def stop(): Unit = {
      pvWatcher.close()
      pvcWatcher.close()
    }

    private def maybeDeploymentAndServiceDone(name: String): Boolean = {
      val finished = pvCache.get(name).contains("Available") &&
        pvcCache.get(name).contains(name)
      if (!finished) {
        logInfo(s"$name is not available")
        if (name == "nn-hadoop") nnIsUp = false
        else if (name == "server-keytab") ktIsUp = false
      }
      finished
    }

    private def deploy(kbs: KerberosStorage) : Unit = {
      kubernetesClient
        .persistentVolumeClaims().inNamespace(namespace).create(kbs.persistentVolumeClaim)
      kubernetesClient
        .persistentVolumes().create(kbs.persistentVolume)
    }

    private class MSThread(name: String) extends Runnable {
      override def run(): Unit = {
        logInfo(s"$name PV and PVC are bounded")
        lock.lock()
        if (name == "nn-hadoop") {
          nnIsUp = true
          logInfo(s"nn-hadoop is bounded")
          try {
            nnBounded.signalAll()
          } finally {
            lock.unlock()
          }
          if (!ktSpawned) {
            logInfo("Launching the KT Hadoop PV+PVC")
            ktSpawned = true
            deploy(kerberosUtils.getKTStorage)
          }
        }
        else if (name == "server-keytab") {
          while (!nnIsUp) ktBounded.await()
          ktIsUp = true
          logInfo(s"server-keytab is bounded")
          try {
            ktBounded.signalAll()
          } finally {
            lock.unlock()
          }
        }}
    }
}
