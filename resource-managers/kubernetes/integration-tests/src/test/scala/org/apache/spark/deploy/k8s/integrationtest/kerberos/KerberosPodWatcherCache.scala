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

import io.fabric8.kubernetes.api.model.{Pod, Service}
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.internal.Logging

 /**
  * This class is used to ensure that the Hadoop cluster that is launched is executed
  * in this order: KDC --> NN --> DN --> Data-Populator and that each one of these nodes
  * is running before launching the Kerberos test.
  */
private[spark] class KerberosPodWatcherCache(
  kerberosUtils: KerberosUtils,
  labels: Map[String, String]) extends Logging {
  private val kubernetesClient = kerberosUtils.getClient
  private val namespace = kerberosUtils.getNamespace
  private var podWatcher: Watch = _
  private var serviceWatcher: Watch = _
  private var podCache =
    scala.collection.mutable.Map[String, String]()
  private var serviceCache =
    scala.collection.mutable.Map[String, String]()
  private var lock: Lock = new ReentrantLock()
  private var kdcRunning: Condition = lock.newCondition()
  private var nnRunning: Condition = lock.newCondition()
  private var dnRunning: Condition = lock.newCondition()
  private var dpRunning: Condition = lock.newCondition()
  private var kdcIsUp: Boolean = false
  private var nnIsUp: Boolean = false
  private var dnIsUp: Boolean = false
  private var dpIsUp: Boolean = false
  private var kdcSpawned: Boolean = false
  private var nnSpawned: Boolean = false
  private var dnSpawned: Boolean = false
  private var dpSpawned: Boolean = false
  private var dnName: String = _
  private var dpName: String = _

  private val blockingThread = new Thread(new Runnable {
    override def run(): Unit = {
      logInfo("Beginning of Cluster lock")
      lock.lock()
      try {
        while (!kdcIsUp) kdcRunning.await()
        while (!nnIsUp) nnRunning.await()
        while (!dnIsUp) dnRunning.await()
        while (!dpIsUp) dpRunning.await()
      } finally {
        logInfo("Ending the Cluster lock")
        lock.unlock()
        stop()
      }
    }
  })

  private val podWatcherThread = new Thread(new Runnable {
    override def run(): Unit = {
      logInfo("Beginning the watch of Pods")
      podWatcher = kubernetesClient
        .pods()
        .withLabels(labels.asJava)
        .watch(new Watcher[Pod] {
          override def onClose(cause: KubernetesClientException): Unit =
            logInfo("Ending the watch of Pods")
          override def eventReceived(action: Watcher.Action, resource: Pod): Unit = {
            val name = resource.getMetadata.getName
            val keyName = podNameParse(name)
            action match {
              case Action.DELETED | Action.ERROR =>
                logInfo(s"$name either deleted or error")
                podCache.remove(keyName)
              case Action.ADDED | Action.MODIFIED =>
                val phase = resource.getStatus.getPhase
                logInfo(s"$name is as $phase")
                if (name.startsWith("dn1")) { dnName = name }
                if (name.startsWith("data-populator")) { dpName = name }
                podCache(keyName) = phase
                if (maybeDeploymentAndServiceDone(keyName)) {
                  val modifyAndSignal: Runnable = new MSThread(keyName)
                  new Thread(modifyAndSignal).start()
                }}}})
    }})

  private val serviceWatcherThread = new Thread(new Runnable {
    override def run(): Unit = {
      logInfo("Beginning the watch of Services")
      serviceWatcher = kubernetesClient
        .services()
        .withLabels(labels.asJava)
        .watch(new Watcher[Service] {
          override def onClose(cause: KubernetesClientException): Unit =
            logInfo("Ending the watch of Services")
          override def eventReceived(action: Watcher.Action, resource: Service): Unit = {
            val name = resource.getMetadata.getName
            action match {
              case Action.DELETED | Action.ERROR =>
                logInfo(s"$name either deleted or error")
                serviceCache.remove(name)
              case Action.ADDED | Action.MODIFIED =>
                val bound = resource.getSpec.getSelector.get("kerberosService")
                logInfo(s"$name is bounded to $bound")
                serviceCache(name) = bound
                if (maybeDeploymentAndServiceDone(name)) {
                  val modifyAndSignal: Runnable = new MSThread(name)
                  new Thread(modifyAndSignal).start()
                }}}})
      logInfo("Launching the Cluster")
      if (!kdcSpawned) {
        logInfo("Launching the KDC Node")
        kdcSpawned = true
        deploy(kerberosUtils.getKDC)
      }
    }})

  def start(): Unit = {
    blockingThread.start()
    podWatcherThread.start()
    serviceWatcherThread.start()
    blockingThread.join()
    podWatcherThread.join()
    serviceWatcherThread.join()
  }

  def stop(): String = {
    podWatcher.close()
    serviceWatcher.close()
    dpName
  }

  private def maybeDeploymentAndServiceDone(name: String): Boolean = {
    val finished = podCache.get(name).contains("Running") &&
      serviceCache.get(name).contains(name)
    if (!finished) {
      logInfo(s"$name is not up with a service")
      if (name == "kerberos") kdcIsUp = false
      else if (name == "nn") nnIsUp = false
      else if (name == "dn1") dnIsUp = false
      else if (name == "data-populator") dpIsUp = false
    }
    finished
  }

  private def deploy(kdc: KerberosDeployment) : Unit = {
    kubernetesClient
      .extensions().deployments().inNamespace(namespace).create(kdc.podDeployment)
    kubernetesClient
      .services().inNamespace(namespace).create(kdc.service)
  }

  private class MSThread(name: String) extends Runnable {
    override def run(): Unit = {
      logInfo(s"$name Node and Service is up")
      lock.lock()
      if (name == "kerberos") {
        kdcIsUp = true
        logInfo(s"kdc has signaled")
        try {
          kdcRunning.signalAll()
        } finally {
          lock.unlock()
        }
        if (!nnSpawned) {
          logInfo("Launching the NN Node")
          nnSpawned = true
          deploy(kerberosUtils.getNN)
        }
      }
      else if (name == "nn") {
        while (!kdcIsUp) kdcRunning.await()
        nnIsUp = true
        logInfo(s"nn has signaled")
        try {
          nnRunning.signalAll()
        } finally {
          lock.unlock()
        }
        if (!dnSpawned) {
          logInfo("Launching the DN Node")
          dnSpawned = true
          deploy(kerberosUtils.getDN)
        }
      }
      else if (name == "dn1") {
        while (!kdcIsUp) kdcRunning.await()
        while (!nnIsUp) nnRunning.await()
        dnIsUp = true
        logInfo(s"dn1 has signaled")
        try {
          dnRunning.signalAll()
        } finally {
          lock.unlock()
        }
        if (!dpSpawned) {
          logInfo("Launching the DP Node")
          dpSpawned = true
          deploy(kerberosUtils.getDP)
        }
      }
      else if (name == "data-populator") {
        while (!kdcIsUp) kdcRunning.await()
        while (!nnIsUp) nnRunning.await()
        while (!dnIsUp) dnRunning.await()
        while (!hasInLogs(dnName, "Got finalize command for block pool")) {
          logInfo("Waiting on DN to be formatted")
          Thread.sleep(500)
        }
        dpIsUp = true
        logInfo(s"data-populator has signaled")
        try {
          dpRunning.signalAll()
        } finally {
          lock.unlock()
        }
      }
    }
  }

  private def podNameParse(name: String) : String = {
    name match {
      case _ if name.startsWith("kerberos") => "kerberos"
      case _ if name.startsWith("nn") => "nn"
      case _ if name.startsWith("dn1") => "dn1"
      case _ if name.startsWith("data-populator") => "data-populator"
    }
  }

  def hasInLogs(name: String, expectation: String): Boolean = {
    kubernetesClient
      .pods()
      .withName(name)
      .getLog().contains(expectation)
  }
}
